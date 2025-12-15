use crate::config::Config;
use crate::flashloan::{FlashLoanInstructions, FlashLoanSpec};
use crate::meteora::{MeteoraDammParams, MeteoraDlmmParams};
use crate::orca::{OrcaPoolParams, OrcaSwapAccounts};
use crate::raydium::{RaydiumPoolParams, RaydiumSwapAccounts};
use anyhow::{bail, Result};
use crossbeam_queue::ArrayQueue;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::sync::Arc;
use std::time::Instant;

/// Shared in-memory queues that connect ingest, planner, and executor stages.
pub struct Pipeline {
    pool_updates: Arc<ArrayQueue<PoolUpdate>>,
    trade_updates: Arc<ArrayQueue<TradeUpdate>>,
    arb_plans: Arc<ArrayQueue<ArbPlan>>,
    curve_states: Arc<ArrayQueue<CurveStateUpdate>>,
}

impl Pipeline {
    pub fn new(config: &Config) -> Result<Self> {
        let pool_capacity = config.corecast.cache.channel_capacity;
        let trade_capacity = config.corecast.cache.channel_capacity;
        let plan_capacity = config.corecast.cache.warm_capacity.max(64);
        let state_capacity = config.corecast.cache.channel_capacity;

        for (name, cap) in [
            ("pool_updates", pool_capacity),
            ("trade_updates", trade_capacity),
            ("arb_plans", plan_capacity),
            ("curve_states", state_capacity),
        ] {
            if cap == 0 {
                bail!("queue {name} must have capacity > 0");
            }
        }

        Ok(Self {
            pool_updates: Arc::new(ArrayQueue::new(pool_capacity)),
            trade_updates: Arc::new(ArrayQueue::new(trade_capacity)),
            arb_plans: Arc::new(ArrayQueue::new(plan_capacity)),
            curve_states: Arc::new(ArrayQueue::new(state_capacity)),
        })
    }

    pub fn handles(&self) -> PipelineHandles {
        PipelineHandles {
            pool_updates: self.pool_updates.clone(),
            trade_updates: self.trade_updates.clone(),
            arb_plans: self.arb_plans.clone(),
            curve_states: self.curve_states.clone(),
        }
    }
}

#[derive(Clone)]
pub struct PipelineHandles {
    pub pool_updates: Arc<ArrayQueue<PoolUpdate>>,
    pub trade_updates: Arc<ArrayQueue<TradeUpdate>>,
    pub arb_plans: Arc<ArrayQueue<ArbPlan>>,
    pub curve_states: Arc<ArrayQueue<CurveStateUpdate>>,
}

/// Describes the liquidity state delta for a single pool emitted by Bitquery CoreCast.
#[derive(Debug, Clone)]
pub struct PoolUpdate {
    pub pool: Pubkey,
    pub program: Pubkey,
    pub curve: CurveKind,
    pub slot: u64,
    pub block_time: i64,
    pub unix_timestamp: i64,
    pub seq: u64,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_reserve: u64,
    pub quote_reserve: u64,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub signature: Option<Signature>,
}

/// Advisory trade heartbeat derived from Bitquery `dex_trades` stream.
#[derive(Debug, Clone)]
pub struct TradeUpdate {
    pub pool: Pubkey,
    pub trader: Pubkey,
    pub base_amount: i128,
    pub quote_amount: i128,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub slot: u64,
    pub block_time: i64,
    pub signature: Option<Signature>,
    pub instruction_index: u32,
    pub program: Pubkey,
    pub curve_hint: Option<CurveKind>,
    pub accounts: Vec<TradeAccountMeta>,
}

#[derive(Debug, Clone)]
pub struct TradeAccountMeta {
    pub address: Pubkey,
    pub mint: Option<Pubkey>,
    pub owner: Option<Pubkey>,
    pub is_signer: bool,
    pub is_writable: bool,
}

/// Planned arbitrage bundle ready for execution.
#[derive(Debug, Clone)]
pub struct ArbPlan {
    pub id: u64,
    pub borrow_mint: Pubkey,
    pub notional: u64,
    pub expected_profit_lamports: i64,
    pub hop_count: u8,
    pub hops: Vec<PlanHop>,
    pub enqueue_instant: Instant,
    /// Solend flash-loan metadata captured at planning time.
    pub flash_loan: Arc<FlashLoanSpec>,
    /// Pre-baked Solend borrow/repay instructions for the plan.
    pub flash_instructions: FlashLoanInstructions,
    /// Curve-specific swap instructions prepared at planning time.
    pub swap_instructions: Vec<Instruction>,
    /// Optional instruction batches to run after the flash-loan bundle completes.
    pub post_flash_batches: Vec<Vec<Instruction>>,
    /// Serialized Solana transactions (UDP packet payloads) that form this bundle.
    pub transactions: Vec<Vec<u8>>,
}

/// Describes a single DEX hop the executor must realize when building the bundle.
#[derive(Debug, Clone)]
pub struct PlanHop {
    pub pool: Pubkey,
    pub program: Pubkey,
    pub curve: CurveKind,
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub amount_in: u64,
    pub min_amount_out: u64,
    pub accounts: HopAccounts,
}

#[derive(Debug, Clone)]
pub enum HopAccounts {
    RaydiumClmm(RaydiumSwapAccounts),
    OrcaWhirlpool(OrcaSwapAccounts),
    MeteoraDamm(MeteoraDammParams),
    MeteoraDlmm(MeteoraDlmmParams),
}

#[derive(Debug, Clone)]
pub enum CurveStateUpdate {
    RaydiumClmm {
        pool: Pubkey,
        params: RaydiumPoolParams,
    },
    OrcaWhirlpool {
        pool: Pubkey,
        params: OrcaPoolParams,
    },
    MeteoraDamm {
        pool: Pubkey,
        params: MeteoraDammParams,
    },
    MeteoraDlmm {
        pool: Pubkey,
        params: MeteoraDlmmParams,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CurveKind {
    RaydiumClmm,
    OrcaWhirlpool,
    MeteoraDamm,
    MeteoraDlmm,
}

impl CurveKind {
    /// Returns a conservative default swap fee in basis points for the curve.
    pub fn default_fee_bps(self) -> u16 {
        match self {
            CurveKind::RaydiumClmm => 25,
            CurveKind::OrcaWhirlpool => 30,
            CurveKind::MeteoraDamm => 20,
            CurveKind::MeteoraDlmm => 20,
        }
    }

    /// Returns a slippage cushion in basis points applied when emitting `min_amount_out`.
    pub fn safety_slippage_bps(self) -> u16 {
        50
    }
}
