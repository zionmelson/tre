use std::time::Instant;

use solana_sdk::pubkey::Pubkey;

use crate::builder::{BundleBuildContext, FlashLoanRoutePlan};
use crate::watch::types::{compute_edge_bps, PoolQuoteDirection, PoolSnapshot, TradeUpdate};
use crate::template::TemplateContext;

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub seq: u64,
    pub trade: TradeUpdate,
    pub pool_snapshot: Option<PoolSnapshot>,
    pub snapshot_generation: Option<u64>,
    pub estimated_edge_bps: f64,
    pub created_at: Instant,
    pub route_plan: Option<RoutePlan>,
}

impl Opportunity {
    pub fn from_trade(
        seq: u64,
        trade: TradeUpdate,
        snapshot: Option<PoolSnapshot>,
        snapshot_generation: Option<u64>,
        override_edge_bps: Option<f64>,
    ) -> Self {
        let estimated_edge_bps = override_edge_bps
            .or_else(|| {
                snapshot.as_ref().and_then(|snap| {
                    snap.price_for_mints(&trade.sell_mint, &trade.buy_mint)
                        .map(|pool_price| compute_edge_bps(trade.price(), pool_price))
                })
            })
            .unwrap_or_default();

        Self {
            seq,
            trade,
            pool_snapshot: snapshot,
            snapshot_generation,
            estimated_edge_bps,
            created_at: Instant::now(),
            route_plan: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BundleCandidate {
    pub seq: u64,
    pub opportunity_seq: u64,
    pub pool_id: String,
    pub trade_signature: String,
    pub estimated_edge_bps: f64,
    pub created_at: Instant,
    pub route_plan: Option<RoutePlan>,
    pub instructions: Vec<PlannedInstruction>,
    pub bundle_context: Option<BundleBuildContext>,
    pub tip_lamports: Option<u64>,
    pub tip_account: Option<Pubkey>,
    pub flash_loan: Option<FlashLoanSummary>,
    pub flash_loan_reservation: Option<FlashLoanRoutePlan>,
    pub stage2_attempts: u8,
    pub stage3_attempts: u8,
    pub stage3_ready_at: Option<Instant>,
    pub pool_snapshot: Option<PoolSnapshot>,
    pub snapshot_generation: Option<u64>,
}

impl BundleCandidate {
    pub fn from_opportunity(opportunity: &Opportunity) -> Self {
        Self {
            seq: opportunity.seq,
            opportunity_seq: opportunity.seq,
            pool_id: opportunity.trade.pool_id.clone(),
            trade_signature: opportunity.trade.signature.clone(),
            estimated_edge_bps: opportunity.estimated_edge_bps,
            created_at: opportunity.created_at,
            route_plan: opportunity.route_plan.clone(),
            instructions: Vec::new(),
            bundle_context: None,
            tip_lamports: None,
            tip_account: None,
            flash_loan: None,
            flash_loan_reservation: None,
            stage2_attempts: 0,
            stage3_attempts: 0,
            stage3_ready_at: None,
            pool_snapshot: opportunity.pool_snapshot.clone(),
            snapshot_generation: opportunity.snapshot_generation,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoutePlan {
    pub start_mint: String,
    pub end_mint: String,
    pub legs: Vec<RouteLegPlan>,
    pub estimated_final_output_amount: Option<u64>,
    pub flash_loan_permitted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RouteLegPlan {
    pub pool_id: String,
    pub program_id: String,
    pub input_mint: String,
    pub output_mint: String,
    pub input_amount: u64,
    pub available_input_liquidity: u64,
    pub estimated_output_amount: u64,
    pub amount_specified_is_input: bool,
    pub effective_price: f64,
    pub fee_bps: u64,
    pub estimated_slippage_bps: f64,
    pub template_fee_bps: u16,
    pub direction: PoolQuoteDirection,
    pub sqrt_price_limit: Option<u128>,
    pub min_output_amount: Option<u64>,
    pub limit_price: Option<i64>,
    pub price_impact_limit_bps: Option<u16>,
    pub context: TemplateContext,
}

#[derive(Debug, Clone)]
pub struct PlannedInstruction {
    pub pool_id: String,
    pub program_id: String,
    pub input_mint: String,
    pub output_mint: String,
    pub input_amount: u64,
    pub amount_specified: u64,
    pub amount_specified_is_input: bool,
    pub estimated_output_amount: u64,
    pub fee_bps: u64,
    pub template_fee_bps: u16,
    pub direction: PoolQuoteDirection,
    pub template_hash: [u8; 32],
    pub lookup_table: Option<PlannedLookupTable>,
    pub sqrt_price_limit: Option<u128>,
    pub min_output_amount: Option<u64>,
    pub limit_price: Option<i64>,
    pub price_impact_limit_bps: Option<u16>,
    pub context: TemplateContext,
}

#[derive(Debug, Clone)]
pub struct FlashLoanSummary {
    pub plan_id: u64,
    pub token: String,
    pub liquidity_mint: String,
    pub borrow_amount: u64,
    pub repay_amount: u64,
    pub fee_amount: u64,
    pub program_id: String,
    pub lending_market: String,
    pub reserve: String,
}

#[derive(Debug, Clone)]
pub struct PlannedLookupTable {
    pub handle_id: u32,
    pub table_address: String,
    pub authority: String,
    pub creation_slot: u64,
    pub last_extended_slot: u64,
    pub accounts: Vec<String>,
}
