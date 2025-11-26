use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};

use crate::stages::types::{PoolBidirectionalQuotes, PoolSnapshot, PoolUpdate};

mod cp_amm;
mod lb_clmm;
mod lifinity_v2;
mod raydium_v3;
mod whirlpool;

pub use cp_amm::{CpAmmAdapter, CpAmmAdapterConfig, CpAmmState};
pub use lb_clmm::{LbClmmAdapter, LbClmmAdapterConfig, LbClmmState};
pub use lifinity_v2::{LifinityAdapter, LifinityAdapterConfig, LifinityPoolState};
pub use raydium_v3::{RaydiumV3Adapter, RaydiumV3AdapterConfig, RaydiumV3State};
pub use whirlpool::{WhirlpoolAdapter, WhirlpoolAdapterConfig, WhirlpoolState};

/// Trait implemented by protocol-specific adapters that normalize pool updates
/// and emit swap quotes/templates for Stage1 + Stage2 consumers.
pub trait ProtocolAdapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn program_id(&self) -> Pubkey;

    fn supports(&self, program_id: &Pubkey) -> bool {
        self.program_id() == *program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate);

    fn quote_exact_in(&self, _req: QuoteRequest) -> Option<QuoteResponse> {
        None
    }

    fn quote_exact_out(&self, _req: QuoteRequest) -> Option<QuoteResponse> {
        None
    }

    fn build_swap_template(&self, _quote: &QuoteResponse) -> Option<SwapTemplate> {
        None
    }

    fn bidirectional_quotes(&self, _snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapDirection {
    ExactIn,
    ExactOut,
}

#[derive(Debug, Clone)]
pub struct QuoteRequest {
    pub pool_id: Pubkey,
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub amount: u64,
    pub slot: u64,
    pub direction: SwapDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteStatus {
    Ok,
    StaleState,
    InsufficientLiquidity,
    MissingState,
}

#[derive(Debug, Clone)]
pub struct QuoteResponse {
    pub request: QuoteRequest,
    pub amount_in: u64,
    pub amount_out: u64,
    pub edge_bps: f64,
    pub sqrt_price_limit: Option<u128>,
    pub status: QuoteStatus,
}

#[derive(Debug, Clone)]
pub struct SwapTemplate {
    pub program_id: Pubkey,
    pub discriminator: [u8; 8],
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
    pub context: TemplateContext,
}

#[derive(Debug, Clone, Default)]
pub struct TemplateContext {
    pub payer: Option<Pubkey>,
    pub requires_memo: bool,
}

pub(crate) fn quote_from_price(
    req: &QuoteRequest,
    mint_a: &Pubkey,
    mint_b: &Pubkey,
    price: f64,
) -> Option<u64> {
    if price <= 0.0 {
        return None;
    }

    if req.input_mint == *mint_a && req.output_mint == *mint_b {
        Some(mul_div_price(req.amount, price))
    } else if req.input_mint == *mint_b && req.output_mint == *mint_a {
        Some(mul_div_price(req.amount, 1.0 / price))
    } else {
        None
    }
}

pub(crate) fn build_quote_response(req: QuoteRequest, amount_out: u64) -> QuoteResponse {
    QuoteResponse {
        amount_in: req.amount,
        amount_out,
        edge_bps: 0.0,
        sqrt_price_limit: None,
        status: QuoteStatus::Ok,
        request: req,
    }
}

fn mul_div_price(amount: u64, price: f64) -> u64 {
    let value = (amount as f64) * price;
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    value.min(u64::MAX as f64) as u64
}
