use solana_sdk::pubkey::Pubkey;

pub use crate::watch::types::QuoteStatus;
use crate::watch::types::{PoolBidirectionalQuotes, PoolSnapshot, PoolUpdate};

mod cp_amm;
mod lb_clmm;
mod raydium_v3;
mod whirlpool;

pub use crate::template::TemplateContext;
pub use cp_amm::{CpAmmAdapter, CpAmmAdapterConfig, CpAmmState};
pub use lb_clmm::{LbClmmAdapter, LbClmmAdapterConfig, LbClmmState};
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

    fn prefer_exact_out_quotes(&self) -> bool {
        false
    }

    fn ingest_pool_update(&self, update: &PoolUpdate);

    fn quote_exact_in(&self, _req: QuoteRequest) -> Option<QuoteResponse> {
        None
    }

    fn quote_exact_out(&self, _req: QuoteRequest) -> Option<QuoteResponse> {
        None
    }

    fn bidirectional_quotes(&self, _snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        None
    }

    fn enrich_snapshot(&self, _snapshot: &mut PoolSnapshot) {}

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
    pub block_time: Option<i64>,
    pub direction: SwapDirection,
}

impl QuoteRequest {
    pub fn block_time_u64(&self) -> Option<u64> {
        self.block_time.and_then(|ts| u64::try_from(ts).ok())
    }
}

#[derive(Debug, Clone)]
pub struct QuoteResponse {
    pub request: QuoteRequest,
    pub amount_in: u64,
    pub amount_out: u64,
    pub edge_bps: f64,
    pub sqrt_price_limit: Option<u128>,
    pub min_output_amount: Option<u64>,
    pub status: QuoteStatus,
    pub context: TemplateContext,
}

pub(crate) fn build_quote_response(req: QuoteRequest, amount_out: u64) -> QuoteResponse {
    QuoteResponse {
        amount_in: req.amount,
        amount_out,
        edge_bps: 0.0,
        sqrt_price_limit: None,
        min_output_amount: None,
        status: QuoteStatus::Ok,
        request: req,
        context: TemplateContext::default(),
    }
}
