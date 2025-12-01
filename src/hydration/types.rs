use std::time::Instant;

use crate::proto::solana_corecast::{DexTradeStreamMessage, PoolLiquidityChangeStreamMessage};
use crate::proto::solana_messages::{ParsedIdlInstruction, PoolLiquidityChangeEvent};
use crate::template::TemplateContext;

mod protocols;

pub use protocols::{
    CpAmmPoolExtras, LbClmmBinArrayExtras, LbClmmBinLiquidityExtras, LbClmmBitmapExtras,
    LbClmmPoolExtras, LifinityPoolExtras, RaydiumV3PoolExtras, WhirlpoolPoolExtras,
    CP_AMM_INPUT_UTILIZATION_BPS, CP_AMM_PROGRAM_ID, LB_CLMM_INPUT_UTILIZATION_BPS,
    LB_CLMM_PROGRAM_ID, LIFINITY_V2_INPUT_UTILIZATION_BPS, LIFINITY_V2_PROGRAM_ID,
    RAYDIUM_V3_INPUT_UTILIZATION_BPS, RAYDIUM_V3_PROGRAM_ID, WHIRLPOOL_INPUT_UTILIZATION_BPS,
    WHIRLPOOL_PROGRAM_ID,
};

use protocols::{
    parse_cp_amm_extras, parse_lb_clmm_extras, parse_lifinity_extras, parse_raydium_v3_extras,
    parse_whirlpool_extras,
};

#[derive(Debug, Clone)]
pub struct PoolUpdate {
    pub seq: u64,
    pub slot: u64,
    pub program_id: String,
    pub pool_id: String,
    pub block_hash: Option<String>,
    pub block_time_unix: Option<i64>,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_post_amount: u128,
    pub quote_post_amount: u128,
    pub base_change: i128,
    pub quote_change: i128,
    pub extensions: PoolUpdateExtensions,
}

impl PoolUpdate {
    pub fn from_corecast(seq: u64, message: &PoolLiquidityChangeStreamMessage) -> Option<Self> {
        let block = message.block.as_ref()?;
        let event = message.pool_event.as_ref()?;
        let dex = event.dex.as_ref()?;
        let market = event.market.as_ref()?;
        let base_currency = market.base_currency.as_ref()?;
        let quote_currency = market.quote_currency.as_ref()?;
        let base_side = event.base_currency.as_ref()?;
        let quote_side = event.quote_currency.as_ref()?;

        let program_id = encode_pubkey(&dex.program_address);
        let pool_id = encode_pubkey(&market.market_address);
        if program_id.is_empty() || pool_id.is_empty() {
            return None;
        }

        // CoreCast BlockAttributes currently expose only the slot number, so block metadata
        // such as hash/timestamp is unavailable for now.
        let block_hash = None;
        let block_time_unix = None;

        Some(Self {
            seq,
            slot: block.slot,
            program_id,
            pool_id,
            block_hash,
            block_time_unix,
            base_mint: encode_pubkey(&base_currency.mint_address),
            quote_mint: encode_pubkey(&quote_currency.mint_address),
            base_post_amount: base_side.post_amount as u128,
            quote_post_amount: quote_side.post_amount as u128,
            base_change: base_side.change_amount as i128,
            quote_change: quote_side.change_amount as i128,
            extensions: PoolUpdateExtensions::from_pool_event(event),
        })
    }

    pub fn into_snapshot(self) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: self.pool_id,
            program_id: self.program_id,
            slot: self.slot,
            block_hash: self.block_hash,
            block_time_unix: self.block_time_unix,
            base_mint: self.base_mint,
            quote_mint: self.quote_mint,
            base_post_amount: self.base_post_amount,
            quote_post_amount: self.quote_post_amount,
            updated_at: Instant::now(),
            freshness: SnapshotFreshness::LiveCorecast,
            quotes: None,
            extensions: self.extensions,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PoolUpdateExtensions {
    pub instruction: Option<ParsedIdlInstruction>,
    pub raydium_v3: Option<RaydiumV3PoolExtras>,
    pub cp_amm: Option<CpAmmPoolExtras>,
    pub lb_clmm: Option<LbClmmPoolExtras>,
    pub lifinity_v2: Option<LifinityPoolExtras>,
    pub whirlpool: Option<WhirlpoolPoolExtras>,
}

impl PoolUpdateExtensions {
    fn from_pool_event(event: &PoolLiquidityChangeEvent) -> Self {
        let raydium_v3 = parse_raydium_v3_extras(event);
        let cp_amm = parse_cp_amm_extras(event);
        let lb_clmm = parse_lb_clmm_extras(event);
        let lifinity_v2 = parse_lifinity_extras(event);
        let whirlpool = parse_whirlpool_extras(event);
        Self {
            instruction: event.instruction.clone(),
            raydium_v3,
            cp_amm,
            lb_clmm,
            lifinity_v2,
            whirlpool,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolQuoteDirection {
    BaseToQuote,
    QuoteToBase,
}

#[derive(Debug, Clone)]
pub struct PoolQuoteSide {
    pub input_mint: String,
    pub output_mint: String,
    pub notional: u64,
    pub expected_output: u64,
    pub fee_bps: u64,
    pub template_fee_bps: u16,
    pub effective_price: f64,
    pub estimated_slippage_bps: f64,
    pub sqrt_price_limit: Option<u128>,
    pub min_output_amount: Option<u64>,
    pub limit_price: Option<i64>,
    pub price_impact_limit_bps: Option<u16>,
    pub context: TemplateContext,
}

impl PoolQuoteSide {
    pub fn price(&self) -> f64 {
        self.effective_price
    }

    pub fn slippage_bps(&self) -> f64 {
        self.estimated_slippage_bps
    }
}

#[derive(Debug, Clone)]
pub struct PoolBidirectionalQuotes {
    pub base_to_quote: PoolQuoteSide,
    pub quote_to_base: PoolQuoteSide,
}

impl PoolBidirectionalQuotes {
    pub fn side(&self, direction: PoolQuoteDirection) -> &PoolQuoteSide {
        match direction {
            PoolQuoteDirection::BaseToQuote => &self.base_to_quote,
            PoolQuoteDirection::QuoteToBase => &self.quote_to_base,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotFreshness {
    LiveCorecast,
    RpcRefresh,
    Stale,
}

impl SnapshotFreshness {
    pub fn is_stale(self) -> bool {
        matches!(self, SnapshotFreshness::Stale)
    }
}

#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub pool_id: String,
    pub program_id: String,
    pub slot: u64,
    pub block_hash: Option<String>,
    pub block_time_unix: Option<i64>,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_post_amount: u128,
    pub quote_post_amount: u128,
    pub updated_at: Instant,
    pub freshness: SnapshotFreshness,
    pub quotes: Option<PoolBidirectionalQuotes>,
    pub extensions: PoolUpdateExtensions,
}

impl PoolSnapshot {
    pub fn spot_price(&self) -> f64 {
        if self.base_post_amount == 0 {
            return 0.0;
        }
        self.quote_post_amount as f64 / self.base_post_amount as f64
    }

    pub fn quotes(&self) -> Option<&PoolBidirectionalQuotes> {
        self.quotes.as_ref()
    }

    pub fn quote(&self, direction: PoolQuoteDirection) -> Option<&PoolQuoteSide> {
        self.quotes().map(|bundle| bundle.side(direction))
    }

    pub fn base_to_quote_quote(&self) -> Option<&PoolQuoteSide> {
        self.quotes().map(|bundle| &bundle.base_to_quote)
    }

    pub fn quote_to_base_quote(&self) -> Option<&PoolQuoteSide> {
        self.quotes().map(|bundle| &bundle.quote_to_base)
    }
}

#[derive(Debug, Clone)]
pub struct TradeUpdate {
    pub seq: u64,
    pub slot: u64,
    pub program_id: String,
    pub pool_id: String,
    pub signature: String,
    pub buy_mint: String,
    pub sell_mint: String,
    pub buy_amount: u64,
    pub sell_amount: u64,
}

impl TradeUpdate {
    pub fn from_corecast(seq: u64, message: &DexTradeStreamMessage) -> Option<Self> {
        let block = message.block.as_ref()?;
        let trade = message.trade.as_ref()?;
        let dex = trade.dex.as_ref()?;
        let market = trade.market.as_ref()?;
        let buy_side = trade.buy.as_ref()?;
        let sell_side = trade.sell.as_ref()?;
        let buy_currency = buy_side.currency.as_ref()?;
        let sell_currency = sell_side.currency.as_ref()?;
        let tx = message.transaction.as_ref()?;

        let program_id = encode_pubkey(&dex.program_address);
        let pool_id = encode_pubkey(&market.market_address);
        let signature = encode_pubkey(&tx.signature);
        if program_id.is_empty() || pool_id.is_empty() || signature.is_empty() {
            return None;
        }

        Some(Self {
            seq,
            slot: block.slot,
            program_id,
            pool_id,
            signature,
            buy_mint: encode_pubkey(&buy_currency.mint_address),
            sell_mint: encode_pubkey(&sell_currency.mint_address),
            buy_amount: buy_side.amount,
            sell_amount: sell_side.amount,
        })
    }

    pub fn price(&self) -> f64 {
        if self.sell_amount == 0 {
            return 0.0;
        }
        self.buy_amount as f64 / self.sell_amount as f64
    }
}

fn encode_pubkey(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        String::new()
    } else {
        bs58::encode(bytes).into_string()
    }
}

pub fn compute_edge_bps(trade_price: f64, pool_price: f64) -> f64 {
    if trade_price == 0.0 || pool_price == 0.0 {
        return 0.0;
    }
    ((pool_price - trade_price) / pool_price) * 10_000.0
}
