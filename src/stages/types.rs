use std::time::Instant;

#[derive(Debug, Clone)]
pub struct PoolUpdate {
    pub seq: u64,
    pub slot: u64,
    pub program_id: String,
    pub pool_id: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_post_amount: u128,
    pub quote_post_amount: u128,
    pub base_change: i128,
    pub quote_change: i128,
}

impl PoolUpdate {
    pub fn synthetic(seq: u64, pool_id: String) -> Self {
        let base_post = 1_000_000_000u128 + (seq as u128 * 17);
        let quote_post = 2_000_000_000u128 + (seq as u128 * 23);
        Self {
            seq,
            slot: 10_000 + seq,
            program_id: "dex_program".into(),
            pool_id,
            base_mint: "So11111111111111111111111111111111111111112".into(),
            quote_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            base_post_amount: base_post,
            quote_post_amount: quote_post,
            base_change: 1_000 + (seq as i128 % 1000),
            quote_change: -2_000 + (seq as i128 % 1000),
        }
    }

    pub fn into_snapshot(self) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: self.pool_id,
            program_id: self.program_id,
            slot: self.slot,
            base_mint: self.base_mint,
            quote_mint: self.quote_mint,
            base_post_amount: self.base_post_amount,
            quote_post_amount: self.quote_post_amount,
            updated_at: Instant::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub pool_id: String,
    pub program_id: String,
    pub slot: u64,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_post_amount: u128,
    pub quote_post_amount: u128,
    pub updated_at: Instant,
}

impl PoolSnapshot {
    pub fn spot_price(&self) -> f64 {
        if self.base_post_amount == 0 {
            return 0.0;
        }
        self.quote_post_amount as f64 / self.base_post_amount as f64
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
    pub fn synthetic(seq: u64, pool_id: String) -> Self {
        Self {
            seq,
            slot: 20_000 + seq,
            program_id: "dex_program".into(),
            pool_id,
            signature: format!("fake_sig_{}", seq),
            buy_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            sell_mint: "So11111111111111111111111111111111111111112".into(),
            buy_amount: 100_000 + (seq as u64 * 10),
            sell_amount: 50_000 + (seq as u64 * 5),
        }
    }

    pub fn price(&self) -> f64 {
        if self.sell_amount == 0 {
            return 0.0;
        }
        self.buy_amount as f64 / self.sell_amount as f64
    }
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub seq: u64,
    pub trade: TradeUpdate,
    pub pool_snapshot: Option<PoolSnapshot>,
    pub estimated_edge_bps: f64,
    pub created_at: Instant,
}

impl Opportunity {
    pub fn from_trade(seq: u64, trade: TradeUpdate, snapshot: Option<PoolSnapshot>) -> Self {
        let estimated_edge_bps = snapshot
            .as_ref()
            .map(|snap| compute_edge_bps(trade.price(), snap.spot_price()))
            .unwrap_or_default();

        Self {
            seq,
            trade,
            pool_snapshot: snapshot,
            estimated_edge_bps,
            created_at: Instant::now(),
        }
    }
}

fn compute_edge_bps(trade_price: f64, pool_price: f64) -> f64 {
    if trade_price == 0.0 || pool_price == 0.0 {
        return 0.0;
    }
    ((pool_price - trade_price) / pool_price) * 10_000.0
}

#[derive(Debug, Clone)]
pub struct BundleCandidate {
    pub seq: u64,
    pub opportunity_seq: u64,
    pub pool_id: String,
    pub trade_signature: String,
    pub estimated_edge_bps: f64,
    pub created_at: Instant,
}

impl BundleCandidate {
    pub fn from_opportunity(opportunity: &Opportunity) -> Self {
        Self {
            seq: opportunity.seq,
            opportunity_seq: opportunity.seq,
            pool_id: opportunity.trade.pool_id.clone(),
            trade_signature: opportunity.trade.signature.clone(),
            estimated_edge_bps: opportunity.estimated_edge_bps,
            created_at: Instant::now(),
        }
    }
}
