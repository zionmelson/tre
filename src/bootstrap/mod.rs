use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;

pub mod cp_amm;
pub mod lb_clmm;
pub mod raydium;
pub mod whirlpool;

pub use cp_amm::CpAmmStaticSnapshot;
pub use lb_clmm::LbClmmStaticSnapshot;
pub use raydium::{
    HydratedAmmConfig, HydratedPoolMeta, HydratedTick, HydratedTickArray,
    HydratedTickArraySelection, InitializedTick, MintAccountMeta, RaydiumStaticSnapshot,
    TransferFeeConfigSummary,
};
pub use whirlpool::WhirlpoolStaticSnapshot;

#[derive(Clone, Default)]
pub struct StaticPoolRegistry {
    entries: Arc<DashMap<Pubkey, Arc<StaticPoolEntry>>>,
}

#[derive(Debug)]
pub struct StaticPoolEntry {
    pub program_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub recorded_at: Instant,
    pub data: StaticPoolData,
}

#[derive(Debug)]
pub enum StaticPoolData {
    Raydium(Arc<RaydiumStaticSnapshot>),
    CpAmm(Arc<CpAmmStaticSnapshot>),
    LbClmm(Arc<LbClmmStaticSnapshot>),
    Whirlpool(Arc<WhirlpoolStaticSnapshot>),
}

impl StaticPoolRegistry {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn record_raydium_snapshot(&self, snapshot: RaydiumStaticSnapshot) {
        let pool_id = snapshot.pool_id;
        let entry = StaticPoolEntry {
            program_id: snapshot.program_id,
            base_mint: snapshot.base_mint,
            quote_mint: snapshot.quote_mint,
            recorded_at: snapshot.recorded_at,
            data: StaticPoolData::Raydium(Arc::new(snapshot)),
        };
        self.entries.insert(pool_id, Arc::new(entry));
    }

    pub fn raydium_snapshot(&self, pool: &Pubkey) -> Option<Arc<RaydiumStaticSnapshot>> {
        self.entries.get(pool).and_then(|entry| match &entry.data {
            StaticPoolData::Raydium(snapshot) => Some(snapshot.clone()),
            _ => None,
        })
    }

    pub fn record_cp_amm_snapshot(&self, snapshot: CpAmmStaticSnapshot) {
        let pool_id = snapshot.pool_id;
        let entry = StaticPoolEntry {
            program_id: snapshot.program_id,
            base_mint: snapshot.base_mint,
            quote_mint: snapshot.quote_mint,
            recorded_at: snapshot.recorded_at,
            data: StaticPoolData::CpAmm(Arc::new(snapshot)),
        };
        self.entries.insert(pool_id, Arc::new(entry));
    }

    pub fn cp_amm_snapshot(&self, pool: &Pubkey) -> Option<Arc<CpAmmStaticSnapshot>> {
        self.entries.get(pool).and_then(|entry| match &entry.data {
            StaticPoolData::CpAmm(snapshot) => Some(snapshot.clone()),
            _ => None,
        })
    }

    pub fn record_lb_clmm_snapshot(&self, snapshot: LbClmmStaticSnapshot) {
        let pool_id = snapshot.lb_pair;
        let entry = StaticPoolEntry {
            program_id: snapshot.program_id,
            base_mint: snapshot.token_x_mint,
            quote_mint: snapshot.token_y_mint,
            recorded_at: snapshot.recorded_at,
            data: StaticPoolData::LbClmm(Arc::new(snapshot)),
        };
        self.entries.insert(pool_id, Arc::new(entry));
    }

    pub fn lb_clmm_snapshot(&self, pool: &Pubkey) -> Option<Arc<LbClmmStaticSnapshot>> {
        self.entries.get(pool).and_then(|entry| match &entry.data {
            StaticPoolData::LbClmm(snapshot) => Some(snapshot.clone()),
            _ => None,
        })
    }

    pub fn record_whirlpool_snapshot(&self, snapshot: WhirlpoolStaticSnapshot) {
        let pool_id = snapshot.whirlpool;
        let entry = StaticPoolEntry {
            program_id: snapshot.program_id,
            base_mint: snapshot.token_mint_a,
            quote_mint: snapshot.token_mint_b,
            recorded_at: snapshot.recorded_at,
            data: StaticPoolData::Whirlpool(Arc::new(snapshot)),
        };
        self.entries.insert(pool_id, Arc::new(entry));
    }

    pub fn whirlpool_snapshot(&self, pool: &Pubkey) -> Option<Arc<WhirlpoolStaticSnapshot>> {
        self.entries.get(pool).and_then(|entry| match &entry.data {
            StaticPoolData::Whirlpool(snapshot) => Some(snapshot.clone()),
            _ => None,
        })
    }
}
