use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use dashmap::{iter::Iter, DashMap};
use tracing::debug;

use crate::watch::types::{PoolQuoteDirection, PoolSnapshot};

#[derive(Clone, Debug)]
pub enum PoolStateEvent {
    Upsert(PoolSnapshot),
    Remove,
}

#[derive(Clone, Debug)]
pub struct PoolState {
    pub pool_id: String,
    pub program_id: String,
    pub slot: u64,
    pub block_hash: Option<String>,
    pub block_time_unix: Option<i64>,
    pub last_updated: Instant,
    pub generation: u64,
    pub base_liquidity: u128,
    pub quote_liquidity: u128,
    pub price: f64,
    pub snapshot: PoolSnapshot,
}

#[derive(Clone, Debug)]
pub struct PoolStateUpdate {
    pub pool_id: String,
    pub program_id: String,
    pub slot: u64,
    pub block_hash: Option<String>,
    pub block_time_unix: Option<i64>,
    pub last_updated: Instant,
    pub generation: u64,
    pub event: PoolStateEvent,
}

#[derive(Default)]
pub struct PoolStore {
    inner: Arc<DashMap<String, PoolState>>,
    generation: Arc<AtomicU64>,
    max_entries: usize,
    subscribers: DashMap<u64, Sender<PoolStateUpdate>>,
    subscriber_seq: AtomicU64,
}

#[derive(Clone)]
pub struct PoolStoreView {
    inner: Arc<DashMap<String, PoolState>>,
    generation: Arc<AtomicU64>,
}

impl PoolStore {
    pub fn new() -> Self {
        Self::with_capacity(usize::MAX)
    }

    pub fn with_capacity(max_entries: usize) -> Self {
        let capacity = if max_entries == 0 {
            usize::MAX
        } else {
            max_entries
        };
        Self {
            inner: Arc::new(DashMap::new()),
            generation: Arc::new(AtomicU64::new(0)),
            max_entries: capacity,
            subscribers: DashMap::new(),
            subscriber_seq: AtomicU64::new(0),
        }
    }

    pub fn view(&self) -> PoolStoreView {
        PoolStoreView {
            inner: self.inner.clone(),
            generation: self.generation.clone(),
        }
    }

    pub fn subscribe(&self, capacity: usize) -> Receiver<PoolStateUpdate> {
        let channel_capacity = capacity.max(1);
        let (tx, rx) = bounded(channel_capacity);
        let id = self.subscriber_seq.fetch_add(1, Ordering::Relaxed);
        self.subscribers.insert(id, tx);
        rx
    }

    pub fn update(&self, snapshot: PoolSnapshot) -> PoolStateUpdate {
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let last_updated = Instant::now();
        let pool_id = snapshot.pool_id.clone();
        let program_id = snapshot.program_id.clone();
        let slot = snapshot.slot;
        let block_hash = snapshot.block_hash.clone();
        let block_time_unix = snapshot.block_time_unix;
        let price = snapshot
            .quoted_price(PoolQuoteDirection::BaseToQuote)
            .or_else(|| {
                if snapshot.allow_reserve_backfill {
                    let spot = snapshot.spot_price();
                    if spot > 0.0 {
                        Some(spot)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap_or(0.0);
        let state = PoolState {
            pool_id: pool_id.clone(),
            program_id: program_id.clone(),
            slot,
            block_hash: block_hash.clone(),
            block_time_unix,
            last_updated,
            generation,
            base_liquidity: snapshot.base_post_amount,
            quote_liquidity: snapshot.quote_post_amount,
            price,
            snapshot: snapshot.clone(),
        };
        self.inner.insert(pool_id.clone(), state);
        self.prune_if_needed();
        let update = PoolStateUpdate {
            pool_id,
            program_id,
            slot,
            block_hash,
            block_time_unix,
            last_updated,
            generation,
            event: PoolStateEvent::Upsert(snapshot),
        };
        self.broadcast_update(&update);
        update
    }

    fn prune_if_needed(&self) {
        if self.max_entries == usize::MAX {
            return;
        }
        while self.inner.len() > self.max_entries {
            let victim = self
                .inner
                .iter()
                .min_by_key(|entry| entry.generation)
                .map(|entry| entry.key().clone());
            match victim {
                Some(key) => {
                    let _ = self.inner.remove(&key);
                }
                None => break,
            }
        }
    }

    fn broadcast_update(&self, update: &PoolStateUpdate) {
        let mut disconnected = Vec::new();
        for subscriber in self.subscribers.iter() {
            let id = *subscriber.key();
            match subscriber.value().try_send(update.clone()) {
                Ok(_) => {}
                Err(TrySendError::Full(_)) => {
                    debug!(
                        pool = %update.pool_id,
                        subscriber_id = id,
                        "watcher update consumer lagging; dropping newest update"
                    );
                }
                Err(TrySendError::Disconnected(_)) => {
                    disconnected.push(id);
                }
            }
        }
        for id in disconnected {
            self.subscribers.remove(&id);
        }
    }

    pub fn latest(&self, pool_id: &str) -> Option<PoolState> {
        self.inner.get(pool_id).map(|entry| entry.value().clone())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

}

impl PoolStoreView {
    pub fn get(&self, pool_id: &str) -> Option<PoolState> {
        self.inner.get(pool_id).map(|entry| entry.value().clone())
    }

    pub fn iter(&self) -> Iter<'_, String, PoolState> {
        self.inner.iter()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn pin_generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watch::types::{PoolSnapshot, PoolUpdateExtensions, SnapshotFreshness};

    fn snapshot_with_freshness(freshness: SnapshotFreshness) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: "test_pool".to_string(),
            program_id: "test_program".to_string(),
            slot: 1,
            block_hash: None,
            block_time_unix: None,
            base_mint: "base".to_string(),
            quote_mint: "quote".to_string(),
            base_post_amount: 10,
            quote_post_amount: 20,
            updated_at: Instant::now(),
            freshness,
            allow_reserve_backfill: true,
            quotes: None,
            extensions: PoolUpdateExtensions::default(),
        }
    }

    #[test]
    fn update_preserves_snapshot_freshness() {
        let store = PoolStore::new();
        let snapshot = snapshot_with_freshness(SnapshotFreshness::RpcRefresh);
        store.update(snapshot.clone());

        let state = store.latest(&snapshot.pool_id).expect("pool state");
        assert!(matches!(
            state.snapshot.freshness,
            SnapshotFreshness::RpcRefresh
        ));
    }
}
