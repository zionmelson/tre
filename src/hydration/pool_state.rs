use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use dashmap::{iter::Iter, DashMap};
use tracing::debug;

use crate::config::QueueDropPolicy;
use crate::hydration::types::{PoolQuoteDirection, PoolSnapshot, SnapshotFreshness};
use crate::telemetry;

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
    pub ready: bool,
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
    ready_target: usize,
    ready_state: Mutex<bool>,
    ready_notify: Condvar,
    drop_policy: QueueDropPolicy,
    ready_count: AtomicUsize,
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
        Self::with_limits(max_entries, 0)
    }

    pub fn with_limits(max_entries: usize, ready_target: usize) -> Self {
        Self::with_limits_and_policy(max_entries, ready_target, QueueDropPolicy::default())
    }

    pub fn with_limits_and_policy(
        max_entries: usize,
        ready_target: usize,
        drop_policy: QueueDropPolicy,
    ) -> Self {
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
            ready_target,
            ready_state: Mutex::new(ready_target == 0),
            ready_notify: Condvar::new(),
            drop_policy,
            ready_count: AtomicUsize::new(0),
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

    pub fn update(&self, snapshot: PoolSnapshot, ready: bool) -> PoolStateUpdate {
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
        let effective_ready = ready;
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
            ready: effective_ready,
        };
        telemetry::record_hydration_pool_snapshot(
            &program_id,
            &pool_id,
            &snapshot.base_mint,
            &snapshot.quote_mint,
            slot,
            price,
            snapshot.base_post_amount,
            snapshot.quote_post_amount,
        );
        if let Some(previous) = self.inner.insert(pool_id.clone(), state) {
            self.update_ready_count(previous.ready, effective_ready);
        } else if effective_ready {
            self.ready_count.fetch_add(1, Ordering::Relaxed);
        }
        self.maybe_notify_ready();
        self.prune_if_needed();
        self.emit_pool_counts();
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

    pub fn wait_until_ready(&self, timeout: Duration) -> bool {
        if self.ready_target == 0 {
            return true;
        }
        let ready = self.ready_state.lock().expect("ready mutex poisoned");
        if *ready {
            return true;
        }
        let (guard, status) = self
            .ready_notify
            .wait_timeout_while(ready, timeout, |ready| !*ready)
            .expect("ready condvar poisoned");
        if *guard {
            true
        } else {
            !status.timed_out()
        }
    }

    pub fn sweep_stale(&self, ttl: Duration) -> usize {
        if ttl.is_zero() {
            return 0;
        }
        let mut stale_ids = Vec::new();
        for entry in self.inner.iter() {
            if entry.value().last_updated.elapsed() >= ttl {
                stale_ids.push(entry.key().clone());
            }
        }
        if stale_ids.is_empty() {
            return 0;
        }
        let mut removed = 0usize;
        for pool_id in stale_ids {
            if let Some((_, state)) = self.inner.remove(&pool_id) {
                if state.ready {
                    self.ready_count.fetch_sub(1, Ordering::Relaxed);
                }
                removed += 1;
                let update = PoolStateUpdate {
                    pool_id: state.pool_id.clone(),
                    program_id: state.program_id.clone(),
                    slot: state.slot,
                    block_hash: state.block_hash.clone(),
                    block_time_unix: state.block_time_unix,
                    last_updated: Instant::now(),
                    generation: self.generation.fetch_add(1, Ordering::Relaxed) + 1,
                    event: PoolStateEvent::Remove,
                };
                self.broadcast_update(&update);
            }
        }
        if removed > 0 {
            self.emit_pool_counts();
        }
        removed
    }

    pub fn collect_stale(&self, ttl: Duration) -> Vec<PoolState> {
        if ttl.is_zero() {
            return Vec::new();
        }
        self.inner
            .iter()
            .filter_map(|entry| {
                let value = entry.value();
                if value.last_updated.elapsed() >= ttl && !value.snapshot.freshness.is_stale() {
                    Some(value.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn mark_snapshot_stale(&self, pool_id: &str) -> bool {
        let update = {
            let mut entry = match self.inner.get_mut(pool_id) {
                Some(entry) => entry,
                None => return false,
            };
            if entry.snapshot.freshness.is_stale() {
                return false;
            }
            entry.snapshot.freshness = SnapshotFreshness::Stale;
            let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
            entry.generation = generation;
            if entry.ready {
                entry.ready = false;
                self.ready_count.fetch_sub(1, Ordering::Relaxed);
            }
            PoolStateUpdate {
                pool_id: entry.pool_id.clone(),
                program_id: entry.program_id.clone(),
                slot: entry.slot,
                block_hash: entry.block_hash.clone(),
                block_time_unix: entry.block_time_unix,
                last_updated: entry.last_updated,
                generation,
                event: PoolStateEvent::Upsert(entry.snapshot.clone()),
            }
        };
        self.broadcast_update(&update);
        self.emit_pool_counts();
        true
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
                    if let Some((_, state)) = self.inner.remove(&key) {
                        if state.ready {
                            self.ready_count.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
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
                    telemetry::record_hydration_queue_drop();
                    match self.drop_policy {
                        QueueDropPolicy::DropNewest => {
                            debug!(pool = %update.pool_id, subscriber_id = id, "watcher update consumer lagging; dropping newest update");
                        }
                        QueueDropPolicy::DropSubscriber => {
                            debug!(pool = %update.pool_id, subscriber_id = id, "watcher update consumer lagging; disconnecting subscriber");
                            disconnected.push(id);
                        }
                    }
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

    pub fn ready_len(&self) -> usize {
        self.ready_count.load(Ordering::Relaxed)
    }

    fn maybe_notify_ready(&self) {
        if self.ready_target == 0 {
            return;
        }
        if self.ready_len() < self.ready_target {
            return;
        }
        let mut ready = self.ready_state.lock().expect("ready mutex poisoned");
        if *ready {
            return;
        }
        *ready = true;
        self.ready_notify.notify_all();
    }

    fn update_ready_count(&self, previous: bool, current: bool) {
        match (previous, current) {
            (false, true) => {
                self.ready_count.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.ready_count.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn emit_pool_counts(&self) {
        telemetry::record_hydration_pool_counts(self.ready_len(), self.len());
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
    use crate::hydration::types::{HydrationStatus, PoolUpdateExtensions};

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
            hydration: HydrationStatus::new(1),
            extensions: PoolUpdateExtensions::default(),
        }
    }

    #[test]
    fn update_preserves_snapshot_freshness() {
        let store = PoolStore::new();
        let snapshot = snapshot_with_freshness(SnapshotFreshness::RpcRefresh);
        store.update(snapshot.clone(), false);

        let state = store.latest(&snapshot.pool_id).expect("pool state");
        assert!(matches!(
            state.snapshot.freshness,
            SnapshotFreshness::RpcRefresh
        ));
    }

    #[test]
    fn stale_mark_prevents_repeated_collection() {
        let store = PoolStore::new();
        let snapshot = snapshot_with_freshness(SnapshotFreshness::LiveCorecast);
        store.update(snapshot.clone(), false);

        std::thread::sleep(Duration::from_millis(2));

        let stale = store.collect_stale(Duration::from_millis(1));
        assert_eq!(stale.len(), 1);

        assert!(store.mark_snapshot_stale(&snapshot.pool_id));

        let remaining = store.collect_stale(Duration::from_millis(1));
        assert!(remaining.is_empty());
    }

    #[test]
    fn ready_count_tracks_transitions() {
        let store = PoolStore::with_limits(usize::MAX, 0);
        let mut snapshot = snapshot_with_freshness(SnapshotFreshness::LiveCorecast);
        snapshot.pool_id = "ready_pool".to_string();

        store.update(snapshot.clone(), true);
        assert_eq!(store.ready_len(), 1);

        store.update(snapshot.clone(), false);
        assert_eq!(store.ready_len(), 0);

        store.update(snapshot, true);
        assert_eq!(store.ready_len(), 1);

        assert!(store.mark_snapshot_stale("ready_pool"));
        assert_eq!(store.ready_len(), 0);
    }
}
