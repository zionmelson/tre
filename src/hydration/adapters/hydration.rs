use std::{
    fmt::Debug,
    hash::Hash,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use dashmap::{DashMap, DashSet};
use tracing::{debug, warn};

use crate::telemetry::{self, HydrationJobStatus};

const HYDRATION_FAILURE_BACKOFF_BASE_MS: u64 = 100;
const HYDRATION_FAILURE_BACKOFF_MAX_MS: u64 = 5_000;

#[derive(Clone)]
pub(crate) struct HydratedValue<V> {
    pub value: V,
    pub slot: Option<u64>,
    pub fetched_at: Instant,
}

impl<V> HydratedValue<V> {
    pub fn new(value: V, slot: Option<u64>) -> Self {
        Self {
            value,
            slot,
            fetched_at: Instant::now(),
        }
    }

    pub fn with_timestamp(value: V, slot: Option<u64>, fetched_at: Instant) -> Self {
        Self {
            value,
            slot,
            fetched_at,
        }
    }

    pub fn is_fresh(&self, min_slot: Option<u64>, max_age: Option<Duration>) -> bool {
        if let Some(target) = min_slot {
            match self.slot {
                Some(slot) if slot >= target => {}
                _ => return false,
            }
        }
        if let Some(ttl) = max_age {
            if self.fetched_at.elapsed() > ttl {
                return false;
            }
        }
        true
    }
}

/// Lightweight worker queue that hydrates on-chain accounts off critical paths.
/// The hydrator keeps an in-memory cache and deduplicates in-flight requests so
/// callers can simply request hydration and continue without blocking.
pub(crate) struct AsyncHydrator<K, V>
where
    K: Copy + Eq + Hash + Send + Sync + Debug + 'static,
    V: Clone + Send + Sync + 'static,
{
    cache: Arc<DashMap<K, HydratedValue<V>>>,
    pending: Arc<DashSet<K>>,
    sender: Sender<K>,
    name: &'static str,
}

impl<K, V> AsyncHydrator<K, V>
where
    K: Copy + Eq + Hash + Send + Sync + Debug + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) fn new<F>(name: &'static str, worker_count: usize, fetcher: F) -> Self
    where
        F: Fn(K) -> Option<HydratedValue<V>> + Send + Sync + 'static,
    {
        let cache = Arc::new(DashMap::new());
        Self::with_cache(name, worker_count, fetcher, cache)
    }

    pub(crate) fn with_cache<F>(
        name: &'static str,
        worker_count: usize,
        fetcher: F,
        cache: Arc<DashMap<K, HydratedValue<V>>>,
    ) -> Self
    where
        F: Fn(K) -> Option<HydratedValue<V>> + Send + Sync + 'static,
    {
        let pending = Arc::new(DashSet::new());
        let (sender, receiver) = bounded(worker_count.max(1) * 512);
        let fetcher = Arc::new(fetcher);

        for index in 0..worker_count.max(1) {
            Self::spawn_worker(
                name,
                index,
                receiver.clone(),
                cache.clone(),
                pending.clone(),
                fetcher.clone(),
            );
        }

        Self {
            cache,
            pending,
            sender,
            name,
        }
    }

    pub(crate) fn get_or_schedule(&self, key: K) -> Option<V> {
        self.get_or_schedule_fresh(key, None, None)
    }

    pub(crate) fn get_or_schedule_fresh(
        &self,
        key: K,
        min_slot: Option<u64>,
        max_age: Option<Duration>,
    ) -> Option<V> {
        if let Some(entry) = self.cache.get(&key) {
            if entry.value().is_fresh(min_slot, max_age) {
                return Some(entry.value().value.clone());
            }
        }
        let max_age_ms = max_age.map(|ttl| ttl.as_millis() as u64);
        debug!(
            target = "hydration",
            worker = self.name,
            ?key,
            min_slot,
            max_age_ms,
            "hydration cache miss; scheduling fetch"
        );
        self.schedule(key);
        None
    }

    pub(crate) fn cached(&self, key: &K) -> Option<V> {
        self.cache.get(key).map(|entry| entry.value().value.clone())
    }

    pub(crate) fn cached_entry(&self, key: &K) -> Option<HydratedValue<V>> {
        self.cache.get(key).map(|entry| entry.value().clone())
    }

    pub(crate) fn invalidate(&self, key: &K) {
        self.cache.remove(key);
        self.pending.remove(key);
        debug!(
            target = "hydration",
            worker = self.name,
            ?key,
            "hydration cache invalidated"
        );
    }

    pub(crate) fn invalidate_if_stale(
        &self,
        key: &K,
        min_slot: Option<u64>,
        max_age: Option<Duration>,
    ) -> bool {
        if let Some(entry) = self.cache.get(key) {
            if !entry.value().is_fresh(min_slot, max_age) {
                drop(entry);
                self.invalidate(key);
                let max_age_ms = max_age.map(|ttl| ttl.as_millis() as u64);
                debug!(
                    target = "hydration",
                    worker = self.name,
                    ?key,
                    min_slot,
                    max_age_ms,
                    "hydration cache invalidated (stale)"
                );
                return true;
            }
        }
        false
    }

    pub(crate) fn refresh(&self, key: K) {
        self.invalidate(&key);
        debug!(
            target = "hydration",
            worker = self.name,
            ?key,
            "hydration refresh requested"
        );
        self.schedule(key);
    }

    pub(crate) fn schedule(&self, key: K) {
        if !self.pending.insert(key) {
            return;
        }
        match self.sender.try_send(key) {
            Ok(()) => {
                telemetry::record_hydration_job_scheduled(self.name);
                debug!(
                    target = "hydration",
                    worker = self.name,
                    ?key,
                    "hydration job enqueued"
                );
            }
            Err(TrySendError::Full(_)) => {
                self.pending.remove(&key);
                telemetry::record_hydration_job_queue_failure(self.name, "queue_full");
                warn!(
                    target = "hydration",
                    worker = self.name,
                    ?key,
                    "hydration queue full; dropping request"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                self.pending.remove(&key);
                telemetry::record_hydration_job_queue_failure(self.name, "disconnected");
                warn!(
                    target = "hydration",
                    worker = self.name,
                    ?key,
                    "hydration queue dropped (disconnected)"
                );
            }
        }
    }

    fn spawn_worker(
        name: &'static str,
        index: usize,
        receiver: Receiver<K>,
        cache: Arc<DashMap<K, HydratedValue<V>>>,
        pending: Arc<DashSet<K>>,
        fetcher: Arc<dyn Fn(K) -> Option<HydratedValue<V>> + Send + Sync>,
    ) {
        let thread_name = format!("{}_hydrator_{index}", name);
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let mut consecutive_failures = 0u32;
                while let Ok(key) = receiver.recv() {
                    let job_start = Instant::now();
                    let mut backoff_delay = None;
                    let mut status = HydrationJobStatus::Failure;
                    if let Some(value) = fetcher(key) {
                        let should_store = cache
                            .get(&key)
                            .map(|existing| Self::should_replace(existing.value(), &value))
                            .unwrap_or(true);
                        if should_store {
                            cache.insert(key, value);
                        }
                        consecutive_failures = 0;
                        status = HydrationJobStatus::Success;
                    } else {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        backoff_delay = Some(failure_backoff_delay(consecutive_failures));
                    }
                    pending.remove(&key);
                    let elapsed = job_start.elapsed();
                    telemetry::record_hydration_job_finished(name, status, elapsed);
                    let status_label = match status {
                        HydrationJobStatus::Success => "success",
                        HydrationJobStatus::Failure => "failure",
                    };
                    debug!(
                        target = "hydration",
                        worker = name,
                        ?key,
                        status = status_label,
                        latency_ms = elapsed.as_millis() as u64,
                        consecutive_failures,
                        "hydration job completed"
                    );
                    if let Some(delay) = backoff_delay {
                        telemetry::record_hydration_job_backoff(name, delay);
                        debug!(
                            target = "hydration",
                            worker = name,
                            ?key,
                            backoff_ms = delay.as_millis() as u64,
                            consecutive_failures,
                            "hydration worker backing off after failure"
                        );
                        thread::sleep(delay);
                    }
                }
            })
            .expect("failed to spawn hydration worker thread");
    }

    fn should_replace(existing: &HydratedValue<V>, candidate: &HydratedValue<V>) -> bool {
        match (candidate.slot, existing.slot) {
            (Some(candidate_slot), Some(existing_slot)) => candidate_slot >= existing_slot,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => candidate.fetched_at >= existing.fetched_at,
        }
    }
}

fn failure_backoff_delay(consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return Duration::from_millis(0);
    }
    let exponent = consecutive_failures.saturating_sub(1).min(8);
    let multiplier = 1u64 << exponent;
    let delay_ms = HYDRATION_FAILURE_BACKOFF_BASE_MS
        .saturating_mul(multiplier)
        .min(HYDRATION_FAILURE_BACKOFF_MAX_MS);
    Duration::from_millis(delay_ms)
}
