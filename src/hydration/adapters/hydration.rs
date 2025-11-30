use std::{fmt::Debug, hash::Hash, sync::Arc, thread};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use dashmap::{DashMap, DashSet};
use tracing::warn;

/// Lightweight worker queue that hydrates on-chain accounts off critical paths.
/// The hydrator keeps an in-memory cache and deduplicates in-flight requests so
/// callers can simply request hydration and continue without blocking.
pub(crate) struct AsyncHydrator<K, V>
where
    K: Copy + Eq + Hash + Send + Sync + Debug + 'static,
    V: Clone + Send + Sync + 'static,
{
    cache: Arc<DashMap<K, V>>,
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
        F: Fn(K) -> Option<V> + Send + Sync + 'static,
    {
        let cache = Arc::new(DashMap::new());
        Self::with_cache(name, worker_count, fetcher, cache)
    }

    pub(crate) fn with_cache<F>(
        name: &'static str,
        worker_count: usize,
        fetcher: F,
        cache: Arc<DashMap<K, V>>,
    ) -> Self
    where
        F: Fn(K) -> Option<V> + Send + Sync + 'static,
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
        if let Some(entry) = self.cache.get(&key) {
            return Some(entry.clone());
        }
        self.schedule(key);
        None
    }

    pub(crate) fn cached(&self, key: &K) -> Option<V> {
        self.cache.get(key).map(|entry| entry.clone())
    }

    pub(crate) fn schedule(&self, key: K) {
        if !self.pending.insert(key) {
            return;
        }
        match self.sender.try_send(key) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.pending.remove(&key);
                warn!(
                    target = "hydration",
                    worker = self.name,
                    ?key,
                    "hydration queue full; dropping request"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                self.pending.remove(&key);
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
        cache: Arc<DashMap<K, V>>,
        pending: Arc<DashSet<K>>,
        fetcher: Arc<dyn Fn(K) -> Option<V> + Send + Sync>,
    ) {
        let thread_name = format!("{}_hydrator_{index}", name);
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                while let Ok(key) = receiver.recv() {
                    if let Some(value) = fetcher(key) {
                        cache.insert(key, value);
                    }
                    pending.remove(&key);
                }
            })
            .expect("failed to spawn hydration worker thread");
    }
}
