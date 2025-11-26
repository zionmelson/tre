use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::config::AppConfig;

#[derive(Clone)]
pub struct MevGuard {
    policy_revision: Arc<AtomicU64>,
    protected_bundles: Arc<AtomicU64>,
}

impl MevGuard {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let _ = config; // future: ingest MEV policy from config
        Self {
            policy_revision: Arc::new(AtomicU64::new(1)),
            protected_bundles: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn current_revision(&self) -> u64 {
        self.policy_revision.load(Ordering::Relaxed)
    }

    pub fn record_protected_bundle(&self) {
        self.protected_bundles.fetch_add(1, Ordering::Relaxed);
    }

    pub fn protected_bundle_count(&self) -> u64 {
        self.protected_bundles.load(Ordering::Relaxed)
    }
}
