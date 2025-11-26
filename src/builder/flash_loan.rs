use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::config::AppConfig;

#[derive(Clone)]
pub struct FlashLoanPlanner {
    prepared_routes: Arc<AtomicU64>,
}

impl FlashLoanPlanner {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let _ = config; // placeholder until flash-loan sources are wired
        Self {
            prepared_routes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn prepare_default_routes(&self) {
        self.prepared_routes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn prepared_route_count(&self) -> u64 {
        self.prepared_routes.load(Ordering::Relaxed)
    }
}
