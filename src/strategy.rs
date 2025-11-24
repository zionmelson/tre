use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{
    bundle::BundleRequest, codegen::pools_static::POOL_TEMPLATES, config::AppConfig,
    corecast::Opportunity,
};

#[derive(Clone)]
pub struct StrategyEngine {
    _config: Arc<AppConfig>,
    template_count: usize,
    counter: Arc<AtomicU64>,
}

impl StrategyEngine {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self {
            _config: config,
            template_count: POOL_TEMPLATES.len(),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn evaluate(&self, opportunity: &Opportunity) -> Option<BundleRequest> {
        if self.template_count == 0 {
            return None;
        }

        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        if seq % 256 != 0 {
            return None;
        }

        Some(BundleRequest {
            uuid: format!("bundle-{seq:08x}-{}", opportunity.signature),
        })
    }
}
