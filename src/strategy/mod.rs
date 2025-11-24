use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tracing::debug;

use crate::{
    bundle::BundleRequest, config::AppConfig, corecast::Opportunity, idl::IdlCache,
    protocol::ProtocolRegistry,
};

mod amm_v3;
mod cross_pool;

use amm_v3::AmmV3Strategy;
use cross_pool::CrossPoolBook;

#[derive(Debug, Clone)]
pub struct ProtocolObservation {
    pub slot: u64,
    pub price: f64,
    pub liquidity: f64,
}

pub trait ProtocolStrategy: Send + Sync {
    fn slug(&self) -> &'static str;

    fn observe(&self, _opportunity: &Opportunity) -> Option<ProtocolObservation> {
        None
    }

    fn evaluate(
        &self,
        opportunity: &Opportunity,
        seq: u64,
        observation: Option<&ProtocolObservation>,
    ) -> Option<BundleRequest>;
}

#[derive(Clone)]
pub struct StrategyEngine {
    _config: Arc<AppConfig>,
    _registry: Arc<ProtocolRegistry>,
    _idl_cache: Arc<IdlCache>,
    template_count: usize,
    strategies: Arc<HashMap<String, Arc<dyn ProtocolStrategy>>>,
    counter: Arc<AtomicU64>,
    cross_pool: Arc<CrossPoolBook>,
}

impl StrategyEngine {
    pub fn new(
        config: Arc<AppConfig>,
        registry: Arc<ProtocolRegistry>,
        idl_cache: Arc<IdlCache>,
    ) -> Result<Self> {
        let template_count = registry.len();

        let mut strategy_map: HashMap<String, Arc<dyn ProtocolStrategy>> = HashMap::new();
        let amm_v3 = Arc::new(AmmV3Strategy::new(config.clone(), idl_cache.clone())?);
        strategy_map.insert(amm_v3.slug().to_string(), amm_v3);

        Ok(Self {
            _config: config,
            _registry: registry,
            _idl_cache: idl_cache,
            template_count,
            strategies: Arc::new(strategy_map),
            counter: Arc::new(AtomicU64::new(0)),
            cross_pool: Arc::new(CrossPoolBook::new()),
        })
    }

    pub fn evaluate(&self, opportunity: &Opportunity) -> Option<BundleRequest> {
        if self.template_count == 0 {
            return None;
        }

        let seq = self.counter.fetch_add(1, Ordering::Relaxed);

        let slug = opportunity.protocol.slug();
        let Some(strategy) = self.strategies.get(slug) else {
            debug!(
                target = "strategy",
                protocol = slug,
                signature = %opportunity.signature,
                "no registered strategy for protocol"
            );
            return None;
        };

        let observation = strategy.observe(opportunity);

        if let Some(bundle) = self
            .cross_pool
            .observe(opportunity, seq, observation.as_ref())
        {
            return Some(bundle);
        }

        strategy.evaluate(opportunity, seq, observation.as_ref())
    }
}
