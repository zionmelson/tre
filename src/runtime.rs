use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;

use crate::bundle::BundleClient;
use crate::config::AppConfig;
use crate::core::{spsc_ring, SlabPool};
use crate::corecast::CorecastSubscriber;
use crate::idl::IdlCache;
use crate::notifier::Notifier;
use crate::stages::{
    stage0_ingest::OpportunityLease, stage1_eval::BundleLease, Stage0Ingest, Stage1Eval,
    Stage2Submit,
};
use crate::strategy::StrategyEngine;

pub struct Executor {
    running: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
    _config: Arc<AppConfig>,
    _idl_cache: Arc<IdlCache>,
}

impl Executor {
    pub fn build(config: AppConfig) -> Result<Self> {
        let config = Arc::new(config);
        let perf = config.perf.clone();
        let idl_cache = Arc::new(IdlCache::bootstrap()?);
        let running = Arc::new(AtomicBool::new(true));

        let (opp_tx, opp_rx) = spsc_ring::<OpportunityLease>(perf.sanitized_ring_capacity());
        let (bundle_tx, bundle_rx) = spsc_ring::<BundleLease>(perf.sanitized_ring_capacity());

        let opp_pool = SlabPool::new(perf.opportunity_capacity());
        let bundle_pool = SlabPool::new(perf.bundle_capacity());

        let corecast = CorecastSubscriber::new(config.clone());
        let strategy = StrategyEngine::new(config.clone());
        let bundle_client = BundleClient::new(config.clone());
        let notifier = Notifier::new(config.clone());

        tracing::info!(
            target = "runtime",
            opp_slab = opp_pool.capacity(),
            bundle_slab = bundle_pool.capacity(),
            ring_capacity = perf.sanitized_ring_capacity(),
            "flat executor booting"
        );

        let mut handles = Vec::with_capacity(3);
        handles.push(Stage0Ingest::spawn(
            corecast,
            opp_pool.clone(),
            opp_tx,
            running.clone(),
            perf.ingest_core,
        ));
        handles.push(Stage1Eval::spawn(
            opp_rx,
            bundle_tx,
            bundle_pool.clone(),
            strategy,
            running.clone(),
            perf.eval_core,
        ));
        handles.push(Stage2Submit::spawn(
            bundle_rx,
            bundle_client,
            notifier,
            running.clone(),
            perf.submit_core,
        ));

        Ok(Self {
            running,
            handles,
            _config: config,
            _idl_cache: idl_cache,
        })
    }

    pub async fn run(self) -> Result<()> {
        tracing::info!(target = "runtime", "executor online; awaiting ctrl+c");
        tokio::signal::ctrl_c().await?;
        tracing::info!(target = "runtime", "shutdown signal received");
        self.running.store(false, Ordering::Release);
        for handle in self.handles {
            let _ = handle.join();
        }
        Ok(())
    }
}
