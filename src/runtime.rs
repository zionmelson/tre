use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;

use anyhow::{anyhow, Result};

use crate::config::AppConfig;
use crate::core::{Lease, SlabPool, SpscRing};
use crate::stages::{self, BundleCandidate, PoolUpdate, TradeUpdate};

pub struct ExecutorRuntime {
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<Result<()>>>,
    _config: Arc<AppConfig>,
}

impl ExecutorRuntime {
    pub fn spawn(config: Arc<AppConfig>) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));

        let trade_pool = SlabPool::new(config.perf.opportunity_slab);
        let pool_update_pool = SlabPool::new(config.perf.opportunity_slab);
        let bundle_pool = SlabPool::new(config.perf.bundle_slab);
        let (trade_tx, trade_rx) =
            SpscRing::<Lease<TradeUpdate>>::with_capacity(config.perf.ring_capacity).into_split();
        let (pool_tx, pool_rx) =
            SpscRing::<Lease<PoolUpdate>>::with_capacity(config.perf.ring_capacity).into_split();
        let (bundle_tx, bundle_rx) =
            SpscRing::<Lease<BundleCandidate>>::with_capacity(config.perf.ring_capacity)
                .into_split();

        let mut handles = Vec::new();
        handles.push(stages::spawn_ingest_stage(
            stop.clone(),
            Some(config.perf.ingest_core),
            config.clone(),
            trade_pool.clone(),
            trade_tx,
            pool_update_pool.clone(),
            pool_tx,
        )?);
        handles.push(stages::spawn_eval_stage(
            stop.clone(),
            Some(config.perf.eval_core),
            config.clone(),
            trade_rx,
            pool_rx,
            bundle_tx,
            bundle_pool.clone(),
        )?);
        handles.push(stages::spawn_submit_stage(
            stop.clone(),
            Some(config.perf.submit_core),
            bundle_rx,
        )?);

        Ok(Self {
            stop,
            handles,
            _config: config,
        })
    }

    pub fn request_shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn join(self) -> Result<()> {
        for handle in self.handles {
            match handle.join() {
                Ok(result) => result?,
                Err(panic) => {
                    return Err(anyhow!("stage thread panicked: {}", describe_panic(panic)))
                }
            }
        }
        Ok(())
    }
}

fn describe_panic(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(msg) = payload.downcast_ref::<&str>() {
        msg.to_string()
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        msg.clone()
    } else {
        "unknown panic".to_string()
    }
}

pub async fn run(config: Arc<AppConfig>) -> Result<()> {
    let runtime = ExecutorRuntime::spawn(config)?;
    tokio::signal::ctrl_c().await?;
    runtime.request_shutdown();
    runtime.join()
}
