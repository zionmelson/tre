use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;

use anyhow::{anyhow, Result};
use tokio::{signal, task};

use crate::builder::BuilderHandle;
use crate::bundle::{BundleClient, BundleTracker};
use crate::cluster::{self, LeaderScheduleGate};
use crate::config::AppConfig;
use crate::core::{Lease, SlabPool, SpscRing};
use crate::notifier::Notifier;
use crate::stages::{self, BundleCandidate, PoolUpdate, TradeUpdate};
use crate::strategy::StrategyEngine;

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
        let (submit_tx, submit_rx) =
            SpscRing::<Lease<BundleCandidate>>::with_capacity(config.perf.ring_capacity)
                .into_split();

        let mut handles = Vec::new();
        let builder = BuilderHandle::new(config.clone())?;
        let strategy = Arc::new(StrategyEngine::new(config.clone())?);
        let notifier = Arc::new(Notifier::new(strategy.clone(), config.clone()));
        let bam_allowlist = Arc::new(cluster::BamValidatorSet::load_from_csv(
            config.bam_validator_csv_path(),
        )?);
        let (leader_state, leader_handle) =
            cluster::spawn_leader_schedule_watcher(stop.clone(), config.quicknode_rpcs.clone())?;
        handles.push(leader_handle);
        let leader_gate = Arc::new(LeaderScheduleGate::new(leader_state.clone(), bam_allowlist));

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
            builder.clone(),
            notifier.clone(),
            strategy.clone(),
        )?);
        handles.push(stages::spawn_submit_stage(
            stop.clone(),
            Some(config.perf.submit_core),
            bundle_rx,
            submit_tx,
            builder.clone(),
        )?);
        let bundle_client = Arc::new(BundleClient::new(config.clone(), builder.auth_signer())?);
        let (tracker, mut tracker_handles) = BundleTracker::spawn(
            stop.clone(),
            config.clone(),
            notifier.clone(),
            builder.auth_signer(),
        )?;
        handles.append(&mut tracker_handles);
        handles.push(stages::spawn_send_stage(
            stop.clone(),
            None,
            submit_rx,
            builder.clone(),
            bundle_client,
            notifier.clone(),
            tracker,
            leader_gate,
            config.quicknode_rpcs.clone(),
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
    let config_for_executor = config.clone();
    let executor = task::spawn_blocking(move || ExecutorRuntime::spawn(config_for_executor))
        .await
        .map_err(|err| anyhow!("executor initialization panicked: {err}"))??;

    signal::ctrl_c().await?;
    executor.request_shutdown();

    task::spawn_blocking(move || executor.join())
        .await
        .map_err(|err| anyhow!("executor shutdown panicked: {err}"))??;

    Ok(())
}
