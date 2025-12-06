use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::{signal, task};
use tracing::{info, warn};

use crate::builder::BuilderHandle;
use crate::bundle::{BundleClient, BundleTracker};
use crate::config::AppConfig;
use crate::core::{Lease, RingConsumer, SlabPool, SpscRing};
use crate::hydration::{HydrationService, SnapshotChannel};
use crate::notifier::Notifier;
use crate::planner::types::BundleCandidate;
use crate::planner::{
    spawn_fast_lane_planner, spawn_fast_lane_trade_feed, spawn_graph_service, PlannerConfig,
};
use crate::strategy::StrategyEngine;
use crate::submission::{self, SubmissionConfig};

pub struct ExecutorRuntime {
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<Result<()>>>,
    _config: Arc<AppConfig>,
}

impl ExecutorRuntime {
    pub fn spawn(config: Arc<AppConfig>) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let hydration_only = config.pipeline.hydration_only_mode;
        let dry_run = config.pipeline.dry_run_mode;
        if hydration_only {
            info!("hydration-only mode enabled; execution lanes disabled");
        }
        if dry_run {
            info!("dry-run mode enabled; submission lanes disabled");
        }

        let mut handles = Vec::new();
        let builder = BuilderHandle::new(config.clone())?;
        let hydration_service = HydrationService::new(builder.clone(), config.clone());
        let pool_store = hydration_service.pool_store();
        let pool_store_view = hydration_service.pool_store_view();
        let strategy = Arc::new(StrategyEngine::new(config.clone())?);
        let notifier = Arc::new(Notifier::new(strategy.clone(), config.clone()));
        let (snapshot_publisher, snapshot_reader) = SnapshotChannel::new().split();
        handles.push(hydration_service.spawn_adapter_ingest(stop.clone())?);
        handles.push(hydration_service.spawn_snapshot_worker(stop.clone(), snapshot_publisher)?);
        handles.push(hydration_service.spawn_watcher_metrics(stop.clone())?);
        handles.push(hydration_service.spawn_pool_maintenance(stop.clone())?);
        let mut hydration_reader = Some(snapshot_reader);

        let queue_capacity = config.watcher.queue_capacity.max(1);
        let graph_updates = pool_store.subscribe(queue_capacity);
        let planner_updates = pool_store.subscribe(queue_capacity);
        let mut planner_config = PlannerConfig::default();
        planner_config.require_alt_ready = !config.pipeline.dry_run_mode;
        let (graph_handle, graph_thread) = spawn_graph_service(
            stop.clone(),
            None,
            graph_updates,
            pool_store_view.clone(),
            builder.clone(),
            planner_config.clone(),
        )?;
        handles.push(graph_thread);

        let ready_timeout = Duration::from_millis(config.watcher.initial_ready_timeout_ms.max(1));
        if pool_store.wait_until_ready(ready_timeout) {
            info!(
                target = "hydration",
                ready_pools = pool_store.ready_len(),
                tracked_pools = pool_store.len(),
                "pool store ready; planner lanes may begin"
            );
        } else {
            warn!(
                target = "hydration",
                timeout_ms = config.watcher.initial_ready_timeout_ms,
                ready_pools = pool_store.ready_len(),
                tracked_pools = pool_store.len(),
                min_ready = config.watcher.min_ready_pools,
                "timed out waiting for initial pool hydration"
            );
            hydration_service.log_readiness_blockers();
        }

        if hydration_only {
            info!("hydration-only topology active; planner + submitter skipped");
            return Ok(Self {
                stop,
                handles,
                _config: config,
            });
        }

        let hydration_reader = hydration_reader
            .take()
            .expect("hydration snapshot reader missing");

        let (planner_tx, planner_rx) =
            SpscRing::<Lease<BundleCandidate>>::with_capacity(config.perf.ring_capacity)
                .into_split();
        let bundle_pool = SlabPool::new(config.perf.bundle_slab);

        let (trade_rx, trade_handle) =
            spawn_fast_lane_trade_feed(stop.clone(), config.clone(), config.perf.ring_capacity)?;
        handles.push(trade_handle);

        handles.push(spawn_fast_lane_planner(
            stop.clone(),
            Some(config.perf.eval_core),
            hydration_reader,
            graph_handle.clone(),
            notifier.clone(),
            planner_tx,
            builder.clone(),
            planner_config.clone(),
            bundle_pool,
            trade_rx,
            planner_updates,
            pool_store.clone(),
            ready_timeout,
            config.watcher.min_ready_pools,
        )?);

        if dry_run {
            handles.push(spawn_planner_drain(stop.clone(), planner_rx)?);
            info!("dry-run sink active; planner outputs discarded");
        } else {
            let bundle_client = Arc::new(BundleClient::new(config.clone(), builder.auth_signer())?);
            let (tracker, mut tracker_handles) = BundleTracker::spawn(
                stop.clone(),
                config.clone(),
                notifier.clone(),
                builder.auth_signer(),
            )?;
            handles.append(&mut tracker_handles);
            handles.push(submission::spawn_fast_lane_submitter(
                stop.clone(),
                Some(config.perf.submit_core),
                planner_rx,
                builder.clone(),
                bundle_client,
                notifier.clone(),
                tracker,
                SubmissionConfig::default(),
            )?);
        }

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

fn spawn_planner_drain(
    stop: Arc<AtomicBool>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("planner_drain".into())
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                while consumer.pop().is_some() {}
                thread::sleep(Duration::from_millis(5));
            }
            Ok(())
        })
        .map_err(Into::into)
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
