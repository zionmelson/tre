use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, Result};
use tokio::{signal, task};

use crate::builder::BuilderHandle;
use crate::bundle::{BundleClient, BundleTracker};
use crate::config::AppConfig;
use crate::core::{Lease, SlabPool, SpscRing};
use crate::hydration::ingest::spawn_ingest_stage;
use crate::hydration::readiness::{ReadinessPolicy, ReadinessRegistry};
use crate::hydration::{HydrationService, PoolStore, SnapshotChannel};
use crate::notifier::Notifier;
use crate::planner::types::BundleCandidate;
use crate::planner::{self, PlannerConfig};
use crate::strategy::StrategyEngine;
use crate::submission::{self, SubmissionConfig};

pub struct ExecutorRuntime {
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<Result<()>>>,
}

impl ExecutorRuntime {
    pub fn spawn(config: Arc<AppConfig>) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let readiness_policy = ReadinessPolicy::from_watcher_config(&config.as_ref().watcher);
        let readiness = ReadinessRegistry::new(readiness_policy);
        let mut handles = Vec::new();

        let builder = BuilderHandle::new_with_readiness(config.clone(), readiness.clone())?;
        let strategy = Arc::new(StrategyEngine::new(config.clone())?);
        let notifier = Arc::new(Notifier::new(strategy, config.clone()));

        let hydration = HydrationService::new(builder.clone(), config.clone(), readiness.clone());
        let (snapshot_publisher, _) = SnapshotChannel::new().split();
        handles.push(hydration.spawn_snapshot_worker(stop.clone(), snapshot_publisher)?);
        handles.push(hydration.spawn_adapter_ingest(stop.clone())?);
        handles.push(hydration.spawn_pool_maintenance(stop.clone())?);

        let ingest = spawn_ingest_stage(
            stop.clone(),
            config.clone(),
            readiness.clone(),
            config.watcher.queue_capacity.max(1),
        )?;
        handles.push(ingest.handle);

        let pool_store = Arc::new(PoolStore::new());
        let planner_config = PlannerConfig::default();
        let updates = pool_store.subscribe(config.watcher.queue_capacity.max(1));
        let (graph_handle, graph_worker) = planner::spawn_graph_service(
            stop.clone(),
            None,
            updates,
            pool_store.view(),
            builder.clone(),
            planner_config.clone(),
        )?;
        handles.push(graph_worker);

        let (planner_tx, planner_rx) =
            SpscRing::<Lease<BundleCandidate>>::with_capacity(config.perf.ring_capacity)
                .into_split();
        let bundle_pool = SlabPool::new(config.perf.bundle_slab);
        let (stage2_tx, stage2_rx) =
            SpscRing::<Lease<BundleCandidate>>::with_capacity(config.perf.ring_capacity)
                .into_split();

        handles.push(planner::spawn_fast_lane_planner(
            stop.clone(),
            planner_config.clone(),
            readiness,
            ingest.trade_rx,
            ingest.pool_rx,
            planner_tx,
            bundle_pool,
            pool_store.clone(),
            graph_handle,
            Some(config.perf.eval_core),
        )?);

        handles.push(planner::spawn_submit_stage(
            stop.clone(),
            Some(config.perf.submit_core),
            planner_rx,
            stage2_tx,
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

        handles.push(submission::spawn_fast_lane_submitter(
            stop.clone(),
            Some(config.perf.submit_core),
            stage2_rx,
            builder,
            bundle_client,
            tracker,
            SubmissionConfig::default(),
        )?);

        Ok(Self { stop, handles })
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
    let executor = task::spawn_blocking(move || ExecutorRuntime::spawn(config))
        .await
        .map_err(|err| anyhow!("executor initialization panicked: {err}"))??;

    signal::ctrl_c().await?;
    executor.request_shutdown();

    task::spawn_blocking(move || executor.join())
        .await
        .map_err(|err| anyhow!("executor shutdown panicked: {err}"))??;

    Ok(())
}
