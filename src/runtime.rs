use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::{signal, task};
use tracing::info;

use crate::builder::BuilderHandle;
use crate::bundle::{BundleClient, BundleTracker};
use crate::config::AppConfig;
use crate::core::{Lease, RingConsumer, SpscRing};
use crate::hydration::{HydrationService, HydrationSnapshot};
use crate::notifier::Notifier;
use crate::planner::{spawn_fast_lane_planner, PlannerConfig};
use crate::stages::types::BundleCandidate;
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
        let fast_lane_enabled = config.pipeline.enable_fast_lane && !hydration_only;
        if hydration_only {
            info!("hydration-only mode enabled; execution lanes disabled");
        }
        if !fast_lane_enabled && !hydration_only {
            return Err(anyhow!(
                "fast lane disabled but legacy execution path has been removed; set pipeline.enable_fast_lane=true or enable hydration_only_mode"
            ));
        }

        let mut handles = Vec::new();
        let builder = BuilderHandle::new(config.clone())?;
        let hydration_service = HydrationService::new(builder.clone());
        let strategy = Arc::new(StrategyEngine::new(config.clone())?);
        let notifier = Arc::new(Notifier::new(strategy.clone(), config.clone()));
        let (hydration_tx, hydration_rx) =
            SpscRing::<HydrationSnapshot>::with_capacity(config.perf.ring_capacity).into_split();
        handles.push(hydration_service.spawn_snapshot_worker(stop.clone(), hydration_tx)?);

        if hydration_only {
            handles.push(spawn_snapshot_drain(stop.clone(), hydration_rx)?);
            info!("hydration-only topology active; planner + submitter skipped");
            return Ok(Self {
                stop,
                handles,
                _config: config,
            });
        }

        let (planner_tx, planner_rx) =
            SpscRing::<Lease<BundleCandidate>>::with_capacity(config.perf.ring_capacity)
                .into_split();

        handles.push(spawn_fast_lane_planner(
            stop.clone(),
            Some(config.perf.eval_core),
            hydration_rx,
            planner_tx,
            builder.clone(),
            PlannerConfig::default(),
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
            planner_rx,
            builder.clone(),
            bundle_client,
            notifier.clone(),
            tracker,
            SubmissionConfig::default(),
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

fn spawn_snapshot_drain(
    stop: Arc<AtomicBool>,
    consumer: RingConsumer<HydrationSnapshot>,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("hydration_drain".into())
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
