use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use tracing::{debug, info};

use crate::builder::{BuilderHandle, BuilderStage};
use crate::core::{pin_current_thread, Lease, RingConsumer, RingProducer};
use crate::hydration::HydrationSnapshot;
use crate::stages::types::BundleCandidate;
use crate::telemetry;

/// Static knobs for the fast-lane planner.
///
/// These values intentionally live outside of runtime `AppConfig` for now so we can
/// iterate on the planner API without touching the user-facing config schema.
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// Maximum wall-clock budget from trade ingestion to a planned bundle.
    pub detection_budget: Duration,
    /// Maximum age allowed for any resource (snapshots, templates, ALTs).
    pub freshness_budget: Duration,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            detection_budget: Duration::from_millis(40),
            freshness_budget: Duration::from_millis(30),
        }
    }
}

/// Skeleton for the new fast-lane planner. The concrete implementation will replace
/// Stage1 + Stage2 once resource freshness tracking is in place.
pub struct FastLanePlanner {
    builder: BuilderHandle,
    config: PlannerConfig,
}

impl FastLanePlanner {
    pub fn new(builder: BuilderHandle, config: PlannerConfig) -> Self {
        Self { builder, config }
    }

    /// Returns the freshness limit enforced by the planner.
    pub fn freshness_budget(&self) -> Duration {
        self.config.freshness_budget
    }

    /// Temporary helper for time-budget accounting. Callers can compare the returned
    /// deadline against `Instant::now()` to decide whether to drop stale work.
    pub fn deadline_from(&self, start: Instant) -> Instant {
        start + self.config.detection_budget
    }

    pub fn run(
        self,
        stop: Arc<AtomicBool>,
        core_id: Option<usize>,
        snapshot_consumer: RingConsumer<HydrationSnapshot>,
        bundle_producer: RingProducer<Lease<BundleCandidate>>,
    ) -> Result<()> {
        if let Some(core) = core_id {
            let _ = pin_current_thread(core);
        }
        let _bundles = bundle_producer;
        info!(
            detection_budget_ms = self.config.detection_budget.as_millis(),
            freshness_budget_ms = self.config.freshness_budget.as_millis(),
            "fast-lane planner online"
        );
        while !stop.load(Ordering::Relaxed) {
            self.builder.heartbeat(BuilderStage::EvalHotPath);
            if let Some(snapshot) = snapshot_consumer.pop() {
                self.handle_snapshot(snapshot);
            } else {
                thread::yield_now();
                thread::sleep(Duration::from_millis(5));
            }
        }
        info!("fast-lane planner shutting down");
        Ok(())
    }

    fn handle_snapshot(&self, snapshot: HydrationSnapshot) {
        let freshness_deadline = snapshot.stale_at(self.config.freshness_budget);
        let detection_deadline = self.deadline_from(snapshot.issued_at);
        telemetry::record_detection_latency(Duration::from_millis(0));
        debug!(
            templates = snapshot.template_stats.template_count,
            ready_tables = snapshot.alt_stats.ready_tables,
            flash_routes = snapshot.flash_routes_prepared,
            freshness_deadline = ?freshness_deadline,
            detection_deadline = ?detection_deadline,
            "ingested hydration snapshot"
        );
    }
}

pub fn spawn_fast_lane_planner(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    snapshot_consumer: RingConsumer<HydrationSnapshot>,
    bundle_producer: RingProducer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
    config: PlannerConfig,
) -> Result<thread::JoinHandle<Result<()>>> {
    let worker = FastLanePlanner::new(builder, config);
    thread::Builder::new()
        .name("fast_lane_planner".into())
        .spawn(move || worker.run(stop, core_id, snapshot_consumer, bundle_producer))
        .map_err(Into::into)
}
