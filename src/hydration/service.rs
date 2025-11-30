use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use tracing::{debug, warn};

use super::{AltManagerStats, FlashLoanPlanner, TemplateCache, TemplateCacheStats};
use crate::builder::BuilderHandle;
use crate::core::RingProducer;

/// Snapshot emitted by the hydration service containing readiness metadata for
/// the fast-lane planner. Each snapshot is a cheap summary that can be fanned
/// out to multiple subscribers without cloning the heavier caches.
#[derive(Clone, Copy, Debug)]
pub struct HydrationSnapshot {
    pub issued_at: Instant,
    pub template_stats: TemplateCacheStats,
    pub alt_stats: AltManagerStats,
    pub flash_routes_prepared: u64,
}

impl HydrationSnapshot {
    pub fn stale_at(&self, ttl: Duration) -> Instant {
        self.issued_at + ttl
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TemplateReadiness {
    pub template_ready: bool,
    pub alt_ready: bool,
    pub template_fresh_until: Option<Instant>,
    pub alt_fresh_until: Option<Instant>,
}

impl TemplateReadiness {
    pub const fn missing() -> Self {
        Self {
            template_ready: false,
            alt_ready: false,
            template_fresh_until: None,
            alt_fresh_until: None,
        }
    }
}

/// HydrationService owns the resource managers (template cache, ALT manager,
/// flash-loan planner) and periodically publishes readiness summaries to the
/// fast-lane planner. The heavy lifting still lives inside those components;
/// the service simply coordinates refresh cadence and exposes lightweight
/// snapshots.
#[derive(Clone)]
pub struct HydrationService {
    builder: BuilderHandle,
    publish_interval: Duration,
}

impl HydrationService {
    pub fn new(builder: BuilderHandle) -> Self {
        Self {
            builder,
            publish_interval: Duration::from_millis(25),
        }
    }

    /// Spawns a thread that periodically publishes readiness snapshots onto the
    /// provided ring. The caller owns the stop flag and controls shutdown.
    pub fn spawn_snapshot_worker(
        &self,
        stop: Arc<AtomicBool>,
        producer: RingProducer<HydrationSnapshot>,
    ) -> Result<thread::JoinHandle<Result<()>>> {
        let builder = self.builder.clone();
        let interval = self.publish_interval;
        thread::Builder::new()
            .name("hydration_service".into())
            .spawn(move || run_snapshot_loop(stop, producer, builder, interval))
            .map_err(Into::into)
    }

    pub fn template_cache(&self) -> &TemplateCache {
        self.builder.template_cache()
    }

    pub fn flash_loan_planner(&self) -> &FlashLoanPlanner {
        self.builder.flash_loan_planner()
    }

    pub fn builder(&self) -> &BuilderHandle {
        &self.builder
    }
}

fn run_snapshot_loop(
    stop: Arc<AtomicBool>,
    producer: RingProducer<HydrationSnapshot>,
    builder: BuilderHandle,
    interval: Duration,
) -> Result<()> {
    while !stop.load(Ordering::Relaxed) {
        let snapshot = HydrationSnapshot {
            issued_at: Instant::now(),
            template_stats: builder.template_cache().stats(),
            alt_stats: builder.alt_manager().stats(),
            flash_routes_prepared: builder.flash_loan_planner().prepared_route_count(),
        };
        match producer.push(snapshot) {
            Ok(()) => {
                debug!(
                    templates = snapshot.template_stats.template_count,
                    ready_tables = snapshot.alt_stats.ready_tables,
                    flash_routes = snapshot.flash_routes_prepared,
                    "published hydration snapshot"
                );
            }
            Err(_) => {
                warn!("hydration snapshot channel full; dropping readiness sample");
            }
        }
        thread::sleep(interval);
    }
    Ok(())
}
