use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::runtime::Builder as TokioRuntime;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::{AdapterBook, AltManagerStats, FlashLoanPlanner, TemplateCache, TemplateCacheStats};
use crate::builder::BuilderHandle;
use crate::config::AppConfig;
use crate::core::RingProducer;
use crate::corecast::CoreCastClient;
use crate::hydration::types::{PoolSnapshot, PoolUpdate};
use crate::telemetry;
use crate::token_pair::TokenPair;

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
    config: Arc<AppConfig>,
    publish_interval: Duration,
    adapter_book: Arc<AdapterBook>,
}

impl HydrationService {
    pub fn new(builder: BuilderHandle, config: Arc<AppConfig>) -> Self {
        let adapter_book = Arc::new(AdapterBook::new(config.clone(), builder.clone()));
        Self {
            builder,
            config,
            publish_interval: Duration::from_millis(25),
            adapter_book,
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

    /// Spawns the CoreCast ingest worker responsible for updating the shared
    /// adapter book. This keeps protocol adapters hydrated for both the legacy
    /// planner and the new fast lane without duplicating ingestion logic.
    pub fn spawn_adapter_ingest(
        &self,
        stop: Arc<AtomicBool>,
        pool_producer: RingProducer<PoolSnapshot>,
    ) -> Result<thread::JoinHandle<Result<()>>> {
        let config = self.config.clone();
        let adapter_book = self.adapter_book.clone();
        thread::Builder::new()
            .name("hydration_ingest".into())
            .spawn(move || run_adapter_ingest_loop(stop, config, adapter_book, pool_producer))
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

    pub fn adapter_book(&self) -> Arc<AdapterBook> {
        self.adapter_book.clone()
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

fn run_adapter_ingest_loop(
    stop: Arc<AtomicBool>,
    config: Arc<AppConfig>,
    adapter_book: Arc<AdapterBook>,
    pool_producer: RingProducer<PoolSnapshot>,
) -> Result<()> {
    let client = CoreCastClient::new(config.clone())
        .context("failed to initialize CoreCast client for hydration ingest")?;
    let pair_allowlist = Arc::new(config.token_pair_allowlist());
    let runtime = TokioRuntime::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build hydration ingest runtime")?;

    runtime.block_on(async move {
        pump_pool_stream(stop, client, adapter_book, pair_allowlist, pool_producer).await
    })
}

async fn pump_pool_stream(
    stop: Arc<AtomicBool>,
    client: CoreCastClient,
    adapter_book: Arc<AdapterBook>,
    allowlist: Arc<HashSet<TokenPair>>,
    pool_producer: RingProducer<PoolSnapshot>,
) -> Result<()> {
    let mut seq = 0u64;
    let mut retry = Duration::from_millis(250);
    let max_retry = Duration::from_secs(5);

    while !stop.load(Ordering::Relaxed) {
        match client.pool_stream().await {
            Ok(mut stream) => {
                info!(target = "hydration", "CoreCast pool stream subscribed");
                retry = Duration::from_millis(250);
                while !stop.load(Ordering::Relaxed) {
                    match stream.message().await {
                        Ok(Some(message)) => {
                            if let Some(update) = PoolUpdate::from_corecast(seq, &message) {
                                seq = seq.wrapping_add(1);
                                if allowlist.contains(&TokenPair::new(
                                    &update.base_mint,
                                    &update.quote_mint,
                                )) {
                                    if let Some(adapter) = adapter_book.ingest(&update) {
                                        telemetry::record_hydration_adapter_ingest(adapter);
                                    }
                                    let mut snapshot = update.into_snapshot();
                                    if let Some(quotes) = adapter_book.snapshot_quotes(&snapshot) {
                                        snapshot.quotes = Some(quotes);
                                    }
                                    if let Err(snapshot) = pool_producer.push(snapshot) {
                                        warn!(
                                            target = "hydration",
                                            "pool snapshot ring full; dropping update"
                                        );
                                        drop(snapshot);
                                    }
                                }
                            } else {
                                debug!(target = "hydration", "dropped malformed pool update");
                            }
                        }
                        Ok(None) => {
                            warn!(target = "hydration", "pool stream closed by server");
                            break;
                        }
                        Err(status) => {
                            warn!(target = "hydration", %status, "pool stream error");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                warn!(
                    target = "hydration",
                    %err,
                    "failed to subscribe to pool stream"
                );
                info!(
                    target = "hydration",
                    error = ?err,
                    "pool stream subscription error detail"
                );
            }
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }

        sleep(retry).await;
        retry = (retry * 2).min(max_retry);
    }

    Ok(())
}
