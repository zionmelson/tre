use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use tokio::runtime::Builder as TokioRuntime;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::{
    AdapterBook, AltManagerStats, FlashLoanPlanner, PoolStateEvent, PoolStateUpdate, PoolStore,
    PoolStoreView, RpcRefresher, TemplateCache, TemplateCacheStats,
};
use crate::builder::BuilderHandle;
use crate::config::AppConfig;
use crate::core::RingProducer;
use crate::corecast::CoreCastClient;
use crate::hydration::types::PoolUpdate;
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
    pool_store: Arc<PoolStore>,
    rpc_refresher: Arc<RpcRefresher>,
}

impl HydrationService {
    pub fn new(builder: BuilderHandle, config: Arc<AppConfig>) -> Self {
        let adapter_book = Arc::new(AdapterBook::new(config.clone(), builder.clone()));
        let pool_store = Arc::new(PoolStore::with_limits_and_policy(
            config.watcher.max_tracked_pools,
            config.watcher.min_ready_pools,
            config.watcher.queue_drop_policy,
        ));
        let rpc_refresher = Arc::new(RpcRefresher::new(config.clone()));
        Self {
            builder,
            config,
            publish_interval: Duration::from_millis(25),
            adapter_book,
            pool_store,
            rpc_refresher,
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
    ) -> Result<thread::JoinHandle<Result<()>>> {
        let config = self.config.clone();
        let adapter_book = self.adapter_book.clone();
        let pool_store = self.pool_store.clone();
        thread::Builder::new()
            .name("hydration_ingest".into())
            .spawn(move || run_adapter_ingest_loop(stop, config, adapter_book, pool_store))
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

    pub fn pool_store(&self) -> Arc<PoolStore> {
        self.pool_store.clone()
    }

    pub fn pool_store_view(&self) -> PoolStoreView {
        self.pool_store.view()
    }

    pub fn wait_for_pool_readiness(&self, timeout: Duration) -> bool {
        self.pool_store.wait_until_ready(timeout)
    }

    pub fn spawn_watcher_metrics(
        &self,
        stop: Arc<AtomicBool>,
    ) -> Result<thread::JoinHandle<Result<()>>> {
        let updates = self
            .pool_store
            .subscribe(self.config.watcher.queue_capacity.max(1));
        thread::Builder::new()
            .name("watcher_metrics".into())
            .spawn(move || run_watcher_metrics_loop(stop, updates))
            .map_err(Into::into)
    }

    pub fn spawn_pool_maintenance(
        &self,
        stop: Arc<AtomicBool>,
    ) -> Result<thread::JoinHandle<Result<()>>> {
        let pool_store = self.pool_store.clone();
        let adapter_book = self.adapter_book.clone();
        let refresher = self.rpc_refresher.clone();
        let ttl = Duration::from_millis(self.config.watcher.pool_stale_ttl_ms.max(1));
        let sweep = Duration::from_millis(self.config.watcher.pool_stale_sweep_ms.max(50));
        thread::Builder::new()
            .name("hydration_pool_maintenance".into())
            .spawn(move || {
                run_pool_maintenance_loop(stop, pool_store, adapter_book, refresher, ttl, sweep)
            })
            .map_err(Into::into)
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
    pool_store: Arc<PoolStore>,
) -> Result<()> {
    let client = CoreCastClient::new(config.clone())
        .context("failed to initialize CoreCast client for hydration ingest")?;
    let pair_allowlist = Arc::new(config.token_pair_allowlist());
    let runtime = TokioRuntime::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("failed to build hydration ingest runtime")?;

    runtime.block_on(async move {
        pump_pool_stream(stop, client, adapter_book, pool_store, pair_allowlist).await
    })
}

async fn pump_pool_stream(
    stop: Arc<AtomicBool>,
    client: CoreCastClient,
    adapter_book: Arc<AdapterBook>,
    pool_store: Arc<PoolStore>,
    allowlist: Arc<HashSet<TokenPair>>,
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
                                    adapter_book.enrich_snapshot(&mut snapshot);
                                    if let Some(quotes) = adapter_book.snapshot_quotes(&snapshot) {
                                        snapshot.quotes = Some(quotes);
                                    }
                                    pool_store.update(snapshot);
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

fn run_watcher_metrics_loop(
    stop: Arc<AtomicBool>,
    updates: Receiver<PoolStateUpdate>,
) -> Result<()> {
    let capacity = updates.capacity().unwrap_or(0);
    while !stop.load(Ordering::Relaxed) {
        match updates.recv_timeout(Duration::from_millis(50)) {
            Ok(update) => match update.event {
                PoolStateEvent::Upsert(snapshot) => telemetry::record_watcher_update_age(
                    &snapshot.program_id,
                    update.last_updated.elapsed(),
                ),
                PoolStateEvent::Remove => {}
            },
            Err(RecvTimeoutError::Timeout) => {
                telemetry::record_hydration_queue_depth(updates.len(), capacity);
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
        telemetry::record_hydration_queue_depth(updates.len(), capacity);
    }
    Ok(())
}

fn run_pool_maintenance_loop(
    stop: Arc<AtomicBool>,
    pool_store: Arc<PoolStore>,
    adapter_book: Arc<AdapterBook>,
    refresher: Arc<RpcRefresher>,
    ttl: Duration,
    sweep: Duration,
) -> Result<()> {
    while !stop.load(Ordering::Relaxed) {
        let stale = pool_store.collect_stale(ttl);
        if stale.is_empty() {
            thread::sleep(sweep);
            continue;
        }
        for state in stale {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let protocol = state.program_id.clone();
            if let Some(mut refreshed) = refresher.refresh(&state.snapshot) {
                if let Some(quotes) = adapter_book.snapshot_quotes(&refreshed) {
                    refreshed.quotes = Some(quotes);
                }
                pool_store.update(refreshed);
                telemetry::record_hydration_rpc_refresh(&protocol);
            } else {
                telemetry::record_hydration_rpc_refresh_failure(&protocol);
                if pool_store.mark_snapshot_stale(&state.pool_id) {
                    telemetry::record_hydration_stale_pools(1);
                }
            }
        }
        thread::sleep(sweep);
    }
    Ok(())
}
