use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use solana_sdk::pubkey::Pubkey;
use tokio::runtime::Builder as TokioRuntime;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::{
    adapters::{AdapterReadiness, ReadinessBlocker, ReadinessBlockerKind},
    AdapterBook, AltManagerStats, FlashLoanPlanner, PoolStateEvent, PoolStateUpdate, PoolStore,
    PoolStoreView, RpcRefresher, SnapshotPublisher, TemplateCache, TemplateCacheStats,
};
use crate::builder::{templates::SwapTemplateDirection, BuilderHandle};
use crate::config::AppConfig;
use crate::corecast::CoreCastClient;
use crate::hydration::types::{HydrationComponent, PoolSnapshot, PoolUpdate, WHIRLPOOL_PROGRAM_ID};
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
    alt_required_for_readiness: bool,
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
        let alt_required_for_readiness = !config.pipeline.dry_run_mode;
        Self {
            builder,
            config,
            publish_interval: Duration::from_millis(25),
            adapter_book,
            pool_store,
            rpc_refresher,
            alt_required_for_readiness,
        }
    }

    /// Spawns a thread that periodically publishes readiness snapshots onto the
    /// provided ring. The caller owns the stop flag and controls shutdown.
    pub fn spawn_snapshot_worker(
        &self,
        stop: Arc<AtomicBool>,
        publisher: SnapshotPublisher,
    ) -> Result<thread::JoinHandle<Result<()>>> {
        let builder = self.builder.clone();
        let interval = self.publish_interval;
        thread::Builder::new()
            .name("hydration_service".into())
            .spawn(move || run_snapshot_loop(stop, publisher, builder, interval))
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
        let builder = self.builder.clone();
        let require_alt_ready = self.alt_required_for_readiness;
        thread::Builder::new()
            .name("hydration_ingest".into())
            .spawn(move || {
                run_adapter_ingest_loop(
                    stop,
                    config,
                    adapter_book,
                    pool_store,
                    builder,
                    require_alt_ready,
                )
            })
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

    pub fn readiness_diagnostics(&self) -> AdapterReadiness {
        self.adapter_book.readiness_diagnostics()
    }

    pub fn wait_for_pool_readiness(&self, timeout: Duration) -> bool {
        if self.pool_store.wait_until_ready(timeout) {
            true
        } else {
            self.log_readiness_blockers();
            false
        }
    }

    pub fn spawn_watcher_metrics(
        &self,
        stop: Arc<AtomicBool>,
    ) -> Result<thread::JoinHandle<Result<()>>> {
        let updates = self
            .pool_store
            .subscribe(self.config.watcher.queue_capacity.max(1));
        let pool_store = self.pool_store.clone();
        thread::Builder::new()
            .name("watcher_metrics".into())
            .spawn(move || run_watcher_metrics_loop(stop, pool_store, updates))
            .map_err(Into::into)
    }

    pub fn spawn_pool_maintenance(
        &self,
        stop: Arc<AtomicBool>,
    ) -> Result<thread::JoinHandle<Result<()>>> {
        let pool_store = self.pool_store.clone();
        let adapter_book = self.adapter_book.clone();
        let refresher = self.rpc_refresher.clone();
        let ttl = self.config.watcher.pool_stale_ttl_ms.and_then(|ms| {
            if ms > 0 {
                Some(Duration::from_millis(ms))
            } else {
                None
            }
        });
        let sweep = Duration::from_millis(self.config.watcher.pool_stale_sweep_ms.max(50));
        let builder = self.builder.clone();
        let require_alt_ready = self.alt_required_for_readiness;
        if ttl.is_none() {
            info!(
                target = "hydration",
                "pool stale TTL disabled; readiness will stay latched until explicit failures"
            );
        }
        thread::Builder::new()
            .name("hydration_pool_maintenance".into())
            .spawn(move || {
                run_pool_maintenance_loop(
                    stop,
                    pool_store,
                    adapter_book,
                    refresher,
                    ttl,
                    sweep,
                    builder,
                    require_alt_ready,
                )
            })
            .map_err(Into::into)
    }

    pub(crate) fn log_readiness_blockers(&self) {
        let diagnostics = self.readiness_diagnostics();
        if diagnostics.is_empty() {
            warn!(
                target = "hydration",
                "pool readiness timeout; no adapter blockers reported"
            );
        } else {
            let total = diagnostics.len();
            let samples = diagnostics
                .samples(3)
                .into_iter()
                .map(format_blocker)
                .collect::<Vec<_>>()
                .join(" | ");
            warn!(
                target = "hydration",
                blocked = total,
                samples = %samples,
                "pool readiness blocked by adapter backoff"
            );
        }

        let stalled = self.sample_unready_pool_reasons(3);
        if !stalled.is_empty() {
            warn!(
                target = "hydration",
                samples = %stalled.join(" | "),
                "pool readiness stalled; sample pools failing fast-lane checks"
            );
        }
    }

    fn sample_unready_pool_reasons(&self, limit: usize) -> Vec<String> {
        if limit == 0 {
            return Vec::new();
        }
        let mut samples = Vec::new();
        let view = self.pool_store.view();
        for entry in view.iter() {
            let state = entry.value().clone();
            if state.ready {
                continue;
            }
            if let Err(reason) = evaluate_snapshot_readiness(
                &self.builder,
                &state.snapshot,
                self.alt_required_for_readiness,
            ) {
                samples.push(format!("{}:{} {}", state.program_id, state.pool_id, reason));
            }
            if samples.len() >= limit {
                break;
            }
        }
        samples
    }
}

fn format_blocker(blocker: ReadinessBlocker) -> String {
    let retry_ms = blocker.retry_in.as_millis() as u64;
    match blocker.kind {
        ReadinessBlockerKind::PoolMeta => format!(
            "{}:{} failures={} retry_in_ms={}",
            blocker.adapter, blocker.pool, blocker.failures, retry_ms
        ),
        ReadinessBlockerKind::TickArray => format!(
            "{}:{} tick={} failures={} retry_in_ms={}",
            blocker.adapter,
            blocker.pool,
            blocker.tick_start.unwrap_or_default(),
            blocker.failures,
            retry_ms
        ),
    }
}

fn run_snapshot_loop(
    stop: Arc<AtomicBool>,
    publisher: SnapshotPublisher,
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
        let template_count = snapshot.template_stats.template_count;
        let ready_tables = snapshot.alt_stats.ready_tables;
        let flash_routes = snapshot.flash_routes_prepared;
        let overwritten = publisher.publish(snapshot);
        if overwritten {
            telemetry::record_hydration_snapshot_overwrite();
        }
        debug!(
            templates = template_count,
            ready_tables, flash_routes, overwritten, "published hydration snapshot"
        );
        thread::sleep(interval);
    }
    Ok(())
}

fn run_adapter_ingest_loop(
    stop: Arc<AtomicBool>,
    config: Arc<AppConfig>,
    adapter_book: Arc<AdapterBook>,
    pool_store: Arc<PoolStore>,
    builder: BuilderHandle,
    require_alt_ready: bool,
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
        pump_pool_stream(
            stop,
            client,
            adapter_book,
            pool_store,
            pair_allowlist,
            builder,
            require_alt_ready,
        )
        .await
    })
}

async fn pump_pool_stream(
    stop: Arc<AtomicBool>,
    client: CoreCastClient,
    adapter_book: Arc<AdapterBook>,
    pool_store: Arc<PoolStore>,
    allowlist: Arc<HashSet<TokenPair>>,
    builder: BuilderHandle,
    require_alt_ready: bool,
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
                                    if let Some(quotes) =
                                        adapter_book.snapshot_quotes(&mut snapshot)
                                    {
                                        snapshot.quotes = Some(quotes);
                                    }
                                    let pending = snapshot.hydration.pending_components();
                                    if !pending.is_empty() {
                                        adapter_book.warm_components(&snapshot, &pending);
                                    }
                                    let ready = snapshot_ready_for_fastlane(
                                        &builder,
                                        &snapshot,
                                        require_alt_ready,
                                    );
                                    pool_store.update(snapshot, ready);
                                    telemetry::record_hydration_heartbeat(unix_timestamp_ms());
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

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_millis() as u64)
        .unwrap_or_default()
}

fn run_watcher_metrics_loop(
    stop: Arc<AtomicBool>,
    pool_store: Arc<PoolStore>,
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
        telemetry::record_hydration_pool_counts(pool_store.ready_len(), pool_store.len());
    }
    Ok(())
}

fn run_pool_maintenance_loop(
    stop: Arc<AtomicBool>,
    pool_store: Arc<PoolStore>,
    adapter_book: Arc<AdapterBook>,
    refresher: Arc<RpcRefresher>,
    ttl: Option<Duration>,
    sweep: Duration,
    builder: BuilderHandle,
    require_alt_ready: bool,
) -> Result<()> {
    while !stop.load(Ordering::Relaxed) {
        let Some(ttl_value) = ttl else {
            thread::sleep(sweep);
            continue;
        };
        let stale = pool_store.collect_stale(ttl_value);
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
                if let Some(quotes) = adapter_book.snapshot_quotes(&mut refreshed) {
                    refreshed.quotes = Some(quotes);
                }
                let ready = snapshot_ready_for_fastlane(&builder, &refreshed, require_alt_ready);
                pool_store.update(refreshed, ready);
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

fn snapshot_ready_for_fastlane(
    builder: &BuilderHandle,
    snapshot: &PoolSnapshot,
    require_alt_ready: bool,
) -> bool {
    evaluate_snapshot_readiness(builder, snapshot, require_alt_ready).is_ok()
}

fn evaluate_snapshot_readiness(
    builder: &BuilderHandle,
    snapshot: &PoolSnapshot,
    require_alt_ready: bool,
) -> Result<(), PoolReadinessFailure> {
    let pool_pubkey =
        Pubkey::from_str(&snapshot.pool_id).map_err(|_| PoolReadinessFailure::InvalidPoolId)?;

    for direction in [
        SwapTemplateDirection::BaseIn,
        SwapTemplateDirection::BaseOut,
    ] {
        let readiness = builder.template_readiness(&pool_pubkey, direction);
        if !readiness.template_ready {
            return Err(PoolReadinessFailure::MissingTemplate(direction));
        }
        if require_alt_ready && !readiness.alt_ready {
            return Err(PoolReadinessFailure::MissingAlt(direction));
        }
    }

    if !snapshot.hydration.ready() {
        let pending = snapshot.hydration.pending_components();
        let lag_ms = snapshot.hydration.pending_age_ms(snapshot.updated_at);
        let lag_slots = snapshot.hydration.slot_lag(snapshot.slot);
        for component in &pending {
            telemetry::record_hydration_component_pending(
                &snapshot.program_id,
                component.as_str(),
                Some(lag_ms),
                Some(lag_slots),
            );
        }
        return Err(PoolReadinessFailure::HydrationPending(pending));
    }

    if snapshot.quotes.is_none() {
        return Err(PoolReadinessFailure::MissingQuotes);
    }

    if snapshot.program_id == WHIRLPOOL_PROGRAM_ID {
        match snapshot.extensions.whirlpool.as_ref() {
            Some(extras)
                if extras.tick_array0.is_some()
                    && extras.tick_array1.is_some()
                    && extras.tick_array2.is_some() => {}
            _ => return Err(PoolReadinessFailure::MissingWhirlpoolTickArrays),
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
enum PoolReadinessFailure {
    InvalidPoolId,
    MissingTemplate(SwapTemplateDirection),
    MissingAlt(SwapTemplateDirection),
    MissingQuotes,
    MissingWhirlpoolTickArrays,
    HydrationPending(Vec<HydrationComponent>),
}

impl fmt::Display for PoolReadinessFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolReadinessFailure::InvalidPoolId => write!(f, "invalid-pool-pubkey"),
            PoolReadinessFailure::MissingTemplate(direction) => write!(
                f,
                "missing-template direction={}",
                format_swap_direction(*direction)
            ),
            PoolReadinessFailure::MissingAlt(direction) => write!(
                f,
                "missing-alt-table direction={}",
                format_swap_direction(*direction)
            ),
            PoolReadinessFailure::MissingQuotes => write!(f, "missing-quotes"),
            PoolReadinessFailure::MissingWhirlpoolTickArrays => {
                write!(f, "missing-whirlpool-tick-arrays")
            }
            PoolReadinessFailure::HydrationPending(components) => {
                write!(
                    f,
                    "hydration-pending components={}",
                    format_hydration_components(components)
                )
            }
        }
    }
}

fn format_swap_direction(direction: SwapTemplateDirection) -> &'static str {
    match direction {
        SwapTemplateDirection::BaseIn => "base_in",
        SwapTemplateDirection::BaseOut => "base_out",
    }
}

fn format_hydration_components(components: &[HydrationComponent]) -> String {
    if components.is_empty() {
        return "none".to_string();
    }
    components
        .iter()
        .map(|component| component.as_str())
        .collect::<Vec<_>>()
        .join(",")
}
