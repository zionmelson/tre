use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use dashmap::{DashMap, DashSet};
use solana_sdk::pubkey::Pubkey;
use tokio::runtime::Builder as TokioRuntime;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::{
    adapters::{AdapterReadiness, ReadinessBlocker, ReadinessBlockerKind},
    AdapterBook, FlashLoanPlanner, PoolStateEvent, PoolStateUpdate, PoolStore, PoolStoreView,
    RpcRefresher, SnapshotPublisher, TemplateCache, TemplateCacheStats,
};
use crate::builder::{templates::SwapTemplateDirection, BuilderHandle};
use crate::config::AppConfig;
use crate::corecast::CoreCastClient;
use crate::hydration::logging::{
    describe_pending_components, program_log_target, PendingComponentSummary,
};
use crate::hydration::readiness::ReadinessRegistry;
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
    pub template_fresh_until: Option<Instant>,
}

impl TemplateReadiness {
    pub const fn missing() -> Self {
        Self {
            template_ready: false,
            template_fresh_until: None,
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
    readiness: ReadinessRegistry,
    template_epoch: Arc<AtomicU64>,
}

impl HydrationService {
    pub fn new(
        builder: BuilderHandle,
        config: Arc<AppConfig>,
        readiness: ReadinessRegistry,
    ) -> Self {
        let adapter_book = Arc::new(AdapterBook::new(config.clone(), builder.clone()));
        let pool_store = Arc::new(PoolStore::with_limits_and_policy(
            config.watcher.max_tracked_pools,
            config.watcher.min_ready_pools,
            config.watcher.queue_drop_policy,
        ));
        let rpc_refresher = Arc::new(RpcRefresher::new(config.clone()));
        let template_epoch = Arc::new(AtomicU64::new(builder.template_cache().epoch()));
        Self {
            builder,
            config,
            publish_interval: Duration::from_millis(25),
            adapter_book,
            pool_store,
            rpc_refresher,
            readiness,
            template_epoch,
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
        let readiness = self.readiness.clone();
        let adapter_book = self.adapter_book.clone();
        let template_epoch = self.template_epoch.clone();
        thread::Builder::new()
            .name("hydration_service".into())
            .spawn(move || {
                run_snapshot_loop(
                    stop,
                    publisher,
                    builder,
                    interval,
                    readiness,
                    adapter_book,
                    template_epoch,
                )
            })
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
        let readiness = self.readiness.clone();
        thread::Builder::new()
            .name("hydration_ingest".into())
            .spawn(move || {
                run_adapter_ingest_loop(stop, config, adapter_book, pool_store, builder, readiness)
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

    pub fn reset_template_cache(&self) -> u64 {
        let epoch = self.builder.reset_template_cache();
        self.template_epoch.store(epoch, Ordering::Relaxed);
        self.adapter_book.refresh_templates(epoch);
        epoch
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
        let slot_lag = self.config.watcher.pool_stale_slot_lag;
        let sweep = Duration::from_millis(self.config.watcher.pool_stale_sweep_ms.max(50));
        let builder = self.builder.clone();
        let readiness = self.readiness.clone();
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
                    slot_lag,
                    sweep,
                    builder,
                    readiness,
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
            let summary = stalled
                .iter()
                .map(PoolReadinessSample::short_summary)
                .collect::<Vec<_>>()
                .join(" | ");
            warn!(
                target = "hydration",
                samples = %summary,
                "pool readiness stalled; sample pools failing fast-lane checks"
            );
        }

        for sample in &stalled {
            telemetry::record_hydration_readiness_failure(
                &sample.program_id,
                sample.reason.label(),
            );
            let target = program_log_target(&sample.program_id);
            match (&sample.reason, &sample.pending) {
                (PoolReadinessFailure::HydrationPending(_), Some(summary)) => {
                    warn!(
                        target = target,
                        pool = %sample.pool_id,
                        slot = sample.slot,
                        lag_ms = summary.lag_ms,
                        lag_slots = summary.lag_slots,
                        components = %summary.component_names(),
                        details = %summary.detail_string(),
                        "hydration pending; fast-lane readiness stalled"
                    );
                }
                _ => {
                    warn!(
                        target = target,
                        pool = %sample.pool_id,
                        slot = sample.slot,
                        reason = sample.reason.label(),
                        "pool readiness blocked"
                    );
                }
            }
        }
    }

    fn sample_unready_pool_reasons(&self, limit: usize) -> Vec<PoolReadinessSample> {
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
            if let Err(reason) = evaluate_snapshot_readiness(&self.builder, &state.snapshot) {
                let pending = matches!(reason, PoolReadinessFailure::HydrationPending(_))
                    .then(|| describe_pending_components(&state.snapshot));
                samples.push(PoolReadinessSample {
                    program_id: state.program_id.clone(),
                    pool_id: state.pool_id.clone(),
                    slot: state.slot,
                    reason,
                    pending,
                });
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
    readiness: ReadinessRegistry,
    adapter_book: Arc<AdapterBook>,
    template_epoch: Arc<AtomicU64>,
) -> Result<()> {
    let mut waiting_logged = false;
    while !stop.load(Ordering::Relaxed) {
        readiness.expire_stale();
        let stats = builder.template_cache().stats();
        let previous_epoch = template_epoch.swap(stats.epoch, Ordering::Relaxed);
        if stats.epoch > previous_epoch {
            adapter_book.refresh_templates(stats.epoch);
        }
        let snapshot = HydrationSnapshot {
            issued_at: Instant::now(),
            template_stats: stats,
            flash_routes_prepared: builder.flash_loan_planner().prepared_route_count(),
        };
        let template_count = snapshot.template_stats.template_count;
        let flash_routes = snapshot.flash_routes_prepared;

        if !hydration_snapshot_ready(&snapshot) {
            if !waiting_logged {
                debug!(
                    templates = template_count,
                    flash_routes, "hydration snapshot withheld; waiting for full readiness"
                );
                waiting_logged = true;
            }
            thread::sleep(interval);
            continue;
        }

        waiting_logged = false;
        let overwritten = publisher.publish(snapshot);
        if overwritten {
            telemetry::record_hydration_snapshot_overwrite();
        }
        debug!(
            templates = template_count,
            flash_routes, overwritten, "published hydration snapshot"
        );
        thread::sleep(interval);
    }
    Ok(())
}

fn hydration_snapshot_ready(snapshot: &HydrationSnapshot) -> bool {
    let template_count = snapshot.template_stats.template_count;
    let flash_routes = snapshot.flash_routes_prepared;

    template_count > 0 && flash_routes > 0
}

fn run_adapter_ingest_loop(
    stop: Arc<AtomicBool>,
    config: Arc<AppConfig>,
    adapter_book: Arc<AdapterBook>,
    pool_store: Arc<PoolStore>,
    builder: BuilderHandle,
    readiness: ReadinessRegistry,
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
            readiness,
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
    readiness: ReadinessRegistry,
) -> Result<()> {
    let mut seq = 0u64;
    let mut retry = Duration::from_millis(250);
    let max_retry = Duration::from_secs(5);
    let update_cache = Arc::new(UpdateCache::default());
    let processor = Arc::new(ProcessContext::new(
        adapter_book.clone(),
        pool_store.clone(),
        builder,
        readiness.clone(),
    ));
    let hydrator_retry = HydrationRetry::new(stop.clone(), processor.clone(), update_cache.clone());

    while !stop.load(Ordering::Relaxed) {
        match client.pool_stream().await {
            Ok(mut stream) => {
                info!(target = "hydration", "CoreCast pool stream subscribed");
                retry = Duration::from_millis(250);
                while !stop.load(Ordering::Relaxed) {
                    match stream.next_message().await {
                        Ok(Some(message)) => {
                            if let Some(update) = PoolUpdate::from_corecast(seq, &message) {
                                seq = seq.wrapping_add(1);
                                if allowlist.contains(&TokenPair::new(
                                    &update.base_mint,
                                    &update.quote_mint,
                                )) {
                                    readiness.record_pool_update(&update);
                                    update_cache.store(&update);
                                    let needs_retry = processor.process(&update, true).needs_retry;
                                    if needs_retry {
                                        hydrator_retry.schedule(
                                            update.program_id.clone(),
                                            update.pool_id.clone(),
                                        );
                                    }
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

struct ProcessContext {
    adapter_book: Arc<AdapterBook>,
    pool_store: Arc<PoolStore>,
    builder: BuilderHandle,
    readiness: ReadinessRegistry,
}

impl ProcessContext {
    fn new(
        adapter_book: Arc<AdapterBook>,
        pool_store: Arc<PoolStore>,
        builder: BuilderHandle,
        readiness: ReadinessRegistry,
    ) -> Self {
        Self {
            adapter_book,
            pool_store,
            builder,
            readiness,
        }
    }

    fn process(&self, update: &PoolUpdate, emit_ingest_metrics: bool) -> ProcessResult {
        if let Some(adapter) = self.adapter_book.ingest(update) {
            if emit_ingest_metrics {
                telemetry::record_hydration_adapter_ingest(adapter);
            }
        }
        let mut snapshot = update.clone().into_snapshot();
        self.adapter_book.enrich_snapshot(&mut snapshot);
        if let Some(quotes) = self.adapter_book.snapshot_quotes(&mut snapshot) {
            snapshot.quotes = Some(quotes);
        }
        let pending = snapshot.hydration.pending_components();
        if !pending.is_empty() {
            self.adapter_book.warm_components(&snapshot, &pending);
        }
        let readiness = evaluate_snapshot_readiness(&self.builder, &snapshot);
        if let Err(reason) = &readiness {
            self.handle_readiness_failure(&snapshot, reason);
        }
        let ready = readiness.is_ok();
        self.pool_store.update(snapshot.clone(), ready);
        self.readiness.record_hydration_status(
            &snapshot.pool_id,
            snapshot.slot,
            &snapshot.hydration,
        );
        ProcessResult {
            needs_retry: !pending.is_empty(),
        }
    }
}

impl ProcessContext {
    fn handle_readiness_failure(&self, snapshot: &PoolSnapshot, failure: &PoolReadinessFailure) {
        let target = program_log_target(&snapshot.program_id);
        match failure {
            PoolReadinessFailure::InvalidPoolId => {
                debug!(
                    target = target,
                    pool = %snapshot.pool_id,
                    slot = snapshot.slot,
                    "pool readiness failed: invalid pool id"
                );
            }
            PoolReadinessFailure::MissingTemplate(direction) => {
                debug!(
                    target = target,
                    pool = %snapshot.pool_id,
                    slot = snapshot.slot,
                    direction = format_swap_direction(*direction),
                    "pool readiness failed: template missing"
                );
                if let Ok(pool_pubkey) = Pubkey::from_str(&snapshot.pool_id) {
                    let epoch = self.builder.template_cache().epoch();
                    self.adapter_book.refresh_pool_templates(
                        &snapshot.program_id,
                        pool_pubkey,
                        epoch,
                    );
                }
            }
            PoolReadinessFailure::MissingQuotes => {
                debug!(
                    target = target,
                    pool = %snapshot.pool_id,
                    slot = snapshot.slot,
                    "pool readiness failed: missing quotes"
                );
            }
            PoolReadinessFailure::MissingWhirlpoolTickArrays => {
                debug!(
                    target = target,
                    pool = %snapshot.pool_id,
                    slot = snapshot.slot,
                    "pool readiness failed: whirlpool tick arrays unavailable"
                );
            }
            PoolReadinessFailure::HydrationPending(_) => {
                let summary = describe_pending_components(snapshot);
                debug!(
                    target = target,
                    pool = %snapshot.pool_id,
                    slot = snapshot.slot,
                    lag_ms = summary.lag_ms,
                    lag_slots = summary.lag_slots,
                    components = %summary.component_names(),
                    details = %summary.detail_string(),
                    "pool readiness failed: hydration pending"
                );
            }
        }
    }
}

struct ProcessResult {
    needs_retry: bool,
}

#[derive(Default)]
struct UpdateCache {
    entries: DashMap<String, PoolUpdate>,
}

impl UpdateCache {
    fn store(&self, update: &PoolUpdate) {
        self.entries.insert(
            Self::key(&update.program_id, &update.pool_id),
            update.clone(),
        );
    }

    fn get(&self, program_id: &str, pool_id: &str) -> Option<PoolUpdate> {
        self.entries
            .get(&Self::key(program_id, pool_id))
            .map(|entry| entry.value().clone())
    }

    fn key(program_id: &str, pool_id: &str) -> String {
        format!("{program_id}:{pool_id}")
    }
}

#[derive(Clone)]
struct HydrationRetry {
    pending: Arc<DashSet<String>>,
    stop: Arc<AtomicBool>,
    processor: Arc<ProcessContext>,
    update_cache: Arc<UpdateCache>,
    base_delay: Duration,
    max_attempts: u32,
}

impl HydrationRetry {
    fn new(
        stop: Arc<AtomicBool>,
        processor: Arc<ProcessContext>,
        update_cache: Arc<UpdateCache>,
    ) -> Self {
        Self {
            pending: Arc::new(DashSet::new()),
            stop,
            processor,
            update_cache,
            base_delay: Duration::from_millis(100),
            max_attempts: 6,
        }
    }

    fn schedule(&self, program_id: String, pool_id: String) {
        let key = UpdateCache::key(&program_id, &pool_id);
        if !self.pending.insert(key.clone()) {
            return;
        }
        let runner = self.clone();
        tokio::spawn(async move {
            runner.run_worker(program_id, pool_id, key).await;
        });
    }

    async fn run_worker(&self, program_id: String, pool_id: String, key: String) {
        let mut attempt = 1;
        while attempt <= self.max_attempts && !self.stop.load(Ordering::Relaxed) {
            sleep(self.delay_for_attempt(attempt)).await;
            if self.stop.load(Ordering::Relaxed) {
                break;
            }
            let Some(update) = self.update_cache.get(&program_id, &pool_id) else {
                break;
            };
            let outcome = self.processor.process(&update, false);
            if !outcome.needs_retry {
                break;
            }
            attempt = attempt.saturating_add(1);
        }
        self.pending.remove(&key);
    }

    fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return self.base_delay;
        }
        let shift = attempt.saturating_sub(1).min(4);
        let millis = self.base_delay.as_millis() as u64 * (1u64 << shift);
        Duration::from_millis(millis.min(2_000))
    }
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
    slot_lag: Option<u64>,
    sweep: Duration,
    builder: BuilderHandle,
    readiness: ReadinessRegistry,
) -> Result<()> {
    while !stop.load(Ordering::Relaxed) {
        let flagged = pool_store.flag_stale_candidates(ttl, slot_lag);
        if flagged > 0 {
            debug!(
                target = "hydration",
                flagged, "maintenance sweep marked pools stale"
            );
        }
        let pending = pool_store.drain_refresh_queue();
        if pending.is_empty() {
            thread::sleep(sweep);
            continue;
        }
        debug!(
            target = "hydration",
            queued = pending.len(),
            "maintenance dispatching rpc refreshes"
        );
        for (state, min_slot) in pending {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let protocol = state.program_id.clone();
            debug!(
                target = "hydration",
                program = %protocol,
                pool = %state.pool_id,
                snapshot_slot = state.slot,
                min_slot,
                "issuing rpc refresh"
            );
            if let Some(mut refreshed) = refresher.refresh(&state.snapshot) {
                if refreshed.slot < min_slot {
                    pool_store.queue_refresh(&state.pool_id, min_slot);
                    telemetry::record_hydration_rpc_refresh_failure(&protocol);
                    debug!(
                        target = "hydration",
                        program = %protocol,
                        pool = %state.pool_id,
                        refreshed_slot = refreshed.slot,
                        min_slot,
                        "rpc refresh slot below requirement; requeued"
                    );
                    continue;
                }
                if let Some(quotes) = adapter_book.snapshot_quotes(&mut refreshed) {
                    refreshed.quotes = Some(quotes);
                }
                let ready = snapshot_ready_for_fastlane(&builder, &refreshed);
                let refreshed_slot = refreshed.slot;
                pool_store.update(refreshed.clone(), ready);
                readiness.record_hydration_status(
                    &refreshed.pool_id,
                    refreshed.slot,
                    &refreshed.hydration,
                );
                telemetry::record_hydration_rpc_refresh(&protocol);
                debug!(
                    target = "hydration",
                    program = %protocol,
                    pool = %state.pool_id,
                    refreshed_slot,
                    ready,
                    "rpc refresh applied"
                );
            } else {
                telemetry::record_hydration_rpc_refresh_failure(&protocol);
                pool_store.queue_refresh(&state.pool_id, min_slot);
                telemetry::record_hydration_stale_pools(1);
                debug!(
                    target = "hydration",
                    program = %protocol,
                    pool = %state.pool_id,
                    min_slot,
                    "rpc refresh failed; requeued"
                );
            }
        }
        thread::sleep(sweep);
    }
    Ok(())
}

fn snapshot_ready_for_fastlane(builder: &BuilderHandle, snapshot: &PoolSnapshot) -> bool {
    evaluate_snapshot_readiness(builder, snapshot).is_ok()
}

fn evaluate_snapshot_readiness(
    builder: &BuilderHandle,
    snapshot: &PoolSnapshot,
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

impl PoolReadinessFailure {
    fn label(&self) -> &'static str {
        match self {
            PoolReadinessFailure::InvalidPoolId => "invalid_pool_id",
            PoolReadinessFailure::MissingTemplate(direction) => match direction {
                SwapTemplateDirection::BaseIn => "missing_template_base_in",
                SwapTemplateDirection::BaseOut => "missing_template_base_out",
            },
            PoolReadinessFailure::MissingQuotes => "missing_quotes",
            PoolReadinessFailure::MissingWhirlpoolTickArrays => "missing_whirlpool_tick_arrays",
            PoolReadinessFailure::HydrationPending(_) => "hydration_pending",
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

#[derive(Debug, Clone)]
struct PoolReadinessSample {
    program_id: String,
    pool_id: String,
    slot: u64,
    reason: PoolReadinessFailure,
    pending: Option<PendingComponentSummary>,
}

impl PoolReadinessSample {
    fn short_summary(&self) -> String {
        match &self.pending {
            Some(summary) if !summary.is_empty() => format!(
                "{}:{} {} lag_ms={} components={}",
                self.program_id,
                self.pool_id,
                self.reason,
                summary.lag_ms,
                summary.component_names()
            ),
            _ => format!("{}:{} {}", self.program_id, self.pool_id, self.reason),
        }
    }
}
