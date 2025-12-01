use std::{
    convert::Infallible,
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bs58;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use once_cell::sync::OnceCell;
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use tokio::{runtime::Handle, task::JoinHandle};
use tracing::{info, warn};

const PROMETHEUS_ENV: &str = "PROMETHEUS_METRICS_ADDR";
const PROMETHEUS_METRIC_PREFIX_ALLOWLIST: &[&str] = &["hydration_", "watcher_"];

static TELEMETRY: OnceCell<TelemetryState> = OnceCell::new();
static HTTP_SERVER: OnceCell<Option<JoinHandle<()>>> = OnceCell::new();

struct TelemetryState {
    registry: Arc<Registry>,
    detection_latency: Histogram,
    execution_latency: Histogram,
    execution_attempts: Family<Stage3StatusLabels, Counter>,
    execution_tip_lamports: Gauge,
    execution_bundle_hash: Gauge,
    execution_tip_multiplier_milli: Gauge,
    bundle_result_latency: Histogram,
    bundle_result_status: Family<BundleResultLabels, Counter>,
    stage2_plan_events: Family<Stage2PlanLabels, Counter>,
    stage2_pool_blocks: Family<Stage2PoolLabels, Counter>,
    execution_retry_events: Family<Stage3RetryLabels, Counter>,
    hydration_template_age_ms: Gauge,
    hydration_alt_age_ms: Gauge,
    hydration_backlog_gauge: Gauge,
    hydration_template_cache_entries: Gauge,
    hydration_alt_ready_tables: Gauge,
    hydration_flash_routes_prepared: Gauge,
    hydration_adapter_ingest: Family<HydrationAdapterLabels, Counter>,
    hydration_rpc_refresh_success: Family<HydrationAdapterLabels, Counter>,
    hydration_rpc_refresh_failure: Family<HydrationAdapterLabels, Counter>,
    hydration_stale_pools: Counter,
    hydration_queue_depth_pct: Gauge,
    hydration_queue_depth_items: Gauge,
    hydration_queue_drop_events: Counter,
    graph_pool_count: Gauge,
    graph_cycle_count: Gauge,
    graph_snapshot_latency: Histogram,
    watcher_update_age: Family<WatcherUpdateLabels, Histogram>,
    watcher_cache_size: Gauge,
    watcher_cycle_pending: Gauge,
    watcher_update_backlog: Gauge,
}

impl TelemetryState {
    fn new() -> Self {
        let mut registry = Registry::default();

        let detection_latency = Histogram::new(exponential_buckets(0.1, 2.0, 20));
        registry.register(
            "bundle_detection_latency_ms",
            "Elapsed milliseconds between opportunity detection and Stage2 submission",
            detection_latency.clone(),
        );

        let execution_latency = Histogram::new(exponential_buckets(0.1, 2.0, 20));
        registry.register(
            "bundle_execution_latency_ms",
            "Elapsed milliseconds between opportunity detection and fast-lane bundle submission",
            execution_latency.clone(),
        );
        let execution_attempts = Family::default();
        registry.register(
            "bundle_execution_attempts_total",
            "Fast-lane bundle submission attempts partitioned by result",
            execution_attempts.clone(),
        );

        let execution_tip_lamports = Gauge::default();
        registry.register(
            "bundle_execution_tip_lamports",
            "Tip lamports observed in the last successful fast-lane bundle submission",
            execution_tip_lamports.clone(),
        );

        let execution_bundle_hash = Gauge::default();
        registry.register(
            "bundle_execution_last_bundle_hash",
            "Lower 63 bits of the most recent bundle_id (decoded as base58) for correlation/debugging",
            execution_bundle_hash.clone(),
        );

        let execution_tip_multiplier_milli = Gauge::default();
        registry.register(
            "bundle_execution_tip_multiplier_milli",
            "Tip multiplier applied to the last bundle (scaled by 1e3)",
            execution_tip_multiplier_milli.clone(),
        );

        let bundle_result_latency = Histogram::new(exponential_buckets(0.1, 2.0, 20));
        registry.register(
            "bundle_result_latency_ms",
            "Elapsed milliseconds between opportunity detection and the observed bundle result",
            bundle_result_latency.clone(),
        );

        let bundle_result_status = Family::default();
        registry.register(
            "bundle_result_events_total",
            "Bundle lifecycle events grouped by result",
            bundle_result_status.clone(),
        );

        let stage2_plan_events = Family::default();
        registry.register(
            "stage2_plan_events_total",
            "Stage2 bundle planning outcomes grouped by event",
            stage2_plan_events.clone(),
        );

        let stage2_pool_blocks = Family::default();
        registry.register(
            "stage2_pool_blockers_total",
            "Stage2 readiness blocks grouped by pool id",
            stage2_pool_blocks.clone(),
        );

        let execution_retry_events = Family::default();
        registry.register(
            "bundle_execution_retry_events_total",
            "Fast-lane submission retry lifecycle grouped by event",
            execution_retry_events.clone(),
        );

        let hydration_template_age_ms = Gauge::default();
        registry.register(
            "hydration_template_age_ms",
            "Milliseconds since the template cache last refreshed",
            hydration_template_age_ms.clone(),
        );

        let hydration_alt_age_ms = Gauge::default();
        registry.register(
            "hydration_alt_age_ms",
            "Milliseconds since the ALT manager last refreshed",
            hydration_alt_age_ms.clone(),
        );

        let hydration_backlog_gauge = Gauge::default();
        registry.register(
            "hydration_backlog_items",
            "Pending hydration jobs/backlog queued by the ALT manager",
            hydration_backlog_gauge.clone(),
        );

        let hydration_template_cache_entries = Gauge::default();
        registry.register(
            "hydration_template_cache_entries",
            "Number of hydrated instruction templates currently cached",
            hydration_template_cache_entries.clone(),
        );

        let hydration_alt_ready_tables = Gauge::default();
        registry.register(
            "hydration_alt_ready_tables",
            "Count of ALT tables ready for immediate use",
            hydration_alt_ready_tables.clone(),
        );

        let hydration_flash_routes_prepared = Gauge::default();
        registry.register(
            "hydration_flash_routes_prepared",
            "Flash-loan routes currently pre-built by the hydration service",
            hydration_flash_routes_prepared.clone(),
        );

        let hydration_adapter_ingest = Family::default();
        registry.register(
            "hydration_adapter_ingest_total",
            "Pool updates successfully applied per protocol adapter",
            hydration_adapter_ingest.clone(),
        );

        let hydration_rpc_refresh_success = Family::default();
        registry.register(
            "hydration_rpc_refresh_total",
            "Successful RPC-based pool refresh events grouped by protocol",
            hydration_rpc_refresh_success.clone(),
        );

        let hydration_rpc_refresh_failure = Family::default();
        registry.register(
            "hydration_rpc_refresh_failure_total",
            "Failed RPC-based pool refresh events grouped by protocol",
            hydration_rpc_refresh_failure.clone(),
        );

        let hydration_stale_pools = Counter::default();
        registry.register(
            "hydration_stale_pool_events_total",
            "Pool cache entries evicted due to exceeding the freshness budget",
            hydration_stale_pools.clone(),
        );

        let hydration_queue_depth_pct = Gauge::default();
        registry.register(
            "hydration_queue_depth_pct",
            "Percentage (0-100) of the hydrator broadcast queue currently occupied",
            hydration_queue_depth_pct.clone(),
        );

        let hydration_queue_depth_items = Gauge::default();
        registry.register(
            "hydration_queue_depth_items",
            "Approximate count of pool updates buffered in the hydrator broadcast queue",
            hydration_queue_depth_items.clone(),
        );

        let hydration_queue_drop_events = Counter::default();
        registry.register(
            "hydration_queue_drop_events_total",
            "Number of hydrator broadcast updates dropped due to backpressure",
            hydration_queue_drop_events.clone(),
        );

        let graph_pool_count = Gauge::default();
        registry.register(
            "graph_pool_count",
            "Number of pools currently indexed by the graph service",
            graph_pool_count.clone(),
        );

        let graph_cycle_count = Gauge::default();
        registry.register(
            "graph_cycle_count",
            "Number of profitable arbitrage cycles currently tracked",
            graph_cycle_count.clone(),
        );

        let graph_snapshot_latency = Histogram::new(exponential_buckets(0.05, 2.0, 20));
        registry.register(
            "graph_snapshot_latency_ms",
            "Milliseconds between pool snapshot creation and graph ingestion",
            graph_snapshot_latency.clone(),
        );

        let watcher_update_age =
            Family::new_with_constructor(watcher_update_histogram_constructor as fn() -> Histogram);
        registry.register(
            "watcher_update_age_ms",
            "Milliseconds between hydrator publish and watcher consumption grouped by adapter",
            watcher_update_age.clone(),
        );

        let watcher_cache_size = Gauge::default();
        registry.register(
            "watcher_cache_entries",
            "Number of pool snapshots cached by the watcher lane",
            watcher_cache_size.clone(),
        );

        let watcher_cycle_pending = Gauge::default();
        registry.register(
            "watcher_cycle_scan_pending",
            "Boolean gauge indicating whether a cycle scan is queued (1 = pending)",
            watcher_cycle_pending.clone(),
        );

        let watcher_update_backlog = Gauge::default();
        registry.register(
            "watcher_pool_update_backlog_pct",
            "Percentage (0-100) of the watcher pool-update channel currently filled",
            watcher_update_backlog.clone(),
        );

        Self {
            registry: Arc::new(registry),
            detection_latency,
            execution_latency,
            execution_attempts,
            execution_tip_lamports,
            execution_bundle_hash,
            execution_tip_multiplier_milli,
            bundle_result_latency,
            bundle_result_status,
            stage2_plan_events,
            stage2_pool_blocks,
            execution_retry_events,
            hydration_template_age_ms,
            hydration_alt_age_ms,
            hydration_backlog_gauge,
            hydration_template_cache_entries,
            hydration_alt_ready_tables,
            hydration_flash_routes_prepared,
            hydration_adapter_ingest,
            hydration_rpc_refresh_success,
            hydration_rpc_refresh_failure,
            hydration_stale_pools,
            hydration_queue_depth_pct,
            hydration_queue_depth_items,
            hydration_queue_drop_events,
            graph_pool_count,
            graph_cycle_count,
            graph_snapshot_latency,
            watcher_update_age,
            watcher_cache_size,
            watcher_cycle_pending,
            watcher_update_backlog,
        }
    }
}

pub async fn init_from_env() -> Result<()> {
    let handle = Handle::try_current()
        .context("telemetry::init_from_env must run inside a Tokio runtime")?;
    let state = TELEMETRY.get_or_init(TelemetryState::new);
    HTTP_SERVER.get_or_init(|| {
        prometheus_bind_addr().map(|addr| {
            info!(%addr, "prometheus metrics exporter listening");
            spawn_http_server(state.registry.clone(), addr, handle.clone())
        })
    });
    Ok(())
}

fn prometheus_bind_addr() -> Option<SocketAddr> {
    match env::var(PROMETHEUS_ENV) {
        Ok(raw) => match raw.parse::<SocketAddr>() {
            Ok(addr) => Some(addr),
            Err(err) => {
                warn!(value = %raw, error = %err, "invalid PROMETHEUS_METRICS_ADDR, metrics endpoint disabled");
                None
            }
        },
        Err(_) => None,
    }
}

fn spawn_http_server(registry: Arc<Registry>, addr: SocketAddr, handle: Handle) -> JoinHandle<()> {
    handle.spawn(async move {
        let make_service = make_service_fn(move |_conn| {
            let registry = registry.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |_req: Request<Body>| {
                    let registry = registry.clone();
                    async move { Ok::<_, Infallible>(metrics_response(registry)) }
                }))
            }
        });

        if let Err(err) = Server::bind(&addr).serve(make_service).await {
            warn!(error = %err, "metrics HTTP server stopped");
        }
    })
}

fn metrics_response(registry: Arc<Registry>) -> Response<Body> {
    match encode_filtered_metrics(registry.as_ref()) {
        Ok(buffer) => Response::builder()
            .status(200)
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(Body::from(buffer))
            .unwrap_or_else(|builder_err| Response::new(Body::from(builder_err.to_string()))),
        Err(err) => {
            warn!(error = %err, "failed to encode metrics response");
            Response::builder()
                .status(500)
                .body(Body::from("encoding error"))
                .unwrap_or_else(|builder_err| Response::new(Body::from(builder_err.to_string())))
        }
    }
}

fn encode_filtered_metrics(registry: &Registry) -> Result<String, std::fmt::Error> {
    let mut buffer = String::new();
    encode(&mut buffer, registry)?;
    Ok(filter_metrics_by_prefix(&buffer))
}

// Trims the encoded output down to hydration/watcher metrics so Prometheus stays uncluttered.
fn filter_metrics_by_prefix(input: &str) -> String {
    let mut filtered = String::new();
    let mut keep_current_block = false;

    for line in input.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let metric_name = rest.split_whitespace().next().unwrap_or("");
            keep_current_block = should_export_metric(metric_name);
            if keep_current_block {
                filtered.push_str(line);
                filtered.push('\n');
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let metric_name = rest.split_whitespace().next().unwrap_or("");
            keep_current_block = should_export_metric(metric_name);
            if keep_current_block {
                filtered.push_str(line);
                filtered.push('\n');
            }
            continue;
        }

        if line.is_empty() {
            if keep_current_block {
                filtered.push('\n');
            }
            continue;
        }

        if keep_current_block {
            filtered.push_str(line);
            filtered.push('\n');
        }
    }

    filtered
}

fn should_export_metric(metric_name: &str) -> bool {
    PROMETHEUS_METRIC_PREFIX_ALLOWLIST
        .iter()
        .any(|prefix| metric_name.starts_with(prefix))
}

fn watcher_update_histogram_constructor() -> Histogram {
    Histogram::new(exponential_buckets(0.1, 2.0, 20))
}

pub fn snapshot_metrics() -> Option<String> {
    TELEMETRY
        .get()
        .and_then(|state| encode_filtered_metrics(state.registry.as_ref()).ok())
}

pub fn record_detection_latency(duration: Duration) {
    if let Some(state) = TELEMETRY.get() {
        state
            .detection_latency
            .observe(duration.as_secs_f64() * 1_000.0);
    }
}

pub fn record_stage3_latency(duration: Duration) {
    if let Some(state) = TELEMETRY.get() {
        state
            .execution_latency
            .observe(duration.as_secs_f64() * 1_000.0);
    }
}

pub fn record_tip_multiplier(multiplier: f64) {
    if let Some(state) = TELEMETRY.get() {
        if multiplier.is_finite() {
            let scaled = (multiplier * 1_000.0).round();
            let clamped = scaled.clamp(i64::MIN as f64, i64::MAX as f64) as i64;
            state.execution_tip_multiplier_milli.set(clamped);
        }
    }
}

pub enum Stage3SubmissionStatus {
    Success,
    Failure,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct Stage3StatusLabels {
    status: &'static str,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct BundleResultLabels {
    status: &'static str,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct Stage3RetryLabels {
    event: &'static str,
    reason: &'static str,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct Stage2PlanLabels {
    event: &'static str,
    reason: &'static str,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct Stage2PoolLabels {
    pool_id: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct HydrationAdapterLabels {
    adapter: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct WatcherUpdateLabels {
    adapter: String,
}

pub enum Stage3RetryEvent {
    RetryQueued(Stage3RetryReason),
    RetryFlushed,
    RetriesExhausted(Stage3RetryReason),
    FailureRecorded(Stage3RetryReason),
}

pub enum Stage2PlanEvent {
    PlanFailed,
    PrepareFailed,
}

impl Stage2PlanEvent {
    fn event(&self) -> &'static str {
        "failure"
    }

    fn reason(&self) -> &'static str {
        match self {
            Stage2PlanEvent::PlanFailed => "plan_failed",
            Stage2PlanEvent::PrepareFailed => "prepare_failed",
        }
    }
}

#[derive(Clone, Copy)]
pub enum Stage3RetryReason {
    Context,
    Sign,
    Submit,
}

impl Stage3RetryReason {
    fn label(&self) -> &'static str {
        match self {
            Stage3RetryReason::Context => "context",
            Stage3RetryReason::Sign => "sign",
            Stage3RetryReason::Submit => "submit",
        }
    }
}

impl Stage3RetryEvent {
    fn event(&self) -> &'static str {
        match self {
            Stage3RetryEvent::RetryQueued(_) => "retry_queued",
            Stage3RetryEvent::RetryFlushed => "retry_flushed",
            Stage3RetryEvent::RetriesExhausted(_) => "retries_exhausted",
            Stage3RetryEvent::FailureRecorded(_) => "failure_recorded",
        }
    }

    fn reason(&self) -> &'static str {
        match self {
            Stage3RetryEvent::RetryQueued(reason)
            | Stage3RetryEvent::RetriesExhausted(reason)
            | Stage3RetryEvent::FailureRecorded(reason) => reason.label(),
            Stage3RetryEvent::RetryFlushed => "retry",
        }
    }
}

pub fn record_stage3_retry_event(event: Stage3RetryEvent) {
    if let Some(state) = TELEMETRY.get() {
        let labels = Stage3RetryLabels {
            event: event.event(),
            reason: event.reason(),
        };
        state.execution_retry_events.get_or_create(&labels).inc();
    }
}

pub fn record_stage2_plan_event(event: Stage2PlanEvent) {
    if let Some(state) = TELEMETRY.get() {
        let labels = Stage2PlanLabels {
            event: event.event(),
            reason: event.reason(),
        };
        state.stage2_plan_events.get_or_create(&labels).inc();
    }
}

pub fn record_stage3_outcome(
    status: Stage3SubmissionStatus,
    bundle_id: Option<&str>,
    tip_lamports: Option<u64>,
) {
    if let Some(state) = TELEMETRY.get() {
        let status_label = match status {
            Stage3SubmissionStatus::Success => "success",
            Stage3SubmissionStatus::Failure => "failure",
        };
        let labels = Stage3StatusLabels {
            status: status_label,
        };
        state.execution_attempts.get_or_create(&labels).inc();

        if matches!(status, Stage3SubmissionStatus::Success) {
            if let Some(tip) = tip_lamports {
                state.execution_tip_lamports.set(clamp_u64_to_i64(tip));
            }
            if let Some(id) = bundle_id {
                if let Some(hash) = bundle_id_hash(id) {
                    state.execution_bundle_hash.set(hash);
                }
            }
        }
    }
}

pub fn record_hydration_template_refresh(last_refresh_unix_ms: u64) {
    if let Some(state) = TELEMETRY.get() {
        if let Some(age_ms) = age_since_unix_ms(last_refresh_unix_ms) {
            state
                .hydration_template_age_ms
                .set(clamp_u64_to_i64(age_ms));
        }
    }
}

pub fn record_hydration_alt_refresh(last_refresh_unix_ms: u64) {
    if let Some(state) = TELEMETRY.get() {
        if let Some(age_ms) = age_since_unix_ms(last_refresh_unix_ms) {
            state.hydration_alt_age_ms.set(clamp_u64_to_i64(age_ms));
        }
    }
}

pub fn record_hydration_backlog(pending_jobs: u64) {
    if let Some(state) = TELEMETRY.get() {
        state
            .hydration_backlog_gauge
            .set(clamp_u64_to_i64(pending_jobs));
    }
}

pub fn record_hydration_template_count(template_count: u64) {
    if let Some(state) = TELEMETRY.get() {
        state
            .hydration_template_cache_entries
            .set(clamp_u64_to_i64(template_count));
    }
}

pub fn record_hydration_alt_ready_tables(ready_tables: u64) {
    if let Some(state) = TELEMETRY.get() {
        state
            .hydration_alt_ready_tables
            .set(clamp_u64_to_i64(ready_tables));
    }
}

pub fn record_hydration_flash_routes(prepared_routes: u64) {
    if let Some(state) = TELEMETRY.get() {
        state
            .hydration_flash_routes_prepared
            .set(clamp_u64_to_i64(prepared_routes));
    }
}

pub fn record_graph_pool_count(count: usize) {
    if let Some(state) = TELEMETRY.get() {
        state.graph_pool_count.set(clamp_u64_to_i64(count as u64));
    }
}

pub fn record_graph_cycle_count(count: usize) {
    if let Some(state) = TELEMETRY.get() {
        state.graph_cycle_count.set(clamp_u64_to_i64(count as u64));
    }
}

pub fn record_graph_snapshot_latency(latency: Duration) {
    if let Some(state) = TELEMETRY.get() {
        state
            .graph_snapshot_latency
            .observe(latency.as_secs_f64() * 1_000.0);
    }
}

pub fn record_hydration_adapter_ingest(adapter: &str) {
    if let Some(state) = TELEMETRY.get() {
        let labels = HydrationAdapterLabels {
            adapter: adapter.to_string(),
        };
        state.hydration_adapter_ingest.get_or_create(&labels).inc();
    }
}

pub fn record_hydration_rpc_refresh(adapter: &str) {
    if let Some(state) = TELEMETRY.get() {
        let labels = HydrationAdapterLabels {
            adapter: adapter.to_string(),
        };
        state
            .hydration_rpc_refresh_success
            .get_or_create(&labels)
            .inc();
    }
}

pub fn record_hydration_rpc_refresh_failure(adapter: &str) {
    if let Some(state) = TELEMETRY.get() {
        let labels = HydrationAdapterLabels {
            adapter: adapter.to_string(),
        };
        state
            .hydration_rpc_refresh_failure
            .get_or_create(&labels)
            .inc();
    }
}

pub fn record_hydration_stale_pools(evicted: u64) {
    if evicted == 0 {
        return;
    }
    if let Some(state) = TELEMETRY.get() {
        state.hydration_stale_pools.inc_by(evicted);
    }
}

pub fn record_hydration_queue_depth(len: usize, capacity: usize) {
    if let Some(state) = TELEMETRY.get() {
        let pct = if capacity == 0 {
            0.0
        } else {
            (len as f64 / capacity as f64 * 100.0).clamp(0.0, 100.0)
        };
        state.hydration_queue_depth_pct.set(pct.round() as i64);
        state
            .hydration_queue_depth_items
            .set(clamp_u64_to_i64(len as u64));
    }
}

pub fn record_hydration_queue_drop() {
    if let Some(state) = TELEMETRY.get() {
        state.hydration_queue_drop_events.inc();
    }
}

pub fn record_watcher_update_age(adapter: &str, latency: Duration) {
    if let Some(state) = TELEMETRY.get() {
        let labels = WatcherUpdateLabels {
            adapter: adapter.to_string(),
        };
        state
            .watcher_update_age
            .get_or_create(&labels)
            .observe(latency.as_secs_f64() * 1_000.0);
    }
}

pub fn record_watcher_cache_depth(cache_len: usize) {
    if let Some(state) = TELEMETRY.get() {
        state
            .watcher_cache_size
            .set(clamp_u64_to_i64(cache_len as u64));
    }
}

pub fn record_watcher_cycle_pending(pending: bool) {
    if let Some(state) = TELEMETRY.get() {
        state.watcher_cycle_pending.set(if pending { 1 } else { 0 });
    }
}

pub fn record_watcher_update_backlog(len: usize, capacity: usize) {
    if let Some(state) = TELEMETRY.get() {
        let pct = if capacity == 0 {
            0.0
        } else {
            (len as f64 / capacity as f64 * 100.0).clamp(0.0, 100.0)
        };
        state.watcher_update_backlog.set(pct.round() as i64);
    }
}

fn clamp_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn age_since_unix_ms(last_refresh_unix_ms: u64) -> Option<u64> {
    if last_refresh_unix_ms == 0 {
        return None;
    }
    let now_ms = unix_timestamp_millis()?;
    now_ms.checked_sub(last_refresh_unix_ms)
}

fn unix_timestamp_millis() -> Option<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(duration.as_millis()).ok()
}

fn bundle_id_hash(bundle_id: &str) -> Option<i64> {
    let decoded = bs58::decode(bundle_id).into_vec().ok()?;
    let mut buf = [0u8; 8];
    for (idx, byte) in decoded.iter().take(8).enumerate() {
        buf[idx] = *byte;
    }
    let raw = u64::from_le_bytes(buf);
    Some(clamp_u64_to_i64(raw))
}

pub enum BundleResultStatus {
    Accepted,
    Rejected,
    Processed,
    Finalized,
    Dropped,
    AuctionWon,
    AuctionLost,
    Expired,
}

impl BundleResultStatus {
    fn label(&self) -> &'static str {
        match self {
            BundleResultStatus::Accepted => "accepted",
            BundleResultStatus::Rejected => "rejected",
            BundleResultStatus::Processed => "processed",
            BundleResultStatus::Finalized => "finalized",
            BundleResultStatus::Dropped => "dropped",
            BundleResultStatus::AuctionWon => "auction_won",
            BundleResultStatus::AuctionLost => "auction_lost",
            BundleResultStatus::Expired => "expired",
        }
    }
}

pub fn record_bundle_result(status: BundleResultStatus, latency: Duration) {
    if let Some(state) = TELEMETRY.get() {
        state
            .bundle_result_latency
            .observe(latency.as_secs_f64() * 1_000.0);
        let labels = BundleResultLabels {
            status: status.label(),
        };
        state.bundle_result_status.get_or_create(&labels).inc();
    }
}

pub fn record_stage2_pool_block(pool_id: &str) {
    if let Some(state) = TELEMETRY.get() {
        let labels = Stage2PoolLabels {
            pool_id: pool_id.to_string(),
        };
        state.stage2_pool_blocks.get_or_create(&labels).inc();
    }
}

#[cfg(test)]
mod tests {
    use super::filter_metrics_by_prefix;

    #[test]
    fn filters_non_allowlisted_metrics() {
        let input = "# HELP hydration_queue_depth_pct Hydration queue depth\n# TYPE hydration_queue_depth_pct gauge\nhydration_queue_depth_pct 42\n\n# HELP bundle_detection_latency_ms Stage 2 latency\n# TYPE bundle_detection_latency_ms histogram\nbundle_detection_latency_ms_bucket{le=\"0.1\"} 0\n";
        let filtered = filter_metrics_by_prefix(input);
        assert!(filtered.contains("hydration_queue_depth_pct 42"));
        assert!(!filtered.contains("bundle_detection_latency_ms"));
    }
}
