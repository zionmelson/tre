use std::{
    convert::Infallible,
    env,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
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
const PROMETHEUS_METRIC_PREFIX_ALLOWLIST: &[&str] = &[
    "template_",
    "corecast_",
    "bundle_",
    "pool_",
];

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
    execution_retry_events: Family<Stage3RetryLabels, Counter>,
    template_build_total: Counter,
    template_cache_entries: Gauge,
    corecast_delta_lag_ms: Gauge,
    corecast_stream_reconnects: Family<CorecastStreamLabels, Counter>,
    pool_bootstrap_total: Family<PoolBootstrapLabels, Counter>,
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

        let execution_retry_events = Family::default();
        registry.register(
            "bundle_execution_retry_events_total",
            "Fast-lane submission retry lifecycle grouped by event",
            execution_retry_events.clone(),
        );

        let template_build_total = Counter::default();
        registry.register(
            "template_build_total",
            "Instruction templates hydrated since startup",
            template_build_total.clone(),
        );

        let template_cache_entries = Gauge::default();
        registry.register(
            "template_cache_entries",
            "Active instruction templates currently cached",
            template_cache_entries.clone(),
        );

        let corecast_delta_lag_ms = Gauge::default();
        registry.register(
            "corecast_delta_lag_ms",
            "Milliseconds elapsed since the most recent CoreCast delta was applied",
            corecast_delta_lag_ms.clone(),
        );

        let corecast_stream_reconnects = Family::default();
        registry.register(
            "corecast_stream_reconnects_total",
            "CoreCast stream reconnect attempts grouped by stream type",
            corecast_stream_reconnects.clone(),
        );

        let pool_bootstrap_total = Family::default();
        registry.register(
            "pool_bootstrap_total",
            "One-time pool bootstrap results grouped by status",
            pool_bootstrap_total.clone(),
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
            execution_retry_events,
            template_build_total,
            template_cache_entries,
            corecast_delta_lag_ms,
            corecast_stream_reconnects,
            pool_bootstrap_total,
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

// Exports only the allowlisted metric prefixes so Prometheus stays focused on runtime-critical data.
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

    Failure,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]

    fn reason(&self) -> &'static str {
        match self {
            TransferHookEvent::ResolveSuccess => "success",
            TransferHookEvent::ResolveFailure(reason) => reason.label(),
        }
    }
}

impl TransferHookFailureReason {
    fn label(&self) -> &'static str {
        match self {
            TransferHookFailureReason::MintOwnerLookup => "mint_owner_lookup",
            TransferHookFailureReason::ValidationAccountMissing => "validation_missing",
            TransferHookFailureReason::AccountFetch => "account_fetch",
            TransferHookFailureReason::ExtraAccountBuild => "extra_account_build",
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

#[derive(Clone, Copy)]
pub enum CorecastStreamKind {
    Pool,
    Trade,
}

impl CorecastStreamKind {
    fn label(&self) -> &'static str {
        match self {
            CorecastStreamKind::Pool => "pool",
            CorecastStreamKind::Trade => "trade",
        }
    }
}

#[derive(Clone, Copy)]
pub enum PoolBootstrapStatus {
    Success,
    Failure,
}

impl PoolBootstrapStatus {
    fn label(&self) -> &'static str {
        match self {
            PoolBootstrapStatus::Success => "success",
            PoolBootstrapStatus::Failure => "failure",
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

pub fn record_corecast_delta_lag(lag: Duration) {
    if let Some(state) = TELEMETRY.get() {
        let millis = lag.as_secs_f64() * 1_000.0;
        state.corecast_delta_lag_ms.set(millis.round() as i64);
    }
}

pub fn record_corecast_stream_reconnect(kind: CorecastStreamKind) {
    if let Some(state) = TELEMETRY.get() {
        let labels = CorecastStreamLabels {
            stream: kind.label(),
        };
        state
            .corecast_stream_reconnects
            .get_or_create(&labels)
            .inc();
    }
}

pub fn record_pool_bootstrap(status: PoolBootstrapStatus, count: u64) {
    if count == 0 {
        return;
    }
    if let Some(state) = TELEMETRY.get() {
        let labels = PoolBootstrapLabels {
            status: status.label(),
        };
        let counter = state.pool_bootstrap_total.get_or_create(&labels);
        for _ in 0..count {
            counter.inc();
        }
    }
}

pub fn record_template_build() {
    if let Some(state) = TELEMETRY.get() {
        state.template_build_total.inc();
    }
}

pub fn record_template_cache_entries(entry_count: usize) {
    if let Some(state) = TELEMETRY.get() {
        state
            .template_cache_entries
            .set(clamp_u64_to_i64(entry_count as u64));
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

pub fn record_transfer_hook_event(event: TransferHookEvent) {
    let _ = event.event();
    let _ = event.reason();
}
fn clamp_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
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

#[cfg(test)]
mod tests {
    use super::filter_metrics_by_prefix;

    #[test]
    fn filters_non_allowlisted_metrics() {
        let input = "# HELP corecast_delta_lag_ms CoreCast delta lag\n# TYPE corecast_delta_lag_ms gauge\ncorecast_delta_lag_ms 42\n\n# HELP bundle_detection_latency_ms Stage 2 latency\n# TYPE bundle_detection_latency_ms histogram\nbundle_detection_latency_ms_bucket{le=\"0.1\"} 0\n";
        let filtered = filter_metrics_by_prefix(input);
        assert!(filtered.contains("corecast_delta_lag_ms 42"));
        assert!(!filtered.contains("bundle_detection_latency_ms"));
    }
}
