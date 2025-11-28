use std::{convert::Infallible, env, net::SocketAddr, sync::Arc, time::Duration};

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

static TELEMETRY: OnceCell<TelemetryState> = OnceCell::new();
static HTTP_SERVER: OnceCell<Option<JoinHandle<()>>> = OnceCell::new();

struct TelemetryState {
    registry: Arc<Registry>,
    path_latency: Histogram,
    candidate_gauge: Gauge,
    detection_latency: Histogram,
    stage3_latency: Histogram,
    stage3_submissions: Family<Stage3StatusLabels, Counter>,
    stage3_tip_lamports: Gauge,
    stage3_bundle_hash: Gauge,
    tip_multiplier_milli: Gauge,
    bundle_result_latency: Histogram,
    bundle_result_status: Family<BundleResultLabels, Counter>,
    stage1_backpressure_events: Family<Stage1BackpressureLabels, Counter>,
    stage1_pool_drain_events: Family<Stage1PoolDrainLabels, Counter>,
    stage2_plan_events: Family<Stage2PlanLabels, Counter>,
    stage3_retry_events: Family<Stage3RetryLabels, Counter>,
    stage1_feed_gap_events: Family<Stage1FeedGapLabels, Counter>,
}

impl TelemetryState {
    fn new() -> Self {
        let mut registry = Registry::default();

        let path_latency = Histogram::new(exponential_buckets(1.0, 2.0, 20));
        registry.register(
            "stage1_path_search_us",
            "Stage1 GraphManager multi-leg search duration in microseconds",
            path_latency.clone(),
        );

        let candidate_gauge = Gauge::default();
        registry.register(
            "stage1_path_candidate_count",
            "Candidate path count produced by Stage1 multi-hop search",
            candidate_gauge.clone(),
        );

        let detection_latency = Histogram::new(exponential_buckets(0.1, 2.0, 20));
        registry.register(
            "bundle_detection_latency_ms",
            "Elapsed milliseconds between opportunity detection and Stage2 submission",
            detection_latency.clone(),
        );

        let stage3_latency = Histogram::new(exponential_buckets(0.1, 2.0, 20));
        registry.register(
            "bundle_stage3_latency_ms",
            "Elapsed milliseconds between opportunity detection and Stage3/Jito submission",
            stage3_latency.clone(),
        );

        let stage3_submissions = Family::default();
        registry.register(
            "bundle_stage3_submissions_total",
            "Stage3 bundle submission attempts partitioned by result",
            stage3_submissions.clone(),
        );

        let stage3_tip_lamports = Gauge::default();
        registry.register(
            "bundle_stage3_tip_lamports",
            "Tip lamports observed in the last successful Stage3 bundle submission",
            stage3_tip_lamports.clone(),
        );

        let stage3_bundle_hash = Gauge::default();
        registry.register(
            "bundle_stage3_last_bundle_hash",
            "Lower 63 bits of the most recent bundle_id (decoded as base58) for correlation/debugging",
            stage3_bundle_hash.clone(),
        );

        let tip_multiplier_milli = Gauge::default();
        registry.register(
            "bundle_stage3_tip_multiplier_milli",
            "Tip multiplier applied to the last bundle (scaled by 1e3)",
            tip_multiplier_milli.clone(),
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

        let stage1_backpressure_events = Family::default();
        registry.register(
            "stage1_backpressure_events_total",
            "Stage1 bundle candidate backpressure events grouped by reason",
            stage1_backpressure_events.clone(),
        );

        let stage1_pool_drain_events = Family::default();
        registry.register(
            "stage1_pool_drain_events_total",
            "Stage1 pool-consumer drain patterns grouped by mode",
            stage1_pool_drain_events.clone(),
        );

        let stage2_plan_events = Family::default();
        registry.register(
            "stage2_plan_events_total",
            "Stage2 bundle planning outcomes grouped by event",
            stage2_plan_events.clone(),
        );

        let stage3_retry_events = Family::default();
        registry.register(
            "stage3_retry_events_total",
            "Stage3 submission retry lifecycle grouped by event",
            stage3_retry_events.clone(),
        );

        let stage1_feed_gap_events = Family::default();
        registry.register(
            "stage1_feed_gap_events_total",
            "Stage1 ingest gaps grouped by cause",
            stage1_feed_gap_events.clone(),
        );

        Self {
            registry: Arc::new(registry),
            path_latency,
            candidate_gauge,
            detection_latency,
            stage3_latency,
            stage3_submissions,
            stage3_tip_lamports,
            stage3_bundle_hash,
            tip_multiplier_milli,
            bundle_result_latency,
            bundle_result_status,
            stage1_backpressure_events,
            stage1_pool_drain_events,
            stage2_plan_events,
            stage3_retry_events,
            stage1_feed_gap_events,
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
    let mut buffer = String::new();
    if let Err(err) = encode(&mut buffer, registry.as_ref()) {
        warn!(error = %err, "failed to encode metrics response");
        return Response::builder()
            .status(500)
            .body(Body::from("encoding error"))
            .unwrap_or_else(|builder_err| Response::new(Body::from(builder_err.to_string())));
    }
    Response::builder()
        .status(200)
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(Body::from(buffer))
        .unwrap_or_else(|builder_err| Response::new(Body::from(builder_err.to_string())))
}

pub fn snapshot_metrics() -> Option<String> {
    TELEMETRY.get().and_then(|state| {
        let mut buffer = String::new();
        encode(&mut buffer, state.registry.as_ref()).ok()?;
        Some(buffer)
    })
}

pub fn record_path_search(duration: Duration, candidate_count: usize) {
    if let Some(state) = TELEMETRY.get() {
        state
            .path_latency
            .observe(duration.as_secs_f64() * 1_000_000.0);
        state.candidate_gauge.set(candidate_count as i64);
    }
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
            .stage3_latency
            .observe(duration.as_secs_f64() * 1_000.0);
    }
}

pub fn record_tip_multiplier(multiplier: f64) {
    if let Some(state) = TELEMETRY.get() {
        if multiplier.is_finite() {
            let scaled = (multiplier * 1_000.0).round();
            let clamped = scaled.clamp(i64::MIN as f64, i64::MAX as f64) as i64;
            state.tip_multiplier_milli.set(clamped);
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
struct Stage1BackpressureLabels {
    reason: &'static str,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct Stage1PoolDrainLabels {
    mode: &'static str,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct Stage3RetryLabels {
    event: &'static str,
    reason: &'static str,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct Stage1FeedGapLabels {
    reason: &'static str,
}

pub enum Stage3RetryEvent {
    RetryQueued(Stage3RetryReason),
    RetryFlushed,
    RetriesExhausted(Stage3RetryReason),
    FailureRecorded(Stage3RetryReason),
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
        state.stage3_retry_events.get_or_create(&labels).inc();
    }
}

pub enum Stage1FeedGapEvent {
    MissingSnapshot,
    BacklogQueued,
    BacklogFlushed,
    BacklogOverflow,
}

impl Stage1FeedGapEvent {
    fn label(&self) -> &'static str {
        match self {
            Stage1FeedGapEvent::MissingSnapshot => "missing_snapshot",
            Stage1FeedGapEvent::BacklogQueued => "backlog_queued",
            Stage1FeedGapEvent::BacklogFlushed => "backlog_flushed",
            Stage1FeedGapEvent::BacklogOverflow => "backlog_overflow",
        }
    }
}

pub fn record_stage1_feed_gap(event: Stage1FeedGapEvent) {
    if let Some(state) = TELEMETRY.get() {
        let labels = Stage1FeedGapLabels {
            reason: event.label(),
        };
        state.stage1_feed_gap_events.get_or_create(&labels).inc();
    }
}
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
struct Stage2PlanLabels {
    event: &'static str,
    reason: &'static str,
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
        state.stage3_submissions.get_or_create(&labels).inc();

        if matches!(status, Stage3SubmissionStatus::Success) {
            if let Some(tip) = tip_lamports {
                state.stage3_tip_lamports.set(clamp_u64_to_i64(tip));
            }
            if let Some(id) = bundle_id {
                if let Some(hash) = bundle_id_hash(id) {
                    state.stage3_bundle_hash.set(hash);
                }
            }
        }
    }
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

pub enum Stage1BackpressureEvent {
    SlabExhausted,
    RingFull,
    OverflowQueued,
    OverflowDropped,
    OverflowFlushed,
    TradeIntakePaused,
}

impl Stage1BackpressureEvent {
    fn label(&self) -> &'static str {
        match self {
            Stage1BackpressureEvent::SlabExhausted => "slab_exhausted",
            Stage1BackpressureEvent::RingFull => "ring_full",
            Stage1BackpressureEvent::OverflowQueued => "overflow_queued",
            Stage1BackpressureEvent::OverflowDropped => "overflow_dropped",
            Stage1BackpressureEvent::OverflowFlushed => "overflow_flushed",
            Stage1BackpressureEvent::TradeIntakePaused => "trade_intake_paused",
        }
    }
}

pub fn record_stage1_backpressure(event: Stage1BackpressureEvent) {
    if let Some(state) = TELEMETRY.get() {
        let labels = Stage1BackpressureLabels {
            reason: event.label(),
        };
        state
            .stage1_backpressure_events
            .get_or_create(&labels)
            .inc();
    }
}

pub enum Stage1PoolDrainEvent {
    Single,
    Multiple,
}

impl Stage1PoolDrainEvent {
    fn label(&self) -> &'static str {
        match self {
            Stage1PoolDrainEvent::Single => "single",
            Stage1PoolDrainEvent::Multiple => "multiple",
        }
    }
}

pub fn record_stage1_pool_drain(event: Stage1PoolDrainEvent) {
    if let Some(state) = TELEMETRY.get() {
        let labels = Stage1PoolDrainLabels {
            mode: event.label(),
        };
        state.stage1_pool_drain_events.get_or_create(&labels).inc();
    }
}

pub enum Stage2PlanEvent {
    PlanFailed,
    PrepareFailed,
    RetryQueuedPlan,
    RetryQueuedPrepare,
    RetryFlushed,
    RetriesExhaustedPlan,
    RetriesExhaustedPrepare,
}

impl Stage2PlanEvent {
    fn event(&self) -> &'static str {
        match self {
            Stage2PlanEvent::PlanFailed => "plan_failed",
            Stage2PlanEvent::PrepareFailed => "prepare_failed",
            Stage2PlanEvent::RetryQueuedPlan => "retry_queued",
            Stage2PlanEvent::RetryQueuedPrepare => "retry_queued",
            Stage2PlanEvent::RetryFlushed => "retry_flushed",
            Stage2PlanEvent::RetriesExhaustedPlan => "retries_exhausted",
            Stage2PlanEvent::RetriesExhaustedPrepare => "retries_exhausted",
        }
    }

    fn reason(&self) -> &'static str {
        match self {
            Stage2PlanEvent::PlanFailed | Stage2PlanEvent::RetryQueuedPlan => "plan",
            Stage2PlanEvent::PrepareFailed | Stage2PlanEvent::RetryQueuedPrepare => "prepare",
            Stage2PlanEvent::RetryFlushed => "retry",
            Stage2PlanEvent::RetriesExhaustedPlan => "plan",
            Stage2PlanEvent::RetriesExhaustedPrepare => "prepare",
        }
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
