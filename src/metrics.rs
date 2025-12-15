use crate::arbgraph::{OpportunityCounters, TradeDropCounters};
use anyhow::Result;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Shared metrics handle that registers Prometheus collectors and exposes update helpers.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Arc<RwLock<Registry>>,
    queue_depth: Family<QueueLabel, Gauge>,
    queue_utilization: Family<QueueLabel, Gauge>,
    queue_drops_total: Family<QueueLabel, Counter<u64>>,
    planner_plans_total: Counter<u64>,
    planner_opportunities_total: Family<PlannerOpportunityLabel, Counter<u64>>,
    executor_plans_total: Counter<u64>,
    executor_fallback_txs_total: Counter<u64>,
    executor_results_total: Family<ExecutorResultLabel, Counter<u64>>,
    executor_tx_synth_total: Family<ExecutorSynthLabel, Counter<u64>>,
    executor_revalidation_total: Family<ExecutorRevalidationLabel, Counter<u64>>,
    executor_blockhash_refresh_total: Family<BlockhashRefreshLabel, Counter<u64>>,
    executor_supplemental_hydration_total: Family<ExecutorSupplementalLabel, Counter<u64>>,
    executor_tip_refresh_total: Family<TipRefreshLabel, Counter<u64>>,
    executor_tip_accounts: Gauge,
    executor_tip_floor_refresh_total: Family<TipFloorRefreshLabel, Counter<u64>>,
    executor_tip_floor_lamports: Gauge,
    executor_bundle_packets_total: Counter<u64>,
    executor_bundle_bytes_total: Counter<u64>,
    executor_bundle_tip_lamports: Gauge,
    executor_latency_ms: Family<ExecutorLatencyLabel, Gauge>,
    ingest_messages_total: Family<StreamLabel, Counter<u64>>,
    ingest_errors_total: Family<StreamLabel, Counter<u64>>,
    ingest_backoff_ms: Family<StreamLabel, Gauge>,
    ingest_backoff_jitter_ms: Family<StreamLabel, Gauge>,
    ingest_stale_total: Family<StreamLabel, Counter<u64>>,
    ingest_pair_filter_total: Family<StreamLabel, Counter<u64>>,
    ingest_sequence_evictions_total: Family<StreamLabel, Counter<u64>>,
    ingest_sequence_occupancy_pct: Family<StreamLabel, Gauge>,
    ingest_restart_total: Family<IngestRestartLabel, Counter<u64>>,
    bundle_results_total: Family<ResultLabel, Counter<u64>>,
    bundle_latency_ms: Family<BundleLatencyLabel, Gauge>,
    bundle_failure_streak: Gauge,
    bundle_failure_reason_total: Family<BundleFailureLabel, Counter<u64>>,
    bundle_inflight: Gauge,
    planner_cache_events_total: Family<PlannerCacheLabel, Counter<u64>>,
    planner_prefetch_backlog: Family<PrefetchBacklogLabel, Gauge>,
    planner_hop_validation_total: Family<PlannerHopValidationLabel, Counter<u64>>,
    planner_hop_refresh_total: Family<PlannerHopRefreshLabel, Counter<u64>>,
    planner_plan_drops_total: Family<PlannerPlanDropLabel, Counter<u64>>,
    planner_flash_lookup_total: Family<PlannerFlashLookupLabel, Counter<u64>>,
    planner_flash_refcount: Gauge,
    planner_trade_drops_total: Family<PlannerTradeDropLabel, Counter<u64>>,
    planner_trade_ingest_total: Family<PlannerTradeIngestLabel, Counter<u64>>,
    planner_seed_inference_total: Family<SeedInferenceLabel, Counter<u64>>,
    planner_seed_mismatch_total: Family<SeedMismatchLabel, Counter<u64>>,
    planner_seed_snapshot_age_secs: Gauge,
    curve_state_fetch_total: Family<CurveStateFetchLabel, Counter<u64>>,
}

impl Metrics {
    /// Builds a new metrics registry with default collectors.
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let queue_depth = Family::<QueueLabel, Gauge>::default();
        registry.register(
            "queue_depth",
            "Current depth of in-memory worker queues",
            queue_depth.clone(),
        );

        let queue_utilization = Family::<QueueLabel, Gauge>::default();
        registry.register(
            "queue_saturation_pct",
            "Instantaneous queue saturation expressed as a percentage",
            queue_utilization.clone(),
        );

        let queue_drops_total = Family::<QueueLabel, Counter<u64>>::default();
        registry.register(
            "queue_drops_total",
            "Total number of dropped queue items due to saturation",
            queue_drops_total.clone(),
        );

        let planner_plans_total = Counter::<u64>::default();
        registry.register(
            "planner_plans_total",
            "Total arbitrage plans emitted by planner",
            planner_plans_total.clone(),
        );

        let planner_opportunities_total =
            Family::<PlannerOpportunityLabel, Counter<u64>>::default();
        registry.register(
            "planner_opportunities_total",
            "Planner opportunity lifecycle events keyed by event type",
            planner_opportunities_total.clone(),
        );

        let executor_plans_total = Counter::<u64>::default();
        registry.register(
            "executor_plans_total",
            "Plans processed by executor",
            executor_plans_total.clone(),
        );

        let executor_fallback_txs_total = Counter::<u64>::default();
        registry.register(
            "executor_fallback_txs_total",
            "Fallback transactions synthesized when plans lack packets",
            executor_fallback_txs_total.clone(),
        );

        let executor_results_total = Family::<ExecutorResultLabel, Counter<u64>>::default();
        registry.register(
            "executor_results_total",
            "Bundle submission results emitted by executor",
            executor_results_total.clone(),
        );

        let executor_tx_synth_total = Family::<ExecutorSynthLabel, Counter<u64>>::default();
        registry.register(
            "executor_tx_synth_total",
            "Executor transaction synthesis outcomes keyed by status",
            executor_tx_synth_total.clone(),
        );

        let executor_revalidation_total =
            Family::<ExecutorRevalidationLabel, Counter<u64>>::default();
        registry.register(
            "executor_plan_revalidation_total",
            "Executor plan revalidation outcomes keyed by status",
            executor_revalidation_total.clone(),
        );

        let executor_blockhash_refresh_total =
            Family::<BlockhashRefreshLabel, Counter<u64>>::default();
        registry.register(
            "executor_blockhash_refresh_total",
            "Blockhash refresh outcomes for fallback transaction signing",
            executor_blockhash_refresh_total.clone(),
        );

        let executor_supplemental_hydration_total =
            Family::<ExecutorSupplementalLabel, Counter<u64>>::default();
        registry.register(
            "executor_supplemental_hydration_total",
            "Executor supplemental-account hydration sources keyed by curve",
            executor_supplemental_hydration_total.clone(),
        );

        let executor_tip_refresh_total = Family::<TipRefreshLabel, Counter<u64>>::default();
        registry.register(
            "executor_tip_refresh_total",
            "Tip account refresh attempts keyed by status",
            executor_tip_refresh_total.clone(),
        );

        let executor_tip_accounts = Gauge::default();
        registry.register(
            "executor_tip_accounts",
            "Most recently observed number of usable tip accounts",
            executor_tip_accounts.clone(),
        );

        let executor_tip_floor_refresh_total =
            Family::<TipFloorRefreshLabel, Counter<u64>>::default();
        registry.register(
            "executor_tip_floor_refresh_total",
            "Tip floor refresh outcomes keyed by status",
            executor_tip_floor_refresh_total.clone(),
        );

        let executor_tip_floor_lamports = Gauge::default();
        registry.register(
            "executor_tip_floor_lamports",
            "Most recent dynamic tip floor observed from Jito APIs",
            executor_tip_floor_lamports.clone(),
        );

        let executor_bundle_packets_total = Counter::<u64>::default();
        registry.register(
            "executor_bundle_packets_total",
            "Total number of packets submitted inside executor bundles",
            executor_bundle_packets_total.clone(),
        );

        let executor_bundle_bytes_total = Counter::<u64>::default();
        registry.register(
            "executor_bundle_bytes_total",
            "Total serialized byte volume of packets sent to Jito",
            executor_bundle_bytes_total.clone(),
        );

        let executor_bundle_tip_lamports = Gauge::default();
        registry.register(
            "executor_bundle_tip_lamports",
            "Most recent tip transfer size used inside an executor bundle",
            executor_bundle_tip_lamports.clone(),
        );

        let executor_latency_ms = Family::<ExecutorLatencyLabel, Gauge>::default();
        registry.register(
            "executor_latency_ms",
            "Executor latency measurements keyed by kind (queue_wait, synth)",
            executor_latency_ms.clone(),
        );

        let ingest_messages_total = Family::<StreamLabel, Counter<u64>>::default();
        registry.register(
            "ingest_messages_total",
            "Messages successfully processed from Corecast streams",
            ingest_messages_total.clone(),
        );

        let ingest_errors_total = Family::<StreamLabel, Counter<u64>>::default();
        registry.register(
            "ingest_errors_total",
            "Stream errors encountered while consuming Corecast",
            ingest_errors_total.clone(),
        );

        let ingest_backoff_ms = Family::<StreamLabel, Gauge>::default();
        registry.register(
            "ingest_backoff_ms",
            "Most recent reconnect backoff duration per stream (ms)",
            ingest_backoff_ms.clone(),
        );

        let ingest_backoff_jitter_ms = Family::<StreamLabel, Gauge>::default();
        registry.register(
            "ingest_backoff_jitter_ms",
            "Magnitude of jitter applied to reconnect backoffs per stream (ms)",
            ingest_backoff_jitter_ms.clone(),
        );

        let ingest_stale_total = Family::<StreamLabel, Counter<u64>>::default();
        registry.register(
            "ingest_stale_total",
            "Out-of-order Corecast messages dropped prior to enqueue",
            ingest_stale_total.clone(),
        );

        let ingest_pair_filter_total = Family::<StreamLabel, Counter<u64>>::default();
        registry.register(
            "ingest_pair_filter_total",
            "Corecast updates discarded because their token pair is not allowlisted",
            ingest_pair_filter_total.clone(),
        );

        let ingest_sequence_occupancy_pct = Family::<StreamLabel, Gauge>::default();
        registry.register(
            "ingest_sequence_occupancy_pct",
            "Occupancy of per-stream sequence filters expressed as a percentage",
            ingest_sequence_occupancy_pct.clone(),
        );

        let bundle_results_total = Family::<ResultLabel, Counter<u64>>::default();
        registry.register(
            "bundle_results_total",
            "Bundle result events received from Jito block engine",
            bundle_results_total.clone(),
        );

        let bundle_latency_ms = Family::<BundleLatencyLabel, Gauge>::default();
        registry.register(
            "bundle_latency_ms",
            "Bundle result latency in milliseconds keyed by result type",
            bundle_latency_ms.clone(),
        );

        let bundle_failure_streak = Gauge::default();
        registry.register(
            "bundle_failure_streak",
            "Consecutive bundle failures observed by the monitor",
            bundle_failure_streak.clone(),
        );

        let bundle_failure_reason_total = Family::<BundleFailureLabel, Counter<u64>>::default();
        registry.register(
            "bundle_failure_reason_total",
            "Bundle failures grouped by detailed reason",
            bundle_failure_reason_total.clone(),
        );

        let bundle_inflight = Gauge::default();
        registry.register(
            "bundle_inflight",
            "Current number of bundles sent to Jito awaiting a terminal result",
            bundle_inflight.clone(),
        );

        let planner_cache_events_total = Family::<PlannerCacheLabel, Counter<u64>>::default();
        registry.register(
            "planner_supplemental_cache_total",
            "Planner supplemental-account cache events keyed by curve + status",
            planner_cache_events_total.clone(),
        );

        let planner_prefetch_backlog = Family::<PrefetchBacklogLabel, Gauge>::default();
        registry.register(
            "planner_prefetch_backlog",
            "Outstanding supplemental-prefetch tasks awaiting RPC hydration",
            planner_prefetch_backlog.clone(),
        );

        let planner_hop_validation_total =
            Family::<PlannerHopValidationLabel, Counter<u64>>::default();
        registry.register(
            "planner_hop_validation_total",
            "Planner hop validation outcomes keyed by curve and status",
            planner_hop_validation_total.clone(),
        );

        let planner_hop_refresh_total = Family::<PlannerHopRefreshLabel, Counter<u64>>::default();
        registry.register(
            "planner_hop_refresh_total",
            "Planner hop refresh requests keyed by curve and reason",
            planner_hop_refresh_total.clone(),
        );

        let planner_plan_drops_total = Family::<PlannerPlanDropLabel, Counter<u64>>::default();
        registry.register(
            "planner_plan_drops_total",
            "Planner plans dropped before executor enqueue keyed by reason",
            planner_plan_drops_total.clone(),
        );

        let planner_flash_lookup_total = Family::<PlannerFlashLookupLabel, Counter<u64>>::default();
        registry.register(
            "planner_flash_lookup_total",
            "Solend flash-loan lookup outcomes keyed by status",
            planner_flash_lookup_total.clone(),
        );

        let planner_flash_refcount = Gauge::default();
        registry.register(
            "planner_flash_refcount",
            "Most recent strong reference count observed for a Solend flash-loan spec",
            planner_flash_refcount.clone(),
        );

        let planner_trade_drops_total = Family::<PlannerTradeDropLabel, Counter<u64>>::default();
        registry.register(
            "planner_trade_drop_total",
            "Planner trades discarded before plan generation keyed by reason",
            planner_trade_drops_total.clone(),
        );

        let planner_trade_ingest_total = Family::<PlannerTradeIngestLabel, Counter<u64>>::default();
        registry.register(
            "planner_trade_ingest_total",
            "Trades ingested by planner keyed by curve kind",
            planner_trade_ingest_total.clone(),
        );

        let planner_seed_inference_total = Family::<SeedInferenceLabel, Counter<u64>>::default();
        registry.register(
            "planner_seed_inference_total",
            "Pool identity inference outcomes keyed by status",
            planner_seed_inference_total.clone(),
        );

        let planner_seed_mismatch_total = Family::<SeedMismatchLabel, Counter<u64>>::default();
        registry.register(
            "planner_seed_mismatch_total",
            "Pool identity mismatches keyed by field",
            planner_seed_mismatch_total.clone(),
        );

        let planner_seed_snapshot_age_secs = Gauge::default();
        registry.register(
            "planner_seed_snapshot_age_secs",
            "Age of the most recently loaded pool seed snapshot in seconds",
            planner_seed_snapshot_age_secs.clone(),
        );

        let curve_state_fetch_total = Family::<CurveStateFetchLabel, Counter<u64>>::default();
        registry.register(
            "curve_state_fetch_total",
            "Curve-state hydration attempts grouped by curve and status",
            curve_state_fetch_total.clone(),
        );

        let ingest_sequence_evictions_total = Family::<StreamLabel, Counter<u64>>::default();
        registry.register(
            "ingest_sequence_evictions_total",
            "Sequence-filter evictions triggered when capacity trims old pools",
            ingest_sequence_evictions_total.clone(),
        );

        let ingest_restart_total = Family::<IngestRestartLabel, Counter<u64>>::default();
        registry.register(
            "ingest_restart_total",
            "Ingest stream restarts keyed by stream and root cause",
            ingest_restart_total.clone(),
        );

        let inner = MetricsInner {
            registry: Arc::new(RwLock::new(registry)),
            queue_depth,
            queue_utilization,
            queue_drops_total,
            planner_plans_total,
            planner_opportunities_total,
            executor_plans_total,
            executor_fallback_txs_total,
            executor_results_total,
            executor_tx_synth_total,
            executor_revalidation_total,
            executor_blockhash_refresh_total,
            executor_supplemental_hydration_total,
            executor_tip_refresh_total,
            executor_tip_accounts,
            executor_tip_floor_refresh_total,
            executor_tip_floor_lamports,
            executor_bundle_packets_total,
            executor_bundle_bytes_total,
            executor_bundle_tip_lamports,
            executor_latency_ms,
            ingest_messages_total,
            ingest_errors_total,
            ingest_backoff_ms,
            ingest_backoff_jitter_ms,
            ingest_stale_total,
            ingest_pair_filter_total,
            ingest_sequence_evictions_total,
            ingest_sequence_occupancy_pct,
            ingest_restart_total,
            bundle_results_total,
            bundle_latency_ms,
            bundle_failure_streak,
            bundle_failure_reason_total,
            bundle_inflight,
            planner_cache_events_total,
            planner_prefetch_backlog,
            planner_hop_validation_total,
            planner_hop_refresh_total,
            planner_plan_drops_total,
            planner_flash_lookup_total,
            planner_flash_refcount,
            planner_trade_drops_total,
            planner_trade_ingest_total,
            planner_seed_inference_total,
            planner_seed_mismatch_total,
            planner_seed_snapshot_age_secs,
            curve_state_fetch_total,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Observes the current depth for a named queue.
    pub fn observe_queue_depth(&self, queue: &'static str, depth: usize) {
        let gauge = self.inner.queue_depth.get_or_create(&QueueLabel { queue });
        gauge.set(depth as i64);
    }

    /// Tracks instantaneous queue utilization as a whole-number percentage.
    pub fn observe_queue_saturation(&self, queue: &'static str, depth: usize, capacity: usize) {
        if capacity == 0 {
            return;
        }
        let ratio = depth.min(capacity) as f64 / capacity as f64;
        let pct = (ratio * 100.0).round() as i64;
        let gauge = self
            .inner
            .queue_utilization
            .get_or_create(&QueueLabel { queue });
        gauge.set(pct);
    }

    /// Records a queue drop occurrence for a queue.
    pub fn inc_queue_drop(&self, queue: &'static str) {
        self.inner
            .queue_drops_total
            .get_or_create(&QueueLabel { queue })
            .inc();
    }

    /// Increments the planner plans counter.
    pub fn inc_planner_plans(&self, delta: u64) {
        if delta > 0 {
            self.inner.planner_plans_total.inc_by(delta);
        }
    }

    /// Records planner opportunity lifecycle stats broken down by event type.
    pub fn inc_planner_opportunities(&self, counters: &OpportunityCounters) {
        self.inc_planner_opportunity("emitted", counters.emitted);
        self.inc_planner_opportunity("duplicates", counters.duplicates);
        self.inc_planner_opportunity("replaced", counters.replaced);
        self.inc_planner_opportunity("drained", counters.drained);
        self.inc_planner_opportunity("evicted", counters.evicted);
    }

    fn inc_planner_opportunity(&self, event: &'static str, value: u64) {
        if value == 0 {
            return;
        }
        self.inner
            .planner_opportunities_total
            .get_or_create(&PlannerOpportunityLabel { event })
            .inc_by(value);
    }

    /// Increments the executor plans counter.
    pub fn inc_executor_plans(&self, delta: u64) {
        if delta > 0 {
            self.inner.executor_plans_total.inc_by(delta);
        }
    }

    /// Counts fallback transactions synthesized by the executor.
    pub fn inc_executor_fallback_tx(&self) {
        self.inner.executor_fallback_txs_total.inc();
    }

    /// Records executor submission results keyed by status.
    pub fn inc_executor_result(&self, status: &'static str) {
        self.inner
            .executor_results_total
            .get_or_create(&ExecutorResultLabel { status })
            .inc();
    }

    /// Tracks transaction synthesis attempts keyed by status.
    pub fn inc_executor_tx_synth(&self, status: &'static str) {
        self.inner
            .executor_tx_synth_total
            .get_or_create(&ExecutorSynthLabel { status })
            .inc();
    }

    /// Records executor plan revalidation outcomes keyed by status.
    pub fn inc_executor_revalidation(&self, status: &'static str) {
        self.inner
            .executor_revalidation_total
            .get_or_create(&ExecutorRevalidationLabel { status })
            .inc();
    }

    /// Tracks blockhash refresh attempts used by fallback signing.
    pub fn inc_executor_blockhash_refresh(&self, status: &'static str) {
        self.inner
            .executor_blockhash_refresh_total
            .get_or_create(&BlockhashRefreshLabel { status })
            .inc();
    }

    /// Records where supplemental accounts were sourced during hydration.
    pub fn inc_executor_supplemental_hydration(&self, curve: &'static str, source: &'static str) {
        self.inner
            .executor_supplemental_hydration_total
            .get_or_create(&ExecutorSupplementalLabel { curve, source })
            .inc();
    }

    /// Records tip account refresh attempts keyed by result status.
    pub fn inc_executor_tip_refresh(&self, status: &'static str) {
        self.inner
            .executor_tip_refresh_total
            .get_or_create(&TipRefreshLabel { status })
            .inc();
    }

    /// Captures the latest number of unique tip accounts returned by Jito.
    pub fn observe_executor_tip_accounts(&self, count: usize) {
        self.inner.executor_tip_accounts.set(count as i64);
    }

    /// Records tip floor refresh attempts keyed by status.
    pub fn inc_executor_tip_floor_refresh(&self, status: &'static str) {
        self.inner
            .executor_tip_floor_refresh_total
            .get_or_create(&TipFloorRefreshLabel { status })
            .inc();
    }

    /// Observes the latest tip floor value exposed by Jito APIs.
    pub fn observe_executor_tip_floor(&self, lamports: u64) {
        let clamped = lamports.min(i64::MAX as u64) as i64;
        self.inner.executor_tip_floor_lamports.set(clamped);
    }

    /// Counts packets forwarded inside Jito bundles.
    pub fn inc_executor_bundle_packets(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.inner
            .executor_bundle_packets_total
            .inc_by(count as u64);
    }

    /// Counts serialized bytes forwarded inside Jito bundles.
    pub fn inc_executor_bundle_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.inner.executor_bundle_bytes_total.inc_by(bytes as u64);
    }

    /// Observes the most recent explicit tip transfer value used in a bundle.
    pub fn observe_executor_bundle_tip(&self, lamports: u64) {
        let clamped = lamports.min(i64::MAX as u64) as i64;
        self.inner.executor_bundle_tip_lamports.set(clamped);
    }

    /// Records queue wait duration for executor plans.
    pub fn observe_executor_queue_wait(&self, delay: Duration) {
        self.observe_executor_latency("queue_wait", delay);
    }

    /// Records transaction synthesis time for executor bundles.
    pub fn observe_executor_synth_time(&self, delay: Duration) {
        self.observe_executor_latency("synth", delay);
    }

    fn observe_executor_latency(&self, kind: &'static str, delay: Duration) {
        let ms = delay.as_millis().min(i64::MAX as u128) as i64;
        self.inner
            .executor_latency_ms
            .get_or_create(&ExecutorLatencyLabel { kind })
            .set(ms);
    }

    /// Increments the ingest message counter for a stream.
    pub fn inc_ingest_message(&self, stream: &'static str) {
        self.inner
            .ingest_messages_total
            .get_or_create(&StreamLabel { stream })
            .inc();
    }

    /// Increments the ingest error counter for a stream.
    pub fn inc_ingest_error(&self, stream: &'static str) {
        self.inner
            .ingest_errors_total
            .get_or_create(&StreamLabel { stream })
            .inc();
    }

    /// Records the most recent backoff duration (in milliseconds) for a stream.
    pub fn observe_ingest_backoff(&self, stream: &'static str, delay: Duration) {
        let gauge = self
            .inner
            .ingest_backoff_ms
            .get_or_create(&StreamLabel { stream });
        gauge.set(delay.as_millis() as i64);
    }

    /// Records the jitter amount applied to the latest reconnect backoff.
    pub fn observe_ingest_backoff_jitter(&self, stream: &'static str, jitter: Duration) {
        let gauge = self
            .inner
            .ingest_backoff_jitter_ms
            .get_or_create(&StreamLabel { stream });
        gauge.set(jitter.as_millis() as i64);
    }

    /// Counts stale Corecast messages dropped before hitting downstream queues.
    pub fn inc_ingest_stale(&self, stream: &'static str) {
        self.inner
            .ingest_stale_total
            .get_or_create(&StreamLabel { stream })
            .inc();
    }

    /// Counts Corecast updates filtered because their token pair is not allowlisted.
    pub fn inc_ingest_pair_filter(&self, stream: &'static str) {
        self.inner
            .ingest_pair_filter_total
            .get_or_create(&StreamLabel { stream })
            .inc();
    }

    /// Observes sequence-filter occupancy as a percentage.
    pub fn observe_ingest_sequence_occupancy(
        &self,
        stream: &'static str,
        used: usize,
        capacity: usize,
    ) {
        if capacity == 0 {
            return;
        }
        let ratio = used.min(capacity) as f64 / capacity as f64;
        let pct = (ratio * 100.0).round() as i64;
        let gauge = self
            .inner
            .ingest_sequence_occupancy_pct
            .get_or_create(&StreamLabel { stream });
        gauge.set(pct);
    }

    /// Counts sequence-filter evictions generated for a stream.
    pub fn inc_ingest_sequence_evictions(&self, stream: &'static str, count: u64) {
        if count == 0 {
            return;
        }
        self.inner
            .ingest_sequence_evictions_total
            .get_or_create(&StreamLabel { stream })
            .inc_by(count);
    }

    /// Counts ingest restarts keyed by failure reason.
    pub fn inc_ingest_restart(&self, stream: &'static str, reason: &'static str) {
        self.inner
            .ingest_restart_total
            .get_or_create(&IngestRestartLabel { stream, reason })
            .inc();
    }

    /// Records a bundle result event keyed by result type.
    pub fn inc_bundle_result(&self, result: &'static str) {
        self.inner
            .bundle_results_total
            .get_or_create(&ResultLabel { result })
            .inc();
    }

    /// Records bundle result latency for a given result kind.
    pub fn observe_bundle_latency(&self, result: &'static str, delay: Duration) {
        let ms = delay.as_millis().min(i64::MAX as u128) as i64;
        self.inner
            .bundle_latency_ms
            .get_or_create(&BundleLatencyLabel { result })
            .set(ms);
    }

    /// Records a detailed failure reason for a rejected/dropped bundle.
    pub fn inc_bundle_failure_reason(&self, reason: &'static str) {
        self.inner
            .bundle_failure_reason_total
            .get_or_create(&BundleFailureLabel { reason })
            .inc();
    }

    /// Observes the current consecutive failure streak.
    pub fn observe_bundle_failure_streak(&self, streak: u64) {
        self.inner
            .bundle_failure_streak
            .set(streak.min(i64::MAX as u64) as i64);
    }

    /// Captures the number of bundles that have been submitted but not yet resolved.
    pub fn observe_bundle_inflight(&self, inflight: usize) {
        self.inner.bundle_inflight.set(inflight as i64);
    }

    /// Tracks planner supplemental-account cache activity.
    pub fn inc_planner_supplemental_cache(&self, curve: &'static str, status: &'static str) {
        self.inner
            .planner_cache_events_total
            .get_or_create(&PlannerCacheLabel { curve, status })
            .inc();
    }

    /// Observes the outstanding supplemental-prefetch backlog per curve.
    pub fn observe_planner_prefetch_backlog(&self, curve: &'static str, backlog: usize) {
        let gauge = self
            .inner
            .planner_prefetch_backlog
            .get_or_create(&PrefetchBacklogLabel { curve });
        gauge.set(backlog as i64);
    }

    /// Records the wall-clock age for the currently loaded pool seed snapshot.
    pub fn observe_pool_seed_snapshot_age(&self, age: Option<Duration>) {
        let secs = age
            .map(|dur| dur.as_secs().min(i64::MAX as u64))
            .unwrap_or(0) as i64;
        self.inner.planner_seed_snapshot_age_secs.set(secs);
    }

    /// Records the outcome of planner seed inference attempts.
    pub fn inc_planner_seed_inference(&self, status: &'static str) {
        self.inner
            .planner_seed_inference_total
            .get_or_create(&SeedInferenceLabel { status })
            .inc();
    }

    /// Records observed mismatches between trusted seeds and inferred data.
    pub fn inc_planner_seed_mismatch(&self, field: &'static str) {
        self.inner
            .planner_seed_mismatch_total
            .get_or_create(&SeedMismatchLabel { field })
            .inc();
    }

    /// Records hop validation outcomes before emitting swap instructions.
    pub fn inc_planner_hop_validation(&self, curve: &'static str, status: &'static str) {
        self.inner
            .planner_hop_validation_total
            .get_or_create(&PlannerHopValidationLabel { curve, status })
            .inc();
    }

    /// Tracks hop refresh requests triggered by planner validation logic.
    pub fn inc_planner_hop_refresh(&self, curve: &'static str, reason: &'static str) {
        self.inner
            .planner_hop_refresh_total
            .get_or_create(&PlannerHopRefreshLabel { curve, reason })
            .inc();
    }

    /// Counts planner plans dropped before enqueueing to executor queues.
    pub fn inc_planner_plan_drop(&self, reason: &'static str) {
        self.inner
            .planner_plan_drops_total
            .get_or_create(&PlannerPlanDropLabel { reason })
            .inc();
    }

    /// Records flash-loan lookup outcomes (hit/miss) inside the planner.
    pub fn inc_planner_flash_lookup(&self, status: &'static str) {
        self.inner
            .planner_flash_lookup_total
            .get_or_create(&PlannerFlashLookupLabel { status })
            .inc();
    }

    /// Observes the current strong reference count for a flash-loan spec Arc.
    pub fn observe_planner_flash_refcount(&self, refs: usize) {
        self.inner
            .planner_flash_refcount
            .set(refs.min(i64::MAX as usize) as i64);
    }

    /// Records trades processed by the planner keyed by curve kind.
    pub fn inc_planner_trade_ingest(&self, curve: &'static str) {
        self.inner
            .planner_trade_ingest_total
            .get_or_create(&PlannerTradeIngestLabel { curve })
            .inc();
    }

    /// Records trade drop reasons reported by the planner.
    pub fn inc_planner_trade_drops(&self, counters: &TradeDropCounters) {
        self.inc_planner_trade_drop("pair_blocked", counters.pair_blocked);
        self.inc_planner_trade_drop("notional_zero", counters.notional_zero);
        self.inc_planner_trade_drop("curve_state_missing", counters.curve_state_missing);
        self.inc_planner_trade_drop("quote_missing", counters.quote_missing);
        self.inc_planner_trade_drop("profit_floor", counters.profit_floor);
    }

    fn inc_planner_trade_drop(&self, reason: &'static str, value: u64) {
        if value == 0 {
            return;
        }
        self.inner
            .planner_trade_drops_total
            .get_or_create(&PlannerTradeDropLabel { reason })
            .inc_by(value);
    }

    /// Records curve-state hydration outcomes keyed by curve + status.
    pub fn inc_curve_state_fetch(&self, curve: &'static str, status: &'static str) {
        self.inner
            .curve_state_fetch_total
            .get_or_create(&CurveStateFetchLabel { curve, status })
            .inc();
    }
}

/// Runs the HTTP exporter that exposes Prometheus metrics under `/metrics`.
pub async fn run_exporter(metrics: Metrics, addr: SocketAddr) -> Result<()> {
    let registry = metrics.inner.registry.clone();
    let make_svc = make_service_fn(move |_| {
        let registry = registry.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                let registry = registry.clone();
                async move { Ok::<_, Infallible>(handle_request(req, registry)) }
            }))
        }
    });

    tracing::info!(target: "telemetry::metrics", %addr, "metrics exporter listening");
    Server::bind(&addr).serve(make_svc).await?;
    Ok(())
}

fn handle_request(req: Request<Body>, registry: Arc<RwLock<Registry>>) -> Response<Body> {
    if req.method() != Method::GET || req.uri().path() != "/metrics" {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found"))
            .unwrap();
    }

    match encode_registry(&registry) {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; version=0.0.4")
            .body(Body::from(body))
            .unwrap(),
        Err(err) => {
            tracing::error!(target: "telemetry::metrics", error = %err, "failed to encode metrics");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("encode error"))
                .unwrap()
        }
    }
}

fn encode_registry(registry: &Arc<RwLock<Registry>>) -> Result<String, String> {
    let guard = registry
        .read()
        .map_err(|_| "metrics registry poisoned".to_string())?;
    let mut buffer = String::new();
    encode(&mut buffer, &guard).map_err(|err| err.to_string())?;
    Ok(buffer)
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueueLabel {
    queue: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StreamLabel {
    stream: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ResultLabel {
    result: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BundleLatencyLabel {
    result: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BundleFailureLabel {
    reason: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct IngestRestartLabel {
    stream: &'static str,
    reason: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ExecutorResultLabel {
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ExecutorSynthLabel {
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ExecutorRevalidationLabel {
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CurveStateFetchLabel {
    curve: &'static str,
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ExecutorSupplementalLabel {
    curve: &'static str,
    source: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BlockhashRefreshLabel {
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TipRefreshLabel {
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TipFloorRefreshLabel {
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PlannerCacheLabel {
    curve: &'static str,
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PlannerOpportunityLabel {
    event: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PrefetchBacklogLabel {
    curve: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PlannerHopValidationLabel {
    curve: &'static str,
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PlannerHopRefreshLabel {
    curve: &'static str,
    reason: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PlannerPlanDropLabel {
    reason: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PlannerFlashLookupLabel {
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PlannerTradeDropLabel {
    reason: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PlannerTradeIngestLabel {
    curve: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SeedInferenceLabel {
    status: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SeedMismatchLabel {
    field: &'static str,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ExecutorLatencyLabel {
    kind: &'static str,
}
