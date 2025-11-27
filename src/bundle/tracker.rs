use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use futures::StreamExt;
use reqwest::blocking::Client as BlockingClient;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use solana_sdk::signature::Keypair;
use tokio::runtime::Runtime;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message},
};
use tracing::{debug, info, warn};

use crate::config::{AppConfig, SubmissionTrackingConfig};
use crate::notifier::{BundleSubmissionEvent, BundleSubmissionStatus, Notifier};
use crate::proto::bundle::{self, bundle_result, BundleResult, DroppedReason};
use crate::proto::searcher::SubscribeBundleResultsRequest;
use crate::telemetry::{self, BundleResultStatus};

use super::{connect_searcher, load_jito_ca, normalize_endpoints};

pub struct BundleTracker {
    pending: DashMap<String, PendingBundle>,
    failed: DashMap<u64, FailedSubmission>,
    notifier: Arc<Notifier>,
    submission: SubmissionTrackingConfig,
}

#[derive(Clone)]
pub struct PendingSubmission {
    pub bundle_id: String,
    pub seq: u64,
    pub endpoint: String,
    pub detected_at: Instant,
    pub tip_lamports: Option<u64>,
}

struct PendingBundle {
    seq: u64,
    endpoint: String,
    detected_at: Instant,
    submitted_at: Instant,
    tip_lamports: Option<u64>,
    last_status: Option<BundleSubmissionStatus>,
}

struct FailedSubmission {
    recorded_at: Instant,
}

struct StatusPollTarget {
    bundle_id: String,
    endpoint: String,
}

struct PolledBundleStatus {
    status: BundleSubmissionStatus,
    slot: Option<u64>,
    detail: Option<String>,
    terminal: bool,
}

impl BundleTracker {
    pub fn spawn(
        stop: Arc<AtomicBool>,
        config: Arc<AppConfig>,
        notifier: Arc<Notifier>,
        auth: Arc<Keypair>,
    ) -> Result<(Arc<Self>, Vec<thread::JoinHandle<Result<()>>>)> {
        if config.jito.endpoints.is_empty() {
            return Err(anyhow!("jito endpoints required for bundle tracking"));
        }

        let tls_ca = load_jito_ca(&config)?;

        let tracker = Arc::new(Self {
            pending: DashMap::new(),
            failed: DashMap::new(),
            notifier,
            submission: config.submission.clone(),
        });

        let endpoints = normalize_endpoints(&config.jito.endpoints);
        let mut handles = Vec::new();
        handles.push(spawn_sweeper(tracker.clone(), stop.clone())?);
        handles.push(spawn_result_listener(
            tracker.clone(),
            stop.clone(),
            endpoints.clone(),
            auth,
            config.submission.clone(),
            tls_ca,
        )?);

        if config.submission.status_poll_interval_secs > 0 {
            handles.push(spawn_status_poller(
                tracker.clone(),
                stop.clone(),
                endpoints.clone(),
                config.jito.api_token.clone(),
                config.submission.clone(),
            )?);
        } else {
            info!(
                target = "bundle_tracker",
                "bundle status poller disabled via config"
            );
        }

        if let Some(url) = config.jito.auction_stream_url.clone() {
            if !url.trim().is_empty() {
                handles.push(spawn_auction_listener(
                    tracker.clone(),
                    stop.clone(),
                    url,
                    config.submission.clone(),
                )?);
            }
        }

        Ok((tracker, handles))
    }

    pub fn track_submission(&self, submission: PendingSubmission) {
        let entry = PendingBundle {
            seq: submission.seq,
            endpoint: submission.endpoint,
            detected_at: submission.detected_at,
            submitted_at: Instant::now(),
            tip_lamports: submission.tip_lamports,
            last_status: Some(BundleSubmissionStatus::Submitted),
        };
        self.pending.insert(submission.bundle_id, entry);
    }

    pub fn record_stage3_failure(
        &self,
        seq: u64,
        detected_at: Instant,
        tip_lamports: Option<u64>,
        detail: String,
    ) {
        info!(target = "bundle_tracker", seq, detail = %detail, "stage3 failure recorded");
        self.failed.insert(
            seq,
            FailedSubmission {
                recorded_at: Instant::now(),
            },
        );
        let event = BundleSubmissionEvent {
            seq,
            status: BundleSubmissionStatus::Failed,
            bundle_id: None,
            endpoint: None,
            tip_lamports,
            detection_latency: detected_at.elapsed(),
            slot: None,
            detail: Some(detail.clone()),
            error: Some(detail),
        };
        if let Err(err) = self.notifier.notify_bundle_submission(event) {
            warn!(target = "bundle_tracker", seq, error = %err, "notifier failed for stage3 failure");
        }
    }

    fn pending_poll_targets(&self) -> Vec<StatusPollTarget> {
        self.pending
            .iter()
            .map(|entry| StatusPollTarget {
                bundle_id: entry.key().clone(),
                endpoint: entry.endpoint.clone(),
            })
            .collect()
    }

    fn apply_polled_status(&self, bundle_id: &str, update: PolledBundleStatus) {
        if let Some(mut entry) = self.pending.get_mut(bundle_id) {
            if entry.last_status == Some(update.status) {
                return;
            }
            let detail = update.detail.clone();
            self.emit_event(bundle_id, &entry, update.status, update.slot, detail);
            entry.last_status = Some(update.status);
            if update.terminal {
                drop(entry);
                self.pending.remove(bundle_id);
            }
        }
    }

    fn handle_bundle_result(&self, result: BundleResult) {
        let bundle_id = if result.bundle_id.is_empty() {
            warn!("bundle result missing id");
            return;
        } else {
            result.bundle_id.clone()
        };

        let update = match interpret_bundle_result(&result) {
            Some(update) => update,
            None => return,
        };

        if let Some(mut entry) = self.pending.get_mut(&bundle_id) {
            self.emit_event(
                &bundle_id,
                &entry,
                update.status,
                update.slot,
                update.detail,
            );
            entry.last_status = Some(update.status);
            if update.terminal {
                drop(entry);
                self.pending.remove(&bundle_id);
            }
        } else {
            warn!(%bundle_id, "bundle result arrived for unknown bundle id");
        }
    }

    fn handle_auction_outcome(
        &self,
        bundle_id: &str,
        outcome: AuctionOutcome,
        slot: Option<u64>,
        detail: Option<String>,
    ) {
        if let Some(mut entry) = self.pending.get_mut(bundle_id) {
            let status = match outcome {
                AuctionOutcome::Won => BundleSubmissionStatus::AuctionWon,
                AuctionOutcome::Lost => BundleSubmissionStatus::AuctionLost,
            };
            self.emit_event(bundle_id, &entry, status, slot, detail);
            entry.last_status = Some(status);
        } else {
            debug!(bundle_id, "auction outcome for stale bundle");
        }
    }

    fn emit_event(
        &self,
        bundle_id: &str,
        entry: &PendingBundle,
        status: BundleSubmissionStatus,
        slot: Option<u64>,
        detail: Option<String>,
    ) {
        let detection_latency = entry.detected_at.elapsed();
        if let Some(metric_status) = telemetry_status(status) {
            telemetry::record_bundle_result(metric_status, detection_latency);
        }
        let event = BundleSubmissionEvent {
            seq: entry.seq,
            status,
            bundle_id: Some(bundle_id.to_string()),
            endpoint: Some(entry.endpoint.clone()),
            tip_lamports: entry.tip_lamports,
            detection_latency,
            slot,
            detail,
            error: None,
        };
        if let Err(err) = self.notifier.notify_bundle_submission(event) {
            warn!(%bundle_id, error = %err, "bundle tracker notifier failure");
        }
    }

    fn evict_expired(&self) {
        let ttl = Duration::from_secs(self.submission.pending_ttl_secs.max(30));
        let now = Instant::now();
        let expired: Vec<String> = self
            .pending
            .iter()
            .filter_map(|entry| {
                if now.duration_since(entry.submitted_at) > ttl {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        for key in expired {
            if let Some((_, entry)) = self.pending.remove(&key) {
                self.emit_event(
                    &key,
                    &entry,
                    BundleSubmissionStatus::Expired,
                    None,
                    Some("bundle tracking TTL reached without terminal status".into()),
                );
            }
        }

        let failed_expired: Vec<u64> = self
            .failed
            .iter()
            .filter_map(|entry| {
                if now.duration_since(entry.recorded_at) > ttl {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();
        for seq in failed_expired {
            self.failed.remove(&seq);
        }
    }
}

struct ResultUpdate {
    status: BundleSubmissionStatus,
    slot: Option<u64>,
    detail: Option<String>,
    terminal: bool,
}

fn interpret_bundle_result(result: &BundleResult) -> Option<ResultUpdate> {
    match result.result.as_ref()? {
        bundle_result::Result::Accepted(accepted) => Some(ResultUpdate {
            status: BundleSubmissionStatus::Accepted,
            slot: Some(accepted.slot),
            detail: Some(format!("validator {}", accepted.validator_identity)),
            terminal: false,
        }),
        bundle_result::Result::Rejected(rejected) => Some(ResultUpdate {
            status: BundleSubmissionStatus::Rejected,
            slot: None,
            detail: Some(rejection_reason(rejected)),
            terminal: true,
        }),
        bundle_result::Result::Finalized(_) => Some(ResultUpdate {
            status: BundleSubmissionStatus::Finalized,
            slot: None,
            detail: None,
            terminal: true,
        }),
        bundle_result::Result::Processed(processed) => Some(ResultUpdate {
            status: BundleSubmissionStatus::Processed,
            slot: Some(processed.slot),
            detail: Some(format!(
                "validator {} bundle_index {}",
                processed.validator_identity, processed.bundle_index
            )),
            terminal: false,
        }),
        bundle_result::Result::Dropped(dropped) => Some(ResultUpdate {
            status: BundleSubmissionStatus::Dropped,
            slot: None,
            detail: Some(format!("reason {:?}", dropped_reason_value(dropped))),
            terminal: true,
        }),
    }
}

fn rejection_reason(rejected: &bundle::Rejected) -> String {
    match rejected.reason.as_ref() {
        Some(bundle::rejected::Reason::StateAuctionBidRejected(reason)) => format!(
            "auction {} simulated_bid {}",
            reason.auction_id, reason.simulated_bid_lamports
        ),
        Some(bundle::rejected::Reason::WinningBatchBidRejected(reason)) => format!(
            "auction {} winning batch rejected simulated_bid {}",
            reason.auction_id, reason.simulated_bid_lamports
        ),
        Some(bundle::rejected::Reason::SimulationFailure(reason)) => {
            format!("simulation failure tx {}", reason.tx_signature)
        }
        Some(bundle::rejected::Reason::InternalError(reason)) => {
            format!("internal error {}", reason.msg)
        }
        Some(bundle::rejected::Reason::DroppedBundle(reason)) => {
            format!("dropped bundle {}", reason.msg)
        }
        None => "rejected".to_string(),
    }
}

fn telemetry_status(status: BundleSubmissionStatus) -> Option<BundleResultStatus> {
    match status {
        BundleSubmissionStatus::Accepted => Some(BundleResultStatus::Accepted),
        BundleSubmissionStatus::Rejected => Some(BundleResultStatus::Rejected),
        BundleSubmissionStatus::Processed => Some(BundleResultStatus::Processed),
        BundleSubmissionStatus::Finalized => Some(BundleResultStatus::Finalized),
        BundleSubmissionStatus::Dropped => Some(BundleResultStatus::Dropped),
        BundleSubmissionStatus::AuctionWon => Some(BundleResultStatus::AuctionWon),
        BundleSubmissionStatus::AuctionLost => Some(BundleResultStatus::AuctionLost),
        BundleSubmissionStatus::Expired => Some(BundleResultStatus::Expired),
        _ => None,
    }
}

fn spawn_result_listener(
    tracker: Arc<BundleTracker>,
    stop: Arc<AtomicBool>,
    endpoints: Vec<String>,
    auth: Arc<Keypair>,
    submission: SubmissionTrackingConfig,
    ca_pem: Option<Arc<Vec<u8>>>,
) -> Result<thread::JoinHandle<Result<()>>> {
    if endpoints.is_empty() {
        return Err(anyhow!("no endpoints available for bundle results stream"));
    }

    thread::Builder::new()
        .name("bundle_results".into())
        .spawn(move || {
            let runtime = Runtime::new().context("failed to start bundle results runtime")?;
            runtime.block_on(result_listener_loop(
                tracker, stop, endpoints, auth, submission, ca_pem,
            ))
        })
        .map_err(Into::into)
}

async fn result_listener_loop(
    tracker: Arc<BundleTracker>,
    stop: Arc<AtomicBool>,
    endpoints: Vec<String>,
    auth: Arc<Keypair>,
    submission: SubmissionTrackingConfig,
    ca_pem: Option<Arc<Vec<u8>>>,
) -> Result<()> {
    let base_backoff = Duration::from_millis(submission.result_stream_backoff_ms.max(100));
    let mut backoff = base_backoff;
    let mut endpoint_idx = 0usize;

    while !stop.load(Ordering::Relaxed) {
        let endpoint = endpoints[endpoint_idx % endpoints.len()].clone();
        endpoint_idx = endpoint_idx.wrapping_add(1);
        match connect_searcher(&endpoint, auth.clone(), ca_pem.clone()).await {
            Ok(mut client) => {
                match client
                    .subscribe_bundle_results(SubscribeBundleResultsRequest {})
                    .await
                {
                    Ok(response) => {
                        info!(target = "bundle_tracker", %endpoint, "subscribed bundle results");
                        backoff = base_backoff;
                        let mut stream = response.into_inner();
                        loop {
                            if stop.load(Ordering::Relaxed) {
                                return Ok(());
                            }
                            match timeout(Duration::from_secs(5), stream.message()).await {
                                Ok(Ok(Some(result))) => tracker.handle_bundle_result(result),
                                Ok(Ok(None)) => {
                                    warn!(target = "bundle_tracker", %endpoint, "bundle results stream closed by server");
                                    break;
                                }
                                Ok(Err(status)) => {
                                    warn!(target = "bundle_tracker", %endpoint, error = %status, "bundle results stream error");
                                    break;
                                }
                                Err(_) => {
                                    continue;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!(target = "bundle_tracker", %endpoint, error = %err, "failed to subscribe bundle results");
                    }
                }
            }
            Err(err) => {
                warn!(target = "bundle_tracker", %endpoint, error = %err, "failed to connect results stream client");
            }
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }

    Ok(())
}

fn spawn_status_poller(
    tracker: Arc<BundleTracker>,
    stop: Arc<AtomicBool>,
    endpoints: Vec<String>,
    api_token: Option<String>,
    submission: SubmissionTrackingConfig,
) -> Result<thread::JoinHandle<Result<()>>> {
    if endpoints.is_empty() {
        return Err(anyhow!("no endpoints available for status poller"));
    }

    thread::Builder::new()
        .name("bundle_status_poller".into())
        .spawn(move || {
            let interval = Duration::from_secs(submission.status_poll_interval_secs.max(1));
            let batch_size = submission.status_poll_batch_size.max(1);
            let timeout_secs = submission.status_poll_timeout_secs.max(1);
            let client = BlockingClient::builder()
                .timeout(Duration::from_secs(timeout_secs))
                .build()
                .context("failed to initialize bundle status HTTP client")?;
            status_poller_loop(
                tracker, stop, client, endpoints, api_token, interval, batch_size,
            )
        })
        .map_err(Into::into)
}

fn status_poller_loop(
    tracker: Arc<BundleTracker>,
    stop: Arc<AtomicBool>,
    client: BlockingClient,
    endpoints: Vec<String>,
    api_token: Option<String>,
    interval: Duration,
    batch_size: usize,
) -> Result<()> {
    info!(
        target = "bundle_tracker",
        poll_interval_secs = interval.as_secs(),
        batch_size,
        "bundle status poller online"
    );

    while !stop.load(Ordering::Relaxed) {
        let targets = tracker.pending_poll_targets();
        if targets.is_empty() {
            interruptible_sleep(&stop, interval);
            continue;
        }

        for chunk in targets.chunks(batch_size) {
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }
            for target in chunk {
                if let Some(update) =
                    poll_bundle_status(&client, &endpoints, target, api_token.as_deref())
                {
                    tracker.apply_polled_status(&target.bundle_id, update);
                }
            }
        }

        interruptible_sleep(&stop, interval);
    }

    Ok(())
}

fn poll_bundle_status(
    client: &BlockingClient,
    endpoints: &[String],
    target: &StatusPollTarget,
    api_token: Option<&str>,
) -> Option<PolledBundleStatus> {
    for endpoint in endpoint_poll_order(&target.endpoint, endpoints) {
        let url = build_bundle_status_url(&endpoint, &target.bundle_id);
        let mut request = client.get(&url);
        if let Some(token) = api_token {
            request = request.header("x-api-key", token).bearer_auth(token);
        }
        match request.send() {
            Ok(response) => match response.status() {
                StatusCode::OK => match response.json::<Value>() {
                    Ok(value) => {
                        if let Some(update) = extract_polled_status(value) {
                            debug!(
                                target = "bundle_tracker",
                                endpoint = %endpoint,
                                bundle_id = %target.bundle_id,
                                status = ?update.status,
                                "bundle status polled"
                            );
                            return Some(update);
                        }
                    }
                    Err(err) => {
                        warn!(
                            target = "bundle_tracker",
                            endpoint = %endpoint,
                            bundle_id = %target.bundle_id,
                            error = %err,
                            "failed to decode bundle status payload"
                        );
                    }
                },
                StatusCode::NOT_FOUND => {}
                status => {
                    warn!(
                        target = "bundle_tracker",
                        endpoint = %endpoint,
                        bundle_id = %target.bundle_id,
                        status = %status,
                        "bundle status poll unexpected status"
                    );
                }
            },
            Err(err) => {
                warn!(
                    target = "bundle_tracker",
                    endpoint = %endpoint,
                    bundle_id = %target.bundle_id,
                    error = %err,
                    "bundle status poll request failed"
                );
            }
        }
    }
    None
}

fn build_bundle_status_url(endpoint: &str, bundle_id: &str) -> String {
    format!(
        "{}/api/v1/bundles/by-uuid?uuid={}",
        endpoint.trim_end_matches('/'),
        bundle_id
    )
}

fn extract_polled_status(value: Value) -> Option<PolledBundleStatus> {
    if let Some(array) = value.as_array() {
        for entry in array {
            if let Some(update) = interpret_polled_entry(entry) {
                return Some(update);
            }
        }
    }

    if let Some(obj) = value.as_object() {
        if let Some(inner) = obj.get("data") {
            if let Some(update) = extract_polled_status(inner.clone()) {
                return Some(update);
            }
        }
        if let Some(inner) = obj.get("result") {
            if let Some(update) = extract_polled_status(inner.clone()) {
                return Some(update);
            }
        }
        if let Some(inner) = obj.get("bundle") {
            if let Some(update) = interpret_polled_entry(inner) {
                return Some(update);
            }
        }
    }

    interpret_polled_entry(&value)
}

fn interpret_polled_entry(entry: &Value) -> Option<PolledBundleStatus> {
    let obj = entry.as_object()?;
    let label = obj
        .get("conclusion")
        .and_then(|v| v.as_str())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| obj.get("state").and_then(|v| v.as_str()))
        .or_else(|| obj.get("status").and_then(|v| v.as_str()))?;
    let (status, terminal) = classify_status_label(label)?;
    let slot = obj
        .get("landed_slot")
        .or_else(|| obj.get("slot"))
        .or_else(|| obj.get("processed_slot"))
        .and_then(|v| v.as_u64());
    let detail = obj
        .get("dropped_reason")
        .or_else(|| obj.get("error"))
        .or_else(|| obj.get("message"))
        .or_else(|| obj.get("reason"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some(PolledBundleStatus {
        status,
        slot,
        detail,
        terminal,
    })
}

fn classify_status_label(raw: &str) -> Option<(BundleSubmissionStatus, bool)> {
    let label = raw.trim().to_ascii_lowercase();
    if label.is_empty() || label == "none" {
        return None;
    }

    let status = match label.as_str() {
        "submitted" | "pending" | "queued" | "received" => {
            (BundleSubmissionStatus::Submitted, false)
        }
        "accepted" | "build" | "built" | "acknowledged" => {
            (BundleSubmissionStatus::Accepted, false)
        }
        "processed" | "processing" | "simulated" => (BundleSubmissionStatus::Processed, false),
        "finalized" | "landed" | "confirmed" => (BundleSubmissionStatus::Finalized, true),
        "rejected" | "failed" | "failure" => (BundleSubmissionStatus::Rejected, true),
        "dropped" => (BundleSubmissionStatus::Dropped, true),
        "expired" => (BundleSubmissionStatus::Expired, true),
        _ => return None,
    };

    Some(status)
}

fn endpoint_poll_order(primary: &str, endpoints: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut order = Vec::new();
    if !primary.is_empty() && seen.insert(primary.to_string()) {
        order.push(primary.to_string());
    }
    for endpoint in endpoints {
        if seen.insert(endpoint.clone()) {
            order.push(endpoint.clone());
        }
    }
    order
}

fn interruptible_sleep(stop: &Arc<AtomicBool>, duration: Duration) {
    if duration.is_zero() {
        return;
    }
    let mut remaining = duration;
    let quantum = Duration::from_millis(100);
    while remaining > Duration::ZERO && !stop.load(Ordering::Relaxed) {
        let sleep_for = if remaining > quantum {
            quantum
        } else {
            remaining
        };
        thread::sleep(sleep_for);
        remaining = remaining.saturating_sub(sleep_for);
    }
}

fn spawn_auction_listener(
    tracker: Arc<BundleTracker>,
    stop: Arc<AtomicBool>,
    url: String,
    submission: SubmissionTrackingConfig,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("auction_stream".into())
        .spawn(move || {
            let runtime = Runtime::new().context("failed to start auction stream runtime")?;
            runtime.block_on(auction_listener_loop(tracker, stop, url, submission))
        })
        .map_err(Into::into)
}

async fn auction_listener_loop(
    tracker: Arc<BundleTracker>,
    stop: Arc<AtomicBool>,
    url: String,
    submission: SubmissionTrackingConfig,
) -> Result<()> {
    let base_backoff = Duration::from_millis(submission.auction_stream_backoff_ms.max(250));
    let mut backoff = base_backoff;

    while !stop.load(Ordering::Relaxed) {
        match connect_async(&url).await {
            Ok((mut socket, _)) => {
                info!(target = "bundle_tracker", url = %url, "auction stream connected");
                backoff = base_backoff;
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => handle_auction_message(&tracker, &text),
                        Ok(Message::Binary(bytes)) => {
                            if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                                handle_auction_message(&tracker, &text);
                            }
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => {
                            warn!(target = "bundle_tracker", "auction stream closed by server");
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            warn!(target = "bundle_tracker", error = %err, "auction stream error");
                            break;
                        }
                    }

                    if stop.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                }
            }
            Err(err) => {
                if should_suppress_auction_stream_error(&err) {
                    debug!(target = "bundle_tracker", url = %url, error = %err, "auction stream unavailable");
                } else {
                    warn!(target = "bundle_tracker", url = %url, error = %err, "failed to connect auction stream");
                }
            }
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }

    Ok(())
}

fn handle_auction_message(tracker: &Arc<BundleTracker>, payload: &str) {
    match serde_json::from_str::<AuctionEvent>(payload) {
        Ok(event) => {
            if let Some(bundle_id) = event.bundle_id.as_deref() {
                if let Some(outcome) = event.outcome() {
                    tracker.handle_auction_outcome(
                        bundle_id,
                        outcome,
                        event.slot,
                        event.describe_detail(),
                    );
                }
            }
        }
        Err(err) => {
            debug!(target = "bundle_tracker", error = %err, payload, "failed to parse auction event");
        }
    }
}

fn should_suppress_auction_stream_error(err: &WsError) -> bool {
    matches!(err, WsError::Http(response) if response.status().as_u16() == 404)
}

fn spawn_sweeper(
    tracker: Arc<BundleTracker>,
    stop: Arc<AtomicBool>,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("bundle_tracker_sweeper".into())
        .spawn(move || {
            let interval = Duration::from_secs(tracker.submission.sweep_interval_secs.max(1));
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(interval);
                tracker.evict_expired();
            }
            Ok(())
        })
        .map_err(Into::into)
}

#[derive(Debug, Deserialize)]
struct AuctionEvent {
    bundle_id: Option<String>,
    auction_id: Option<String>,
    outcome: Option<String>,
    status: Option<String>,
    winning: Option<bool>,
    slot: Option<u64>,
}

impl AuctionEvent {
    fn outcome(&self) -> Option<AuctionOutcome> {
        if let Some(flag) = self.winning {
            return Some(if flag {
                AuctionOutcome::Won
            } else {
                AuctionOutcome::Lost
            });
        }
        let label = self
            .status
            .as_deref()
            .or(self.outcome.as_deref())?
            .to_ascii_lowercase();
        if matches!(label.as_str(), "winning" | "won" | "accepted") {
            Some(AuctionOutcome::Won)
        } else if matches!(label.as_str(), "losing" | "lost" | "rejected") {
            Some(AuctionOutcome::Lost)
        } else {
            None
        }
    }

    fn describe_detail(&self) -> Option<String> {
        if self.auction_id.is_none() && self.status.is_none() {
            return None;
        }
        Some(format!(
            "auction {:?} status {:?}",
            self.auction_id.as_deref().unwrap_or("unknown"),
            self.status
                .as_deref()
                .or(self.outcome.as_deref())
                .unwrap_or("unknown")
        ))
    }
}

enum AuctionOutcome {
    Won,
    Lost,
}

fn dropped_reason_value(drop: &bundle::Dropped) -> DroppedReason {
    DroppedReason::try_from(drop.reason).unwrap_or(DroppedReason::BlockhashExpired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classify_status_label_maps_states() {
        let (status, terminal) = classify_status_label("accepted").expect("status");
        assert_eq!(status, BundleSubmissionStatus::Accepted);
        assert!(!terminal);

        let (status, terminal) = classify_status_label("finalized").expect("status");
        assert_eq!(status, BundleSubmissionStatus::Finalized);
        assert!(terminal);
    }

    #[test]
    fn extract_polled_status_handles_nested_payloads() {
        let payload = json!({
            "data": [{
                "status": "processed",
                "slot": 42
            }]
        });

        let update = extract_polled_status(payload).expect("polled status");
        assert_eq!(update.status, BundleSubmissionStatus::Processed);
        assert_eq!(update.slot, Some(42));
        assert!(!update.terminal);
    }
}
