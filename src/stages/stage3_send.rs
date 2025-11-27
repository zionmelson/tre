use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use tracing::{debug, info, warn};
const SYSTEM_PROGRAM_ID: Pubkey = pubkey!("11111111111111111111111111111111");

use crate::builder::{BuilderHandle, BuilderStage, BundleBuildContext};
use crate::bundle::{BundleClient, BundleSubmitResult, BundleTracker, PendingSubmission};
use crate::core::{pin_current_thread, Lease, RingConsumer};
use crate::notifier::{BundleSubmissionEvent, BundleSubmissionStatus, Notifier};
use crate::stages::types::{BundleCandidate, RoutePlan};
use crate::telemetry::{self, Stage3RetryEvent, Stage3RetryReason, Stage3SubmissionStatus};

const MAX_STAGE3_ATTEMPTS: u8 = 3;
const STAGE3_RETRY_QUEUE_CAPACITY: usize = 128;

pub trait Stage3Builder: Send + Sync {
    fn payer_pubkey(&self) -> Pubkey;
    fn sign_bundle(&self, ctx: &BundleBuildContext) -> Result<VersionedTransaction>;
    fn heartbeat(&self, stage: BuilderStage);
}

pub trait Stage3BundleClient: Send + Sync {
    fn submit(&self, tx: &VersionedTransaction) -> Result<BundleSubmitResult>;
}

pub trait Stage3Notifier: Send + Sync {
    fn notify_bundle_submission(&self, event: BundleSubmissionEvent) -> Result<()>;
}

pub trait Stage3Tracker: Send + Sync {
    fn track_submission(&self, submission: PendingSubmission);
    fn record_stage3_failure(
        &self,
        seq: u64,
        detected_at: Instant,
        tip_lamports: Option<u64>,
        detail: String,
    );
}

pub fn spawn_send_stage<B, C, N, T>(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    builder: B,
    client: Arc<C>,
    notifier: Arc<N>,
    tracker: Arc<T>,
) -> Result<thread::JoinHandle<Result<()>>>
where
    B: Stage3Builder + Send + 'static,
    C: Stage3BundleClient + 'static,
    N: Stage3Notifier + 'static,
    T: Stage3Tracker + 'static,
{
    thread::Builder::new()
        .name("stage3_send".into())
        .spawn(move || run(stop, core_id, consumer, builder, client, notifier, tracker))
        .map_err(Into::into)
}

fn run<B, C, N, T>(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    builder: B,
    client: Arc<C>,
    notifier: Arc<N>,
    tracker: Arc<T>,
) -> Result<()>
where
    B: Stage3Builder,
    C: Stage3BundleClient,
    N: Stage3Notifier,
    T: Stage3Tracker,
{
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    let mut retry_queue: VecDeque<Lease<BundleCandidate>> = VecDeque::new();

    info!("stage3 send online");
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::SubmitHotPath);
        if let Some((bundle, source)) = pop_bundle(&consumer, &mut retry_queue) {
            process_bundle(
                &builder,
                client.as_ref(),
                notifier.as_ref(),
                tracker.as_ref(),
                bundle,
                source,
                &mut retry_queue,
            );
        } else {
            thread::yield_now();
            thread::sleep(Duration::from_millis(10));
        }
    }

    info!("stage3 send shutting down");
    Ok(())
}

fn pop_bundle(
    consumer: &RingConsumer<Lease<BundleCandidate>>,
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
) -> Option<(Lease<BundleCandidate>, BundleSource)> {
    if let Some(bundle) = retry_queue.pop_front() {
        Some((bundle, BundleSource::Retry))
    } else {
        consumer.pop().map(|bundle| (bundle, BundleSource::Fresh))
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum BundleSource {
    Fresh,
    Retry,
}

fn process_bundle<B, C, N, T>(
    builder: &B,
    client: &C,
    notifier: &N,
    tracker: &T,
    lease: Lease<BundleCandidate>,
    source: BundleSource,
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
) where
    B: Stage3Builder,
    C: Stage3BundleClient,
    N: Stage3Notifier,
    T: Stage3Tracker,
{
    let hop_count = lease
        .route_plan
        .as_ref()
        .map(|plan| plan.legs.len())
        .unwrap_or(0);
    let route_summary = lease
        .route_plan
        .as_ref()
        .map(describe_route_plan)
        .unwrap_or_else(|| lease.pool_id.clone());
    debug!(
        seq = lease.seq,
        source = ?source,
        hop_count,
        has_context = lease.bundle_context.is_some(),
        planned_ix = lease.instructions.len(),
        tip_lamports = lease.tip_lamports,
        route = %route_summary,
        "stage3 processing bundle candidate"
    );
    match attempt_submit(&lease, builder, client) {
        Ok((result, tip_lamports)) => {
            handle_success(lease, result, tip_lamports, notifier, tracker, source);
        }
        Err(err) => handle_failure(
            lease,
            err,
            builder.payer_pubkey(),
            notifier,
            tracker,
            retry_queue,
        ),
    }
}

fn record_stage3_outcome(
    lease: &Lease<BundleCandidate>,
    status: Stage3SubmissionStatus,
    bundle_id: Option<&str>,
    tip_lamports: Option<u64>,
) -> Duration {
    let latency = lease.created_at.elapsed();
    telemetry::record_stage3_latency(latency);
    telemetry::record_stage3_outcome(status, bundle_id, tip_lamports);
    latency
}

fn extract_tip_lamports(context: &BundleBuildContext, payer: Pubkey) -> Option<u64> {
    let mut total = 0u64;
    for ix in &context.instructions {
        if ix.program_id != SYSTEM_PROGRAM_ID {
            continue;
        }
        if payer_account(ix.accounts.as_slice()) != Some(payer) {
            continue;
        }
        if let Some(lamports) = decode_transfer_lamports(&ix.data) {
            total = total.saturating_add(lamports);
        }
    }
    if total > 0 {
        Some(total)
    } else {
        None
    }
}

fn payer_account(accounts: &[AccountMeta]) -> Option<Pubkey> {
    accounts.first().map(|meta| meta.pubkey)
}

fn decode_transfer_lamports(data: &[u8]) -> Option<u64> {
    if data.len() < 12 {
        return None;
    }
    let discr = u32::from_le_bytes(data[0..4].try_into().ok()?);
    const TRANSFER_OPCODE: u32 = 2;
    if discr != TRANSFER_OPCODE {
        return None;
    }
    let lamports = u64::from_le_bytes(data[4..12].try_into().ok()?);
    Some(lamports)
}

fn emit_submission_alert<N: Stage3Notifier>(notifier: &N, event: BundleSubmissionEvent) {
    let seq = event.seq;
    if let Err(err) = notifier.notify_bundle_submission(event) {
        warn!(seq, error = %err, "failed to dispatch bundle submission alert");
    }
}

fn attempt_submit<B, C>(
    lease: &Lease<BundleCandidate>,
    builder: &B,
    client: &C,
) -> Result<(BundleSubmitResult, Option<u64>), Stage3Error>
where
    B: Stage3Builder,
    C: Stage3BundleClient,
{
    let context = lease
        .bundle_context
        .as_ref()
        .ok_or(Stage3Error::MissingContext)?;
    if let Some(plan) = lease.route_plan.as_ref() {
        info!(
            seq = lease.seq,
            hops = plan.legs.len(),
            route = %describe_route_plan(plan),
            "stage3 delivering route plan to bundle builder"
        );
    } else {
        info!(
            seq = lease.seq,
            "stage3 delivering bundle without route plan to builder"
        );
    }
    let payer = builder.payer_pubkey();
    let tip_lamports = extract_tip_lamports(context, payer);
    debug!(
        seq = lease.seq,
        instructions = context.instructions.len(),
        lookup_tables = context.lookup_tables.len(),
        tip_lamports,
        "stage3 preparing bundle for signing"
    );
    let tx = builder.sign_bundle(context).map_err(Stage3Error::Sign)?;
    debug!(seq = lease.seq, "stage3 bundle signed");
    info!(
        seq = lease.seq,
        instructions = context.instructions.len(),
        "stage3 forwarding signed bundle to jito client"
    );
    let result = client.submit(&tx).map_err(Stage3Error::Submit)?;
    debug!(
        seq = lease.seq,
        endpoint = %result.endpoint.as_str(),
        bundle_id = %result.bundle_id.as_str(),
        "stage3 dispatched bundle to client"
    );
    Ok((result, tip_lamports))
}

fn handle_success<N, T>(
    lease: Lease<BundleCandidate>,
    submit_result: BundleSubmitResult,
    tip_lamports: Option<u64>,
    notifier: &N,
    tracker: &T,
    source: BundleSource,
) where
    N: Stage3Notifier,
    T: Stage3Tracker,
{
    let seq = lease.seq;
    let detection_latency = record_stage3_outcome(
        &lease,
        Stage3SubmissionStatus::Success,
        Some(&submit_result.bundle_id),
        tip_lamports,
    );
    if matches!(source, BundleSource::Retry) {
        telemetry::record_stage3_retry_event(Stage3RetryEvent::RetryFlushed);
    }
    info!(
        seq,
        endpoint = %submit_result.endpoint,
        bundle_id = %submit_result.bundle_id,
        stage3_latency_ms = detection_latency.as_secs_f64() * 1_000.0,
        "bundle submitted via gRPC"
    );
    tracker.track_submission(PendingSubmission {
        bundle_id: submit_result.bundle_id.clone(),
        seq,
        endpoint: submit_result.endpoint.clone(),
        detected_at: lease.created_at,
        tip_lamports,
    });
    emit_submission_alert(
        notifier,
        BundleSubmissionEvent {
            seq,
            status: BundleSubmissionStatus::Submitted,
            bundle_id: Some(submit_result.bundle_id),
            endpoint: Some(submit_result.endpoint),
            tip_lamports,
            detection_latency,
            slot: None,
            detail: None,
            error: None,
        },
    );
}

fn handle_failure<N, T>(
    mut lease: Lease<BundleCandidate>,
    err: Stage3Error,
    payer: Pubkey,
    notifier: &N,
    tracker: &T,
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
) where
    N: Stage3Notifier,
    T: Stage3Tracker,
{
    let seq = lease.seq;
    let attempt = lease.stage3_attempts.saturating_add(1);
    lease.stage3_attempts = attempt;
    let detail = err.detail();
    let reason = err.reason();
    warn!(seq, attempt, error = %detail, "stage3 submission failure");

    let retryable = err.retryable();
    if retryable && attempt < MAX_STAGE3_ATTEMPTS && retry_queue.len() < STAGE3_RETRY_QUEUE_CAPACITY
    {
        telemetry::record_stage3_retry_event(Stage3RetryEvent::RetryQueued(reason));
        retry_queue.push_back(lease);
        return;
    }

    if retryable {
        telemetry::record_stage3_retry_event(Stage3RetryEvent::RetriesExhausted(reason));
    } else {
        telemetry::record_stage3_retry_event(Stage3RetryEvent::FailureRecorded(reason));
    }

    let tip_lamports = lease
        .bundle_context
        .as_ref()
        .and_then(|ctx| extract_tip_lamports(ctx, payer));
    let latency =
        record_stage3_outcome(&lease, Stage3SubmissionStatus::Failure, None, tip_lamports);
    emit_submission_alert(
        notifier,
        BundleSubmissionEvent {
            seq,
            status: BundleSubmissionStatus::Failed,
            bundle_id: None,
            endpoint: None,
            tip_lamports,
            detection_latency: latency,
            slot: None,
            detail: Some(detail.clone()),
            error: Some(detail.clone()),
        },
    );
    tracker.record_stage3_failure(seq, lease.created_at, tip_lamports, detail);
}

#[derive(Debug)]
enum Stage3Error {
    MissingContext,
    Sign(anyhow::Error),
    Submit(anyhow::Error),
}

impl Stage3Error {
    fn retryable(&self) -> bool {
        !matches!(self, Stage3Error::MissingContext)
    }

    fn detail(&self) -> String {
        match self {
            Stage3Error::MissingContext => "bundle context missing".to_string(),
            Stage3Error::Sign(err) => err.to_string(),
            Stage3Error::Submit(err) => err.to_string(),
        }
    }

    fn reason(&self) -> Stage3RetryReason {
        match self {
            Stage3Error::MissingContext => Stage3RetryReason::Context,
            Stage3Error::Sign(_) => Stage3RetryReason::Sign,
            Stage3Error::Submit(_) => Stage3RetryReason::Submit,
        }
    }
}

impl std::fmt::Display for Stage3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.detail())
    }
}

impl std::error::Error for Stage3Error {}

impl Stage3Builder for BuilderHandle {
    fn payer_pubkey(&self) -> Pubkey {
        BuilderHandle::payer_pubkey(self)
    }

    fn sign_bundle(&self, ctx: &BundleBuildContext) -> Result<VersionedTransaction> {
        BuilderHandle::sign_bundle(self, ctx)
    }

    fn heartbeat(&self, stage: BuilderStage) {
        BuilderHandle::heartbeat(self, stage)
    }
}

impl Stage3BundleClient for BundleClient {
    fn submit(&self, tx: &VersionedTransaction) -> Result<BundleSubmitResult> {
        BundleClient::submit(self, tx)
    }
}

fn describe_route_plan(plan: &RoutePlan) -> String {
    plan.legs
        .iter()
        .map(|leg| format!("{}->{}@{}", leg.input_mint, leg.output_mint, leg.pool_id))
        .collect::<Vec<_>>()
        .join(" | ")
}

impl Stage3Notifier for Notifier {
    fn notify_bundle_submission(&self, event: BundleSubmissionEvent) -> Result<()> {
        Notifier::notify_bundle_submission(self, event)
    }
}

impl Stage3Tracker for BundleTracker {
    fn track_submission(&self, submission: PendingSubmission) {
        BundleTracker::track_submission(self, submission)
    }

    fn record_stage3_failure(
        &self,
        seq: u64,
        detected_at: Instant,
        tip_lamports: Option<u64>,
        detail: String,
    ) {
        BundleTracker::record_stage3_failure(self, seq, detected_at, tip_lamports, detail)
    }
}
