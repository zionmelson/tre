use std::collections::{hash_map::DefaultHasher, VecDeque};
use std::hash::Hasher;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bincode::serialize;
use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use reqwest::blocking::Client;
use serde_json::json;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use tonic::{Code, Status};
use tracing::{debug, info, warn, Level};
const SYSTEM_PROGRAM_ID: Pubkey = pubkey!("11111111111111111111111111111111");

use crate::builder::{BuilderHandle, BuilderStage, BundleBuildContext, FlashLoanRoutePlan};
use crate::bundle::{BundleClient, BundleSubmitResult, BundleTracker, PendingSubmission};
use crate::cluster::{GateBlockReason, GateVerdict, LeaderScheduleGate};
use crate::core::{pin_current_thread, Lease, RingConsumer};
use crate::notifier::{BundleSubmissionEvent, BundleSubmissionStatus, Notifier};
use crate::stages::types::{BundleCandidate, RoutePlan};
use crate::telemetry::{self, Stage3RetryEvent, Stage3RetryReason, Stage3SubmissionStatus};
use crate::tip_pressure;

const MAX_STAGE3_ATTEMPTS: u8 = 3;
const STAGE3_RETRY_QUEUE_CAPACITY: usize = 128;
const DEFAULT_RATE_LIMIT_BACKOFF_MS: u64 = 1_000;
const RATE_LIMIT_JITTER_MS: u64 = 500;
const MIN_SUBMISSION_INTERVAL_MS: u64 = 1_000;
const SIMULATION_RECV_TIMEOUT_MS: u64 = 100;
const SYSTEM_PROGRAM_ID_STR: &str = "11111111111111111111111111111111";
const COMPUTE_BUDGET_PROGRAM_ID_STR: &str = "ComputeBudget111111111111111111111111111111";
const TOKEN_PROGRAM_ID_STR: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ASSOCIATED_TOKEN_PROGRAM_ID_STR: &str = "ATokenGPvR93dthLvYbAphit9L8ZsfsZp7r4HQJDebg";
const MEMO_PROGRAM_ID_STR: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const JITO_DONT_FRONT_PROGRAM_ID_STR: &str = "jitodontfront111111111111111111111111111111";
const RAYDIUM_V3_PROGRAM_ID_STR: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const CP_AMM_PROGRAM_ID_STR: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
const LB_CLMM_PROGRAM_ID_STR: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const LIFINITY_V2_PROGRAM_ID_STR: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";
const WHIRLPOOL_PROGRAM_ID_STR: &str = "whirLbMiicV1cqQhBYtbJX3Yq9z7Dfc7gXLMGr5vLyN";

#[derive(Debug)]
#[allow(dead_code)]
struct RouteLegLog {
    index: usize,
    protocol: &'static str,
    program_id: String,
    pool_id: String,
    direction: String,
    input_amount: u64,
    estimated_output_amount: u64,
    fee_bps: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct InstructionAccountLog {
    pubkey: String,
    is_signer: bool,
    is_writable: bool,
}

#[derive(Debug)]
#[allow(dead_code)]
struct InstructionLog {
    index: usize,
    program_id: String,
    label: &'static str,
    data_len: usize,
    accounts: Vec<InstructionAccountLog>,
}

#[derive(Clone)]
struct BundleSimulator {
    sender: Sender<SimulationJob>,
}

impl BundleSimulator {
    fn spawn(stop: Arc<AtomicBool>, endpoints: Vec<String>) -> Result<Option<Self>> {
        if endpoints.is_empty() {
            return Ok(None);
        }
        let (sender, receiver) = unbounded();
        let worker = BundleSimulatorWorker::new(stop, endpoints)?;
        thread::Builder::new()
            .name("bundle_simulator".into())
            .spawn(move || worker.run(receiver))?;
        Ok(Some(Self { sender }))
    }

    fn enqueue(&self, seq: u64, payload: String) {
        if let Err(err) = self.sender.send(SimulationJob { seq, payload }) {
            warn!(seq, error = %err, "failed to enqueue bundle simulation job");
        }
    }
}

struct SimulationJob {
    seq: u64,
    payload: String,
}

struct BundleSimulatorWorker {
    stop: Arc<AtomicBool>,
    endpoints: Vec<String>,
    next_endpoint: usize,
    client: Client,
}

impl BundleSimulatorWorker {
    fn new(stop: Arc<AtomicBool>, endpoints: Vec<String>) -> Result<Self> {
        let client = Client::builder().timeout(Duration::from_secs(3)).build()?;
        Ok(Self {
            stop,
            endpoints,
            next_endpoint: 0,
            client,
        })
    }

    fn run(mut self, receiver: Receiver<SimulationJob>) {
        while !self.stop.load(Ordering::Relaxed) {
            match receiver.recv_timeout(Duration::from_millis(SIMULATION_RECV_TIMEOUT_MS)) {
                Ok(job) => self.process_job(job),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        while let Ok(job) = receiver.try_recv() {
            self.process_job(job);
        }
    }

    fn process_job(&mut self, job: SimulationJob) {
        let url = self.next_endpoint();
        let request_body = json!({
            "jsonrpc": "2.0",
            "id": job.seq,
            "method": "simulateBundle",
            "params": [{
                "encodedTransactions": [job.payload],
                "replaceRecentBlockhash": true,
                "skipSigVerify": true,
            }],
        });
        match self
            .client
            .post(&url)
            .json(&request_body)
            .send()
            .and_then(|resp| resp.text())
        {
            Ok(body) => {
                info!(seq = job.seq, endpoint = %url, response = %body, "quicknode simulateBundle response")
            }
            Err(err) => {
                warn!(seq = job.seq, endpoint = %url, error = %err, "quicknode simulateBundle request failed")
            }
        }
    }

    fn next_endpoint(&mut self) -> String {
        let url = self.endpoints[self.next_endpoint].clone();
        self.next_endpoint = (self.next_endpoint + 1) % self.endpoints.len();
        url
    }
}

pub trait Stage3Builder: Send + Sync {
    fn payer_pubkey(&self) -> Pubkey;
    fn sign_bundle(&self, ctx: &BundleBuildContext) -> Result<VersionedTransaction>;
    fn heartbeat(&self, stage: BuilderStage);
    fn release_flash_loan_route(&self, plan: &FlashLoanRoutePlan);
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
    leader_gate: Arc<LeaderScheduleGate>,
    simulation_rpcs: Vec<String>,
) -> Result<thread::JoinHandle<Result<()>>>
where
    B: Stage3Builder + Send + 'static,
    C: Stage3BundleClient + 'static,
    N: Stage3Notifier + 'static,
    T: Stage3Tracker + 'static,
{
    let simulator = BundleSimulator::spawn(stop.clone(), simulation_rpcs)?;
    thread::Builder::new()
        .name("stage3_send".into())
        .spawn(move || {
            run(
                stop,
                core_id,
                consumer,
                builder,
                client,
                notifier,
                tracker,
                leader_gate,
                simulator,
            )
        })
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
    leader_gate: Arc<LeaderScheduleGate>,
    simulator: Option<BundleSimulator>,
) -> Result<()>
where
    B: Stage3Builder,
    C: Stage3BundleClient,
    N: Stage3Notifier,
    T: Stage3Tracker,
{
    let simulator_ref = simulator.as_ref();
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    let mut retry_queue: VecDeque<Lease<BundleCandidate>> = VecDeque::new();
    let mut last_gate_reason: Option<GateBlockReason> = None;
    let mut last_gate_leader: Option<String> = None;
    let mut last_gate_log = Instant::now();
    let mut next_submit_allowed = Instant::now();

    info!("stage3 send online");
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::SubmitHotPath);
        match leader_gate.verdict() {
            GateVerdict::Allowed { slot, leader } => {
                if last_gate_reason.take().is_some() {
                    info!(slot, leader = %leader, "leader schedule gate cleared");
                }
                last_gate_leader = None;
            }
            GateVerdict::Blocked {
                reason,
                slot,
                leader,
                snapshot_age_ms,
                last_error,
            } => {
                if last_gate_reason != Some(reason)
                    || last_gate_leader != leader
                    || last_gate_log.elapsed() >= Duration::from_secs(1)
                {
                    match reason {
                        GateBlockReason::NotBamLeader => debug!(
                            slot,
                            leader = leader.as_deref(),
                            snapshot_age_ms,
                            "stage3 gated: current leader not running BAM"
                        ),
                        GateBlockReason::LeaderUnknown => warn!(
                            slot,
                            leader = leader.as_deref(),
                            snapshot_age_ms,
                            error = last_error.as_deref(),
                            "stage3 gated: leader identity unavailable"
                        ),
                        GateBlockReason::SnapshotStale => warn!(
                            slot,
                            leader = leader.as_deref(),
                            snapshot_age_ms,
                            error = last_error.as_deref(),
                            "stage3 gated: leader snapshot stale"
                        ),
                    }
                    last_gate_reason = Some(reason);
                    last_gate_leader = leader.clone();
                    last_gate_log = Instant::now();
                }
                thread::sleep(Duration::from_millis(5));
                continue;
            }
        }
        if let Some((bundle, source)) = pop_bundle(&consumer, &mut retry_queue) {
            let now = Instant::now();
            if now < next_submit_allowed {
                let delay = next_submit_allowed - now;
                debug!(
                    delay_ms = delay.as_millis(),
                    "stage3 throttling bundle submission"
                );
                thread::sleep(delay);
            }
            process_bundle(
                &builder,
                client.as_ref(),
                notifier.as_ref(),
                tracker.as_ref(),
                bundle,
                source,
                &mut retry_queue,
                simulator_ref,
            );
            next_submit_allowed =
                Instant::now() + Duration::from_millis(MIN_SUBMISSION_INTERVAL_MS.max(1));
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
    if let Some(front) = retry_queue.front() {
        if Instant::now() >= bundle_ready_deadline(front) {
            return retry_queue
                .pop_front()
                .map(|bundle| (bundle, BundleSource::Retry));
        }
    }
    consumer.pop().map(|bundle| (bundle, BundleSource::Fresh))
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
    simulator: Option<&BundleSimulator>,
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
        flash_borrow = lease.flash_loan.as_ref().map(|f| f.borrow_amount),
        flash_token = lease.flash_loan.as_ref().map(|f| f.token.as_str()),
        route = %route_summary,
        "stage3 processing bundle candidate"
    );
    match attempt_submit(&lease, builder, client, simulator) {
        Ok((result, tip_lamports)) => {
            handle_success(
                builder,
                lease,
                result,
                tip_lamports,
                notifier,
                tracker,
                source,
            );
        }
        Err(err) => handle_failure(builder, lease, err, notifier, tracker, retry_queue),
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
    tip_pressure::record_stage3_latency(latency);
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
    simulator: Option<&BundleSimulator>,
) -> Result<(BundleSubmitResult, Option<u64>), Stage3Error>
where
    B: Stage3Builder,
    C: Stage3BundleClient,
{
    let context = lease
        .bundle_context
        .as_ref()
        .ok_or(Stage3Error::MissingContext)?;
    log_bundle_blueprint(lease, context);
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
    if let Some(encoded) = encode_bundle_base64(&tx) {
        info!(seq = lease.seq, payload_base64 = %encoded, "stage3 bundle payload encoded");
        if let Some(sim) = simulator {
            sim.enqueue(lease.seq, encoded);
        }
    }
    debug!(seq = lease.seq, "stage3 bundle signed");
    info!(
        seq = lease.seq,
        instructions = context.instructions.len(),
        "stage3 forwarding signed bundle to jito client"
    );
    let submit_started_at = Instant::now();
    info!(
        seq = lease.seq,
        "stage3 invoking bundle_service.submit_bundle"
    );
    let result = client.submit(&tx).map_err(Stage3Error::Submit)?;
    let client_submit_latency = submit_started_at.elapsed();
    debug!(
        seq = lease.seq,
        endpoint = %result.endpoint.as_str(),
        bundle_id = %result.bundle_id.as_str(),
        client_submit_latency_ms = client_submit_latency.as_secs_f64() * 1_000.0,
        "stage3 dispatched bundle to client"
    );
    Ok((result, tip_lamports))
}

fn encode_bundle_base64(tx: &VersionedTransaction) -> Option<String> {
    let raw = serialize(tx).ok()?;
    Some(BASE64_STANDARD.encode(raw))
}

fn log_bundle_blueprint(lease: &Lease<BundleCandidate>, context: &BundleBuildContext) {
    if !tracing::enabled!(Level::INFO) {
        return;
    }
    let route = lease.route_plan.as_ref().map(describe_route_legs_verbose);
    let instructions = describe_instruction_layout(&context.instructions);
    let lookup_tables = context
        .lookup_tables
        .iter()
        .map(|table| table.key.to_string())
        .collect::<Vec<_>>();
    info!(
        seq = lease.seq,
        tip_lamports = lease.tip_lamports,
        route = ?route,
        instructions = ?instructions,
        lookup_tables = ?lookup_tables,
        flash_loan = ?lease.flash_loan,
        "stage3 bundle blueprint"
    );
}

fn describe_route_legs_verbose(plan: &RoutePlan) -> Vec<RouteLegLog> {
    plan.legs
        .iter()
        .enumerate()
        .map(|(index, leg)| RouteLegLog {
            index,
            protocol: classify_protocol(&leg.program_id),
            program_id: leg.program_id.clone(),
            pool_id: leg.pool_id.clone(),
            direction: format!("{} -> {}", leg.input_mint, leg.output_mint),
            input_amount: leg.input_amount,
            estimated_output_amount: leg.estimated_output_amount,
            fee_bps: leg.fee_bps,
        })
        .collect()
}

fn describe_instruction_layout(instructions: &[Instruction]) -> Vec<InstructionLog> {
    instructions
        .iter()
        .enumerate()
        .map(|(index, ix)| {
            let program_id = ix.program_id.to_string();
            let label = classify_program(&program_id);
            let accounts = ix
                .accounts
                .iter()
                .map(|meta| InstructionAccountLog {
                    pubkey: meta.pubkey.to_string(),
                    is_signer: meta.is_signer,
                    is_writable: meta.is_writable,
                })
                .collect();
            InstructionLog {
                index,
                program_id,
                label,
                data_len: ix.data.len(),
                accounts,
            }
        })
        .collect()
}

fn classify_program(program_id: &str) -> &'static str {
    match program_id {
        SYSTEM_PROGRAM_ID_STR => "system_program",
        COMPUTE_BUDGET_PROGRAM_ID_STR => "compute_budget",
        TOKEN_PROGRAM_ID_STR => "spl_token",
        ASSOCIATED_TOKEN_PROGRAM_ID_STR => "spl_associated_token",
        MEMO_PROGRAM_ID_STR => "memo",
        JITO_DONT_FRONT_PROGRAM_ID_STR => "jito_dont_front",
        _ => classify_protocol(program_id),
    }
}

fn classify_protocol(program_id: &str) -> &'static str {
    match program_id {
        RAYDIUM_V3_PROGRAM_ID_STR => "raydium_v3",
        CP_AMM_PROGRAM_ID_STR => "cp_amm",
        LB_CLMM_PROGRAM_ID_STR => "lb_clmm",
        LIFINITY_V2_PROGRAM_ID_STR => "lifinity_v2",
        WHIRLPOOL_PROGRAM_ID_STR => "whirlpool",
        _ => "unknown",
    }
}

fn handle_success<B, N, T>(
    builder: &B,
    mut lease: Lease<BundleCandidate>,
    submit_result: BundleSubmitResult,
    tip_lamports: Option<u64>,
    notifier: &N,
    tracker: &T,
    source: BundleSource,
) where
    B: Stage3Builder,
    N: Stage3Notifier,
    T: Stage3Tracker,
{
    release_flash_loan_reservation(builder, &mut lease);
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

fn handle_failure<B, N, T>(
    builder: &B,
    mut lease: Lease<BundleCandidate>,
    err: Stage3Error,
    notifier: &N,
    tracker: &T,
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
) where
    B: Stage3Builder,
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
        schedule_retry(lease, &err, retry_queue);
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
        .and_then(|ctx| extract_tip_lamports(ctx, builder.payer_pubkey()));
    release_flash_loan_reservation(builder, &mut lease);
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

fn release_flash_loan_reservation<B: Stage3Builder>(
    builder: &B,
    lease: &mut Lease<BundleCandidate>,
) {
    if let Some(plan) = lease.flash_loan_reservation.take() {
        builder.release_flash_loan_route(&plan);
    }
}

fn schedule_retry(
    mut lease: Lease<BundleCandidate>,
    err: &Stage3Error,
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
) {
    let ready_at = compute_retry_ready_at(&lease, err);
    let delay = ready_at.saturating_duration_since(Instant::now());
    lease.stage3_ready_at = Some(ready_at);
    debug!(
        seq = lease.seq,
        attempt = lease.stage3_attempts,
        delay_ms = delay.as_millis(),
        "stage3 retry scheduled"
    );
    insert_retry_bundle(retry_queue, lease);
}

fn insert_retry_bundle(
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
    lease: Lease<BundleCandidate>,
) {
    if retry_queue.is_empty() {
        retry_queue.push_back(lease);
        return;
    }
    let ready_at = bundle_ready_deadline(&lease);
    let position = retry_queue
        .iter()
        .position(|candidate| ready_at < bundle_ready_deadline(candidate));
    if let Some(idx) = position {
        retry_queue.insert(idx, lease);
    } else {
        retry_queue.push_back(lease);
    }
}

fn bundle_ready_deadline(lease: &Lease<BundleCandidate>) -> Instant {
    lease.stage3_ready_at.unwrap_or(lease.created_at)
}

fn compute_retry_ready_at(lease: &Lease<BundleCandidate>, err: &Stage3Error) -> Instant {
    if let Some(retry_after) = err.rate_limit_retry_after() {
        return Instant::now() + retry_after;
    }
    if err.is_rate_limited() {
        return Instant::now() + rate_limit_backoff(lease);
    }
    Instant::now()
}

fn rate_limit_backoff(lease: &Lease<BundleCandidate>) -> Duration {
    let attempt = lease.stage3_attempts.max(1) as u64;
    let capped = attempt.min(3);
    let base = Duration::from_millis(DEFAULT_RATE_LIMIT_BACKOFF_MS * capped);
    base + rate_limit_jitter(lease.seq, lease.stage3_attempts)
}

fn rate_limit_jitter(seq: u64, attempt: u8) -> Duration {
    if RATE_LIMIT_JITTER_MS == 0 {
        return Duration::from_millis(0);
    }
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(seq);
    hasher.write_u8(attempt);
    let jitter = hasher.finish() % RATE_LIMIT_JITTER_MS.max(1);
    Duration::from_millis(jitter)
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

    fn grpc_code(&self) -> Option<Code> {
        match self {
            Stage3Error::Submit(err) => err.downcast_ref::<Status>().map(|status| status.code()),
            _ => None,
        }
    }

    fn is_rate_limited(&self) -> bool {
        matches!(self.grpc_code(), Some(Code::ResourceExhausted))
    }

    fn rate_limit_retry_after(&self) -> Option<Duration> {
        let status = match self {
            Stage3Error::Submit(err) => err.downcast_ref::<Status>()?,
            _ => return None,
        };
        if status.code() != Code::ResourceExhausted {
            return None;
        }
        let header = status.metadata().get("x-wait-to-retry-ms")?;
        let millis = header.to_str().ok()?.parse::<u64>().ok()?;
        Some(Duration::from_millis(millis))
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

    fn release_flash_loan_route(&self, plan: &FlashLoanRoutePlan) {
        BuilderHandle::flash_loan_planner(self).release_route(plan);
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
