use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tracing::{debug, info, warn};

use crate::builder::{BuilderHandle, BuilderStage};
use crate::bundle::{BundleClient, BundleSubmitResult, BundleTracker, PendingSubmission};
use crate::core::{pin_current_thread, Lease, RingConsumer};
use crate::planner::types::BundleCandidate;
use crate::telemetry::{self, Stage3RetryEvent, Stage3RetryReason, Stage3SubmissionStatus};

#[derive(Debug, Clone)]
pub struct SubmissionConfig {
    pub submission_budget: Duration,
    pub max_retries: usize,
    pub retry_backoff: Duration,
    pub idle_backoff: Duration,
}

impl Default for SubmissionConfig {
    fn default() -> Self {
        Self {
            submission_budget: Duration::from_millis(20),
            max_retries: 2,
            retry_backoff: Duration::from_millis(25),
            idle_backoff: Duration::from_millis(5),
        }
    }
}

pub fn spawn_fast_lane_submitter(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
    client: Arc<BundleClient>,
    tracker: Arc<BundleTracker>,
    config: SubmissionConfig,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("fast_lane_submitter".into())
        .spawn(move || run_submitter(stop, core_id, consumer, builder, client, tracker, config))
        .map_err(Into::into)
}

fn run_submitter(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
    client: Arc<BundleClient>,
    tracker: Arc<BundleTracker>,
    config: SubmissionConfig,
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }
    info!(
        target = "submitter",
        budget_ms = config.submission_budget.as_millis(),
        retries = config.max_retries,
        retry_backoff_ms = config.retry_backoff.as_millis(),
        "fast-lane submitter online"
    );
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::SubmitHotPath);
        if let Some(lease) = consumer.pop() {
            if let Err(err) =
                process_bundle(lease, &builder, client.as_ref(), tracker.as_ref(), &config)
            {
                warn!(target = "submitter", error = %err.detail, seq = err.seq, "stage3 submission failed");
                telemetry::record_stage3_retry_event(Stage3RetryEvent::FailureRecorded(err.reason));
                telemetry::record_stage3_latency(err.latency);
                telemetry::record_stage3_outcome(
                    Stage3SubmissionStatus::Failure,
                    None,
                    err.tip_lamports,
                );
                tracker.record_stage3_failure(
                    err.seq,
                    err.tip_lamports,
                    format!("{:#}", err.detail),
                );
            }
        } else {
            thread::sleep(config.idle_backoff);
        }
    }
    info!(target = "submitter", "fast-lane submitter shutting down");
    Ok(())
}

fn process_bundle(
    mut lease: Lease<BundleCandidate>,
    builder: &BuilderHandle,
    client: &BundleClient,
    tracker: &BundleTracker,
    config: &SubmissionConfig,
) -> Result<(), Stage3Failure> {
    let seq = lease.seq;
    let tip = lease.tip_lamports;
    let started = Instant::now();
    match submit_with_retries(&mut lease, builder, client, config) {
        Ok(result) => {
            telemetry::record_stage3_latency(started.elapsed());
            telemetry::record_stage3_outcome(
                Stage3SubmissionStatus::Success,
                Some(&result.bundle_id),
                tip,
            );
            tracker.track_submission(PendingSubmission {
                bundle_id: result.bundle_id.clone(),
                seq,
                endpoint: result.endpoint.clone(),
                tip_lamports: tip,
            });
            info!(
                target = "submitter",
                seq,
                endpoint = %result.endpoint,
                bundle = %result.bundle_id,
                "bundle submitted"
            );
            Ok(())
        }
        Err(err) => Err(Stage3Failure {
            seq,
            tip_lamports: tip,
            reason: err.reason,
            detail: err.error,
            latency: started.elapsed(),
        }),
    }
}

fn submit_with_retries(
    bundle: &mut Lease<BundleCandidate>,
    builder: &BuilderHandle,
    client: &BundleClient,
    config: &SubmissionConfig,
) -> Result<BundleSubmitResult, Stage3Error> {
    let context = bundle.bundle_context.clone().ok_or_else(|| {
        Stage3Error::new(
            Stage3RetryReason::Context,
            anyhow!("bundle missing context"),
        )
    })?;
    for attempt in 0..=config.max_retries {
        bundle.stage3_attempts = bundle.stage3_attempts.saturating_add(1);
        let tx = builder
            .sign_bundle(&context)
            .map_err(|err| Stage3Error::new(Stage3RetryReason::Sign, err))?;
        match client.submit(&tx) {
            Ok(result) => {
                return Ok(result);
            }
            Err(err) => {
                debug!(target = "submitter", seq = bundle.seq, attempt, error = %err, "bundle submission attempt failed");
                if attempt == config.max_retries {
                    telemetry::record_stage3_retry_event(Stage3RetryEvent::RetriesExhausted(
                        Stage3RetryReason::Submit,
                    ));
                    return Err(Stage3Error::new(Stage3RetryReason::Submit, err));
                }
                telemetry::record_stage3_retry_event(Stage3RetryEvent::RetryQueued(
                    Stage3RetryReason::Submit,
                ));
                thread::sleep(config.retry_backoff);
            }
        }
    }
    unreachable!("retry loop must return")
}

struct Stage3Error {
    reason: Stage3RetryReason,
    error: anyhow::Error,
}

impl Stage3Error {
    fn new(reason: Stage3RetryReason, error: anyhow::Error) -> Self {
        Self { reason, error }
    }
}

struct Stage3Failure {
    seq: u64,
    tip_lamports: Option<u64>,
    reason: Stage3RetryReason,
    detail: anyhow::Error,
    latency: Duration,
}
