use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tracing::{debug, info, warn};

use crate::builder::{BuilderHandle, BuilderStage};
use crate::bundle::{BundleClient, BundleTracker, PendingSubmission};
use crate::core::{pin_current_thread, Lease, RingConsumer};
use crate::notifier::{BundleSubmissionEvent, BundleSubmissionStatus, Notifier};
use crate::stages::types::BundleCandidate;
use crate::telemetry::{self, Stage3SubmissionStatus};
use crate::tip_pressure;

/// Configuration for the fast-lane submission workers.
#[derive(Debug, Clone)]
pub struct SubmissionConfig {
    /// Maximum tolerated latency from planner to outbound send.
    pub submission_budget: Duration,
}

impl Default for SubmissionConfig {
    fn default() -> Self {
        Self {
            submission_budget: Duration::from_millis(20),
        }
    }
}

/// Spawns the simplified fast-lane submitter which replaces Stage3 send logic when the
/// feature flag is enabled.
pub fn spawn_fast_lane_submitter(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
    client: Arc<BundleClient>,
    notifier: Arc<Notifier>,
    tracker: Arc<BundleTracker>,
    config: SubmissionConfig,
) -> Result<thread::JoinHandle<Result<()>>> {
    let worker = FastLaneSubmitter::new(builder, client, notifier, tracker, config);
    thread::Builder::new()
        .name("fast_lane_submitter".into())
        .spawn(move || worker.run(stop, core_id, consumer))
        .map_err(Into::into)
}

struct FastLaneSubmitter {
    builder: BuilderHandle,
    client: Arc<BundleClient>,
    notifier: Arc<Notifier>,
    tracker: Arc<BundleTracker>,
    config: SubmissionConfig,
}

impl FastLaneSubmitter {
    fn new(
        builder: BuilderHandle,
        client: Arc<BundleClient>,
        notifier: Arc<Notifier>,
        tracker: Arc<BundleTracker>,
        config: SubmissionConfig,
    ) -> Self {
        Self {
            builder,
            client,
            notifier,
            tracker,
            config,
        }
    }

    fn run(
        &self,
        stop: Arc<AtomicBool>,
        core_id: Option<usize>,
        consumer: RingConsumer<Lease<BundleCandidate>>,
    ) -> Result<()> {
        if let Some(core) = core_id {
            let _ = pin_current_thread(core);
        }
        info!(
            submission_budget_ms = self.config.submission_budget.as_millis(),
            "fast-lane submitter online"
        );
        while !stop.load(Ordering::Relaxed) {
            self.builder.heartbeat(BuilderStage::SubmitHotPath);
            if let Some(lease) = consumer.pop() {
                if let Err(err) = self.process_bundle(lease) {
                    warn!(error = %err, "fast-lane bundle submission failed");
                }
            } else {
                thread::yield_now();
                thread::sleep(Duration::from_millis(5));
            }
        }
        info!("fast-lane submitter shutting down");
        Ok(())
    }

    fn process_bundle(&self, mut lease: Lease<BundleCandidate>) -> Result<()> {
        let seq = lease.seq;
        let detection_latency = lease.created_at.elapsed();
        let tip_lamports = lease.tip_lamports;
        let ctx = match lease.bundle_context.as_ref() {
            Some(ctx) => ctx,
            None => {
                release_flash_loan_reservation(&self.builder, &mut lease);
                self.record_failure(
                    &lease,
                    detection_latency,
                    tip_lamports,
                    "missing bundle context".to_string(),
                );
                return Err(anyhow!("bundle {} missing build context", seq));
            }
        };
        let signed = match self.builder.sign_bundle(ctx) {
            Ok(tx) => tx,
            Err(err) => {
                release_flash_loan_reservation(&self.builder, &mut lease);
                self.record_failure(
                    &lease,
                    detection_latency,
                    tip_lamports,
                    format!("signing failed for bundle {}: {err}", seq),
                );
                return Err(anyhow!("signing failed for bundle {}: {err}", seq));
            }
        };
        debug!(seq, "fast-lane submitting bundle");
        match self.client.submit(&signed) {
            Ok(result) => {
                release_flash_loan_reservation(&self.builder, &mut lease);
                record_submission_metrics(
                    Stage3SubmissionStatus::Success,
                    detection_latency,
                    Some(result.bundle_id.as_str()),
                    tip_lamports,
                );
                self.tracker.track_submission(PendingSubmission {
                    bundle_id: result.bundle_id.clone(),
                    seq,
                    endpoint: result.endpoint.clone(),
                    detected_at: lease.created_at,
                    tip_lamports,
                });
                let event = BundleSubmissionEvent {
                    seq,
                    status: BundleSubmissionStatus::Submitted,
                    bundle_id: Some(result.bundle_id),
                    endpoint: Some(result.endpoint),
                    tip_lamports,
                    detection_latency,
                    slot: None,
                    detail: None,
                    error: None,
                };
                if let Err(err) = self.notifier.notify_bundle_submission(event) {
                    warn!(seq, error = %err, "failed to emit submission event");
                }
                Ok(())
            }
            Err(err) => {
                release_flash_loan_reservation(&self.builder, &mut lease);
                self.record_failure(
                    &lease,
                    detection_latency,
                    tip_lamports,
                    format!("fast-lane submission error: {err}"),
                );
                Err(err)
            }
        }
    }

    fn record_failure(
        &self,
        lease: &Lease<BundleCandidate>,
        detection_latency: Duration,
        tip_lamports: Option<u64>,
        detail: String,
    ) {
        record_submission_metrics(
            Stage3SubmissionStatus::Failure,
            detection_latency,
            None,
            tip_lamports,
        );
        self.tracker
            .record_stage3_failure(lease.seq, lease.created_at, tip_lamports, detail);
    }
}

fn release_flash_loan_reservation(builder: &BuilderHandle, lease: &mut Lease<BundleCandidate>) {
    if let Some(plan) = lease.flash_loan_reservation.take() {
        builder.flash_loan_planner().release_route(&plan);
    }
}

fn record_submission_metrics(
    status: Stage3SubmissionStatus,
    latency: Duration,
    bundle_id: Option<&str>,
    tip_lamports: Option<u64>,
) {
    telemetry::record_stage3_latency(latency);
    tip_pressure::record_stage3_latency(latency);
    telemetry::record_stage3_outcome(status, bundle_id, tip_lamports);
}
