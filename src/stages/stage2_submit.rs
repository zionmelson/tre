use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tracing::{error, info, trace, warn};

use crate::bundle::BundleClient;
use crate::core::RingReceiver;
use crate::notifier::Notifier;

use super::spawn_stage_thread;
use super::stage1_eval::BundleLease;

pub struct Stage2Submit;

impl Stage2Submit {
    pub fn spawn(
        rx: RingReceiver<BundleLease>,
        client: BundleClient,
        notifier: Notifier,
        running: Arc<AtomicBool>,
        core_index: Option<usize>,
    ) -> std::thread::JoinHandle<()> {
        spawn_stage_thread("stage2-submit", core_index, move || {
            info!(target = "stage2", "submitter online");
            Self::run(rx, client, notifier, running);
        })
    }

    fn run(
        rx: RingReceiver<BundleLease>,
        client: BundleClient,
        notifier: Notifier,
        running: Arc<AtomicBool>,
    ) {
        while running.load(Ordering::Acquire) {
            match rx.spin_pop(&running) {
                Some(bundle) => {
                    trace!(target = "stage2", uuid = %bundle.uuid, "submitting bundle");
                    if let Err(err) = client.submit_bundle(&bundle) {
                        error!(target = "stage2", error = %err, "bundle submission failed");
                    } else if let Err(err) = notifier.bundle_success(&bundle.uuid) {
                        warn!(target = "stage2", error = %err, "notifier failed");
                    }
                }
                None => break,
            }
        }
        info!(target = "stage2", "submitter shutdown");
    }
}
