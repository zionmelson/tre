use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_utils::Backoff;
use tracing::{info, trace, warn};

use crate::bundle::BundleRequest;
use crate::core::{RingReceiver, RingSender, SlabPool, SlabRef};
use crate::strategy::StrategyEngine;

use super::spawn_stage_thread;
use super::stage0_ingest::OpportunityLease;

pub type BundleLease = SlabRef<BundleRequest>;

pub struct Stage1Eval;

impl Stage1Eval {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        rx: RingReceiver<OpportunityLease>,
        tx: RingSender<BundleLease>,
        bundle_pool: SlabPool<BundleRequest>,
        strategy: StrategyEngine,
        running: Arc<AtomicBool>,
        core_index: Option<usize>,
    ) -> std::thread::JoinHandle<()> {
        spawn_stage_thread("stage1-eval", core_index, move || {
            info!(
                target = "stage1",
                cap = bundle_pool.capacity(),
                "eval online"
            );
            Self::run(rx, tx, bundle_pool, strategy, running);
        })
    }

    fn run(
        rx: RingReceiver<OpportunityLease>,
        tx: RingSender<BundleLease>,
        bundle_pool: SlabPool<BundleRequest>,
        strategy: StrategyEngine,
        running: Arc<AtomicBool>,
    ) {
        while running.load(Ordering::Acquire) {
            match rx.spin_pop(&running) {
                Some(opportunity) => {
                    trace!(target = "stage1", sig = %opportunity.signature, "evaluating opportunity");
                    let maybe_bundle = strategy.evaluate(&opportunity);
                    drop(opportunity);

                    if let Some(bundle) = maybe_bundle {
                        if let Some(slot) = bundle_pool.try_insert(bundle) {
                            if !tx.spin_push(slot, &running) {
                                break;
                            }
                        } else {
                            warn!(target = "stage1", "bundle slab saturated; dropping bundle");
                            Backoff::new().snooze();
                        }
                    }
                }
                None => break,
            }
        }
        info!(target = "stage1", "eval shutdown");
    }
}
