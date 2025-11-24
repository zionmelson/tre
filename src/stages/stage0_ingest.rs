use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_utils::Backoff;
use tracing::{info, trace, warn};

use crate::core::{RingSender, SlabPool, SlabRef};
use crate::corecast::{CorecastSubscriber, Opportunity};

use super::spawn_stage_thread;

pub type OpportunityLease = SlabRef<Opportunity>;

pub struct Stage0Ingest;

impl Stage0Ingest {
    pub fn spawn(
        corecast: CorecastSubscriber,
        pool: SlabPool<Opportunity>,
        tx: RingSender<OpportunityLease>,
        running: Arc<AtomicBool>,
        core_index: Option<usize>,
    ) -> std::thread::JoinHandle<()> {
        spawn_stage_thread("stage0-ingest", core_index, move || {
            info!(target = "stage0", cap = pool.capacity(), "ingest online");
            Self::run(corecast, pool, tx, running);
        })
    }

    fn run(
        corecast: CorecastSubscriber,
        pool: SlabPool<Opportunity>,
        tx: RingSender<OpportunityLease>,
        running: Arc<AtomicBool>,
    ) {
        let mut backoff = Backoff::new();
        while running.load(Ordering::Acquire) {
            match corecast.next_opportunity() {
                Some(opportunity) => {
                    backoff = Backoff::new();
                    if let Some(slot) = pool.try_insert(opportunity) {
                        if !tx.spin_push(slot, &running) {
                            break;
                        }
                    } else {
                        warn!(
                            target = "stage0",
                            "opportunity slab saturated; dropping event"
                        );
                    }
                }
                None => {
                    backoff.snooze();
                    trace!(target = "stage0", "corecast idle");
                }
            }
        }
        info!(target = "stage0", "ingest shutdown");
    }
}
