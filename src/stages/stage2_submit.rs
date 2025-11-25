use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tracing::info;

use crate::core::{pin_current_thread, Lease, RingConsumer};

use super::types::BundleCandidate;

pub fn spawn_submit_stage(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("stage2_submit".into())
        .spawn(move || run(stop, core_id, consumer))
        .map_err(Into::into)
}

fn run(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    info!("stage2 submit online");
    while !stop.load(Ordering::Relaxed) {
        if let Some(bundle) = consumer.pop() {
            info!(
                seq = bundle.seq,
                pool = %bundle.pool_id,
                edge_bps = bundle.estimated_edge_bps,
                "pretending to submit bundle"
            );
            drop(bundle);
        } else {
            thread::yield_now();
            thread::sleep(Duration::from_millis(10));
        }
    }
    info!("stage2 submit shutting down");
    Ok(())
}
