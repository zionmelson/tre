use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tracing::{info, warn};

use crate::core::{pin_current_thread, Lease, RingProducer, SlabPool};

use super::types::{PoolUpdate, TradeUpdate};

pub fn spawn_ingest_stage(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    trade_pool: SlabPool<TradeUpdate>,
    trade_producer: RingProducer<Lease<TradeUpdate>>,
    pool_pool: SlabPool<PoolUpdate>,
    pool_producer: RingProducer<Lease<PoolUpdate>>,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("stage0_ingest".into())
        .spawn(move || {
            run(
                stop,
                core_id,
                trade_pool,
                trade_producer,
                pool_pool,
                pool_producer,
            )
        })
        .map_err(Into::into)
}

fn run(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    trade_pool: SlabPool<TradeUpdate>,
    trade_producer: RingProducer<Lease<TradeUpdate>>,
    pool_pool: SlabPool<PoolUpdate>,
    pool_producer: RingProducer<Lease<PoolUpdate>>,
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    let mut seq = 0u64;
    info!("stage0 ingest online");
    while !stop.load(Ordering::Relaxed) {
        let pool_id = format!("pool-{}", seq % 5);

        if let Some(lease) =
            pool_pool.try_acquire_with(|| PoolUpdate::synthetic(seq, pool_id.clone()))
        {
            if let Err(err) = pool_producer.push(lease) {
                warn!(%err, "failed to enqueue pool update");
            }
        }

        if let Some(lease) =
            trade_pool.try_acquire_with(|| TradeUpdate::synthetic(seq, pool_id.clone()))
        {
            if let Err(err) = trade_producer.push(lease) {
                warn!(%err, "failed to enqueue trade update");
            }
        }

        seq = seq.wrapping_add(1);
        thread::sleep(Duration::from_millis(10));
    }

    info!("stage0 ingest shutting down");
    Ok(())
}
