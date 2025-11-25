use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tracing::{debug, info};

use crate::core::{pin_current_thread, Lease, RingConsumer, RingProducer, SlabPool};

use super::types::{BundleCandidate, Opportunity, PoolSnapshot, PoolUpdate, TradeUpdate};

pub fn spawn_eval_stage(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    trade_consumer: RingConsumer<Lease<TradeUpdate>>,
    pool_consumer: RingConsumer<Lease<PoolUpdate>>,
    producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("stage1_eval".into())
        .spawn(move || {
            run(
                stop,
                core_id,
                trade_consumer,
                pool_consumer,
                producer,
                bundle_pool,
            )
        })
        .map_err(Into::into)
}

fn run(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    trade_consumer: RingConsumer<Lease<TradeUpdate>>,
    pool_consumer: RingConsumer<Lease<PoolUpdate>>,
    producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    let mut pool_snapshots: HashMap<String, PoolSnapshot> = HashMap::new();
    let mut opportunity_seq = 0u64;

    info!("stage1 eval online");
    while !stop.load(Ordering::Relaxed) {
        let mut made_progress = false;

        if let Some(pool_lease) = pool_consumer.pop() {
            let update = pool_lease.into_inner();
            let snapshot = update.into_snapshot();
            pool_snapshots.insert(snapshot.pool_id.clone(), snapshot);
            made_progress = true;
        }

        if let Some(trade_lease) = trade_consumer.pop() {
            let trade = trade_lease.into_inner();
            let snapshot = pool_snapshots.get(&trade.pool_id).cloned();
            let opportunity = Opportunity::from_trade(opportunity_seq, trade, snapshot);
            opportunity_seq = opportunity_seq.wrapping_add(1);

            if let Some(lease) =
                bundle_pool.try_acquire_with(|| BundleCandidate::from_opportunity(&opportunity))
            {
                if producer.push(lease).is_ok() {
                    debug!(
                        opp_seq = opportunity.seq,
                        edge_bps = opportunity.estimated_edge_bps,
                        signature = %opportunity.trade.signature,
                        "emitted bundle candidate"
                    );
                }
            }

            made_progress = true;
        }

        if !made_progress {
            thread::yield_now();
            thread::sleep(Duration::from_millis(5));
        }
    }

    info!("stage1 eval shutting down");
    Ok(())
}
