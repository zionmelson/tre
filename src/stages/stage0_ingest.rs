use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::runtime::Builder as TokioRuntime;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::core::{pin_current_thread, Lease, RingProducer, SlabPool};
use crate::corecast::CoreCastClient;
use crate::token_pair::TokenPair;

use super::types::{PoolUpdate, TradeUpdate};

pub fn spawn_ingest_stage(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    config: Arc<AppConfig>,
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
                config,
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
    config: Arc<AppConfig>,
    trade_pool: SlabPool<TradeUpdate>,
    trade_producer: RingProducer<Lease<TradeUpdate>>,
    pool_pool: SlabPool<PoolUpdate>,
    pool_producer: RingProducer<Lease<PoolUpdate>>,
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    let pair_allowlist = Arc::new(config.token_pair_allowlist());
    info!(
        target = "stage0",
        pairs = pair_allowlist.len(),
        "ingest online"
    );

    let client = CoreCastClient::new(config).context("failed to initialize CoreCast client")?;
    let runtime = TokioRuntime::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build stage0 runtime")?;

    runtime.block_on(async move {
        tokio::try_join!(
            pump_trade_stream(
                stop.clone(),
                client.clone(),
                trade_pool,
                trade_producer,
                pair_allowlist.clone(),
            ),
            pump_pool_stream(stop, client, pool_pool, pool_producer, pair_allowlist,)
        )
    })?;

    info!(target = "stage0", "ingest shutting down");
    Ok(())
}

async fn pump_trade_stream(
    stop: Arc<AtomicBool>,
    client: CoreCastClient,
    pool: SlabPool<TradeUpdate>,
    producer: RingProducer<Lease<TradeUpdate>>,
    allowlist: Arc<HashSet<TokenPair>>,
) -> Result<()> {
    let mut seq = 0u64;
    let mut retry = Duration::from_millis(250);
    let max_retry = Duration::from_secs(5);

    while !stop.load(Ordering::Relaxed) {
        match client.trade_stream().await {
            Ok(mut stream) => {
                info!(target = "stage0", "CoreCast trade stream subscribed");
                retry = Duration::from_millis(250);
                while !stop.load(Ordering::Relaxed) {
                    match stream.message().await {
                        Ok(Some(message)) => {
                            if let Some(update) = TradeUpdate::from_corecast(seq, &message) {
                                seq = seq.wrapping_add(1);
                                if allowlist
                                    .contains(&TokenPair::new(&update.sell_mint, &update.buy_mint))
                                {
                                    dispatch_trade_update(&pool, &producer, update);
                                }
                            } else {
                                debug!(target = "stage0", "dropped malformed trade update");
                            }
                        }
                        Ok(None) => {
                            warn!(target = "stage0", "trade stream closed by server");
                            break;
                        }
                        Err(status) => {
                            warn!(target = "stage0", %status, "trade stream error");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                warn!(target = "stage0", %err, "failed to subscribe to trade stream");
                info!(target = "stage0", error = ?err, "trade stream subscription error detail");
            }
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }

        sleep(retry).await;
        retry = (retry * 2).min(max_retry);
    }

    Ok(())
}

async fn pump_pool_stream(
    stop: Arc<AtomicBool>,
    client: CoreCastClient,
    pool: SlabPool<PoolUpdate>,
    producer: RingProducer<Lease<PoolUpdate>>,
    allowlist: Arc<HashSet<TokenPair>>,
) -> Result<()> {
    let mut seq = 0u64;
    let mut retry = Duration::from_millis(250);
    let max_retry = Duration::from_secs(5);

    while !stop.load(Ordering::Relaxed) {
        match client.pool_stream().await {
            Ok(mut stream) => {
                info!(target = "stage0", "CoreCast pool stream subscribed");
                retry = Duration::from_millis(250);
                while !stop.load(Ordering::Relaxed) {
                    match stream.message().await {
                        Ok(Some(message)) => {
                            if let Some(update) = PoolUpdate::from_corecast(seq, &message) {
                                seq = seq.wrapping_add(1);
                                if allowlist.contains(&TokenPair::new(
                                    &update.base_mint,
                                    &update.quote_mint,
                                )) {
                                    dispatch_pool_update(&pool, &producer, update);
                                }
                            } else {
                                debug!(target = "stage0", "dropped malformed pool update");
                            }
                        }
                        Ok(None) => {
                            warn!(target = "stage0", "pool stream closed by server");
                            break;
                        }
                        Err(status) => {
                            warn!(target = "stage0", %status, "pool stream error");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                warn!(target = "stage0", %err, "failed to subscribe to pool stream");
                info!(target = "stage0", error = ?err, "pool stream subscription error detail");
            }
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }

        sleep(retry).await;
        retry = (retry * 2).min(max_retry);
    }

    Ok(())
}

fn dispatch_trade_update(
    pool: &SlabPool<TradeUpdate>,
    producer: &RingProducer<Lease<TradeUpdate>>,
    update: TradeUpdate,
) {
    if let Some(lease) = pool.try_acquire(update) {
        if let Err(lease) = producer.push(lease) {
            warn!(target = "stage0", "trade ring saturated; dropping update");
            drop(lease);
        }
    } else {
        warn!(target = "stage0", "trade slab saturated; dropping update");
    }
}

fn dispatch_pool_update(
    pool: &SlabPool<PoolUpdate>,
    producer: &RingProducer<Lease<PoolUpdate>>,
    update: PoolUpdate,
) {
    if let Some(lease) = pool.try_acquire(update) {
        if let Err(lease) = producer.push(lease) {
            warn!(target = "stage0", "pool ring saturated; dropping update");
            drop(lease);
        }
    } else {
        warn!(target = "stage0", "pool slab saturated; dropping update");
    }
}
