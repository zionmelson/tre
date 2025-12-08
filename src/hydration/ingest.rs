use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use tokio::runtime::Builder as TokioRuntime;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::corecast::CoreCastClient;
use crate::hydration::readiness::ReadinessRegistry;
use crate::hydration::types::{PoolUpdate, TradeUpdate};
use crate::token_pair::TokenPair;

pub struct IngestHandles {
    pub trade_rx: Receiver<TradeUpdate>,
    pub pool_rx: Receiver<PoolUpdate>,
    pub handle: thread::JoinHandle<Result<()>>,
}

pub fn spawn_ingest_stage(
    stop: Arc<AtomicBool>,
    config: Arc<AppConfig>,
    readiness: ReadinessRegistry,
    channel_capacity: usize,
) -> Result<IngestHandles> {
    let capacity = channel_capacity.max(1);
    let (trade_tx, trade_rx) = bounded(capacity);
    let (pool_tx, pool_rx) = bounded(capacity);
    let allowlist = Arc::new(config.token_pair_allowlist());
    let client =
        CoreCastClient::new(config.clone()).context("failed to initialize CoreCast client")?;
    let handle = thread::Builder::new()
        .name("corecast_ingest".into())
        .spawn(move || {
            let runtime = TokioRuntime::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build ingest runtime")?;
            runtime.block_on(async move {
                IngestWorker::new(stop, client, readiness, allowlist, trade_tx, pool_tx)
                    .run()
                    .await
            })
        })?;

    Ok(IngestHandles {
        trade_rx,
        pool_rx,
        handle,
    })
}

struct IngestWorker {
    stop: Arc<AtomicBool>,
    client: CoreCastClient,
    readiness: ReadinessRegistry,
    allowlist: Arc<HashSet<TokenPair>>,
    trade_tx: Sender<TradeUpdate>,
    pool_tx: Sender<PoolUpdate>,
}

impl IngestWorker {
    fn new(
        stop: Arc<AtomicBool>,
        client: CoreCastClient,
        readiness: ReadinessRegistry,
        allowlist: Arc<HashSet<TokenPair>>,
        trade_tx: Sender<TradeUpdate>,
        pool_tx: Sender<PoolUpdate>,
    ) -> Self {
        Self {
            stop,
            client,
            readiness,
            allowlist,
            trade_tx,
            pool_tx,
        }
    }

    async fn run(self) -> Result<()> {
        let worker = Arc::new(self);
        let trade_task = worker.clone().trade_loop();
        let pool_task = worker.pool_loop();
        tokio::try_join!(trade_task, pool_task)?;
        Ok(())
    }

    async fn trade_loop(self: Arc<Self>) -> Result<()> {
        let mut seq = 0u64;
        let mut retry = Duration::from_millis(250);
        let max_retry = Duration::from_secs(5);

        while !self.stop.load(Ordering::Relaxed) {
            match self.client.trade_stream().await {
                Ok(mut stream) => {
                    info!(target = "ingest", "CoreCast trade stream subscribed");
                    retry = Duration::from_millis(250);
                    while !self.stop.load(Ordering::Relaxed) {
                        match stream.next_message().await {
                            Ok(Some(message)) => {
                                if let Some(update) = TradeUpdate::from_corecast(seq, &message) {
                                    seq = seq.wrapping_add(1);
                                    if self.allowlist.contains(&TokenPair::new(
                                        &update.sell_mint,
                                        &update.buy_mint,
                                    )) {
                                        self.enqueue_trade(update);
                                    }
                                } else {
                                    debug!(target = "ingest", "dropped malformed trade update");
                                }
                            }
                            Ok(None) => {
                                warn!(target = "ingest", "trade stream closed by server");
                                break;
                            }
                            Err(status) => {
                                warn!(target = "ingest", %status, "trade stream error");
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(target = "ingest", %err, "failed to subscribe to trade stream");
                }
            }

            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            sleep(retry).await;
            retry = (retry * 2).min(max_retry);
        }

        Ok(())
    }

    async fn pool_loop(self: Arc<Self>) -> Result<()> {
        let mut seq = 0u64;
        let mut retry = Duration::from_millis(250);
        let max_retry = Duration::from_secs(5);

        while !self.stop.load(Ordering::Relaxed) {
            match self.client.pool_stream().await {
                Ok(mut stream) => {
                    info!(target = "ingest", "CoreCast pool stream subscribed");
                    retry = Duration::from_millis(250);
                    while !self.stop.load(Ordering::Relaxed) {
                        match stream.next_message().await {
                            Ok(Some(message)) => {
                                if let Some(update) = PoolUpdate::from_corecast(seq, &message) {
                                    seq = seq.wrapping_add(1);
                                    if self.allowlist.contains(&TokenPair::new(
                                        &update.base_mint,
                                        &update.quote_mint,
                                    )) {
                                        self.readiness.record_pool_update(&update);
                                        self.enqueue_pool(update);
                                    }
                                } else {
                                    debug!(target = "ingest", "dropped malformed pool update");
                                }
                            }
                            Ok(None) => {
                                warn!(target = "ingest", "pool stream closed by server");
                                break;
                            }
                            Err(status) => {
                                warn!(target = "ingest", %status, "pool stream error");
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(target = "ingest", %err, "failed to subscribe to pool stream");
                }
            }

            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            sleep(retry).await;
            retry = (retry * 2).min(max_retry);
        }

        Ok(())
    }

    fn enqueue_trade(&self, update: TradeUpdate) {
        match self.trade_tx.try_send(update) {
            Ok(_) => {}
            Err(TrySendError::Full(_)) => {
                warn!(
                    target = "ingest",
                    "trade channel saturated; dropping update"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                warn!(
                    target = "ingest",
                    "trade channel disconnected; stopping stream"
                );
                self.stop.store(true, Ordering::SeqCst);
            }
        }
    }

    fn enqueue_pool(&self, update: PoolUpdate) {
        match self.pool_tx.try_send(update) {
            Ok(_) => {}
            Err(TrySendError::Full(_)) => {
                warn!(target = "ingest", "pool channel saturated; dropping update");
            }
            Err(TrySendError::Disconnected(_)) => {
                warn!(
                    target = "ingest",
                    "pool channel disconnected; stopping stream"
                );
                self.stop.store(true, Ordering::SeqCst);
            }
        }
    }
}
