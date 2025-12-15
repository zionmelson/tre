use crate::config::Config;
use crate::jito::JitoClient;
use crate::metrics::{self, Metrics};
use crate::pipeline;
use crate::pool_seeds::{PoolSeedOverlay, PoolSeedRegistry};
use crate::shutdown::{Shutdown, ShutdownEvent};
use crate::wallet::LoadedWallets;
use crate::workers::{self, StopReason};
use anyhow::{anyhow, Context, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, watch};
use tokio::task;

/// Central application supervisor coordinating ingestion, search, and execution.
pub struct App {
    config_path: Arc<std::path::PathBuf>,
    config_rx: watch::Receiver<Arc<Config>>,
    config_tx: watch::Sender<Arc<Config>>,
    wallet_rx: watch::Receiver<Arc<LoadedWallets>>,
    wallet_tx: watch::Sender<Arc<LoadedWallets>>,
    pipeline: pipeline::Pipeline,
    metrics: Metrics,
    metrics_addr: SocketAddr,
    replay_pools: Option<PathBuf>,
    replay_trades: Option<PathBuf>,
    pool_seeds: Arc<PoolSeedRegistry>,
    inferred_seeds: Arc<Mutex<PoolSeedOverlay>>,
}

impl App {
    const POOL_SEED_STALE_WARN_SECS: u64 = 6 * 60 * 60;
    pub fn new(
        config: Config,
        config_path: std::path::PathBuf,
        metrics: Metrics,
        metrics_addr: SocketAddr,
        replay_pools: Option<PathBuf>,
        replay_trades: Option<PathBuf>,
    ) -> Result<Self> {
        let pipeline = pipeline::Pipeline::new(&config)?;
        let arc_cfg = Arc::new(config);
        let wallets = Arc::new(LoadedWallets::load(&arc_cfg.wallets).context("load wallets")?);
        let (tx, rx) = watch::channel(arc_cfg.clone());
        let (wallet_tx, wallet_rx) = watch::channel(wallets);
        let seed_path = PoolSeedRegistry::resolve_default_path();
        let pool_seeds = match PoolSeedRegistry::load_optional(&seed_path)? {
            Some(registry) => {
                let count = registry.len();
                let age = registry.snapshot_age();
                if let Some(path) = registry.path() {
                    tracing::info!(
                        target: "app",
                        path = %path.display(),
                        seeds = count,
                        age_secs = age.map(|dur| dur.as_secs()).unwrap_or(0),
                        "loaded pool seed snapshot"
                    );
                } else {
                    tracing::info!(
                        target: "app",
                        seeds = count,
                        age_secs = age.map(|dur| dur.as_secs()).unwrap_or(0),
                        "loaded pool seed snapshot"
                    );
                }
                Arc::new(registry)
            }
            None => {
                tracing::warn!(
                    target: "app",
                    path = %seed_path.display(),
                    "pool seed snapshot missing; planner will rely on live updates until populated"
                );
                Arc::new(PoolSeedRegistry::with_path(Some(seed_path), Vec::new()))
            }
        };

        let inferred_seed_path = PoolSeedOverlay::resolve_default_path();
        let inferred_overlay = PoolSeedOverlay::load_or_default(&inferred_seed_path)?;
        tracing::info!(
            target: "app",
            path = %inferred_seed_path.display(),
            seeds = inferred_overlay.len(),
            "loaded inferred pool seed overlay"
        );
        let inferred_seeds = Arc::new(Mutex::new(inferred_overlay));

        if let Some(age) = pool_seeds.snapshot_age() {
            metrics.observe_pool_seed_snapshot_age(Some(age));
            if age.as_secs() >= Self::POOL_SEED_STALE_WARN_SECS {
                tracing::warn!(
                    target: "app",
                    age_secs = age.as_secs(),
                    threshold_secs = Self::POOL_SEED_STALE_WARN_SECS,
                    "pool seed snapshot appears stale"
                );
            }
        } else {
            metrics.observe_pool_seed_snapshot_age(None);
        }
        Ok(Self {
            config_path: Arc::new(config_path),
            config_rx: rx,
            config_tx: tx,
            wallet_rx,
            wallet_tx,
            pipeline,
            metrics,
            metrics_addr,
            replay_pools,
            replay_trades,
            pool_seeds,
            inferred_seeds,
        })
    }

    /// Main async entrypoint. Spawns worker tasks (future work) and waits for shutdown/reload signals.
    pub async fn run(self) -> Result<()> {
        let metrics_task = {
            let metrics = self.metrics.clone();
            let addr = self.metrics_addr;
            tokio::spawn(async move {
                if let Err(err) = metrics::run_exporter(metrics, addr).await {
                    tracing::error!(
                        target: "telemetry::metrics",
                        %addr,
                        ?err,
                        "metrics exporter exited"
                    );
                }
            })
        };

        let initial_cfg = self.config_rx.borrow().clone();
        let initial_wallets = self.wallet_rx.borrow().clone();
        let jito = Arc::new(
            JitoClient::connect(&initial_cfg.jito, Some(initial_wallets.auth()))
                .await
                .context("connect jito block engine")?,
        );

        let replay_pools = self.replay_pools.clone();
        let replay_trades = self.replay_trades.clone();
        let replay_mode = replay_pools.is_some() || replay_trades.is_some();
        if replay_mode {
            tracing::info!(
                target: "app",
                ?replay_pools,
                ?replay_trades,
                "replay mode enabled; live ingest disabled"
            );
        }

        let pipeline_handles = self.pipeline.handles();

        let mut shutdown = Shutdown::new()?;
        let workers = workers::WorkerSet::spawn(
            self.config_rx.clone(),
            pipeline_handles.clone(),
            self.metrics.clone(),
            jito,
            self.wallet_rx.clone(),
            !replay_mode,
            self.pool_seeds.clone(),
            self.inferred_seeds.clone(),
        );

        let mut replay_done_rx: Option<oneshot::Receiver<Result<()>>> = None;
        let mut replay_task: Option<tokio::task::JoinHandle<()>> = None;
        if replay_mode {
            let (tx, rx) = oneshot::channel();
            replay_done_rx = Some(rx);
            let cfg = self.config_rx.borrow().clone();
            let metrics = self.metrics.clone();
            let pools_path = replay_pools;
            let trades_path = replay_trades;
            let handles = pipeline_handles.clone();
            replay_task = Some(tokio::spawn(async move {
                let result =
                    workers::replay_from_files(cfg, pools_path, trades_path, handles, metrics)
                        .await;
                let _ = tx.send(result);
            }));
        }
        tracing::info!(target: "app", "bootstrap complete; waiting for signals");

        loop {
            if let Some(rx) = replay_done_rx.as_mut() {
                tokio::select! {
                    event_res = shutdown.wait() => {
                        let event = match event_res {
                            Ok(evt) => evt,
                            Err(err) => return Err(err),
                        };
                        match event {
                            ShutdownEvent::Terminate => {
                                tracing::info!(target: "app", "shutdown signal received");
                                workers.signal(StopReason::Terminate);
                                workers.shutdown().await?;
                                if let Some(handle) = replay_task.take() {
                                    handle.abort();
                                    let _ = handle.await;
                                }
                                replay_done_rx.take();
                                break;
                            }
                            ShutdownEvent::Reload => {
                                tracing::info!(target: "app", "reload signal received; reloading config");
                                if let Err(err) = self.reload_config().await {
                                    tracing::error!(target: "app", ?err, "config reload failed");
                                }
                            }
                        }
                    }
                    replay_res = rx => {
                        replay_done_rx.take();
                        let join_res = if let Some(handle) = replay_task.take() {
                            handle.await
                        } else {
                            Ok(())
                        };
                        match replay_res {
                            Ok(Ok(())) => {
                                tracing::info!(target: "app", "replay harness completed");
                            }
                            Ok(Err(err)) => {
                                tracing::error!(target: "app", ?err, "replay harness exited with error");
                            }
                            Err(_) => {
                                tracing::warn!(target: "app", "replay harness channel closed before completion");
                            }
                        }
                        if let Err(err) = join_res {
                            tracing::warn!(target: "app", ?err, "failed to join replay task");
                        }
                        workers.signal(StopReason::Terminate);
                        workers.shutdown().await?;
                        break;
                    }
                }
            } else {
                match shutdown.wait().await? {
                    ShutdownEvent::Terminate => {
                        tracing::info!(target: "app", "shutdown signal received");
                        workers.signal(StopReason::Terminate);
                        workers.shutdown().await?;
                        if let Some(handle) = replay_task.take() {
                            handle.abort();
                            let _ = handle.await;
                        }
                        break;
                    }
                    ShutdownEvent::Reload => {
                        tracing::info!(target: "app", "reload signal received; reloading config");
                        if let Err(err) = self.reload_config().await {
                            tracing::error!(target: "app", ?err, "config reload failed");
                        }
                    }
                }
            }
        }

        tracing::info!(target: "app", "shutdown complete");
        metrics_task.abort();
        let _ = metrics_task.await;
        Ok(())
    }

    async fn reload_config(&self) -> Result<()> {
        let path = self.config_path.clone();
        let loaded = task::spawn_blocking(move || Config::load(&*path))
            .await
            .context("config reload task panicked")??;

        let arc_cfg = Arc::new(loaded);
        let wallets = Arc::new(LoadedWallets::load(&arc_cfg.wallets).context("load wallets")?);
        self.config_tx
            .send(arc_cfg)
            .map_err(|_| anyhow!("no config subscribers available"))?;
        self.wallet_tx
            .send(wallets)
            .map_err(|_| anyhow!("no wallet subscribers available"))?;

        tracing::info!(target: "app", "config reload complete");
        Ok(())
    }
}
