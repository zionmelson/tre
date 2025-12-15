use crate::arbgraph::{ArbGraph, PlannerTuning};
use crate::config::Config;
use crate::jito::JitoClient;
use crate::metrics::Metrics;
use crate::pipeline::PipelineHandles;
use crate::pool_seeds::{PoolSeedOverlay, PoolSeedRegistry};
use crate::wallet::LoadedWallets;
use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};
use std::time::{Duration as StdDuration, Instant as StdInstant};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinSet;

/// Reasons a worker should shut down.
#[derive(Debug, Clone, Copy)]
pub enum StopReason {
    Terminate,
}

pub struct WorkerSet {
    stop_tx: broadcast::Sender<StopReason>,
    join_set: JoinSet<Result<()>>,
}

#[derive(Default)]
struct BundleTracker {
    sent: DashMap<String, StdInstant>,
}

impl BundleTracker {
    fn record_send(&self, uuid: &str) {
        self.sent.insert(uuid.to_string(), StdInstant::now());
    }

    fn latency(&self, uuid: &str) -> Option<StdDuration> {
        self.sent.get(uuid).map(|entry| entry.elapsed())
    }

    fn finish(&self, uuid: &str) -> bool {
        self.sent.remove(uuid).is_some()
    }

    fn prune_stale(&self, max_age: StdDuration) -> usize {
        let now = StdInstant::now();
        let mut stale = Vec::new();
        for entry in self.sent.iter() {
            if now.duration_since(*entry.value()) > max_age {
                stale.push(entry.key().clone());
            }
        }
        let mut removed = 0;
        for key in stale {
            if self.sent.remove(&key).is_some() {
                removed += 1;
            }
        }
        removed
    }

    fn inflight(&self) -> usize {
        self.sent.len()
    }
}

#[derive(Default)]
struct BundleFeedback {
    failure_streak: AtomicU64,
}

impl BundleFeedback {
    fn streak(&self) -> u64 {
        self.failure_streak.load(Ordering::Relaxed)
    }

    fn record_failure(&self, metrics: &Metrics) -> u64 {
        let mut current = self.failure_streak.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(1);
            match self.failure_streak.compare_exchange(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    metrics.observe_bundle_failure_streak(next);
                    return next;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn reset(&self, metrics: &Metrics) {
        if self.failure_streak.swap(0, Ordering::Relaxed) != 0 {
            metrics.observe_bundle_failure_streak(0);
        }
    }
}

impl WorkerSet {
    pub fn spawn(
        config_rx: watch::Receiver<Arc<Config>>,
        pipeline: PipelineHandles,
        metrics: Metrics,
        jito: Arc<JitoClient>,
        wallets_rx: watch::Receiver<Arc<LoadedWallets>>,
        ingest_enabled: bool,
        pool_seeds: Arc<PoolSeedRegistry>,
        inferred_seeds: Arc<Mutex<PoolSeedOverlay>>,
    ) -> Self {
        let (stop_tx, _) = broadcast::channel(8);
        let mut join_set = JoinSet::new();
        let bundle_tracker = Arc::new(BundleTracker::default());
        let bundle_feedback = Arc::new(BundleFeedback::default());
        bundle_feedback.reset(&metrics);
        let cfg_seed = config_rx.borrow().clone();
        let tuning = PlannerTuning::from_config(&cfg_seed.token_pairs, &cfg_seed.planner);
        let shared_graph = Arc::new(RwLock::new(ArbGraph::new(&cfg_seed.token_pairs, tuning)));
        let initial_seeds = {
            let mut seeds = pool_seeds.seeds().to_vec();
            match inferred_seeds.lock() {
                Ok(overlay) => seeds.extend(overlay.seeds()),
                Err(err) => {
                    tracing::warn!(
                        target: "arbgraph",
                        "inferred pool seed overlay mutex poisoned; recovering"
                    );
                    seeds.extend(err.into_inner().seeds());
                }
            }
            seeds
        };
        {
            let mut guard = shared_graph
                .write()
                .expect("arbgraph poisoned during bootstrap");
            let seeded = guard.apply_pool_seeds(&initial_seeds);
            if seeded > 0 {
                tracing::info!(
                    target: "arbgraph",
                    seeded,
                    "preloaded pool seeds before workers started"
                );
            }
        }

        if ingest_enabled {
            let cfg_rx = config_rx.clone();
            let stop_rx = stop_tx.subscribe();
            let pipeline = pipeline.clone();
            let metrics = metrics.clone();
            join_set.spawn(async move { ingest::run(cfg_rx, stop_rx, pipeline, metrics).await });
        }

        {
            let cfg_rx = config_rx.clone();
            let wallet_rx = wallets_rx.clone();
            let stop_rx = stop_tx.subscribe();
            let pipeline = pipeline.clone();
            let metrics = metrics.clone();
            let graph = shared_graph.clone();
            let pool_seeds = pool_seeds.clone();
            let inferred_seeds = inferred_seeds.clone();
            join_set.spawn(async move {
                planner::run(
                    cfg_rx,
                    wallet_rx,
                    stop_rx,
                    pipeline,
                    metrics,
                    graph,
                    pool_seeds,
                    inferred_seeds,
                )
                .await
            });
        }

        {
            let cfg_rx = config_rx.clone();
            let wallet_rx = wallets_rx.clone();
            let stop_rx = stop_tx.subscribe();
            let pipeline = pipeline.clone();
            let metrics = metrics.clone();
            let jito = jito.clone();
            let bundle_tracker = bundle_tracker.clone();
            let graph = shared_graph.clone();
            let bundle_feedback = bundle_feedback.clone();
            join_set.spawn(async move {
                executor::run(
                    cfg_rx,
                    stop_rx,
                    wallet_rx,
                    pipeline,
                    metrics,
                    jito,
                    bundle_tracker,
                    graph,
                    bundle_feedback,
                )
                .await
            });
        }

        {
            let stop_rx = stop_tx.subscribe();
            let jito = jito.clone();
            let metrics = metrics.clone();
            let bundle_tracker = bundle_tracker.clone();
            let bundle_feedback = bundle_feedback.clone();
            join_set.spawn(async move {
                bundle_monitor::run(stop_rx, jito, metrics, bundle_tracker, bundle_feedback).await
            });
        }

        Self { stop_tx, join_set }
    }

    pub fn signal(&self, reason: StopReason) {
        let _ = self.stop_tx.send(reason);
    }

    pub async fn shutdown(mut self) -> Result<()> {
        while let Some(res) = self.join_set.join_next().await {
            res??;
        }
        Ok(())
    }
}

pub async fn replay_from_files(
    cfg: Arc<Config>,
    pools_path: Option<PathBuf>,
    trades_path: Option<PathBuf>,
    pipeline: PipelineHandles,
    metrics: Metrics,
) -> Result<()> {
    ingest::replay_from_files(cfg, pools_path, trades_path, pipeline, metrics).await
}

async fn wait_for_stop(stop_rx: &mut broadcast::Receiver<StopReason>) -> Option<StopReason> {
    use tokio::sync::broadcast::error::RecvError;
    match stop_rx.recv().await {
        Ok(reason) => Some(reason),
        Err(RecvError::Lagged(_)) => Some(StopReason::Terminate),
        Err(RecvError::Closed) => None,
    }
}

async fn watch_config(
    cfg_rx: &mut watch::Receiver<Arc<Config>>,
    worker_name: &str,
) -> Option<Arc<Config>> {
    if cfg_rx.changed().await.is_ok() {
        let cfg = cfg_rx.borrow().clone();
        tracing::debug!(worker = worker_name, "config updated");
        Some(cfg)
    } else {
        None
    }
}

async fn watch_wallets(
    wallets_rx: &mut watch::Receiver<Arc<LoadedWallets>>,
) -> Option<Arc<LoadedWallets>> {
    if wallets_rx.changed().await.is_ok() {
        Some(wallets_rx.borrow().clone())
    } else {
        None
    }
}

mod ingest {
    use super::{wait_for_stop, watch_config, StopReason};
    use crate::config::Config;
    use crate::corecast::{self, CorecastClientPool};
    use crate::meteora::{METEORA_DAMM_PROGRAM_ID, METEORA_DLMM_PROGRAM_ID};
    use crate::metrics::Metrics;
    use crate::pipeline::{
        CurveKind, CurveStateUpdate, PipelineHandles, PoolUpdate, TradeAccountMeta, TradeUpdate,
    };
    use crate::proto::{solana_corecast, solana_messages};
    use crate::{meteora, orca, raydium};
    use anyhow::{bail, Context, Result};
    use crossbeam_queue::ArrayQueue;
    use dashmap::{DashMap, DashSet};
    use prost::Message;
    use rand::{thread_rng, Rng};
    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::convert::TryFrom;
    use std::io;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;
    use tokio::sync::{broadcast, watch};
    use tokio::task::JoinSet;
    use tokio::time::sleep;

    const NAME: &str = "worker::ingest";
    const POOLS_NAME: &str = "worker::ingest::pools";
    const TRADES_NAME: &str = "worker::ingest::trades";
    const MIN_SEQUENCE_TRACKED: usize = 1024;
    const BACKOFF_JITTER_PCT: f64 = 0.25;
    const BACKOFF_JITTER_MIN_MS: u64 = 25;
    const BACKOFF_JITTER_MAX_MS: u64 = 750;
    const HEALTH_SNAPSHOT_SECS: u64 = 30;
    const DEFAULT_CURVE_STATE_REFRESH_TTL_SECS: u64 = 45;

    enum RestartReason {
        ConfigUpdate,
        StreamClosed,
    }

    pub async fn run(
        cfg_rx: watch::Receiver<Arc<Config>>,
        mut stop_rx: broadcast::Receiver<StopReason>,
        pipeline: PipelineHandles,
        metrics: Metrics,
    ) -> Result<()> {
        tracing::info!(
            target: NAME,
            pool_queue_capacity = pipeline.pool_updates.capacity(),
            trade_queue_capacity = pipeline.trade_updates.capacity(),
            "ingest worker online"
        );

        let mut join_set = JoinSet::new();

        let pools_cfg = cfg_rx.clone();
        let pools_stop = stop_rx.resubscribe();
        let pools_handles = pipeline.clone();
        let pools_metrics = metrics.clone();
        join_set.spawn(async move {
            pump_pools(pools_cfg, pools_stop, pools_handles, pools_metrics).await
        });

        let trades_cfg = cfg_rx;
        let trades_stop = stop_rx.resubscribe();
        join_set
            .spawn(async move { pump_trades(trades_cfg, trades_stop, pipeline, metrics).await });

        loop {
            tokio::select! {
                stop = wait_for_stop(&mut stop_rx) => {
                    if let Some(reason) = stop {
                        tracing::info!(target: NAME, ?reason, "stopping ingest supervisor");
                    }
                    break;
                }
                Some(res) = join_set.join_next() => {
                    res??;
                    tracing::warn!(target: NAME, "ingest subtask exited; stopping supervisor");
                    break;
                }
            }
        }

        while let Some(res) = join_set.join_next().await {
            res??;
        }

        tracing::info!(target: NAME, "ingest worker shutdown complete");
        Ok(())
    }

    async fn pump_pools(
        mut cfg_rx: watch::Receiver<Arc<Config>>,
        mut stop_rx: broadcast::Receiver<StopReason>,
        pipeline: PipelineHandles,
        metrics: Metrics,
    ) -> Result<()> {
        let queue = pipeline.pool_updates.clone();
        let state_queue = pipeline.curve_states.clone();
        let mut backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(5));
        let mut cfg_snapshot = cfg_rx.borrow().clone();
        let mut client_pool = CorecastClientPool::new(&cfg_snapshot.corecast);
        let mut prefetcher = CurvePrefetcher::new(&cfg_snapshot, state_queue, metrics.clone());
        let mut seq_filter = SequenceFilter::new(sequence_filter_capacity(&cfg_snapshot));
        let mut eviction_warn = sequence_eviction_warn(&cfg_snapshot);
        let mut pair_allowlist = PairAllowlist::new(&cfg_snapshot);
        let mut health = StreamHealth::new("pools", POOLS_NAME);
        metrics.observe_ingest_sequence_occupancy("pools", seq_filter.len(), seq_filter.capacity());

        loop {
            health.try_log(queue.len(), &seq_filter);
            let client = match client_pool.client().await {
                Ok(client) => client,
                Err(err) => {
                    metrics.inc_ingest_error("pools");
                    health.record_error();
                    metrics.inc_ingest_restart("pools", "connect_error");
                    health.record_restart();
                    tracing::warn!(target: POOLS_NAME, ?err, "corecast connect failed; backing off");
                    let (delay, jitter) = backoff.next_delay_with_jitter();
                    metrics.observe_ingest_backoff("pools", delay);
                    metrics.observe_ingest_backoff_jitter("pools", jitter);
                    if wait_delay_or_stop(delay, &mut stop_rx, POOLS_NAME).await? {
                        return Ok(());
                    }
                    continue;
                }
            };

            let request = build_pool_request(&cfg_snapshot);
            let mut stream = match client.subscribe_pools(request).await {
                Ok(stream) => {
                    tracing::info!(target: POOLS_NAME, "connected to pool stream");
                    backoff.reset();
                    metrics.observe_ingest_backoff("pools", Duration::from_millis(0));
                    metrics.observe_ingest_backoff_jitter("pools", Duration::from_millis(0));
                    stream
                }
                Err(err) => {
                    metrics.inc_ingest_error("pools");
                    health.record_error();
                    metrics.inc_ingest_restart("pools", "subscribe_error");
                    health.record_restart();
                    tracing::warn!(target: POOLS_NAME, ?err, "subscribe pools failed; reconnecting");
                    client_pool.invalidate();
                    let (delay, jitter) = backoff.next_delay_with_jitter();
                    metrics.observe_ingest_backoff("pools", delay);
                    metrics.observe_ingest_backoff_jitter("pools", jitter);
                    if wait_delay_or_stop(delay, &mut stop_rx, POOLS_NAME).await? {
                        return Ok(());
                    }
                    continue;
                }
            };

            let restart_reason = loop {
                health.try_log(queue.len(), &seq_filter);
                tokio::select! {
                    stop = wait_for_stop(&mut stop_rx) => {
                        if let Some(reason) = stop {
                            tracing::info!(target: POOLS_NAME, ?reason, "pool stream stopping");
                        }
                        return Ok(());
                    }
                    cfg = watch_config(&mut cfg_rx, POOLS_NAME) => {
                        match cfg {
                            Some(cfg) => {
                                cfg_snapshot = cfg;
                                client_pool.reconfigure(&cfg_snapshot.corecast);
                                prefetcher.reconfigure(&cfg_snapshot);
                                seq_filter.reset(sequence_filter_capacity(&cfg_snapshot));
                                eviction_warn = sequence_eviction_warn(&cfg_snapshot);
                                pair_allowlist = PairAllowlist::new(&cfg_snapshot);
                                metrics.observe_ingest_sequence_occupancy(
                                    "pools",
                                    seq_filter.len(),
                                    seq_filter.capacity(),
                                );
                                metrics.inc_ingest_restart("pools", "config_update");
                                health.record_restart();
                                tracing::info!(target: POOLS_NAME, "config updated; restarting stream");
                                break RestartReason::ConfigUpdate;
                            }
                            None => {
                                tracing::info!(target: POOLS_NAME, "config channel closed; exiting pool loop");
                                return Ok(());
                            }
                        }
                    }
                    message = stream.message() => {
                        match message {
                            Ok(Some(msg)) => {
                                if let Some(update) = convert_pool_update(msg) {
                                    // Temporary instrumentation to confirm pool updates reach the ingest queue.
                                    tracing::debug!(
                                        target: POOLS_NAME,
                                        queue = "pool_updates",
                                        pool = %update.pool,
                                        program = %update.program,
                                        base_mint = %update.base_mint,
                                        quote_mint = %update.quote_mint,
                                        slot = update.slot,
                                        seq = update.seq,
                                        ?update.curve,
                                        "received pool update"
                                    );
                                    if !pair_allowlist.contains(update.base_mint, update.quote_mint) {
                                        metrics.inc_ingest_pair_filter("pools");
                                        tracing::trace!(
                                            target: POOLS_NAME,
                                            queue = "pool_updates",
                                            pool = %update.pool,
                                            base_mint = %update.base_mint,
                                            quote_mint = %update.quote_mint,
                                            "pool update filtered by token pair allowlist"
                                        );
                                        continue;
                                    }
                                    let accepted = seq_filter.accept(update.pool, update.slot, update.seq);
                                    let evicted = seq_filter.take_evictions();
                                    record_sequence_evictions(
                                        "pools",
                                        POOLS_NAME,
                                        evicted,
                                        eviction_warn,
                                        &seq_filter,
                                        &metrics,
                                    );
                                    metrics.observe_ingest_sequence_occupancy(
                                        "pools",
                                        seq_filter.len(),
                                        seq_filter.capacity(),
                                    );
                                    if !accepted {
                                        metrics.inc_ingest_stale("pools");
                                        health.record_stale();
                                        tracing::debug!(
                                            target: POOLS_NAME,
                                            queue = "pool_updates",
                                            pool = %update.pool,
                                            slot = update.slot,
                                            seq = update.seq,
                                            "stale pool update dropped"
                                        );
                                        continue;
                                    }

                                    let pool = update.pool;
                                    let curve = update.curve;
                                    prefetcher.maybe_prefetch(pool, curve);
                                    push_with_drop(&queue, update, "pool_updates", &metrics, |ctx| {
                                        tracing::warn!(
                                            target: POOLS_NAME,
                                            queue = "pool_updates",
                                            pool = %ctx.pool,
                                            program = %ctx.program,
                                            base_mint = %ctx.base_mint,
                                            quote_mint = %ctx.quote_mint,
                                            slot = ctx.slot,
                                            seq = ctx.seq,
                                            "pool update dropped due to saturation"
                                        );
                                    });
                                    metrics.inc_ingest_message("pools");
                                    health.record_message();
                                }
                            }
                            Ok(None) => {
                                metrics.inc_ingest_error("pools");
                                health.record_error();
                                metrics.inc_ingest_restart("pools", "stream_closed");
                                health.record_restart();
                                tracing::warn!(target: POOLS_NAME, "pool stream closed by server; reconnecting");
                                break RestartReason::StreamClosed;
                            }
                            Err(status) => {
                                metrics.inc_ingest_error("pools");
                                health.record_error();
                                metrics.inc_ingest_restart("pools", "stream_error");
                                health.record_restart();
                                tracing::warn!(target: POOLS_NAME, error = %status, "pool stream errored; reconnecting");
                                break RestartReason::StreamClosed;
                            }
                        }
                    }
                }
            };

            match restart_reason {
                RestartReason::ConfigUpdate => continue,
                RestartReason::StreamClosed => {
                    client_pool.invalidate();
                    let (delay, jitter) = backoff.next_delay_with_jitter();
                    metrics.observe_ingest_backoff("pools", delay);
                    metrics.observe_ingest_backoff_jitter("pools", jitter);
                    tracing::info!(
                        target: POOLS_NAME,
                        ?delay,
                        jitter_ms = jitter.as_millis() as u64,
                        "pool stream reconnect backoff"
                    );
                    if wait_delay_or_stop(delay, &mut stop_rx, POOLS_NAME).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn pump_trades(
        mut cfg_rx: watch::Receiver<Arc<Config>>,
        mut stop_rx: broadcast::Receiver<StopReason>,
        pipeline: PipelineHandles,
        metrics: Metrics,
    ) -> Result<()> {
        let queue = pipeline.trade_updates.clone();
        let mut backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(5));
        let mut cfg_snapshot = cfg_rx.borrow().clone();
        let mut client_pool = CorecastClientPool::new(&cfg_snapshot.corecast);
        let mut seq_filter = SequenceFilter::new(sequence_filter_capacity(&cfg_snapshot));
        let mut eviction_warn = sequence_eviction_warn(&cfg_snapshot);
        let mut pair_allowlist = PairAllowlist::new(&cfg_snapshot);
        let mut health = StreamHealth::new("trades", TRADES_NAME);
        metrics.observe_ingest_sequence_occupancy(
            "trades",
            seq_filter.len(),
            seq_filter.capacity(),
        );

        loop {
            health.try_log(queue.len(), &seq_filter);
            let client = match client_pool.client().await {
                Ok(client) => client,
                Err(err) => {
                    metrics.inc_ingest_error("trades");
                    health.record_error();
                    metrics.inc_ingest_restart("trades", "connect_error");
                    health.record_restart();
                    tracing::warn!(target: TRADES_NAME, ?err, "corecast connect failed; backing off");
                    let (delay, jitter) = backoff.next_delay_with_jitter();
                    metrics.observe_ingest_backoff("trades", delay);
                    metrics.observe_ingest_backoff_jitter("trades", jitter);
                    if wait_delay_or_stop(delay, &mut stop_rx, TRADES_NAME).await? {
                        return Ok(());
                    }
                    continue;
                }
            };

            let request = build_trade_request(&cfg_snapshot);
            let mut stream = match client.subscribe_trades(request).await {
                Ok(stream) => {
                    tracing::info!(target: TRADES_NAME, "connected to trade stream");
                    backoff.reset();
                    metrics.observe_ingest_backoff("trades", Duration::from_millis(0));
                    metrics.observe_ingest_backoff_jitter("trades", Duration::from_millis(0));
                    stream
                }
                Err(err) => {
                    metrics.inc_ingest_error("trades");
                    health.record_error();
                    metrics.inc_ingest_restart("trades", "subscribe_error");
                    health.record_restart();
                    tracing::warn!(target: TRADES_NAME, ?err, "subscribe trades failed; reconnecting");
                    client_pool.invalidate();
                    let (delay, jitter) = backoff.next_delay_with_jitter();
                    metrics.observe_ingest_backoff("trades", delay);
                    metrics.observe_ingest_backoff_jitter("trades", jitter);
                    if wait_delay_or_stop(delay, &mut stop_rx, TRADES_NAME).await? {
                        return Ok(());
                    }
                    continue;
                }
            };

            let restart_reason = loop {
                health.try_log(queue.len(), &seq_filter);
                tokio::select! {
                    stop = wait_for_stop(&mut stop_rx) => {
                        if let Some(reason) = stop {
                            tracing::info!(target: TRADES_NAME, ?reason, "trade stream stopping");
                        }
                        return Ok(());
                    }
                    cfg = watch_config(&mut cfg_rx, TRADES_NAME) => {
                        match cfg {
                            Some(cfg) => {
                                cfg_snapshot = cfg;
                                client_pool.reconfigure(&cfg_snapshot.corecast);
                                seq_filter.reset(sequence_filter_capacity(&cfg_snapshot));
                                eviction_warn = sequence_eviction_warn(&cfg_snapshot);
                                pair_allowlist = PairAllowlist::new(&cfg_snapshot);
                                metrics.observe_ingest_sequence_occupancy(
                                    "trades",
                                    seq_filter.len(),
                                    seq_filter.capacity(),
                                );
                                metrics.inc_ingest_restart("trades", "config_update");
                                health.record_restart();
                                tracing::info!(target: TRADES_NAME, "config updated; restarting stream");
                                break RestartReason::ConfigUpdate;
                            }
                            None => {
                                tracing::info!(target: TRADES_NAME, "config channel closed; exiting trade loop");
                                return Ok(());
                            }
                        }
                    }
                    message = stream.message() => {
                        match message {
                            Ok(Some(msg)) => {
                                if let Some(update) = convert_trade_update(msg) {
                                    if !pair_allowlist.contains(update.base_mint, update.quote_mint) {
                                        metrics.inc_ingest_pair_filter("trades");
                                        tracing::trace!(
                                            target: TRADES_NAME,
                                            queue = "trade_updates",
                                            pool = %update.pool,
                                            base_mint = %update.base_mint,
                                            quote_mint = %update.quote_mint,
                                            "trade update filtered by token pair allowlist"
                                        );
                                        continue;
                                    }
                                    let accepted = seq_filter.accept(
                                        update.pool,
                                        update.slot,
                                        update.instruction_index as u64,
                                    );
                                    let evicted = seq_filter.take_evictions();
                                    record_sequence_evictions(
                                        "trades",
                                        TRADES_NAME,
                                        evicted,
                                        eviction_warn,
                                        &seq_filter,
                                        &metrics,
                                    );
                                    metrics.observe_ingest_sequence_occupancy(
                                        "trades",
                                        seq_filter.len(),
                                        seq_filter.capacity(),
                                    );
                                    if !accepted {
                                        metrics.inc_ingest_stale("trades");
                                        health.record_stale();
                                        tracing::debug!(
                                            target: TRADES_NAME,
                                            queue = "trade_updates",
                                            pool = %update.pool,
                                            slot = update.slot,
                                            instruction_index = update.instruction_index,
                                            "stale trade update dropped"
                                        );
                                        continue;
                                    }

                                    push_with_drop(&queue, update, "trade_updates", &metrics, |ctx| {
                                        tracing::warn!(
                                            target: TRADES_NAME,
                                            queue = "trade_updates",
                                            pool = %ctx.pool,
                                            trader = %ctx.trader,
                                            base_mint = %ctx.base_mint,
                                            quote_mint = %ctx.quote_mint,
                                            slot = ctx.slot,
                                            "trade update dropped due to saturation"
                                        );
                                    });
                                    metrics.inc_ingest_message("trades");
                                    health.record_message();
                                }
                            }
                            Ok(None) => {
                                metrics.inc_ingest_error("trades");
                                health.record_error();
                                metrics.inc_ingest_restart("trades", "stream_closed");
                                health.record_restart();
                                tracing::warn!(target: TRADES_NAME, "trade stream closed by server; reconnecting");
                                break RestartReason::StreamClosed;
                            }
                            Err(status) => {
                                metrics.inc_ingest_error("trades");
                                health.record_error();
                                metrics.inc_ingest_restart("trades", "stream_error");
                                health.record_restart();
                                tracing::warn!(target: TRADES_NAME, error = %status, "trade stream errored; reconnecting");
                                break RestartReason::StreamClosed;
                            }
                        }
                    }
                }
            };

            match restart_reason {
                RestartReason::ConfigUpdate => continue,
                RestartReason::StreamClosed => {
                    client_pool.invalidate();
                    let (delay, jitter) = backoff.next_delay_with_jitter();
                    metrics.observe_ingest_backoff("trades", delay);
                    metrics.observe_ingest_backoff_jitter("trades", jitter);
                    tracing::info!(
                        target: TRADES_NAME,
                        ?delay,
                        jitter_ms = jitter.as_millis() as u64,
                        "trade stream reconnect backoff"
                    );
                    if wait_delay_or_stop(delay, &mut stop_rx, TRADES_NAME).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn wait_delay_or_stop(
        delay: Duration,
        stop_rx: &mut broadcast::Receiver<StopReason>,
        stream_label: &'static str,
    ) -> Result<bool> {
        tokio::select! {
            _ = sleep(delay) => Ok(false),
            stop = wait_for_stop(stop_rx) => {
                if let Some(reason) = stop {
                    tracing::info!(stream = stream_label, ?reason, "stream stopping during backoff");
                }
                Ok(true)
            }
        }
    }

    fn build_pool_request(cfg: &Config) -> solana_corecast::SubscribePoolsRequest {
        let mut req = solana_corecast::SubscribePoolsRequest::default();
        req.program = corecast::address_filter(&cfg.program_addresses);
        let tokens = collect_allowed_tokens(cfg);
        req.token = corecast::address_filter(&tokens);
        req
    }

    fn build_trade_request(cfg: &Config) -> solana_corecast::SubscribeTradesRequest {
        let mut req = solana_corecast::SubscribeTradesRequest::default();
        req.program = corecast::address_filter(&cfg.program_addresses);
        let tokens = collect_allowed_tokens(cfg);
        req.token = corecast::address_filter(&tokens);
        req
    }

    fn collect_allowed_tokens(cfg: &Config) -> Vec<Pubkey> {
        let mut uniques = HashSet::with_capacity(cfg.token_pairs.len() * 2);
        for pair in &cfg.token_pairs {
            uniques.insert(pair.base);
            uniques.insert(pair.quote);
        }
        uniques.into_iter().collect()
    }

    #[derive(Clone)]
    struct PairAllowlist {
        pairs: HashSet<(Pubkey, Pubkey)>,
    }

    impl PairAllowlist {
        fn new(cfg: &Config) -> Self {
            let mut pairs = HashSet::with_capacity(cfg.token_pairs.len());
            for pair in &cfg.token_pairs {
                pairs.insert(normalize_pair(pair.base, pair.quote));
            }
            Self { pairs }
        }

        fn contains(&self, base: Pubkey, quote: Pubkey) -> bool {
            if base == Pubkey::default() || quote == Pubkey::default() {
                return false;
            }
            self.pairs.contains(&normalize_pair(base, quote))
        }
    }

    fn normalize_pair(a: Pubkey, b: Pubkey) -> (Pubkey, Pubkey) {
        if a == b {
            return (a, b);
        }
        let a_bytes = a.to_bytes();
        let b_bytes = b.to_bytes();
        if a_bytes <= b_bytes {
            (a, b)
        } else {
            (b, a)
        }
    }

    fn sequence_filter_capacity(cfg: &Config) -> usize {
        if let Some(explicit) = cfg.corecast.cache.sequence_capacity {
            return explicit.max(MIN_SEQUENCE_TRACKED);
        }
        let queue_cap = cfg.corecast.cache.channel_capacity.max(1);
        let pair_estimate = cfg.token_pairs.len().saturating_mul(8).max(1);
        queue_cap.max(pair_estimate).max(MIN_SEQUENCE_TRACKED)
    }

    fn sequence_eviction_warn(cfg: &Config) -> u64 {
        cfg.corecast.cache.sequence_eviction_warn.unwrap_or(0)
    }

    pub async fn replay_from_files(
        cfg: Arc<Config>,
        pools_path: Option<PathBuf>,
        trades_path: Option<PathBuf>,
        pipeline: PipelineHandles,
        metrics: Metrics,
    ) -> Result<()> {
        if pools_path.is_none() && trades_path.is_none() {
            bail!("replay mode requires at least one pools or trades file");
        }
        let mut join_set = JoinSet::new();

        if let Some(path) = pools_path {
            let cfg = cfg.clone();
            let queue = pipeline.pool_updates.clone();
            let state_queue = pipeline.curve_states.clone();
            let metrics_clone = metrics.clone();
            join_set.spawn(async move {
                replay_pool_file(cfg, path, queue, state_queue, metrics_clone).await
            });
        }

        if let Some(path) = trades_path {
            let cfg = cfg.clone();
            let queue = pipeline.trade_updates.clone();
            let metrics_clone = metrics.clone();
            join_set.spawn(async move { replay_trade_file(cfg, path, queue, metrics_clone).await });
        }

        while let Some(res) = join_set.join_next().await {
            res??;
        }

        Ok(())
    }

    struct CurvePrefetcher {
        rpc_clients: Arc<Vec<Arc<RpcClient>>>,
        queue: Arc<ArrayQueue<CurveStateUpdate>>,
        metrics: Metrics,
        inflight: Arc<DashSet<Pubkey>>,
        fetched: Arc<DashMap<Pubkey, Instant>>,
        ttl: Duration,
    }

    impl CurvePrefetcher {
        fn new(
            cfg: &Arc<Config>,
            queue: Arc<ArrayQueue<CurveStateUpdate>>,
            metrics: Metrics,
        ) -> Self {
            let ttl = curve_state_ttl(cfg);
            Self {
                rpc_clients: Arc::new(build_rpc_clients(cfg)),
                queue,
                metrics,
                inflight: Arc::new(DashSet::new()),
                fetched: Arc::new(DashMap::new()),
                ttl,
            }
        }

        fn reconfigure(&mut self, cfg: &Arc<Config>) {
            self.rpc_clients = Arc::new(build_rpc_clients(cfg));
            self.ttl = curve_state_ttl(cfg);
            self.fetched.clear();
        }

        fn maybe_prefetch(&self, pool: Pubkey, curve: CurveKind) {
            if !matches!(
                curve,
                CurveKind::RaydiumClmm
                    | CurveKind::OrcaWhirlpool
                    | CurveKind::MeteoraDamm
                    | CurveKind::MeteoraDlmm
            ) {
                return;
            }
            let curve_label = ingest_curve_label(curve);
            if self.rpc_clients.is_empty() {
                tracing::warn!(
                    target: NAME,
                    pool = %pool,
                    "curve prefetcher has no RPC endpoints configured"
                );
                self.metrics
                    .inc_curve_state_fetch(curve_label, "rpc_unavailable");
                return;
            }
            if let Some(entry) = self.fetched.get(&pool) {
                if entry.value().elapsed() < self.ttl {
                    return;
                }
            }
            if !self.inflight.insert(pool) {
                return;
            }

            let queue = self.queue.clone();
            let metrics = self.metrics.clone();
            let inflight = self.inflight.clone();
            let fetched = self.fetched.clone();
            let rpc_clients = self.rpc_clients.clone();
            tokio::spawn(async move {
                let update = fetch_curve_state_from_any(rpc_clients, pool, curve).await;

                if let Some((state, attempts)) = update {
                    push_with_drop(&queue, state, "curve_states", &metrics, |ctx| {
                        let (curve, pool) = curve_state_meta(ctx);
                        tracing::warn!(
                            target: NAME,
                            queue = "curve_states",
                            curve,
                            pool = %pool,
                            "curve state dropped due to saturation"
                        );
                    });
                    fetched.insert(pool, Instant::now());
                    let status = if attempts > 1 {
                        "success_fallback"
                    } else {
                        "success"
                    };
                    metrics.inc_curve_state_fetch(curve_label, status);
                } else {
                    tracing::warn!(
                        target: NAME,
                        pool = %pool,
                        curve = ?curve,
                        "failed to fetch curve state from all RPC endpoints"
                    );
                    metrics.inc_curve_state_fetch(curve_label, "failed");
                }

                inflight.remove(&pool);
            });
        }
    }

    async fn fetch_curve_state_from_any(
        rpc_clients: Arc<Vec<Arc<RpcClient>>>,
        pool: Pubkey,
        curve: CurveKind,
    ) -> Option<(CurveStateUpdate, usize)> {
        if rpc_clients.is_empty() {
            return None;
        }

        for (attempt, rpc) in rpc_clients.iter().enumerate() {
            let result = match curve {
                CurveKind::RaydiumClmm => fetch_raydium_state(rpc.clone(), pool).await,
                CurveKind::OrcaWhirlpool => fetch_orca_state(rpc.clone(), pool).await,
                CurveKind::MeteoraDamm => fetch_meteora_damm_state(rpc.clone(), pool).await,
                CurveKind::MeteoraDlmm => fetch_meteora_dlmm_state(rpc.clone(), pool).await,
            };

            if result.is_some() {
                if attempt > 0 {
                    tracing::debug!(
                        target: NAME,
                        pool = %pool,
                        curve = ?curve,
                        attempts = attempt + 1,
                        "curve state fetched via fallback RPC endpoint"
                    );
                }
                return result.map(|state| (state, attempt + 1));
            }
        }

        None
    }

    async fn fetch_raydium_state(rpc: Arc<RpcClient>, pool: Pubkey) -> Option<CurveStateUpdate> {
        match rpc.get_account_data(&pool).await {
            Ok(data) => match raydium::RaydiumPoolParams::decode(&data) {
                Some(params) => Some(CurveStateUpdate::RaydiumClmm { pool, params }),
                None => {
                    tracing::warn!(target: NAME, pool = %pool, "failed to decode raydium pool account");
                    None
                }
            },
            Err(err) => {
                tracing::warn!(target: NAME, pool = %pool, ?err, "rpc error fetching raydium pool");
                None
            }
        }
    }

    async fn fetch_orca_state(rpc: Arc<RpcClient>, pool: Pubkey) -> Option<CurveStateUpdate> {
        match rpc.get_account_data(&pool).await {
            Ok(data) => match orca::OrcaPoolParams::decode(&data) {
                Some(params) => Some(CurveStateUpdate::OrcaWhirlpool { pool, params }),
                None => {
                    tracing::warn!(target: NAME, pool = %pool, "failed to decode orca whirlpool account");
                    None
                }
            },
            Err(err) => {
                tracing::warn!(target: NAME, pool = %pool, ?err, "rpc error fetching orca whirlpool");
                None
            }
        }
    }

    async fn fetch_meteora_damm_state(
        rpc: Arc<RpcClient>,
        pool: Pubkey,
    ) -> Option<CurveStateUpdate> {
        match rpc.get_account_data(&pool).await {
            Ok(data) => match meteora::MeteoraDammParams::decode(&data) {
                Some(params) => Some(CurveStateUpdate::MeteoraDamm { pool, params }),
                None => {
                    tracing::warn!(target: NAME, pool = %pool, "failed to decode meteora damm account");
                    None
                }
            },
            Err(err) => {
                tracing::warn!(target: NAME, pool = %pool, ?err, "rpc error fetching meteora damm");
                None
            }
        }
    }

    async fn fetch_meteora_dlmm_state(
        rpc: Arc<RpcClient>,
        pool: Pubkey,
    ) -> Option<CurveStateUpdate> {
        match rpc.get_account_data(&pool).await {
            Ok(data) => match meteora::MeteoraDlmmParams::decode(&data) {
                Some(params) => Some(CurveStateUpdate::MeteoraDlmm { pool, params }),
                None => {
                    tracing::warn!(target: NAME, pool = %pool, "failed to decode meteora dlmm account");
                    None
                }
            },
            Err(err) => {
                tracing::warn!(target: NAME, pool = %pool, ?err, "rpc error fetching meteora dlmm");
                None
            }
        }
    }

    fn curve_state_meta(update: &CurveStateUpdate) -> (&'static str, Pubkey) {
        match update {
            CurveStateUpdate::RaydiumClmm { pool, .. } => ("raydium", *pool),
            CurveStateUpdate::OrcaWhirlpool { pool, .. } => ("orca", *pool),
            CurveStateUpdate::MeteoraDamm { pool, .. } => ("meteora_damm", *pool),
            CurveStateUpdate::MeteoraDlmm { pool, .. } => ("meteora_dlmm", *pool),
        }
    }

    fn ingest_curve_label(curve: CurveKind) -> &'static str {
        match curve {
            CurveKind::RaydiumClmm => "raydium",
            CurveKind::OrcaWhirlpool => "orca",
            CurveKind::MeteoraDamm => "meteora_damm",
            CurveKind::MeteoraDlmm => "meteora_dlmm",
        }
    }

    fn build_rpc_clients(cfg: &Arc<Config>) -> Vec<Arc<RpcClient>> {
        cfg.quicknode_rpcs
            .iter()
            .filter_map(|url| {
                if url.is_empty() {
                    None
                } else {
                    Some(Arc::new(RpcClient::new(url.clone())))
                }
            })
            .collect()
    }

    fn curve_state_ttl(cfg: &Arc<Config>) -> Duration {
        let secs = cfg
            .corecast
            .curve_state_ttl_secs
            .unwrap_or(DEFAULT_CURVE_STATE_REFRESH_TTL_SECS)
            .max(1);
        Duration::from_secs(secs)
    }

    async fn replay_pool_file(
        cfg: Arc<Config>,
        path: PathBuf,
        queue: Arc<ArrayQueue<PoolUpdate>>,
        state_queue: Arc<ArrayQueue<CurveStateUpdate>>,
        metrics: Metrics,
    ) -> Result<()> {
        tracing::info!(target: POOLS_NAME, ?path, "replaying pools stream from file");
        let mut file = File::open(&path)
            .await
            .with_context(|| format!("open pools replay file {path:?}"))?;
        let prefetcher = CurvePrefetcher::new(&cfg, state_queue, metrics.clone());
        let mut seq_filter = SequenceFilter::new(sequence_filter_capacity(&cfg));
        let eviction_warn = sequence_eviction_warn(&cfg);
        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read pools replay length from {path:?}"));
                }
            }
            let frame_len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; frame_len];
            file.read_exact(&mut buf)
                .await
                .with_context(|| format!("read pools replay frame from {path:?}"))?;
            match solana_corecast::PoolLiquidityChangeStreamMessage::decode(&*buf) {
                Ok(msg) => {
                    if let Some(update) = convert_pool_update(msg) {
                        let accepted = seq_filter.accept(update.pool, update.slot, update.seq);
                        let evicted = seq_filter.take_evictions();
                        record_sequence_evictions(
                            "pools",
                            POOLS_NAME,
                            evicted,
                            eviction_warn,
                            &seq_filter,
                            &metrics,
                        );
                        metrics.observe_ingest_sequence_occupancy(
                            "pools",
                            seq_filter.len(),
                            seq_filter.capacity(),
                        );
                        if !accepted {
                            metrics.inc_ingest_stale("pools");
                            tracing::debug!(
                                target: POOLS_NAME,
                                queue = "pool_updates",
                                pool = %update.pool,
                                slot = update.slot,
                                seq = update.seq,
                                "stale pool update dropped during replay"
                            );
                            continue;
                        }

                        let pool = update.pool;
                        let curve = update.curve;
                        prefetcher.maybe_prefetch(pool, curve);
                        push_with_drop(&queue, update, "pool_updates", &metrics, |ctx| {
                            tracing::warn!(
                                target: POOLS_NAME,
                                queue = "pool_updates",
                                pool = %ctx.pool,
                                program = %ctx.program,
                                base_mint = %ctx.base_mint,
                                quote_mint = %ctx.quote_mint,
                                slot = ctx.slot,
                                seq = ctx.seq,
                                "pool update dropped during replay"
                            );
                        });
                        metrics.inc_ingest_message("pools");
                    }
                }
                Err(err) => {
                    tracing::warn!(target: POOLS_NAME, ?err, "failed to decode pool replay frame");
                    metrics.inc_ingest_error("pools");
                }
            }
        }
        tracing::info!(target: POOLS_NAME, ?path, "completed pools replay");
        Ok(())
    }

    async fn replay_trade_file(
        cfg: Arc<Config>,
        path: PathBuf,
        queue: Arc<ArrayQueue<TradeUpdate>>,
        metrics: Metrics,
    ) -> Result<()> {
        tracing::info!(target: TRADES_NAME, ?path, "replaying trades stream from file");
        let mut file = File::open(&path)
            .await
            .with_context(|| format!("open trades replay file {path:?}"))?;
        let mut seq_filter = SequenceFilter::new(sequence_filter_capacity(&cfg));
        let eviction_warn = sequence_eviction_warn(&cfg);
        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read trades replay length from {path:?}"));
                }
            }
            let frame_len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; frame_len];
            file.read_exact(&mut buf)
                .await
                .with_context(|| format!("read trades replay frame from {path:?}"))?;
            match solana_corecast::DexTradeStreamMessage::decode(&*buf) {
                Ok(msg) => {
                    if let Some(update) = convert_trade_update(msg) {
                        let accepted = seq_filter.accept(
                            update.pool,
                            update.slot,
                            update.instruction_index as u64,
                        );
                        let evicted = seq_filter.take_evictions();
                        record_sequence_evictions(
                            "trades",
                            TRADES_NAME,
                            evicted,
                            eviction_warn,
                            &seq_filter,
                            &metrics,
                        );
                        metrics.observe_ingest_sequence_occupancy(
                            "trades",
                            seq_filter.len(),
                            seq_filter.capacity(),
                        );
                        if !accepted {
                            metrics.inc_ingest_stale("trades");
                            tracing::debug!(
                                target: TRADES_NAME,
                                queue = "trade_updates",
                                pool = %update.pool,
                                slot = update.slot,
                                instruction_index = update.instruction_index,
                                "stale trade update dropped during replay"
                            );
                            continue;
                        }
                        push_with_drop(&queue, update, "trade_updates", &metrics, |ctx| {
                            tracing::warn!(
                                target: TRADES_NAME,
                                queue = "trade_updates",
                                pool = %ctx.pool,
                                trader = %ctx.trader,
                                base_mint = %ctx.base_mint,
                                quote_mint = %ctx.quote_mint,
                                slot = ctx.slot,
                                "trade update dropped during replay"
                            );
                        });
                        metrics.inc_ingest_message("trades");
                    }
                }
                Err(err) => {
                    tracing::warn!(target: TRADES_NAME, ?err, "failed to decode trade replay frame");
                    metrics.inc_ingest_error("trades");
                }
            }
        }
        tracing::info!(target: TRADES_NAME, ?path, "completed trades replay");
        Ok(())
    }

    fn convert_pool_update(
        msg: solana_corecast::PoolLiquidityChangeStreamMessage,
    ) -> Option<PoolUpdate> {
        let block = msg.block?;
        let event = msg.pool_event?;
        let market = event.market?;
        let dex = event.dex?;
        let pool = decode_pubkey(&market.market_address)?;
        let program = decode_pubkey(&dex.program_address)?;
        let curve = infer_curve_kind(&dex, &program)?;
        let base_currency = market.base_currency.as_ref();
        let quote_currency = market.quote_currency.as_ref();
        let base_side = event.base_currency.as_ref();
        let quote_side = event.quote_currency.as_ref();
        let base_mint = base_currency
            .and_then(|cur| decode_pubkey(&cur.mint_address))
            .unwrap_or_default();
        let quote_mint = quote_currency
            .and_then(|cur| decode_pubkey(&cur.mint_address))
            .unwrap_or_default();
        let base_decimals = base_currency
            .map(|cur| clamp_decimals(cur.decimals))
            .unwrap_or_default();
        let quote_decimals = quote_currency
            .map(|cur| clamp_decimals(cur.decimals))
            .unwrap_or_default();
        let base_reserve = base_side.map(|side| side.post_amount).unwrap_or_default();
        let quote_reserve = quote_side.map(|side| side.post_amount).unwrap_or_default();
        let block_time = block_timestamp(&block).unwrap_or_else(unix_timestamp);
        let signature = msg
            .transaction
            .as_ref()
            .and_then(|tx| decode_signature(&tx.signature));

        Some(PoolUpdate {
            pool,
            program,
            curve,
            slot: block.slot,
            block_time,
            unix_timestamp: unix_timestamp(),
            seq: event.instruction_index as u64,
            base_mint,
            quote_mint,
            base_reserve,
            quote_reserve,
            base_decimals,
            quote_decimals,
            signature,
        })
    }

    fn convert_trade_update(msg: solana_corecast::DexTradeStreamMessage) -> Option<TradeUpdate> {
        let block = msg.block?;
        let trade = msg.trade?;
        let market = trade.market?;
        let dex = trade.dex?;
        let pool = decode_pubkey(&market.market_address)?;
        let instruction_index = trade.instruction_index;
        let program = decode_pubkey(&dex.program_address).unwrap_or_default();
        let curve_hint = infer_curve_kind(&dex, &program);

        let trader = trade
            .buy
            .as_ref()
            .and_then(|side| side.account.as_ref())
            .and_then(|acct| decode_pubkey(&acct.address))
            .or_else(|| {
                trade
                    .sell
                    .as_ref()
                    .and_then(|side| side.account.as_ref())
                    .and_then(|acct| decode_pubkey(&acct.address))
            })?;

        let base_amount = trade
            .buy
            .as_ref()
            .map(|side| side.amount as i128)
            .unwrap_or_default();
        let quote_amount = trade
            .sell
            .as_ref()
            .map(|side| side.amount as i128)
            .unwrap_or_default();
        let (base_mint, base_decimals) = trade
            .buy
            .as_ref()
            .map(|side| side_currency_or_account_meta(side))
            .unwrap_or_default();
        let (quote_mint, quote_decimals) = trade
            .sell
            .as_ref()
            .map(|side| side_currency_or_account_meta(side))
            .unwrap_or_default();
        let block_time = block_timestamp(&block).unwrap_or_else(unix_timestamp);
        let signature = msg
            .transaction
            .as_ref()
            .and_then(|tx| decode_signature(&tx.signature));
        let accounts = extract_trade_accounts(msg.transaction.as_ref());

        Some(TradeUpdate {
            pool,
            trader,
            base_amount,
            quote_amount,
            base_mint,
            quote_mint,
            base_decimals,
            quote_decimals,
            slot: block.slot,
            block_time,
            signature,
            instruction_index,
            program,
            curve_hint,
            accounts,
        })
    }

    fn side_currency_or_account_meta(side: &solana_messages::DexTradeSide) -> (Pubkey, u8) {
        if let Some(currency) = side.currency.as_ref() {
            let (mint, decimals) = currency_meta(currency);
            if mint != Pubkey::default() {
                return (mint, decimals);
            }
        }
        side.account
            .as_ref()
            .map(account_token_meta)
            .unwrap_or((Pubkey::default(), 0))
    }

    fn account_token_meta(account: &solana_messages::Account) -> (Pubkey, u8) {
        let Some(token) = account.token.as_ref() else {
            return (Pubkey::default(), 0);
        };
        let mint = decode_pubkey(&token.mint).unwrap_or_default();
        if mint == Pubkey::default() {
            return (Pubkey::default(), 0);
        }
        let decimals = token.decimals.min(u32::from(u8::MAX)) as u8;
        (mint, decimals)
    }

    fn extract_trade_accounts(
        tx: Option<&solana_corecast::TransactionAttributes>,
    ) -> Vec<TradeAccountMeta> {
        let Some(header) = tx.and_then(|tx| tx.header.as_ref()) else {
            return Vec::new();
        };
        header
            .accounts
            .iter()
            .filter_map(|account| {
                let address = decode_pubkey(&account.address)?;
                let (mint, owner) = account
                    .token
                    .as_ref()
                    .map(|token| (decode_pubkey(&token.mint), decode_pubkey(&token.owner)))
                    .unwrap_or((None, None));
                Some(TradeAccountMeta {
                    address,
                    mint,
                    owner,
                    is_signer: account.is_signer,
                    is_writable: account.is_writable,
                })
            })
            .collect()
    }

    fn block_timestamp(block: &solana_corecast::BlockAttributes) -> Option<i64> {
        #[cfg(feature = "corecast_block_time")]
        {
            return Some(block.block_time);
        }

        #[cfg(not(feature = "corecast_block_time"))]
        {
            let _ = block;
            None
        }
    }

    fn decode_signature(bytes: &[u8]) -> Option<Signature> {
        Signature::try_from(bytes).ok()
    }

    fn decode_pubkey(bytes: &[u8]) -> Option<Pubkey> {
        if bytes.len() == 32 {
            Pubkey::try_from(bytes).ok()
        } else {
            None
        }
    }

    fn infer_curve_kind(dex: &solana_messages::DexInfo, program: &Pubkey) -> Option<CurveKind> {
        if program == &METEORA_DLMM_PROGRAM_ID {
            return Some(CurveKind::MeteoraDlmm);
        }
        if program == &METEORA_DAMM_PROGRAM_ID {
            return Some(CurveKind::MeteoraDamm);
        }
        let family = dex.protocol_family.to_ascii_lowercase();
        let name = dex.protocol_name.to_ascii_lowercase();

        if family.contains("raydium") {
            Some(CurveKind::RaydiumClmm)
        } else if family.contains("orca") {
            Some(CurveKind::OrcaWhirlpool)
        } else if family.contains("meteora") {
            if name.contains("dlmm") {
                Some(CurveKind::MeteoraDlmm)
            } else {
                Some(CurveKind::MeteoraDamm)
            }
        } else if name.contains("whirlpool") {
            Some(CurveKind::OrcaWhirlpool)
        } else {
            tracing::trace!(
                target: POOLS_NAME,
                ?program,
                family = %dex.protocol_family,
                name = %dex.protocol_name,
                "unknown curve family"
            );
            None
        }
    }

    fn currency_meta(currency: &solana_messages::Currency) -> (Pubkey, u8) {
        let mint = decode_pubkey(&currency.mint_address).unwrap_or_default();
        let decimals = clamp_decimals(currency.decimals);
        (mint, decimals)
    }

    fn clamp_decimals(value: u32) -> u8 {
        value.min(u8::MAX as u32) as u8
    }

    fn push_with_drop<T, F>(
        queue: &Arc<ArrayQueue<T>>,
        mut value: T,
        queue_name: &'static str,
        metrics: &Metrics,
        mut on_drop: F,
    ) where
        T: std::fmt::Debug,
        F: FnMut(&T),
    {
        loop {
            match queue.push(value) {
                Ok(()) => break,
                Err(returned) => {
                    metrics.inc_queue_drop(queue_name);
                    metrics.observe_queue_depth(queue_name, queue.len());
                    metrics.observe_queue_saturation(queue_name, queue.len(), queue.capacity());
                    on_drop(&returned);
                    value = returned;
                    if queue.pop().is_some() {
                        tracing::debug!(
                            target: NAME,
                            queue = queue_name,
                            "queue saturated; dropped oldest update"
                        );
                    } else {
                        tracing::warn!(
                            target: NAME,
                            queue = queue_name,
                            "queue saturated; unable to free slot"
                        );
                        break;
                    }
                }
            }
        }
    }

    fn unix_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|dur| dur.as_secs() as i64)
            .unwrap_or_default()
    }

    struct Backoff {
        current: Duration,
        min: Duration,
        max: Duration,
    }

    impl Backoff {
        fn new(min: Duration, max: Duration) -> Self {
            Self {
                current: min,
                min,
                max,
            }
        }

        fn reset(&mut self) {
            self.current = self.min;
        }

        fn next_delay(&mut self) -> Duration {
            let delay = self.current;
            self.current = (self.current * 2).min(self.max);
            delay
        }

        fn next_delay_with_jitter(&mut self) -> (Duration, Duration) {
            let base = self.next_delay();
            let cap_ms = backoff_jitter_cap_ms(base);
            if cap_ms == 0 {
                return (base, Duration::from_millis(0));
            }
            let mut rng = thread_rng();
            let offset_ms = rng.gen_range(0..=cap_ms);
            if offset_ms == 0 {
                return (base, Duration::from_millis(0));
            }
            let jitter = Duration::from_millis(offset_ms);
            let jittered = if rng.gen_bool(0.5) {
                base.checked_add(jitter).unwrap_or(base)
            } else {
                base.checked_sub(jitter).unwrap_or_default()
            };
            (jittered, jitter)
        }
    }

    fn backoff_jitter_cap_ms(delay: Duration) -> u64 {
        if delay.is_zero() {
            return 0;
        }
        let base_ms = delay.as_millis().min(u128::from(u64::MAX)) as u64;
        if base_ms == 0 {
            return 0;
        }
        let scaled = (base_ms as f64 * BACKOFF_JITTER_PCT).round() as u64;
        let cap = scaled.clamp(BACKOFF_JITTER_MIN_MS, BACKOFF_JITTER_MAX_MS);
        cap.min(base_ms)
    }

    #[derive(Clone, Copy)]
    struct SequenceEntry {
        slot: u64,
        seq: u64,
        stamp: u64,
    }

    /// Drops stale CoreCast updates per pool/trade and bounds memory via an eviction queue.
    struct SequenceFilter {
        entries: HashMap<Pubkey, SequenceEntry>,
        order: VecDeque<(Pubkey, u64)>,
        capacity: usize,
        next_stamp: u64,
        evictions: u64,
    }

    impl SequenceFilter {
        fn new(capacity: usize) -> Self {
            Self {
                entries: HashMap::new(),
                order: VecDeque::new(),
                capacity: capacity.max(1),
                next_stamp: 0,
                evictions: 0,
            }
        }

        fn reset(&mut self, capacity: usize) {
            self.capacity = capacity.max(1);
            self.clear();
        }

        fn clear(&mut self) {
            self.entries.clear();
            self.order.clear();
            self.next_stamp = 0;
            self.evictions = 0;
        }

        fn accept(&mut self, pool: Pubkey, slot: u64, seq: u64) -> bool {
            if let Some(prev) = self.entries.get(&pool) {
                if slot < prev.slot {
                    return false;
                }
                if slot == prev.slot && seq <= prev.seq {
                    return false;
                }
            }

            let stamp = self.next_stamp;
            self.next_stamp = self.next_stamp.wrapping_add(1);
            self.entries
                .insert(pool, SequenceEntry { slot, seq, stamp });
            self.order.push_back((pool, stamp));
            self.trim();
            true
        }

        fn take_evictions(&mut self) -> u64 {
            let evicted = self.evictions;
            self.evictions = 0;
            evicted
        }

        fn trim(&mut self) {
            while self.entries.len() > self.capacity {
                match self.order.pop_front() {
                    Some((pool, stamp)) => {
                        if let Some(entry) = self.entries.get(&pool) {
                            if entry.stamp == stamp {
                                self.entries.remove(&pool);
                                self.evictions = self.evictions.saturating_add(1);
                            }
                        }
                    }
                    None => break,
                }
            }
        }

        fn capacity(&self) -> usize {
            self.capacity
        }

        fn len(&self) -> usize {
            self.entries.len()
        }
    }

    fn record_sequence_evictions(
        stream_label: &'static str,
        log_target: &'static str,
        evicted: u64,
        warn_threshold: u64,
        filter: &SequenceFilter,
        metrics: &Metrics,
    ) {
        if evicted == 0 {
            return;
        }

        metrics.inc_ingest_sequence_evictions(stream_label, evicted);
        if warn_threshold > 0 && evicted >= warn_threshold {
            tracing::warn!(
                target: NAME,
                stream = stream_label,
                worker = log_target,
                evicted,
                warn_threshold,
                capacity = filter.capacity(),
                occupied = filter.len(),
                "sequence filter evicted entries; consider raising corecast.cache.sequence_capacity"
            );
        }
    }

    /// Aggregates ingest counters for periodic health heartbeats.
    struct StreamHealth {
        stream: &'static str,
        log_target: &'static str,
        last_report: Instant,
        interval: Duration,
        messages: u64,
        errors: u64,
        stale: u64,
        restarts: u64,
    }

    impl StreamHealth {
        fn new(stream: &'static str, log_target: &'static str) -> Self {
            Self {
                stream,
                log_target,
                last_report: Instant::now(),
                interval: Duration::from_secs(HEALTH_SNAPSHOT_SECS),
                messages: 0,
                errors: 0,
                stale: 0,
                restarts: 0,
            }
        }

        fn record_message(&mut self) {
            self.messages = self.messages.saturating_add(1);
        }

        fn record_error(&mut self) {
            self.errors = self.errors.saturating_add(1);
        }

        fn record_stale(&mut self) {
            self.stale = self.stale.saturating_add(1);
        }

        fn record_restart(&mut self) {
            self.restarts = self.restarts.saturating_add(1);
        }

        fn try_log(&mut self, queue_depth: usize, seq_filter: &SequenceFilter) {
            if self.last_report.elapsed() < self.interval {
                return;
            }
            tracing::info!(
                target: NAME,
                stream = self.stream,
                worker = self.log_target,
                queue_depth,
                seq_capacity = seq_filter.capacity(),
                seq_occupancy = seq_filter.len(),
                messages = self.messages,
                errors = self.errors,
                stale = self.stale,
                restarts = self.restarts,
                "ingest health snapshot"
            );
            self.messages = 0;
            self.errors = 0;
            self.stale = 0;
            self.restarts = 0;
            self.last_report = Instant::now();
        }
    }
}

mod planner {
    use super::{wait_for_stop, watch_config, watch_wallets, StopReason};
    use crate::arbgraph::{ArbGraph, Opportunity, OpportunityCounters, PlannerTuning};
    use crate::config::{Config, PlannerConfig, TokenPair};
    use crate::flashloan::FlashLoanBook;
    use crate::meteora;
    use crate::metrics::Metrics;
    use crate::orca;
    use crate::pipeline::{
        ArbPlan, CurveKind, CurveStateUpdate, HopAccounts, PipelineHandles, PlanHop, PoolUpdate,
        TradeUpdate,
    };
    use crate::pool_seeds::{PoolSeed, PoolSeedOverlay, PoolSeedRegistry};
    use crate::raydium;
    use crate::wallet::LoadedWallets;
    use anyhow::Result;
    use crossbeam_queue::ArrayQueue;
    use serde::{Deserialize, Serialize};
    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_sdk::instruction::{AccountMeta, Instruction};
    use solana_sdk::pubkey::Pubkey;
    use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
    use std::fs;
    use std::io::ErrorKind;
    use std::path::Path;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::{broadcast, watch};
    use tokio::task::yield_now;
    use tokio::time::{self, Duration, Instant, MissedTickBehavior};

    const NAME: &str = "worker::planner";
    const MAX_OPPORTUNITIES_PER_TICK: usize = 16;
    const RAYDIUM_FETCH_BACKOFF_SECS: u64 = 30;
    const ORCA_FETCH_BACKOFF_SECS: u64 = 30;
    const METEORA_FETCH_BACKOFF_SECS: u64 = 30;
    const FLASH_BORROW_INDEX: u8 = 2;
    const DEFAULT_RAYDIUM_SUPPLEMENTAL_ARRAYS: usize = 2;
    const DEFAULT_ORCA_SUPPLEMENTAL_ARRAYS: usize = 2;
    const DEFAULT_SUPPLEMENTAL_CACHE_TTL_SECS: u64 = 45;
    const RAYDIUM_CACHE_LABEL: &str = "raydium";
    const ORCA_CACHE_LABEL: &str = "orca";
    const METEORA_DAMM_LABEL: &str = "meteora_damm";
    const METEORA_DLMM_LABEL: &str = "meteora_dlmm";
    const CACHE_STATUS_HIT: &str = "hit";
    const CACHE_STATUS_MISS: &str = "miss";
    const CACHE_STATUS_STORE: &str = "store";
    const HYDRATION_SOURCE_TEMPLATE: &str = "template";
    const HYDRATION_SOURCE_CACHE: &str = "cache";
    const HYDRATION_SOURCE_PLANNER: &str = "planner_fallback";
    const VALIDATION_STATUS_PASS: &str = "pass";
    const VALIDATION_STATUS_STALE: &str = "stale";
    const VALIDATION_STATUS_MISSING: &str = "missing";
    const REFRESH_REASON_PREFETCH: &str = "prefetch";
    const REFRESH_REASON_SKIP: &str = "skip";
    const REFRESH_REASON_UNSUPPORTED: &str = "unsupported";
    const PLAN_DROP_REASON_EMPTY_HOPS: &str = "empty_hops";
    const PLAN_DROP_REASON_BORROW_MINT: &str = "borrow_mint_missing";
    const PLAN_DROP_REASON_FLASH_MARKET: &str = "flash_market_missing";
    const PLAN_DROP_REASON_FLASH_BLOCKED: &str = "flash_program_blocked";
    const PLAN_DROP_REASON_WALLET: &str = "wallet_missing";
    const PLAN_DROP_REASON_ATA: &str = "ata_missing";
    const PLAN_DROP_REASON_SWAP_BUILD: &str = "swap_build_failed";
    const CACHE_SNAPSHOT_PATH: &str = "planner_cache.json";
    const DEFAULT_SUPPLEMENTAL_PREFETCH_PER_TICK: usize = 2;
    const MEMO_PROGRAM_ID: Pubkey =
        solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

    #[derive(Clone, Copy, Debug)]
    struct SupplementalCacheTuning {
        raydium_max_arrays: usize,
        orca_max_arrays: usize,
        prefetch_per_tick: usize,
        ttl: Duration,
    }

    impl SupplementalCacheTuning {
        fn from_config(cfg: &PlannerConfig) -> Self {
            let raydium_max_arrays = cfg
                .raydium_max_supplemental_arrays
                .unwrap_or(DEFAULT_RAYDIUM_SUPPLEMENTAL_ARRAYS)
                .max(1);
            let orca_max_arrays = cfg
                .orca_max_supplemental_arrays
                .unwrap_or(DEFAULT_ORCA_SUPPLEMENTAL_ARRAYS)
                .max(1);
            let prefetch_per_tick = cfg
                .supplemental_prefetch_per_tick
                .unwrap_or(DEFAULT_SUPPLEMENTAL_PREFETCH_PER_TICK)
                .max(1);
            let ttl_secs = cfg
                .supplemental_cache_ttl_secs
                .unwrap_or(DEFAULT_SUPPLEMENTAL_CACHE_TTL_SECS)
                .max(1);
            Self {
                raydium_max_arrays,
                orca_max_arrays,
                prefetch_per_tick,
                ttl: Duration::from_secs(ttl_secs),
            }
        }
    }

    fn curve_metric_label(curve: CurveKind) -> &'static str {
        match curve {
            CurveKind::RaydiumClmm => RAYDIUM_CACHE_LABEL,
            CurveKind::OrcaWhirlpool => ORCA_CACHE_LABEL,
            CurveKind::MeteoraDamm => METEORA_DAMM_LABEL,
            CurveKind::MeteoraDlmm => METEORA_DLMM_LABEL,
        }
    }

    pub async fn run(
        mut cfg_rx: watch::Receiver<Arc<Config>>,
        mut wallet_rx: watch::Receiver<Arc<LoadedWallets>>,
        mut stop_rx: broadcast::Receiver<StopReason>,
        pipeline: PipelineHandles,
        metrics: Metrics,
        graph: Arc<RwLock<ArbGraph>>,
        pool_seeds: Arc<PoolSeedRegistry>,
        inferred_seeds: Arc<Mutex<PoolSeedOverlay>>,
    ) -> Result<()> {
        tracing::info!(
            target: NAME,
            plan_queue_capacity = pipeline.arb_plans.capacity(),
            pool_queue_capacity = pipeline.pool_updates.capacity(),
            "planner worker online"
        );

        let pool_queue = pipeline.pool_updates.clone();
        let trade_queue = pipeline.trade_updates.clone();
        let plan_queue = pipeline.arb_plans.clone();
        let state_queue = pipeline.curve_states.clone();

        let mut cfg_snapshot = cfg_rx.borrow().clone();
        let mut state = PlannerState::new(
            &cfg_snapshot,
            metrics.clone(),
            graph.clone(),
            pool_seeds.clone(),
            inferred_seeds.clone(),
        );
        let wallet_snapshot = wallet_rx.borrow().clone();
        state.set_wallet_authority(wallet_snapshot.payer_pubkey());
        let mut local_metrics = PlannerMetrics::default();
        let mut last_report = Instant::now();
        let mut tick = time::interval(Duration::from_millis(5));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                stop = wait_for_stop(&mut stop_rx) => {
                    if let Some(reason) = stop {
                        tracing::info!(target: NAME, ?reason, "planner worker exiting");
                    }
                    break;
                }
                cfg = watch_config(&mut cfg_rx, NAME) => {
                    match cfg {
                        Some(cfg) => {
                            tracing::debug!(
                                target: NAME,
                                token_count = cfg.tokens.len(),
                                "planner config updated"
                            );
                            state.reconfigure(&cfg);
                            cfg_snapshot = cfg;
                        }
                        None => {
                            tracing::info!(target: NAME, "config channel closed; stopping");
                            break;
                        }
                    }
                }
                wallets = watch_wallets(&mut wallet_rx) => {
                    match wallets {
                        Some(wallets) => {
                            tracing::info!(
                                target: NAME,
                                payer = %wallets.payer_pubkey(),
                                "wallet update applied"
                            );
                            state.set_wallet_authority(wallets.payer_pubkey());
                        }
                        None => {
                            tracing::warn!(
                                target: NAME,
                                "wallet channel closed; stopping planner"
                            );
                            break;
                        }
                    }
                }
                _ = tick.tick() => {
                    let stats = state
                        .pump(
                            &cfg_snapshot,
                            &state_queue,
                            &pool_queue,
                            &trade_queue,
                            &plan_queue,
                        )
                        .await;
                    local_metrics.record(&stats);
                    metrics.observe_queue_depth("curve_states", state_queue.len());
                    metrics.observe_queue_depth("pool_updates", pool_queue.len());
                    metrics.observe_queue_depth("trade_updates", trade_queue.len());
                    metrics.observe_queue_depth("arb_plans", plan_queue.len());
                    metrics.inc_planner_plans(stats.plans);
                    metrics.inc_planner_opportunities(&stats.opportunities);

                    if !stats.did_work() {
                        yield_now().await;
                    }

                    if last_report.elapsed() >= Duration::from_secs(5) {
                        tracing::info!(
                            target: NAME,
                            pools_cached = state.pool_count(),
                            queue_depth = plan_queue.len(),
                            pool_updates = local_metrics.pool_updates,
                            trade_updates = local_metrics.trade_updates,
                            plans = local_metrics.plans,
                            opp_emitted = local_metrics.opportunities_emitted,
                            opp_duplicates = local_metrics.opportunities_duplicates,
                            opp_replaced = local_metrics.opportunities_replaced,
                            opp_drained = local_metrics.opportunities_drained,
                            opp_evicted = local_metrics.opportunities_evicted,
                            "planner heartbeat"
                        );
                        local_metrics.reset();
                        last_report = Instant::now();
                    }
                }
            }
        }

        Ok(())
    }

    struct PlannerState {
        graph: Arc<RwLock<ArbGraph>>,
        next_plan_id: u64,
        rpc: Option<Arc<RpcClient>>,
        flash_loans: FlashLoanBook,
        wallet_authority: Option<Pubkey>,
        ata_by_mint: HashMap<Pubkey, Pubkey>,
        raydium_backoff: HashMap<Pubkey, Instant>,
        orca_backoff: HashMap<Pubkey, Instant>,
        meteora_damm_backoff: HashMap<Pubkey, Instant>,
        meteora_dlmm_backoff: HashMap<Pubkey, Instant>,
        raydium_extras: HashMap<RaydiumCacheKey, RaydiumExtrasCache>,
        orca_extras: HashMap<OrcaCacheKey, OrcaExtrasCache>,
        raydium_prefetch: VecDeque<RaydiumPrefetchTask>,
        orca_prefetch: VecDeque<OrcaPrefetchTask>,
        pending_raydium_prefetch: HashSet<RaydiumCacheKey>,
        pending_orca_prefetch: HashSet<OrcaCacheKey>,
        metrics: Metrics,
        cache_tuning: SupplementalCacheTuning,
        pool_seeds: Arc<PoolSeedRegistry>,
        seed_overlay: Arc<Mutex<PoolSeedOverlay>>,
        allowed_pairs: HashSet<(Pubkey, Pubkey)>,
        seed_inference: SeedInferenceTracker,
    }

    #[derive(Debug)]
    enum SwapBuildError {
        HopValidationFailed {
            pool: Pubkey,
        },
        MissingAta {
            pool: Pubkey,
            mint: Pubkey,
            role: &'static str,
        },
        MissingAccounts {
            pool: Pubkey,
            curve: CurveKind,
        },
        HydrationUnavailable {
            pool: Pubkey,
            curve: CurveKind,
            detail: &'static str,
        },
        InstructionConstruction {
            pool: Pubkey,
            curve: CurveKind,
            detail: &'static str,
        },
    }

    impl std::fmt::Display for SwapBuildError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                SwapBuildError::HopValidationFailed { pool } => {
                    write!(f, "hop validation failed for pool {pool}")
                }
                SwapBuildError::MissingAta { pool, mint, role } => {
                    write!(f, "missing {role} ATA for mint {mint} (pool {pool})")
                }
                SwapBuildError::MissingAccounts { pool, curve } => {
                    write!(f, "missing {:?} account bundle for pool {pool}", curve)
                }
                SwapBuildError::HydrationUnavailable {
                    pool,
                    curve,
                    detail,
                } => {
                    write!(
                        f,
                        "hydration failed for {:?} pool {} ({detail})",
                        curve, pool
                    )
                }
                SwapBuildError::InstructionConstruction {
                    pool,
                    curve,
                    detail,
                } => {
                    write!(
                        f,
                        "instruction build failed for {:?} pool {} ({detail})",
                        curve, pool
                    )
                }
            }
        }
    }

    impl PlannerState {
        fn new(
            cfg: &Arc<Config>,
            metrics: Metrics,
            graph: Arc<RwLock<ArbGraph>>,
            pool_seeds: Arc<PoolSeedRegistry>,
            seed_overlay: Arc<Mutex<PoolSeedOverlay>>,
        ) -> Self {
            let allowed_pairs = build_planner_pair_allowlist(&cfg.token_pairs);
            let seed_inference = SeedInferenceTracker::new(
                metrics.clone(),
                pool_seeds.clone(),
                seed_overlay.clone(),
            );
            let mut state = Self {
                graph,
                next_plan_id: 1,
                rpc: build_rpc_client(cfg),
                flash_loans: FlashLoanBook::from_config(&cfg.solend_flash_loans),
                wallet_authority: None,
                ata_by_mint: build_ata_map(cfg),
                raydium_backoff: HashMap::new(),
                orca_backoff: HashMap::new(),
                meteora_damm_backoff: HashMap::new(),
                meteora_dlmm_backoff: HashMap::new(),
                raydium_extras: HashMap::new(),
                orca_extras: HashMap::new(),
                raydium_prefetch: VecDeque::new(),
                orca_prefetch: VecDeque::new(),
                pending_raydium_prefetch: HashSet::new(),
                pending_orca_prefetch: HashSet::new(),
                metrics,
                cache_tuning: SupplementalCacheTuning::from_config(&cfg.planner),
                pool_seeds,
                seed_overlay,
                allowed_pairs,
                seed_inference,
            };
            state.apply_known_seeds("bootstrap");
            state.load_cache_snapshot();
            state
        }

        fn reconfigure(&mut self, cfg: &Arc<Config>) {
            let tuning = PlannerTuning::from_config(&cfg.token_pairs, &cfg.planner);
            let seeds = self.collect_known_seeds();
            let seeded = self.with_graph_mut(|graph| {
                graph.reconfigure(&cfg.token_pairs, tuning);
                graph.apply_pool_seeds(&seeds)
            });
            self.log_seed_application("config_reload", seeded);
            self.next_plan_id = self.next_plan_id.max(1);
            self.rpc = build_rpc_client(cfg);
            self.flash_loans = FlashLoanBook::from_config(&cfg.solend_flash_loans);
            self.ata_by_mint = build_ata_map(cfg);
            self.raydium_backoff.clear();
            self.orca_backoff.clear();
            self.meteora_damm_backoff.clear();
            self.meteora_dlmm_backoff.clear();
            self.cache_tuning = SupplementalCacheTuning::from_config(&cfg.planner);
            self.allowed_pairs = build_planner_pair_allowlist(&cfg.token_pairs);
            self.seed_inference.reset();
            tracing::info!(
                target: NAME,
                raydium_arrays = self.cache_tuning.raydium_max_arrays,
                orca_arrays = self.cache_tuning.orca_max_arrays,
                prefetch_per_tick = self.cache_tuning.prefetch_per_tick,
                cache_ttl_secs = self.cache_tuning.ttl.as_secs(),
                "planner supplemental cache tuning updated"
            );
        }

        fn collect_known_seeds(&self) -> Vec<PoolSeed> {
            let mut seeds = self.pool_seeds.seeds().to_vec();
            match self.seed_overlay.lock() {
                Ok(overlay) => seeds.extend(overlay.seeds()),
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        "inferred pool seed overlay mutex poisoned; recovering"
                    );
                    seeds.extend(err.into_inner().seeds());
                }
            }
            seeds
        }

        fn apply_known_seeds(&mut self, reason: &str) {
            let seeds = self.collect_known_seeds();
            let seeded = self.with_graph_mut(|graph| graph.apply_pool_seeds(&seeds));
            self.log_seed_application(reason, seeded);
        }

        fn log_seed_application(&self, reason: &str, count: usize) {
            if count > 0 {
                tracing::info!(
                    target: NAME,
                    seeded = count,
                    reason,
                    "applied pool seed snapshot to planner graph"
                );
            }
        }

        fn apply_inferred_seed(&mut self, seed: PoolSeed) {
            let applied =
                self.with_graph_mut(|graph| graph.apply_pool_seeds(std::slice::from_ref(&seed)));
            if applied > 0 {
                tracing::info!(
                    target: NAME,
                    pool = %seed.pool,
                    curve = ?seed.curve,
                    base_mint = %seed.base_mint,
                    quote_mint = %seed.quote_mint,
                    "planner inferred pool identity"
                );
            }
        }

        fn set_wallet_authority(&mut self, authority: Pubkey) {
            self.wallet_authority = Some(authority);
        }

        fn ata_for_mint(&self, mint: &Pubkey) -> Option<Pubkey> {
            self.ata_by_mint.get(mint).copied()
        }

        fn validate_hop(&mut self, hop: &PlanHop) -> bool {
            let curve_label = curve_metric_label(hop.curve);
            match self.with_graph(|graph| graph.reprice_hop(hop)) {
                Some(requote) => {
                    if requote.min_amount_out >= hop.min_amount_out {
                        self.metrics
                            .inc_planner_hop_validation(curve_label, VALIDATION_STATUS_PASS);
                        true
                    } else {
                        self.metrics
                            .inc_planner_hop_validation(curve_label, VALIDATION_STATUS_STALE);
                        self.request_hop_refresh(hop);
                        tracing::debug!(
                            target: NAME,
                            hop_pool = %hop.pool,
                            hop_curve = ?hop.curve,
                            hop_amount_in = hop.amount_in,
                            hop_min_out = hop.min_amount_out,
                            requote_min_out = requote.min_amount_out,
                            "hop validation failed: slippage floor degraded"
                        );
                        false
                    }
                }
                None => {
                    self.metrics
                        .inc_planner_hop_validation(curve_label, VALIDATION_STATUS_MISSING);
                    self.request_hop_refresh(hop);
                    tracing::debug!(
                        target: NAME,
                        hop_pool = %hop.pool,
                        hop_curve = ?hop.curve,
                        "hop validation aborted: insufficient local data"
                    );
                    false
                }
            }
        }

        fn request_hop_refresh(&mut self, hop: &PlanHop) {
            match (&hop.accounts, hop.curve) {
                (HopAccounts::RaydiumClmm(accounts), CurveKind::RaydiumClmm) => {
                    if let Some(direction) =
                        raydium::swap_direction(&accounts.params, hop.input_mint, hop.output_mint)
                    {
                        self.enqueue_raydium_prefetch(hop.pool, accounts.params, direction);
                        self.metrics
                            .inc_planner_hop_refresh(RAYDIUM_CACHE_LABEL, REFRESH_REASON_PREFETCH);
                    } else {
                        self.metrics
                            .inc_planner_hop_refresh(RAYDIUM_CACHE_LABEL, REFRESH_REASON_SKIP);
                    }
                }
                (HopAccounts::OrcaWhirlpool(accounts), CurveKind::OrcaWhirlpool) => {
                    if let Some(direction) =
                        orca::swap_direction(&accounts.params, hop.input_mint, hop.output_mint)
                    {
                        self.enqueue_orca_prefetch(hop.pool, accounts.params, direction);
                        self.metrics
                            .inc_planner_hop_refresh(ORCA_CACHE_LABEL, REFRESH_REASON_PREFETCH);
                    } else {
                        self.metrics
                            .inc_planner_hop_refresh(ORCA_CACHE_LABEL, REFRESH_REASON_SKIP);
                    }
                }
                _ => {
                    self.metrics.inc_planner_hop_refresh(
                        curve_metric_label(hop.curve),
                        REFRESH_REASON_UNSUPPORTED,
                    );
                }
            }
        }

        fn build_swap_instructions(
            &mut self,
            hops: &[PlanHop],
            authority: Pubkey,
        ) -> Result<Vec<Instruction>, SwapBuildError> {
            let mut instructions = Vec::with_capacity(hops.len());

            for hop in hops {
                if !self.validate_hop(hop) {
                    return Err(SwapBuildError::HopValidationFailed { pool: hop.pool });
                }
                let input_ata = self.ata_for_mint(&hop.input_mint).ok_or_else(|| {
                    SwapBuildError::MissingAta {
                        pool: hop.pool,
                        mint: hop.input_mint,
                        role: "input",
                    }
                })?;
                let output_ata = self.ata_for_mint(&hop.output_mint).ok_or_else(|| {
                    SwapBuildError::MissingAta {
                        pool: hop.pool,
                        mint: hop.output_mint,
                        role: "output",
                    }
                })?;

                let instruction = match hop.curve {
                    CurveKind::RaydiumClmm => {
                        let HopAccounts::RaydiumClmm(accounts) = &hop.accounts else {
                            return Err(SwapBuildError::MissingAccounts {
                                pool: hop.pool,
                                curve: hop.curve,
                            });
                        };
                        let hydrated = self
                            .hydrate_raydium_accounts(
                                hop.pool,
                                hop.input_mint,
                                hop.output_mint,
                                accounts,
                            )
                            .map_err(|detail| SwapBuildError::HydrationUnavailable {
                                pool: hop.pool,
                                curve: hop.curve,
                                detail,
                            })?;
                        raydium::build_swap_v2_instruction(
                            &hydrated,
                            hop.pool,
                            hop.input_mint,
                            hop.output_mint,
                            input_ata,
                            output_ata,
                            authority,
                            hop.amount_in,
                            hop.min_amount_out,
                        )
                        .ok_or_else(|| {
                            SwapBuildError::InstructionConstruction {
                                pool: hop.pool,
                                curve: hop.curve,
                                detail: "raydium::build_swap_v2_instruction returned None",
                            }
                        })?
                    }
                    CurveKind::OrcaWhirlpool => {
                        let HopAccounts::OrcaWhirlpool(accounts) = &hop.accounts else {
                            return Err(SwapBuildError::MissingAccounts {
                                pool: hop.pool,
                                curve: hop.curve,
                            });
                        };
                        let hydrated = self
                            .hydrate_orca_accounts(
                                hop.pool,
                                hop.input_mint,
                                hop.output_mint,
                                accounts,
                            )
                            .map_err(|detail| SwapBuildError::HydrationUnavailable {
                                pool: hop.pool,
                                curve: hop.curve,
                                detail,
                            })?;
                        orca::build_swap_v2_instruction(
                            &hydrated,
                            hop.pool,
                            hop.input_mint,
                            hop.output_mint,
                            input_ata,
                            output_ata,
                            authority,
                            hop.amount_in,
                            hop.min_amount_out,
                        )
                        .ok_or_else(|| {
                            SwapBuildError::InstructionConstruction {
                                pool: hop.pool,
                                curve: hop.curve,
                                detail: "orca::build_swap_v2_instruction returned None",
                            }
                        })?
                    }
                    CurveKind::MeteoraDamm => {
                        let HopAccounts::MeteoraDamm(params) = &hop.accounts else {
                            return Err(SwapBuildError::MissingAccounts {
                                pool: hop.pool,
                                curve: hop.curve,
                            });
                        };
                        meteora::build_damm_swap2_instruction(
                            params,
                            hop.pool,
                            hop.input_mint,
                            hop.output_mint,
                            input_ata,
                            output_ata,
                            authority,
                            hop.amount_in,
                            hop.min_amount_out,
                            None,
                        )
                        .ok_or_else(|| {
                            SwapBuildError::InstructionConstruction {
                                pool: hop.pool,
                                curve: hop.curve,
                                detail: "meteora::build_damm_swap2_instruction returned None",
                            }
                        })?
                    }
                    CurveKind::MeteoraDlmm => {
                        let HopAccounts::MeteoraDlmm(params) = &hop.accounts else {
                            return Err(SwapBuildError::MissingAccounts {
                                pool: hop.pool,
                                curve: hop.curve,
                            });
                        };
                        meteora::build_dlmm_swap2_instruction(
                            params,
                            hop.pool,
                            hop.input_mint,
                            hop.output_mint,
                            input_ata,
                            output_ata,
                            authority,
                            hop.amount_in,
                            hop.min_amount_out,
                            None,
                        )
                        .ok_or_else(|| {
                            SwapBuildError::InstructionConstruction {
                                pool: hop.pool,
                                curve: hop.curve,
                                detail: "meteora::build_dlmm_swap2_instruction returned None",
                            }
                        })?
                    }
                };

                instructions.push(instruction);
            }

            Ok(instructions)
        }

        fn build_post_flash_batches(
            &self,
            cfg: &Arc<Config>,
            plan_id: u64,
            expected_profit: i64,
        ) -> Vec<Vec<Instruction>> {
            if !cfg.planner.emit_plan_memos {
                return Vec::new();
            }
            vec![vec![Self::plan_memo_instruction(plan_id, expected_profit)]]
        }

        fn plan_memo_instruction(plan_id: u64, expected_profit: i64) -> Instruction {
            let profit = expected_profit.max(0);
            let memo = format!("tre-plan:{plan_id}|profit:{profit}");
            Instruction {
                program_id: MEMO_PROGRAM_ID,
                accounts: Vec::new(),
                data: memo.into_bytes(),
            }
        }

        fn hydrate_raydium_accounts(
            &mut self,
            pool: Pubkey,
            input_mint: Pubkey,
            output_mint: Pubkey,
            template: &raydium::RaydiumSwapAccounts,
        ) -> Result<raydium::RaydiumSwapAccounts, &'static str> {
            let Some(direction) =
                raydium::swap_direction(&template.params, input_mint, output_mint)
            else {
                tracing::warn!(
                    target: NAME,
                    pool = %pool,
                    curve = "RaydiumClmm",
                    input_mint = %input_mint,
                    output_mint = %output_mint,
                    token_mint_0 = %template.params.token_mint_0,
                    token_mint_1 = %template.params.token_mint_1,
                    "raydium swap direction unavailable"
                );
                return Err("raydium_swap_direction_unavailable");
            };

            tracing::debug!(
                target: NAME,
                pool = %pool,
                curve = "RaydiumClmm",
                input_mint = %input_mint,
                output_mint = %output_mint,
                token_mint_0 = %template.params.token_mint_0,
                token_mint_1 = %template.params.token_mint_1,
                token_vault_0 = %template.params.token_vault_0,
                token_vault_1 = %template.params.token_vault_1,
                input_matches_token0 = input_mint == template.params.token_mint_0,
                input_matches_token1 = input_mint == template.params.token_mint_1,
                output_matches_token0 = output_mint == template.params.token_mint_0,
                output_matches_token1 = output_mint == template.params.token_mint_1,
                zero_for_one = direction,
                "raydium direction resolved"
            );

            let key = RaydiumCacheKey {
                pool,
                zero_for_one: direction,
            };

            if !template.supplemental_tick_arrays.is_empty() {
                self.cache_raydium_entry(
                    key,
                    &template.supplemental_tick_arrays,
                    template.include_bitmap_extension,
                );
                self.metrics.inc_executor_supplemental_hydration(
                    RAYDIUM_CACHE_LABEL,
                    HYDRATION_SOURCE_TEMPLATE,
                );
                return Ok(template.clone());
            }

            if let Some(entry) = self.raydium_extras.get(&key).and_then(|entry| {
                if entry.is_fresh(self.cache_tuning.ttl) {
                    Some(entry.clone())
                } else {
                    None
                }
            }) {
                self.record_cache_event(RAYDIUM_CACHE_LABEL, CACHE_STATUS_HIT);
                let hydrated = apply_raydium_cached_accounts(template, &entry);
                self.metrics.inc_executor_supplemental_hydration(
                    RAYDIUM_CACHE_LABEL,
                    HYDRATION_SOURCE_CACHE,
                );
                return Ok(hydrated);
            }

            self.raydium_extras.remove(&key);
            self.record_cache_event(RAYDIUM_CACHE_LABEL, CACHE_STATUS_MISS);
            self.enqueue_raydium_prefetch(pool, template.params, direction);
            let fallback_accounts = self.with_graph(|graph| {
                graph.raydium_tick_account_addresses(
                    &pool,
                    direction,
                    self.cache_tuning.raydium_max_arrays,
                )
            });
            let mut hydrated = template.clone();
            hydrated.supplemental_tick_arrays = fallback_accounts;
            if self.with_graph(|graph| graph.raydium_bitmap_present(&pool)) {
                hydrated.include_bitmap_extension = true;
            }
            self.metrics
                .inc_executor_supplemental_hydration(RAYDIUM_CACHE_LABEL, HYDRATION_SOURCE_PLANNER);
            Ok(hydrated)
        }

        fn hydrate_orca_accounts(
            &mut self,
            pool: Pubkey,
            input_mint: Pubkey,
            output_mint: Pubkey,
            template: &orca::OrcaSwapAccounts,
        ) -> Result<orca::OrcaSwapAccounts, &'static str> {
            let Some(direction) = orca::swap_direction(&template.params, input_mint, output_mint)
            else {
                tracing::warn!(
                    target: NAME,
                    pool = %pool,
                    curve = "OrcaWhirlpool",
                    input_mint = %input_mint,
                    output_mint = %output_mint,
                    token_a = %template.params.token_mint_a,
                    token_b = %template.params.token_mint_b,
                    "orca swap direction unavailable"
                );
                return Err("orca_swap_direction_unavailable");
            };

            if !template.extras.remaining_accounts.is_empty()
                || !template.extras.remaining_account_slices.is_empty()
            {
                let hydrated = template.clone();
                let cached = extract_orca_supplementals(&hydrated);
                if !cached.is_empty() {
                    self.cache_orca_entry(
                        OrcaCacheKey {
                            pool,
                            a_to_b: direction,
                        },
                        &cached,
                    );
                }
                self.metrics.inc_executor_supplemental_hydration(
                    ORCA_CACHE_LABEL,
                    HYDRATION_SOURCE_TEMPLATE,
                );
                return Ok(hydrated);
            }

            let key = OrcaCacheKey {
                pool,
                a_to_b: direction,
            };

            if let Some(entry) = self.orca_extras.get(&key).and_then(|entry| {
                if entry.is_fresh(self.cache_tuning.ttl) {
                    Some(entry.clone())
                } else {
                    None
                }
            }) {
                self.record_cache_event(ORCA_CACHE_LABEL, CACHE_STATUS_HIT);
                let mut hydrated = template.clone();
                if entry.tick_arrays.is_empty() {
                    self.metrics.inc_executor_supplemental_hydration(
                        ORCA_CACHE_LABEL,
                        HYDRATION_SOURCE_CACHE,
                    );
                    return Ok(hydrated);
                }
                if let Ok(count) = u8::try_from(entry.tick_arrays.len()) {
                    apply_orca_cached_accounts(&entry.tick_arrays, count, &mut hydrated);
                    self.metrics.inc_executor_supplemental_hydration(
                        ORCA_CACHE_LABEL,
                        HYDRATION_SOURCE_CACHE,
                    );
                    return Ok(hydrated);
                }
                self.metrics
                    .inc_executor_supplemental_hydration(ORCA_CACHE_LABEL, HYDRATION_SOURCE_CACHE);
                return Ok(hydrated);
            }

            self.orca_extras.remove(&key);
            self.record_cache_event(ORCA_CACHE_LABEL, CACHE_STATUS_MISS);
            self.enqueue_orca_prefetch(pool, template.params, direction);
            let fallback_accounts = self.with_graph(|graph| {
                graph.orca_tick_account_addresses(
                    &pool,
                    direction,
                    self.cache_tuning.orca_max_arrays,
                )
            });
            let mut hydrated = template.clone();
            let limited_len = fallback_accounts.len().min(u8::MAX as usize);
            self.metrics
                .inc_executor_supplemental_hydration(ORCA_CACHE_LABEL, HYDRATION_SOURCE_PLANNER);
            if limited_len == 0 {
                return Ok(hydrated);
            }
            let count = limited_len as u8;
            apply_orca_cached_accounts(&fallback_accounts[..limited_len], count, &mut hydrated);
            Ok(hydrated)
        }

        fn enqueue_raydium_prefetch(
            &mut self,
            pool: Pubkey,
            params: raydium::RaydiumPoolParams,
            zero_for_one: bool,
        ) {
            let key = RaydiumCacheKey { pool, zero_for_one };
            if self.pending_raydium_prefetch.insert(key) {
                self.raydium_prefetch
                    .push_back(RaydiumPrefetchTask { key, params });
            }
        }

        fn enqueue_orca_prefetch(
            &mut self,
            pool: Pubkey,
            params: orca::OrcaPoolParams,
            a_to_b: bool,
        ) {
            let key = OrcaCacheKey { pool, a_to_b };
            if self.pending_orca_prefetch.insert(key) {
                self.orca_prefetch
                    .push_back(OrcaPrefetchTask { key, params });
            }
        }

        fn invalidate_supplemental_cache(&mut self, pool: Pubkey, curve: CurveKind) {
            let mut removed = false;
            match curve {
                CurveKind::RaydiumClmm => {
                    let before = self.raydium_extras.len();
                    self.raydium_extras.retain(|key, _| key.pool != pool);
                    removed |= before != self.raydium_extras.len();
                    self.pending_raydium_prefetch.retain(|key| key.pool != pool);
                    if !self.raydium_prefetch.is_empty() {
                        self.raydium_prefetch = self
                            .raydium_prefetch
                            .drain(..)
                            .filter(|task| task.key.pool != pool)
                            .collect();
                    }
                    self.with_graph_mut(|graph| graph.clear_raydium_tick_arrays(pool));
                }
                CurveKind::OrcaWhirlpool => {
                    let before = self.orca_extras.len();
                    self.orca_extras.retain(|key, _| key.pool != pool);
                    removed |= before != self.orca_extras.len();
                    self.pending_orca_prefetch.retain(|key| key.pool != pool);
                    if !self.orca_prefetch.is_empty() {
                        self.orca_prefetch = self
                            .orca_prefetch
                            .drain(..)
                            .filter(|task| task.key.pool != pool)
                            .collect();
                    }
                    self.with_graph_mut(|graph| graph.clear_orca_tick_arrays(pool));
                }
                _ => {}
            }
            if removed {
                self.persist_cache_snapshot();
            }
        }

        fn cache_raydium_entry(&mut self, key: RaydiumCacheKey, arrays: &[Pubkey], bitmap: bool) {
            self.raydium_extras.insert(
                key,
                RaydiumExtrasCache {
                    tick_arrays: arrays.to_vec(),
                    bitmap_present: bitmap,
                    cached_at: Instant::now(),
                },
            );
            self.record_cache_event(RAYDIUM_CACHE_LABEL, CACHE_STATUS_STORE);
            self.persist_cache_snapshot();
        }

        fn cache_orca_entry(&mut self, key: OrcaCacheKey, arrays: &[Pubkey]) {
            self.orca_extras.insert(
                key,
                OrcaExtrasCache {
                    tick_arrays: arrays.to_vec(),
                    cached_at: Instant::now(),
                },
            );
            self.record_cache_event(ORCA_CACHE_LABEL, CACHE_STATUS_STORE);
            self.persist_cache_snapshot();
        }

        fn record_cache_event(&self, curve: &'static str, status: &'static str) {
            self.metrics.inc_planner_supplemental_cache(curve, status);
        }

        fn load_cache_snapshot(&mut self) {
            let path = Path::new(CACHE_SNAPSHOT_PATH);
            let data = match fs::read_to_string(path) {
                Ok(data) => data,
                Err(err) => {
                    if err.kind() != ErrorKind::NotFound {
                        tracing::warn!(target: NAME, path = ?path, ?err, "failed to read planner cache snapshot");
                    }
                    return;
                }
            };

            let snapshot: PlannerCacheSnapshot = match serde_json::from_str(&data) {
                Ok(snap) => snap,
                Err(err) => {
                    tracing::warn!(target: NAME, path = ?path, ?err, "failed to parse planner cache snapshot");
                    return;
                }
            };

            let mut raydium_restored = 0usize;
            for entry in snapshot.raydium {
                if let Some((key, cache)) = restore_raydium_cache(entry) {
                    if cache.is_fresh(self.cache_tuning.ttl) {
                        self.raydium_extras.insert(key, cache);
                        raydium_restored += 1;
                    }
                }
            }

            let mut orca_restored = 0usize;
            for entry in snapshot.orca {
                if let Some((key, cache)) = restore_orca_cache(entry) {
                    if cache.is_fresh(self.cache_tuning.ttl) {
                        self.orca_extras.insert(key, cache);
                        orca_restored += 1;
                    }
                }
            }

            if raydium_restored > 0 || orca_restored > 0 {
                tracing::info!(
                    target: NAME,
                    raydium_restored,
                    orca_restored,
                    "planner supplemental cache restored"
                );
            }
        }

        fn persist_cache_snapshot(&self) {
            let snapshot = PlannerCacheSnapshot {
                raydium: self
                    .raydium_extras
                    .iter()
                    .filter_map(|(key, cache)| cache.to_snapshot_entry(key))
                    .collect(),
                orca: self
                    .orca_extras
                    .iter()
                    .filter_map(|(key, cache)| cache.to_snapshot_entry(key))
                    .collect(),
            };

            let path = Path::new(CACHE_SNAPSHOT_PATH);
            match serde_json::to_string_pretty(&snapshot) {
                Ok(serialized) => {
                    if let Err(err) = fs::write(path, serialized) {
                        tracing::warn!(
                            target: NAME,
                            path = ?path,
                            ?err,
                            "failed to write planner cache snapshot"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        path = ?path,
                        ?err,
                        "failed to serialize planner cache snapshot"
                    );
                }
            }
        }
    }

    impl PlannerState {
        async fn pump(
            &mut self,
            cfg: &Arc<Config>,
            state_queue: &Arc<ArrayQueue<CurveStateUpdate>>,
            pool_queue: &Arc<ArrayQueue<PoolUpdate>>,
            trade_queue: &Arc<ArrayQueue<TradeUpdate>>,
            plan_queue: &Arc<ArrayQueue<ArbPlan>>,
        ) -> PumpStats {
            let mut stats = PumpStats::default();

            while let Some(state) = state_queue.pop() {
                self.apply_curve_state(state);
            }

            while let Some(update) = pool_queue.pop() {
                stats.pool_updates += 1;
                let pool = update.pool;
                let curve = update.curve;
                self.with_graph_mut(|graph| graph.apply_pool_update(update));
                self.ensure_curve_state(pool, Some(curve)).await;
            }

            while let Some(trade) = trade_queue.pop() {
                stats.trade_updates += 1;
                let pool = trade.pool;
                let curve = self.with_graph(|graph| graph.curve_kind(&pool));
                if let Some(seed) =
                    self.seed_inference
                        .observe_trade(&trade, curve, &self.allowed_pairs)
                {
                    self.apply_inferred_seed(seed);
                }
                self.with_graph_mut(|graph| graph.handle_trade(trade));
                let curve_label = curve.map(curve_metric_label).unwrap_or("unknown");
                self.metrics.inc_planner_trade_ingest(curve_label);
                self.ensure_curve_state(pool, curve).await;
            }

            self.process_prefetch_queues().await;

            let opportunities =
                self.with_graph_mut(|graph| graph.drain_opportunities(MAX_OPPORTUNITIES_PER_TICK));
            for opportunity in opportunities {
                if let Some(plan) = self.build_plan(cfg, opportunity).await {
                    push_plan(plan_queue, plan);
                    stats.plans += 1;
                }
            }

            stats.opportunities = self.with_graph_mut(|graph| graph.take_opportunity_stats());
            let trade_drops = self.with_graph_mut(|graph| graph.take_trade_drop_stats());
            self.metrics.inc_planner_trade_drops(&trade_drops);

            stats
        }

        fn apply_curve_state(&mut self, update: CurveStateUpdate) {
            match update {
                CurveStateUpdate::RaydiumClmm { pool, params } => {
                    self.with_graph_mut(|graph| graph.set_raydium_state(pool, params));
                    self.invalidate_supplemental_cache(pool, CurveKind::RaydiumClmm);
                    self.raydium_backoff.remove(&pool);
                }
                CurveStateUpdate::OrcaWhirlpool { pool, params } => {
                    self.with_graph_mut(|graph| graph.set_orca_state(pool, params));
                    self.invalidate_supplemental_cache(pool, CurveKind::OrcaWhirlpool);
                    self.orca_backoff.remove(&pool);
                }
                CurveStateUpdate::MeteoraDamm { pool, params } => {
                    self.with_graph_mut(|graph| graph.set_meteora_damm_state(pool, params));
                    self.meteora_damm_backoff.remove(&pool);
                }
                CurveStateUpdate::MeteoraDlmm { pool, params } => {
                    self.with_graph_mut(|graph| graph.set_meteora_dlmm_state(pool, params));
                    self.meteora_dlmm_backoff.remove(&pool);
                }
            }
        }

        async fn build_plan(
            &mut self,
            cfg: &Arc<Config>,
            opportunity: Opportunity,
        ) -> Option<ArbPlan> {
            if opportunity.hops.is_empty() {
                self.metrics
                    .inc_planner_plan_drop(PLAN_DROP_REASON_EMPTY_HOPS);
                return None;
            }
            let borrow_mint = opportunity
                .hops
                .first()
                .map(|hop| hop.input_mint)
                .or_else(|| cfg.tokens.values().next().copied())
                .unwrap_or_default();
            if borrow_mint == Pubkey::default() {
                self.metrics
                    .inc_planner_plan_drop(PLAN_DROP_REASON_BORROW_MINT);
                return None;
            }
            let flash_spec = match self.flash_loans.get(&borrow_mint) {
                Some(spec) => {
                    self.metrics.inc_planner_flash_lookup("hit");
                    let refs = Arc::strong_count(&spec);
                    self.metrics.observe_planner_flash_refcount(refs);
                    spec
                }
                None => {
                    self.metrics.inc_planner_flash_lookup("miss");
                    tracing::trace!(
                        target: NAME,
                        plan_pool = %opportunity.pool,
                        borrow_mint = %borrow_mint,
                        "no solend market configured for borrow mint"
                    );
                    self.metrics
                        .inc_planner_plan_drop(PLAN_DROP_REASON_FLASH_MARKET);
                    return None;
                }
            };
            if let Err(blocked_program) =
                flash_spec.ensure_programs_allowed(opportunity.hops.iter().map(|hop| hop.program))
            {
                let bad_hop = opportunity
                    .hops
                    .iter()
                    .find(|hop| hop.program == blocked_program);
                let blocked_pool = bad_hop.map(|hop| hop.pool).unwrap_or_default();
                tracing::trace!(
                    target: NAME,
                    plan_pool = %opportunity.pool,
                    borrow_mint = %borrow_mint,
                    flash_market = flash_spec.label(),
                    hop_pool = %blocked_pool,
                    hop_program = %blocked_program,
                    "hop program not allowed for selected flash-loan market"
                );
                self.metrics
                    .inc_planner_plan_drop(PLAN_DROP_REASON_FLASH_BLOCKED);
                return None;
            }
            let authority = match self.wallet_authority {
                Some(pk) => pk,
                None => {
                    tracing::debug!(
                        target: NAME,
                        plan_pool = %opportunity.pool,
                        "wallet authority not yet loaded; dropping plan"
                    );
                    self.metrics.inc_planner_plan_drop(PLAN_DROP_REASON_WALLET);
                    return None;
                }
            };
            let user_ata = match self.ata_for_mint(&borrow_mint) {
                Some(ata) => ata,
                None => {
                    tracing::debug!(
                        target: NAME,
                        plan_pool = %opportunity.pool,
                        borrow_mint = %borrow_mint,
                        "no ATA configured for borrow mint; dropping plan"
                    );
                    self.metrics.inc_planner_plan_drop(PLAN_DROP_REASON_ATA);
                    return None;
                }
            };
            let notional = opportunity.notional.max(1);
            let expected_profit = opportunity.expected_profit_lamports;
            let plan_id = self.next_plan_id();
            let hop_count = opportunity.hops.len().min(u8::MAX as usize) as u8;
            let swap_instructions = match self.build_swap_instructions(&opportunity.hops, authority)
            {
                Ok(ix) => ix,
                Err(err) => {
                    tracing::trace!(
                        target: NAME,
                        plan_pool = %opportunity.pool,
                        error = %err,
                        "failed to build swap instructions; dropping plan"
                    );
                    self.metrics
                        .inc_planner_plan_drop(PLAN_DROP_REASON_SWAP_BUILD);
                    return None;
                }
            };
            let flash_instructions = flash_spec.build_instructions(
                notional,
                user_ata,
                user_ata,
                authority,
                FLASH_BORROW_INDEX,
            );
            let post_flash_batches = self.build_post_flash_batches(cfg, plan_id, expected_profit);
            let plan = ArbPlan {
                id: plan_id,
                borrow_mint,
                notional,
                expected_profit_lamports: expected_profit,
                hop_count,
                hops: opportunity.hops,
                enqueue_instant: std::time::Instant::now(),
                flash_loan: flash_spec,
                flash_instructions,
                swap_instructions,
                post_flash_batches,
                transactions: Vec::new(),
            };

            tracing::debug!(
                target: NAME,
                plan_id = plan.id,
                pool = %opportunity.pool,
                curve = ?opportunity.curve,
                notional = plan.notional,
                expected_profit = plan.expected_profit_lamports,
                "arb plan enqueued"
            );

            Some(plan)
        }

        fn next_plan_id(&mut self) -> u64 {
            if self.next_plan_id == 0 {
                self.next_plan_id = 1;
            }
            let id = self.next_plan_id;
            self.next_plan_id = self.next_plan_id.wrapping_add(1);
            if self.next_plan_id == 0 {
                self.next_plan_id = 1;
            }
            id
        }

        fn pool_count(&self) -> usize {
            self.with_graph(|graph| graph.pool_count())
        }

        async fn ensure_curve_state(&mut self, pool: Pubkey, curve: Option<CurveKind>) {
            match curve {
                Some(CurveKind::RaydiumClmm) => self.ensure_raydium_state(pool).await,
                Some(CurveKind::OrcaWhirlpool) => self.ensure_orca_state(pool).await,
                Some(CurveKind::MeteoraDamm) => self.ensure_meteora_damm_state(pool).await,
                Some(CurveKind::MeteoraDlmm) => self.ensure_meteora_dlmm_state(pool).await,
                _ => {}
            }
        }

        async fn ensure_raydium_state(&mut self, pool: Pubkey) {
            if !matches!(
                self.with_graph(|graph| graph.curve_kind(&pool)),
                Some(CurveKind::RaydiumClmm)
            ) {
                return;
            }

            if self.with_graph(|graph| graph.has_raydium_state(&pool)) {
                return;
            }

            if !self.should_attempt_raydium_fetch(pool) {
                return;
            }

            let Some(rpc) = self.rpc.as_ref() else {
                return;
            };

            match rpc.get_account_data(&pool).await {
                Ok(data) => match raydium::RaydiumPoolParams::decode(&data) {
                    Some(params) => {
                        self.with_graph_mut(|graph| graph.set_raydium_state(pool, params));
                        self.invalidate_supplemental_cache(pool, CurveKind::RaydiumClmm);
                        self.raydium_backoff.remove(&pool);
                        tracing::debug!(target: NAME, pool = %pool, "raydium pool snapshot cached");
                    }
                    None => {
                        tracing::warn!(
                            target: NAME,
                            pool = %pool,
                            "failed to decode raydium pool account"
                        );
                        self.schedule_raydium_retry(pool);
                    }
                },
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        pool = %pool,
                        ?err,
                        "rpc error fetching raydium pool account"
                    );
                    self.schedule_raydium_retry(pool);
                }
            }
        }

        fn should_attempt_raydium_fetch(&mut self, pool: Pubkey) -> bool {
            match self.raydium_backoff.get(&pool) {
                Some(next_allowed) if *next_allowed > Instant::now() => false,
                _ => true,
            }
        }

        fn schedule_raydium_retry(&mut self, pool: Pubkey) {
            let deadline = Instant::now() + Duration::from_secs(RAYDIUM_FETCH_BACKOFF_SECS);
            self.raydium_backoff.insert(pool, deadline);
        }

        async fn ensure_orca_state(&mut self, pool: Pubkey) {
            if !matches!(
                self.with_graph(|graph| graph.curve_kind(&pool)),
                Some(CurveKind::OrcaWhirlpool)
            ) {
                return;
            }

            if self.with_graph(|graph| graph.has_orca_state(&pool)) {
                return;
            }

            if !self.should_attempt_orca_fetch(pool) {
                return;
            }

            let Some(rpc) = self.rpc.as_ref() else {
                return;
            };

            match rpc.get_account_data(&pool).await {
                Ok(data) => match orca::OrcaPoolParams::decode(&data) {
                    Some(params) => {
                        self.with_graph_mut(|graph| graph.set_orca_state(pool, params));
                        self.invalidate_supplemental_cache(pool, CurveKind::OrcaWhirlpool);
                        self.orca_backoff.remove(&pool);
                        tracing::debug!(target: NAME, pool = %pool, "orca pool snapshot cached");
                    }
                    None => {
                        tracing::warn!(
                            target: NAME,
                            pool = %pool,
                            "failed to decode orca whirlpool account"
                        );
                        self.schedule_orca_retry(pool);
                    }
                },
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        pool = %pool,
                        ?err,
                        "rpc error fetching orca whirlpool account"
                    );
                    self.schedule_orca_retry(pool);
                }
            }
        }

        fn should_attempt_orca_fetch(&mut self, pool: Pubkey) -> bool {
            match self.orca_backoff.get(&pool) {
                Some(next_allowed) if *next_allowed > Instant::now() => false,
                _ => true,
            }
        }

        fn schedule_orca_retry(&mut self, pool: Pubkey) {
            let deadline = Instant::now() + Duration::from_secs(ORCA_FETCH_BACKOFF_SECS);
            self.orca_backoff.insert(pool, deadline);
        }

        async fn ensure_meteora_damm_state(&mut self, pool: Pubkey) {
            if !matches!(
                self.with_graph(|graph| graph.curve_kind(&pool)),
                Some(CurveKind::MeteoraDamm)
            ) {
                return;
            }

            if self.with_graph(|graph| graph.has_meteora_damm_state(&pool)) {
                return;
            }

            if !self.should_attempt_meteora_damm_fetch(pool) {
                return;
            }

            let Some(rpc) = self.rpc.as_ref() else {
                return;
            };

            match rpc.get_account_data(&pool).await {
                Ok(data) => match meteora::MeteoraDammParams::decode(&data) {
                    Some(params) => {
                        self.with_graph_mut(|graph| graph.set_meteora_damm_state(pool, params));
                        self.meteora_damm_backoff.remove(&pool);
                        tracing::debug!(target: NAME, pool = %pool, "meteora damm pool snapshot cached");
                    }
                    None => {
                        tracing::warn!(
                            target: NAME,
                            pool = %pool,
                            "failed to decode meteora damm pool account"
                        );
                        self.schedule_meteora_damm_retry(pool);
                    }
                },
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        pool = %pool,
                        ?err,
                        "rpc error fetching meteora damm pool account"
                    );
                    self.schedule_meteora_damm_retry(pool);
                }
            }
        }

        fn should_attempt_meteora_damm_fetch(&mut self, pool: Pubkey) -> bool {
            match self.meteora_damm_backoff.get(&pool) {
                Some(next_allowed) if *next_allowed > Instant::now() => false,
                _ => true,
            }
        }

        fn schedule_meteora_damm_retry(&mut self, pool: Pubkey) {
            let deadline = Instant::now() + Duration::from_secs(METEORA_FETCH_BACKOFF_SECS);
            self.meteora_damm_backoff.insert(pool, deadline);
        }

        async fn ensure_meteora_dlmm_state(&mut self, pool: Pubkey) {
            if !matches!(
                self.with_graph(|graph| graph.curve_kind(&pool)),
                Some(CurveKind::MeteoraDlmm)
            ) {
                return;
            }

            if self.with_graph(|graph| graph.has_meteora_dlmm_state(&pool)) {
                return;
            }

            if !self.should_attempt_meteora_dlmm_fetch(pool) {
                return;
            }

            let Some(rpc) = self.rpc.as_ref() else {
                return;
            };

            match rpc.get_account_data(&pool).await {
                Ok(data) => match meteora::MeteoraDlmmParams::decode(&data) {
                    Some(params) => {
                        self.with_graph_mut(|graph| graph.set_meteora_dlmm_state(pool, params));
                        self.meteora_dlmm_backoff.remove(&pool);
                        tracing::debug!(target: NAME, pool = %pool, "meteora dlmm pool snapshot cached");
                    }
                    None => {
                        tracing::warn!(
                            target: NAME,
                            pool = %pool,
                            "failed to decode meteora dlmm pool account"
                        );
                        self.schedule_meteora_dlmm_retry(pool);
                    }
                },
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        pool = %pool,
                        ?err,
                        "rpc error fetching meteora dlmm pool account"
                    );
                    self.schedule_meteora_dlmm_retry(pool);
                }
            }
        }

        fn should_attempt_meteora_dlmm_fetch(&mut self, pool: Pubkey) -> bool {
            match self.meteora_dlmm_backoff.get(&pool) {
                Some(next_allowed) if *next_allowed > Instant::now() => false,
                _ => true,
            }
        }

        fn schedule_meteora_dlmm_retry(&mut self, pool: Pubkey) {
            let deadline = Instant::now() + Duration::from_secs(METEORA_FETCH_BACKOFF_SECS);
            self.meteora_dlmm_backoff.insert(pool, deadline);
        }

        async fn process_prefetch_queues(&mut self) {
            self.process_raydium_prefetch_queue().await;
            self.process_orca_prefetch_queue().await;
            self.metrics
                .observe_planner_prefetch_backlog("raydium", self.raydium_prefetch.len());
            self.metrics
                .observe_planner_prefetch_backlog("orca", self.orca_prefetch.len());
        }

        async fn process_raydium_prefetch_queue(&mut self) {
            let Some(rpc) = self.rpc.as_ref().cloned() else {
                return;
            };
            let rpc = rpc.as_ref();
            let mut processed = 0usize;
            while processed < self.cache_tuning.prefetch_per_tick {
                let Some(task) = self.raydium_prefetch.pop_front() else {
                    break;
                };
                processed += 1;
                self.pending_raydium_prefetch.remove(&task.key);

                let candidates = raydium::derive_supplemental_tick_arrays(
                    task.key.pool,
                    &task.params,
                    task.key.zero_for_one,
                    self.cache_tuning.raydium_max_arrays,
                );
                if candidates.is_empty() {
                    self.cache_raydium_entry(task.key, &[], false);
                    continue;
                }

                let fetched = fetch_account_buffers(rpc, &candidates).await;
                if fetched.is_empty() {
                    self.cache_raydium_entry(task.key, &[], false);
                    continue;
                }

                let bitmap_present = bitmap_extension_exists(rpc, task.key.pool).await;
                let account_keys: Vec<Pubkey> = fetched.iter().map(|entry| entry.pubkey).collect();
                self.cache_raydium_entry(task.key, &account_keys, bitmap_present);

                let mut decoded = Vec::new();
                for entry in &fetched {
                    match raydium::decode_tick_array(&entry.data) {
                        Some(array) if array.pool_id == task.key.pool => decoded.push(array),
                        Some(_) | None => continue,
                    }
                }
                if !decoded.is_empty() {
                    self.with_graph_mut(|graph| {
                        graph.set_raydium_tick_arrays(task.key.pool, decoded, bitmap_present)
                    });
                }
            }
        }

        async fn process_orca_prefetch_queue(&mut self) {
            let Some(rpc) = self.rpc.as_ref().cloned() else {
                return;
            };
            let rpc = rpc.as_ref();
            let mut processed = 0usize;
            while processed < self.cache_tuning.prefetch_per_tick {
                let Some(task) = self.orca_prefetch.pop_front() else {
                    break;
                };
                processed += 1;
                self.pending_orca_prefetch.remove(&task.key);

                let candidates = orca::derive_supplemental_tick_arrays(
                    task.key.pool,
                    &task.params,
                    task.key.a_to_b,
                    self.cache_tuning.orca_max_arrays,
                );
                if candidates.is_empty() {
                    self.cache_orca_entry(task.key, &[]);
                    continue;
                }

                let fetched = fetch_account_buffers(rpc, &candidates).await;
                let existing: Vec<Pubkey> = fetched.iter().map(|entry| entry.pubkey).collect();
                self.cache_orca_entry(task.key, &existing);

                if fetched.is_empty() {
                    continue;
                }

                let mut decoded = Vec::new();
                for entry in &fetched {
                    match orca::decode_tick_array(&entry.data, task.params.tick_spacing) {
                        Some(array) if array.whirlpool == task.key.pool => decoded.push(array),
                        Some(_) | None => continue,
                    }
                }
                if !decoded.is_empty() {
                    self.with_graph_mut(|graph| graph.set_orca_tick_arrays(task.key.pool, decoded));
                }
            }
        }

        fn with_graph<R>(&self, f: impl FnOnce(&ArbGraph) -> R) -> R {
            let guard = self.graph.read().expect("planner graph poisoned");
            f(&guard)
        }

        fn with_graph_mut<R>(&self, f: impl FnOnce(&mut ArbGraph) -> R) -> R {
            let mut guard = self.graph.write().expect("planner graph poisoned");
            f(&mut guard)
        }
    }

    #[derive(Default)]
    struct PumpStats {
        pool_updates: u64,
        trade_updates: u64,
        plans: u64,
        opportunities: OpportunityCounters,
    }

    impl PumpStats {
        fn did_work(&self) -> bool {
            self.pool_updates > 0 || self.trade_updates > 0 || self.plans > 0
        }
    }

    #[derive(Default)]
    struct PlannerMetrics {
        pool_updates: u64,
        trade_updates: u64,
        plans: u64,
        opportunities_emitted: u64,
        opportunities_duplicates: u64,
        opportunities_replaced: u64,
        opportunities_drained: u64,
        opportunities_evicted: u64,
    }

    impl PlannerMetrics {
        fn record(&mut self, stats: &PumpStats) {
            self.pool_updates += stats.pool_updates;
            self.trade_updates += stats.trade_updates;
            self.plans += stats.plans;
            self.opportunities_emitted += stats.opportunities.emitted;
            self.opportunities_duplicates += stats.opportunities.duplicates;
            self.opportunities_replaced += stats.opportunities.replaced;
            self.opportunities_drained += stats.opportunities.drained;
            self.opportunities_evicted += stats.opportunities.evicted;
        }
        fn reset(&mut self) {
            self.pool_updates = 0;
            self.trade_updates = 0;
            self.plans = 0;
            self.opportunities_emitted = 0;
            self.opportunities_duplicates = 0;
            self.opportunities_replaced = 0;
            self.opportunities_drained = 0;
            self.opportunities_evicted = 0;
        }
    }

    fn build_planner_pair_allowlist(pairs: &[TokenPair]) -> HashSet<(Pubkey, Pubkey)> {
        let mut set = HashSet::new();
        for pair in pairs {
            set.insert((pair.base, pair.quote));
            set.insert((pair.quote, pair.base));
        }
        set
    }

    const MIN_INFERENCE_HITS: u32 = 3;
    const MAX_INFERENCE_HYPOTHESES: usize = 4096;
    const HYPOTHESIS_SLOT_TTL: u64 = 512;

    const STATUS_PENDING: &str = "pending";
    const STATUS_ACCEPTED: &str = "applied";
    const STATUS_TRUSTED_MATCH: &str = "trusted_match";
    const STATUS_TRUSTED_MISMATCH: &str = "trusted_mismatch";
    const STATUS_INSUFFICIENT: &str = "insufficient_accounts";
    const STATUS_AMBIGUOUS: &str = "ambiguous_accounts";
    const STATUS_PAIR_BLOCKED: &str = "pair_blocked";
    const STATUS_CURVE_UNKNOWN: &str = "curve_unknown";
    const STATUS_PROGRAM_UNKNOWN: &str = "program_unknown";

    #[derive(Clone)]
    struct SeedHypothesis {
        seed: PoolSeed,
        hits: u32,
        last_slot: u64,
        orientation_confident: bool,
    }

    impl SeedHypothesis {
        fn new(seed: PoolSeed, slot: u64, orientation_confident: bool) -> Self {
            Self {
                seed,
                hits: 1,
                last_slot: slot,
                orientation_confident,
            }
        }

        fn matches(&self, other: &PoolSeed) -> bool {
            mint_pair_matches(
                self.seed.base_mint,
                self.seed.quote_mint,
                other.base_mint,
                other.quote_mint,
            ) && self.seed.curve == other.curve
                && self.seed.program == other.program
        }

        fn observe(&mut self, slot: u64, orientation_confident: bool, candidate: &PoolSeed) {
            self.last_slot = slot;
            if orientation_confident {
                self.seed.base_mint = candidate.base_mint;
                self.seed.quote_mint = candidate.quote_mint;
                self.orientation_confident = true;
            }
            self.hits = self.hits.saturating_add(1);
        }
    }

    enum PairSelection {
        Selected {
            base: Pubkey,
            quote: Pubkey,
            oriented: bool,
        },
        Insufficient,
        Ambiguous,
        PairBlocked,
    }

    #[derive(Clone, Copy)]
    enum SeedMismatchField {
        Program,
        Curve,
        BaseMint,
        QuoteMint,
    }

    impl SeedMismatchField {
        fn label(self) -> &'static str {
            match self {
                SeedMismatchField::Program => "program",
                SeedMismatchField::Curve => "curve",
                SeedMismatchField::BaseMint => "base_mint",
                SeedMismatchField::QuoteMint => "quote_mint",
            }
        }
    }

    struct SeedInferenceTracker {
        hypotheses: HashMap<Pubkey, SeedHypothesis>,
        metrics: Metrics,
        trusted: Arc<PoolSeedRegistry>,
        overlay: Arc<Mutex<PoolSeedOverlay>>,
    }

    impl SeedInferenceTracker {
        fn new(
            metrics: Metrics,
            trusted: Arc<PoolSeedRegistry>,
            overlay: Arc<Mutex<PoolSeedOverlay>>,
        ) -> Self {
            Self {
                hypotheses: HashMap::new(),
                metrics,
                trusted,
                overlay,
            }
        }

        fn reset(&mut self) {
            self.hypotheses.clear();
        }

        fn observe_trade(
            &mut self,
            trade: &TradeUpdate,
            existing_curve: Option<CurveKind>,
            allowed_pairs: &HashSet<(Pubkey, Pubkey)>,
        ) -> Option<PoolSeed> {
            self.prune_stale(trade.slot);

            let curve = trade.curve_hint.or(existing_curve);
            let Some(curve) = curve else {
                self.metrics
                    .inc_planner_seed_inference(STATUS_CURVE_UNKNOWN);
                return None;
            };

            let program = trade.program;
            if program == Pubkey::default() {
                self.metrics
                    .inc_planner_seed_inference(STATUS_PROGRAM_UNKNOWN);
                return None;
            }

            let selection = self.select_pair(trade, allowed_pairs);
            let PairSelection::Selected {
                base,
                quote,
                oriented,
            } = selection
            else {
                match selection {
                    PairSelection::Insufficient => {
                        self.metrics.inc_planner_seed_inference(STATUS_INSUFFICIENT);
                    }
                    PairSelection::Ambiguous => {
                        self.metrics.inc_planner_seed_inference(STATUS_AMBIGUOUS);
                    }
                    PairSelection::PairBlocked => {
                        self.metrics.inc_planner_seed_inference(STATUS_PAIR_BLOCKED);
                    }
                    PairSelection::Selected { .. } => unreachable!(),
                }
                return None;
            };

            if base == quote {
                self.metrics.inc_planner_seed_inference(STATUS_INSUFFICIENT);
                return None;
            }

            let mut candidate = PoolSeed {
                pool: trade.pool,
                program,
                curve,
                base_mint: base,
                quote_mint: quote,
            };
            candidate.canonicalize_curve();
            self.promote_candidate(candidate, trade.slot, oriented)
        }

        fn select_pair(
            &self,
            trade: &TradeUpdate,
            allowed_pairs: &HashSet<(Pubkey, Pubkey)>,
        ) -> PairSelection {
            let base = trade.base_mint;
            let quote = trade.quote_mint;
            if base != Pubkey::default()
                && quote != Pubkey::default()
                && pair_in_allowlist(allowed_pairs, base, quote)
            {
                return PairSelection::Selected {
                    base,
                    quote,
                    oriented: true,
                };
            }

            let mut candidates = Vec::new();
            let mut insert_unique = |mint: Pubkey| {
                if mint != Pubkey::default() && !candidates.contains(&mint) {
                    candidates.push(mint);
                }
            };
            insert_unique(base);
            insert_unique(quote);
            for account in &trade.accounts {
                if let Some(mint) = account.mint {
                    insert_unique(mint);
                }
            }

            if candidates.len() < 2 {
                return PairSelection::Insufficient;
            }

            let mut matches = Vec::new();
            for i in 0..candidates.len() {
                for j in (i + 1)..candidates.len() {
                    let a = candidates[i];
                    let b = candidates[j];
                    if pair_in_allowlist(allowed_pairs, a, b) {
                        matches.push((a, b));
                    }
                }
            }

            match matches.len() {
                0 => PairSelection::PairBlocked,
                1 => PairSelection::Selected {
                    base: matches[0].0,
                    quote: matches[0].1,
                    oriented: false,
                },
                _ => PairSelection::Ambiguous,
            }
        }

        fn promote_candidate(
            &mut self,
            candidate: PoolSeed,
            slot: u64,
            oriented: bool,
        ) -> Option<PoolSeed> {
            match self.hypotheses.entry(candidate.pool) {
                Entry::Vacant(vacant) => {
                    vacant.insert(SeedHypothesis::new(candidate, slot, oriented));
                    self.metrics.inc_planner_seed_inference(STATUS_PENDING);
                    None
                }
                Entry::Occupied(mut occupied) => {
                    let entry = occupied.get_mut();
                    if !entry.matches(&candidate) {
                        *entry = SeedHypothesis::new(candidate, slot, oriented);
                        self.metrics.inc_planner_seed_inference(STATUS_PENDING);
                        return None;
                    }
                    entry.observe(slot, oriented, &candidate);
                    if entry.hits < MIN_INFERENCE_HITS {
                        self.metrics.inc_planner_seed_inference(STATUS_PENDING);
                        return None;
                    }
                    let hypothesis = occupied.remove();
                    self.handle_finalized_seed(hypothesis.seed)
                }
            }
        }

        fn handle_finalized_seed(&mut self, seed: PoolSeed) -> Option<PoolSeed> {
            if let Some(trusted) = self.trusted.get(&seed.pool) {
                let mismatches = collect_mismatches(trusted, &seed);
                if mismatches.is_empty() {
                    self.metrics
                        .inc_planner_seed_inference(STATUS_TRUSTED_MATCH);
                } else {
                    self.metrics
                        .inc_planner_seed_inference(STATUS_TRUSTED_MISMATCH);
                    for field in mismatches {
                        self.metrics.inc_planner_seed_mismatch(field.label());
                    }
                    tracing::warn!(
                        target: NAME,
                        pool = %seed.pool,
                        trusted_program = %trusted.program,
                        inferred_program = %seed.program,
                        trusted_curve = ?trusted.curve,
                        inferred_curve = ?seed.curve,
                        trusted_base = %trusted.base_mint,
                        trusted_quote = %trusted.quote_mint,
                        inferred_base = %seed.base_mint,
                        inferred_quote = %seed.quote_mint,
                        "pool seed mismatch detected"
                    );
                }
                return None;
            }

            let mut overlay = match self.overlay.lock() {
                Ok(guard) => guard,
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        pool = %seed.pool,
                        "inferred pool seed overlay mutex poisoned; recovering"
                    );
                    err.into_inner()
                }
            };

            if let Some(existing) = overlay.get(&seed.pool).cloned() {
                let mismatches = collect_mismatches(&existing, &seed);
                if mismatches.is_empty() {
                    self.metrics
                        .inc_planner_seed_inference(STATUS_TRUSTED_MATCH);
                } else {
                    self.metrics
                        .inc_planner_seed_inference(STATUS_TRUSTED_MISMATCH);
                    for field in mismatches {
                        self.metrics.inc_planner_seed_mismatch(field.label());
                    }
                    tracing::warn!(
                        target: NAME,
                        pool = %seed.pool,
                        trusted_program = %existing.program,
                        inferred_program = %seed.program,
                        trusted_curve = ?existing.curve,
                        inferred_curve = ?seed.curve,
                        trusted_base = %existing.base_mint,
                        trusted_quote = %existing.quote_mint,
                        inferred_base = %seed.base_mint,
                        inferred_quote = %seed.quote_mint,
                        source = "overlay",
                        "pool seed mismatch detected"
                    );
                }
                return None;
            }

            match overlay.upsert(seed.clone()) {
                Ok(true) => {
                    tracing::info!(
                        target: NAME,
                        pool = %seed.pool,
                        curve = ?seed.curve,
                        base_mint = %seed.base_mint,
                        quote_mint = %seed.quote_mint,
                        path = %overlay.path().display(),
                        "persisted inferred pool seed"
                    );
                }
                Ok(false) => {
                    self.metrics
                        .inc_planner_seed_inference(STATUS_TRUSTED_MATCH);
                    return None;
                }
                Err(err) => {
                    tracing::error!(
                        target: NAME,
                        pool = %seed.pool,
                        path = %overlay.path().display(),
                        ?err,
                        "failed to persist inferred pool seed"
                    );
                }
            }

            self.metrics.inc_planner_seed_inference(STATUS_ACCEPTED);
            Some(seed)
        }

        fn prune_stale(&mut self, slot: u64) {
            self.hypotheses
                .retain(|_, hypo| slot.saturating_sub(hypo.last_slot) <= HYPOTHESIS_SLOT_TTL);
            if self.hypotheses.len() <= MAX_INFERENCE_HYPOTHESES {
                return;
            }
            let mut entries: Vec<_> = self
                .hypotheses
                .iter()
                .map(|(pool, hypo)| (*pool, hypo.last_slot))
                .collect();
            entries.sort_by_key(|(_, last_slot)| *last_slot);
            let excess = self.hypotheses.len() - MAX_INFERENCE_HYPOTHESES;
            for (pool, _) in entries.into_iter().take(excess) {
                self.hypotheses.remove(&pool);
            }
        }
    }

    fn pair_in_allowlist(allowed: &HashSet<(Pubkey, Pubkey)>, a: Pubkey, b: Pubkey) -> bool {
        allowed.is_empty() || allowed.contains(&(a, b)) || allowed.contains(&(b, a))
    }

    fn mint_pair_matches(a_base: Pubkey, a_quote: Pubkey, b_base: Pubkey, b_quote: Pubkey) -> bool {
        (a_base == b_base && a_quote == b_quote) || (a_base == b_quote && a_quote == b_base)
    }

    fn collect_mismatches(trusted: &PoolSeed, inferred: &PoolSeed) -> Vec<SeedMismatchField> {
        let mut mismatches = Vec::new();
        if trusted.program != inferred.program {
            mismatches.push(SeedMismatchField::Program);
        }
        if trusted.curve != inferred.curve {
            mismatches.push(SeedMismatchField::Curve);
        }
        if !mint_pair_matches(
            trusted.base_mint,
            trusted.quote_mint,
            inferred.base_mint,
            inferred.quote_mint,
        ) {
            mismatches.push(SeedMismatchField::BaseMint);
            mismatches.push(SeedMismatchField::QuoteMint);
        }
        mismatches
    }

    struct AccountBuffer {
        pubkey: Pubkey,
        data: Vec<u8>,
    }

    async fn fetch_account_buffers(rpc: &RpcClient, candidates: &[Pubkey]) -> Vec<AccountBuffer> {
        if candidates.is_empty() {
            return Vec::new();
        }

        match rpc.get_multiple_accounts(candidates).await {
            Ok(accounts) => accounts
                .into_iter()
                .enumerate()
                .filter_map(|(idx, account)| {
                    account.map(|acct| AccountBuffer {
                        pubkey: candidates[idx],
                        data: acct.data,
                    })
                })
                .collect(),
            Err(err) => {
                tracing::debug!(
                    target: NAME,
                    ?err,
                    "failed to fetch supplemental accounts"
                );
                Vec::new()
            }
        }
    }

    async fn bitmap_extension_exists(rpc: &RpcClient, pool: Pubkey) -> bool {
        let bitmap = raydium::bitmap_extension_address(pool);
        rpc.get_account(&bitmap).await.is_ok()
    }

    #[derive(Serialize, Deserialize)]
    struct PlannerCacheSnapshot {
        raydium: Vec<RaydiumCacheSnapshotEntry>,
        orca: Vec<OrcaCacheSnapshotEntry>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct RaydiumCacheKey {
        pool: Pubkey,
        zero_for_one: bool,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct OrcaCacheKey {
        pool: Pubkey,
        a_to_b: bool,
    }

    #[derive(Clone, Copy)]
    struct RaydiumPrefetchTask {
        key: RaydiumCacheKey,
        params: raydium::RaydiumPoolParams,
    }

    #[derive(Clone, Copy)]
    struct OrcaPrefetchTask {
        key: OrcaCacheKey,
        params: orca::OrcaPoolParams,
    }

    #[derive(Serialize, Deserialize)]
    struct RaydiumCacheSnapshotEntry {
        pool: String,
        tick_arrays: Vec<String>,
        bitmap_present: bool,
        cached_at_unix: u64,
        #[serde(default)]
        zero_for_one: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct OrcaCacheSnapshotEntry {
        pool: String,
        tick_arrays: Vec<String>,
        cached_at_unix: u64,
        #[serde(default)]
        a_to_b: bool,
    }

    #[derive(Clone)]
    struct RaydiumExtrasCache {
        tick_arrays: Vec<Pubkey>,
        bitmap_present: bool,
        cached_at: Instant,
    }

    impl RaydiumExtrasCache {
        fn is_fresh(&self, ttl: Duration) -> bool {
            self.cached_at.elapsed() <= ttl
        }

        fn to_snapshot_entry(&self, key: &RaydiumCacheKey) -> Option<RaydiumCacheSnapshotEntry> {
            let cached_at = snapshot_unix_timestamp(self.cached_at)?;
            Some(RaydiumCacheSnapshotEntry {
                pool: key.pool.to_string(),
                tick_arrays: self.tick_arrays.iter().map(|pk| pk.to_string()).collect(),
                bitmap_present: self.bitmap_present,
                cached_at_unix: cached_at,
                zero_for_one: key.zero_for_one,
            })
        }
    }

    #[derive(Clone)]
    struct OrcaExtrasCache {
        tick_arrays: Vec<Pubkey>,
        cached_at: Instant,
    }

    impl OrcaExtrasCache {
        fn is_fresh(&self, ttl: Duration) -> bool {
            self.cached_at.elapsed() <= ttl
        }

        fn to_snapshot_entry(&self, key: &OrcaCacheKey) -> Option<OrcaCacheSnapshotEntry> {
            let cached_at = snapshot_unix_timestamp(self.cached_at)?;
            Some(OrcaCacheSnapshotEntry {
                pool: key.pool.to_string(),
                tick_arrays: self.tick_arrays.iter().map(|pk| pk.to_string()).collect(),
                cached_at_unix: cached_at,
                a_to_b: key.a_to_b,
            })
        }
    }

    fn restore_raydium_cache(
        entry: RaydiumCacheSnapshotEntry,
    ) -> Option<(RaydiumCacheKey, RaydiumExtrasCache)> {
        let pool = Pubkey::from_str(&entry.pool).ok()?;
        let tick_arrays: Option<Vec<Pubkey>> = entry
            .tick_arrays
            .into_iter()
            .map(|value| Pubkey::from_str(&value).ok())
            .collect();
        let tick_arrays = tick_arrays?;
        let cached_at = instant_from_unix(entry.cached_at_unix)?;
        Some((
            RaydiumCacheKey {
                pool,
                zero_for_one: entry.zero_for_one,
            },
            RaydiumExtrasCache {
                tick_arrays,
                bitmap_present: entry.bitmap_present,
                cached_at,
            },
        ))
    }

    fn restore_orca_cache(
        entry: OrcaCacheSnapshotEntry,
    ) -> Option<(OrcaCacheKey, OrcaExtrasCache)> {
        let pool = Pubkey::from_str(&entry.pool).ok()?;
        let tick_arrays: Option<Vec<Pubkey>> = entry
            .tick_arrays
            .into_iter()
            .map(|value| Pubkey::from_str(&value).ok())
            .collect();
        let tick_arrays = tick_arrays?;
        let cached_at = instant_from_unix(entry.cached_at_unix)?;
        Some((
            OrcaCacheKey {
                pool,
                a_to_b: entry.a_to_b,
            },
            OrcaExtrasCache {
                tick_arrays,
                cached_at,
            },
        ))
    }

    fn snapshot_unix_timestamp(cached_at: Instant) -> Option<u64> {
        let age = cached_at.elapsed();
        let system_time = SystemTime::now().checked_sub(age)?;
        system_time
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|dur| dur.as_secs())
    }

    fn instant_from_unix(timestamp: u64) -> Option<Instant> {
        let cached_time = UNIX_EPOCH.checked_add(Duration::from_secs(timestamp))?;
        let now_system = SystemTime::now();
        let age = now_system.duration_since(cached_time).ok()?;
        let now_instant = Instant::now();
        now_instant.checked_sub(age).or(Some(now_instant))
    }

    fn extract_orca_supplementals(accounts: &orca::OrcaSwapAccounts) -> Vec<Pubkey> {
        if accounts.extras.remaining_accounts.is_empty() {
            return Vec::new();
        }

        let mut cursor = 0usize;
        let mut collected = Vec::new();
        for slice in &accounts.extras.remaining_account_slices {
            let len = slice.length as usize;
            let end = cursor
                .saturating_add(len)
                .min(accounts.extras.remaining_accounts.len());
            if matches!(
                slice.accounts_type,
                orca::OrcaRemainingAccountsType::SupplementalTickArrays
                    | orca::OrcaRemainingAccountsType::SupplementalTickArraysOne
                    | orca::OrcaRemainingAccountsType::SupplementalTickArraysTwo
            ) {
                collected.extend(
                    accounts.extras.remaining_accounts[cursor..end]
                        .iter()
                        .map(|meta| meta.pubkey),
                );
            }
            cursor = end;
        }

        collected
    }

    fn apply_orca_cached_accounts(
        arrays: &[Pubkey],
        count: u8,
        accounts: &mut orca::OrcaSwapAccounts,
    ) {
        for pk in arrays {
            accounts
                .extras
                .remaining_accounts
                .push(AccountMeta::new(*pk, false));
        }
        accounts
            .extras
            .remaining_account_slices
            .push(orca::OrcaRemainingAccountsSlice {
                accounts_type: orca::OrcaRemainingAccountsType::SupplementalTickArrays,
                length: count,
            });
    }

    fn apply_raydium_cached_accounts(
        template: &raydium::RaydiumSwapAccounts,
        entry: &RaydiumExtrasCache,
    ) -> raydium::RaydiumSwapAccounts {
        let mut hydrated = template.clone();
        hydrated.supplemental_tick_arrays = entry.tick_arrays.clone();
        if entry.bitmap_present {
            hydrated.include_bitmap_extension = true;
        }
        hydrated
    }

    fn push_plan(queue: &Arc<ArrayQueue<ArbPlan>>, mut plan: ArbPlan) {
        loop {
            match queue.push(plan) {
                Ok(()) => break,
                Err(returned) => {
                    plan = returned;
                    if queue.pop().is_none() {
                        tracing::warn!(target: NAME, "plan queue saturated; dropping plan");
                        break;
                    }
                }
            }
        }
    }

    fn build_rpc_client(cfg: &Arc<Config>) -> Option<Arc<RpcClient>> {
        cfg.quicknode_rpcs
            .get(0)
            .map(|url| RpcClient::new(url.clone()))
            .map(Arc::new)
    }

    fn build_ata_map(cfg: &Arc<Config>) -> HashMap<Pubkey, Pubkey> {
        let mut map = HashMap::new();
        for (symbol, mint) in &cfg.tokens {
            if let Some(ata) = cfg.atas.get(symbol) {
                map.insert(*mint, *ata);
            }
        }
        map
    }
}

mod executor {
    use super::{
        wait_for_stop, watch_config, watch_wallets, BundleFeedback, BundleTracker, StopReason,
    };
    use crate::arbgraph::ArbGraph;
    use crate::config::{Config, ExecutorConfig, LookupTableConfig};
    use crate::flashloan::FlashLoanInstructions;
    use crate::jito::{JitoClient, JitoError};
    use crate::metrics::Metrics;
    use crate::pipeline::{ArbPlan, PipelineHandles};
    use crate::proto::{bundle, packet, shared};
    use crate::wallet::LoadedWallets;
    use anyhow::{anyhow, Result};
    use crossbeam_queue::ArrayQueue;
    use futures::{future::pending, SinkExt, StreamExt};
    use prost_types::Timestamp;
    use rand::{thread_rng, Rng};
    use reqwest::Client as HttpClient;
    use serde_json::Value;
    use solana_address_lookup_table_interface::state::AddressLookupTable as LookupTableState;
    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_client::rpc_config::RpcSimulateTransactionConfig;
    use solana_commitment_config::CommitmentConfig;
    use solana_compute_budget_interface::ComputeBudgetInstruction;
    use solana_packet::PACKET_DATA_SIZE;
    use solana_sdk::hash::Hash;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::AddressLookupTableAccount;
    use solana_sdk::message::{v0::Message as MessageV0, Message, VersionedMessage};
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::{Keypair, Signer};
    use solana_sdk::transaction::{Transaction, VersionedTransaction};
    use solana_system_interface::instruction as system_instruction;
    use std::collections::HashMap;
    use std::fmt;
    use std::str::FromStr;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration as StdDuration, Instant as StdInstant, SystemTime, UNIX_EPOCH};
    use tokio::sync::{broadcast, watch};
    use tokio::task::{yield_now, JoinHandle};
    use tokio::time::{self, Duration, Instant, MissedTickBehavior};
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;

    const NAME: &str = "worker::executor";
    const MAX_PLAN_STALENESS_MS: u64 = 400;
    const BUNDLE_SEND_ATTEMPTS: usize = 3;
    const BUNDLE_RETRY_BACKOFF_MS: u64 = 50;
    const TIP_STREAM_BACKOFF_MIN_MS: u64 = 250;
    const TIP_STREAM_BACKOFF_MAX_SECS: u64 = 5;
    const TIP_STREAM_JITTER_MIN_MS: u64 = 25;
    const TIP_STREAM_JITTER_MAX_MS: u64 = 750;
    const TIP_STREAM_JITTER_PCT: f64 = 0.25;
    const LOOKUP_TABLE_CACHE_TTL: StdDuration = StdDuration::from_secs(30);

    struct LookupTableRegistry {
        configs: Vec<LookupTableConfig>,
        cache: HashMap<Pubkey, CachedLookupTable>,
    }

    impl LookupTableRegistry {
        fn new(configs: Vec<LookupTableConfig>) -> Self {
            Self {
                configs,
                cache: HashMap::new(),
            }
        }

        fn reconfigure(&mut self, configs: Vec<LookupTableConfig>) {
            self.configs = configs;
            self.cache
                .retain(|address, _| self.configs.iter().any(|cfg| cfg.address == *address));
        }

        async fn load_accounts(
            &mut self,
            rpc: Option<&RpcClient>,
        ) -> Vec<AddressLookupTableAccount> {
            let Some(client) = rpc else {
                return Vec::new();
            };
            let mut resolved = Vec::new();
            for cfg in self.configs.clone() {
                if let Some(entry) = self.fetch_or_get_cached(client, &cfg).await {
                    resolved.push(entry);
                }
            }
            resolved
        }

        async fn fetch_or_get_cached(
            &mut self,
            client: &RpcClient,
            cfg: &LookupTableConfig,
        ) -> Option<AddressLookupTableAccount> {
            if let Some(entry) = self.cache.get(&cfg.address) {
                if entry.is_fresh() {
                    return Some(entry.account.clone());
                }
            }

            match Self::fetch_account(client, cfg).await {
                Some(account) => {
                    self.cache.insert(
                        cfg.address,
                        CachedLookupTable {
                            account: account.clone(),
                            refreshed_at: StdInstant::now(),
                        },
                    );
                    Some(account)
                }
                None => None,
            }
        }

        async fn fetch_account(
            client: &RpcClient,
            cfg: &LookupTableConfig,
        ) -> Option<AddressLookupTableAccount> {
            match client.get_account(&cfg.address).await {
                Ok(account) => match LookupTableState::deserialize(&account.data) {
                    Ok(state) => Some(AddressLookupTableAccount {
                        key: cfg.address,
                        addresses: state.addresses.into_owned(),
                    }),
                    Err(err) => {
                        tracing::warn!(
                            target: NAME,
                            table = %cfg.address,
                            label = %cfg.label,
                            ?err,
                            "failed to decode lookup table account"
                        );
                        None
                    }
                },
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        table = %cfg.address,
                        label = %cfg.label,
                        ?err,
                        "failed to fetch lookup table account"
                    );
                    None
                }
            }
        }
    }

    struct CachedLookupTable {
        account: AddressLookupTableAccount,
        refreshed_at: StdInstant,
    }

    impl CachedLookupTable {
        fn is_fresh(&self) -> bool {
            self.refreshed_at.elapsed() <= LOOKUP_TABLE_CACHE_TTL
        }
    }

    pub async fn run(
        mut cfg_rx: watch::Receiver<Arc<Config>>,
        mut stop_rx: broadcast::Receiver<StopReason>,
        mut wallet_rx: watch::Receiver<Arc<LoadedWallets>>,
        pipeline: PipelineHandles,
        metrics: Metrics,
        jito: Arc<JitoClient>,
        bundle_tracker: Arc<BundleTracker>,
        graph: Arc<RwLock<ArbGraph>>,
        bundle_feedback: Arc<BundleFeedback>,
    ) -> Result<()> {
        tracing::info!(
            target: NAME,
            plan_queue_capacity = pipeline.arb_plans.capacity(),
            "executor worker online"
        );

        let mut wallets = wallet_rx.borrow().clone();
        tracing::info!(target: NAME, payer = %wallets.payer_pubkey(), "payer wallet loaded");

        let plan_queue = pipeline.arb_plans.clone();
        let mut cfg_snapshot = cfg_rx.borrow().clone();
        let mut lookup_registry = LookupTableRegistry::new(cfg_snapshot.lookup_tables.clone());
        let mut rpc_client = build_rpc_client(&cfg_snapshot);
        let mut local_metrics = ExecutorMetrics::default();
        let mut last_report = Instant::now();
        let mut tick = time::interval(Duration::from_millis(5));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let http_client = HttpClient::builder()
            .user_agent("tre-executor/1.0")
            .timeout(Duration::from_secs(3))
            .build()?;
        let mut tip_floor =
            TipFloor::new(http_client, Some(cfg_snapshot.jito.tip_floor_url.clone()));
        let tip_cache = TipCache::load_static(&metrics);
        let mut blockhash_cache = BlockhashCache::default();
        if let Err(err) = tip_floor.refresh(&metrics).await {
            tracing::debug!(target: NAME, ?err, "failed to fetch initial tip floor");
        }
        let mut tip_floor_refresh = time::interval(Duration::from_secs(5));
        tip_floor_refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut tip_stream = TipStreamState::spawn(
            Some(cfg_snapshot.jito.tip_stream_url.clone()),
            metrics.clone(),
        );

        loop {
            let event = tokio::select! {
                stop = wait_for_stop(&mut stop_rx) => ExecutorEvent::Stop(stop),
                cfg = watch_config(&mut cfg_rx, NAME) => ExecutorEvent::Config(cfg),
                wallets_ev = watch_wallets(&mut wallet_rx) => ExecutorEvent::Wallet(wallets_ev),
                _ = tick.tick() => ExecutorEvent::Tick,
                _ = tip_floor_refresh.tick() => ExecutorEvent::TipFloorRefresh,
                tip_stream_status = async {
                    match tip_stream.as_mut() {
                        Some(stream) => stream.rx.changed().await.is_ok(),
                        None => pending::<bool>().await,
                    }
                } => ExecutorEvent::TipStream(tip_stream_status),
            };

            match event {
                ExecutorEvent::Stop(stop) => {
                    if let Some(reason) = stop {
                        tracing::info!(target: NAME, ?reason, "executor worker exiting");
                    }
                    break;
                }
                ExecutorEvent::Config(cfg) => match cfg {
                    Some(cfg) => {
                        tracing::debug!(
                            target: NAME,
                            jito_endpoints = cfg.jito.endpoints.len(),
                            "executor config updated"
                        );
                        rpc_client = build_rpc_client(&cfg);
                        lookup_registry.reconfigure(cfg.lookup_tables.clone());
                        cfg_snapshot = cfg;
                        tip_floor.set_url(Some(cfg_snapshot.jito.tip_floor_url.clone()));
                        if let Err(err) = tip_floor.refresh(&metrics).await {
                            tracing::debug!(
                                target: NAME,
                                ?err,
                                "tip floor refresh after config update failed"
                            );
                        }
                        tip_stream = TipStreamState::spawn(
                            Some(cfg_snapshot.jito.tip_stream_url.clone()),
                            metrics.clone(),
                        );
                    }
                    None => {
                        tracing::info!(target: NAME, "config channel closed; stopping");
                        break;
                    }
                },
                ExecutorEvent::Tick => {
                    let stats = process_queue(
                        &plan_queue,
                        &tip_cache,
                        &jito,
                        &metrics,
                        &wallets,
                        &mut blockhash_cache,
                        &mut lookup_registry,
                        rpc_client.as_deref(),
                        &graph,
                        bundle_feedback.as_ref(),
                        &cfg_snapshot.executor,
                        cfg_snapshot.planner.min_profit_lamports.max(1),
                        tip_floor.current(),
                        &bundle_tracker,
                    )
                    .await;
                    local_metrics.record(&stats);
                    let queue_depth = plan_queue.len();
                    metrics.observe_queue_depth("arb_plans", queue_depth);
                    metrics.observe_queue_saturation(
                        "arb_plans",
                        queue_depth,
                        plan_queue.capacity(),
                    );
                    metrics.inc_executor_plans(stats.plans);

                    if !stats.did_work() {
                        yield_now().await;
                    }

                    if last_report.elapsed() >= Duration::from_secs(5) {
                        tracing::info!(
                            target: NAME,
                            queue_depth = plan_queue.len(),
                            plans_executed = local_metrics.plans,
                            bundles_sent = local_metrics.sent,
                            dispatch_failures = local_metrics.failed,
                            last_plan_id = local_metrics.last_plan_id,
                            "executor heartbeat"
                        );
                        local_metrics.reset();
                        last_report = Instant::now();
                    }
                }
                ExecutorEvent::TipFloorRefresh => {
                    if let Err(err) = tip_floor.refresh(&metrics).await {
                        tracing::debug!(
                            target: NAME,
                            ?err,
                            "tip floor refresh failed"
                        );
                    }
                }
                ExecutorEvent::TipStream(active) => {
                    if !active {
                        tip_stream = TipStreamState::spawn(
                            Some(cfg_snapshot.jito.tip_stream_url.clone()),
                            metrics.clone(),
                        );
                        continue;
                    }
                    if let Some(stream) = tip_stream.as_ref() {
                        if let Some(value) = stream.latest() {
                            tip_floor.ingest_stream_value(value, &metrics);
                        }
                    }
                }
                ExecutorEvent::Wallet(update) => match update {
                    Some(new_wallets) => {
                        tracing::info!(
                            target: NAME,
                            payer = %new_wallets.payer_pubkey(),
                            "wallet config updated"
                        );
                        wallets = new_wallets;
                    }
                    None => {
                        tracing::warn!(target: NAME, "wallet channel closed; stopping");
                        break;
                    }
                },
            }
        }

        Ok(())
    }

    async fn process_queue(
        plan_queue: &Arc<ArrayQueue<ArbPlan>>,
        tips: &TipCache,
        jito: &JitoClient,
        metrics: &Metrics,
        wallets: &LoadedWallets,
        blockhash_cache: &mut BlockhashCache,
        lookup_registry: &mut LookupTableRegistry,
        rpc_client: Option<&RpcClient>,
        graph: &Arc<RwLock<ArbGraph>>,
        bundle_feedback: &BundleFeedback,
        executor_cfg: &ExecutorConfig,
        min_profit_lamports: i64,
        tip_floor_lamports: Option<u64>,
        bundle_tracker: &BundleTracker,
    ) -> ProcessStats {
        let mut stats = ProcessStats::default();

        while let Some(mut plan) = plan_queue.pop() {
            stats.plans += 1;
            stats.last_plan_id = Some(plan.id);
            metrics.observe_executor_queue_wait(plan.enqueue_instant.elapsed());

            if plan.expected_profit_lamports < min_profit_lamports {
                tracing::debug!(
                    target: NAME,
                    plan_id = plan.id,
                    expected_profit = plan.expected_profit_lamports,
                    min_profit_lamports,
                    "plan below executor profit floor; dropping"
                );
                metrics.inc_executor_result("skipped_profit_floor");
                continue;
            }

            if plan.enqueue_instant.elapsed() > Duration::from_millis(MAX_PLAN_STALENESS_MS) {
                tracing::debug!(
                    target: NAME,
                    plan_id = plan.id,
                    age_ms = plan.enqueue_instant.elapsed().as_millis(),
                    "plan stale before execution; dropping"
                );
                metrics.inc_executor_result("skipped_stale");
                continue;
            }

            let flash_spec = &plan.flash_loan;
            let flash_label = flash_spec.label();
            let flash_instructions = &plan.flash_instructions;

            match revalidate_plan(&plan, graph, min_profit_lamports) {
                Ok(profit) => {
                    plan.expected_profit_lamports = profit;
                    metrics.inc_executor_revalidation("pass");
                }
                Err(err) => {
                    metrics.inc_executor_revalidation(err.metric_label());
                    match err {
                        RevalidationError::GraphUnavailable | RevalidationError::EmptyRoute => {
                            tracing::warn!(
                                target: NAME,
                                plan_id = plan.id,
                                flash_market = flash_label,
                                reason = %err,
                                "plan failed executor revalidation"
                            );
                        }
                        _ => {
                            tracing::debug!(
                                target: NAME,
                                plan_id = plan.id,
                                flash_market = flash_label,
                                reason = %err,
                                "plan failed executor revalidation"
                            );
                        }
                    }
                    continue;
                }
            }

            let selected_tip = tips.select(plan.id);
            let tip_account = selected_tip.map(|acct| acct.pubkey);
            let tip_account_label = selected_tip.map(|acct| acct.raw.clone());
            let tip_lamports = estimate_tip(
                plan.expected_profit_lamports,
                executor_cfg,
                tip_floor_lamports,
                bundle_feedback.streak(),
            );

            let synth_start = StdInstant::now();
            let packet_bytes = if plan.transactions.is_empty() {
                match synthesize_plan_packets(
                    &plan,
                    flash_instructions,
                    wallets,
                    blockhash_cache,
                    lookup_registry,
                    rpc_client,
                    metrics,
                    tip_account,
                    tip_lamports,
                    executor_cfg.compute_unit_limit,
                    executor_cfg.compute_unit_price_micro_lamports,
                )
                .await
                {
                    Ok(bytes) => {
                        metrics.inc_executor_tx_synth("success");
                        bytes
                    }
                    Err(err) => {
                        let status = err.metric_label();
                        metrics.inc_executor_tx_synth(status);
                        tracing::warn!(
                            target: NAME,
                            plan_id = plan.id,
                            flash_market = flash_label,
                            reason = %err,
                            "plan transaction synthesis failed"
                        );
                        match fallback_packet(
                            plan.id,
                            wallets,
                            blockhash_cache,
                            rpc_client,
                            metrics,
                        )
                        .await
                        {
                            Some(bytes) => vec![bytes],
                            None => {
                                tracing::warn!(
                                    target: NAME,
                                    plan_id = plan.id,
                                    "failed to synthesize fallback transaction"
                                );
                                continue;
                            }
                        }
                    }
                }
            } else {
                plan.transactions.clone()
            };
            metrics.observe_executor_synth_time(synth_start.elapsed());

            let tx_count = packet_bytes.len();
            let total_bytes: usize = packet_bytes.iter().map(|pkt| pkt.len()).sum();
            metrics.inc_executor_bundle_packets(tx_count);
            metrics.inc_executor_bundle_bytes(total_bytes);
            metrics.observe_executor_bundle_tip(tip_lamports);
            let Some(bundle) = build_bundle(packet_bytes) else {
                tracing::warn!(
                    target: NAME,
                    plan_id = plan.id,
                    "bundle construction failed"
                );
                continue;
            };

            match send_bundle_with_retries(jito, &bundle, BUNDLE_SEND_ATTEMPTS, plan.id).await {
                Ok(uuid) => {
                    stats.sent += 1;
                    metrics.inc_executor_result("success");
                    bundle_tracker.record_send(&uuid);
                    metrics.observe_bundle_inflight(bundle_tracker.inflight());
                    tracing::info!(
                        target: NAME,
                        plan_id = plan.id,
                        flash_market = flash_label,
                        tip_account = tip_account_label.as_deref().unwrap_or("unknown"),
                        tip_lamports,
                        expected_profit = plan.expected_profit_lamports,
                        hop_count = plan.hop_count,
                        tx_count,
                        bundle_uuid = %uuid,
                        "bundle submitted"
                    );
                }
                Err(err) => {
                    stats.failed += 1;
                    metrics.inc_executor_result("failure");
                    bundle_feedback.record_failure(metrics);
                    tracing::warn!(
                        target: NAME,
                        plan_id = plan.id,
                        flash_market = flash_label,
                        tip_account = tip_account_label.as_deref().unwrap_or("unknown"),
                        tip_lamports,
                        expected_profit = plan.expected_profit_lamports,
                        tx_count,
                        ?err,
                        "bundle submission failed"
                    );
                    metrics.observe_bundle_inflight(bundle_tracker.inflight());
                }
            }
        }

        stats
    }

    fn build_bundle(packets: Vec<Vec<u8>>) -> Option<bundle::Bundle> {
        if packets.is_empty() {
            return None;
        }

        Some(bundle::Bundle {
            header: Some(shared::Header {
                ts: Some(timestamp_now()),
            }),
            packets: packets_into_protos(packets),
        })
    }

    async fn send_bundle_with_retries(
        jito: &JitoClient,
        bundle: &bundle::Bundle,
        attempts: usize,
        plan_id: u64,
    ) -> Result<String, JitoError> {
        let total_attempts = attempts.max(1);
        let mut last_error: Option<JitoError> = None;
        for attempt in 0..total_attempts {
            match jito.send_bundle(bundle.clone()).await {
                Ok(uuid) => {
                    if attempt > 0 {
                        tracing::debug!(
                            target: NAME,
                            plan_id,
                            attempt = attempt + 1,
                            total_attempts,
                            "bundle send succeeded after retries"
                        );
                    }
                    return Ok(uuid);
                }
                Err(err) => {
                    tracing::warn!(
                        target: NAME,
                        plan_id,
                        attempt = attempt + 1,
                        total_attempts,
                        ?err,
                        "bundle send attempt failed"
                    );
                    last_error = Some(err);
                    if attempt + 1 < total_attempts {
                        tokio::time::sleep(Duration::from_millis(
                            BUNDLE_RETRY_BACKOFF_MS * ((attempt as u64) + 1),
                        ))
                        .await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or(JitoError::NoEndpoints))
    }

    fn packets_into_protos(packets: Vec<Vec<u8>>) -> Vec<packet::Packet> {
        packets
            .into_iter()
            .map(|tx| {
                let size = tx.len() as u64;
                packet::Packet {
                    data: tx,
                    meta: Some(packet::Meta {
                        size,
                        addr: String::new(),
                        port: 0,
                        flags: None,
                        sender_stake: 0,
                    }),
                }
            })
            .collect()
    }

    async fn synthesize_plan_packets(
        plan: &ArbPlan,
        flash_instructions: &FlashLoanInstructions,
        wallets: &LoadedWallets,
        blockhash_cache: &mut BlockhashCache,
        lookup_registry: &mut LookupTableRegistry,
        rpc_client: Option<&RpcClient>,
        metrics: &Metrics,
        tip_account: Option<Pubkey>,
        tip_lamports: u64,
        compute_unit_limit: u32,
        compute_unit_price_micro_lamports: u64,
    ) -> Result<Vec<Vec<u8>>, TxSynthesisError> {
        if plan.swap_instructions.is_empty() {
            tracing::warn!(
                target: NAME,
                plan_id = plan.id,
                "plan missing swap instructions; cannot build transactions"
            );
            return Err(TxSynthesisError::MissingSwapInstructions);
        }

        if plan.swap_instructions.len() != plan.hop_count as usize {
            tracing::debug!(
                target: NAME,
                plan_id = plan.id,
                swap_count = plan.swap_instructions.len(),
                hop_count = plan.hop_count,
                "swap instruction count mismatch"
            );
        }

        let ctx = TransactionContext {
            payer: wallets.payer(),
            tip_account,
            tip_lamports,
            compute_unit_limit,
            compute_unit_price_micro_lamports,
        };

        let mut packets = Vec::new();
        let atomic = assemble_atomic_transaction(
            plan.id,
            flash_instructions,
            &plan.swap_instructions,
            &ctx,
            blockhash_cache,
            lookup_registry,
            rpc_client,
            metrics,
        )
        .await
        .map_err(|source| TxSynthesisError::Atomic { source })?;
        packets.push(atomic);

        for (idx, batch) in plan.post_flash_batches.iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let standalone = assemble_instruction_transaction(
                plan.id,
                batch,
                &ctx,
                blockhash_cache,
                lookup_registry,
                rpc_client,
                metrics,
            )
            .await
            .map_err(|err| TxSynthesisError::Standalone {
                batch: idx,
                source: err,
            })?;
            packets.push(standalone);
        }

        Ok(packets)
    }

    async fn fallback_packet(
        plan_id: u64,
        wallets: &LoadedWallets,
        blockhash_cache: &mut BlockhashCache,
        rpc_client: Option<&RpcClient>,
        metrics: &Metrics,
    ) -> Option<Vec<u8>> {
        let payer = wallets.payer();
        let blockhash = blockhash_cache.latest(plan_id, rpc_client, metrics).await;

        let ix = system_instruction::transfer(&payer.pubkey(), &payer.pubkey(), 1);
        let message = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[payer.as_ref()], message, blockhash);
        match bincode::serialize(&tx) {
            Ok(bytes) => {
                metrics.inc_executor_fallback_tx();
                Some(bytes)
            }
            Err(err) => {
                tracing::warn!(target: NAME, plan_id, ?err, "failed to serialize fallback tx");
                None
            }
        }
    }

    struct TransactionContext {
        payer: Arc<Keypair>,
        tip_account: Option<Pubkey>,
        tip_lamports: u64,
        compute_unit_limit: u32,
        compute_unit_price_micro_lamports: u64,
    }

    async fn assemble_atomic_transaction(
        plan_id: u64,
        flash_instructions: &FlashLoanInstructions,
        swaps: &[Instruction],
        ctx: &TransactionContext,
        blockhash_cache: &mut BlockhashCache,
        lookup_registry: &mut LookupTableRegistry,
        rpc_client: Option<&RpcClient>,
        metrics: &Metrics,
    ) -> Result<Vec<u8>, TxAssemblyError> {
        let tip_slots = if ctx.tip_account.is_some() && ctx.tip_lamports > 0 {
            1
        } else {
            0
        };
        let mut instructions = Vec::with_capacity(swaps.len() + 4 + tip_slots);
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            ctx.compute_unit_limit,
        ));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
            ctx.compute_unit_price_micro_lamports,
        ));
        if let (Some(account), true) = (ctx.tip_account, ctx.tip_lamports > 0) {
            instructions.push(system_instruction::transfer(
                &ctx.payer.pubkey(),
                &account,
                ctx.tip_lamports,
            ));
        }
        instructions.push(flash_instructions.borrow.clone());
        instructions.extend(swaps.iter().cloned());
        instructions.push(flash_instructions.repay.clone());

        build_transaction_bytes(
            plan_id,
            instructions,
            &ctx.payer,
            blockhash_cache,
            lookup_registry,
            rpc_client,
            metrics,
            SimulationMode::Atomic,
        )
        .await
    }

    async fn assemble_instruction_transaction(
        plan_id: u64,
        batch: &[Instruction],
        ctx: &TransactionContext,
        blockhash_cache: &mut BlockhashCache,
        lookup_registry: &mut LookupTableRegistry,
        rpc_client: Option<&RpcClient>,
        metrics: &Metrics,
    ) -> Result<Vec<u8>, TxAssemblyError> {
        if batch.is_empty() {
            return Err(TxAssemblyError::Serialize(
                "instruction batch empty".to_string(),
            ));
        }

        let mut instructions = Vec::with_capacity(batch.len() + 2);
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            ctx.compute_unit_limit,
        ));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
            ctx.compute_unit_price_micro_lamports,
        ));
        instructions.extend(batch.iter().cloned());

        build_transaction_bytes(
            plan_id,
            instructions,
            &ctx.payer,
            blockhash_cache,
            lookup_registry,
            rpc_client,
            metrics,
            SimulationMode::None,
        )
        .await
    }

    async fn build_transaction_bytes(
        plan_id: u64,
        instructions: Vec<Instruction>,
        payer: &Arc<Keypair>,
        blockhash_cache: &mut BlockhashCache,
        lookup_registry: &mut LookupTableRegistry,
        rpc_client: Option<&RpcClient>,
        metrics: &Metrics,
        simulation: SimulationMode,
    ) -> Result<Vec<u8>, TxAssemblyError> {
        let blockhash = blockhash_cache.latest(plan_id, rpc_client, metrics).await;
        let simulate_atomic = matches!(simulation, SimulationMode::Atomic);
        let mut legacy_message = Message::new(&instructions, Some(&payer.pubkey()));
        legacy_message.recent_blockhash = blockhash;
        let legacy_tx = VersionedTransaction::try_new(
            VersionedMessage::Legacy(legacy_message),
            &[payer.as_ref()],
        )
        .map_err(|err| TxAssemblyError::Serialize(err.to_string()))?;
        let mut bytes = serialize_versioned_transaction(&legacy_tx)?;
        if simulate_atomic {
            if let Some(client) = rpc_client {
                if let Err(err) = simulate_versioned_transaction(client, &legacy_tx).await {
                    tracing::debug!(
                        target: NAME,
                        plan_id,
                        reason = %err,
                        "atomic transaction simulation failed"
                    );
                    return Err(TxAssemblyError::Simulation(err));
                }
            }
        }
        if bytes.len() <= PACKET_DATA_SIZE {
            return Ok(bytes);
        }

        let lookup_accounts = lookup_registry.load_accounts(rpc_client).await;
        if lookup_accounts.is_empty() {
            tracing::debug!(
                target: NAME,
                plan_id,
                "legacy transaction exceeded packet limit and no lookup tables available"
            );
            return Err(TxAssemblyError::LookupTablesUnavailable { size: bytes.len() });
        }

        tracing::debug!(
            target: NAME,
            plan_id,
            legacy_size = bytes.len(),
            tables = lookup_accounts.len(),
            "attempting versioned transaction fallback"
        );

        let v0_message = MessageV0::try_compile(
            &payer.pubkey(),
            &instructions,
            lookup_accounts.as_slice(),
            blockhash,
        )
        .map_err(|err| TxAssemblyError::VersionedCompile(err.to_string()))?;
        let versioned_tx =
            VersionedTransaction::try_new(VersionedMessage::V0(v0_message), &[payer.as_ref()])
                .map_err(|err| TxAssemblyError::Serialize(err.to_string()))?;
        if simulate_atomic {
            if let Some(client) = rpc_client {
                if let Err(err) = simulate_versioned_transaction(client, &versioned_tx).await {
                    tracing::debug!(
                        target: NAME,
                        plan_id,
                        reason = %err,
                        "versioned transaction simulation failed"
                    );
                    return Err(TxAssemblyError::Simulation(err));
                }
            }
        }
        bytes = serialize_versioned_transaction(&versioned_tx)?;
        if bytes.len() > PACKET_DATA_SIZE {
            Err(TxAssemblyError::PacketTooLarge { size: bytes.len() })
        } else {
            Ok(bytes)
        }
    }

    fn serialize_versioned_transaction(
        tx: &VersionedTransaction,
    ) -> Result<Vec<u8>, TxAssemblyError> {
        bincode::serialize(tx).map_err(|err| TxAssemblyError::Serialize(err.to_string()))
    }

    enum SimulationMode {
        None,
        Atomic,
    }

    async fn simulate_versioned_transaction(
        client: &RpcClient,
        tx: &VersionedTransaction,
    ) -> Result<(), String> {
        let mut config = RpcSimulateTransactionConfig::default();
        config.sig_verify = false;
        config.commitment = Some(CommitmentConfig::processed());
        match client.simulate_transaction_with_config(tx, config).await {
            Ok(result) => {
                if let Some(err) = result.value.err {
                    let logs = result.value.logs.unwrap_or_default().join(" | ");
                    Err(format!("simulation error: {err:?}; logs: {logs}"))
                } else {
                    Ok(())
                }
            }
            Err(err) => Err(format!("simulation rpc error: {err}")),
        }
    }

    #[derive(Debug)]
    enum TxSynthesisError {
        MissingSwapInstructions,
        Atomic {
            source: TxAssemblyError,
        },
        Standalone {
            batch: usize,
            source: TxAssemblyError,
        },
    }

    impl TxSynthesisError {
        fn metric_label(&self) -> &'static str {
            match self {
                TxSynthesisError::MissingSwapInstructions => "missing_swaps",
                TxSynthesisError::Atomic { source } => source.metric_label_atomic(),
                TxSynthesisError::Standalone { source, .. } => source.metric_label_post(),
            }
        }
    }

    impl fmt::Display for TxSynthesisError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TxSynthesisError::MissingSwapInstructions => {
                    write!(f, "plan missing swap instructions")
                }
                TxSynthesisError::Atomic { source } => {
                    write!(f, "atomic transaction assembly failed: {source}")
                }
                TxSynthesisError::Standalone { batch, source } => {
                    write!(f, "post-flash batch {batch} assembly failed: {source}")
                }
            }
        }
    }

    #[derive(Debug)]
    enum TxAssemblyError {
        PacketTooLarge { size: usize },
        LookupTablesUnavailable { size: usize },
        Serialize(String),
        Simulation(String),
        VersionedCompile(String),
    }

    impl TxAssemblyError {
        fn metric_label_atomic(&self) -> &'static str {
            match self {
                TxAssemblyError::PacketTooLarge { .. } => "atomic_packet_too_large",
                TxAssemblyError::LookupTablesUnavailable { .. } => "atomic_lookup_unavailable",
                TxAssemblyError::Serialize(_) => "atomic_serialize",
                TxAssemblyError::Simulation(_) => "atomic_simulation",
                TxAssemblyError::VersionedCompile(_) => "atomic_versioned_compile",
            }
        }

        fn metric_label_post(&self) -> &'static str {
            match self {
                TxAssemblyError::PacketTooLarge { .. } => "post_packet_too_large",
                TxAssemblyError::LookupTablesUnavailable { .. } => "post_lookup_unavailable",
                TxAssemblyError::Serialize(_) => "post_serialize",
                TxAssemblyError::Simulation(_) => "post_simulation",
                TxAssemblyError::VersionedCompile(_) => "post_versioned_compile",
            }
        }
    }

    impl fmt::Display for TxAssemblyError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TxAssemblyError::PacketTooLarge { size } => {
                    write!(f, "packet size {size} exceeds {PACKET_DATA_SIZE} bytes")
                }
                TxAssemblyError::LookupTablesUnavailable { size } => {
                    write!(
                        f,
                        "packet size {size} exceeds {PACKET_DATA_SIZE} bytes and no lookup tables were available"
                    )
                }
                TxAssemblyError::Serialize(msg) => write!(f, "serialization error: {msg}"),
                TxAssemblyError::Simulation(msg) => write!(f, "simulation failed: {msg}"),
                TxAssemblyError::VersionedCompile(msg) => {
                    write!(f, "failed to compile versioned message: {msg}")
                }
            }
        }
    }

    fn build_rpc_client(cfg: &Arc<Config>) -> Option<Arc<RpcClient>> {
        cfg.quicknode_rpcs
            .get(0)
            .map(|url| RpcClient::new(url.clone()))
            .map(Arc::new)
    }

    fn timestamp_now() -> Timestamp {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0));
        Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        }
    }

    fn estimate_tip(
        expected_profit: i64,
        cfg: &ExecutorConfig,
        floor_lamports: Option<u64>,
        failure_streak: u64,
    ) -> u64 {
        let divisor = cfg.tip_divisor.max(1);
        let base = if expected_profit > 0 {
            (expected_profit as u64) / divisor
        } else {
            0
        };
        let mut tip = base.max(cfg.min_tip_lamports);
        if failure_streak > 0 {
            tip = apply_tip_escalation(tip, failure_streak);
        }
        if let Some(floor) = floor_lamports {
            tip = tip.max(floor);
        }
        tip
    }

    fn apply_tip_escalation(base: u64, failure_streak: u64) -> u64 {
        const BPS_SCALE: u64 = 10_000;
        const ESCALATION_BPS_PER_STREAK: u64 = 750; // 7.5% per failure
        let capped_streak = failure_streak.min(8);
        let multiplier_bps = BPS_SCALE + ESCALATION_BPS_PER_STREAK * capped_streak;
        base.saturating_mul(multiplier_bps)
            .saturating_add(BPS_SCALE - 1)
            / BPS_SCALE
    }

    #[derive(Default)]
    struct ProcessStats {
        plans: u64,
        sent: u64,
        failed: u64,
        last_plan_id: Option<u64>,
    }

    impl ProcessStats {
        fn did_work(&self) -> bool {
            self.plans > 0 || self.sent > 0 || self.failed > 0
        }
    }

    fn revalidate_plan(
        plan: &ArbPlan,
        graph: &Arc<RwLock<ArbGraph>>,
        min_profit_lamports: i64,
    ) -> Result<i64, RevalidationError> {
        if plan.hops.is_empty() {
            return Err(RevalidationError::EmptyRoute);
        }

        let guard = graph
            .read()
            .map_err(|_| RevalidationError::GraphUnavailable)?;
        let mut amount_out = plan.notional;
        for (idx, hop) in plan.hops.iter().enumerate() {
            let requote =
                guard
                    .reprice_hop(hop)
                    .ok_or_else(|| RevalidationError::MissingState {
                        hop_index: idx,
                        pool: hop.pool,
                    })?;
            if requote.min_amount_out < hop.min_amount_out {
                return Err(RevalidationError::Slippage {
                    hop_index: idx,
                    pool: hop.pool,
                    planned_min_out: hop.min_amount_out,
                    current_min_out: requote.min_amount_out,
                });
            }
            amount_out = requote.amount_out;
        }
        drop(guard);

        let profit = amount_out as i128 - plan.notional as i128;
        let clamped = profit.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        if clamped < min_profit_lamports {
            return Err(RevalidationError::ProfitFloor {
                profit: clamped,
                min_profit: min_profit_lamports,
            });
        }

        Ok(clamped)
    }

    #[derive(Debug)]
    enum RevalidationError {
        EmptyRoute,
        GraphUnavailable,
        MissingState {
            hop_index: usize,
            pool: Pubkey,
        },
        Slippage {
            hop_index: usize,
            pool: Pubkey,
            planned_min_out: u64,
            current_min_out: u64,
        },
        ProfitFloor {
            profit: i64,
            min_profit: i64,
        },
    }

    impl RevalidationError {
        fn metric_label(&self) -> &'static str {
            match self {
                RevalidationError::EmptyRoute => "empty_route",
                RevalidationError::GraphUnavailable => "graph_unavailable",
                RevalidationError::MissingState { .. } => "missing_state",
                RevalidationError::Slippage { .. } => "slippage",
                RevalidationError::ProfitFloor { .. } => "profit_floor",
            }
        }
    }

    impl fmt::Display for RevalidationError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                RevalidationError::EmptyRoute => write!(f, "plan missing hops"),
                RevalidationError::GraphUnavailable => {
                    write!(f, "arbgraph lock unavailable for revalidation")
                }
                RevalidationError::MissingState { hop_index, pool } => {
                    write!(f, "hop {hop_index} missing state for pool {pool}")
                }
                RevalidationError::Slippage {
                    hop_index,
                    pool,
                    planned_min_out,
                    current_min_out,
                } => {
                    write!(
                        f,
                        "hop {hop_index} slippage degraded for pool {pool}: planned {planned_min_out}, current {current_min_out}"
                    )
                }
                RevalidationError::ProfitFloor { profit, min_profit } => {
                    write!(
                        f,
                        "expected profit {profit} below executor floor {min_profit}"
                    )
                }
            }
        }
    }

    #[derive(Default)]
    struct ExecutorMetrics {
        plans: u64,
        sent: u64,
        failed: u64,
        last_plan_id: Option<u64>,
    }

    impl ExecutorMetrics {
        fn record(&mut self, stats: &ProcessStats) {
            self.plans += stats.plans;
            self.sent += stats.sent;
            self.failed += stats.failed;
            if let Some(id) = stats.last_plan_id {
                self.last_plan_id = Some(id);
            }
        }

        fn reset(&mut self) {
            self.plans = 0;
            self.sent = 0;
            self.failed = 0;
            self.last_plan_id = None;
        }
    }

    #[derive(Debug)]
    enum ExecutorEvent {
        Stop(Option<StopReason>),
        Config(Option<Arc<Config>>),
        Wallet(Option<Arc<LoadedWallets>>),
        Tick,
        TipFloorRefresh,
        TipStream(bool),
    }

    const STATIC_TIP_ACCOUNTS: &[&str] = &[
        "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
        "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
        "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
        "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
        "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
        "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
        "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    ];

    struct TipCache {
        accounts: Vec<TipAccount>,
    }

    #[derive(Clone)]
    struct TipAccount {
        pubkey: Pubkey,
        raw: String,
    }

    impl TipCache {
        fn select(&self, plan_id: u64) -> Option<&TipAccount> {
            if self.accounts.is_empty() {
                None
            } else {
                let idx = (plan_id as usize) % self.accounts.len();
                self.accounts.get(idx)
            }
        }

        fn load_static(metrics: &Metrics) -> Self {
            let mut parsed = Vec::new();
            for raw in STATIC_TIP_ACCOUNTS {
                match Pubkey::from_str(raw) {
                    Ok(pubkey) => parsed.push(TipAccount {
                        pubkey,
                        raw: raw.to_string(),
                    }),
                    Err(err) => tracing::warn!(
                        target: NAME,
                        tip_account = %raw,
                        ?err,
                        "invalid static tip account"
                    ),
                }
            }
            metrics.observe_executor_tip_accounts(parsed.len());
            metrics.inc_executor_tip_refresh("static");
            Self { accounts: parsed }
        }
    }

    struct TipFloor {
        client: HttpClient,
        url: Option<String>,
        latest: Option<u64>,
    }

    impl TipFloor {
        fn new(client: HttpClient, url: Option<String>) -> Self {
            let normalized = url.and_then(|value| sanitize_url(&value));
            Self {
                client,
                url: normalized,
                latest: None,
            }
        }

        fn current(&self) -> Option<u64> {
            self.latest
        }

        fn set_url(&mut self, url: Option<String>) {
            self.url = url.and_then(|value| sanitize_url(&value));
        }

        async fn refresh(&mut self, metrics: &Metrics) -> Result<Option<u64>> {
            let Some(url) = self.url.clone() else {
                return Ok(None);
            };
            match self.fetch(&url).await {
                Ok(value) => {
                    self.record_value(value, metrics, "http_success");
                    Ok(Some(value))
                }
                Err(err) => {
                    metrics.inc_executor_tip_floor_refresh("http_error");
                    Err(err)
                }
            }
        }

        fn ingest_stream_value(&mut self, value: u64, metrics: &Metrics) {
            self.record_value(value, metrics, "stream_value");
        }

        fn record_value(&mut self, value: u64, metrics: &Metrics, source: &'static str) {
            self.latest = Some(value);
            metrics.inc_executor_tip_floor_refresh(source);
            metrics.observe_executor_tip_floor(value);
            tracing::trace!(
                target: NAME,
                tip_floor_lamports = value,
                source,
                "tip floor updated"
            );
        }

        async fn fetch(&self, url: &str) -> Result<u64> {
            let response = self
                .client
                .get(url)
                .header("accept", "application/json")
                .send()
                .await?;
            let status = response.status();
            if !status.is_success() {
                return Err(anyhow!("tip floor request returned status {status}"));
            }
            let body = response.text().await?;
            let json: Value = serde_json::from_str(&body)?;
            extract_tip_floor(&json)
                .ok_or_else(|| anyhow!("tip floor response missing numeric floor"))
        }
    }

    fn sanitize_url(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    fn extract_tip_floor(value: &Value) -> Option<u64> {
        const PATHS: &[&[&str]] = &[
            &["data", "value"],
            &["data", "floor"],
            &["data", "min"],
            &["value"],
            &["floor"],
            &["min"],
            &["tip_floor"],
            &["lamports"],
        ];
        for path in PATHS {
            if let Some(candidate) = follow_path(value, path) {
                if let Some(num) = value_to_u64(candidate) {
                    return Some(num);
                }
            }
        }
        match value {
            Value::Array(items) => {
                for item in items {
                    if let Some(num) = extract_tip_floor(item) {
                        return Some(num);
                    }
                }
                None
            }
            Value::Object(map) => {
                const KEYS: &[&str] = &[
                    "value",
                    "floor",
                    "min",
                    "tip_floor",
                    "lamports",
                    "median",
                    "p50",
                ];
                for key in KEYS {
                    if let Some(candidate) = map.get(*key) {
                        if let Some(num) = value_to_u64(candidate) {
                            return Some(num);
                        }
                    }
                }
                for candidate in map.values() {
                    if let Some(num) = extract_tip_floor(candidate) {
                        return Some(num);
                    }
                }
                None
            }
            _ => value_to_u64(value),
        }
    }

    fn follow_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
        let mut node = value;
        for key in path {
            node = node.get(*key)?;
        }
        Some(node)
    }

    fn value_to_u64(value: &Value) -> Option<u64> {
        match value {
            Value::Number(num) => num.as_u64().or_else(|| {
                num.as_f64().and_then(|f| {
                    if f.is_finite() {
                        Some(f.max(0.0).floor() as u64)
                    } else {
                        None
                    }
                })
            }),
            Value::String(text) => {
                let trimmed = text.trim();
                trimmed.parse::<u64>().ok().or_else(|| {
                    trimmed.parse::<f64>().ok().and_then(|f| {
                        if f.is_finite() {
                            Some(f.max(0.0).floor() as u64)
                        } else {
                            None
                        }
                    })
                })
            }
            _ => None,
        }
    }

    struct TipStreamState {
        rx: watch::Receiver<Option<u64>>,
        task: JoinHandle<()>,
    }

    impl TipStreamState {
        fn spawn(url: Option<String>, metrics: Metrics) -> Option<Self> {
            let url = url.and_then(|value| sanitize_stream_url(&value))?;
            let (tx, rx) = watch::channel(None);
            let task = tokio::spawn(run_tip_floor_stream(url.clone(), tx, metrics));
            Some(Self { rx, task })
        }

        fn latest(&self) -> Option<u64> {
            *self.rx.borrow()
        }
    }

    impl Drop for TipStreamState {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn run_tip_floor_stream(url: String, tx: watch::Sender<Option<u64>>, metrics: Metrics) {
        let mut backoff = StreamBackoff::new(
            Duration::from_millis(TIP_STREAM_BACKOFF_MIN_MS),
            Duration::from_secs(TIP_STREAM_BACKOFF_MAX_SECS),
        );
        loop {
            if tx.is_closed() {
                break;
            }
            match connect_async(&url).await {
                Ok((mut socket, _)) => {
                    tracing::info!(target: NAME, tip_stream = %url, "tip floor stream connected");
                    metrics.inc_executor_tip_floor_refresh("stream_connected");
                    backoff.reset();
                    while let Some(message) = socket.next().await {
                        match message {
                            Ok(WsMessage::Text(text)) => {
                                handle_tip_stream_payload(&text, &tx, &metrics);
                            }
                            Ok(WsMessage::Binary(data)) => match std::str::from_utf8(&data) {
                                Ok(text) => handle_tip_stream_payload(text, &tx, &metrics),
                                Err(err) => {
                                    metrics.inc_executor_tip_floor_refresh("stream_parse_error");
                                    tracing::debug!(
                                        target: NAME,
                                        ?err,
                                        "binary tip floor payload not utf-8"
                                    );
                                }
                            },
                            Ok(WsMessage::Ping(payload)) => {
                                let _ = socket.send(WsMessage::Pong(payload)).await;
                            }
                            Ok(WsMessage::Pong(_)) => {}
                            Ok(WsMessage::Close(frame)) => {
                                tracing::warn!(target: NAME, ?frame, "tip floor stream closed");
                                break;
                            }
                            Ok(WsMessage::Frame(_)) => {}
                            Err(err) => {
                                metrics.inc_executor_tip_floor_refresh("stream_error");
                                tracing::warn!(
                                    target: NAME,
                                    ?err,
                                    "tip floor stream errored"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    metrics.inc_executor_tip_floor_refresh("stream_error");
                    tracing::warn!(target: NAME, ?err, "failed to connect tip floor stream");
                }
            }

            if tx.is_closed() {
                break;
            }

            let (delay, jitter) = backoff.next_delay_with_jitter();
            tracing::info!(
                target: NAME,
                reconnect_delay_ms = delay.as_millis() as u64,
                jitter_ms = jitter.as_millis() as u64,
                "reconnecting tip floor stream"
            );
            tokio::time::sleep(delay).await;
        }
        tracing::debug!(target: NAME, "tip floor stream task exiting");
    }

    fn handle_tip_stream_payload(
        payload: &str,
        tx: &watch::Sender<Option<u64>>,
        metrics: &Metrics,
    ) {
        if let Some(value) = parse_tip_stream_payload(payload) {
            let _ = tx.send(Some(value));
        } else {
            metrics.inc_executor_tip_floor_refresh("stream_parse_error");
            tracing::trace!(target: NAME, "tip floor stream payload missing usable value");
        }
    }

    fn parse_tip_stream_payload(payload: &str) -> Option<u64> {
        let trimmed = payload.trim();
        if trimmed.is_empty() {
            return None;
        }

        if let Ok(json) = serde_json::from_str::<Value>(trimmed) {
            if let Some(value) = extract_tip_floor(&json) {
                return Some(value);
            }
        }

        for line in trimmed.lines() {
            let raw = line.trim();
            if raw.is_empty() {
                continue;
            }
            let data = raw.strip_prefix("data:").map(str::trim).unwrap_or(raw);
            if data.is_empty() {
                continue;
            }
            if let Ok(json) = serde_json::from_str::<Value>(data) {
                if let Some(value) = extract_tip_floor(&json) {
                    return Some(value);
                }
            }
            if let Ok(value) = data.parse::<u64>() {
                return Some(value);
            }
            if let Ok(value) = data.parse::<f64>() {
                if value.is_finite() {
                    return Some(value.max(0.0).floor() as u64);
                }
            }
        }

        trimmed.parse::<u64>().ok()
    }

    fn sanitize_stream_url(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }
        if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
            return Some(trimmed.to_string());
        }
        if let Some(rest) = trimmed.strip_prefix("https://") {
            return Some(format!("wss://{rest}"));
        }
        if let Some(rest) = trimmed.strip_prefix("http://") {
            return Some(format!("ws://{rest}"));
        }
        Some(format!("wss://{trimmed}"))
    }

    struct StreamBackoff {
        current: Duration,
        min: Duration,
        max: Duration,
    }

    impl StreamBackoff {
        fn new(min: Duration, max: Duration) -> Self {
            Self {
                current: min,
                min,
                max,
            }
        }

        fn reset(&mut self) {
            self.current = self.min;
        }

        fn next_delay(&mut self) -> Duration {
            let delay = self.current;
            self.current = (self.current * 2).min(self.max);
            delay
        }

        fn next_delay_with_jitter(&mut self) -> (Duration, Duration) {
            let base = self.next_delay();
            let base_ms = base.as_millis().min(u128::from(u64::MAX)) as u64;
            if base_ms == 0 {
                return (base, Duration::from_millis(0));
            }
            let scaled = (base_ms as f64 * TIP_STREAM_JITTER_PCT).round() as u64;
            let cap = scaled
                .clamp(TIP_STREAM_JITTER_MIN_MS, TIP_STREAM_JITTER_MAX_MS)
                .min(base_ms);
            if cap == 0 {
                return (base, Duration::from_millis(0));
            }
            let mut rng = thread_rng();
            let jitter_ms = rng.gen_range(0..=cap);
            if jitter_ms == 0 {
                return (base, Duration::from_millis(0));
            }
            let jitter = Duration::from_millis(jitter_ms);
            let jittered = if rng.gen_bool(0.5) {
                base.saturating_add(jitter)
            } else {
                base.saturating_sub(jitter)
            };
            (jittered, jitter)
        }
    }

    #[derive(Default)]
    struct BlockhashCache {
        value: Option<Hash>,
        fetched_at: Option<Instant>,
    }

    impl BlockhashCache {
        const TTL: Duration = Duration::from_secs(2);

        async fn latest(
            &mut self,
            plan_id: u64,
            rpc_client: Option<&RpcClient>,
            metrics: &Metrics,
        ) -> Hash {
            if let Some(client) = rpc_client {
                let stale = self
                    .fetched_at
                    .map(|ts| ts.elapsed() >= Self::TTL)
                    .unwrap_or(true);
                if stale {
                    match client.get_latest_blockhash().await {
                        Ok(hash) => {
                            self.value = Some(hash);
                            self.fetched_at = Some(Instant::now());
                            metrics.inc_executor_blockhash_refresh("success");
                            return hash;
                        }
                        Err(err) => {
                            metrics.inc_executor_blockhash_refresh("error");
                            tracing::warn!(
                                target: NAME,
                                plan_id,
                                ?err,
                                "failed to refresh blockhash; using cached/random hash"
                            );
                        }
                    }
                }
                if let Some(hash) = self.value {
                    return hash;
                }
            }

            let hash = Hash::new_unique();
            self.value = Some(hash);
            self.fetched_at = Some(Instant::now());
            hash
        }
    }
}

mod bundle_monitor {
    use super::{wait_for_stop, BundleFeedback, BundleTracker, StopReason};
    use crate::jito::JitoClient;
    use crate::metrics::Metrics;
    use crate::proto::bundle;
    use anyhow::{anyhow, Context, Result};
    use rand::{thread_rng, Rng};
    use std::sync::Arc;
    use std::time::Duration as StdDuration;
    use tokio::sync::broadcast;
    use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
    use tonic::{Code, Status};

    const NAME: &str = "worker::bundle_monitor";
    const TRACKER_PRUNE_INTERVAL: Duration = Duration::from_secs(30);
    const TRACKER_MAX_AGE: StdDuration = StdDuration::from_secs(300);
    const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
    const BACKOFF_MAX: Duration = Duration::from_secs(30);
    const BACKOFF_JITTER_MS: u64 = 750;
    /// Ensures we respect the documented 1 rps/IP/region rate-limit when the
    /// block engine explicitly responds with `ResourceExhausted`.
    const RATE_LIMIT_DELAY: Duration = Duration::from_secs(5);

    pub async fn run(
        mut stop_rx: broadcast::Receiver<StopReason>,
        jito: Arc<JitoClient>,
        metrics: Metrics,
        tracker: Arc<BundleTracker>,
        feedback: Arc<BundleFeedback>,
    ) -> Result<()> {
        let mut backoff = BACKOFF_INITIAL;

        loop {
            let stream_stop = stop_rx.resubscribe();
            tokio::select! {
                stop = wait_for_stop(&mut stop_rx) => {
                    if let Some(reason) = stop {
                        tracing::info!(target: NAME, ?reason, "bundle monitor exiting");
                    }
                    break;
                }
                result = run_stream(
                    jito.clone(),
                    metrics.clone(),
                    tracker.clone(),
                    feedback.clone(),
                    stream_stop,
                ) => {
                    match result {
                        Ok(()) => {
                            // stop signal propagated; exit loop
                            break;
                        }
                        Err(err) => {
                            let rate_limited = is_rate_limit_error(&err);
                            if rate_limited {
                                tracing::warn!(
                                    target: NAME,
                                    ?err,
                                    "bundle stream rate limited by block engine; pausing before reconnect"
                                );
                            } else {
                                tracing::warn!(target: NAME, ?err, "bundle stream error; reconnecting");
                            }

                            let delay = if rate_limited {
                                jitter_delay(RATE_LIMIT_DELAY)
                            } else {
                                jitter_delay(backoff)
                            };
                            backoff = if rate_limited {
                                BACKOFF_INITIAL
                            } else {
                                (backoff * 2).min(BACKOFF_MAX)
                            };
                            tokio::select! {
                                _ = sleep(delay) => {},
                                stop = wait_for_stop(&mut stop_rx) => {
                                    if let Some(reason) = stop {
                                        tracing::info!(target: NAME, ?reason, "bundle monitor exiting");
                                    }
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn jitter_delay(base: Duration) -> Duration {
        if BACKOFF_JITTER_MS == 0 {
            return base;
        }
        let jitter = thread_rng().gen_range(0..=BACKOFF_JITTER_MS);
        base + Duration::from_millis(jitter)
    }

    async fn run_stream(
        jito: Arc<JitoClient>,
        metrics: Metrics,
        tracker: Arc<BundleTracker>,
        feedback: Arc<BundleFeedback>,
        mut stop_rx: broadcast::Receiver<StopReason>,
    ) -> Result<()> {
        let mut stream = jito
            .subscribe_bundle_results()
            .await
            .context("subscribe bundle results")?;
        tracing::info!(target: NAME, "bundle result stream connected");
        let mut prune_timer = interval(TRACKER_PRUNE_INTERVAL);
        prune_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                stop = wait_for_stop(&mut stop_rx) => {
                    if let Some(reason) = stop {
                        tracing::info!(target: NAME, ?reason, "bundle monitor stopping");
                    }
                    return Ok(());
                }
                _ = prune_timer.tick() => {
                    let removed = tracker.prune_stale(TRACKER_MAX_AGE);
                    if removed > 0 {
                        tracing::debug!(target: NAME, removed, "pruned stale bundle tracker entries");
                        metrics.observe_bundle_inflight(tracker.inflight());
                    }
                }
                message = stream.message() => {
                    match message {
                        Ok(Some(result)) => {
                            let removed = handle_bundle_result(
                                &metrics,
                                tracker.as_ref(),
                                feedback.as_ref(),
                                result,
                            );
                            if removed {
                                metrics.observe_bundle_inflight(tracker.inflight());
                            }
                        }
                        Ok(None) => {
                            return Err(anyhow!("bundle result stream closed"));
                        }
                        Err(status) => {
                            return Err(status.into());
                        }
                    }
                }
            }
        }
    }

    fn handle_bundle_result(
        metrics: &Metrics,
        tracker: &BundleTracker,
        feedback: &BundleFeedback,
        result: bundle::BundleResult,
    ) -> bool {
        use bundle::bundle_result;
        let bundle_id = result.bundle_id.clone();
        let latency = tracker.latency(&bundle_id);
        let mut finish_tracker = false;
        match result.result {
            Some(bundle_result::Result::Accepted(accepted)) => {
                metrics.inc_bundle_result("accepted");
                if let Some(delay) = latency {
                    metrics.observe_bundle_latency("accepted", delay);
                }
                feedback.reset(metrics);
                tracing::debug!(
                    target: NAME,
                    bundle_id = %bundle_id,
                    slot = accepted.slot,
                    validator = accepted.validator_identity,
                    "bundle accepted"
                );
            }
            Some(bundle_result::Result::Rejected(rejected)) => {
                metrics.inc_bundle_result("rejected");
                feedback.record_failure(metrics);
                metrics.inc_bundle_failure_reason(classify_rejected_reason(&rejected));
                if let Some(delay) = latency {
                    metrics.observe_bundle_latency("rejected", delay);
                }
                tracing::debug!(target: NAME, bundle_id = %bundle_id, ?rejected, "bundle rejected");
                finish_tracker = true;
            }
            Some(bundle_result::Result::Finalized(_)) => {
                metrics.inc_bundle_result("finalized");
                if let Some(delay) = latency {
                    metrics.observe_bundle_latency("finalized", delay);
                }
                feedback.reset(metrics);
                tracing::debug!(target: NAME, bundle_id = %bundle_id, "bundle finalized");
                finish_tracker = true;
            }
            Some(bundle_result::Result::Processed(processed)) => {
                metrics.inc_bundle_result("processed");
                if let Some(delay) = latency {
                    metrics.observe_bundle_latency("processed", delay);
                }
                feedback.reset(metrics);
                tracing::debug!(
                    target: NAME,
                    bundle_id = %bundle_id,
                    slot = processed.slot,
                    validator = processed.validator_identity,
                    index = processed.bundle_index,
                    "bundle processed"
                );
            }
            Some(bundle_result::Result::Dropped(dropped)) => {
                metrics.inc_bundle_result("dropped");
                feedback.record_failure(metrics);
                metrics.inc_bundle_failure_reason(classify_dropped_reason(dropped.reason));
                if let Some(delay) = latency {
                    metrics.observe_bundle_latency("dropped", delay);
                }
                tracing::debug!(target: NAME, bundle_id = %bundle_id, reason = ?dropped.reason, "bundle dropped");
                finish_tracker = true;
            }
            None => {
                metrics.inc_bundle_result("unknown");
                feedback.record_failure(metrics);
                metrics.inc_bundle_failure_reason("unknown");
                if let Some(delay) = latency {
                    metrics.observe_bundle_latency("unknown", delay);
                }
                tracing::debug!(target: NAME, bundle_id = %bundle_id, "bundle result missing payload");
                finish_tracker = true;
            }
        }

        if finish_tracker {
            tracker.finish(&bundle_id)
        } else {
            false
        }
    }

    fn classify_rejected_reason(rejected: &bundle::Rejected) -> &'static str {
        use bundle::rejected;
        match rejected.reason {
            Some(rejected::Reason::StateAuctionBidRejected(_)) => "state_auction_bid",
            Some(rejected::Reason::WinningBatchBidRejected(_)) => "winning_batch_bid",
            Some(rejected::Reason::SimulationFailure(_)) => "simulation_failure",
            Some(rejected::Reason::InternalError(_)) => "internal_error",
            Some(rejected::Reason::DroppedBundle(_)) => "dropped_bundle",
            None => "unknown",
        }
    }

    fn classify_dropped_reason(value: i32) -> &'static str {
        match bundle::DroppedReason::try_from(value) {
            Ok(bundle::DroppedReason::BlockhashExpired) => "blockhash_expired",
            Ok(bundle::DroppedReason::PartiallyProcessed) => "partially_processed",
            Ok(bundle::DroppedReason::NotFinalized) => "not_finalized",
            Err(_) => "unknown",
        }
    }

    fn is_rate_limit_error(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            cause
                .downcast_ref::<Status>()
                .map(|status| status.code() == Code::ResourceExhausted)
                .unwrap_or(false)
        })
    }
}
