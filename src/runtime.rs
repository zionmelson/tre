use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::signal;

use crate::adapter_book::AdapterBook;
use crate::builder::BuilderHandle;
use crate::bundle::{BundleClient, BundleTracker};
use crate::config::AppConfig;
use crate::core::{Lease, RingConsumer, SlabPool, SpscRing};
use crate::bootstrap::raydium::{resolve_pool_targets, RaydiumBootstrapFetcher};
use crate::corecast::{spawn_ingest_stage, IngestHandles, PoolStore};
use crate::notifier::Notifier;
use crate::planner::types::BundleCandidate;
use crate::planner::{
    spawn_fast_lane_planner, spawn_graph_service, spawn_submit_stage, PlannerConfig,
};
use crate::strategy::StrategyEngine;
use crate::submission::{spawn_fast_lane_submitter, SubmissionConfig};
use crate::telemetry;

const SHUTDOWN_LOG_TARGET: &str = "runtime";

pub async fn run(config: Arc<AppConfig>) -> Result<()> {
    telemetry::init_from_env().await?;
    let stop = Arc::new(AtomicBool::new(false));

    let builder = BuilderHandle::new(config.clone()).context("failed to initialize builder")?;
    let adapter_book = Arc::new(AdapterBook::new(config.clone()));
    let pool_store = Arc::new(PoolStore::with_capacity(config.perf.opportunity_slab));
    let pool_updates = pool_store.subscribe(config.perf.ring_capacity);

    let (graph_handle, graph_thread) = spawn_graph_service(
        stop.clone(),
        None,
        pool_updates,
        pool_store.view(),
        PlannerConfig::default(),
    )
    .context("failed to spawn graph service")?;

    let bootstrapped_pools = run_bootstrap_phase(config.clone(), adapter_book.clone()).await?;
    tracing::info!(bootstrapped_pools, "bootstrap phase complete");

    let strategy = Arc::new(
        StrategyEngine::new(config.clone()).context("failed to initialize strategy engine")?,
    );
    let notifier = Arc::new(Notifier::new(strategy, config.clone()));

    let IngestHandles {
        trade_rx,
        pool_rx,
        handle: ingest_thread,
    } = spawn_ingest_stage(stop.clone(), config.clone(), config.perf.ring_capacity)
    .context("failed to spawn CoreCast ingest stage")?;

    let planner_ring = SpscRing::with_capacity(config.perf.ring_capacity);
    let (planner_producer, stage2_consumer) = planner_ring.into_split();
    let submit_ring = SpscRing::with_capacity(config.perf.ring_capacity);
    let (stage2_producer, stage3_consumer) = submit_ring.into_split();
    let bundle_slab = SlabPool::new(config.perf.bundle_slab);

    let planner_handle = spawn_fast_lane_planner(
        stop.clone(),
        PlannerConfig::default(),
        trade_rx,
        pool_rx,
        planner_producer,
        bundle_slab,
        pool_store.clone(),
        graph_handle,
        Some(config.perf.eval_core),
    )
    .context("failed to spawn fast-lane planner")?;

    let stage2_handle = spawn_submit_stage(
        stop.clone(),
        Some(config.perf.eval_core),
        stage2_consumer,
        stage2_producer,
        builder.clone(),
    )
    .context("failed to spawn stage2 submitter")?;

    let (stage3_handle, tracker_handles) = if config.pipeline.dry_run_mode {
        tracing::warn!(
            target = SHUTDOWN_LOG_TARGET,
            "pipeline dry-run mode enabled; bundle submission disabled"
        );
        let sink = spawn_dry_run_sink(stop.clone(), stage3_consumer)
            .context("failed to spawn dry-run submission sink")?;
        (sink, Vec::new())
    } else {
        let auth = builder.auth_signer();
        let bundle_client = Arc::new(
            BundleClient::new(config.clone(), auth.clone())
                .context("failed to initialize bundle client")?,
        );
        let (bundle_tracker, tracker_handles) =
            BundleTracker::spawn(stop.clone(), config.clone(), notifier.clone(), auth)
                .context("failed to start bundle tracker")?;
        let submitter = spawn_fast_lane_submitter(
            stop.clone(),
            Some(config.perf.submit_core),
            stage3_consumer,
            builder.clone(),
            bundle_client,
            bundle_tracker,
            SubmissionConfig::default(),
        )
        .context("failed to spawn fast-lane submitter")?;
        (submitter, tracker_handles)
    };

    let handles = PipelineHandles {
        ingest: ingest_thread,
        planner: planner_handle,
        graph: graph_thread,
        stage2: stage2_handle,
        stage3: stage3_handle,
        tracker: tracker_handles,
    };

    tracing::info!(
        target = SHUTDOWN_LOG_TARGET,
        "runtime online; press Ctrl-C to exit"
    );
    signal::ctrl_c()
        .await
        .context("failed to install Ctrl-C handler")?;
    tracing::info!(
        target = SHUTDOWN_LOG_TARGET,
        "shutdown signal received; stopping workers"
    );

    stop.store(true, Ordering::SeqCst);
    handles.join()?;

    Ok(())
}

async fn run_bootstrap_phase(
    config: Arc<AppConfig>,
    adapter_book: Arc<AdapterBook>,
) -> Result<usize> {
    let endpoints = if !config.bootstrap.rpc_endpoints.is_empty() {
        config.bootstrap.rpc_endpoints.clone()
    } else {
        config.quicknode_rpcs.clone()
    };
    if endpoints.is_empty() {
        tracing::info!(
            target = "bootstrap",
            "bootstrap endpoints not configured; relying on CoreCast discovery"
        );
        return Ok(0);
    }

    let targets = resolve_pool_targets(&config.bootstrap.pool_allowlist)
        .context("invalid bootstrap pool allowlist entry")?;
    if targets.is_empty() {
        tracing::info!(
            target = "bootstrap",
            "pool allowlist empty; skipping bootstrap fetch"
        );
        return Ok(0);
    }

    let registry = adapter_book.registry();
    let recorded = tokio::task::spawn_blocking(move || {
        let fetcher = RaydiumBootstrapFetcher::new(endpoints)?;
        fetcher.bootstrap(&targets, registry.as_ref())
    })
    .await
    .context("bootstrap worker task panicked")??;

    Ok(recorded)
}

struct PipelineHandles {
    ingest: thread::JoinHandle<Result<()>>,
    planner: thread::JoinHandle<Result<()>>,
    graph: thread::JoinHandle<Result<()>>,
    stage2: thread::JoinHandle<Result<()>>,
    stage3: thread::JoinHandle<Result<()>>,
    tracker: Vec<thread::JoinHandle<Result<()>>>,
}

impl PipelineHandles {
    fn join(self) -> Result<()> {
        join_thread("ingest", self.ingest)?;
        join_thread("planner", self.planner)?;
        join_thread("graph", self.graph)?;
        join_thread("stage2", self.stage2)?;
        join_thread("stage3", self.stage3)?;
        for (idx, handle) in self.tracker.into_iter().enumerate() {
            join_thread(&format!("tracker_{idx}"), handle)?;
        }
        Ok(())
    }
}

fn join_thread(label: &str, handle: thread::JoinHandle<Result<()>>) -> Result<()> {
    match handle.join() {
        Ok(result) => result.with_context(|| format!("{label} thread exited with an error")),
        Err(payload) => {
            let message = if let Some(msg) = payload.downcast_ref::<&str>() {
                (*msg).to_string()
            } else if let Some(msg) = payload.downcast_ref::<String>() {
                msg.clone()
            } else {
                "unknown panic".to_string()
            };
            Err(anyhow!("{label} thread panicked: {message}"))
        }
    }
}

fn spawn_dry_run_sink(
    stop: Arc<AtomicBool>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("dry_run_submit".into())
        .spawn(move || run_dry_run_sink(stop, consumer))
        .map_err(Into::into)
}

fn run_dry_run_sink(
    stop: Arc<AtomicBool>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
) -> Result<()> {
    tracing::info!(target = "dry_run", "dry-run submission sink online");
    while !stop.load(Ordering::Relaxed) {
        if let Some(lease) = consumer.pop() {
            tracing::info!(
                target = "dry_run",
                seq = lease.seq,
                edge_bps = lease.estimated_edge_bps,
                "dry-run dropping bundle candidate"
            );
            drop(lease);
        } else {
            thread::sleep(Duration::from_millis(10));
        }
    }
    tracing::info!(target = "dry_run", "dry-run submission sink shutting down");
    Ok(())
}
