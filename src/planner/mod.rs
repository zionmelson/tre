use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use tokio::runtime::Builder as TokioRuntime;
use tokio::time::sleep;
use tracing::{debug, info, trace, warn};

use crate::builder::{BuilderHandle, BuilderStage};
use crate::config::AppConfig;
use crate::core::{pin_current_thread, Lease, RingConsumer, RingProducer, SlabPool};
use crate::corecast::CoreCastClient;
use crate::hydration::types::{PoolSnapshot, TradeUpdate};
use crate::hydration::{HydrationSnapshot, PoolStateEvent, PoolStateUpdate, PoolStore};
use crate::notifier::Notifier;
use crate::planner::graph_manager::ArbitrageOpportunity;
use crate::planner::types::{BundleCandidate, Opportunity};
use crate::telemetry;
use crate::token_pair::TokenPair;

mod graph;
pub mod graph_manager;
pub mod route_builder;
mod submit;
pub mod types;

pub use graph::{spawn_graph_service, GraphHandle};
pub use graph_manager::{GraphManager, PoolPath};
pub use submit::spawn_submit_stage;

const CYCLE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const CYCLE_ALERT_INTERVAL: Duration = Duration::from_millis(500);
const MAX_CYCLE_ALERTS_PER_BATCH: usize = 5;
const DEFAULT_CYCLE_NOTIONAL_LAMPORTS: u64 = 1_000_000;
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub detection_budget: Duration,
    pub freshness_budget: Duration,
    pub cycle_notional_lamports: u64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            detection_budget: Duration::from_millis(40),
            freshness_budget: Duration::from_millis(30),
            cycle_notional_lamports: DEFAULT_CYCLE_NOTIONAL_LAMPORTS,
        }
    }
}

pub struct FastLanePlanner {
    builder: BuilderHandle,
    config: PlannerConfig,
    trade_rx: Receiver<TradeUpdate>,
    pool_updates: Receiver<PoolStateUpdate>,
    bundle_pool: SlabPool<BundleCandidate>,
    bundle_producer: RingProducer<Lease<BundleCandidate>>,
    pending_cycle_scan: bool,
    pool_cache: HashMap<String, CachedPool>,
    graph: GraphHandle,
    notifier: Arc<Notifier>,
    cycle_cache: HashSet<String>,
    next_cycle_poll: Instant,
    next_cycle_alert: Instant,
    opportunity_seq: u64,
    pool_store: Arc<PoolStore>,
    readiness_timeout: Duration,
    min_ready_pools: usize,
}

impl FastLanePlanner {
    pub fn new(
        builder: BuilderHandle,
        config: PlannerConfig,
        graph: GraphHandle,
        notifier: Arc<Notifier>,
        trade_rx: Receiver<TradeUpdate>,
        pool_updates: Receiver<PoolStateUpdate>,
        bundle_pool: SlabPool<BundleCandidate>,
        bundle_producer: RingProducer<Lease<BundleCandidate>>,
        pool_store: Arc<PoolStore>,
        readiness_timeout: Duration,
        min_ready_pools: usize,
    ) -> Self {
        Self {
            builder,
            config,
            trade_rx,
            pool_updates,
            bundle_pool,
            bundle_producer,
            pending_cycle_scan: false,
            pool_cache: HashMap::new(),
            graph,
            notifier,
            cycle_cache: HashSet::new(),
            next_cycle_poll: Instant::now(),
            next_cycle_alert: Instant::now(),
            opportunity_seq: 0,
            pool_store,
            readiness_timeout,
            min_ready_pools,
        }
    }

    pub fn freshness_budget(&self) -> Duration {
        self.config.freshness_budget
    }

    pub fn deadline_from(&self, start: Instant) -> Instant {
        start + self.config.detection_budget
    }

    pub fn run(
        mut self,
        stop: Arc<AtomicBool>,
        core_id: Option<usize>,
        snapshot_consumer: RingConsumer<HydrationSnapshot>,
    ) -> Result<()> {
        if let Some(core) = core_id {
            let _ = pin_current_thread(core);
        }
        if !self.await_initial_readiness(&stop) {
            warn!(
                target = "fastlane",
                "fast-lane planner shutting down before hydration ready"
            );
            return Ok(());
        }
        info!(
            target = "fastlane",
            pools = self.pool_store.len(),
            min_ready = self.min_ready_pools,
            "initial pool hydration satisfied"
        );
        info!(
            detection_budget_ms = self.config.detection_budget.as_millis(),
            freshness_budget_ms = self.config.freshness_budget.as_millis(),
            "fast-lane planner online"
        );
        while !stop.load(Ordering::Relaxed) {
            self.builder.heartbeat(BuilderStage::EvalHotPath);
            let mut made_progress = false;
            if self.drain_pool_updates() {
                made_progress = true;
                self.pending_cycle_scan = true;
            }
            while let Some(snapshot) = snapshot_consumer.pop() {
                self.handle_snapshot(snapshot);
                made_progress = true;
            }
            while let Ok(trade) = self.trade_rx.try_recv() {
                self.handle_trade(trade);
                made_progress = true;
            }
            let now = Instant::now();
            if self.pending_cycle_scan || now >= self.next_cycle_poll {
                self.emit_arbitrage_cycles(now);
                self.next_cycle_poll = now + CYCLE_POLL_INTERVAL;
                self.pending_cycle_scan = false;
            }
            self.emit_watcher_metrics();
            if !made_progress {
                thread::yield_now();
                thread::sleep(Duration::from_millis(2));
            }
        }
        info!("fast-lane planner shutting down");
        Ok(())
    }

    fn await_initial_readiness(&self, stop: &Arc<AtomicBool>) -> bool {
        loop {
            if stop.load(Ordering::Relaxed) {
                return false;
            }
            if self.pool_store.wait_until_ready(self.readiness_timeout) {
                return true;
            }
            warn!(
                target = "fastlane",
                timeout_ms = self.readiness_timeout.as_millis(),
                ready_pools = self.pool_store.len(),
                min_ready = self.min_ready_pools,
                "still waiting for initial pool hydration"
            );
        }
    }

    fn handle_snapshot(&self, snapshot: HydrationSnapshot) {
        let freshness_deadline = snapshot.stale_at(self.config.freshness_budget);
        let detection_deadline = self.deadline_from(snapshot.issued_at);
        telemetry::record_detection_latency(Duration::from_millis(0));
        telemetry::record_hydration_template_refresh(snapshot.template_stats.last_refresh_unix_ms);
        telemetry::record_hydration_alt_refresh(snapshot.alt_stats.last_refresh_unix_ms);
        telemetry::record_hydration_backlog(snapshot.alt_stats.pending_jobs);
        telemetry::record_hydration_template_count(snapshot.template_stats.template_count);
        telemetry::record_hydration_alt_ready_tables(snapshot.alt_stats.ready_tables);
        telemetry::record_hydration_flash_routes(snapshot.flash_routes_prepared);
        debug!(
            templates = snapshot.template_stats.template_count,
            ready_tables = snapshot.alt_stats.ready_tables,
            flash_routes = snapshot.flash_routes_prepared,
            freshness_deadline = ?freshness_deadline,
            detection_deadline = ?detection_deadline,
            "ingested hydration snapshot"
        );
    }

    fn handle_trade(&mut self, trade: TradeUpdate) {
        let (snapshot, generation) = if let Some(entry) = self.snapshot_for_pool(&trade.pool_id) {
            entry
        } else {
            trace!(
                target = "fastlane",
                pool = %trade.pool_id,
                "trade skipped; no pool snapshot yet"
            );
            return;
        };

        if snapshot.freshness.is_stale() {
            trace!(
                target = "fastlane",
                pool = %trade.pool_id,
                "trade skipped; pool snapshot flagged stale"
            );
            return;
        }

        if snapshot.updated_at.elapsed() > self.config.freshness_budget {
            trace!(
                target = "fastlane",
                pool = %trade.pool_id,
                "trade skipped; pool snapshot stale"
            );
            return;
        }

        let seq = self.next_opportunity_seq();
        let opportunity =
            Opportunity::from_trade(seq, trade, Some(snapshot), Some(generation), None);
        self.enqueue_candidate(opportunity);
    }

    fn snapshot_for_pool(&self, pool_id: &str) -> Option<(PoolSnapshot, u64)> {
        self.pool_cache
            .get(pool_id)
            .map(|entry| (entry.snapshot.clone(), entry.generation))
    }

    fn emit_watcher_metrics(&self) {
        telemetry::record_watcher_cache_depth(self.pool_cache.len());
        telemetry::record_watcher_cycle_pending(self.pending_cycle_scan);
    }

    fn drain_pool_updates(&mut self) -> bool {
        let mut updated = false;
        loop {
            match self.pool_updates.try_recv() {
                Ok(update) => {
                    self.apply_pool_update(update);
                    updated = true;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    warn!(target = "fastlane", "pool update channel closed");
                    break;
                }
            }
        }
        let backlog = self.pool_updates.len();
        let capacity = self.pool_updates.capacity().unwrap_or(0);
        telemetry::record_watcher_update_backlog(backlog, capacity);
        updated
    }

    fn apply_pool_update(&mut self, update: PoolStateUpdate) {
        match update.event {
            PoolStateEvent::Upsert(snapshot) => {
                self.pool_cache.insert(
                    update.pool_id,
                    CachedPool {
                        generation: update.generation,
                        snapshot,
                    },
                );
            }
            PoolStateEvent::Remove => {
                self.pool_cache.remove(&update.pool_id);
            }
        }
    }

    fn next_opportunity_seq(&mut self) -> u64 {
        let seq = self.opportunity_seq;
        self.opportunity_seq = self.opportunity_seq.wrapping_add(1);
        seq
    }

    fn emit_arbitrage_cycles(&mut self, now: Instant) {
        let cycles = self.graph.latest_cycles();
        telemetry::record_graph_cycle_count(cycles.len());

        let mut observed = HashSet::with_capacity(cycles.len());
        let mut announced = Vec::new();

        for cycle in cycles {
            let signature = Self::cycle_signature(&cycle);
            if !observed.insert(signature.clone()) {
                continue;
            }
            if self.cycle_cache.contains(&signature) {
                continue;
            }

            let seq = self.next_opportunity_seq();
            match self.build_cycle_opportunity(seq, &cycle) {
                Some(opportunity) => {
                    debug!(
                        target = "fastlane",
                        seq = opportunity.seq,
                        gain = ?cycle.gain,
                        hops = cycle.path.len(),
                        route = %cycle
                            .path
                            .legs
                            .iter()
                            .map(|leg| leg.pool_id.as_str())
                            .collect::<Vec<_>>()
                            .join("->"),
                        "graph cycle converted into bundle candidate"
                    );
                    self.enqueue_candidate(opportunity);
                    self.cycle_cache.insert(signature);
                    announced.push(cycle);
                }
                None => {
                    trace!(
                        target = "fastlane",
                        ?signature,
                        "graph cycle skipped due to missing readiness"
                    );
                }
            }
        }

        self.cycle_cache
            .retain(|signature| observed.contains(signature));

        self.maybe_alert_cycles(now, announced);
    }

    fn build_cycle_opportunity(
        &mut self,
        seq: u64,
        cycle: &ArbitrageOpportunity,
    ) -> Option<Opportunity> {
        let route_plan = cycle.route_plan.clone()?;
        if route_plan.legs.is_empty() {
            return None;
        }
        if !route_plan
            .legs
            .iter()
            .all(|leg| leg.template_ready && leg.alt_ready)
        {
            trace!(
                target = "fastlane",
                route = %cycle
                    .path
                    .legs
                    .iter()
                    .map(|leg| leg.pool_id.as_str())
                    .collect::<Vec<_>>()
                    .join("->"),
                "cycle route not ready"
            );
            return None;
        }

        let first_leg = route_plan.legs.first()?;
        let estimated_final = route_plan.estimated_final_output_amount.unwrap_or(0);
        if estimated_final <= first_leg.input_amount {
            return None;
        }

        let trade = TradeUpdate {
            seq,
            slot: cycle.latest_slot.unwrap_or_default(),
            program_id: first_leg.program_id.clone(),
            pool_id: first_leg.pool_id.clone(),
            signature: format!("cycle:{}", Self::cycle_signature(cycle)),
            buy_mint: route_plan.start_mint.clone(),
            sell_mint: route_plan.start_mint.clone(),
            buy_amount: estimated_final,
            sell_amount: first_leg.input_amount,
        };

        let estimated_edge_bps = ((cycle.gain - 1.0).max(0.0)) * 10_000.0;

        Some(Opportunity {
            seq,
            trade,
            pool_snapshot: None,
            snapshot_generation: cycle.snapshot_generation,
            estimated_edge_bps,
            created_at: Instant::now(),
            route_plan: Some(route_plan),
        })
    }

    fn cycle_signature(opportunity: &ArbitrageOpportunity) -> String {
        let mut signature = String::new();
        for (idx, leg) in opportunity.path.legs.iter().enumerate() {
            if idx > 0 {
                signature.push('>');
            }
            signature.push_str(&leg.pool_id);
        }
        signature
    }

    fn maybe_alert_cycles(&mut self, now: Instant, mut announced: Vec<ArbitrageOpportunity>) {
        if announced.is_empty() || now < self.next_cycle_alert {
            return;
        }

        announced.sort_by(|a, b| b.gain.partial_cmp(&a.gain).unwrap_or(CmpOrdering::Equal));
        if announced.len() > MAX_CYCLE_ALERTS_PER_BATCH {
            announced.truncate(MAX_CYCLE_ALERTS_PER_BATCH);
        }

        if let Err(err) = self.notifier.notify_arbitrage(&announced) {
            warn!(
                target = "fastlane",
                error = %err,
                "failed to dispatch arbitrage notification"
            );
        }

        self.next_cycle_alert = now + CYCLE_ALERT_INTERVAL;
    }

    fn enqueue_candidate(&mut self, opportunity: Opportunity) {
        let seq = opportunity.seq;
        let pool_id = opportunity.trade.pool_id.clone();
        let signature = opportunity.trade.signature.clone();
        let candidate = BundleCandidate::from_opportunity(&opportunity);
        match self.bundle_pool.try_acquire(candidate) {
            Some(lease) => {
                if let Err(lease) = self.bundle_producer.push(lease) {
                    warn!(
                        target = "fastlane",
                        seq,
                        pool = %pool_id,
                        signature = %signature,
                        "fast-lane bundle ring saturated; dropping candidate"
                    );
                    drop(lease);
                }
            }
            None => {
                warn!(
                    target = "fastlane",
                    seq,
                    pool = %pool_id,
                    signature = %signature,
                    "fast-lane bundle slab exhausted; dropping candidate"
                );
            }
        }
    }
}

struct CachedPool {
    generation: u64,
    snapshot: PoolSnapshot,
}

pub fn spawn_fast_lane_planner(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    snapshot_consumer: RingConsumer<HydrationSnapshot>,
    graph: GraphHandle,
    notifier: Arc<Notifier>,
    bundle_producer: RingProducer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
    config: PlannerConfig,
    bundle_pool: SlabPool<BundleCandidate>,
    trade_rx: Receiver<TradeUpdate>,
    pool_updates: Receiver<PoolStateUpdate>,
    pool_store: Arc<PoolStore>,
    readiness_timeout: Duration,
    min_ready_pools: usize,
) -> Result<thread::JoinHandle<Result<()>>> {
    let worker = FastLanePlanner::new(
        builder,
        config,
        graph,
        notifier,
        trade_rx,
        pool_updates,
        bundle_pool,
        bundle_producer,
        pool_store,
        readiness_timeout,
        min_ready_pools,
    );
    thread::Builder::new()
        .name("fast_lane_planner".into())
        .spawn(move || worker.run(stop, core_id, snapshot_consumer))
        .map_err(Into::into)
}

pub fn spawn_fast_lane_trade_feed(
    stop: Arc<AtomicBool>,
    config: Arc<AppConfig>,
    capacity: usize,
) -> Result<(Receiver<TradeUpdate>, thread::JoinHandle<Result<()>>)> {
    let channel_capacity = capacity.max(1);
    let (sender, receiver) = bounded(channel_capacity);
    let thread_name = "fast_lane_trade_feed".to_string();
    let handle = thread::Builder::new().name(thread_name).spawn(move || {
        let client = CoreCastClient::new(config.clone())
            .context("failed to initialize CoreCast client for fast-lane trade feed")?;
        let allowlist = Arc::new(config.token_pair_allowlist());
        let runtime = TokioRuntime::new_current_thread()
            .enable_all()
            .build()
            .context("failed to build fast-lane trade feed runtime")?;
        runtime.block_on(async move { pump_trade_stream(stop, client, sender, allowlist).await })
    })?;
    Ok((receiver, handle))
}

async fn pump_trade_stream(
    stop: Arc<AtomicBool>,
    client: CoreCastClient,
    sender: Sender<TradeUpdate>,
    allowlist: Arc<HashSet<TokenPair>>,
) -> Result<()> {
    let mut seq = 0u64;
    let mut retry = Duration::from_millis(250);
    let max_retry = Duration::from_secs(5);

    while !stop.load(Ordering::Relaxed) {
        match client.trade_stream().await {
            Ok(mut stream) => {
                info!(target = "fastlane", "CoreCast trade stream subscribed");
                retry = Duration::from_millis(250);
                while !stop.load(Ordering::Relaxed) {
                    match stream.message().await {
                        Ok(Some(message)) => {
                            if let Some(update) = TradeUpdate::from_corecast(seq, &message) {
                                seq = seq.wrapping_add(1);
                                if allowlist
                                    .contains(&TokenPair::new(&update.sell_mint, &update.buy_mint))
                                {
                                    if sender.try_send(update).is_err() {
                                        warn!(
                                            target = "fastlane",
                                            "trade channel saturated; dropping update"
                                        );
                                    }
                                }
                            } else {
                                debug!(target = "fastlane", "dropped malformed trade update");
                            }
                        }
                        Ok(None) => {
                            warn!(target = "fastlane", "trade stream closed by server");
                            break;
                        }
                        Err(status) => {
                            warn!(target = "fastlane", %status, "trade stream error");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                warn!(
                    target = "fastlane",
                    %err,
                    "failed to subscribe to trade stream"
                );
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
