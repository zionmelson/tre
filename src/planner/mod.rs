use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam_channel::{select, Receiver};
use tracing::{debug, info, warn};

use crate::core::{pin_current_thread, Lease, RingProducer, SlabPool};
use crate::corecast::{PoolState, PoolStore};
use crate::watch::types::{PoolUpdate, TradeUpdate};
use crate::planner::types::{BundleCandidate, Opportunity, RoutePlan};
use crate::telemetry;

pub mod graph;
pub mod graph_manager;
pub mod route_builder;
pub mod submit;
pub mod types;

pub use graph::{spawn_graph_service, GraphHandle};
pub use graph_manager::{ArbitrageOpportunity, GraphManager, PoolPath};
pub use submit::spawn_submit_stage;

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub detection_budget: Duration,
    pub freshness_budget: Duration,
    pub cycle_notional_lamports: u64,
    pub min_edge_bps: f64,
    pub max_leg_slippage_bps: f64,
    pub backpressure_threshold_pct: u8,
    pub max_corecast_slot_lag: u64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            detection_budget: Duration::from_millis(40),
            freshness_budget: Duration::from_millis(30),
            cycle_notional_lamports: 1_000_000,
            min_edge_bps: 5.0,
            max_leg_slippage_bps: 150.0,
            backpressure_threshold_pct: 85,
            max_corecast_slot_lag: 20,
        }
    }
}

pub fn spawn_fast_lane_planner(
    stop: Arc<AtomicBool>,
    config: PlannerConfig,
    trade_rx: Receiver<TradeUpdate>,
    pool_rx: Receiver<PoolUpdate>,
    bundle_producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
    pool_store: Arc<PoolStore>,
    graph: GraphHandle,
    core_id: Option<usize>,
) -> Result<thread::JoinHandle<Result<()>>> {
    let worker = FastLanePlanner::new(
        config,
        trade_rx,
        pool_rx,
        bundle_producer,
        bundle_pool,
        pool_store,
        graph,
    );
    thread::Builder::new()
        .name("fast_lane_planner".into())
        .spawn(move || worker.run(stop, core_id))
        .map_err(Into::into)
}

struct FastLanePlanner {
    config: PlannerConfig,
    trade_rx: Receiver<TradeUpdate>,
    pool_rx: Receiver<PoolUpdate>,
    bundle_producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
    pool_store: Arc<PoolStore>,
    graph: GraphHandle,
    ring_capacity: usize,
    last_backpressure_log: Option<Instant>,
    next_seq: u64,
    tip_slot: u64,
}

impl FastLanePlanner {
    fn new(
        config: PlannerConfig,
        trade_rx: Receiver<TradeUpdate>,
        pool_rx: Receiver<PoolUpdate>,
        bundle_producer: RingProducer<Lease<BundleCandidate>>,
        bundle_pool: SlabPool<BundleCandidate>,
        pool_store: Arc<PoolStore>,
        graph: GraphHandle,
    ) -> Self {
        let ring_capacity = bundle_producer.capacity();
        Self {
            config,
            trade_rx,
            pool_rx,
            bundle_producer,
            bundle_pool,
            pool_store,
            graph,
            ring_capacity,
            last_backpressure_log: None,
            next_seq: 0,
            tip_slot: 0,
        }
    }

    fn run(mut self, stop: Arc<AtomicBool>, core_id: Option<usize>) -> Result<()> {
        if let Some(core) = core_id {
            let _ = pin_current_thread(core);
        }
        info!(target = "planner", "fast-lane planner online");
        while !stop.load(Ordering::Relaxed) {
            let mut did_work = false;
            select! {
                recv(self.trade_rx) -> msg => {
                    if let Ok(trade) = msg {
                        self.handle_trade(trade);
                        did_work = true;
                    }
                },
                recv(self.pool_rx) -> msg => {
                    if let Ok(update) = msg {
                        self.handle_pool_update(update);
                        did_work = true;
                    }
                },
                default => {}
            }

            if !did_work {
                thread::sleep(Duration::from_millis(5));
            }
        }
        info!(target = "planner", "fast-lane planner shutting down");
        Ok(())
    }

    fn handle_trade(&mut self, trade: TradeUpdate) {
        if !self.pool_ready(&trade.pool_id) {
            debug!(target = "planner", pool = %trade.pool_id, "skipping trade; pool not ready");
            return;
        }

        let Some(state) = self.latest_state(&trade.pool_id) else {
            debug!(target = "planner", pool = %trade.pool_id, "no snapshot available for trade");
            return;
        };

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let mut opportunity = Opportunity::from_trade(
            seq,
            trade,
            Some(state.snapshot.clone()),
            Some(state.generation),
            None,
        );

        if let Some(route_plan) = self.pick_route(&opportunity.trade.pool_id) {
            opportunity.route_plan = Some(route_plan);
        }

        if !self.enforce_limits(&opportunity) {
            return;
        }

        self.enqueue_candidate(opportunity);
    }

    fn handle_pool_update(&mut self, update: PoolUpdate) {
        let pool_id = update.pool_id.clone();
        let slot = update.slot;
        self.tip_slot = self.tip_slot.max(slot);
        let snapshot = update.into_snapshot();
        self.pool_store.update(snapshot);
        debug!(target = "planner", pool = %pool_id, slot, "pool snapshot indexed");
    }

    fn pool_ready(&self, pool_id: &str) -> bool {
        let Some(state) = self.pool_store.latest(pool_id) else {
            return false;
        };

        if self.config.max_corecast_slot_lag == 0 {
            return true;
        }

        let tip_slot = self.tip_slot;
        if tip_slot == 0 {
            return true;
        }

        let lag = tip_slot.saturating_sub(state.slot);
        telemetry::record_corecast_delta_lag(state.snapshot.updated_at.elapsed());
        lag <= self.config.max_corecast_slot_lag
    }

    fn latest_state(&self, pool_id: &str) -> Option<PoolState> {
        self.pool_store.latest(pool_id)
    }

    fn pick_route(&self, pool_id: &str) -> Option<RoutePlan> {
        let mut best: Option<(RoutePlan, f64)> = None;
        for cycle in self.graph.latest_cycles() {
            if !cycle.path.legs.iter().any(|leg| leg.pool_id == pool_id) {
                continue;
            }
            let Some(plan) = cycle.route_plan.clone() else {
                continue;
            };
            if let Some((_, gain)) = &best {
                if cycle.gain <= *gain {
                    continue;
                }
            }
            best = Some((plan, cycle.gain));
        }
        best.map(|(plan, _)| plan)
    }

    fn enforce_limits(&self, opportunity: &Opportunity) -> bool {
        let pool = &opportunity.trade.pool_id;
        if opportunity.estimated_edge_bps < self.config.min_edge_bps {
            debug!(target = "planner", pool = %pool, edge_bps = opportunity.estimated_edge_bps, "edge below planner threshold");
            return false;
        }

        if let Some(snapshot) = opportunity.pool_snapshot.as_ref() {
            if snapshot.updated_at.elapsed() > self.config.detection_budget {
                debug!(target = "planner", pool = %pool, "snapshot outside detection budget");
                return false;
            }
        }

        if let Some(plan) = opportunity.route_plan.as_ref() {
            if plan
                .legs
                .iter()
                .any(|leg| leg.estimated_slippage_bps > self.config.max_leg_slippage_bps)
            {
                debug!(target = "planner", pool = %pool, "route plan leg exceeds slippage guard");
                return false;
            }
        }

        true
    }

    fn enqueue_candidate(&mut self, opportunity: Opportunity) {
        if self.is_ring_overloaded() {
            warn!(
                target = "planner",
                pool = %opportunity.trade.pool_id,
                "bundle ring saturated; candidate dropped"
            );
            return;
        }

        let candidate = BundleCandidate::from_opportunity(&opportunity);
        if let Some(lease) = self.bundle_pool.try_acquire(candidate) {
            if let Err(lease) = self.bundle_producer.push(lease) {
                warn!(
                    target = "planner",
                    "bundle ring rejected push; dropping candidate"
                );
                drop(lease);
            }
        } else {
            warn!(
                target = "planner",
                "bundle slab exhausted; dropping candidate"
            );
        }
    }

    fn is_ring_overloaded(&mut self) -> bool {
        let pct = self.config.backpressure_threshold_pct.max(1) as usize;
        let threshold = ((self.ring_capacity * pct) / 100).max(1);
        let depth = self.bundle_producer.len();
        if depth >= threshold {
            let now = Instant::now();
            let should_log = self
                .last_backpressure_log
                .map(|last| now.duration_since(last) >= Duration::from_millis(250))
                .unwrap_or(true);
            if should_log {
                warn!(
                    target = "planner",
                    depth,
                    capacity = self.ring_capacity,
                    threshold,
                    "planner ring near capacity; applying backpressure"
                );
                self.last_backpressure_log = Some(now);
            }
            true
        } else {
            false
        }
    }
}
