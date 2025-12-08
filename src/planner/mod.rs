use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam_channel::{select, Receiver};
use tracing::{debug, info, warn};

use crate::core::{pin_current_thread, Lease, RingProducer, SlabPool};
use crate::hydration::pool_state::PoolState;
use crate::hydration::readiness::{PoolReadinessState, ReadinessEvent, ReadinessRegistry};
use crate::hydration::types::{HydrationComponent, PoolUpdate, TradeUpdate};
use crate::hydration::PoolStore;
use crate::planner::types::{BundleCandidate, Opportunity, RoutePlan};

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
}

#[derive(Debug, Clone, Copy)]
struct ReadinessDiagnostics {
    tracked: bool,
    hydrated: bool,
    hydration_fresh: bool,
    template_ready: bool,
    template_fresh: bool,
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
        }
    }
}

pub fn spawn_fast_lane_planner(
    stop: Arc<AtomicBool>,
    config: PlannerConfig,
    readiness: ReadinessRegistry,
    trade_rx: Receiver<TradeUpdate>,
    pool_rx: Receiver<PoolUpdate>,
    bundle_producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
    pool_store: Arc<PoolStore>,
    graph: GraphHandle,
    core_id: Option<usize>,
) -> Result<thread::JoinHandle<Result<()>>> {
    let readiness_rx = readiness.subscribe();
    let worker = FastLanePlanner::new(
        config,
        readiness,
        readiness_rx,
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
    readiness: ReadinessRegistry,
    readiness_rx: Receiver<ReadinessEvent>,
    trade_rx: Receiver<TradeUpdate>,
    pool_rx: Receiver<PoolUpdate>,
    bundle_producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
    pool_store: Arc<PoolStore>,
    graph: GraphHandle,
    readiness_cache: HashMap<String, PoolReadinessState>,
    ring_capacity: usize,
    last_backpressure_log: Option<Instant>,
    next_seq: u64,
}

impl FastLanePlanner {
    fn new(
        config: PlannerConfig,
        readiness: ReadinessRegistry,
        readiness_rx: Receiver<ReadinessEvent>,
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
            readiness,
            readiness_rx,
            trade_rx,
            pool_rx,
            bundle_producer,
            bundle_pool,
            pool_store,
            graph,
            readiness_cache: HashMap::new(),
            ring_capacity,
            last_backpressure_log: None,
            next_seq: 0,
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
                recv(self.readiness_rx) -> msg => {
                    if let Ok(event) = msg {
                        self.handle_readiness_event(event);
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
        let readiness = self.readiness_cache.get(&trade.pool_id).cloned();
        let mut opportunity = Opportunity::from_trade(
            seq,
            trade,
            Some(state.snapshot.clone()),
            Some(state.generation),
            None,
            readiness.clone(),
        );

        if let Some(route_plan) = self.pick_route(&opportunity.trade.pool_id) {
            opportunity.route_plan = Some(route_plan);
        }

        if !self.enforce_limits(&opportunity) {
            return;
        }

        self.enqueue_candidate(opportunity);
    }

    fn handle_pool_update(&self, update: PoolUpdate) {
        let pool_id = update.pool_id.clone();
        let slot = update.slot;
        let ready = self.pool_ready(&pool_id);
        let snapshot = update.into_snapshot();
        self.pool_store.update(snapshot, ready);
        debug!(target = "planner", pool = %pool_id, slot, ready, "pool snapshot indexed");
    }

    fn handle_readiness_event(&mut self, event: ReadinessEvent) {
        match event {
            ReadinessEvent::PoolDiscovered { pool_id } => {
                info!(target = "planner", pool = %pool_id, "pool discovered via readiness registry");
                if let Some(state) = self.readiness.state_for_pool(&pool_id) {
                    self.readiness_cache.insert(pool_id.clone(), state);
                }
            }
            ReadinessEvent::PoolUpdated { pool_id, slot } => {
                debug!(target = "planner", pool = %pool_id, slot, "pool updated");
            }
            ReadinessEvent::HydrationTransition {
                pool_id,
                ready,
                hydrated_slot,
                ..
            } => {
                if ready {
                    debug!(target = "planner", pool = %pool_id, hydrated_slot, "hydration ready");
                } else {
                    warn!(target = "planner", pool = %pool_id, "hydration expired; pool warming");
                }
            }
            ReadinessEvent::TemplateTransition {
                pool_id,
                ready,
                template_epoch,
                ..
            } => {
                if ready {
                    debug!(
                        target = "planner",
                        pool = %pool_id,
                        template_epoch,
                        "template ready"
                    );
                } else {
                    let latest_state = self.latest_state(&pool_id);
                    let corecast_slot = latest_state.as_ref().map(|state| state.slot);
                    let hydrated_meta_slot = latest_state
                        .as_ref()
                        .and_then(|state| {
                            state
                                .snapshot
                                .hydration
                                .component_slot(HydrationComponent::Meta)
                        });
                    let readiness_epoch = self
                        .readiness_cache
                        .get(&pool_id)
                        .and_then(|state| state.template_epoch);
                    warn!(
                        target = "planner",
                        pool = %pool_id,
                        corecast_slot,
                        hydrated_meta_slot,
                        template_epoch,
                        readiness_epoch,
                        "template expired; awaiting refresh"
                    );
                }
            }
            ReadinessEvent::ReadinessChanged { pool_id, state } => {
                let ready = self.state_ready(&state);
                self.readiness_cache.insert(pool_id.clone(), state.clone());
                if ready {
                    info!(target = "planner", pool = %pool_id, "pool ready");
                } else {
                    let diag = Self::readiness_diagnostics(&state);
                    warn!(
                        target = "planner",
                        pool = %pool_id,
                        tracked = diag.tracked,
                        hydrated = diag.hydrated,
                        hydration_fresh = diag.hydration_fresh,
                        hydration_deadline = ?state.hydration_ready_until,
                        template_ready = diag.template_ready,
                        template_fresh = diag.template_fresh,
                        template_deadline = ?state.template_ready_until,
                        last_update_slot = state.last_update_slot,
                        hydrated_slot = state.hydrated_slot,
                        template_hash = ?state.template_hash,
                        "pool left ready state; readiness breakdown"
                    );
                }
            }
        }
    }

    fn pool_ready(&self, pool_id: &str) -> bool {
        if let Some(state) = self.readiness_cache.get(pool_id) {
            return self.state_ready(state);
        }
        self.readiness
            .state_for_pool(pool_id)
            .map(|state| self.state_ready(&state))
            .unwrap_or(false)
    }

    fn state_ready(&self, state: &PoolReadinessState) -> bool {
        state.is_ready()
    }

    fn readiness_diagnostics(state: &PoolReadinessState) -> ReadinessDiagnostics {
        let now = Instant::now();
        ReadinessDiagnostics {
            tracked: state.tracked,
            hydrated: state.hydrated,
            hydration_fresh: state.hydrated
                && state
                    .hydration_ready_until
                    .map(|deadline| deadline >= now)
                    .unwrap_or(true),
            template_ready: state.template_ready,
            template_fresh: state.template_ready
                && state
                    .template_ready_until
                    .map(|deadline| deadline >= now)
                    .unwrap_or(true),
        }
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

        if let Some(readiness) = opportunity.readiness.as_ref() {
            if readiness.last_transition.elapsed() > self.config.freshness_budget {
                debug!(target = "planner", pool = %pool, "readiness stale; waiting for refresh");
                return false;
            }
        }

        if let Some(plan) = opportunity.route_plan.as_ref() {
            if !self.route_plan_ready(plan) {
                debug!(target = "planner", pool = %pool, "route plan missing template readiness");
                return false;
            }
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

    fn route_plan_ready(&self, plan: &RoutePlan) -> bool {
        plan.legs.iter().all(|leg| leg.template_ready)
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
