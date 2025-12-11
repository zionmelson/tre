use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use crossbeam_channel::{Receiver, TryRecvError};
use tracing::trace;

use crate::core::pin_current_thread;
use crate::corecast::{PoolStateEvent, PoolStateUpdate, PoolStoreView};
use crate::watch::types::PoolSnapshot;
use crate::planner::graph_manager::{ArbitrageOpportunity, GraphManager, PoolPath};
use crate::planner::route_builder::RouteBuilder;
use crate::planner::PlannerConfig;

#[derive(Clone)]
pub struct GraphHandle {
    state: Arc<GraphState>,
}

struct GraphState {
    pool_store: PoolStoreView,
    graph: RwLock<GraphManager>,
    routes: RwLock<Vec<ArbitrageOpportunity>>,
    route_builder: RouteBuilder,
    cycle_notional: u64,
}

impl GraphState {
    fn new(pool_store: PoolStoreView, config: PlannerConfig) -> Self {
        let route_builder = RouteBuilder::new(pool_store.clone(), config.freshness_budget);
        Self {
            pool_store,
            graph: RwLock::new(GraphManager::new()),
            routes: RwLock::new(Vec::new()),
            route_builder,
            cycle_notional: config.cycle_notional_lamports,
        }
    }
}

struct GraphWorker {
    state: Arc<GraphState>,
    updates: Receiver<PoolStateUpdate>,
}

impl GraphWorker {
    fn run(self, stop: Arc<AtomicBool>, core_id: Option<usize>) -> Result<()> {
        if let Some(core) = core_id {
            let _ = pin_current_thread(core);
        }
        trace!(target = "graph", "graph service online");
        while !stop.load(Ordering::Relaxed) {
            let mut made_progress = false;
            loop {
                match self.updates.try_recv() {
                    Ok(update) => {
                        made_progress |= self.state.apply_update(update);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(()),
                }
            }
            if !made_progress {
                thread::yield_now();
                thread::sleep(Duration::from_millis(2));
            }
        }
        trace!(target = "graph", "graph service shutting down");
        Ok(())
    }
}

impl GraphState {
    fn apply_snapshot(&self, snapshot: PoolSnapshot) {
        let pool_id = snapshot.pool_id.clone();
        let cycles = if let Ok(mut graph) = self.graph.write() {
            graph.index_pool(&snapshot);
            graph.latest_profitable_cycles().to_vec()
        } else {
            Vec::new()
        };
        self.refresh_routes(cycles);
        trace!(target = "graph", pool = %pool_id, slot = snapshot.slot, "pool snapshot indexed");
    }

    fn remove_snapshot(&self, pool_id: &str) {
        let cycles = if let Ok(mut graph) = self.graph.write() {
            graph.remove_pool(pool_id);
            graph.latest_profitable_cycles().to_vec()
        } else {
            Vec::new()
        };
        self.refresh_routes(cycles);
        trace!(target = "graph", pool = %pool_id, "pool snapshot removed");
    }

    fn apply_update(&self, update: PoolStateUpdate) -> bool {
        let pool_id = update.pool_id.clone();
        match update.event {
            PoolStateEvent::Upsert(snapshot) => {
                if snapshot.freshness.is_stale() {
                    self.remove_snapshot(&pool_id);
                    return true;
                }
                self.apply_snapshot(snapshot);
                true
            }
            PoolStateEvent::Remove => {
                self.remove_snapshot(&pool_id);
                true
            }
        }
    }

    fn refresh_routes(&self, cycles: Vec<ArbitrageOpportunity>) {
        let mut planned = Vec::with_capacity(cycles.len());
        for mut cycle in cycles {
            if let Some(result) = self
                .route_builder
                .build_from_path(&cycle.path, self.cycle_notional)
            {
                cycle.route_plan = Some(result.route_plan);
                cycle.snapshot_generation = result.min_generation;
                cycle.latest_slot = Some(result.max_slot);
                planned.push(cycle);
            }
        }
        if let Ok(mut guard) = self.routes.write() {
            *guard = planned;
        }
    }

    fn latest_cycles(&self) -> Vec<ArbitrageOpportunity> {
        self.routes
            .read()
            .map(|cycles| cycles.clone())
            .unwrap_or_default()
    }
}

impl GraphHandle {
    pub fn latest_snapshot(&self, pool_id: &str) -> Option<PoolSnapshot> {
        self.state
            .pool_store
            .get(pool_id)
            .map(|state| state.snapshot.clone())
    }

    pub fn latest_cycles(&self) -> Vec<ArbitrageOpportunity> {
        self.state.latest_cycles()
    }

    pub fn find_paths(
        &self,
        start_mint: &str,
        target_mint: &str,
        max_hops: usize,
        max_results: usize,
    ) -> Vec<PoolPath> {
        self.with_graph(|graph| graph.find_paths(start_mint, target_mint, max_hops, max_results))
            .unwrap_or_default()
    }

    pub fn find_cycles_through(
        &self,
        anchor_mint: &str,
        max_hops: usize,
        max_results: usize,
    ) -> Vec<PoolPath> {
        self.with_graph(|graph| graph.find_cycles_through(anchor_mint, max_hops, max_results))
            .unwrap_or_default()
    }

    pub fn pools_for_pair(&self, mint_a: &str, mint_b: &str) -> Vec<String> {
        self.with_graph(|graph| graph.pools_for_pair(mint_a, mint_b))
            .unwrap_or_default()
    }

    fn with_graph<R>(&self, f: impl FnOnce(&GraphManager) -> R) -> Option<R> {
        self.state.graph.read().ok().map(|guard| f(&*guard))
    }
}

pub fn spawn_graph_service(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    updates: Receiver<PoolStateUpdate>,
    pool_store: PoolStoreView,
    planner_config: PlannerConfig,
) -> Result<(GraphHandle, thread::JoinHandle<Result<()>>)> {
    let state = Arc::new(GraphState::new(pool_store, planner_config));
    let handle = GraphHandle {
        state: state.clone(),
    };
    let worker = GraphWorker { state, updates };
    let join_handle = thread::Builder::new()
        .name("graph_owner".into())
        .spawn(move || worker.run(stop, core_id))?;
    Ok((handle, join_handle))
}
