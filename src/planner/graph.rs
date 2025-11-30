use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use dashmap::DashMap;
use tracing::trace;

use crate::core::{pin_current_thread, RingConsumer};
use crate::hydration::types::PoolSnapshot;
use crate::planner::graph_manager::{ArbitrageOpportunity, GraphManager, PoolPath};
use crate::telemetry;

#[derive(Clone)]
pub struct GraphHandle {
    state: Arc<GraphState>,
}

struct GraphState {
    pools: DashMap<String, PoolSnapshot>,
    graph: RwLock<GraphManager>,
}

impl Default for GraphState {
    fn default() -> Self {
        Self {
            pools: DashMap::new(),
            graph: RwLock::new(GraphManager::new()),
        }
    }
}

struct GraphWorker {
    state: Arc<GraphState>,
    pool_consumer: RingConsumer<PoolSnapshot>,
}

impl GraphWorker {
    fn run(self, stop: Arc<AtomicBool>, core_id: Option<usize>) -> Result<()> {
        if let Some(core) = core_id {
            let _ = pin_current_thread(core);
        }
        trace!(target = "graph", "graph service online");
        while !stop.load(Ordering::Relaxed) {
            let mut made_progress = false;
            while let Some(snapshot) = self.pool_consumer.pop() {
                self.state.apply_snapshot(snapshot);
                made_progress = true;
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
        let latency = snapshot.updated_at.elapsed();
        self.pools.insert(pool_id.clone(), snapshot.clone());
        telemetry::record_graph_pool_count(self.pools.len());
        telemetry::record_graph_snapshot_latency(latency);
        if let Ok(mut graph) = self.graph.write() {
            graph.index_pool(&snapshot);
            telemetry::record_graph_cycle_count(graph.latest_profitable_cycles().len());
        }
        trace!(target = "graph", pool = %pool_id, slot = snapshot.slot, "pool snapshot indexed");
    }
}

impl GraphHandle {
    pub fn latest_snapshot(&self, pool_id: &str) -> Option<PoolSnapshot> {
        self.state
            .pools
            .get(pool_id)
            .map(|entry| entry.value().clone())
    }

    pub fn latest_cycles(&self) -> Vec<ArbitrageOpportunity> {
        self.with_graph(|graph| graph.latest_profitable_cycles().to_vec())
            .unwrap_or_default()
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
    pool_consumer: RingConsumer<PoolSnapshot>,
) -> Result<(GraphHandle, thread::JoinHandle<Result<()>>)> {
    let state = Arc::new(GraphState::default());
    let handle = GraphHandle {
        state: state.clone(),
    };
    let worker = GraphWorker {
        state,
        pool_consumer,
    };
    let join_handle = thread::Builder::new()
        .name("graph_owner".into())
        .spawn(move || worker.run(stop, core_id))?;
    Ok((handle, join_handle))
}
