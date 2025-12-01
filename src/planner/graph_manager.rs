use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

const NEGATIVE_CYCLE_EPSILON: f64 = 1e-12;

use crate::hydration::types::PoolSnapshot;
use crate::planner::types::RoutePlan;
use crate::token_pair::TokenPair;

/// Represents a concrete swap leg along a path through the liquidity graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathLeg {
    pub pool_id: String,
    pub program_id: String,
    pub input_mint: String,
    pub output_mint: String,
}

/// Describes a full swap route (potentially multi-leg) between two mints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolPath {
    pub start_mint: String,
    pub end_mint: String,
    pub legs: Vec<PathLeg>,
}

impl PoolPath {
    pub fn len(&self) -> usize {
        self.legs.len()
    }
}

#[derive(Clone, Debug)]
struct RankedPath {
    ratio: f64,
    hop_count: usize,
    path: PoolPath,
}

impl PartialEq for RankedPath {
    fn eq(&self, other: &Self) -> bool {
        self.ratio.to_bits() == other.ratio.to_bits()
            && self.hop_count == other.hop_count
            && self.path == other.path
    }
}

impl Eq for RankedPath {}

impl PartialOrd for RankedPath {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedPath {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.ratio.total_cmp(&other.ratio) {
            Ordering::Equal => other.hop_count.cmp(&self.hop_count),
            other => other,
        }
    }
}

/// Captures a detected negative-weight cycle that can yield a profitable swap loop.
#[derive(Clone, Debug, PartialEq)]
pub struct ArbitrageOpportunity {
    pub path: PoolPath,
    pub gain: f64,
    pub route_plan: Option<RoutePlan>,
    pub snapshot_generation: Option<u64>,
    pub latest_slot: Option<u64>,
}

#[derive(Debug, Default)]
pub struct GraphManager {
    pools: HashMap<String, PoolSummary>,
    adjacency: HashMap<String, Vec<GraphEdge>>, // token -> ordered neighbors
    pair_index: HashMap<TokenPair, Vec<String>>, // unordered pair -> pool ids
    profitable_cycles: Vec<ArbitrageOpportunity>,
}

impl GraphManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn index_pool(&mut self, snapshot: &PoolSnapshot) {
        let summary = PoolSummary::from_snapshot(snapshot);
        let pool_id = snapshot.pool_id.clone();
        let requires_rewire = self
            .pools
            .get(&pool_id)
            .map(|existing| {
                existing.base_mint != summary.base_mint || existing.quote_mint != summary.quote_mint
            })
            .unwrap_or(true);

        if requires_rewire {
            self.remove_pool(&pool_id);
        }

        self.attach_edges(&pool_id, &summary);
        self.index_pair_membership(&pool_id, &summary);
        let affected_tokens = vec![summary.base_mint.clone(), summary.quote_mint.clone()];
        self.pools.insert(pool_id, summary);
        self.refresh_profitable_cycles(&affected_tokens);
    }

    pub fn remove_pool(&mut self, pool_id: &str) -> bool {
        let summary = match self.pools.remove(pool_id) {
            Some(summary) => summary,
            None => return false,
        };

        self.detach_edge(&summary.base_mint, pool_id);
        self.detach_edge(&summary.quote_mint, pool_id);
        let key = TokenPair::new(&summary.base_mint, &summary.quote_mint);
        if let Some(pools) = self.pair_index.get_mut(&key) {
            pools.retain(|entry| entry != pool_id);
            if pools.is_empty() {
                self.pair_index.remove(&key);
            }
        }
        let affected_tokens = vec![summary.base_mint, summary.quote_mint];
        self.refresh_profitable_cycles(&affected_tokens);
        true
    }

    pub fn pools_for_pair(&self, mint_a: &str, mint_b: &str) -> Vec<String> {
        let key = TokenPair::new(mint_a, mint_b);
        self.pair_index.get(&key).cloned().unwrap_or_default()
    }

    pub fn find_paths(
        &self,
        start_mint: &str,
        target_mint: &str,
        max_hops: usize,
        max_results: usize,
    ) -> Vec<PoolPath> {
        if max_hops == 0 || max_results == 0 || start_mint == target_mint {
            return Vec::new();
        }
        let mut best_paths: BinaryHeap<Reverse<RankedPath>> = BinaryHeap::new();
        let mut queue = VecDeque::new();
        queue.push_back(SearchState::new(start_mint));

        while let Some(state) = queue.pop_front() {
            if state.current_mint == target_mint && !state.legs.is_empty() {
                let path = PoolPath {
                    start_mint: start_mint.to_string(),
                    end_mint: target_mint.to_string(),
                    legs: state.legs.clone(),
                };
                let ratio = self.path_price_ratio(&path).unwrap_or(1.0);
                best_paths.push(Reverse(RankedPath {
                    ratio,
                    hop_count: path.len(),
                    path,
                }));
                if best_paths.len() > max_results {
                    best_paths.pop();
                }
                continue;
            }

            if state.legs.len() == max_hops {
                continue;
            }

            let edges = match self.adjacency.get(&state.current_mint) {
                Some(edges) => edges,
                None => continue,
            };

            for edge in edges {
                if state.visited.contains(&edge.neighbor_mint) {
                    continue;
                }
                let mut next = state.clone();
                next.visited.insert(edge.neighbor_mint.clone());
                next.legs.push(PathLeg {
                    pool_id: edge.pool_id.clone(),
                    program_id: edge.program_id.clone(),
                    input_mint: state.current_mint.clone(),
                    output_mint: edge.neighbor_mint.clone(),
                });
                next.current_mint = edge.neighbor_mint.clone();
                queue.push_back(next);
            }
        }

        let mut ordered = Vec::with_capacity(best_paths.len());
        while let Some(Reverse(entry)) = best_paths.pop() {
            ordered.push(entry.path);
        }
        ordered.reverse();
        ordered
    }

    pub fn find_cycles_through(
        &self,
        anchor_mint: &str,
        max_hops: usize,
        max_results: usize,
    ) -> Vec<PoolPath> {
        if max_hops < 2 || max_results == 0 || !self.token_known(anchor_mint) {
            return Vec::new();
        }

        let anchor = anchor_mint.to_string();
        let mut best_paths: BinaryHeap<Reverse<RankedPath>> = BinaryHeap::new();
        let mut queue = VecDeque::new();
        queue.push_back(SearchState::new(anchor_mint));

        while let Some(state) = queue.pop_front() {
            if state.current_mint == anchor && !state.legs.is_empty() {
                let path = PoolPath {
                    start_mint: anchor.clone(),
                    end_mint: anchor.clone(),
                    legs: state.legs.clone(),
                };
                if path.len() > max_hops {
                    continue;
                }
                if let Some(ratio) = self.path_price_ratio(&path) {
                    if ratio <= 1.0 {
                        continue;
                    }
                    best_paths.push(Reverse(RankedPath {
                        ratio,
                        hop_count: path.len(),
                        path,
                    }));
                    if best_paths.len() > max_results {
                        best_paths.pop();
                    }
                }
                continue;
            }

            if state.legs.len() == max_hops {
                continue;
            }

            let edges = match self.adjacency.get(&state.current_mint) {
                Some(edges) => edges,
                None => continue,
            };

            for edge in edges {
                let mut next = state.clone();
                let neighbor = edge.neighbor_mint.clone();
                let is_return_leg = !next.legs.is_empty() && neighbor == anchor;
                if next.legs.len() + 1 > max_hops {
                    continue;
                }
                if !is_return_leg && next.visited.contains(&neighbor) {
                    continue;
                }
                if !is_return_leg {
                    next.visited.insert(neighbor.clone());
                }
                next.legs.push(PathLeg {
                    pool_id: edge.pool_id.clone(),
                    program_id: edge.program_id.clone(),
                    input_mint: next.current_mint.clone(),
                    output_mint: neighbor.clone(),
                });
                next.current_mint = neighbor;
                queue.push_back(next);
            }
        }

        let mut ordered = Vec::with_capacity(best_paths.len());
        while let Some(Reverse(entry)) = best_paths.pop() {
            ordered.push(entry.path);
        }
        ordered.reverse();
        ordered
    }

    /// Returns the multiplicative price ratio (product of spot prices) for a path, if prices are known.
    pub fn path_price_ratio(&self, path: &PoolPath) -> Option<f64> {
        let mut ratio = 1.0_f64;
        for leg in &path.legs {
            let summary = self.pools.get(&leg.pool_id)?;
            let leg_ratio = summary.price_ratio(&leg.input_mint, &leg.output_mint)?;
            ratio *= leg_ratio;
        }
        Some(ratio)
    }

    /// Forces a re-computation of profitable cycles for the component containing the provided seeds.
    pub fn refresh_profitable_cycles(&mut self, seeds: &[String]) -> &[ArbitrageOpportunity] {
        self.recompute_profitable_cycles(seeds);
        self.latest_profitable_cycles()
    }

    pub fn latest_profitable_cycles(&self) -> &[ArbitrageOpportunity] {
        &self.profitable_cycles
    }

    fn attach_edges(&mut self, pool_id: &str, summary: &PoolSummary) {
        let (forward_weight, has_forward_price) = summary
            .price
            .forward_weight
            .map(|w| (w, true))
            .unwrap_or((0.0, false));
        let forward = GraphEdge::new(
            pool_id,
            &summary.program_id,
            &summary.quote_mint,
            summary.liquidity_score,
            forward_weight,
            has_forward_price,
        );
        self.upsert_edge(&summary.base_mint, forward);

        let (reverse_weight, has_reverse_price) = summary
            .price
            .reverse_weight
            .map(|w| (w, true))
            .unwrap_or((0.0, false));
        let reverse = GraphEdge::new(
            pool_id,
            &summary.program_id,
            &summary.base_mint,
            summary.liquidity_score,
            reverse_weight,
            has_reverse_price,
        );
        self.upsert_edge(&summary.quote_mint, reverse);
    }

    fn upsert_edge(&mut self, token: &str, edge: GraphEdge) {
        let edges = self.adjacency.entry(token.to_string()).or_default();
        if let Some(pos) = edges.iter().position(|entry| entry.pool_id == edge.pool_id) {
            edges.remove(pos);
        }
        let insert_pos = edges
            .binary_search_by(|probe| edge.liquidity_score.cmp(&probe.liquidity_score))
            .unwrap_or_else(|idx| idx);
        edges.insert(insert_pos, edge);
    }

    fn detach_edge(&mut self, token: &str, pool_id: &str) {
        if let Some(edges) = self.adjacency.get_mut(token) {
            edges.retain(|edge| edge.pool_id != pool_id);
            if edges.is_empty() {
                self.adjacency.remove(token);
            }
        }
    }

    fn index_pair_membership(&mut self, pool_id: &str, summary: &PoolSummary) {
        let key = TokenPair::new(&summary.base_mint, &summary.quote_mint);
        let pools = self.pair_index.entry(key).or_default();
        match pools.binary_search_by(|existing| existing.as_str().cmp(pool_id)) {
            Ok(_) => {}
            Err(pos) => pools.insert(pos, pool_id.to_string()),
        }
    }

    fn recompute_profitable_cycles(&mut self, seeds: &[String]) {
        if self.adjacency.is_empty() {
            self.profitable_cycles.clear();
            return;
        }

        let component = self.collect_component(seeds);
        if component.is_empty() {
            self.profitable_cycles.clear();
            return;
        }
        self.profitable_cycles = self.detect_negative_cycles(&component);
    }

    fn collect_component(&self, seeds: &[String]) -> HashSet<String> {
        if seeds.is_empty() {
            let mut all = HashSet::new();
            for (mint, edges) in &self.adjacency {
                all.insert(mint.clone());
                for edge in edges {
                    all.insert(edge.neighbor_mint.clone());
                }
            }
            return all;
        }

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        for seed in seeds {
            if visited.contains(seed) {
                continue;
            }
            if !self.token_known(seed) {
                continue;
            }
            visited.insert(seed.clone());
            queue.push_back(seed.clone());
        }

        while let Some(token) = queue.pop_front() {
            if let Some(edges) = self.adjacency.get(&token) {
                for edge in edges {
                    if visited.insert(edge.neighbor_mint.clone()) {
                        queue.push_back(edge.neighbor_mint.clone());
                    }
                }
            }
        }

        visited
    }

    fn token_known(&self, mint: &str) -> bool {
        if self.adjacency.contains_key(mint) {
            return true;
        }
        self.adjacency
            .values()
            .any(|edges| edges.iter().any(|edge| edge.neighbor_mint == mint))
    }

    fn detect_negative_cycles(&self, component: &HashSet<String>) -> Vec<ArbitrageOpportunity> {
        if component.is_empty() {
            return Vec::new();
        }

        let mut tokens: Vec<&String> = component.iter().collect();
        tokens.sort();
        let mut index_map = HashMap::new();
        for (idx, mint) in tokens.iter().enumerate() {
            index_map.insert((*mint).clone(), idx);
        }

        let mut weighted_edges = Vec::new();
        for mint in &tokens {
            if let Some(edges) = self.adjacency.get(*mint) {
                for edge in edges {
                    if !edge.has_price {
                        continue;
                    }
                    if !component.contains(&edge.neighbor_mint) {
                        continue;
                    }
                    let from = *index_map.get(mint.as_str()).expect("source index exists");
                    let to = *index_map
                        .get(edge.neighbor_mint.as_str())
                        .expect("target index exists");
                    weighted_edges.push(WeightedEdgeCtx {
                        from,
                        to,
                        weight: edge.negative_weight,
                        leg: Some(PathLeg {
                            pool_id: edge.pool_id.clone(),
                            program_id: edge.program_id.clone(),
                            input_mint: (*mint).clone(),
                            output_mint: edge.neighbor_mint.clone(),
                        }),
                    });
                }
            }
        }

        if weighted_edges.is_empty() {
            return Vec::new();
        }

        let super_index = tokens.len();
        for idx in 0..tokens.len() {
            weighted_edges.push(WeightedEdgeCtx {
                from: super_index,
                to: idx,
                weight: 0.0,
                leg: None,
            });
        }

        let vertex_count = tokens.len() + 1;
        let mut distances = vec![0.0_f64; vertex_count];
        let mut predecessors: Vec<Option<EdgeTrace>> = vec![None; vertex_count];

        for _ in 0..vertex_count - 1 {
            let mut updated = false;
            for edge in &weighted_edges {
                if distances[edge.from] + edge.weight + NEGATIVE_CYCLE_EPSILON < distances[edge.to]
                {
                    distances[edge.to] = distances[edge.from] + edge.weight;
                    if let Some(leg) = &edge.leg {
                        predecessors[edge.to] = Some(EdgeTrace {
                            from: edge.from,
                            weight: edge.weight,
                            leg: leg.clone(),
                        });
                    }
                    updated = true;
                }
            }
            if !updated {
                break;
            }
        }

        let mut cycle_nodes = Vec::new();
        for edge in &weighted_edges {
            if edge.to == super_index {
                continue;
            }
            if distances[edge.from] + edge.weight + NEGATIVE_CYCLE_EPSILON < distances[edge.to] {
                cycle_nodes.push(edge.to);
            }
        }
        cycle_nodes.sort_unstable();
        cycle_nodes.dedup();

        let mut signatures = HashSet::new();
        let mut opportunities = Vec::new();
        for node in cycle_nodes {
            if let Some(edges) = Self::extract_cycle_edges(node, &predecessors) {
                let legs: Vec<PathLeg> = edges.iter().map(|trace| trace.leg.clone()).collect();
                if legs.is_empty() {
                    continue;
                }
                let signature = Self::cycle_signature(&legs);
                if !signatures.insert(signature) {
                    continue;
                }
                let total_weight: f64 = edges.iter().map(|trace| trace.weight).sum();
                let start_mint = legs
                    .first()
                    .map(|leg| leg.input_mint.clone())
                    .unwrap_or_default();
                let candidate_path = PoolPath {
                    start_mint: start_mint.clone(),
                    end_mint: start_mint,
                    legs,
                };
                let gain = self
                    .path_price_ratio(&candidate_path)
                    .unwrap_or_else(|| (-total_weight).exp());
                if gain <= 1.0 + 1e-6 {
                    continue;
                }
                opportunities.push(ArbitrageOpportunity {
                    path: candidate_path,
                    gain,
                    route_plan: None,
                    snapshot_generation: None,
                    latest_slot: None,
                });
            }
        }

        opportunities
    }

    fn extract_cycle_edges(
        mut node: usize,
        predecessors: &[Option<EdgeTrace>],
    ) -> Option<Vec<EdgeTrace>> {
        if node >= predecessors.len() {
            return None;
        }

        for _ in 0..predecessors.len() {
            let trace = predecessors.get(node)?.as_ref()?;
            node = trace.from;
        }
        let cycle_start = node;
        let mut cycle = Vec::new();
        let mut current = cycle_start;
        loop {
            let trace = predecessors.get(current)?.as_ref()?.clone();
            cycle.push(trace.clone());
            current = trace.from;
            if current == cycle_start {
                break;
            }
        }
        cycle.reverse();
        Some(cycle)
    }

    fn cycle_signature(legs: &[PathLeg]) -> String {
        let mut ids: Vec<String> = legs.iter().map(|leg| leg.pool_id.clone()).collect();
        ids.sort();
        ids.join("->")
    }
}

#[derive(Clone, Debug)]
struct GraphEdge {
    pool_id: String,
    program_id: String,
    neighbor_mint: String,
    liquidity_score: u128,
    negative_weight: f64,
    has_price: bool,
}

impl GraphEdge {
    fn new(
        pool_id: &str,
        program_id: &str,
        neighbor_mint: &str,
        liquidity_score: u128,
        negative_weight: f64,
        has_price: bool,
    ) -> Self {
        Self {
            pool_id: pool_id.to_string(),
            program_id: program_id.to_string(),
            neighbor_mint: neighbor_mint.to_string(),
            liquidity_score,
            negative_weight,
            has_price,
        }
    }
}

#[derive(Clone, Debug)]
struct PoolSummary {
    base_mint: String,
    quote_mint: String,
    program_id: String,
    liquidity_score: u128,
    price: PriceMetrics,
}

impl PoolSummary {
    fn from_snapshot(snapshot: &PoolSnapshot) -> Self {
        Self {
            base_mint: snapshot.base_mint.clone(),
            quote_mint: snapshot.quote_mint.clone(),
            program_id: snapshot.program_id.clone(),
            liquidity_score: snapshot
                .base_post_amount
                .saturating_add(snapshot.quote_post_amount),
            price: PriceMetrics::new(snapshot.base_post_amount, snapshot.quote_post_amount),
        }
    }

    fn price_ratio(&self, input_mint: &str, output_mint: &str) -> Option<f64> {
        if input_mint == self.base_mint && output_mint == self.quote_mint {
            self.price.forward_price
        } else if input_mint == self.quote_mint && output_mint == self.base_mint {
            self.price.reverse_price
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Default)]
struct PriceMetrics {
    forward_price: Option<f64>,
    reverse_price: Option<f64>,
    forward_weight: Option<f64>,
    reverse_weight: Option<f64>,
}

impl PriceMetrics {
    fn new(base_reserve: u128, quote_reserve: u128) -> Self {
        if base_reserve == 0 || quote_reserve == 0 {
            return Self::default();
        }

        let base = base_reserve as f64;
        let quote = quote_reserve as f64;
        let forward_price = quote / base;
        let reverse_price = base / quote;
        Self {
            forward_price: Some(forward_price),
            reverse_price: Some(reverse_price),
            forward_weight: Some(-forward_price.ln()),
            reverse_weight: Some(-reverse_price.ln()),
        }
    }
}

#[derive(Clone)]
struct SearchState {
    current_mint: String,
    visited: HashSet<String>,
    legs: Vec<PathLeg>,
}

impl SearchState {
    fn new(start: &str) -> Self {
        let mut visited = HashSet::new();
        visited.insert(start.to_string());
        Self {
            current_mint: start.to_string(),
            visited,
            legs: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct EdgeTrace {
    from: usize,
    weight: f64,
    leg: PathLeg,
}

struct WeightedEdgeCtx {
    from: usize,
    to: usize,
    weight: f64,
    leg: Option<PathLeg>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hydration::types::PoolUpdateExtensions;
    use std::time::Instant;

    fn snapshot(
        pool_id: &str,
        program_id: &str,
        base: &str,
        quote: &str,
        liquidity: u128,
    ) -> PoolSnapshot {
        snapshot_with_reserves(pool_id, program_id, base, quote, liquidity, liquidity)
    }

    fn snapshot_with_reserves(
        pool_id: &str,
        program_id: &str,
        base: &str,
        quote: &str,
        base_liquidity: u128,
        quote_liquidity: u128,
    ) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: pool_id.to_string(),
            program_id: program_id.to_string(),
            slot: 0,
            block_hash: None,
            block_time_unix: None,
            base_mint: base.to_string(),
            quote_mint: quote.to_string(),
            base_post_amount: base_liquidity,
            quote_post_amount: quote_liquidity,
            updated_at: Instant::now(),
            quotes: None,
            extensions: PoolUpdateExtensions::default(),
        }
    }

    #[test]
    fn indexes_pools_by_pair() {
        let mut graph = GraphManager::new();
        graph.index_pool(&snapshot("pool-1", "program", "A", "B", 10));
        graph.index_pool(&snapshot("pool-2", "program", "B", "A", 20));

        let mut pools = graph.pools_for_pair("A", "B");
        pools.sort();
        assert_eq!(pools, vec!["pool-1", "pool-2"]);
    }

    #[test]
    fn finds_multi_leg_paths() {
        let mut graph = GraphManager::new();
        graph.index_pool(&snapshot("pool-ab", "program", "A", "B", 100));
        graph.index_pool(&snapshot("pool-bc", "program", "B", "C", 90));
        graph.index_pool(&snapshot("pool-cd", "program", "C", "D", 80));

        let paths = graph.find_paths("A", "D", 3, 2);
        assert_eq!(paths.len(), 1);
        let path = &paths[0];
        assert_eq!(path.start_mint, "A");
        assert_eq!(path.end_mint, "D");
        assert_eq!(path.len(), 3);
        assert_eq!(path.legs[0].pool_id, "pool-ab");
        assert_eq!(path.legs[1].pool_id, "pool-bc");
        assert_eq!(path.legs[2].pool_id, "pool-cd");
    }

    #[test]
    fn detects_profitable_cycle() {
        let mut graph = GraphManager::new();
        graph.index_pool(&snapshot_with_reserves(
            "pool-ab", "program", "A", "B", 50, 100,
        ));
        graph.index_pool(&snapshot_with_reserves(
            "pool-bc", "program", "B", "C", 40, 120,
        ));
        graph.index_pool(&snapshot_with_reserves(
            "pool-ca", "program", "C", "A", 100, 60,
        ));

        let cycles = graph.latest_profitable_cycles();
        assert_eq!(cycles.len(), 1);
        let cycle = &cycles[0];
        assert_eq!(cycle.path.len(), 3);
        assert!(cycle.gain > 1.0);
        assert_eq!(cycle.path.start_mint, cycle.path.end_mint);
    }

    #[test]
    fn computes_path_price_ratio() {
        let mut graph = GraphManager::new();
        graph.index_pool(&snapshot_with_reserves(
            "pool-ab", "program", "A", "B", 50, 150,
        ));
        graph.index_pool(&snapshot_with_reserves(
            "pool-bc", "program", "B", "C", 200, 100,
        ));

        let paths = graph.find_paths("A", "C", 2, 1);
        assert_eq!(paths.len(), 1);
        let ratio = graph.path_price_ratio(&paths[0]).unwrap();
        assert!((ratio - 1.5).abs() < 1e-9);
    }

    #[test]
    fn prefers_high_ratio_paths() {
        let mut graph = GraphManager::new();
        graph.index_pool(&snapshot_with_reserves(
            "pool-ab", "program", "A", "B", 50, 150,
        ));
        graph.index_pool(&snapshot_with_reserves(
            "pool-bd", "program", "B", "D", 100, 130,
        ));
        graph.index_pool(&snapshot_with_reserves(
            "pool-ac", "program", "A", "C", 100, 110,
        ));
        graph.index_pool(&snapshot_with_reserves(
            "pool-cd", "program", "C", "D", 120, 100,
        ));

        let paths = graph.find_paths("A", "D", 3, 4);
        assert!(paths.len() >= 2);
        let best_route: Vec<&str> = paths[0]
            .legs
            .iter()
            .map(|leg| leg.pool_id.as_str())
            .collect();
        assert_eq!(best_route, vec!["pool-ab", "pool-bd"]);
    }
}
