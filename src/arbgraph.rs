use crate::config::{PlannerConfig, TokenPair};
use crate::math;
use crate::meteora::{self, MeteoraDammParams, MeteoraDlmmParams};
use crate::orca::{self, OrcaPoolParams, OrcaSwapAccounts, OrcaTickArray};
use crate::pipeline::{CurveKind, HopAccounts, PlanHop, PoolUpdate, TradeUpdate};
use crate::pool_seeds::PoolSeed;
use crate::raydium::{self, RaydiumPoolParams, RaydiumSwapAccounts, RaydiumTickArray};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;

const DEFAULT_MAX_HOPS: u8 = 2;
const MIN_PROFIT_LAMPORTS: i64 = 1;
const MAX_OPPORTUNITIES_PER_TRADE: usize = 8;
const DEFAULT_OPPORTUNITY_QUEUE_DEPTH: usize = 4096;
const SQRT_PRICE_SHIFT: u32 = 64;
const BPS_SCALE: u128 = 10_000;
const RAYDIUM_TICK_ARRAY_LEN: i32 = 60;
const MAX_RAYDIUM_SIM_ARRAYS: usize = 6;
const ORCA_TICK_ARRAY_LEN: i32 = 88;
const MAX_ORCA_SIM_ARRAYS: usize = 6;

/// Maintains a lightweight in-memory price graph fed by ingest workers.
pub struct ArbGraph {
    pools: HashMap<Pubkey, PoolSnapshot>,
    edges: HashMap<Pubkey, Vec<DirectedEdge>>,
    opportunities: HashMap<RouteFingerprint, Opportunity>,
    opportunity_order: VecDeque<RouteFingerprint>,
    opportunity_stats: OpportunityCounters,
    trade_drops: TradeDropCounters,
    max_hops: u8,
    min_profit_lamports: i64,
    max_queue_depth: usize,
    allowed_pairs: HashSet<(Pubkey, Pubkey)>,
}

impl ArbGraph {
    pub fn new(pairs: &[TokenPair], tuning: PlannerTuning) -> Self {
        let allowed_pairs = build_allowed_pairs(pairs);
        Self {
            pools: HashMap::new(),
            edges: HashMap::new(),
            opportunities: HashMap::new(),
            opportunity_order: VecDeque::new(),
            opportunity_stats: OpportunityCounters::default(),
            trade_drops: TradeDropCounters::default(),
            max_hops: tuning.max_hops,
            min_profit_lamports: tuning.min_profit_lamports,
            max_queue_depth: tuning.max_queue_depth.max(1),
            allowed_pairs,
        }
    }

    pub fn reconfigure(&mut self, pairs: &[TokenPair], tuning: PlannerTuning) {
        self.max_hops = tuning.max_hops;
        self.min_profit_lamports = tuning.min_profit_lamports;
        self.max_queue_depth = tuning.max_queue_depth.max(1);
        self.opportunities.clear();
        self.opportunity_order.clear();
        self.allowed_pairs = build_allowed_pairs(pairs);
        let allowed_pairs = self.allowed_pairs.clone();
        self.pools.retain(|_, snapshot| {
            snapshot.base_mint == Pubkey::default()
                || snapshot.quote_mint == Pubkey::default()
                || allowed_pairs.is_empty()
                || allowed_pairs.contains(&(snapshot.base_mint, snapshot.quote_mint))
        });
        self.rebuild_edges();
    }

    pub fn apply_pool_seeds(&mut self, seeds: &[PoolSeed]) -> usize {
        if seeds.is_empty() {
            return 0;
        }
        let mut applied = 0;
        for seed in seeds {
            if !self.pair_allowed(seed.base_mint, seed.quote_mint) {
                continue;
            }
            let snapshot = self
                .pools
                .entry(seed.pool)
                .or_insert_with(|| PoolSnapshot::new(seed.pool, seed.curve, seed.program));
            let old_base = snapshot.base_mint;
            let old_quote = snapshot.quote_mint;
            snapshot.curve = seed.curve;
            snapshot.program = seed.program;
            snapshot.base_mint = seed.base_mint;
            snapshot.quote_mint = seed.quote_mint;
            self.deregister_pool_edges(seed.pool, old_base, old_quote);
            self.register_pool_edges(seed.pool, seed.base_mint, seed.quote_mint);
            applied += 1;
        }
        applied
    }

    pub fn apply_pool_update(&mut self, update: PoolUpdate) {
        let allow_pair = self.pair_allowed(update.base_mint, update.quote_mint);
        let (old_base, old_quote, new_base, new_quote) = {
            let snapshot = self
                .pools
                .entry(update.pool)
                .or_insert_with(|| PoolSnapshot::new(update.pool, update.curve, update.program));
            let old_base = snapshot.base_mint;
            let old_quote = snapshot.quote_mint;
            snapshot.refresh(update.clone());
            (old_base, old_quote, snapshot.base_mint, snapshot.quote_mint)
        };

        self.deregister_pool_edges(update.pool, old_base, old_quote);
        if allow_pair {
            self.register_pool_edges(update.pool, new_base, new_quote);
        }
    }

    pub fn handle_trade(&mut self, trade: TradeUpdate) {
        if !self.pair_allowed(trade.base_mint, trade.quote_mint) {
            self.record_trade_drop(
                TradeDropReason::PairBlocked,
                trade.pool,
                trade.base_mint,
                trade.quote_mint,
                None,
            );
            return;
        }

        let mut base_input_mint = trade.base_mint;
        let mut quote_input_mint = trade.quote_mint;
        {
            let snapshot = self.pools.entry(trade.pool).or_insert_with(|| {
                PoolSnapshot::new(trade.pool, CurveKind::RaydiumClmm, Pubkey::default())
            });
            snapshot.record_trade(&trade);
            if base_input_mint == Pubkey::default() {
                base_input_mint = snapshot.base_mint;
            }
            if quote_input_mint == Pubkey::default() {
                quote_input_mint = snapshot.quote_mint;
            }
        }

        let base_notional = clamp_u64(trade.base_amount);
        let quote_notional = clamp_u64(trade.quote_amount);
        if base_notional == 0 && quote_notional == 0 {
            self.record_trade_drop(
                TradeDropReason::NotionalZero,
                trade.pool,
                trade.base_mint,
                trade.quote_mint,
                None,
            );
            return;
        }

        let attempts = [
            (base_input_mint, quote_input_mint, base_notional),
            (quote_input_mint, base_input_mint, quote_notional),
        ];
        let mut last_drop: Option<(TradeDropReason, Pubkey, Pubkey, Option<i64>)> = None;

        for (attempt_idx, (input_mint, output_mint, notional)) in attempts.into_iter().enumerate() {
            if notional == 0 {
                continue;
            }
            if input_mint == Pubkey::default() || output_mint == Pubkey::default() {
                last_drop = Some((
                    TradeDropReason::CurveStateMissing,
                    input_mint,
                    output_mint,
                    None,
                ));
                continue;
            }

            let (opportunities, drop_reason, profit_hint) =
                self.search_from_trade(trade.pool, input_mint, notional);
            if opportunities.is_empty() {
                let reason = drop_reason.unwrap_or(TradeDropReason::ProfitFloor);
                if reason == TradeDropReason::QuoteMissing {
                    self.log_quote_attempt_diagnostics(
                        trade.pool,
                        attempt_idx,
                        input_mint,
                        output_mint,
                        notional,
                        reason,
                    );
                }
                last_drop = Some((reason, input_mint, output_mint, profit_hint));
                if !matches!(
                    reason,
                    TradeDropReason::QuoteMissing | TradeDropReason::CurveStateMissing
                ) {
                    break;
                }
                continue;
            }

            for opportunity in opportunities {
                self.enqueue_opportunity(opportunity);
            }
            return;
        }

        if let Some((reason, input_mint, output_mint, profit_hint)) = last_drop {
            let profit_hint = match reason {
                TradeDropReason::ProfitFloor => profit_hint,
                _ => None,
            };
            self.record_trade_drop(reason, trade.pool, input_mint, output_mint, profit_hint);
        }
    }

    pub fn drain_opportunities(&mut self, max: usize) -> Vec<Opportunity> {
        if max == 0 || self.opportunities.is_empty() {
            return Vec::new();
        }
        let mut drained = Vec::new();
        while drained.len() < max {
            let Some(key) = self.opportunity_order.pop_front() else {
                break;
            };
            if let Some(opportunity) = self.opportunities.remove(&key) {
                drained.push(opportunity);
            }
        }
        self.opportunity_stats.drained += drained.len() as u64;
        drained
    }

    pub fn take_opportunity_stats(&mut self) -> OpportunityCounters {
        std::mem::take(&mut self.opportunity_stats)
    }

    pub fn take_trade_drop_stats(&mut self) -> TradeDropCounters {
        std::mem::take(&mut self.trade_drops)
    }

    fn record_trade_drop(
        &mut self,
        reason: TradeDropReason,
        pool: Pubkey,
        input_mint: Pubkey,
        output_mint: Pubkey,
        profit_lamports: Option<i64>,
    ) {
        let occurrences = self.trade_drops.inc(reason);
        if occurrences % 100 == 1 {
            if let (TradeDropReason::ProfitFloor, Some(profit)) = (reason, profit_lamports) {
                tracing::debug!(
                    target: "arbgraph",
                    reason = reason.as_str(),
                    pool = %pool,
                    input_mint = %input_mint,
                    output_mint = %output_mint,
                    occurrences,
                    threshold = self.min_profit_lamports,
                    expected_profit_lamports = profit,
                    "trade discarded before opportunity search"
                );
            } else {
                tracing::debug!(
                    target: "arbgraph",
                    reason = reason.as_str(),
                    pool = %pool,
                    input_mint = %input_mint,
                    output_mint = %output_mint,
                    occurrences,
                    "trade discarded before opportunity search"
                );
            }
        }
    }

    fn enqueue_opportunity(&mut self, opportunity: Opportunity) {
        let fingerprint = RouteFingerprint::from_hops(&opportunity.hops);
        if let Some(existing) = self.opportunities.get_mut(&fingerprint) {
            if opportunity.expected_profit_lamports > existing.expected_profit_lamports {
                *existing = opportunity;
                self.opportunity_stats.replaced += 1;
            } else {
                self.opportunity_stats.duplicates += 1;
            }
            return;
        }

        self.opportunity_order.push_back(fingerprint.clone());
        self.opportunities.insert(fingerprint, opportunity);
        self.opportunity_stats.emitted += 1;
        self.trim_opportunity_queue();
    }

    fn trim_opportunity_queue(&mut self) {
        while self.opportunity_order.len() > self.max_queue_depth {
            let Some(evicted_key) = self.opportunity_order.pop_front() else {
                break;
            };
            if self.opportunities.remove(&evicted_key).is_some() {
                self.opportunity_stats.evicted += 1;
            }
        }
    }

    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }

    pub fn curve_kind(&self, pool: &Pubkey) -> Option<CurveKind> {
        self.pools.get(pool).map(|snapshot| snapshot.curve)
    }

    fn pair_allowed(&self, base: Pubkey, quote: Pubkey) -> bool {
        if self.allowed_pairs.is_empty() {
            return true;
        }
        if base == Pubkey::default() || quote == Pubkey::default() {
            return true;
        }
        self.allowed_pairs.contains(&(base, quote))
    }

    pub fn set_raydium_state(&mut self, pool: Pubkey, params: RaydiumPoolParams) {
        if let Some(snapshot) = self.pools.get_mut(&pool) {
            snapshot.set_raydium(params);
        }
    }

    pub fn has_raydium_state(&self, pool: &Pubkey) -> bool {
        self.pools
            .get(pool)
            .and_then(|snapshot| snapshot.raydium())
            .is_some()
    }

    pub fn set_raydium_tick_arrays(
        &mut self,
        pool: Pubkey,
        arrays: Vec<RaydiumTickArray>,
        bitmap_present: bool,
    ) {
        if arrays.is_empty() {
            return;
        }
        if let Some(snapshot) = self.pools.get_mut(&pool) {
            snapshot.set_raydium_tick_arrays(arrays, bitmap_present);
        }
    }

    pub fn clear_raydium_tick_arrays(&mut self, pool: Pubkey) {
        if let Some(snapshot) = self.pools.get_mut(&pool) {
            snapshot.clear_raydium_tick_arrays();
        }
    }

    pub fn set_orca_state(&mut self, pool: Pubkey, params: OrcaPoolParams) {
        if let Some(snapshot) = self.pools.get_mut(&pool) {
            snapshot.set_orca(params);
        }
    }

    pub fn has_orca_state(&self, pool: &Pubkey) -> bool {
        self.pools
            .get(pool)
            .and_then(|snapshot| snapshot.orca())
            .is_some()
    }

    pub fn set_orca_tick_arrays(&mut self, pool: Pubkey, arrays: Vec<OrcaTickArray>) {
        if arrays.is_empty() {
            return;
        }
        if let Some(snapshot) = self.pools.get_mut(&pool) {
            snapshot.set_orca_tick_arrays(arrays);
        }
    }

    pub fn clear_orca_tick_arrays(&mut self, pool: Pubkey) {
        if let Some(snapshot) = self.pools.get_mut(&pool) {
            snapshot.clear_orca_tick_arrays();
        }
    }

    pub fn raydium_tick_account_addresses(
        &self,
        pool: &Pubkey,
        zero_for_one: bool,
        limit: usize,
    ) -> Vec<Pubkey> {
        self.pools
            .get(pool)
            .map(|snapshot| snapshot.raydium_tick_accounts(zero_for_one, limit))
            .unwrap_or_default()
    }

    pub fn orca_tick_account_addresses(
        &self,
        pool: &Pubkey,
        a_to_b: bool,
        limit: usize,
    ) -> Vec<Pubkey> {
        self.pools
            .get(pool)
            .map(|snapshot| snapshot.orca_tick_accounts(a_to_b, limit))
            .unwrap_or_default()
    }

    pub fn raydium_bitmap_present(&self, pool: &Pubkey) -> bool {
        self.pools
            .get(pool)
            .map(|snapshot| snapshot.raydium_bitmap_present())
            .unwrap_or(false)
    }

    pub fn reprice_hop(&self, hop: &PlanHop) -> Option<HopRequote> {
        self.pools
            .get(&hop.pool)
            .and_then(|snapshot| snapshot.reprice_hop(hop))
    }

    pub fn set_meteora_damm_state(&mut self, pool: Pubkey, params: MeteoraDammParams) {
        if let Some(snapshot) = self.pools.get_mut(&pool) {
            snapshot.set_meteora_damm(params);
        }
    }

    pub fn has_meteora_damm_state(&self, pool: &Pubkey) -> bool {
        self.pools
            .get(pool)
            .and_then(|snapshot| snapshot.meteora_damm())
            .is_some()
    }

    pub fn set_meteora_dlmm_state(&mut self, pool: Pubkey, params: MeteoraDlmmParams) {
        if let Some(snapshot) = self.pools.get_mut(&pool) {
            snapshot.set_meteora_dlmm(params);
        }
    }

    pub fn has_meteora_dlmm_state(&self, pool: &Pubkey) -> bool {
        self.pools
            .get(pool)
            .and_then(|snapshot| snapshot.meteora_dlmm())
            .is_some()
    }

    fn rebuild_edges(&mut self) {
        self.edges.clear();
        let snapshots: Vec<(Pubkey, Pubkey, Pubkey)> = self
            .pools
            .iter()
            .map(|(pool, snapshot)| (*pool, snapshot.base_mint, snapshot.quote_mint))
            .collect();

        for (pool, base_mint, quote_mint) in snapshots {
            self.register_pool_edges(pool, base_mint, quote_mint);
        }
    }

    fn register_pool_edges(&mut self, pool: Pubkey, base_mint: Pubkey, quote_mint: Pubkey) {
        if base_mint == Pubkey::default() || quote_mint == Pubkey::default() {
            return;
        }
        if !self.pair_allowed(base_mint, quote_mint) {
            return;
        }
        let forward = DirectedEdge {
            pool,
            input_mint: base_mint,
            output_mint: quote_mint,
        };
        let reverse = DirectedEdge {
            pool,
            input_mint: quote_mint,
            output_mint: base_mint,
        };
        self.edges.entry(base_mint).or_default().push(forward);
        self.edges.entry(quote_mint).or_default().push(reverse);
    }

    fn deregister_pool_edges(&mut self, pool: Pubkey, base_mint: Pubkey, quote_mint: Pubkey) {
        if base_mint != Pubkey::default() {
            Self::remove_edge(&mut self.edges, base_mint, pool);
        }
        if quote_mint != Pubkey::default() {
            Self::remove_edge(&mut self.edges, quote_mint, pool);
        }
    }

    fn remove_edge(edges: &mut HashMap<Pubkey, Vec<DirectedEdge>>, mint: Pubkey, pool: Pubkey) {
        if let Some(list) = edges.get_mut(&mint) {
            list.retain(|edge| edge.pool != pool);
            if list.is_empty() {
                edges.remove(&mint);
            }
        }
    }

    fn search_from_trade(
        &self,
        pool: Pubkey,
        input_mint: Pubkey,
        amount_in: u64,
    ) -> (Vec<Opportunity>, Option<TradeDropReason>, Option<i64>) {
        let Some(snapshot) = self.pools.get(&pool) else {
            return (Vec::new(), Some(TradeDropReason::CurveStateMissing), None);
        };
        let mut best_profit_hint: Option<i64> = None;
        let Some(first_quote) = snapshot.quote(input_mint, amount_in) else {
            return (
                Vec::new(),
                Some(TradeDropReason::QuoteMissing),
                best_profit_hint,
            );
        };
        if !self.pair_allowed(first_quote.hop.input_mint, first_quote.hop.output_mint) {
            return (
                Vec::new(),
                Some(TradeDropReason::PairBlocked),
                best_profit_hint,
            );
        }

        let mut results = Vec::new();
        let mut path = vec![first_quote.hop.clone()];
        let mut visited = vec![pool];

        if first_quote.hop.output_mint == input_mint {
            self.emit_opportunity(
                amount_in,
                first_quote.amount_out,
                &path,
                &mut best_profit_hint,
                &mut results,
            );
        }

        if self.max_hops > 1 {
            self.extend_paths(
                first_quote.hop.output_mint,
                first_quote.amount_out,
                input_mint,
                self.max_hops - 1,
                amount_in,
                &mut path,
                &mut visited,
                &mut best_profit_hint,
                &mut results,
            );
        }

        if results.is_empty() {
            if let Some(profit) = best_profit_hint {
                tracing::debug!(
                    target: "arbgraph",
                    pool = %pool,
                    input_mint = %input_mint,
                    amount_in,
                    threshold = self.min_profit_lamports,
                    profit_lamports = profit,
                    "candidate opportunity filtered by profit floor"
                );
            }
        }

        (results, None, best_profit_hint)
    }

    fn extend_paths(
        &self,
        current_mint: Pubkey,
        current_amount: u64,
        target_mint: Pubkey,
        remaining_hops: u8,
        initial_amount: u64,
        path: &mut Vec<PlanHop>,
        visited: &mut Vec<Pubkey>,
        best_profit_hint: &mut Option<i64>,
        results: &mut Vec<Opportunity>,
    ) {
        if remaining_hops == 0 || results.len() >= MAX_OPPORTUNITIES_PER_TRADE {
            return;
        }

        if let Some(edges) = self.edges.get(&current_mint) {
            for edge in edges {
                if results.len() >= MAX_OPPORTUNITIES_PER_TRADE {
                    break;
                }
                if visited.contains(&edge.pool) {
                    continue;
                }
                if !self.pair_allowed(edge.input_mint, edge.output_mint) {
                    continue;
                }
                let Some(quote) = self.quote_edge(edge, current_amount) else {
                    continue;
                };

                path.push(quote.hop.clone());
                visited.push(edge.pool);

                if quote.hop.output_mint == target_mint && path.len() >= 2 {
                    self.emit_opportunity(
                        initial_amount,
                        quote.amount_out,
                        path,
                        best_profit_hint,
                        results,
                    );
                }

                if remaining_hops > 1 {
                    self.extend_paths(
                        quote.hop.output_mint,
                        quote.amount_out,
                        target_mint,
                        remaining_hops - 1,
                        initial_amount,
                        path,
                        visited,
                        best_profit_hint,
                        results,
                    );
                }

                visited.pop();
                path.pop();
            }
        }
    }

    fn quote_edge(&self, edge: &DirectedEdge, amount_in: u64) -> Option<HopQuote> {
        let snapshot = self.pools.get(&edge.pool)?;
        snapshot.quote(edge.input_mint, amount_in)
    }

    fn log_quote_attempt_diagnostics(
        &self,
        pool: Pubkey,
        attempt_idx: usize,
        input_mint: Pubkey,
        output_mint: Pubkey,
        notional: u64,
        reason: TradeDropReason,
    ) {
        let direction = Self::attempt_direction(attempt_idx);
        let (snapshot_base, snapshot_quote) = self
            .pools
            .get(&pool)
            .map(|snapshot| (snapshot.base_mint, snapshot.quote_mint))
            .unwrap_or((Pubkey::default(), Pubkey::default()));
        let edges_from_input = self.describe_edges(input_mint);
        let edges_from_output = self.describe_edges(output_mint);
        tracing::debug!(
            target: "arbgraph",
            pool = %pool,
            reason = reason.as_str(),
            direction,
            attempt_index = attempt_idx,
            input_mint = %input_mint,
            output_mint = %output_mint,
            notional,
            snapshot_base = %snapshot_base,
            snapshot_quote = %snapshot_quote,
            edges_from_input_len = edges_from_input.len(),
            edges_from_output_len = edges_from_output.len(),
            edges_from_input = ?edges_from_input,
            edges_from_output = ?edges_from_output,
            "quote attempt failed"
        );
    }

    fn describe_edges(&self, mint: Pubkey) -> Vec<(Pubkey, Pubkey)> {
        self.edges
            .get(&mint)
            .map(|edges| {
                edges
                    .iter()
                    .map(|edge| (edge.pool, edge.output_mint))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn attempt_direction(attempt_idx: usize) -> &'static str {
        match attempt_idx {
            0 => "base_to_quote",
            1 => "quote_to_base",
            _ => "extra_direction",
        }
    }

    fn emit_opportunity(
        &self,
        initial_amount: u64,
        final_amount: u64,
        hops: &[PlanHop],
        best_profit_hint: &mut Option<i64>,
        results: &mut Vec<Opportunity>,
    ) {
        if results.len() >= MAX_OPPORTUNITIES_PER_TRADE {
            return;
        }
        let delta = final_amount as i128 - initial_amount as i128;
        let profit = clamp_i64(delta);
        if best_profit_hint.map_or(true, |best| profit > best) {
            *best_profit_hint = Some(profit);
        }
        if profit <= 0 {
            return;
        }
        if profit < self.min_profit_lamports {
            if let Some(first_hop) = hops.first() {
                tracing::debug!(
                    target: "arbgraph",
                    pool = %first_hop.pool,
                    curve = ?first_hop.curve,
                    input_mint = %first_hop.input_mint,
                    output_mint = %first_hop.output_mint,
                    initial_amount,
                    final_amount,
                    threshold = self.min_profit_lamports,
                    profit_lamports = profit,
                    hops = hops.len(),
                    "opportunity dropped: profit below floor"
                );
            }
            return;
        }
        let hop_count = hops.len().min(u8::MAX as usize) as u8;
        if hop_count == 0 {
            return;
        }
        let mut plan_hops = Vec::with_capacity(hops.len());
        plan_hops.extend_from_slice(hops);
        let first = &plan_hops[0];
        results.push(Opportunity {
            pool: first.pool,
            curve: first.curve,
            notional: initial_amount,
            expected_profit_lamports: profit,
            hops: plan_hops,
        });
    }
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub pool: Pubkey,
    pub curve: CurveKind,
    pub notional: u64,
    pub expected_profit_lamports: i64,
    pub hops: Vec<PlanHop>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RouteFingerprint {
    parts: Vec<Pubkey>,
}

impl RouteFingerprint {
    fn from_hops(hops: &[PlanHop]) -> Self {
        if hops.is_empty() {
            return Self { parts: Vec::new() };
        }

        let mut parts = Vec::with_capacity(hops.len() * 2 + 1);
        parts.push(hops[0].input_mint);
        for hop in hops {
            parts.push(hop.pool);
            parts.push(hop.output_mint);
        }
        Self { parts }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct OpportunityCounters {
    pub emitted: u64,
    pub duplicates: u64,
    pub replaced: u64,
    pub drained: u64,
    pub evicted: u64,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct TradeDropCounters {
    pub pair_blocked: u64,
    pub notional_zero: u64,
    pub curve_state_missing: u64,
    pub quote_missing: u64,
    pub profit_floor: u64,
}

impl TradeDropCounters {
    fn inc(&mut self, reason: TradeDropReason) -> u64 {
        match reason {
            TradeDropReason::PairBlocked => {
                self.pair_blocked = self.pair_blocked.saturating_add(1);
                self.pair_blocked
            }
            TradeDropReason::NotionalZero => {
                self.notional_zero = self.notional_zero.saturating_add(1);
                self.notional_zero
            }
            TradeDropReason::CurveStateMissing => {
                self.curve_state_missing = self.curve_state_missing.saturating_add(1);
                self.curve_state_missing
            }
            TradeDropReason::QuoteMissing => {
                self.quote_missing = self.quote_missing.saturating_add(1);
                self.quote_missing
            }
            TradeDropReason::ProfitFloor => {
                self.profit_floor = self.profit_floor.saturating_add(1);
                self.profit_floor
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TradeDropReason {
    PairBlocked,
    NotionalZero,
    CurveStateMissing,
    QuoteMissing,
    ProfitFloor,
}

impl TradeDropReason {
    fn as_str(self) -> &'static str {
        match self {
            TradeDropReason::PairBlocked => "pair_blocked",
            TradeDropReason::NotionalZero => "notional_zero",
            TradeDropReason::CurveStateMissing => "curve_state_missing",
            TradeDropReason::QuoteMissing => "quote_missing",
            TradeDropReason::ProfitFloor => "profit_floor",
        }
    }
}

#[derive(Debug, Clone)]
struct DirectedEdge {
    pool: Pubkey,
    input_mint: Pubkey,
    output_mint: Pubkey,
}

#[derive(Debug, Clone)]
struct HopQuote {
    hop: PlanHop,
    amount_out: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct HopRequote {
    pub amount_out: u64,
    pub min_amount_out: u64,
}

struct PoolSnapshot {
    pool: Pubkey,
    curve: CurveKind,
    program: Pubkey,
    last_slot: u64,
    last_seq: u64,
    last_trade: Option<TradeStats>,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_reserve: u64,
    quote_reserve: u64,
    base_decimals: u8,
    quote_decimals: u8,
    raydium: Option<RaydiumPoolParams>,
    orca: Option<OrcaPoolParams>,
    meteora_damm: Option<MeteoraDammParams>,
    meteora_dlmm: Option<MeteoraDlmmParams>,
    raydium_tick_arrays: HashMap<i32, RaydiumTickArray>,
    raydium_bitmap_present: bool,
    orca_tick_arrays: HashMap<i32, OrcaTickArray>,
}

impl PoolSnapshot {
    fn new(pool: Pubkey, curve: CurveKind, program: Pubkey) -> Self {
        Self {
            pool,
            curve,
            program,
            last_slot: 0,
            last_seq: 0,
            last_trade: None,
            base_mint: Pubkey::default(),
            quote_mint: Pubkey::default(),
            base_reserve: 0,
            quote_reserve: 0,
            base_decimals: 0,
            quote_decimals: 0,
            raydium: None,
            orca: None,
            meteora_damm: None,
            meteora_dlmm: None,
            raydium_tick_arrays: HashMap::new(),
            raydium_bitmap_present: false,
            orca_tick_arrays: HashMap::new(),
        }
    }

    fn refresh(&mut self, update: PoolUpdate) {
        self.curve = update.curve;
        self.program = update.program;
        self.last_slot = update.slot;
        self.last_seq = update.seq;
        self.base_mint = update.base_mint;
        self.quote_mint = update.quote_mint;
        self.base_reserve = update.base_reserve;
        self.quote_reserve = update.quote_reserve;
        self.base_decimals = update.base_decimals;
        self.quote_decimals = update.quote_decimals;
        if self.curve != CurveKind::RaydiumClmm {
            self.raydium_tick_arrays.clear();
            self.raydium_bitmap_present = false;
        }
        if self.curve != CurveKind::OrcaWhirlpool {
            self.orca_tick_arrays.clear();
        }
    }

    fn set_raydium(&mut self, params: RaydiumPoolParams) {
        if let Some(existing) = self.raydium {
            if existing.tick_spacing != params.tick_spacing {
                self.clear_raydium_tick_arrays();
            }
        }
        self.raydium = Some(params);
    }

    fn raydium(&self) -> Option<RaydiumPoolParams> {
        self.raydium
    }

    fn raydium_bitmap_present(&self) -> bool {
        self.raydium_bitmap_present
    }

    fn set_raydium_tick_arrays(&mut self, arrays: Vec<RaydiumTickArray>, bitmap_present: bool) {
        if self.curve != CurveKind::RaydiumClmm || arrays.is_empty() {
            return;
        }
        for array in arrays {
            self.raydium_tick_arrays
                .insert(array.start_tick_index, array);
        }
        self.raydium_bitmap_present = bitmap_present;
    }

    fn clear_raydium_tick_arrays(&mut self) {
        self.raydium_tick_arrays.clear();
        self.raydium_bitmap_present = false;
    }

    fn set_orca_tick_arrays(&mut self, arrays: Vec<OrcaTickArray>) {
        if self.curve != CurveKind::OrcaWhirlpool || arrays.is_empty() {
            return;
        }
        for array in arrays {
            self.orca_tick_arrays.insert(array.start_tick_index, array);
        }
    }

    fn clear_orca_tick_arrays(&mut self) {
        self.orca_tick_arrays.clear();
    }

    fn set_orca(&mut self, params: OrcaPoolParams) {
        if let Some(existing) = self.orca {
            if existing.tick_spacing != params.tick_spacing {
                self.clear_orca_tick_arrays();
            }
        }
        self.orca = Some(params);
    }

    fn orca(&self) -> Option<OrcaPoolParams> {
        self.orca
    }

    fn set_meteora_damm(&mut self, params: MeteoraDammParams) {
        self.meteora_damm = Some(params);
    }

    fn meteora_damm(&self) -> Option<MeteoraDammParams> {
        self.meteora_damm
    }

    fn set_meteora_dlmm(&mut self, params: MeteoraDlmmParams) {
        self.meteora_dlmm = Some(params);
    }

    fn meteora_dlmm(&self) -> Option<MeteoraDlmmParams> {
        self.meteora_dlmm
    }

    fn hop_accounts(&self) -> Option<HopAccounts> {
        match self.curve {
            CurveKind::RaydiumClmm => self
                .raydium
                .map(|params| HopAccounts::RaydiumClmm(RaydiumSwapAccounts::from_params(params))),
            CurveKind::OrcaWhirlpool => self
                .orca
                .map(|params| HopAccounts::OrcaWhirlpool(OrcaSwapAccounts::from_params(params))),
            CurveKind::MeteoraDamm => self.meteora_damm.map(HopAccounts::MeteoraDamm),
            CurveKind::MeteoraDlmm => self.meteora_dlmm.map(HopAccounts::MeteoraDlmm),
        }
    }

    fn record_trade(&mut self, trade: &TradeUpdate) {
        self.last_trade = Some(TradeStats {
            base_amount: trade.base_amount,
            quote_amount: trade.quote_amount,
            slot: trade.slot,
        });
    }

    fn quote(&self, input_mint: Pubkey, amount_in: u64) -> Option<HopQuote> {
        if amount_in == 0 {
            return None;
        }

        if let Some(quote) = self.quote_with_curve_reserves(input_mint, amount_in) {
            return Some(quote);
        }

        self.quote_from_local_reserves(input_mint, amount_in)
    }

    fn reprice_hop(&self, hop: &PlanHop) -> Option<HopRequote> {
        if hop.amount_in == 0 {
            return None;
        }
        let quote = self.quote(hop.input_mint, hop.amount_in)?;
        Some(HopRequote {
            amount_out: quote.amount_out,
            min_amount_out: quote.hop.min_amount_out,
        })
    }

    fn quote_with_curve_reserves(&self, input_mint: Pubkey, amount_in: u64) -> Option<HopQuote> {
        match self.curve {
            CurveKind::RaydiumClmm => {
                self.quote_raydium_simulated(input_mint, amount_in)
                    .or_else(|| {
                        self.raydium()
                            .and_then(|params| {
                                raydium::infer_virtual_reserves(&params, SQRT_PRICE_SHIFT)
                            })
                            .and_then(|legs| {
                                self.quote_constant_product(input_mint, amount_in, legs.0, legs.1)
                            })
                    })
            }
            CurveKind::OrcaWhirlpool => {
                self.quote_orca_simulated(input_mint, amount_in)
                    .or_else(|| {
                        self.orca
                            .and_then(|params| {
                                orca::infer_virtual_reserves(&params, SQRT_PRICE_SHIFT)
                            })
                            .and_then(|legs| {
                                self.quote_constant_product(input_mint, amount_in, legs.0, legs.1)
                            })
                    })
            }
            CurveKind::MeteoraDamm => self
                .meteora_damm
                .and_then(|params| meteora::infer_virtual_reserves(&params, SQRT_PRICE_SHIFT))
                .and_then(|legs| {
                    self.quote_constant_product(input_mint, amount_in, legs.0, legs.1)
                }),
            CurveKind::MeteoraDlmm => None,
        }
    }

    fn quote_raydium_simulated(&self, input_mint: Pubkey, amount_in: u64) -> Option<HopQuote> {
        if amount_in == 0 {
            return None;
        }

        let params = self.raydium()?;
        let (output_mint, zero_for_one) = if input_mint == params.token_mint_0 {
            (params.token_mint_1, true)
        } else if input_mint == params.token_mint_1 {
            (params.token_mint_0, false)
        } else {
            return None;
        };

        let tick_arrays = self.collect_raydium_tick_arrays(&params, zero_for_one);
        if tick_arrays.is_empty() {
            return None;
        }

        let fee_rate_millionths = u32::from(self.curve.default_fee_bps()) * 100;
        let simulation = raydium::simulate_swap_exact_in(
            &params,
            &tick_arrays,
            zero_for_one,
            amount_in,
            fee_rate_millionths,
        )?;
        if simulation.amount_in < u128::from(amount_in) || simulation.amount_out == 0 {
            return None;
        }

        let amount_out = simulation.amount_out;
        let amount_out_u64 = clamp_u128_to_u64(amount_out);
        if amount_out_u64 == 0 {
            return None;
        }
        let min_out_u64 = self.apply_slippage_floor(amount_out)?;
        let accounts = self.hop_accounts()?;

        Some(HopQuote {
            amount_out: amount_out_u64,
            hop: PlanHop {
                pool: self.pool,
                program: self.program,
                curve: self.curve,
                input_mint,
                output_mint,
                amount_in,
                min_amount_out: min_out_u64,
                accounts,
            },
        })
    }

    fn raydium_tick_accounts(&self, zero_for_one: bool, limit: usize) -> Vec<Pubkey> {
        if limit == 0 || self.raydium_tick_arrays.is_empty() {
            return Vec::new();
        }
        let Some(params) = self.raydium else {
            return Vec::new();
        };
        let tick_spacing = i32::from(params.tick_spacing);
        if tick_spacing <= 0 {
            return Vec::new();
        }
        let ticks_per_array = match RAYDIUM_TICK_ARRAY_LEN.checked_mul(tick_spacing) {
            Some(value) if value > 0 => value,
            _ => return Vec::new(),
        };

        let mut starts = Vec::new();
        let mut current_start = align_tick_array_start(params.tick_current, ticks_per_array);
        for _ in 0..limit {
            if self.raydium_tick_arrays.contains_key(&current_start) {
                starts.push(current_start);
            } else if starts.is_empty() {
                return Vec::new();
            } else {
                break;
            }

            current_start = if zero_for_one {
                match current_start.checked_sub(ticks_per_array) {
                    Some(next) => next,
                    None => break,
                }
            } else {
                match current_start.checked_add(ticks_per_array) {
                    Some(next) => next,
                    None => break,
                }
            };
        }

        starts
            .into_iter()
            .map(|start| derive_raydium_tick_array_pubkey(&self.pool, start))
            .collect()
    }

    fn quote_orca_simulated(&self, input_mint: Pubkey, amount_in: u64) -> Option<HopQuote> {
        if amount_in == 0 {
            return None;
        }

        let params = self.orca()?;
        let (output_mint, a_to_b) = if input_mint == params.token_mint_a {
            (params.token_mint_b, true)
        } else if input_mint == params.token_mint_b {
            (params.token_mint_a, false)
        } else {
            return None;
        };

        let tick_arrays = self.collect_orca_tick_arrays(&params, a_to_b);
        if tick_arrays.is_empty() {
            return None;
        }

        let fee_rate_millionths = u32::from(params.fee_rate) * 100;
        let simulation = orca::simulate_swap_exact_in(
            &params,
            &tick_arrays,
            a_to_b,
            amount_in,
            fee_rate_millionths,
        )?;
        if simulation.amount_in < u128::from(amount_in) || simulation.amount_out == 0 {
            return None;
        }

        let amount_out = simulation.amount_out;
        let amount_out_u64 = clamp_u128_to_u64(amount_out);
        if amount_out_u64 == 0 {
            return None;
        }
        let min_out_u64 = self.apply_slippage_floor(amount_out)?;
        let accounts = self.hop_accounts()?;

        Some(HopQuote {
            amount_out: amount_out_u64,
            hop: PlanHop {
                pool: self.pool,
                program: self.program,
                curve: self.curve,
                input_mint,
                output_mint,
                amount_in,
                min_amount_out: min_out_u64,
                accounts,
            },
        })
    }

    fn quote_from_local_reserves(&self, input_mint: Pubkey, amount_in: u64) -> Option<HopQuote> {
        if self.base_mint == Pubkey::default() || self.quote_mint == Pubkey::default() {
            return None;
        }
        let reserve_base = u128::from(self.base_reserve);
        let reserve_quote = u128::from(self.quote_reserve);
        if reserve_base == 0 || reserve_quote == 0 {
            return None;
        }

        self.quote_constant_product(
            input_mint,
            amount_in,
            (self.base_mint, reserve_base),
            (self.quote_mint, reserve_quote),
        )
    }

    fn quote_constant_product(
        &self,
        input_mint: Pubkey,
        amount_in: u64,
        leg_a: (Pubkey, u128),
        leg_b: (Pubkey, u128),
    ) -> Option<HopQuote> {
        if input_mint == leg_a.0 {
            self.constant_product(amount_in, leg_a.1, leg_b.1, leg_a.0, leg_b.0)
        } else if input_mint == leg_b.0 {
            self.constant_product(amount_in, leg_b.1, leg_a.1, leg_b.0, leg_a.0)
        } else {
            None
        }
    }

    fn constant_product(
        &self,
        amount_in: u64,
        reserve_in: u128,
        reserve_out: u128,
        input_mint: Pubkey,
        output_mint: Pubkey,
    ) -> Option<HopQuote> {
        if reserve_in == 0 || reserve_out == 0 {
            return None;
        }

        let fee_scale = BPS_SCALE;
        let fee_bps = u128::from(self.curve.default_fee_bps());

        let amount_in_u128 = u128::from(amount_in);
        let fee_numer = fee_scale.saturating_sub(fee_bps.min(fee_scale - 1));
        let net = math::mul_div_u256(amount_in_u128, fee_numer, fee_scale)?;
        if net == 0 {
            return None;
        }

        let denominator = reserve_in.checked_add(net)?;
        if denominator == 0 {
            return None;
        }
        let amount_out = math::mul_div_u256(reserve_out, net, denominator)?;
        if amount_out == 0 {
            return None;
        }

        let amount_out_u64 = clamp_u128_to_u64(amount_out);
        let min_out_u64 = self.apply_slippage_floor(amount_out)?;

        let accounts = match self.hop_accounts() {
            Some(accounts) => accounts,
            None => return None,
        };

        Some(HopQuote {
            amount_out: amount_out_u64,
            hop: PlanHop {
                pool: self.pool,
                program: self.program,
                curve: self.curve,
                input_mint,
                output_mint,
                amount_in,
                min_amount_out: min_out_u64,
                accounts,
            },
        })
    }

    fn apply_slippage_floor(&self, amount_out: u128) -> Option<u64> {
        if amount_out == 0 {
            return None;
        }
        let slippage_bps = u128::from(self.curve.safety_slippage_bps());
        let numerator = BPS_SCALE.saturating_sub(slippage_bps.min(BPS_SCALE - 1));
        let min_out = math::mul_div_u256(amount_out, numerator, BPS_SCALE)?.max(1);
        Some(clamp_u128_to_u64(min_out))
    }

    fn collect_raydium_tick_arrays(
        &self,
        params: &RaydiumPoolParams,
        zero_for_one: bool,
    ) -> Vec<RaydiumTickArray> {
        if self.raydium_tick_arrays.is_empty() {
            return Vec::new();
        }

        let tick_spacing = i32::from(params.tick_spacing);
        if tick_spacing <= 0 {
            return Vec::new();
        }
        let ticks_per_array = match RAYDIUM_TICK_ARRAY_LEN.checked_mul(tick_spacing) {
            Some(value) if value > 0 => value,
            _ => return Vec::new(),
        };

        let mut arrays = Vec::new();
        let mut current_start = align_tick_array_start(params.tick_current, ticks_per_array);
        for _ in 0..MAX_RAYDIUM_SIM_ARRAYS {
            let Some(array) = self.raydium_tick_arrays.get(&current_start) else {
                if arrays.is_empty() {
                    return Vec::new();
                }
                break;
            };
            arrays.push(array.clone());

            current_start = if zero_for_one {
                match current_start.checked_sub(ticks_per_array) {
                    Some(next) => next,
                    None => break,
                }
            } else {
                match current_start.checked_add(ticks_per_array) {
                    Some(next) => next,
                    None => break,
                }
            };
        }

        arrays
    }

    fn collect_orca_tick_arrays(
        &self,
        params: &OrcaPoolParams,
        a_to_b: bool,
    ) -> Vec<OrcaTickArray> {
        if self.orca_tick_arrays.is_empty() {
            return Vec::new();
        }

        let tick_spacing = i32::from(params.tick_spacing);
        if tick_spacing <= 0 {
            return Vec::new();
        }
        let ticks_per_array = match ORCA_TICK_ARRAY_LEN.checked_mul(tick_spacing) {
            Some(value) if value > 0 => value,
            _ => return Vec::new(),
        };

        let mut arrays = Vec::new();
        let mut current_start = align_tick_array_start(params.tick_current_index, ticks_per_array);
        for _ in 0..MAX_ORCA_SIM_ARRAYS {
            let Some(array) = self.orca_tick_arrays.get(&current_start) else {
                if arrays.is_empty() {
                    return Vec::new();
                }
                break;
            };
            arrays.push(array.clone());

            current_start = if a_to_b {
                match current_start.checked_sub(ticks_per_array) {
                    Some(next) => next,
                    None => break,
                }
            } else {
                match current_start.checked_add(ticks_per_array) {
                    Some(next) => next,
                    None => break,
                }
            };
        }

        arrays
    }

    fn orca_tick_accounts(&self, a_to_b: bool, limit: usize) -> Vec<Pubkey> {
        if limit == 0 || self.orca_tick_arrays.is_empty() {
            return Vec::new();
        }
        let Some(params) = self.orca else {
            return Vec::new();
        };
        let tick_spacing = i32::from(params.tick_spacing);
        if tick_spacing <= 0 {
            return Vec::new();
        }
        let ticks_per_array = match ORCA_TICK_ARRAY_LEN.checked_mul(tick_spacing) {
            Some(value) if value > 0 => value,
            _ => return Vec::new(),
        };

        let mut starts = Vec::new();
        let mut current_start = align_tick_array_start(params.tick_current_index, ticks_per_array);
        for _ in 0..limit {
            if self.orca_tick_arrays.contains_key(&current_start) {
                starts.push(current_start);
            } else if starts.is_empty() {
                return Vec::new();
            } else {
                break;
            }

            current_start = if a_to_b {
                match current_start.checked_sub(ticks_per_array) {
                    Some(next) => next,
                    None => break,
                }
            } else {
                match current_start.checked_add(ticks_per_array) {
                    Some(next) => next,
                    None => break,
                }
            };
        }

        starts
            .into_iter()
            .map(|start| derive_orca_tick_array_pubkey(&self.pool, start))
            .collect()
    }
}

#[allow(dead_code)]
struct TradeStats {
    base_amount: i128,
    quote_amount: i128,
    slot: u64,
}

fn compute_max_hops(pairs: &[TokenPair]) -> u8 {
    if pairs.len() >= 2 {
        3
    } else {
        DEFAULT_MAX_HOPS
    }
}

fn clamp_u64(value: i128) -> u64 {
    if value <= 0 {
        return 0;
    }
    let max = i128::from(u64::MAX);
    value.min(max) as u64
}

fn clamp_i64(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn clamp_u128_to_u64(value: u128) -> u64 {
    if value > u128::from(u64::MAX) {
        u64::MAX
    } else {
        value as u64
    }
}

fn derive_raydium_tick_array_pubkey(pool: &Pubkey, start_tick_index: i32) -> Pubkey {
    let start_bytes = start_tick_index.to_le_bytes();
    Pubkey::find_program_address(
        &[b"tick_array", pool.as_ref(), &start_bytes],
        &raydium::RAYDIUM_CLMM_PROGRAM_ID,
    )
    .0
}

fn derive_orca_tick_array_pubkey(pool: &Pubkey, start_tick_index: i32) -> Pubkey {
    let start_bytes = start_tick_index.to_le_bytes();
    Pubkey::find_program_address(
        &[b"tick_array", pool.as_ref(), &start_bytes],
        &orca::ORCA_WHIRLPOOL_PROGRAM_ID,
    )
    .0
}

fn align_tick_array_start(tick_index: i32, ticks_per_array: i32) -> i32 {
    tick_index - tick_index.rem_euclid(ticks_per_array)
}

fn build_allowed_pairs(pairs: &[TokenPair]) -> HashSet<(Pubkey, Pubkey)> {
    let mut set = HashSet::new();
    for pair in pairs {
        set.insert((pair.base, pair.quote));
        set.insert((pair.quote, pair.base));
    }
    set
}

#[derive(Debug, Clone, Copy)]
pub struct PlannerTuning {
    max_hops: u8,
    min_profit_lamports: i64,
    max_queue_depth: usize,
}

impl PlannerTuning {
    pub fn from_config(pairs: &[TokenPair], cfg: &PlannerConfig) -> Self {
        let default_max = compute_max_hops(pairs);
        let max_hops = cfg.max_hops.unwrap_or(default_max).clamp(1, 4);
        let env_override = env::var("TRE_MIN_PROFIT_LAMPORTS_OVERRIDE")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value >= MIN_PROFIT_LAMPORTS);
        if let Some(override_value) = env_override {
            tracing::info!(
                target: "arbgraph",
                override_value,
                "planner min profit override applied"
            );
        }
        let min_profit =
            env_override.unwrap_or_else(|| cfg.min_profit_lamports.max(MIN_PROFIT_LAMPORTS));
        let queue_depth = cfg
            .max_queue_depth
            .unwrap_or(DEFAULT_OPPORTUNITY_QUEUE_DEPTH)
            .max(1);
        Self {
            max_hops,
            min_profit_lamports: min_profit,
            max_queue_depth: queue_depth,
        }
    }
}
