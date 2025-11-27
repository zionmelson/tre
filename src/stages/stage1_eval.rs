use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, warn};

use crate::adapters::{
    CpAmmAdapter, CpAmmAdapterConfig, LbClmmAdapter, LbClmmAdapterConfig, LifinityAdapter,
    LifinityAdapterConfig, ProtocolAdapter, QuoteRequest, QuoteResponse, RaydiumV3Adapter,
    RaydiumV3AdapterConfig, SwapDirection, WhirlpoolAdapter, WhirlpoolAdapterConfig,
};
use crate::builder::templates::SwapTemplateDirection;
use crate::builder::{BuilderHandle, BuilderStage, TemplateReadiness};
use crate::config::AppConfig;
use crate::core::{pin_current_thread, Lease, RingConsumer, RingProducer, SlabPool};
use crate::notifier::Notifier;
use crate::strategy::StrategyEngine;
use crate::telemetry::{self, Stage1BackpressureEvent, Stage1FeedGapEvent};

use super::graph_manager::{ArbitrageOpportunity, GraphManager, PathLeg, PoolPath};
use super::types::{
    compute_edge_bps, BundleCandidate, Opportunity, PoolBidirectionalQuotes, PoolQuoteDirection,
    PoolSnapshot, PoolUpdate, RouteLegPlan, RoutePlan, TradeUpdate,
};

const MAX_MULTI_LEG_PATHS: usize = 4;
const MAX_MULTI_LEG_HOPS: usize = 3;
const MAX_CLOSING_PATHS: usize = 2;
const PATH_PROFILE_LOG_INTERVAL_SECS: u64 = 10;
const OVERFLOW_QUEUE_CAPACITY: usize = 256;
const MAX_DEFERRED_TRADES_PER_POOL: usize = 64;

struct PathSearchProfile {
    total_duration: Duration,
    max_duration: Duration,
    sample_count: u64,
    last_log: Instant,
}

impl PathSearchProfile {
    fn new() -> Self {
        Self {
            total_duration: Duration::ZERO,
            max_duration: Duration::ZERO,
            sample_count: 0,
            last_log: Instant::now(),
        }
    }

    fn record(&mut self, duration: Duration) {
        self.total_duration += duration;
        if duration > self.max_duration {
            self.max_duration = duration;
        }
        self.sample_count += 1;
    }

    fn maybe_log(&mut self, path_count: usize) {
        if self.last_log.elapsed().as_secs() < PATH_PROFILE_LOG_INTERVAL_SECS {
            return;
        }
        if self.sample_count == 0 {
            self.last_log = Instant::now();
            return;
        }

        let divisor = self.sample_count.min(u32::MAX as u64) as u32;
        let avg_duration = if divisor > 0 {
            self.total_duration / divisor
        } else {
            Duration::ZERO
        };
        debug!(
            avg_duration_micros = avg_duration.as_micros(),
            max_duration_micros = self.max_duration.as_micros(),
            samples = self.sample_count,
            last_path_count = path_count,
            "stage1 path search profile",
        );

        self.total_duration = Duration::ZERO;
        self.max_duration = Duration::ZERO;
        self.sample_count = 0;
        self.last_log = Instant::now();
    }
}

pub fn spawn_eval_stage(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    config: Arc<AppConfig>,
    trade_consumer: RingConsumer<Lease<TradeUpdate>>,
    pool_consumer: RingConsumer<Lease<PoolUpdate>>,
    producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
    builder: BuilderHandle,
    notifier: Arc<Notifier>,
    strategy: Arc<StrategyEngine>,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("stage1_eval".into())
        .spawn(move || {
            run(
                stop,
                core_id,
                config,
                trade_consumer,
                pool_consumer,
                producer,
                bundle_pool,
                builder,
                notifier,
                strategy,
            )
        })
        .map_err(Into::into)
}

fn run(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    config: Arc<AppConfig>,
    trade_consumer: RingConsumer<Lease<TradeUpdate>>,
    pool_consumer: RingConsumer<Lease<PoolUpdate>>,
    producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
    builder: BuilderHandle,
    notifier: Arc<Notifier>,
    strategy: Arc<StrategyEngine>,
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    builder.template_cache().prewarm();
    builder.alt_manager().refresh_warm_cache();
    builder.flash_loan_planner().prepare_default_routes();

    let mut pool_snapshots: HashMap<String, PoolSnapshot> = HashMap::new();
    let mut pending_trades: HashMap<String, VecDeque<TradeUpdate>> = HashMap::new();
    let mut overflow_queue: VecDeque<BundleCandidate> =
        VecDeque::with_capacity(OVERFLOW_QUEUE_CAPACITY);
    let mut path_profile = PathSearchProfile::new();
    let mut opportunity_seq = 0u64;
    let adapters = AdapterBook::new(config, builder.clone());
    let mut graph_manager = GraphManager::new();
    let mut last_cycle_signatures: HashSet<String> = HashSet::new();

    info!("stage1 eval online");
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::EvalHotPath);
        let mut made_progress = false;

        if flush_overflow_queue(&mut overflow_queue, &bundle_pool, &producer) {
            made_progress = true;
        }

        if let Some(pool_lease) = pool_consumer.pop() {
            let update = pool_lease.into_inner();
            adapters.ingest(&update);
            let mut snapshot = update.into_snapshot();
            if let Some(quotes) = adapters.snapshot_quotes(&snapshot) {
                snapshot.quotes = Some(quotes);
            }
            graph_manager.index_pool(&snapshot);
            let pool_id = snapshot.pool_id.clone();
            pool_snapshots.insert(pool_id.clone(), snapshot);
            let cycles = graph_manager.latest_profitable_cycles();
            let cycle_signatures: HashSet<String> = cycles.iter().map(cycle_signature).collect();
            if cycles.is_empty() {
                last_cycle_signatures.clear();
            } else if cycle_signatures != last_cycle_signatures {
                if let Err(err) = notifier.notify_arbitrage(cycles) {
                    warn!(error = ?err, "failed to forward arbitrage notification");
                } else {
                    last_cycle_signatures = cycle_signatures;
                }
            }

            if let Some(mut queue) = pending_trades.remove(&pool_id) {
                while let Some(trade) = queue.pop_front() {
                    telemetry::record_stage1_feed_gap(Stage1FeedGapEvent::BacklogFlushed);
                    match process_trade_update(
                        trade,
                        pool_snapshots.get(&pool_id).cloned(),
                        &mut path_profile,
                        &mut graph_manager,
                        &pool_snapshots,
                        &adapters,
                        &builder,
                        strategy.as_ref(),
                        notifier.as_ref(),
                        &bundle_pool,
                        &producer,
                        &mut overflow_queue,
                        &mut opportunity_seq,
                    ) {
                        TradeProcessResult::Processed => {}
                        TradeProcessResult::MissingSnapshot(trade) => {
                            enqueue_pending_trade(&mut pending_trades, trade);
                            break;
                        }
                    }
                }
                if !queue.is_empty() {
                    pending_trades.insert(pool_id.clone(), queue);
                }
            }
            made_progress = true;
        }

        if let Some(trade_lease) = trade_consumer.pop() {
            let trade = trade_lease.into_inner();
            let snapshot = pool_snapshots.get(&trade.pool_id).cloned();
            match process_trade_update(
                trade,
                snapshot,
                &mut path_profile,
                &mut graph_manager,
                &pool_snapshots,
                &adapters,
                &builder,
                strategy.as_ref(),
                notifier.as_ref(),
                &bundle_pool,
                &producer,
                &mut overflow_queue,
                &mut opportunity_seq,
            ) {
                TradeProcessResult::Processed => {}
                TradeProcessResult::MissingSnapshot(trade) => {
                    telemetry::record_stage1_feed_gap(Stage1FeedGapEvent::MissingSnapshot);
                    enqueue_pending_trade(&mut pending_trades, trade);
                }
            }

            made_progress = true;
        }

        if !made_progress {
            thread::yield_now();
            thread::sleep(Duration::from_millis(5));
        }
    }

    info!("stage1 eval shutting down");
    Ok(())
}

fn enqueue_overflow(queue: &mut VecDeque<BundleCandidate>, candidate: BundleCandidate) -> bool {
    if queue.len() >= OVERFLOW_QUEUE_CAPACITY {
        warn!(
            seq = candidate.seq,
            "stage1 overflow queue full; dropping candidate"
        );
        false
    } else {
        queue.push_back(candidate);
        true
    }
}

fn dispatch_bundle_candidate(
    bundle_pool: &SlabPool<BundleCandidate>,
    producer: &RingProducer<Lease<BundleCandidate>>,
    candidate: BundleCandidate,
) -> Result<(), DispatchError> {
    let lease = bundle_pool
        .try_acquire_with(|| candidate.clone())
        .ok_or_else(|| DispatchError::SlabExhausted(candidate.clone()))?;
    producer
        .push(lease)
        .map_err(|lease| DispatchError::RingFull(lease.into_inner()))
}

fn flush_overflow_queue(
    queue: &mut VecDeque<BundleCandidate>,
    bundle_pool: &SlabPool<BundleCandidate>,
    producer: &RingProducer<Lease<BundleCandidate>>,
) -> bool {
    let mut made_progress = false;
    while let Some(candidate) = queue.pop_front() {
        match dispatch_bundle_candidate(bundle_pool, producer, candidate) {
            Ok(()) => {
                telemetry::record_stage1_backpressure(Stage1BackpressureEvent::OverflowFlushed);
                made_progress = true;
            }
            Err(err) => {
                let (event, candidate) = match err {
                    DispatchError::SlabExhausted(candidate) => {
                        (Stage1BackpressureEvent::SlabExhausted, candidate)
                    }
                    DispatchError::RingFull(candidate) => {
                        (Stage1BackpressureEvent::RingFull, candidate)
                    }
                };
                telemetry::record_stage1_backpressure(event);
                queue.push_front(candidate);
                break;
            }
        }
    }
    made_progress
}

fn enqueue_pending_trade(pending: &mut HashMap<String, VecDeque<TradeUpdate>>, trade: TradeUpdate) {
    let pool_id = trade.pool_id.clone();
    let queue = pending.entry(pool_id.clone()).or_default();
    if queue.len() >= MAX_DEFERRED_TRADES_PER_POOL {
        telemetry::record_stage1_feed_gap(Stage1FeedGapEvent::BacklogOverflow);
        warn!(
            pool = %pool_id,
            seq = trade.seq,
            "pending trade backlog full; dropping trade"
        );
        return;
    }

    queue.push_back(trade);
    telemetry::record_stage1_feed_gap(Stage1FeedGapEvent::BacklogQueued);
}

enum TradeProcessResult {
    Processed,
    MissingSnapshot(TradeUpdate),
}

fn process_trade_update(
    trade: TradeUpdate,
    snapshot: Option<PoolSnapshot>,
    path_profile: &mut PathSearchProfile,
    graph_manager: &mut GraphManager,
    pool_snapshots: &HashMap<String, PoolSnapshot>,
    adapters: &AdapterBook,
    builder: &BuilderHandle,
    strategy: &StrategyEngine,
    notifier: &Notifier,
    bundle_pool: &SlabPool<BundleCandidate>,
    producer: &RingProducer<Lease<BundleCandidate>>,
    overflow_queue: &mut VecDeque<BundleCandidate>,
    opportunity_seq: &mut u64,
) -> TradeProcessResult {
    let snapshot = match snapshot {
        Some(snapshot) => snapshot,
        None => return TradeProcessResult::MissingSnapshot(trade),
    };

    let quote_edge = adapters.quote_trade(&trade).map(|quote| {
        if quote.amount_in == 0 || quote.amount_out == 0 {
            return 0.0;
        }
        let quote_price = quote.amount_out as f64 / quote.amount_in as f64;
        compute_edge_bps(trade.price(), quote_price)
    });

    let path_search_start = Instant::now();
    let candidate_paths =
        candidate_paths_for_trade(graph_manager, &trade.sell_mint, &trade.buy_mint);
    let path_search_elapsed = path_search_start.elapsed();
    path_profile.record(path_search_elapsed);
    path_profile.maybe_log(candidate_paths.len());
    telemetry::record_path_search(path_search_elapsed, candidate_paths.len());

    if let Some(best_path) = candidate_paths.first() {
        let direct_pools = graph_manager.pools_for_pair(&trade.sell_mint, &trade.buy_mint);
        let best_ratio = graph_manager.path_price_ratio(best_path);
        debug!(
            path_len = best_path.len(),
            path_count = candidate_paths.len(),
            direct_pool_count = direct_pools.len(),
            best_ratio,
            sell_mint = %trade.sell_mint,
            buy_mint = %trade.buy_mint,
            "multi-leg route candidates discovered",
        );
    }

    let mut selected_route: Option<RoutePlan> = None;
    let mut fallback_route: Option<(RoutePlan, usize, usize)> = None;

    if trade.sell_amount > 0 {
        for path in &candidate_paths {
            let mut plan = match build_route_plan(path, pool_snapshots, trade.sell_amount) {
                Some(plan) => plan,
                None => continue,
            };
            annotate_route_leg_readiness(builder, &mut plan);
            if route_plan_serializable(&plan) {
                selected_route = Some(plan);
                break;
            }
            let score = route_readiness_score(&plan);
            let leg_count = plan.legs.len();
            let replace = match &fallback_route {
                Some((_, best_score, best_len)) => {
                    score > *best_score || (score == *best_score && leg_count < *best_len)
                }
                None => true,
            };
            if replace {
                fallback_route = Some((plan, score, leg_count));
            }
        }
    }

    if selected_route.is_none() {
        selected_route = fallback_route.map(|(plan, _, _)| plan);
    }

    let mut opportunity =
        Opportunity::from_trade(*opportunity_seq, trade, Some(snapshot), quote_edge);
    opportunity.route_plan = selected_route;
    *opportunity_seq = opportunity_seq.wrapping_add(1);

    if let Some(candidate) = strategy.evaluate(&opportunity) {
        if let Err(err) = notifier.notify_bundle_candidate(&candidate) {
            warn!(
                error = %err,
                seq = candidate.seq,
                "failed to send bundle candidate alert"
            );
        }
        match dispatch_bundle_candidate(bundle_pool, producer, candidate) {
            Ok(()) => {
                debug!(
                    opp_seq = opportunity.seq,
                    edge_bps = opportunity.estimated_edge_bps,
                    signature = %opportunity.trade.signature,
                    "emitted bundle candidate"
                );
            }
            Err(err) => {
                let (event, candidate) = match err {
                    DispatchError::SlabExhausted(candidate) => {
                        (Stage1BackpressureEvent::SlabExhausted, candidate)
                    }
                    DispatchError::RingFull(candidate) => {
                        (Stage1BackpressureEvent::RingFull, candidate)
                    }
                };
                telemetry::record_stage1_backpressure(event);
                if enqueue_overflow(overflow_queue, candidate) {
                    telemetry::record_stage1_backpressure(Stage1BackpressureEvent::OverflowQueued);
                } else {
                    telemetry::record_stage1_backpressure(Stage1BackpressureEvent::OverflowDropped);
                }
            }
        }
    }

    TradeProcessResult::Processed
}

fn annotate_route_leg_readiness(builder: &BuilderHandle, plan: &mut RoutePlan) {
    for leg in &mut plan.legs {
        if let Some(status) = readiness_for_leg(builder, leg) {
            leg.template_ready = status.template_ready;
            leg.alt_ready = status.alt_ready;
            leg.context.template_ready = status.template_ready;
            leg.context.alt_ready = status.alt_ready;
        } else {
            leg.template_ready = false;
            leg.alt_ready = false;
            leg.context.template_ready = false;
            leg.context.alt_ready = false;
        }
    }
}

fn readiness_for_leg(builder: &BuilderHandle, leg: &RouteLegPlan) -> Option<TemplateReadiness> {
    let pool = Pubkey::from_str(&leg.pool_id).ok()?;
    let direction = match leg.direction {
        PoolQuoteDirection::BaseToQuote => SwapTemplateDirection::BaseIn,
        PoolQuoteDirection::QuoteToBase => SwapTemplateDirection::BaseOut,
    };
    let fee_bps = leg.fee_bps.min(u16::MAX as u64) as u16;
    Some(builder.template_readiness(&pool, direction, fee_bps))
}

fn route_plan_serializable(plan: &RoutePlan) -> bool {
    plan.legs
        .iter()
        .all(|leg| leg.template_ready && leg.alt_ready)
}

fn route_readiness_score(plan: &RoutePlan) -> usize {
    plan.legs
        .iter()
        .filter(|leg| leg.template_ready && leg.alt_ready)
        .count()
}

enum DispatchError {
    SlabExhausted(BundleCandidate),
    RingFull(BundleCandidate),
}

fn cycle_signature(opportunity: &ArbitrageOpportunity) -> String {
    opportunity
        .path
        .legs
        .iter()
        .map(|leg| leg.pool_id.clone())
        .collect::<Vec<_>>()
        .join("->")
}

fn build_route_plan(
    path: &PoolPath,
    snapshots: &HashMap<String, PoolSnapshot>,
    initial_input_amount: u64,
) -> Option<RoutePlan> {
    if initial_input_amount == 0 {
        return None;
    }

    let mut legs = Vec::with_capacity(path.len());
    let mut running_amount = initial_input_amount;

    for leg in &path.legs {
        let snapshot = snapshots.get(&leg.pool_id)?;
        let direction = quote_direction_for_leg(snapshot, leg)?;
        let quote = snapshot.quote(direction)?.clone();
        let input_amount = running_amount;
        let estimated_output_amount = estimate_output_amount(input_amount, quote.price());
        let min_output_amount = quote
            .min_output_amount
            .and_then(|min_out| scale_quote_amount(min_out, quote.notional, input_amount));
        let limit_price = quote.limit_price;
        let price_impact_limit_bps = quote.price_impact_limit_bps;
        running_amount = estimated_output_amount;

        legs.push(RouteLegPlan {
            pool_id: leg.pool_id.clone(),
            program_id: leg.program_id.clone(),
            input_mint: leg.input_mint.clone(),
            output_mint: leg.output_mint.clone(),
            input_amount,
            estimated_output_amount,
            effective_price: quote.price(),
            fee_bps: quote.fee_bps,
            estimated_slippage_bps: quote.slippage_bps(),
            direction,
            sqrt_price_limit: quote.sqrt_price_limit,
            min_output_amount,
            limit_price,
            price_impact_limit_bps,
            context: quote.context.clone(),
            template_ready: false,
            alt_ready: false,
        });
    }

    Some(RoutePlan {
        start_mint: path.start_mint.clone(),
        end_mint: path.end_mint.clone(),
        legs,
        estimated_final_output_amount: Some(running_amount),
    })
}

fn candidate_paths_for_trade(
    graph_manager: &GraphManager,
    sell_mint: &str,
    buy_mint: &str,
) -> Vec<PoolPath> {
    let mut candidates = Vec::new();

    let cycles =
        graph_manager.find_cycles_through(sell_mint, MAX_MULTI_LEG_HOPS, MAX_MULTI_LEG_PATHS);
    for cycle in cycles {
        candidates.push(cycle);
        if candidates.len() >= MAX_MULTI_LEG_PATHS {
            return candidates;
        }
    }

    let directional_paths =
        graph_manager.find_paths(sell_mint, buy_mint, MAX_MULTI_LEG_HOPS, MAX_MULTI_LEG_PATHS);
    for path in directional_paths {
        if candidates.len() >= MAX_MULTI_LEG_PATHS {
            break;
        }
        if let Some(closed) = close_path_to_start(&path, graph_manager) {
            candidates.push(closed);
        }
    }

    candidates
}

fn close_path_to_start(path: &PoolPath, graph_manager: &GraphManager) -> Option<PoolPath> {
    if path.start_mint == path.end_mint {
        return Some(path.clone());
    }

    let remaining_hops = MAX_MULTI_LEG_HOPS.saturating_sub(path.len());
    if remaining_hops == 0 {
        return None;
    }

    let closing_paths = graph_manager.find_paths(
        &path.end_mint,
        &path.start_mint,
        remaining_hops,
        MAX_CLOSING_PATHS,
    );

    for closing in closing_paths {
        let total_len = path.len() + closing.len();
        if total_len > MAX_MULTI_LEG_HOPS {
            continue;
        }
        let mut legs = Vec::with_capacity(total_len);
        legs.extend(path.legs.iter().cloned());
        legs.extend(closing.legs.into_iter());
        return Some(PoolPath {
            start_mint: path.start_mint.clone(),
            end_mint: path.start_mint.clone(),
            legs,
        });
    }

    None
}

fn quote_direction_for_leg(snapshot: &PoolSnapshot, leg: &PathLeg) -> Option<PoolQuoteDirection> {
    if leg.input_mint == snapshot.base_mint && leg.output_mint == snapshot.quote_mint {
        Some(PoolQuoteDirection::BaseToQuote)
    } else if leg.input_mint == snapshot.quote_mint && leg.output_mint == snapshot.base_mint {
        Some(PoolQuoteDirection::QuoteToBase)
    } else {
        None
    }
}

fn estimate_output_amount(input: u64, price: f64) -> u64 {
    if price <= 0.0 {
        return 0;
    }
    let output = (input as f64) * price;
    if output.is_nan() || output.is_infinite() {
        return 0;
    }
    let clamped = output.round().max(0.0).min(u64::MAX as f64);
    clamped.floor() as u64
}

fn scale_quote_amount(amount: u64, base_notional: u64, target_notional: u64) -> Option<u64> {
    if base_notional == 0 {
        return None;
    }
    let scaled = (amount as u128)
        .saturating_mul(target_notional as u128)
        .checked_div(base_notional as u128)?;
    Some(scaled.min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::graph_manager::PoolPath;
    use crate::stages::types::{PoolBidirectionalQuotes, PoolQuoteSide, PoolUpdateExtensions};
    use crate::template::TemplateContext;
    use std::collections::HashMap;
    use std::time::Instant;

    fn quote_side(input: &str, output: &str, price: f64) -> PoolQuoteSide {
        PoolQuoteSide {
            input_mint: input.into(),
            output_mint: output.into(),
            notional: 100,
            expected_output: (100.0 * price) as u64,
            fee_bps: 30,
            effective_price: price,
            estimated_slippage_bps: 5.0,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: Some(30),
            context: TemplateContext::default(),
        }
    }

    fn snapshot_with_quotes(
        pool_id: &str,
        base: &str,
        quote: &str,
        base_price: f64,
        quote_price: f64,
    ) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: pool_id.into(),
            program_id: "prog".into(),
            slot: 0,
            base_mint: base.into(),
            quote_mint: quote.into(),
            base_post_amount: 1,
            quote_post_amount: 1,
            updated_at: Instant::now(),
            quotes: Some(PoolBidirectionalQuotes {
                base_to_quote: quote_side(base, quote, base_price),
                quote_to_base: quote_side(quote, base, quote_price),
            }),
            extensions: PoolUpdateExtensions::default(),
        }
    }

    #[test]
    fn builds_two_leg_route_plan() {
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "pool-ab".into(),
            snapshot_with_quotes("pool-ab", "A", "B", 2.0, 0.5),
        );
        snapshots.insert(
            "pool-bc".into(),
            snapshot_with_quotes("pool-bc", "B", "C", 0.5, 2.0),
        );

        let path = PoolPath {
            start_mint: "A".into(),
            end_mint: "C".into(),
            legs: vec![
                PathLeg {
                    pool_id: "pool-ab".into(),
                    program_id: "prog".into(),
                    input_mint: "A".into(),
                    output_mint: "B".into(),
                },
                PathLeg {
                    pool_id: "pool-bc".into(),
                    program_id: "prog".into(),
                    input_mint: "B".into(),
                    output_mint: "C".into(),
                },
            ],
        };

        let plan = build_route_plan(&path, &snapshots, 100).expect("route plan");
        assert_eq!(plan.legs.len(), 2);
        assert_eq!(plan.legs[0].input_amount, 100);
        assert_eq!(plan.legs[0].estimated_output_amount, 200);
        assert_eq!(plan.legs[1].input_amount, 200);
        assert_eq!(plan.legs[1].estimated_output_amount, 100);
        assert_eq!(plan.estimated_final_output_amount, Some(100));
    }
}

struct AdapterBook {
    adapters: HashMap<String, Arc<dyn ProtocolAdapter>>, // key = program id string
}

impl AdapterBook {
    fn new(config: Arc<AppConfig>, builder: BuilderHandle) -> Self {
        let mut adapters: HashMap<String, Arc<dyn ProtocolAdapter>> = HashMap::new();

        register_adapter(
            &mut adapters,
            Arc::new(RaydiumV3Adapter::new(
                raydium_config(config.as_ref()),
                Some(builder.clone()),
            )),
        );
        register_adapter(
            &mut adapters,
            Arc::new(CpAmmAdapter::new(
                cp_amm_config(config.as_ref()),
                Some(builder.clone()),
            )),
        );
        register_adapter(
            &mut adapters,
            Arc::new(LbClmmAdapter::new(
                lb_clmm_config(config.as_ref()),
                Some(builder.clone()),
            )),
        );
        register_adapter(
            &mut adapters,
            Arc::new(LifinityAdapter::new(
                lifinity_config(config.as_ref()),
                Some(builder.clone()),
            )),
        );
        register_adapter(
            &mut adapters,
            Arc::new(WhirlpoolAdapter::new(
                whirlpool_config(config.as_ref()),
                Some(builder.clone()),
            )),
        );

        Self { adapters }
    }

    fn ingest(&self, update: &PoolUpdate) {
        if let Some(adapter) = self.adapters.get(&update.program_id) {
            adapter.ingest_pool_update(update);
        }
    }

    fn snapshot_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let adapter = self.adapters.get(&snapshot.program_id)?;
        adapter.bidirectional_quotes(snapshot)
    }

    fn quote_trade(&self, trade: &TradeUpdate) -> Option<QuoteResponse> {
        let adapter = self.adapters.get(&trade.program_id)?;
        let pool_id = Pubkey::from_str(&trade.pool_id).ok()?;
        let input_mint = Pubkey::from_str(&trade.sell_mint).ok()?;
        let output_mint = Pubkey::from_str(&trade.buy_mint).ok()?;

        let request = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: trade.sell_amount,
            slot: trade.slot,
            direction: SwapDirection::ExactIn,
        };

        adapter.quote_exact_in(request)
    }
}

fn register_adapter(
    map: &mut HashMap<String, Arc<dyn ProtocolAdapter>>,
    adapter: Arc<dyn ProtocolAdapter>,
) {
    map.insert(adapter.program_id().to_string(), adapter);
}

fn raydium_config(config: &AppConfig) -> RaydiumV3AdapterConfig {
    let mut adapter_config = RaydiumV3AdapterConfig::default();
    adapter_config.rpc_endpoints = config.protocol_rpc_endpoints("raydium_v3");
    adapter_config
}

fn cp_amm_config(config: &AppConfig) -> CpAmmAdapterConfig {
    let mut adapter_config = CpAmmAdapterConfig::default();
    adapter_config.rpc_endpoints = config.protocol_rpc_endpoints("cp_amm");
    adapter_config
}

fn lb_clmm_config(config: &AppConfig) -> LbClmmAdapterConfig {
    let mut adapter_config = LbClmmAdapterConfig::default();
    adapter_config.rpc_endpoints = config.protocol_rpc_endpoints("lb_clmm");
    adapter_config
}

fn lifinity_config(config: &AppConfig) -> LifinityAdapterConfig {
    let mut adapter_config = LifinityAdapterConfig::default();
    adapter_config.rpc_endpoints = config.protocol_rpc_endpoints("lifinity_v2");
    adapter_config
}

fn whirlpool_config(config: &AppConfig) -> WhirlpoolAdapterConfig {
    let mut adapter_config = WhirlpoolAdapterConfig::default();
    adapter_config.rpc_endpoints = config.protocol_rpc_endpoints("whirlpool");
    adapter_config
}
