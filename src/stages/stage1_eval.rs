use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
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
use crate::telemetry::{self, Stage1BackpressureEvent, Stage1FeedGapEvent, Stage1PoolDrainEvent};

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
const RESERVE_HYDRATE_TIMEOUT_SECS: u64 = 8;
const RESERVE_HYDRATE_INTERVAL_SECS: u64 = 15;
const READINESS_RETRY_DELAY_MS: u64 = 50;
const MAX_PENDING_READINESS_TRADES: usize = 512;
const MAX_READINESS_RETRIES: u32 = 24;

struct PathSearchProfile {
    total_duration: Duration,
    max_duration: Duration,
    sample_count: u64,
    last_log: Instant,
}

struct PendingReadinessTrade {
    trade: TradeUpdate,
    attempt: u32,
    ready_at: Instant,
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
    let alt_manager = builder.alt_manager();
    match alt_manager.refresh_warm_cache() {
        Ok(()) => {
            let stats = alt_manager.stats();
            info!(
                target = "alt_manager",
                ready_tables = stats.ready_tables,
                registered_handles = stats.registered_handles,
                pending_jobs = stats.pending_jobs,
                refresh_count = stats.refresh_count,
                last_refresh_unix_ms = stats.last_refresh_unix_ms,
                "ALT warm cache refreshed"
            );
        }
        Err(err) => {
            warn!(target = "alt_manager", error = ?err, "failed to refresh ALT warm cache");
        }
    }
    builder.flash_loan_planner().prepare_default_routes();
    if !hydrate_flash_loan_reserves(&builder, config.as_ref()) {
        warn!("initial flash-loan reserve hydration failed");
    }
    let reserve_hydrator = spawn_reserve_hydrator(stop.clone(), builder.clone(), config.clone());

    let mut pool_snapshots: HashMap<String, PoolSnapshot> = HashMap::new();
    let mut pending_trades: HashMap<String, VecDeque<TradeUpdate>> = HashMap::new();
    let mut overflow_queue: VecDeque<BundleCandidate> =
        VecDeque::with_capacity(OVERFLOW_QUEUE_CAPACITY);
    let mut pending_readiness: VecDeque<PendingReadinessTrade> =
        VecDeque::with_capacity(MAX_PENDING_READINESS_TRADES);
    let mut path_profile = PathSearchProfile::new();
    let mut opportunity_seq = 0u64;
    let adapters = AdapterBook::new(config, builder.clone());
    let mut graph_manager = GraphManager::new();
    let mut last_cycle_signatures: HashSet<String> = HashSet::new();
    let handle_pool_update = |update: PoolUpdate,
                              pool_snapshots: &mut HashMap<String, PoolSnapshot>,
                              pending_trades: &mut HashMap<String, VecDeque<TradeUpdate>>,
                              graph_manager: &mut GraphManager,
                              last_cycle_signatures: &mut HashSet<String>,
                              adapters: &AdapterBook,
                              notifier: &Notifier,
                              path_profile: &mut PathSearchProfile,
                              builder: &BuilderHandle,
                              strategy: &StrategyEngine,
                              bundle_pool: &SlabPool<BundleCandidate>,
                              producer: &RingProducer<Lease<BundleCandidate>>,
                              overflow_queue: &mut VecDeque<BundleCandidate>,
                              pending_readiness: &mut VecDeque<PendingReadinessTrade>,
                              opportunity_seq: &mut u64| {
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
        } else if cycle_signatures != *last_cycle_signatures {
            if let Err(err) = notifier.notify_arbitrage(cycles) {
                warn!(error = ?err, "failed to forward arbitrage notification");
            } else {
                *last_cycle_signatures = cycle_signatures;
            }
        }

        if let Some(mut queue) = pending_trades.remove(&pool_id) {
            while let Some(trade) = queue.pop_front() {
                telemetry::record_stage1_feed_gap(Stage1FeedGapEvent::BacklogFlushed);
                match process_trade_update(
                    trade,
                    pool_snapshots.get(&pool_id).cloned(),
                    path_profile,
                    graph_manager,
                    pool_snapshots,
                    adapters,
                    builder,
                    strategy,
                    notifier,
                    bundle_pool,
                    producer,
                    overflow_queue,
                    opportunity_seq,
                ) {
                    TradeProcessResult::Processed => {}
                    TradeProcessResult::MissingSnapshot(trade) => {
                        enqueue_pending_trade(pending_trades, trade);
                        break;
                    }
                    TradeProcessResult::PendingReadiness(trade) => {
                        schedule_readiness_retry(pending_readiness, trade, 0);
                        break;
                    }
                }
            }
            if !queue.is_empty() {
                pending_trades.insert(pool_id, queue);
            }
        }
    };

    info!("stage1 eval online");
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::EvalHotPath);
        let mut made_progress = false;

        if flush_overflow_queue(&mut overflow_queue, &bundle_pool, &producer) {
            made_progress = true;
        }
        if flush_pending_readiness_queue(
            &mut pending_readiness,
            &mut path_profile,
            &mut graph_manager,
            &pool_snapshots,
            &mut pending_trades,
            &adapters,
            &builder,
            strategy.as_ref(),
            notifier.as_ref(),
            &bundle_pool,
            &producer,
            &mut overflow_queue,
            &mut opportunity_seq,
        ) {
            made_progress = true;
        }

        if pending_trades.is_empty() {
            if let Some(pool_lease) = pool_consumer.pop() {
                handle_pool_update(
                    pool_lease.into_inner(),
                    &mut pool_snapshots,
                    &mut pending_trades,
                    &mut graph_manager,
                    &mut last_cycle_signatures,
                    &adapters,
                    notifier.as_ref(),
                    &mut path_profile,
                    &builder,
                    strategy.as_ref(),
                    &bundle_pool,
                    &producer,
                    &mut overflow_queue,
                    &mut pending_readiness,
                    &mut opportunity_seq,
                );
                made_progress = true;
            }
        } else {
            let mut drained_count = 0;
            while let Some(pool_lease) = pool_consumer.pop() {
                handle_pool_update(
                    pool_lease.into_inner(),
                    &mut pool_snapshots,
                    &mut pending_trades,
                    &mut graph_manager,
                    &mut last_cycle_signatures,
                    &adapters,
                    notifier.as_ref(),
                    &mut path_profile,
                    &builder,
                    strategy.as_ref(),
                    &bundle_pool,
                    &producer,
                    &mut overflow_queue,
                    &mut pending_readiness,
                    &mut opportunity_seq,
                );
                drained_count += 1;
            }
            if drained_count > 0 {
                let drain_event = if drained_count == 1 {
                    Stage1PoolDrainEvent::Single
                } else {
                    Stage1PoolDrainEvent::Multiple
                };
                telemetry::record_stage1_pool_drain(drain_event);
                made_progress = true;
            }
        }

        if pending_trades.is_empty() {
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
                    TradeProcessResult::PendingReadiness(trade) => {
                        schedule_readiness_retry(&mut pending_readiness, trade, 0);
                    }
                }

                made_progress = true;
            }
        } else {
            telemetry::record_stage1_backpressure(Stage1BackpressureEvent::TradeIntakePaused);
        }

        if !made_progress {
            thread::yield_now();
            thread::sleep(Duration::from_millis(5));
        }
    }

    info!("stage1 eval shutting down");
    if let Some(handle) = reserve_hydrator {
        if let Err(err) = handle.join() {
            warn!(error = ?err, "flash-reserve hydrator thread panicked");
        }
    }
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
    PendingReadiness(TradeUpdate),
}

fn hydrate_flash_loan_reserves(builder: &BuilderHandle, config: &AppConfig) -> bool {
    if config.quicknode_rpcs.is_empty() {
        warn!("no RPC endpoint configured for flash-loan reserve hydration");
        return false;
    }

    for endpoint in &config.quicknode_rpcs {
        let client = RpcClient::new_with_timeout(
            endpoint.clone(),
            Duration::from_secs(RESERVE_HYDRATE_TIMEOUT_SECS),
        );
        match builder
            .flash_loan_planner()
            .refresh_reserve_liquidity(|supply| {
                let balance = client
                    .get_token_account_balance(supply)
                    .with_context(|| format!("failed to fetch reserve liquidity for {supply}"))?;
                let raw = balance
                    .amount
                    .parse::<u128>()
                    .with_context(|| format!("invalid token amount for {supply}"))?;
                Ok(raw.min(u64::MAX as u128) as u64)
            }) {
            Ok(()) => {
                return true;
            }
            Err(err) => {
                warn!(
                    endpoint = endpoint.as_str(),
                    error = ?err,
                    "failed to hydrate flash-loan reserve liquidity"
                );
            }
        }
    }

    warn!("exhausted all RPC endpoints while hydrating flash-loan reserves");
    false
}

fn desired_route_input_amount(
    trade: &TradeUpdate,
    builder: &BuilderHandle,
    strategy: &StrategyEngine,
) -> Option<u64> {
    let mut desired = trade.sell_amount;
    if let Some(budget) = strategy.flash_trade_budget(&trade.sell_mint) {
        desired = desired.max(budget.trade_input_lamports);
    }
    if desired == 0 {
        return None;
    }

    let mint = match Pubkey::from_str(&trade.sell_mint) {
        Ok(pk) => pk,
        Err(err) => {
            debug!(
                sell_mint = %trade.sell_mint,
                error = %err,
                "unable to parse sell mint for flash-loan sizing; using trade amount"
            );
            return Some(desired);
        }
    };

    let planner = builder.flash_loan_planner();
    if !planner.supports_mint(&mint) {
        return Some(desired);
    }

    if let Some(headroom) = planner.reserve_headroom_for_mint(&mint) {
        if headroom == 0 {
            return None;
        }
        let tip_buffer = strategy.tip_buffer_hint(&trade.sell_mint);
        if headroom <= tip_buffer {
            debug!(
                sell_mint = %trade.sell_mint,
                headroom,
                tip_buffer,
                "reserve headroom exhausted after accounting for max tip buffer"
            );
            return None;
        }
        let capped = headroom.saturating_sub(tip_buffer);
        if desired > capped {
            debug!(
                sell_mint = %trade.sell_mint,
                desired,
                capped,
                tip_buffer,
                headroom,
                "clamping trade input to reserve headroom",
            );
            desired = capped;
        }
    }

    if desired == 0 {
        None
    } else {
        Some(desired)
    }
}

fn readiness_retry_delay(attempt: u32) -> Duration {
    let base = READINESS_RETRY_DELAY_MS.max(1);
    let shift = attempt.saturating_sub(1).min(4);
    Duration::from_millis(base * (1u64 << shift))
}

fn schedule_readiness_retry(
    queue: &mut VecDeque<PendingReadinessTrade>,
    trade: TradeUpdate,
    attempt: u32,
) {
    if attempt >= MAX_READINESS_RETRIES {
        warn!(
            pool = %trade.pool_id,
            sell_mint = %trade.sell_mint,
            attempt,
            "dropping trade after exhausting template readiness retries"
        );
        return;
    }
    if queue.len() >= MAX_PENDING_READINESS_TRADES {
        warn!(
            pool = %trade.pool_id,
            sell_mint = %trade.sell_mint,
            "pending readiness queue full; dropping trade"
        );
        telemetry::record_stage1_backpressure(Stage1BackpressureEvent::RingFull);
        return;
    }

    let next_attempt = attempt.saturating_add(1);
    let ready_at = Instant::now() + readiness_retry_delay(next_attempt);
    queue.push_back(PendingReadinessTrade {
        trade,
        attempt: next_attempt,
        ready_at,
    });
}

fn flush_pending_readiness_queue(
    queue: &mut VecDeque<PendingReadinessTrade>,
    path_profile: &mut PathSearchProfile,
    graph_manager: &mut GraphManager,
    pool_snapshots: &HashMap<String, PoolSnapshot>,
    pending_trades: &mut HashMap<String, VecDeque<TradeUpdate>>,
    adapters: &AdapterBook,
    builder: &BuilderHandle,
    strategy: &StrategyEngine,
    notifier: &Notifier,
    bundle_pool: &SlabPool<BundleCandidate>,
    producer: &RingProducer<Lease<BundleCandidate>>,
    overflow_queue: &mut VecDeque<BundleCandidate>,
    opportunity_seq: &mut u64,
) -> bool {
    let mut made_progress = false;
    while let Some(pending) = queue.front() {
        if pending.ready_at > Instant::now() {
            break;
        }
        let pending = queue.pop_front().expect("pending readiness entry");
        let snapshot = pool_snapshots.get(&pending.trade.pool_id).cloned();
        match process_trade_update(
            pending.trade,
            snapshot,
            path_profile,
            graph_manager,
            pool_snapshots,
            adapters,
            builder,
            strategy,
            notifier,
            bundle_pool,
            producer,
            overflow_queue,
            opportunity_seq,
        ) {
            TradeProcessResult::Processed => made_progress = true,
            TradeProcessResult::MissingSnapshot(trade) => {
                telemetry::record_stage1_feed_gap(Stage1FeedGapEvent::MissingSnapshot);
                enqueue_pending_trade(pending_trades, trade);
            }
            TradeProcessResult::PendingReadiness(trade) => {
                schedule_readiness_retry(queue, trade, pending.attempt);
            }
        }
    }
    made_progress
}

fn spawn_reserve_hydrator(
    stop: Arc<AtomicBool>,
    builder: BuilderHandle,
    config: Arc<AppConfig>,
) -> Option<thread::JoinHandle<()>> {
    if config.flash_loans.is_empty() || config.quicknode_rpcs.is_empty() {
        return None;
    }
    let interval = Duration::from_secs(RESERVE_HYDRATE_INTERVAL_SECS.max(1));
    thread::Builder::new()
        .name("flash_reserve_hydrator".into())
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let loop_start = Instant::now();
                let success = hydrate_flash_loan_reserves(&builder, config.as_ref());
                if !success {
                    thread::sleep(Duration::from_secs(RESERVE_HYDRATE_TIMEOUT_SECS));
                    continue;
                }
                let elapsed = loop_start.elapsed();
                if elapsed < interval {
                    thread::sleep(interval - elapsed);
                }
            }
        })
        .map_err(|err| {
            warn!(error = %err, "failed to spawn flash-reserve hydrator thread");
            err
        })
        .ok()
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

    let planned_input_amount =
        desired_route_input_amount(&trade, builder, strategy).unwrap_or(trade.sell_amount);
    if planned_input_amount == 0 {
        return TradeProcessResult::Processed;
    }
    if planned_input_amount != trade.sell_amount {
        debug!(
            sell_mint = %trade.sell_mint,
            observed_amount = trade.sell_amount,
            planned_amount = planned_input_amount,
            "overriding trade input amount for flash-loan sizing",
        );
    }

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

    if planned_input_amount > 0 {
        for path in &candidate_paths {
            let mut plan = match build_route_plan(path, pool_snapshots, planned_input_amount) {
                Some(plan) => plan,
                None => continue,
            };
            annotate_route_leg_readiness(builder, &mut plan);
            plan.flash_loan_permitted = route_allows_flash_loans(&plan, builder);
            if !plan.flash_loan_permitted {
                continue;
            }
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

    let selected_route = match selected_route {
        Some(plan) => plan,
        None => {
            if let Some((_, ready_legs, total_legs)) = fallback_route {
                debug!(
                    sell_mint = %trade.sell_mint,
                    buy_mint = %trade.buy_mint,
                    ready_legs,
                    total_legs,
                    "deferring trade until templates/ALTs are ready"
                );
                return TradeProcessResult::PendingReadiness(trade);
            } else {
                return TradeProcessResult::Processed;
            }
        }
    };

    let mut opportunity =
        Opportunity::from_trade(*opportunity_seq, trade, Some(snapshot), quote_edge);
    opportunity.route_plan = Some(selected_route);
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
            if !(status.template_ready && status.alt_ready) {
                debug!(
                    pool = %leg.pool_id,
                    direction = ?leg.direction,
                    template_ready = status.template_ready,
                    alt_ready = status.alt_ready,
                    "route leg blocked waiting on template hydration or ALT",
                );
            }
        } else {
            leg.template_ready = false;
            leg.alt_ready = false;
            leg.context.template_ready = false;
            leg.context.alt_ready = false;
            debug!(
                pool = %leg.pool_id,
                direction = ?leg.direction,
                "route leg missing template metadata"
            );
        }
    }
}

fn route_allows_flash_loans(plan: &RoutePlan, builder: &BuilderHandle) -> bool {
    if plan.legs.is_empty() {
        return false;
    }
    if plan.start_mint != plan.end_mint {
        debug!(
            start_mint = %plan.start_mint,
            end_mint = %plan.end_mint,
            "route start/end mint mismatch; cannot run flash loan",
        );
        return false;
    }
    let first_leg = &plan.legs[0];
    if first_leg.input_amount == 0 {
        debug!(
            pool = %first_leg.pool_id,
            "first leg input amount is zero; cannot size flash borrow",
        );
        return false;
    }
    if first_leg.input_mint != plan.start_mint {
        debug!(
            start_mint = %plan.start_mint,
            input_mint = %first_leg.input_mint,
            "route first leg input mint does not match borrow mint",
        );
        return false;
    }

    let start_mint = match Pubkey::from_str(&plan.start_mint) {
        Ok(pk) => pk,
        Err(err) => {
            debug!(
                start_mint = %plan.start_mint,
                error = %err,
                "unable to parse route start mint for flash-loan check",
            );
            return false;
        }
    };
    let receiver_program = match Pubkey::from_str(&first_leg.program_id) {
        Ok(pk) => pk,
        Err(err) => {
            debug!(
                program_id = %first_leg.program_id,
                error = %err,
                "unable to parse receiver program for flash-loan check",
            );
            return false;
        }
    };

    let planner = builder.flash_loan_planner();
    if !planner.supports_mint(&start_mint) {
        debug!(
            start_mint = %plan.start_mint,
            "flash-loan planner does not support route mint",
        );
        return false;
    }
    if !planner.receiver_program_allowed(&start_mint, &receiver_program) {
        debug!(
            start_mint = %plan.start_mint,
            program_id = %first_leg.program_id,
            "flash-loan mint does not permit receiver program",
        );
        return false;
    }

    true
}

fn readiness_for_leg(builder: &BuilderHandle, leg: &RouteLegPlan) -> Option<TemplateReadiness> {
    let pool = Pubkey::from_str(&leg.pool_id).ok()?;
    let direction = match leg.direction {
        PoolQuoteDirection::BaseToQuote => SwapTemplateDirection::BaseIn,
        PoolQuoteDirection::QuoteToBase => SwapTemplateDirection::BaseOut,
    };
    Some(builder.template_readiness(&pool, direction))
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
            template_fee_bps: quote.template_fee_bps,
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
        flash_loan_permitted: false,
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
            template_fee_bps: 30,
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
    let mut adapter_config = RaydiumV3AdapterConfig {
        rpc_endpoints: config.protocol_rpc_endpoints("raydium_v3"),
        ..Default::default()
    };
    if let Some(retry_ms) = config.protocol_hydration_retry_ms("raydium_v3") {
        adapter_config.hydration_retry_ms = retry_ms.max(1);
    }
    adapter_config
}

fn cp_amm_config(config: &AppConfig) -> CpAmmAdapterConfig {
    let mut adapter_config = CpAmmAdapterConfig {
        rpc_endpoints: config.protocol_rpc_endpoints("cp_amm"),
        ..Default::default()
    };
    if let Some(retry_ms) = config.protocol_hydration_retry_ms("cp_amm") {
        adapter_config.hydration_retry_ms = retry_ms.max(1);
    }
    adapter_config
}

fn lb_clmm_config(config: &AppConfig) -> LbClmmAdapterConfig {
    let mut adapter_config = LbClmmAdapterConfig {
        rpc_endpoints: config.protocol_rpc_endpoints("lb_clmm"),
        ..Default::default()
    };
    if let Some(retry_ms) = config.protocol_hydration_retry_ms("lb_clmm") {
        adapter_config.hydration_retry_ms = retry_ms.max(1);
    }
    adapter_config
}

fn lifinity_config(config: &AppConfig) -> LifinityAdapterConfig {
    let mut adapter_config = LifinityAdapterConfig {
        rpc_endpoints: config.protocol_rpc_endpoints("lifinity_v2"),
        ..Default::default()
    };
    if let Some(retry_ms) = config.protocol_hydration_retry_ms("lifinity_v2") {
        adapter_config.hydration_retry_ms = retry_ms.max(1);
    }
    adapter_config
}

fn whirlpool_config(config: &AppConfig) -> WhirlpoolAdapterConfig {
    let mut adapter_config = WhirlpoolAdapterConfig {
        rpc_endpoints: config.protocol_rpc_endpoints("whirlpool"),
        ..Default::default()
    };
    if let Some(retry_ms) = config.protocol_hydration_retry_ms("whirlpool") {
        adapter_config.hydration_retry_ms = retry_ms.max(1);
    }
    adapter_config
}
