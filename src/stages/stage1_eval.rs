use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, warn};

use crate::adapters::{
    CpAmmAdapter, CpAmmAdapterConfig, LbClmmAdapter, LbClmmAdapterConfig, LifinityAdapter,
    LifinityAdapterConfig, ProtocolAdapter, QuoteRequest, QuoteResponse, RaydiumV3Adapter,
    RaydiumV3AdapterConfig, SwapDirection, WhirlpoolAdapter, WhirlpoolAdapterConfig,
};
use crate::builder::{BuilderHandle, BuilderStage};
use crate::config::AppConfig;
use crate::core::{pin_current_thread, Lease, RingConsumer, RingProducer, SlabPool};
use crate::notifier::Notifier;
use crate::strategy::StrategyEngine;

use super::graph_manager::{ArbitrageOpportunity, GraphManager, PathLeg, PoolPath};
use super::types::{
    compute_edge_bps, BundleCandidate, Opportunity, PoolBidirectionalQuotes, PoolQuoteDirection,
    PoolSnapshot, PoolUpdate, RouteLegPlan, RoutePlan, TradeUpdate,
};

const MAX_MULTI_LEG_PATHS: usize = 4;
const MAX_MULTI_LEG_HOPS: usize = 3;

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
    let mut opportunity_seq = 0u64;
    let adapters = AdapterBook::new(config);
    let mut graph_manager = GraphManager::new();
    let mut last_cycle_signatures: HashSet<String> = HashSet::new();

    info!("stage1 eval online");
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::EvalHotPath);
        let mut made_progress = false;

        if let Some(pool_lease) = pool_consumer.pop() {
            let update = pool_lease.into_inner();
            adapters.ingest(&update);
            let mut snapshot = update.into_snapshot();
            if let Some(quotes) = adapters.snapshot_quotes(&snapshot) {
                snapshot.quotes = Some(quotes);
            }
            graph_manager.index_pool(&snapshot);
            pool_snapshots.insert(snapshot.pool_id.clone(), snapshot);
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
            made_progress = true;
        }

        if let Some(trade_lease) = trade_consumer.pop() {
            let trade = trade_lease.into_inner();
            let snapshot = pool_snapshots.get(&trade.pool_id).cloned();
            let quote_edge = adapters.quote_trade(&trade).map(|quote| {
                if quote.amount_in == 0 || quote.amount_out == 0 {
                    return 0.0;
                }
                let quote_price = quote.amount_out as f64 / quote.amount_in as f64;
                compute_edge_bps(trade.price(), quote_price)
            });
            let multi_leg_paths = graph_manager.find_paths(
                &trade.sell_mint,
                &trade.buy_mint,
                MAX_MULTI_LEG_HOPS,
                MAX_MULTI_LEG_PATHS,
            );
            let mut selected_route: Option<RoutePlan> = None;
            if let Some(best_path) = multi_leg_paths.first() {
                let direct_pools = graph_manager.pools_for_pair(&trade.sell_mint, &trade.buy_mint);
                let best_ratio = graph_manager.path_price_ratio(best_path);
                if trade.sell_amount > 0 {
                    selected_route =
                        build_route_plan(best_path, &pool_snapshots, trade.sell_amount);
                }
                debug!(
                    path_len = best_path.len(),
                    path_count = multi_leg_paths.len(),
                    direct_pool_count = direct_pools.len(),
                    best_ratio,
                    sell_mint = %trade.sell_mint,
                    buy_mint = %trade.buy_mint,
                    "multi-leg route candidates discovered",
                );
            }
            let mut opportunity =
                Opportunity::from_trade(opportunity_seq, trade, snapshot, quote_edge);
            opportunity.route_plan = selected_route;
            opportunity_seq = opportunity_seq.wrapping_add(1);

            if let Some(candidate) = strategy.evaluate(&opportunity) {
                if let Some(lease) = bundle_pool.try_acquire_with(|| candidate) {
                    if producer.push(lease).is_ok() {
                        debug!(
                            opp_seq = opportunity.seq,
                            edge_bps = opportunity.estimated_edge_bps,
                            signature = %opportunity.trade.signature,
                            "emitted bundle candidate"
                        );
                    }
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
        });
    }

    Some(RoutePlan {
        start_mint: path.start_mint.clone(),
        end_mint: path.end_mint.clone(),
        legs,
        estimated_final_output_amount: Some(running_amount),
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::graph_manager::PoolPath;
    use crate::stages::types::{PoolBidirectionalQuotes, PoolQuoteSide, PoolUpdateExtensions};
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
    fn new(config: Arc<AppConfig>) -> Self {
        let mut adapters: HashMap<String, Arc<dyn ProtocolAdapter>> = HashMap::new();

        register_adapter(
            &mut adapters,
            Arc::new(RaydiumV3Adapter::new(raydium_config(config.as_ref()))),
        );
        register_adapter(
            &mut adapters,
            Arc::new(CpAmmAdapter::new(cp_amm_config(config.as_ref()))),
        );
        register_adapter(
            &mut adapters,
            Arc::new(LbClmmAdapter::new(lb_clmm_config(config.as_ref()))),
        );
        register_adapter(
            &mut adapters,
            Arc::new(LifinityAdapter::new(lifinity_config(config.as_ref()))),
        );
        register_adapter(
            &mut adapters,
            Arc::new(WhirlpoolAdapter::new(whirlpool_config(config.as_ref()))),
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
