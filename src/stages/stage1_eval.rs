use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info};

use crate::adapters::{
    CpAmmAdapter, CpAmmAdapterConfig, LbClmmAdapter, LbClmmAdapterConfig, LifinityAdapter,
    LifinityAdapterConfig, ProtocolAdapter, QuoteRequest, QuoteResponse, RaydiumV3Adapter,
    RaydiumV3AdapterConfig, SwapDirection, WhirlpoolAdapter, WhirlpoolAdapterConfig,
};
use crate::config::AppConfig;
use crate::core::{pin_current_thread, Lease, RingConsumer, RingProducer, SlabPool};

use super::types::{
    compute_edge_bps, BundleCandidate, Opportunity, PoolSnapshot, PoolUpdate, TradeUpdate,
};

pub fn spawn_eval_stage(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    config: Arc<AppConfig>,
    trade_consumer: RingConsumer<Lease<TradeUpdate>>,
    pool_consumer: RingConsumer<Lease<PoolUpdate>>,
    producer: RingProducer<Lease<BundleCandidate>>,
    bundle_pool: SlabPool<BundleCandidate>,
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
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    let mut pool_snapshots: HashMap<String, PoolSnapshot> = HashMap::new();
    let mut opportunity_seq = 0u64;
    let adapters = AdapterBook::new(config);

    info!("stage1 eval online");
    while !stop.load(Ordering::Relaxed) {
        let mut made_progress = false;

        if let Some(pool_lease) = pool_consumer.pop() {
            let update = pool_lease.into_inner();
            adapters.ingest(&update);
            let snapshot = update.into_snapshot();
            pool_snapshots.insert(snapshot.pool_id.clone(), snapshot);
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
            let opportunity = Opportunity::from_trade(opportunity_seq, trade, snapshot, quote_edge);
            opportunity_seq = opportunity_seq.wrapping_add(1);

            if let Some(lease) =
                bundle_pool.try_acquire_with(|| BundleCandidate::from_opportunity(&opportunity))
            {
                if producer.push(lease).is_ok() {
                    debug!(
                        opp_seq = opportunity.seq,
                        edge_bps = opportunity.estimated_edge_bps,
                        signature = %opportunity.trade.signature,
                        "emitted bundle candidate"
                    );
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
