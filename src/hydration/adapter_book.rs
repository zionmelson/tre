use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;

use crate::builder::BuilderHandle;
use crate::config::AppConfig;
use crate::hydration::adapters::{
    CpAmmAdapter, CpAmmAdapterConfig, LbClmmAdapter, LbClmmAdapterConfig, ProtocolAdapter,
    QuoteRequest, QuoteResponse, RaydiumV3Adapter, RaydiumV3AdapterConfig, SwapDirection,
    WhirlpoolAdapter, WhirlpoolAdapterConfig,
};
use crate::hydration::types::{PoolBidirectionalQuotes, PoolSnapshot, PoolUpdate, TradeUpdate};

const BPS_DENOMINATOR: u64 = 10_000;

pub struct AdapterBook {
    adapters: HashMap<String, Arc<dyn ProtocolAdapter>>, // key = program id string
}

impl AdapterBook {
    pub fn new(config: Arc<AppConfig>, builder: BuilderHandle) -> Self {
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
            Arc::new(WhirlpoolAdapter::new(
                whirlpool_config(config.as_ref()),
                Some(builder),
            )),
        );

        Self { adapters }
    }

    pub fn ingest(&self, update: &PoolUpdate) -> Option<&'static str> {
        if let Some(adapter) = self.adapters.get(&update.program_id) {
            adapter.ingest_pool_update(update);
            Some(adapter.name())
        } else {
            None
        }
    }

    pub fn snapshot_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let adapter = self.adapters.get(&snapshot.program_id)?;
        adapter.bidirectional_quotes(snapshot)
    }

    pub fn enrich_snapshot(&self, snapshot: &mut PoolSnapshot) {
        if let Some(adapter) = self.adapters.get(&snapshot.program_id) {
            adapter.enrich_snapshot(snapshot);
        }
    }

    pub fn quote_trade(&self, trade: &TradeUpdate) -> Option<QuoteResponse> {
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
    if let Some(util_bps) = config.protocol_max_utilization_bps("cp_amm") {
        adapter_config.max_swap_utilization_bps = util_bps.clamp(1, BPS_DENOMINATOR);
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
    if let Some(util_bps) = config.protocol_max_utilization_bps("lb_clmm") {
        adapter_config.max_swap_utilization_bps = util_bps.clamp(1, BPS_DENOMINATOR);
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
    if let Some(util_bps) = config.protocol_max_utilization_bps("whirlpool") {
        adapter_config.max_swap_utilization_bps = util_bps.clamp(1, BPS_DENOMINATOR);
    }
    adapter_config
}
