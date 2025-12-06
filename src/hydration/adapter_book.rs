use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use solana_sdk::pubkey::Pubkey;
use tracing::info;

use crate::builder::BuilderHandle;
use crate::config::AppConfig;
use crate::hydration::adapters::{
    AdapterReadiness, CpAmmAdapter, CpAmmAdapterConfig, LbClmmAdapter, LbClmmAdapterConfig,
    ProtocolAdapter, QuoteRequest, QuoteResponse, QuoteStatus, RaydiumV3Adapter,
    RaydiumV3AdapterConfig, SwapDirection, WhirlpoolAdapter, WhirlpoolAdapterConfig,
};
use crate::hydration::types::{
    HydrationComponent, PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate,
    TradeUpdate,
};
use crate::template::TemplateContext;
use crate::telemetry;

const BPS_DENOMINATOR: u64 = 10_000;

pub struct AdapterBook {
    adapters: HashMap<String, Arc<dyn ProtocolAdapter>>, // key = program id string
    strict_price_programs: HashSet<String>,
}

impl AdapterBook {
    pub fn new(config: Arc<AppConfig>, builder: BuilderHandle) -> Self {
        let mut adapters: HashMap<String, Arc<dyn ProtocolAdapter>> = HashMap::new();
        let mut strict_price_programs: HashSet<String> = HashSet::new();

        if config.protocol_enabled("raydium_v3") {
            register_adapter(
                &mut adapters,
                Arc::new(RaydiumV3Adapter::new(
                    raydium_config(config.as_ref()),
                    Some(builder.clone()),
                )),
            );
        } else {
            info!(
                target = "adapter_book",
                protocol = "raydium_v3",
                "protocol disabled; skipping adapter registration"
            );
        }

        if config.protocol_enabled("cp_amm") {
            register_adapter(
                &mut adapters,
                Arc::new(CpAmmAdapter::new(
                    cp_amm_config(config.as_ref()),
                    Some(builder.clone()),
                )),
            );
        } else {
            info!(
                target = "adapter_book",
                protocol = "cp_amm",
                "protocol disabled; skipping adapter registration"
            );
        }

        if config.protocol_enabled("lb_clmm") {
            register_adapter(
                &mut adapters,
                Arc::new(LbClmmAdapter::new(
                    lb_clmm_config(config.as_ref()),
                    Some(builder.clone()),
                )),
            );
        } else {
            info!(
                target = "adapter_book",
                protocol = "lb_clmm",
                "protocol disabled; skipping adapter registration"
            );
        }

        if config.protocol_enabled("whirlpool") {
            let whirlpool_adapter = Arc::new(WhirlpoolAdapter::new(
                whirlpool_config(config.as_ref()),
                Some(builder),
            ));
            strict_price_programs.insert(whirlpool_adapter.program_id().to_string());
            register_adapter(&mut adapters, whirlpool_adapter);
        } else {
            info!(
                target = "adapter_book",
                protocol = "whirlpool",
                "protocol disabled; skipping adapter registration"
            );
        }

        Self {
            adapters,
            strict_price_programs,
        }
    }

    pub fn ingest(&self, update: &PoolUpdate) -> Option<&'static str> {
        if let Some(adapter) = self.adapters.get(&update.program_id) {
            adapter.ingest_pool_update(update);
            Some(adapter.name())
        } else {
            None
        }
    }

    pub fn snapshot_quotes(&self, snapshot: &mut PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        snapshot.allow_reserve_backfill = !self.requires_authoritative_quotes(&snapshot.program_id);
        let adapter = self.adapters.get(&snapshot.program_id)?;
        if !snapshot.hydration.ready() {
            telemetry::record_hydration_quote_blocked(adapter.name());
            return Some(build_hydration_pending_quotes(snapshot));
        }
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

        if adapter.prefer_exact_out_quotes() && trade.buy_amount > 0 {
            let request = QuoteRequest {
                pool_id,
                input_mint,
                output_mint,
                amount: trade.buy_amount,
                slot: trade.slot,
                block_time: None,
                direction: SwapDirection::ExactOut,
            };
            if let Some(quote) = adapter.quote_exact_out(request) {
                return Some(quote);
            }
        }

        let fallback = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: trade.sell_amount,
            slot: trade.slot,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        adapter.quote_exact_in(fallback)
    }

    pub fn warm_components(&self, snapshot: &PoolSnapshot, components: &[HydrationComponent]) {
        if components.is_empty() {
            return;
        }
        if let Some(adapter) = self.adapters.get(&snapshot.program_id) {
            adapter.warm_components(snapshot, components);
        }
    }

    pub fn requires_authoritative_quotes(&self, program_id: &str) -> bool {
        self.strict_price_programs.contains(program_id)
    }

    pub fn readiness_diagnostics(&self) -> AdapterReadiness {
        let mut summary = AdapterReadiness::default();
        for adapter in self.adapters.values() {
            summary.extend(adapter.readiness_blockers());
        }
        summary
    }
}

fn register_adapter(
    map: &mut HashMap<String, Arc<dyn ProtocolAdapter>>,
    adapter: Arc<dyn ProtocolAdapter>,
) {
    map.insert(adapter.program_id().to_string(), adapter);
}

fn build_hydration_pending_quotes(snapshot: &PoolSnapshot) -> PoolBidirectionalQuotes {
    PoolBidirectionalQuotes {
        base_to_quote: build_hydration_pending_side(
            &snapshot.base_mint,
            &snapshot.quote_mint,
            snapshot.slot,
        ),
        quote_to_base: build_hydration_pending_side(
            &snapshot.quote_mint,
            &snapshot.base_mint,
            snapshot.slot,
        ),
    }
}

fn build_hydration_pending_side(input_mint: &str, output_mint: &str, slot: u64) -> PoolQuoteSide {
    PoolQuoteSide {
        input_mint: input_mint.to_string(),
        output_mint: output_mint.to_string(),
        notional: 0,
        expected_output: 0,
        fee_bps: 0,
        template_fee_bps: 0,
        effective_price: 0.0,
        estimated_slippage_bps: 0.0,
        sqrt_price_limit: None,
        min_output_amount: None,
        limit_price: None,
        price_impact_limit_bps: None,
        amount_specified_is_input: true,
        context: TemplateContext::default(),
        status: QuoteStatus::HydrationPending,
        sqrt_price_before_x64: None,
        sqrt_price_after_x64: None,
        tick_arrays_used: Vec::new(),
        cache_slot: Some(slot),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hydration::types::QuoteStatus;
    use crate::template::TemplateContext;

    struct FakeAdapter {
        program_id: Pubkey,
        prefer_exact_out: bool,
        exact_out_success: bool,
        exact_in_success: bool,
    }

    impl FakeAdapter {
        fn new(
            program_id: Pubkey,
            prefer_exact_out: bool,
            exact_out_success: bool,
            exact_in_success: bool,
        ) -> Self {
            Self {
                program_id,
                prefer_exact_out,
                exact_out_success,
                exact_in_success,
            }
        }

        fn build_response(&self, req: QuoteRequest) -> QuoteResponse {
            QuoteResponse {
                amount_in: req.amount,
                amount_out: req.amount,
                edge_bps: 0.0,
                sqrt_price_limit: None,
                min_output_amount: Some(req.amount),
                status: QuoteStatus::Ok,
                request: req,
                context: TemplateContext::default(),
            }
        }
    }

    impl ProtocolAdapter for FakeAdapter {
        fn name(&self) -> &'static str {
            "fake"
        }

        fn program_id(&self) -> Pubkey {
            self.program_id
        }

        fn prefer_exact_out_quotes(&self) -> bool {
            self.prefer_exact_out
        }

        fn ingest_pool_update(&self, _update: &PoolUpdate) {}

        fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
            if self.exact_in_success {
                Some(self.build_response(req))
            } else {
                None
            }
        }

        fn quote_exact_out(&self, req: QuoteRequest) -> Option<QuoteResponse> {
            if self.exact_out_success {
                Some(self.build_response(req))
            } else {
                None
            }
        }
    }

    fn sample_trade(program_id: &Pubkey) -> TradeUpdate {
        let pool_id = Pubkey::new_unique();
        let buy_mint = Pubkey::new_unique();
        let sell_mint = Pubkey::new_unique();
        TradeUpdate {
            seq: 1,
            slot: 123,
            program_id: program_id.to_string(),
            pool_id: pool_id.to_string(),
            signature: Pubkey::new_unique().to_string(),
            buy_mint: buy_mint.to_string(),
            sell_mint: sell_mint.to_string(),
            buy_amount: 50,
            sell_amount: 100,
        }
    }

    fn adapter_book_with(adapter: Arc<dyn ProtocolAdapter>) -> AdapterBook {
        let mut adapters = HashMap::new();
        adapters.insert(adapter.program_id().to_string(), adapter);
        AdapterBook {
            adapters,
            strict_price_programs: HashSet::new(),
        }
    }

    #[test]
    fn quote_trade_prefers_exact_out_when_available() {
        let program_id = Pubkey::new_unique();
        let adapter = Arc::new(FakeAdapter::new(program_id, true, true, false));
        let book = adapter_book_with(adapter);
        let trade = sample_trade(&program_id);

        let quote = book.quote_trade(&trade).expect("quote");
        assert_eq!(quote.request.direction, SwapDirection::ExactOut);
        assert_eq!(quote.request.amount, trade.buy_amount);
    }

    #[test]
    fn quote_trade_falls_back_to_exact_in() {
        let program_id = Pubkey::new_unique();
        let adapter = Arc::new(FakeAdapter::new(program_id, true, false, true));
        let book = adapter_book_with(adapter);
        let trade = sample_trade(&program_id);

        let quote = book.quote_trade(&trade).expect("quote");
        assert_eq!(quote.request.direction, SwapDirection::ExactIn);
        assert_eq!(quote.request.amount, trade.sell_amount);
    }

    #[test]
    fn quote_trade_uses_exact_in_when_not_preferred() {
        let program_id = Pubkey::new_unique();
        let adapter = Arc::new(FakeAdapter::new(program_id, false, true, true));
        let book = adapter_book_with(adapter);
        let trade = sample_trade(&program_id);

        let quote = book.quote_trade(&trade).expect("quote");
        assert_eq!(quote.request.direction, SwapDirection::ExactIn);
        assert_eq!(quote.request.amount, trade.sell_amount);
    }
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
    if let Some(protocol) = config.protocols.get("cp_amm") {
        adapter_config.required_partner = protocol.required_partner;
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
    if let Some(guard_bps) = config.protocol_price_impact_guard_bps("lb_clmm") {
        adapter_config.max_price_impact_bps = guard_bps.max(1u16);
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
    if let Some(backoff_ms) = config.protocol_pool_backoff_ms("whirlpool") {
        adapter_config.pool_failure_backoff_ms = backoff_ms.max(1);
    }
    if let Some(backoff_ms) = config.protocol_tick_array_backoff_ms("whirlpool") {
        adapter_config.tick_array_backoff_ms = backoff_ms.max(1);
    }
    if let Some(max_failures) = config.protocol_max_pool_failures("whirlpool") {
        adapter_config.max_pool_failures = max_failures.max(1);
    }
    if let Some(max_failures) = config.protocol_max_tick_array_failures("whirlpool") {
        adapter_config.max_tick_array_failures = max_failures.max(1);
    }
    if let Some(prefer_exact_out) = config.protocol_prefer_exact_out_quotes("whirlpool") {
        adapter_config.prefer_exact_out_quotes = prefer_exact_out;
    }
    adapter_config
}
