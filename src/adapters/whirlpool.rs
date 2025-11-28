use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::{DashMap, DashSet};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};
use tracing::{debug, info, trace, warn};
use uint::construct_uint;

use super::hydration::AsyncHydrator;

use crate::builder::{BuilderHandle, WhirlpoolPoolMeta};
use crate::stages::types::{
    PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate, WhirlpoolPoolExtras,
};

use super::{
    build_quote_response, mul_div_price, quote_from_price, ProtocolAdapter, QuoteRequest,
    QuoteResponse, QuoteStatus, SwapDirection, TemplateContext,
};

const WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicV1cqQhBYtbJX3Yq9z7Dfc7gXLMGr5vLyN";
const TICK_ARRAY_SEED: &[u8] = b"tick_array";
const SNAPSHOT_PROBE_DIVISOR: u128 = 5_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;
const WHIRLPOOL_DEFAULT_CU_LIMIT: u32 = 380_000;

#[allow(clippy::assign_op_pattern, clippy::manual_div_ceil)]
mod u256 {
    use super::construct_uint;

    construct_uint! {
        pub struct U256(4);
    }
}
pub use u256::U256;

const DEFAULT_WHIRLPOOL_HYDRATION_RETRY_MS: u64 = 60_000;

#[derive(Debug, Clone)]
pub struct WhirlpoolAdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
    pub hydration_retry_ms: u64,
}

impl Default for WhirlpoolAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 15_000,
            hydration_retry_ms: DEFAULT_WHIRLPOOL_HYDRATION_RETRY_MS,
        }
    }
}

pub struct WhirlpoolAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, WhirlpoolState>,
    config: WhirlpoolAdapterConfig,
    hydrator: Option<WhirlpoolPoolHydrator>,
    template_builder: Option<BuilderHandle>,
    registered_templates: DashSet<Pubkey>,
}

impl WhirlpoolAdapter {
    pub fn new(config: WhirlpoolAdapterConfig, template_builder: Option<BuilderHandle>) -> Self {
        let program_id = Pubkey::from_str(WHIRLPOOL_PROGRAM_ID)
            .expect("static Whirlpool program id should be valid");
        let hydrator = WhirlpoolPoolHydrator::new(&config, program_id);
        if let Some(hydrator) = hydrator.as_ref() {
            info!(
                target = "adapter::whirlpool",
                endpoints = hydrator.client_count(),
                "whirlpool pool hydrator online"
            );
        } else {
            debug!(
                target = "adapter::whirlpool",
                "whirlpool pool hydrator disabled (no rpc endpoints)"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            hydrator,
            template_builder,
            registered_templates: DashSet::new(),
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<WhirlpoolState> {
        self.state.get(pool).map(|entry| entry.clone())
    }

    fn evict_stale(&self, current_slot: u64) {
        let ttl = self.config.stale_after_ms;
        self.state.retain(|_, state| {
            let slot_fresh = current_slot.saturating_sub(state.last_update_slot)
                <= self.config.stale_slot_threshold;
            let instant_fresh = state.last_update_at.elapsed().as_millis() as u64 <= ttl;
            if !(slot_fresh && instant_fresh) {
                trace!(
                    target = "adapter::whirlpool",
                    pool = %state.whirlpool,
                    last_slot = state.last_update_slot,
                    "evicting stale Whirlpool state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: WhirlpoolPoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::whirlpool",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let extras = self.enrich_extras(pool_pubkey, extras);

        let Some(token_mint_a) = extras
            .token_mint_a
            .as_ref()
            .and_then(|value| parse_pubkey(value))
        else {
            warn!(target = "adapter::whirlpool", pool = %update.pool_id, "missing mint A");
            return;
        };

        let Some(token_mint_b) = extras
            .token_mint_b
            .as_ref()
            .and_then(|value| parse_pubkey(value))
        else {
            warn!(target = "adapter::whirlpool", pool = %update.pool_id, "missing mint B");
            return;
        };

        let state = WhirlpoolState {
            whirlpool: pool_pubkey,
            token_mint_a,
            token_mint_b,
            token_vault_a: extras
                .token_vault_a
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_vault_b: extras
                .token_vault_b
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            tick_spacing: extras.tick_spacing,
            liquidity: extras.liquidity,
            sqrt_price_x64: extras.sqrt_price_x64,
            tick_current_index: extras.tick_current_index,
            fee_rate: extras.fee_rate,
            protocol_fee_rate: extras.protocol_fee_rate,
            oracle: extras.oracle.as_ref().and_then(|value| parse_pubkey(value)),
            tick_array0: extras
                .tick_array0
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            tick_array1: extras
                .tick_array1
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            tick_array2: extras
                .tick_array2
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
        };

        trace!(
            target = "adapter::whirlpool",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting Whirlpool pool state"
        );

        self.prefetch_tick_array_hints(&state);
        self.register_templates_with_builder(&state);
        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: WhirlpoolPoolExtras) -> WhirlpoolPoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.token_mint_a = extras
                .token_mint_a
                .or_else(|| Some(meta.token_mint_a.to_string()));
            extras.token_mint_b = extras
                .token_mint_b
                .or_else(|| Some(meta.token_mint_b.to_string()));
            extras.token_vault_a = extras
                .token_vault_a
                .or_else(|| Some(meta.token_vault_a.to_string()));
            extras.token_vault_b = extras
                .token_vault_b
                .or_else(|| Some(meta.token_vault_b.to_string()));
            extras.tick_spacing = extras.tick_spacing.or(Some(meta.tick_spacing));
            extras.liquidity = extras.liquidity.or(Some(meta.liquidity));
            extras.sqrt_price_x64 = extras.sqrt_price_x64.or(Some(meta.sqrt_price_x64));
            extras.tick_current_index = extras.tick_current_index.or(Some(meta.tick_current_index));
            extras.fee_rate = extras.fee_rate.or(Some(meta.fee_rate));
            extras.protocol_fee_rate = extras.protocol_fee_rate.or(Some(meta.protocol_fee_rate));
            extras.oracle = extras.oracle.or_else(|| Some(meta.oracle.to_string()));
        }

        extras
    }

    fn register_templates_with_builder(&self, state: &WhirlpoolState) {
        let Some(builder) = self.template_builder.as_ref() else {
            return;
        };
        let Some(raw_fee) = state.fee_rate else {
            return;
        };
        if !self.registered_templates.insert(state.whirlpool) {
            return;
        }

        let mut alt_accounts = Vec::with_capacity(12);
        alt_accounts.push(state.whirlpool);
        alt_accounts.push(state.token_mint_a);
        alt_accounts.push(state.token_mint_b);
        if let Some(pk) = state.token_vault_a {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.token_vault_b {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.oracle {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.tick_array0 {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.tick_array1 {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.tick_array2 {
            alt_accounts.push(pk);
        }

        let fee_bps = fee_bps_from_hundredth_bip(raw_fee);
        let fee_tier_bps = fee_bps.min(u64::from(u16::MAX)) as u16;
        builder.register_whirlpool_pool(WhirlpoolPoolMeta {
            pool_id: state.whirlpool,
            fee_tier_bps,
            alt_accounts,
            alt_handle: None,
        });
    }

    fn decorate_template_context(&self, response: &mut QuoteResponse, state: &WhirlpoolState) {
        response.context = self.snapshot_template_context(state);
    }

    fn snapshot_template_context(&self, state: &WhirlpoolState) -> TemplateContext {
        let mut context = TemplateContext {
            compute_unit_limit: Some(WHIRLPOOL_DEFAULT_CU_LIMIT),
            ..Default::default()
        };
        if let Some(oracle) = state.oracle {
            context
                .remaining_accounts
                .push(AccountMeta::new_readonly(oracle, false));
        }
        for key in [state.tick_array0, state.tick_array1, state.tick_array2]
            .into_iter()
            .flatten()
        {
            context
                .remaining_accounts
                .push(AccountMeta::new(key, false));
        }
        context
    }

    fn prefetch_tick_array_hints(&self, state: &WhirlpoolState) {
        let Some(hydrator) = self.hydrator.as_ref() else {
            return;
        };
        let hints = [
            state.tick_array0.as_ref(),
            state.tick_array1.as_ref(),
            state.tick_array2.as_ref(),
        ];
        let collected: Vec<Pubkey> = hints
            .into_iter()
            .filter_map(|entry| entry.copied())
            .collect();
        if collected.is_empty() {
            return;
        }
        hydrator.prefetch_tick_arrays(state.whirlpool, collected);
    }

    fn build_snapshot_side(
        &self,
        state: &WhirlpoolState,
        pool_id: Pubkey,
        slot: u64,
        input_mint: Pubkey,
        output_mint: Pubkey,
        probe_amount: u64,
    ) -> Option<PoolQuoteSide> {
        if probe_amount == 0 || input_mint == output_mint {
            return None;
        }

        let mut seen = 0;
        for mint in [state.token_mint_a, state.token_mint_b] {
            if mint == input_mint || mint == output_mint {
                seen += 1;
            }
        }
        if seen < 2 {
            return None;
        }

        let req = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: probe_amount,
            slot,
            direction: SwapDirection::ExactIn,
        };

        let mut sqrt_price_limit = None;
        let (expected_output, slippage_bps) = if let Some(simulation) =
            simulate_best_exact_in(self.hydrator.as_ref(), state, &req, probe_amount)
        {
            sqrt_price_limit = Some(simulation.sqrt_price_after);
            (simulation.amount_out, simulation.edge_bps)
        } else {
            let price = state.mid_price()?;
            let amount_out =
                quote_from_price(&req, &state.token_mint_a, &state.token_mint_b, price)?;
            (amount_out, 0.0)
        };

        if expected_output == 0 {
            return None;
        }

        let effective_price = if probe_amount == 0 {
            0.0
        } else {
            expected_output as f64 / probe_amount as f64
        };

        Some(PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional: probe_amount,
            expected_output,
            fee_bps: whirlpool_trade_fee_bps(state),
            effective_price,
            estimated_slippage_bps: slippage_bps,
            sqrt_price_limit,
            min_output_amount: Some(expected_output),
            limit_price: None,
            price_impact_limit_bps: None,
            context: self.snapshot_template_context(state),
        })
    }
}

impl ProtocolAdapter for WhirlpoolAdapter {
    fn name(&self) -> &'static str {
        "whirlpool"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::whirlpool",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        let extras = update.extensions.whirlpool.clone().unwrap_or_default();
        self.upsert_state(update, extras);
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if let Some(simulation) =
            simulate_best_exact_in(self.hydrator.as_ref(), &state, &req, req.amount)
        {
            let mut response = build_quote_response(req, simulation.amount_out);
            response.edge_bps = simulation.edge_bps;
            response.sqrt_price_limit = Some(simulation.sqrt_price_after);
            response.min_output_amount = Some(simulation.amount_out);
            self.decorate_template_context(&mut response, &state);
            return Some(response);
        }
        let price = state.mid_price()?;
        let amount_out = quote_from_price(&req, &state.token_mint_a, &state.token_mint_b, price)?;
        let mut fallback = build_quote_response(req, amount_out);
        fallback.min_output_amount = Some(amount_out);
        self.decorate_template_context(&mut fallback, &state);
        Some(fallback)
    }

    fn quote_exact_out(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactOut {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if req.amount == 0 {
            return None;
        }
        if let Some((amount_in, simulation)) =
            find_input_for_exact_out(self.hydrator.as_ref(), &state, &req, req.amount)
        {
            let mut response = QuoteResponse {
                amount_in,
                amount_out: req.amount,
                edge_bps: simulation.edge_bps,
                sqrt_price_limit: Some(simulation.sqrt_price_after),
                min_output_amount: None,
                status: QuoteStatus::Ok,
                request: req,
                context: TemplateContext::default(),
            };
            self.decorate_template_context(&mut response, &state);
            return Some(response);
        }

        let amount_in = quote_exact_out_from_price(&req, &state)?;
        let mut response = QuoteResponse {
            amount_in,
            amount_out: req.amount,
            edge_bps: 0.0,
            sqrt_price_limit: None,
            min_output_amount: None,
            status: QuoteStatus::Ok,
            request: req,
            context: TemplateContext::default(),
        };
        self.decorate_template_context(&mut response, &state);
        Some(response)
    }

    fn bidirectional_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
        let state = self.get_state(&pool_pubkey)?;
        let base_mint = parse_pubkey(&snapshot.base_mint)?;
        let quote_mint = parse_pubkey(&snapshot.quote_mint)?;
        if base_mint == quote_mint {
            return None;
        }

        let base_probe = snapshot_probe_amount(snapshot.base_post_amount)?;
        let quote_probe = snapshot_probe_amount(snapshot.quote_post_amount)?;

        let base_to_quote = self.build_snapshot_side(
            &state,
            pool_pubkey,
            snapshot.slot,
            base_mint,
            quote_mint,
            base_probe,
        )?;
        let quote_to_base = self.build_snapshot_side(
            &state,
            pool_pubkey,
            snapshot.slot,
            quote_mint,
            base_mint,
            quote_probe,
        )?;

        Some(PoolBidirectionalQuotes {
            base_to_quote,
            quote_to_base,
        })
    }
}

fn needs_hydration(extras: &WhirlpoolPoolExtras) -> bool {
    extras.token_mint_a.is_none()
        || extras.token_mint_b.is_none()
        || extras.token_vault_a.is_none()
        || extras.token_vault_b.is_none()
        || extras.tick_spacing.is_none()
        || extras.sqrt_price_x64.is_none()
        || extras.liquidity.is_none()
        || extras.tick_current_index.is_none()
        || extras.fee_rate.is_none()
        || extras.protocol_fee_rate.is_none()
        || extras.oracle.is_none()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
    tick_spacing: i32,
    liquidity: u128,
    sqrt_price_x64: u128,
    tick_current_index: i32,
    fee_rate: u16,
    protocol_fee_rate: u16,
    oracle: Pubkey,
}

#[derive(Clone, Debug)]
struct HydratedTickArray {
    _address: Pubkey,
    _pool: Pubkey,
    start_tick_index: i32,
    ticks: Vec<TickEntry>,
}

#[derive(Clone, Debug)]
struct TickEntry {
    initialized: bool,
    liquidity_net: i128,
    liquidity_gross: u128,
}

#[derive(Clone, Copy, Debug)]
struct InitializedTick {
    index: i32,
    liquidity_net: i128,
}

struct WhirlpoolPoolHydrator {
    program_id: Pubkey,
    hydrator: AsyncHydrator<Pubkey, HydratedPoolMeta>,
    tick_array_cache: DashMap<(Pubkey, i32), HydratedTickArray>,
    clients: Arc<Vec<Arc<RpcClient>>>,
    endpoint_count: usize,
}

impl WhirlpoolPoolHydrator {
    fn new(config: &WhirlpoolAdapterConfig, program_id: Pubkey) -> Option<Self> {
        if config.rpc_endpoints.is_empty() {
            return None;
        }

        let timeout = Duration::from_millis(config.rpc_timeout_ms.max(100));
        let client_list: Vec<Arc<RpcClient>> = config
            .rpc_endpoints
            .iter()
            .map(|url| Arc::new(RpcClient::new_with_timeout(url.clone(), timeout)))
            .collect();

        if client_list.is_empty() {
            return None;
        }

        let endpoint_count = client_list.len();
        let clients = Arc::new(client_list);
        let fetch_clients = clients.clone();
        let hydrator = AsyncHydrator::new("whirlpool_pool", endpoint_count, move |pool| {
            fetch_whirlpool_meta(program_id, fetch_clients.as_ref(), pool)
        });

        Some(Self {
            program_id,
            hydrator,
            tick_array_cache: DashMap::new(),
            clients,
            endpoint_count,
        })
    }

    fn client_count(&self) -> usize {
        self.endpoint_count
    }

    fn hydrate(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        self.hydrator.get_or_schedule(pool)
    }

    fn hydrate_tick_array(&self, pool: Pubkey, start_tick_index: i32) -> Option<HydratedTickArray> {
        let key = (pool, start_tick_index);
        if let Some(entry) = self.tick_array_cache.get(&key) {
            return Some(entry.clone());
        }
        self.fetch_tick_array_from_rpc(pool, start_tick_index)
    }

    fn hydrate_tick_array_window(
        &self,
        pool: Pubkey,
        tick_spacing: i32,
        anchor_start: i32,
        look_ahead: u32,
    ) {
        if look_ahead == 0 {
            return;
        }
        let span = match tick_array_span(tick_spacing) {
            Some(value) if value != 0 => value,
            _ => return,
        };
        for step in 1..=look_ahead {
            let offset = match span.checked_mul(step as i32) {
                Some(value) => value,
                None => break,
            };
            if let Some(start) = anchor_start.checked_add(offset) {
                let _ = self.hydrate_tick_array(pool, start);
            }
            if let Some(start) = anchor_start.checked_sub(offset) {
                let _ = self.hydrate_tick_array(pool, start);
            }
        }
    }

    fn prefetch_tick_arrays<I>(&self, pool: Pubkey, hints: I)
    where
        I: IntoIterator<Item = Pubkey>,
    {
        for address in hints {
            let _ = self.fetch_tick_array_by_address(pool, address);
        }
    }

    fn fetch_tick_array_from_rpc(
        &self,
        pool: Pubkey,
        start_tick_index: i32,
    ) -> Option<HydratedTickArray> {
        let address = derive_tick_array_address(&pool, start_tick_index, &self.program_id);
        self.fetch_tick_array_account(pool, Some(start_tick_index), address)
    }

    fn fetch_tick_array_by_address(
        &self,
        pool: Pubkey,
        address: Pubkey,
    ) -> Option<HydratedTickArray> {
        self.fetch_tick_array_account(pool, None, address)
    }

    fn fetch_tick_array_account(
        &self,
        pool: Pubkey,
        expected_start: Option<i32>,
        address: Pubkey,
    ) -> Option<HydratedTickArray> {
        for client in self.clients.as_ref() {
            match client.get_account(&address) {
                Ok(account) => {
                    if let Some(array) = parse_tick_array_account(address, pool, &account.data) {
                        if let Some(expected) = expected_start {
                            if array.start_tick_index != expected {
                                warn!(
                                    target = "adapter::whirlpool",
                                    pool = %pool,
                                    expected_start = expected,
                                    actual_start = array.start_tick_index,
                                    "tick array hint start mismatch"
                                );
                            }
                        }
                        let key = (pool, array.start_tick_index);
                        self.tick_array_cache.insert(key, array.clone());
                        return Some(array);
                    } else {
                        warn!(
                            target = "adapter::whirlpool",
                            pool = %pool,
                            start = expected_start.unwrap_or_default(),
                            "unexpected whirlpool tick array layout"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        target = "adapter::whirlpool",
                        pool = %pool,
                        start = expected_start.unwrap_or_default(),
                        %error,
                        "failed to fetch whirlpool tick array"
                    );
                }
            }
        }
        None
    }
}

fn fetch_whirlpool_meta(
    program_id: Pubkey,
    clients: &[Arc<RpcClient>],
    pool: Pubkey,
) -> Option<HydratedPoolMeta> {
    for client in clients {
        match client.get_account(&pool) {
            Ok(account) => {
                if let Some(meta) = parse_whirlpool_account(pool, &program_id, &account.data) {
                    return Some(meta);
                } else {
                    warn!(
                        target = "adapter::whirlpool",
                        pool = %pool,
                        "unexpected whirlpool pool account layout"
                    );
                }
            }
            Err(error) => {
                warn!(
                    target = "adapter::whirlpool",
                    pool = %pool,
                    %error,
                    "failed to fetch whirlpool pool account"
                );
            }
        }
    }

    None
}

struct WhirlpoolTickArrayCursor<'a> {
    hydrator: &'a WhirlpoolPoolHydrator,
    pool: Pubkey,
    tick_spacing: i32,
    cache: HashMap<i32, HydratedTickArray>,
}

impl<'a> WhirlpoolTickArrayCursor<'a> {
    fn new(
        hydrator: &'a WhirlpoolPoolHydrator,
        pool: Pubkey,
        tick_spacing: i32,
        anchor_start: Option<i32>,
    ) -> Self {
        if let Some(start) = anchor_start {
            let _ = hydrator.hydrate_tick_array(pool, start);
            hydrator.hydrate_tick_array_window(pool, tick_spacing, start, 2);
        }
        Self {
            hydrator,
            pool,
            tick_spacing,
            cache: HashMap::new(),
        }
    }

    fn get_array(&mut self, start: i32) -> Option<HydratedTickArray> {
        if let Some(entry) = self.cache.get(&start) {
            return Some(entry.clone());
        }
        if let Some(entry) = self.hydrator.hydrate_tick_array(self.pool, start) {
            self.cache.insert(start, entry.clone());
            self.hydrator
                .hydrate_tick_array_window(self.pool, self.tick_spacing, start, 1);
            return Some(entry);
        }
        None
    }

    fn next_initialized_tick(
        &mut self,
        current_tick: i32,
        zero_for_one: bool,
    ) -> Option<InitializedTick> {
        let span = tick_array_span(self.tick_spacing)?;
        let mut array_start = active_tick_array_start(self.tick_spacing, current_tick)?;
        let mut search_tick = current_tick;

        const MAX_ARRAYS: usize = 16;
        for _ in 0..MAX_ARRAYS {
            let array = self.get_array(array_start)?;
            if zero_for_one {
                if let Some(tick) = find_prev_initialized(&array, search_tick, self.tick_spacing) {
                    return Some(tick);
                }
                array_start = array_start.checked_sub(span)?;
                search_tick = array_start
                    .checked_add(span)?
                    .saturating_sub(self.tick_spacing);
            } else {
                if let Some(tick) = find_next_initialized(&array, search_tick, self.tick_spacing) {
                    return Some(tick);
                }
                array_start = array_start.checked_add(span)?;
                search_tick = array_start.saturating_sub(self.tick_spacing);
            }
        }

        None
    }
}

fn find_prev_initialized(
    array: &HydratedTickArray,
    current_tick: i32,
    tick_spacing: i32,
) -> Option<InitializedTick> {
    let spacing = tick_spacing.abs();
    if spacing == 0 {
        return None;
    }
    for (i, tick) in array.ticks.iter().enumerate().rev() {
        let offset = (i as i32).checked_mul(spacing)?;
        let tick_index = array.start_tick_index.checked_add(offset)?;
        if tick_index <= current_tick && tick.initialized && tick.liquidity_gross > 0 {
            return Some(InitializedTick {
                index: tick_index,
                liquidity_net: tick.liquidity_net,
            });
        }
    }
    None
}

fn find_next_initialized(
    array: &HydratedTickArray,
    current_tick: i32,
    tick_spacing: i32,
) -> Option<InitializedTick> {
    let spacing = tick_spacing.abs();
    if spacing == 0 {
        return None;
    }
    for (i, tick) in array.ticks.iter().enumerate() {
        let offset = (i as i32).checked_mul(spacing)?;
        let tick_index = array.start_tick_index.checked_add(offset)?;
        if tick_index > current_tick && tick.initialized && tick.liquidity_gross > 0 {
            return Some(InitializedTick {
                index: tick_index,
                liquidity_net: tick.liquidity_net,
            });
        }
    }
    None
}

const ANCHOR_DISCRIMINATOR_LEN: usize = 8;
const PUBKEY_BYTES: usize = 32;
const WHIRLPOOL_REWARD_INFO_BYTES: usize = 32 + 32 + 32 + 16 + 16;
const WHIRLPOOL_TICK_ARRAY_TICK_COUNT: usize = 88;
const WHIRLPOOL_TICK_SERIALIZED_LEN: usize = 1 + (16 * 5) + (16 * 3);
const MIN_TICK_ARRAY_ACCOUNT_LEN: usize = ANCHOR_DISCRIMINATOR_LEN
    + 4
    + (WHIRLPOOL_TICK_SERIALIZED_LEN * WHIRLPOOL_TICK_ARRAY_TICK_COUNT)
    + PUBKEY_BYTES;
const WHIRLPOOL_ACCOUNT_MIN_LEN: usize = ANCHOR_DISCRIMINATOR_LEN
    + PUBKEY_BYTES // whirlpools config
    + 1 // whirlpool bump
    + 2 // tick spacing
    + 2 // fee tier seed prefix
    + 2 // fee rate
    + 2 // protocol fee rate
    + 16 // liquidity
    + 16 // sqrt price
    + 4 // tick current index
    + 8 // protocol fee owed a
    + 8 // protocol fee owed b
    + PUBKEY_BYTES // token mint a
    + PUBKEY_BYTES // token vault a
    + 16 // fee growth global a
    + PUBKEY_BYTES // token mint b
    + PUBKEY_BYTES // token vault b
    + 16 // fee growth global b
    + 8 // reward last updated timestamp
    + (WHIRLPOOL_REWARD_INFO_BYTES * 3);

fn parse_whirlpool_account(
    pool: Pubkey,
    program_id: &Pubkey,
    data: &[u8],
) -> Option<HydratedPoolMeta> {
    if data.len() < WHIRLPOOL_ACCOUNT_MIN_LEN {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    skip_bytes(&mut cursor, PUBKEY_BYTES, data)?; // whirlpools config
    skip_bytes(&mut cursor, 1, data)?; // bump
    let tick_spacing = read_u16(&mut cursor, data)? as i32;
    skip_bytes(&mut cursor, 2, data)?; // fee tier seed prefix
    let fee_rate = read_u16(&mut cursor, data)?;
    let protocol_fee_rate = read_u16(&mut cursor, data)?;
    let liquidity = read_u128(&mut cursor, data)?;
    let sqrt_price_x64 = read_u128(&mut cursor, data)?;
    let tick_current_index = read_i32(&mut cursor, data)?;
    skip_bytes(&mut cursor, 8, data)?; // protocol fee owed A
    skip_bytes(&mut cursor, 8, data)?; // protocol fee owed B
    let token_mint_a = read_pubkey_bytes(&mut cursor, data)?;
    let token_vault_a = read_pubkey_bytes(&mut cursor, data)?;
    skip_bytes(&mut cursor, 16, data)?; // fee growth global A
    let token_mint_b = read_pubkey_bytes(&mut cursor, data)?;
    let token_vault_b = read_pubkey_bytes(&mut cursor, data)?;
    skip_bytes(&mut cursor, 16, data)?; // fee growth global B
    skip_bytes(&mut cursor, 8, data)?; // reward last updated timestamp
    skip_bytes(&mut cursor, WHIRLPOOL_REWARD_INFO_BYTES * 3, data)?; // reward infos

    let (oracle, _) = Pubkey::find_program_address(&[b"oracle", pool.as_ref()], program_id);

    Some(HydratedPoolMeta {
        token_mint_a,
        token_mint_b,
        token_vault_a,
        token_vault_b,
        tick_spacing,
        liquidity,
        sqrt_price_x64,
        tick_current_index,
        fee_rate,
        protocol_fee_rate,
        oracle,
    })
}

fn parse_tick_array_account(
    address: Pubkey,
    expected_pool: Pubkey,
    data: &[u8],
) -> Option<HydratedTickArray> {
    if data.len() < MIN_TICK_ARRAY_ACCOUNT_LEN {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    let start_tick_index = read_i32(&mut cursor, data)?;
    let mut ticks = Vec::with_capacity(WHIRLPOOL_TICK_ARRAY_TICK_COUNT);
    for _ in 0..WHIRLPOOL_TICK_ARRAY_TICK_COUNT {
        let initialized = read_bool(&mut cursor, data)?;
        let liquidity_net = read_i128(&mut cursor, data)?;
        let liquidity_gross = read_u128(&mut cursor, data)?;
        skip_bytes(&mut cursor, 16, data)?; // fee growth outside A
        skip_bytes(&mut cursor, 16, data)?; // fee growth outside B
        skip_bytes(&mut cursor, 16 * 3, data)?; // reward growths outside
        ticks.push(TickEntry {
            initialized,
            liquidity_net,
            liquidity_gross,
        });
    }
    let whirlpool = read_pubkey_bytes(&mut cursor, data)?;
    if whirlpool != expected_pool {
        return None;
    }

    Some(HydratedTickArray {
        _address: address,
        _pool: whirlpool,
        start_tick_index,
        ticks,
    })
}

fn skip_bytes(cursor: &mut usize, len: usize, data: &[u8]) -> Option<()> {
    let end = cursor.checked_add(len)?;
    if end > data.len() {
        return None;
    }
    *cursor = end;
    Some(())
}

fn read_u16(cursor: &mut usize, data: &[u8]) -> Option<u16> {
    let end = cursor.checked_add(2)?;
    let bytes: [u8; 2] = data.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(u16::from_le_bytes(bytes))
}

fn read_i32(cursor: &mut usize, data: &[u8]) -> Option<i32> {
    let end = cursor.checked_add(4)?;
    let bytes: [u8; 4] = data.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(i32::from_le_bytes(bytes))
}

fn read_bool(cursor: &mut usize, data: &[u8]) -> Option<bool> {
    let byte = *data.get(*cursor)?;
    let next = (*cursor).checked_add(1)?;
    *cursor = next;
    Some(byte != 0)
}

fn read_i128(cursor: &mut usize, data: &[u8]) -> Option<i128> {
    let end = cursor.checked_add(16)?;
    let bytes: [u8; 16] = data.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(i128::from_le_bytes(bytes))
}

fn read_u128(cursor: &mut usize, data: &[u8]) -> Option<u128> {
    let end = cursor.checked_add(16)?;
    let bytes: [u8; 16] = data.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(u128::from_le_bytes(bytes))
}

fn read_pubkey_bytes(cursor: &mut usize, data: &[u8]) -> Option<Pubkey> {
    let end = cursor.checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

fn derive_tick_array_address(pool: &Pubkey, start_tick_index: i32, program_id: &Pubkey) -> Pubkey {
    let start_bytes = start_tick_index.to_le_bytes();
    Pubkey::find_program_address(&[TICK_ARRAY_SEED, pool.as_ref(), &start_bytes], program_id).0
}

fn tick_array_span(tick_spacing: i32) -> Option<i32> {
    tick_spacing
        .checked_mul(WHIRLPOOL_TICK_ARRAY_TICK_COUNT as i32)
        .map(|value| value.abs())
}

fn active_tick_array_start(tick_spacing: i32, tick_index: i32) -> Option<i32> {
    let span = tick_array_span(tick_spacing)?;
    if span == 0 {
        return None;
    }
    let mut start = tick_index / span * span;
    if tick_index < 0 && tick_index % span != 0 {
        start -= span;
    }
    Some(start)
}

#[derive(Debug, Clone)]
pub struct WhirlpoolState {
    pub whirlpool: Pubkey,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Option<Pubkey>,
    pub token_vault_b: Option<Pubkey>,
    pub tick_spacing: Option<i32>,
    pub liquidity: Option<u128>,
    pub sqrt_price_x64: Option<u128>,
    pub tick_current_index: Option<i32>,
    pub fee_rate: Option<u16>,
    pub protocol_fee_rate: Option<u16>,
    pub oracle: Option<Pubkey>,
    pub tick_array0: Option<Pubkey>,
    pub tick_array1: Option<Pubkey>,
    pub tick_array2: Option<Pubkey>,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
}

impl WhirlpoolState {
    pub fn mid_price(&self) -> Option<f64> {
        self.sqrt_price_x64
            .map(|sqrt_price| (sqrt_price as f64 / (1u128 << 64) as f64).powi(2))
    }
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeDirection {
    AToB,
    BToA,
}

#[derive(Debug, Clone, Copy)]
struct SlippageSimulation {
    amount_out: u64,
    edge_bps: f64,
    sqrt_price_after: u128,
}

fn simulate_constant_liquidity_exact_in(
    state: &WhirlpoolState,
    req: &QuoteRequest,
) -> Option<SlippageSimulation> {
    let liquidity = state.liquidity?;
    let sqrt_price_x64 = state.sqrt_price_x64?;
    let fee_rate = state.fee_rate?;
    if liquidity == 0 || req.amount == 0 {
        return None;
    }

    let direction = resolve_direction(state, req)?;
    let zero_for_one = matches!(direction, TradeDirection::AToB);
    let fee_bps = fee_bps_from_hundredth_bip(fee_rate);

    let sqrt_price_current = math::to_x96(sqrt_price_x64);
    let sqrt_price_limit = if zero_for_one {
        math::sqrt_price_at_tick_x96(math::MIN_TICK)?
    } else {
        math::sqrt_price_at_tick_x96(math::MAX_TICK)?
    };

    let step = math::compute_swap_step(
        sqrt_price_current,
        sqrt_price_limit,
        liquidity,
        req.amount as u128,
        fee_bps,
        zero_for_one,
    )?;

    let amount_out = step.amount_out.min(u64::MAX as u128) as u64;
    if amount_out == 0 {
        return None;
    }

    let actual_amount_in = step
        .amount_in
        .saturating_add(step.fee_amount)
        .min(req.amount as u128) as u64;

    let reference_price = match direction {
        TradeDirection::AToB => state.mid_price(),
        TradeDirection::BToA => {
            state
                .mid_price()
                .and_then(|price| if price > 0.0 { Some(1.0 / price) } else { None })
        }
    };

    let implied_price = if actual_amount_in == 0 {
        0.0
    } else {
        (amount_out as f64) / (actual_amount_in as f64)
    };

    let edge_bps = reference_price
        .filter(|price| *price > 0.0 && implied_price > 0.0)
        .map(|price| ((implied_price / price) - 1.0) * 10_000.0)
        .unwrap_or(0.0);

    let sqrt_price_after = sqrt_price_x64_from_x96(&step.sqrt_price_next);

    Some(SlippageSimulation {
        amount_out,
        edge_bps,
        sqrt_price_after,
    })
}

fn simulate_tick_arrays_exact_in(
    hydrator: &WhirlpoolPoolHydrator,
    state: &WhirlpoolState,
    req: &QuoteRequest,
) -> Option<SlippageSimulation> {
    let tick_spacing = state.tick_spacing?;
    let mut current_tick = state.tick_current_index?;
    let mut liquidity = state.liquidity?;
    let sqrt_price_x64 = state.sqrt_price_x64?;
    let fee_rate = state.fee_rate?;
    if liquidity == 0 || req.amount == 0 {
        return None;
    }

    let direction = resolve_direction(state, req)?;
    let zero_for_one = matches!(direction, TradeDirection::AToB);
    let fee_bps = fee_bps_from_hundredth_bip(fee_rate);

    let mut sqrt_price_x96 = math::to_x96(sqrt_price_x64);
    let sqrt_limit_tick = if zero_for_one {
        math::MIN_TICK
    } else {
        math::MAX_TICK
    };
    let sqrt_price_limit = math::sqrt_price_at_tick_x96(sqrt_limit_tick)?;

    let anchor_start = active_tick_array_start(tick_spacing, current_tick);
    let mut cursor =
        WhirlpoolTickArrayCursor::new(hydrator, state.whirlpool, tick_spacing, anchor_start);

    let mut amount_remaining = req.amount as u128;
    let mut amount_out_accum = 0u128;
    let mut steps = 0usize;

    while amount_remaining > 0 && liquidity > 0 && sqrt_price_x96 != sqrt_price_limit {
        steps += 1;
        if steps > 1024 {
            break;
        }

        let mut target_price = sqrt_price_limit;
        let mut crossing = None;
        if let Some(next_tick) = cursor.next_initialized_tick(current_tick, zero_for_one) {
            let tick_price = math::sqrt_price_at_tick_x96(next_tick.index)?;
            if zero_for_one {
                if tick_price > target_price {
                    target_price = tick_price;
                }
            } else if tick_price < target_price {
                target_price = tick_price;
            }
            crossing = Some(next_tick);
        }

        let step = math::compute_swap_step(
            sqrt_price_x96,
            target_price,
            liquidity,
            amount_remaining,
            fee_bps,
            zero_for_one,
        )?;

        if step.amount_in == 0 {
            break;
        }

        sqrt_price_x96 = step.sqrt_price_next;
        amount_remaining = amount_remaining
            .saturating_sub(step.amount_in)
            .saturating_sub(step.fee_amount);
        amount_out_accum = amount_out_accum.saturating_add(step.amount_out);

        if sqrt_price_x96 == target_price {
            if let Some(init_tick) = crossing {
                current_tick = if zero_for_one {
                    init_tick.index - 1
                } else {
                    init_tick.index
                };
                liquidity =
                    apply_liquidity_delta(liquidity, init_tick.liquidity_net, zero_for_one)?;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    if amount_out_accum == 0 {
        return None;
    }

    let consumed_input = (req.amount as u128).saturating_sub(amount_remaining);
    if consumed_input == 0 {
        return None;
    }

    let amount_out = amount_out_accum.min(u64::MAX as u128) as u64;
    let actual_amount_in = consumed_input.min(u64::MAX as u128) as u64;

    let reference_price = match direction {
        TradeDirection::AToB => state.mid_price(),
        TradeDirection::BToA => {
            state
                .mid_price()
                .and_then(|price| if price > 0.0 { Some(1.0 / price) } else { None })
        }
    };

    let implied_price = if actual_amount_in == 0 {
        0.0
    } else {
        (amount_out as f64) / (actual_amount_in as f64)
    };

    let edge_bps = reference_price
        .filter(|price| *price > 0.0 && implied_price > 0.0)
        .map(|price| ((implied_price / price) - 1.0) * 10_000.0)
        .unwrap_or(0.0);

    Some(SlippageSimulation {
        amount_out,
        edge_bps,
        sqrt_price_after: sqrt_price_x64_from_x96(&sqrt_price_x96),
    })
}

fn simulate_best_exact_in(
    hydrator: Option<&WhirlpoolPoolHydrator>,
    state: &WhirlpoolState,
    req: &QuoteRequest,
    amount: u64,
) -> Option<SlippageSimulation> {
    let mut scoped_req = req.clone();
    scoped_req.amount = amount;
    scoped_req.direction = SwapDirection::ExactIn;
    if let Some(h) = hydrator {
        if let Some(simulation) = simulate_tick_arrays_exact_in(h, state, &scoped_req) {
            return Some(simulation);
        }
    }
    simulate_constant_liquidity_exact_in(state, &scoped_req)
}

fn find_input_for_exact_out(
    hydrator: Option<&WhirlpoolPoolHydrator>,
    state: &WhirlpoolState,
    req: &QuoteRequest,
    target_out: u64,
) -> Option<(u64, SlippageSimulation)> {
    if target_out == 0 {
        return None;
    }

    let mut high = target_out.max(1);
    let mut best: Option<(u64, SlippageSimulation)> = None;

    for _ in 0..32 {
        let simulation = simulate_best_exact_in(hydrator, state, req, high)?;
        if simulation.amount_out >= target_out {
            best = Some((high, simulation));
            break;
        }
        if high >= u64::MAX / 2 {
            break;
        }
        high = high.saturating_mul(2);
    }

    let (mut best_input, mut best_sim) = best?;

    let mut low = 1u64;
    while low < best_input {
        let mid = low + (best_input - low) / 2;
        let simulation = simulate_best_exact_in(hydrator, state, req, mid)?;
        if simulation.amount_out >= target_out {
            best_input = mid;
            best_sim = simulation;
        } else {
            if mid == u64::MAX {
                break;
            }
            low = mid.saturating_add(1);
        }
    }

    Some((best_input, best_sim))
}

fn quote_exact_out_from_price(req: &QuoteRequest, state: &WhirlpoolState) -> Option<u64> {
    let price = state.mid_price()?;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    if req.input_mint == state.token_mint_a && req.output_mint == state.token_mint_b {
        let factor = 1.0 / price;
        let amount_in = mul_div_price(req.amount, factor);
        if amount_in == 0 {
            None
        } else {
            Some(amount_in)
        }
    } else if req.input_mint == state.token_mint_b && req.output_mint == state.token_mint_a {
        let amount_in = mul_div_price(req.amount, price);
        if amount_in == 0 {
            None
        } else {
            Some(amount_in)
        }
    } else {
        None
    }
}

fn resolve_direction(state: &WhirlpoolState, req: &QuoteRequest) -> Option<TradeDirection> {
    if req.input_mint == state.token_mint_a && req.output_mint == state.token_mint_b {
        Some(TradeDirection::AToB)
    } else if req.input_mint == state.token_mint_b && req.output_mint == state.token_mint_a {
        Some(TradeDirection::BToA)
    } else {
        None
    }
}

fn fee_bps_from_hundredth_bip(rate: u16) -> u64 {
    ((rate as u64).saturating_add(50)) / 100
}

fn whirlpool_trade_fee_bps(state: &WhirlpoolState) -> u64 {
    state
        .fee_rate
        .map(fee_bps_from_hundredth_bip)
        .unwrap_or_default()
}

fn snapshot_probe_amount(reserve: u128) -> Option<u64> {
    if reserve == 0 {
        return None;
    }

    let mut probe = reserve / SNAPSHOT_PROBE_DIVISOR;
    if probe == 0 {
        probe = SNAPSHOT_MIN_PROBE_INPUT.min(reserve).max(1u128);
    }

    probe = probe.min(reserve);
    probe = probe.clamp(1u128, SNAPSHOT_MAX_PROBE_INPUT);
    Some(probe as u64)
}

fn sqrt_price_x64_from_x96(value: &U256) -> u128 {
    (value >> 32).low_u128()
}

fn apply_liquidity_delta(liquidity: u128, liquidity_net: i128, zero_for_one: bool) -> Option<u128> {
    let mut current = i128::try_from(liquidity).ok()?;
    current = if zero_for_one {
        current.checked_sub(liquidity_net)?
    } else {
        current.checked_add(liquidity_net)?
    };
    if current < 0 {
        None
    } else {
        Some(current as u128)
    }
}

mod math {
    use super::U256;
    use once_cell::sync::Lazy;

    pub const MIN_TICK: i32 = -443_636;
    pub const MAX_TICK: i32 = 443_636;
    const FEE_DENOMINATOR: u128 = 10_000;

    static TICK_RATIO_FACTORS: Lazy<[U256; 20]> = Lazy::new(|| {
        [
            U256::from_str_radix("fffcb933bd6fad37aa2d162d1a594001", 16).unwrap(),
            U256::from_str_radix("fff97272373d413259a46990580e213a", 16).unwrap(),
            U256::from_str_radix("fff2e50f5f656932ef12357cf3c7fdcc", 16).unwrap(),
            U256::from_str_radix("ffe5caca7e10e4e61c3624eaa0941cd0", 16).unwrap(),
            U256::from_str_radix("ffcb9843d60f6159c9db58835c926644", 16).unwrap(),
            U256::from_str_radix("ff973b41fa98c081472e6896dfb254c0", 16).unwrap(),
            U256::from_str_radix("ff2ea16466c96a3843ec78b326b52861", 16).unwrap(),
            U256::from_str_radix("fe5dee046a99a2a811c461f1969c3053", 16).unwrap(),
            U256::from_str_radix("fcbe86c7900a88aedcffc83b479aa3a4", 16).unwrap(),
            U256::from_str_radix("f987a7253ac413176f2b074cf7815e54", 16).unwrap(),
            U256::from_str_radix("f3392b0822b70005940c7a398e4b70f3", 16).unwrap(),
            U256::from_str_radix("e7159475a2c29b7443b29c7fa6e889d9", 16).unwrap(),
            U256::from_str_radix("d097f3bdfd2022b8845ad8f792aa5825", 16).unwrap(),
            U256::from_str_radix("a9f746462d870fdf8a65dc1f90e061e5", 16).unwrap(),
            U256::from_str_radix("70d869a156d2a1b890bb3df62baf32f7", 16).unwrap(),
            U256::from_str_radix("31be135f97d08fd981231505542fcfa6", 16).unwrap(),
            U256::from_str_radix("9aa508b5b7a84e1c677de54f3e99bc9", 16).unwrap(),
            U256::from_str_radix("5d6af8dedb81196699c329225ee604", 16).unwrap(),
            U256::from_str_radix("2216e584f5fa1ea926041bedfe98", 16).unwrap(),
            U256::from_str_radix("48a170391f7dc42444e8fa2", 16).unwrap(),
        ]
    });

    #[derive(Debug, Clone)]
    pub struct SwapStepResult {
        pub sqrt_price_next: U256,
        pub amount_in: u128,
        pub amount_out: u128,
        pub fee_amount: u128,
    }

    pub fn to_x96(value_x64: u128) -> U256 {
        U256::from(value_x64) << 32
    }

    pub fn sqrt_price_at_tick_x96(tick: i32) -> Option<U256> {
        if !(MIN_TICK..=MAX_TICK).contains(&tick) {
            return None;
        }
        let mut ratio = if (tick & 1) != 0 {
            TICK_RATIO_FACTORS[0]
        } else {
            U256::one() << 128
        };
        let abs_tick = tick.abs();
        for (i, factor) in TICK_RATIO_FACTORS.iter().enumerate().skip(1) {
            if (abs_tick & (1 << i)) != 0 {
                ratio = mul_shift_128(ratio, *factor);
            }
        }

        if tick > 0 {
            ratio = U256::MAX / ratio;
        }

        let remainder_mask = (U256::one() << 32) - U256::one();
        let mut ratio_shifted = ratio >> 32;
        if ratio & remainder_mask != U256::zero() {
            ratio_shifted += U256::one();
        }
        Some(ratio_shifted)
    }

    pub fn compute_swap_step(
        sqrt_price_current: U256,
        sqrt_price_target: U256,
        liquidity: u128,
        amount_remaining: u128,
        fee_bps: u64,
        zero_for_one: bool,
    ) -> Option<SwapStepResult> {
        if liquidity == 0 {
            return None;
        }

        let fee_bps = fee_bps as u128;
        if fee_bps >= FEE_DENOMINATOR {
            return None;
        }

        let amount_less_fee = amount_remaining
            .checked_mul(FEE_DENOMINATOR - fee_bps)?
            .checked_div(FEE_DENOMINATOR)?;

        let (sqrt_price_next, amount_in, amount_out) = if zero_for_one {
            let amount_in_max =
                get_amount0_delta(liquidity, sqrt_price_target, sqrt_price_current, true)?;
            if amount_less_fee >= amount_in_max {
                let out =
                    get_amount1_delta(liquidity, sqrt_price_target, sqrt_price_current, false)?;
                (sqrt_price_target, amount_in_max, out)
            } else {
                let next_price = get_next_sqrt_price_from_input(
                    sqrt_price_current,
                    liquidity,
                    amount_less_fee,
                    true,
                )?;
                let in_amount = get_amount0_delta(liquidity, next_price, sqrt_price_current, true)?;
                let out = get_amount1_delta(liquidity, next_price, sqrt_price_current, false)?;
                (next_price, in_amount, out)
            }
        } else {
            let amount_in_max =
                get_amount1_delta(liquidity, sqrt_price_current, sqrt_price_target, true)?;
            if amount_less_fee >= amount_in_max {
                let out =
                    get_amount0_delta(liquidity, sqrt_price_current, sqrt_price_target, false)?;
                (sqrt_price_target, amount_in_max, out)
            } else {
                let next_price = get_next_sqrt_price_from_input(
                    sqrt_price_current,
                    liquidity,
                    amount_less_fee,
                    false,
                )?;
                let in_amount = get_amount1_delta(liquidity, sqrt_price_current, next_price, true)?;
                let out = get_amount0_delta(liquidity, sqrt_price_current, next_price, false)?;
                (next_price, in_amount, out)
            }
        };

        let fee_amount = mul_div_round_up(amount_in, fee_bps, FEE_DENOMINATOR - fee_bps)?;

        Some(SwapStepResult {
            sqrt_price_next,
            amount_in,
            amount_out,
            fee_amount,
        })
    }

    fn get_amount0_delta(
        liquidity: u128,
        sqrt_a: U256,
        sqrt_b: U256,
        round_up: bool,
    ) -> Option<u128> {
        let (sqrt_low, sqrt_high) = if sqrt_a > sqrt_b {
            (sqrt_b, sqrt_a)
        } else {
            (sqrt_a, sqrt_b)
        };
        if sqrt_low == U256::zero() {
            return None;
        }
        let numerator1 = U256::from(liquidity) << 96;
        let numerator2 = sqrt_high.checked_sub(sqrt_low)?;
        let numerator = numerator1.checked_mul(numerator2)?;
        let denominator = sqrt_high.checked_mul(sqrt_low)?;
        let quotient = if round_up {
            div_round_up(numerator, denominator)
        } else {
            numerator.checked_div(denominator)?
        };
        quotient.try_into().ok()
    }

    fn get_amount1_delta(
        liquidity: u128,
        sqrt_a: U256,
        sqrt_b: U256,
        round_up: bool,
    ) -> Option<u128> {
        let (sqrt_low, sqrt_high) = if sqrt_a > sqrt_b {
            (sqrt_b, sqrt_a)
        } else {
            (sqrt_a, sqrt_b)
        };
        let diff = sqrt_high.checked_sub(sqrt_low)?;
        let numerator = U256::from(liquidity).checked_mul(diff)?;
        let denominator = U256::one() << 96;
        let quotient = if round_up {
            div_round_up(numerator, denominator)
        } else {
            numerator >> 96
        };
        quotient.try_into().ok()
    }

    fn get_next_sqrt_price_from_input(
        sqrt_price_current: U256,
        liquidity: u128,
        amount_in: u128,
        zero_for_one: bool,
    ) -> Option<U256> {
        if amount_in == 0 {
            return Some(sqrt_price_current);
        }
        let liquidity_u256 = U256::from(liquidity);
        if zero_for_one {
            let numerator = sqrt_price_current
                .checked_mul(liquidity_u256)?
                .checked_mul(U256::one() << 96)?;
            let denominator = liquidity_u256
                .checked_mul(U256::one() << 96)?
                .checked_add(U256::from(amount_in).checked_mul(sqrt_price_current)?)?;
            Some(numerator.checked_div(denominator)?)
        } else {
            let increment = (U256::from(amount_in) << 96).checked_div(liquidity_u256)?;
            Some(sqrt_price_current.checked_add(increment)?)
        }
    }

    fn mul_shift_128(value: U256, multiplier: U256) -> U256 {
        value.checked_mul(multiplier).unwrap_or_default() >> 128
    }

    fn div_round_up(numerator: U256, denominator: U256) -> U256 {
        let mut quotient = numerator / denominator;
        if numerator % denominator != U256::zero() {
            quotient += U256::one();
        }
        quotient
    }

    fn mul_div_round_up(value: u128, numerator: u128, denominator: u128) -> Option<u128> {
        if denominator == 0 {
            return None;
        }
        let numerator_u256 = U256::from(value).checked_mul(U256::from(numerator))?;
        let result = div_round_up(numerator_u256, U256::from(denominator));
        result.try_into().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::{QuoteRequest, SwapDirection};
    use crate::stages::types::{PoolSnapshot, PoolUpdateExtensions};
    use dashmap::DashMap;

    #[test]
    fn parses_whirlpool_account_layout() {
        let pool = Pubkey::new_unique();
        let config = Pubkey::new_unique();
        let token_mint_a = Pubkey::new_unique();
        let token_mint_b = Pubkey::new_unique();
        let token_vault_a = Pubkey::new_unique();
        let token_vault_b = Pubkey::new_unique();
        let liquidity = 42_000_000_000_000_000_000u128;
        let sqrt_price = 79228162514264337593543950336u128; // 2^96
        let tick_current_index = -120i32;
        let mut data = vec![0u8; WHIRLPOOL_ACCOUNT_MIN_LEN];

        let mut cursor = 0;
        cursor += ANCHOR_DISCRIMINATOR_LEN;
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(config.as_ref());
        cursor += PUBKEY_BYTES;
        data[cursor] = 3; // bump
        cursor += 1;
        data[cursor..cursor + 2].copy_from_slice(10u16.to_le_bytes().as_ref());
        cursor += 2;
        cursor += 2; // fee tier seed prefix
        data[cursor..cursor + 2].copy_from_slice(300u16.to_le_bytes().as_ref());
        cursor += 2;
        data[cursor..cursor + 2].copy_from_slice(25u16.to_le_bytes().as_ref());
        cursor += 2;
        data[cursor..cursor + 16].copy_from_slice(liquidity.to_le_bytes().as_ref());
        cursor += 16;
        data[cursor..cursor + 16].copy_from_slice(sqrt_price.to_le_bytes().as_ref());
        cursor += 16;
        data[cursor..cursor + 4].copy_from_slice(tick_current_index.to_le_bytes().as_ref());
        cursor += 4;
        cursor += 8; // protocol fee owed A
        cursor += 8; // protocol fee owed B
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(token_mint_a.as_ref());
        cursor += PUBKEY_BYTES;
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(token_vault_a.as_ref());
        cursor += PUBKEY_BYTES;
        cursor += 16; // fee growth global A
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(token_mint_b.as_ref());
        cursor += PUBKEY_BYTES;
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(token_vault_b.as_ref());
        cursor += PUBKEY_BYTES;
        cursor += 16; // fee growth global B
        cursor += 8; // reward last updated timestamp
        cursor += WHIRLPOOL_REWARD_INFO_BYTES * 3; // reward infos
        assert_eq!(cursor, WHIRLPOOL_ACCOUNT_MIN_LEN);

        let program_id = Pubkey::from_str(WHIRLPOOL_PROGRAM_ID).unwrap();
        let meta = parse_whirlpool_account(pool, &program_id, &data).expect("should parse");

        assert_eq!(meta.token_mint_a, token_mint_a);
        assert_eq!(meta.token_mint_b, token_mint_b);
        assert_eq!(meta.token_vault_a, token_vault_a);
        assert_eq!(meta.token_vault_b, token_vault_b);
        assert_eq!(meta.tick_spacing, 10);
        assert_eq!(meta.liquidity, liquidity);
        assert_eq!(meta.sqrt_price_x64, sqrt_price);
        assert_eq!(meta.tick_current_index, tick_current_index);
        assert_eq!(meta.fee_rate, 300);
        assert_eq!(meta.protocol_fee_rate, 25);
        let (expected_oracle, _) =
            Pubkey::find_program_address(&[b"oracle", pool.as_ref()], &program_id);
        assert_eq!(meta.oracle, expected_oracle);
    }

    #[test]
    fn constant_liquidity_simulation_applies_slippage() {
        let pool = Pubkey::new_unique();
        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        let state = WhirlpoolState {
            whirlpool: pool,
            token_mint_a: token_a,
            token_mint_b: token_b,
            token_vault_a: None,
            token_vault_b: None,
            tick_spacing: Some(1),
            liquidity: Some(1_000_000_000_000u128),
            sqrt_price_x64: Some(1u128 << 64),
            tick_current_index: Some(0),
            fee_rate: Some(500),
            protocol_fee_rate: Some(0),
            oracle: None,
            tick_array0: None,
            tick_array1: None,
            tick_array2: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: token_a,
            output_mint: token_b,
            amount: 1_000_000,
            slot: 0,
            direction: SwapDirection::ExactIn,
        };

        let result = simulate_constant_liquidity_exact_in(&state, &req).expect("sim");
        assert!(result.amount_out < req.amount);
        assert!(result.edge_bps < 0.0);
    }

    #[test]
    fn simulation_requires_matching_direction() {
        let pool = Pubkey::new_unique();
        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        let state = WhirlpoolState {
            whirlpool: pool,
            token_mint_a: token_a,
            token_mint_b: token_b,
            token_vault_a: None,
            token_vault_b: None,
            tick_spacing: Some(1),
            liquidity: Some(1_000_000_000_000u128),
            sqrt_price_x64: Some(1u128 << 64),
            tick_current_index: Some(0),
            fee_rate: Some(500),
            protocol_fee_rate: Some(0),
            oracle: None,
            tick_array0: None,
            tick_array1: None,
            tick_array2: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: Pubkey::new_unique(),
            output_mint: token_b,
            amount: 5,
            slot: 0,
            direction: SwapDirection::ExactIn,
        };

        assert!(simulate_constant_liquidity_exact_in(&state, &req).is_none());
    }

    #[test]
    fn tick_array_simulation_uses_seeded_tick_data() {
        let pool = Pubkey::new_unique();
        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        let hydrator = test_hydrator_with_tick(pool, 0, 1);
        let state = WhirlpoolState {
            whirlpool: pool,
            token_mint_a: token_a,
            token_mint_b: token_b,
            token_vault_a: None,
            token_vault_b: None,
            tick_spacing: Some(1),
            liquidity: Some(1_000_000u128),
            sqrt_price_x64: Some(1u128 << 64),
            tick_current_index: Some(0),
            fee_rate: Some(300),
            protocol_fee_rate: Some(0),
            oracle: None,
            tick_array0: None,
            tick_array1: None,
            tick_array2: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: token_b,
            output_mint: token_a,
            amount: 50_000,
            slot: 0,
            direction: SwapDirection::ExactIn,
        };

        let result = simulate_tick_arrays_exact_in(&hydrator, &state, &req).expect("sim");
        assert!(result.amount_out > 0);
        assert!(result.sqrt_price_after >= state.sqrt_price_x64.unwrap());
    }

    #[test]
    fn exact_out_search_finds_minimum_input() {
        let pool = Pubkey::new_unique();
        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        let hydrator = test_hydrator_with_tick(pool, 0, 1);
        let state = WhirlpoolState {
            whirlpool: pool,
            token_mint_a: token_a,
            token_mint_b: token_b,
            token_vault_a: None,
            token_vault_b: None,
            tick_spacing: Some(1),
            liquidity: Some(5_000_000u128),
            sqrt_price_x64: Some(1u128 << 64),
            tick_current_index: Some(0),
            fee_rate: Some(300),
            protocol_fee_rate: Some(0),
            oracle: None,
            tick_array0: None,
            tick_array1: None,
            tick_array2: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: token_a,
            output_mint: token_b,
            amount: 10_000,
            slot: 0,
            direction: SwapDirection::ExactOut,
        };

        let (amount_in, simulation) =
            find_input_for_exact_out(Some(&hydrator), &state, &req, req.amount).expect("search");

        assert!(amount_in > 0);
        assert!(simulation.amount_out >= req.amount);
    }

    fn test_hydrator_with_tick(
        pool: Pubkey,
        start_tick_index: i32,
        tick_index: usize,
    ) -> WhirlpoolPoolHydrator {
        let mut config = WhirlpoolAdapterConfig::default();
        config.rpc_endpoints = vec!["http://localhost:8899".to_string()];
        let program_id = Pubkey::new_unique();
        let hydrator = WhirlpoolPoolHydrator::new(&config, program_id).expect("hydrator fixture");
        let mut ticks = vec![
            TickEntry {
                initialized: false,
                liquidity_net: 0,
                liquidity_gross: 0,
            };
            WHIRLPOOL_TICK_ARRAY_TICK_COUNT
        ];
        if let Some(entry) = ticks.get_mut(tick_index) {
            *entry = TickEntry {
                initialized: true,
                liquidity_net: 0,
                liquidity_gross: 1,
            };
        }
        let array = HydratedTickArray {
            _address: Pubkey::new_unique(),
            _pool: pool,
            start_tick_index,
            ticks,
        };
        hydrator
            .tick_array_cache
            .insert((pool, start_tick_index), array);
        hydrator
    }

    #[test]
    fn snapshot_bidirectional_quotes_emit_sides() {
        let adapter = WhirlpoolAdapter {
            program_id: Pubkey::from_str(WHIRLPOOL_PROGRAM_ID).unwrap(),
            state: DashMap::new(),
            config: WhirlpoolAdapterConfig::default(),
            hydrator: None,
            template_builder: None,
            registered_templates: DashSet::new(),
        };

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();

        let state = WhirlpoolState {
            whirlpool: pool,
            token_mint_a: base_mint,
            token_mint_b: quote_mint,
            token_vault_a: None,
            token_vault_b: None,
            tick_spacing: Some(1),
            liquidity: Some(5_000_000u128),
            sqrt_price_x64: Some(1u128 << 64),
            tick_current_index: Some(0),
            fee_rate: Some(500),
            protocol_fee_rate: Some(0),
            oracle: None,
            tick_array0: None,
            tick_array1: None,
            tick_array2: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        adapter.state.insert(pool, state);

        let snapshot = PoolSnapshot {
            pool_id: pool.to_string(),
            program_id: adapter.program_id.to_string(),
            slot: 1,
            base_mint: base_mint.to_string(),
            quote_mint: quote_mint.to_string(),
            base_post_amount: 2_000_000u128,
            quote_post_amount: 3_000_000u128,
            updated_at: Instant::now(),
            quotes: None,
            extensions: PoolUpdateExtensions::default(),
        };

        let quotes = adapter
            .bidirectional_quotes(&snapshot)
            .expect("quotes missing");

        assert_eq!(quotes.base_to_quote.input_mint, snapshot.base_mint);
        assert_eq!(quotes.quote_to_base.input_mint, snapshot.quote_mint);
        assert!(quotes.base_to_quote.expected_output > 0);
        assert!(quotes.quote_to_base.expected_output > 0);
    }
}
