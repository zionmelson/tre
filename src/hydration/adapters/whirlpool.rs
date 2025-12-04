use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::{mapref::entry::Entry, DashMap};
use solana_client::{client_error::ClientError, rpc_client::RpcClient};
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};
use tracing::{debug, info, trace, warn};
use uint::construct_uint;

use super::hydration::AsyncHydrator;

use crate::builder::{BuilderHandle, WhirlpoolPoolAccounts, WhirlpoolPoolMeta};
use crate::hydration::types::{
    PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate, WhirlpoolPoolExtras,
    WHIRLPOOL_INPUT_UTILIZATION_BPS,
};

use super::{
    build_quote_response, mul_div_price, quote_from_price, AdapterReadiness, ProtocolAdapter,
    QuoteRequest, QuoteResponse, QuoteStatus, ReadinessBlocker, ReadinessBlockerKind,
    SwapDirection, TemplateContext, WhirlpoolRemainingAccountsSlice, WhirlpoolRemainingAccountsType,
};

const WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const TICK_ARRAY_SEED: &[u8] = b"tick_array";
const SNAPSHOT_PROBE_DIVISOR: u128 = 5_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;
const WHIRLPOOL_DEFAULT_CU_LIMIT: u32 = 380_000;
const WHIRLPOOL_DEFAULT_CU_PRICE: u64 = 5_000;
const BPS_DENOMINATOR: u64 = 10_000;
const DEFAULT_WHIRLPOOL_POOL_BACKOFF_MS: u64 = 5_000;
const DEFAULT_WHIRLPOOL_TICK_BACKOFF_MS: u64 = 15_000;
const DEFAULT_WHIRLPOOL_MAX_POOL_FAILURES: u32 = 5;
const DEFAULT_WHIRLPOOL_MAX_TICK_FAILURES: u32 = 5;

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
    pub max_swap_utilization_bps: u64,
    pub pool_failure_backoff_ms: u64,
    pub tick_array_backoff_ms: u64,
    pub max_pool_failures: u32,
    pub max_tick_array_failures: u32,
}

impl Default for WhirlpoolAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 15_000,
            hydration_retry_ms: DEFAULT_WHIRLPOOL_HYDRATION_RETRY_MS,
            max_swap_utilization_bps: WHIRLPOOL_INPUT_UTILIZATION_BPS,
            pool_failure_backoff_ms: DEFAULT_WHIRLPOOL_POOL_BACKOFF_MS,
            tick_array_backoff_ms: DEFAULT_WHIRLPOOL_TICK_BACKOFF_MS,
            max_pool_failures: DEFAULT_WHIRLPOOL_MAX_POOL_FAILURES,
            max_tick_array_failures: DEFAULT_WHIRLPOOL_MAX_TICK_FAILURES,
        }
    }
}

pub struct WhirlpoolAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, WhirlpoolState>,
    config: WhirlpoolAdapterConfig,
    hydrator: Option<WhirlpoolPoolHydrator>,
    template_builder: Option<BuilderHandle>,
    registered_templates: DashMap<Pubkey, WhirlpoolRegistration>,
}

#[derive(Clone, Debug, Default)]
struct WhirlpoolRegistration {
    alt_accounts: Vec<Pubkey>,
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
            registered_templates: DashMap::new(),
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

        let base_mint = parse_pubkey(&update.base_mint);
        let quote_mint = parse_pubkey(&update.quote_mint);
        let (reserve_a, reserve_b) = match (base_mint, quote_mint) {
            (Some(base), Some(quote)) if base == token_mint_a && quote == token_mint_b => (
                Some(update.base_post_amount),
                Some(update.quote_post_amount),
            ),
            (Some(base), Some(quote)) if base == token_mint_b && quote == token_mint_a => (
                Some(update.quote_post_amount),
                Some(update.base_post_amount),
            ),
            _ => (None, None),
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
            reserve_a,
            reserve_b,
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

        let alt_accounts = Self::static_alt_accounts(state);
        if alt_accounts.is_empty() {
            return;
        }

        if let Some(existing) = self.registered_templates.get(&state.whirlpool) {
            let alt_missing = builder
                .alt_manager()
                .handle_for_pool(state.whirlpool)
                .is_none();
            if existing.alt_accounts == alt_accounts && !alt_missing {
                return;
            }
            debug!(
                target = "adapter::whirlpool",
                pool = %state.whirlpool,
                alt_missing,
                old_accounts = existing.alt_accounts.len(),
                new_accounts = alt_accounts.len(),
                "refreshing whirlpool ALT registration"
            );
        }

        let fee_bps = fee_bps_from_hundredth_bip(raw_fee);
        let fee_tier_bps = fee_bps.min(u64::from(u16::MAX)) as u16;
        let pool_accounts = WhirlpoolPoolAccounts {
            whirlpool: state.whirlpool,
            token_mint_a: state.token_mint_a,
            token_mint_b: state.token_mint_b,
            token_vault_a: state.token_vault_a,
            token_vault_b: state.token_vault_b,
            oracle: state.oracle,
        };

        builder.register_whirlpool_pool(WhirlpoolPoolMeta {
            pool_id: state.whirlpool,
            fee_tier_bps,
            alt_accounts: alt_accounts.clone(),
            alt_handle: None,
            accounts: pool_accounts,
        });

        self.registered_templates
            .insert(state.whirlpool, WhirlpoolRegistration { alt_accounts });
    }

    fn static_alt_accounts(state: &WhirlpoolState) -> Vec<Pubkey> {
        let mut accounts = Vec::with_capacity(6);
        accounts.push(state.whirlpool);
        accounts.push(state.token_mint_a);
        accounts.push(state.token_mint_b);
        if let Some(pk) = state.token_vault_a {
            accounts.push(pk);
        }
        if let Some(pk) = state.token_vault_b {
            accounts.push(pk);
        }
        if let Some(pk) = state.oracle {
            accounts.push(pk);
        }
        Self::dedup_pubkeys(&mut accounts);
        accounts
    }

    fn dedup_pubkeys(accounts: &mut Vec<Pubkey>) {
        let mut seen = HashSet::with_capacity(accounts.len());
        accounts.retain(|pk| seen.insert(*pk));
    }

    fn decorate_template_context(&self, response: &mut QuoteResponse, state: &WhirlpoolState) {
        let mut context = self.snapshot_template_context(state);
        let preferred_arrays_full = response.context.whirlpool_tick_arrays.clone();
        let preferred_arrays = preferred_arrays_full.clone();
        if let Some(arrays) = self.resolve_tick_arrays(state, &response.request, &preferred_arrays)
        {
            context.whirlpool_tick_arrays = arrays;
        }
        self.attach_owner_accounts(
            state,
            &mut context,
            response.request.input_mint,
            response.request.output_mint,
        );
        let supplemental_arrays: Vec<Pubkey> = preferred_arrays_full
            .into_iter()
            .skip(3)
            .collect();
        self.attach_supplemental_tick_arrays(&mut context, &supplemental_arrays);
        response.context = context;
    }

    fn resolve_tick_arrays(
        &self,
        state: &WhirlpoolState,
        req: &QuoteRequest,
        preferred: &[Pubkey],
    ) -> Option<Vec<Pubkey>> {
        const REQUIRED: usize = 3;
        let mut resolved = Vec::with_capacity(REQUIRED);
        let mut seen = HashSet::with_capacity(REQUIRED);
        for pk in preferred {
            if seen.insert(*pk) {
                resolved.push(*pk);
            }
            if resolved.len() == REQUIRED {
                return Some(resolved);
            }
        }

        let fallback = self.tick_arrays_for_request(state, req)?;
        for pk in fallback {
            if seen.insert(pk) {
                resolved.push(pk);
            }
            if resolved.len() == REQUIRED {
                break;
            }
        }

        if resolved.len() == REQUIRED {
            Some(resolved)
        } else {
            None
        }
    }

    fn attach_owner_accounts(
        &self,
        state: &WhirlpoolState,
        context: &mut TemplateContext,
        input_mint: Pubkey,
        output_mint: Pubkey,
    ) {
        let Some(builder) = self.template_builder.as_ref() else {
            return;
        };
        if context.payer.is_none() {
            context.payer = Some(builder.payer_pubkey());
        }

        if context.input_token_account.is_none() {
            if let Some(ata) = builder.token_account_for_mint(&input_mint) {
                context.input_token_account = Some(ata);
            } else {
                warn!(
                    target = "adapter::whirlpool",
                    pool = %state.whirlpool,
                    mint = %input_mint,
                    "missing configured input token ATA"
                );
            }
        }
        if context.output_token_account.is_none() {
            if let Some(ata) = builder.token_account_for_mint(&output_mint) {
                context.output_token_account = Some(ata);
            } else {
                warn!(
                    target = "adapter::whirlpool",
                    pool = %state.whirlpool,
                    mint = %output_mint,
                    "missing configured output token ATA"
                );
            }
        }
    }

    fn snapshot_template_context(&self, _state: &WhirlpoolState) -> TemplateContext {
        let context = TemplateContext {
            compute_unit_limit: Some(WHIRLPOOL_DEFAULT_CU_LIMIT),
            compute_unit_price: Some(WHIRLPOOL_DEFAULT_CU_PRICE),
            ..Default::default()
        };
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

    fn fallback_tick_arrays_from_state(&self, state: &WhirlpoolState) -> Option<Vec<Pubkey>> {
        let arrays = [state.tick_array0?, state.tick_array1?, state.tick_array2?];
        Some(arrays.into_iter().collect())
    }

    fn tick_arrays_for_direction(
        &self,
        state: &WhirlpoolState,
        direction: TradeDirection,
    ) -> Option<Vec<Pubkey>> {
        self.fallback_tick_arrays_from_state(state)
            .or_else(|| self.derive_tick_arrays_for_direction(state, direction))
    }

    fn derive_tick_arrays_for_direction(
        &self,
        state: &WhirlpoolState,
        direction: TradeDirection,
    ) -> Option<Vec<Pubkey>> {
        let tick_spacing = state.tick_spacing?;
        let current_tick = state.tick_current_index?;
        let mut span = tick_array_span(tick_spacing)?;
        if span == 0 {
            return None;
        }
        if span < 0 {
            span = span.abs();
        }
        let anchor = active_tick_array_start(tick_spacing, current_tick)?;
        let offsets = match direction {
            TradeDirection::AToB => [0, -1, -2],
            TradeDirection::BToA => [0, 1, 2],
        };

        let mut arrays = Vec::with_capacity(3);
        for multiplier in offsets {
            let step = span.checked_mul(multiplier)?;
            let start = match multiplier {
                0 => anchor,
                _ => anchor.checked_add(step)?,
            };
            if start < math::MIN_TICK || start > math::MAX_TICK {
                return None;
            }
            arrays.push(derive_tick_array_address(
                &state.whirlpool,
                start,
                &self.program_id,
            ));
        }
        Some(arrays)
    }

    fn tick_arrays_for_request(
        &self,
        state: &WhirlpoolState,
        req: &QuoteRequest,
    ) -> Option<Vec<Pubkey>> {
        let direction = resolve_direction(state, req)?;
        self.tick_arrays_for_direction(state, direction)
    }

    fn clamp_amount_for_mint(&self, state: &WhirlpoolState, mint: Pubkey, amount: u64) -> u64 {
        let Some(reserve) = state.reserve_for_mint(mint) else {
            return amount;
        };
        clamp_amount_to_reserve(amount, reserve, self.config.max_swap_utilization_bps)
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

        let direction = resolve_direction(state, &req)?;
        let mut preferred_arrays = Vec::new();
        let simulation =
            simulate_authoritative_exact_in(self.hydrator.as_ref(), state, &req, probe_amount)
                .ok()?;
        let sqrt_price_limit = Some(simulation.sqrt_price_after);
        if !simulation.tick_arrays.is_empty() {
            preferred_arrays = simulation.tick_arrays.clone();
        }
        let expected_output = simulation.amount_out;
        let slippage_bps = simulation.edge_bps;

        if expected_output == 0 {
            return None;
        }

        let effective_price = if probe_amount == 0 {
            0.0
        } else {
            expected_output as f64 / simulation.consumed_input as f64
        };

        let fee_bps = whirlpool_trade_fee_bps(state);
        let template_fee_bps = fee_bps.min(u64::from(u16::MAX)) as u16;

        let mut context = self.snapshot_template_context(state);
        if let Some(arrays) = self.resolve_tick_arrays(state, &req, &preferred_arrays) {
            context.whirlpool_tick_arrays = arrays;
        } else if let Some(arrays) = self.tick_arrays_for_direction(state, direction) {
            context.whirlpool_tick_arrays = arrays;
        }
        self.attach_owner_accounts(state, &mut context, input_mint, output_mint);

        Some(PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional: probe_amount,
            expected_output,
            fee_bps,
            template_fee_bps,
            effective_price,
            estimated_slippage_bps: slippage_bps,
            sqrt_price_limit,
            min_output_amount: Some(expected_output),
            limit_price: None,
            price_impact_limit_bps: None,
            context,
            status: QuoteStatus::Ok,
            sqrt_price_before_x64: simulation.sqrt_price_before,
            sqrt_price_after_x64: Some(simulation.sqrt_price_after),
            tick_arrays_used: preferred_arrays.iter().map(|pk| pk.to_string()).collect(),
            cache_slot: Some(slot),
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

    fn enrich_snapshot(&self, snapshot: &mut PoolSnapshot) {
        let Some(pool_pubkey) = parse_pubkey(&snapshot.pool_id) else {
            return;
        };
        let Some(state) = self.get_state(&pool_pubkey) else {
            return;
        };
        let extras = snapshot
            .extensions
            .whirlpool
            .get_or_insert_with(WhirlpoolPoolExtras::default);

        if extras.token_vault_a.is_none() {
            extras.token_vault_a = state.token_vault_a.map(|pk| pk.to_string());
        }
        if extras.token_vault_b.is_none() {
            extras.token_vault_b = state.token_vault_b.map(|pk| pk.to_string());
        }
        if extras.oracle.is_none() {
            extras.oracle = state.oracle.map(|pk| pk.to_string());
        }
        if extras.tick_array0.is_none() {
            extras.tick_array0 = state.tick_array0.map(|pk| pk.to_string());
        }
        if extras.tick_array1.is_none() {
            extras.tick_array1 = state.tick_array1.map(|pk| pk.to_string());
        }
        if extras.tick_array2.is_none() {
            extras.tick_array2 = state.tick_array2.map(|pk| pk.to_string());
        }
        extras
            .token_mint_a
            .get_or_insert_with(|| state.token_mint_a.to_string());
        extras
            .token_mint_b
            .get_or_insert_with(|| state.token_mint_b.to_string());
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let mut request = req;
        let capped_amount = self.clamp_amount_for_mint(&state, request.input_mint, request.amount);
        if capped_amount == 0 {
            return None;
        }
        request.amount = capped_amount;

        match simulate_best_exact_in(self.hydrator.as_ref(), &state, &request, request.amount) {
            Ok(simulation) => {
                let mut response = build_quote_response(request, simulation.amount_out);
                response.edge_bps = simulation.edge_bps;
                response.sqrt_price_limit = Some(simulation.sqrt_price_after);
                response.min_output_amount = Some(simulation.amount_out);
                if !simulation.tick_arrays.is_empty() {
                    response.context.whirlpool_tick_arrays = simulation.tick_arrays.clone();
                }
                self.decorate_template_context(&mut response, &state);
                Some(response)
            }
            Err(err) => {
                let status: QuoteStatus = err.into();
                let price = state.mid_price()?;
                let amount_out =
                    quote_from_price(&request, &state.token_mint_a, &state.token_mint_b, price)?;
                let mut fallback = build_quote_response(request, amount_out);
                fallback.status = status;
                fallback.min_output_amount = Some(amount_out);
                self.decorate_template_context(&mut fallback, &state);
                Some(fallback)
            }
        }
    }

    fn quote_exact_out(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactOut {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let mut request = req;
        let capped_amount = self.clamp_amount_for_mint(&state, request.output_mint, request.amount);
        if capped_amount == 0 {
            return None;
        }
        request.amount = capped_amount;
        if request.amount == 0 {
            return None;
        }
        match find_input_for_exact_out(self.hydrator.as_ref(), &state, &request, request.amount) {
            Ok((amount_in, simulation)) => {
                let mut response = QuoteResponse {
                    amount_in,
                    amount_out: request.amount,
                    edge_bps: simulation.edge_bps,
                    sqrt_price_limit: Some(simulation.sqrt_price_after),
                    min_output_amount: None,
                    status: QuoteStatus::Ok,
                    request,
                    context: TemplateContext::default(),
                };
                if !simulation.tick_arrays.is_empty() {
                    response.context.whirlpool_tick_arrays = simulation.tick_arrays.clone();
                }
                self.decorate_template_context(&mut response, &state);
                Some(response)
            }
            Err(err) => {
                let status: QuoteStatus = err.into();
                let amount_in = quote_exact_out_from_price(&request, &state)?;
                let mut response = QuoteResponse {
                    amount_in,
                    amount_out: request.amount,
                    edge_bps: 0.0,
                    sqrt_price_limit: None,
                    min_output_amount: None,
                    status,
                    request,
                    context: TemplateContext::default(),
                };
                self.decorate_template_context(&mut response, &state);
                Some(response)
            }
        }
    }

    fn bidirectional_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
        let state = self.get_state(&pool_pubkey)?;
        let base_mint = parse_pubkey(&snapshot.base_mint)?;
        let quote_mint = parse_pubkey(&snapshot.quote_mint)?;
        if base_mint == quote_mint {
            return None;
        }

        let utilization = self.config.max_swap_utilization_bps;
        let base_probe = snapshot_probe_amount(snapshot.base_post_amount, utilization)?;
        let quote_probe = snapshot_probe_amount(snapshot.quote_post_amount, utilization)?;

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

    fn readiness_blockers(&self) -> AdapterReadiness {
        if let Some(hydrator) = self.hydrator.as_ref() {
            AdapterReadiness {
                blockers: hydrator.readiness_blockers(),
            }
        } else {
            AdapterReadiness::default()
        }
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
    address: Pubkey,
    start_tick_index: i32,
    ticks: Vec<TickEntry>,
}

#[derive(Clone, Debug)]
struct TickEntry {
    initialized: bool,
    liquidity_net: i128,
    liquidity_gross: u128,
}

#[derive(Clone, Debug)]
struct FailureWindow {
    failures: u32,
    next_retry_at: Instant,
}

impl FailureWindow {
    fn new(now: Instant) -> Self {
        Self {
            failures: 0,
            next_retry_at: now,
        }
    }
}

fn failure_backoff_pending<K>(
    store: &DashMap<K, FailureWindow>,
    key: &K,
    now: Instant,
) -> Option<Duration>
where
    K: Eq + Hash,
{
    store
        .get(key)
        .and_then(|entry| entry.next_retry_at.checked_duration_since(now))
}

fn failure_clear<K>(store: &DashMap<K, FailureWindow>, key: &K)
where
    K: Eq + Hash,
{
    store.remove(key);
}

fn failure_record<K>(
    store: &DashMap<K, FailureWindow>,
    key: K,
    now: Instant,
    base_backoff: Duration,
    max_failures: u32,
) -> u32
where
    K: Eq + Hash,
{
    let capped_max = max_failures.max(1);
    match store.entry(key) {
        Entry::Occupied(mut entry) => {
            let state = entry.get_mut();
            state.failures = state.failures.saturating_add(1);
            let capped = state.failures.min(capped_max);
            let delay = base_backoff.saturating_mul(capped);
            state.next_retry_at = now + delay;
            state.failures
        }
        Entry::Vacant(entry) => {
            let mut state = FailureWindow::new(now);
            state.failures = 1;
            let delay = base_backoff.saturating_mul(1);
            state.next_retry_at = now + delay;
            entry.insert(state);
            1
        }
    }
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
    pool_failures: DashMap<Pubkey, FailureWindow>,
    tick_failures: DashMap<(Pubkey, i32), FailureWindow>,
    pool_backoff: Duration,
    tick_backoff: Duration,
    max_pool_failures: u32,
    max_tick_failures: u32,
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
        let pool_backoff = Duration::from_millis(config.pool_failure_backoff_ms.max(1));
        let tick_backoff = Duration::from_millis(config.tick_array_backoff_ms.max(1));
        let max_pool_failures = config.max_pool_failures.max(1);
        let max_tick_failures = config.max_tick_array_failures.max(1);
        let pool_failures: DashMap<Pubkey, FailureWindow> = DashMap::new();
        let tick_failures: DashMap<(Pubkey, i32), FailureWindow> = DashMap::new();
        let hydrator_failures = pool_failures.clone();
        let hydrator = AsyncHydrator::new("whirlpool_pool", endpoint_count, move |pool| {
            let now = Instant::now();
            if let Some(wait) = failure_backoff_pending(&hydrator_failures, &pool, now) {
                trace!(
                    target = "adapter::whirlpool",
                    pool = %pool,
                    wait_ms = wait.as_millis(),
                    "pool hydration backoff active"
                );
                return None;
            }

            match fetch_whirlpool_meta(program_id, fetch_clients.as_ref(), pool) {
                Ok(meta) => {
                    failure_clear(&hydrator_failures, &pool);
                    Some(meta)
                }
                Err(error) => {
                    let failures = failure_record(
                        &hydrator_failures,
                        pool,
                        now,
                        pool_backoff,
                        max_pool_failures,
                    );
                    log_pool_fetch_error(pool, &error, failures);
                    None
                }
            }
        });

        Some(Self {
            program_id,
            hydrator,
            tick_array_cache: DashMap::new(),
            clients,
            endpoint_count,
            pool_failures,
            tick_failures,
            pool_backoff,
            tick_backoff,
            max_pool_failures,
            max_tick_failures,
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

    fn tick_backoff_delay(&self, pool: Pubkey, start_tick_index: i32) -> Option<Duration> {
        failure_backoff_pending(
            &self.tick_failures,
            &(pool, start_tick_index),
            Instant::now(),
        )
    }

    fn record_tick_failure(&self, pool: Pubkey, start_tick_index: i32) -> u32 {
        failure_record(
            &self.tick_failures,
            (pool, start_tick_index),
            Instant::now(),
            self.tick_backoff,
            self.max_tick_failures,
        )
    }

    fn clear_tick_failure(&self, pool: Pubkey, start_tick_index: i32) {
        failure_clear(&self.tick_failures, &(pool, start_tick_index));
    }

    fn readiness_blockers(&self) -> Vec<ReadinessBlocker> {
        let now = Instant::now();
        let mut blockers = Vec::new();
        for entry in self.pool_failures.iter() {
            let status = entry.value();
            if let Some(delay) = status.next_retry_at.checked_duration_since(now) {
                blockers.push(ReadinessBlocker {
                    adapter: "whirlpool",
                    pool: *entry.key(),
                    kind: ReadinessBlockerKind::PoolMeta,
                    tick_start: None,
                    failures: status.failures,
                    retry_in: delay,
                });
            }
        }
        for entry in self.tick_failures.iter() {
            let status = entry.value();
            if let Some(delay) = status.next_retry_at.checked_duration_since(now) {
                blockers.push(ReadinessBlocker {
                    adapter: "whirlpool",
                    pool: entry.key().0,
                    kind: ReadinessBlockerKind::TickArray,
                    tick_start: Some(entry.key().1),
                    failures: status.failures,
                    retry_in: delay,
                });
            }
        }
        blockers
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
        let primary_address = derive_tick_array_address(&pool, start_tick_index, &self.program_id);
        let (primary_result, primary_error) =
            self.fetch_tick_array_account_attempt(pool, Some(start_tick_index), primary_address);
        if let Some(array) = primary_result {
            self.clear_tick_failure(pool, start_tick_index);
            return Some(array);
        }

        // Respect existing backoff windows before probing alternate seeds.
        if primary_error.is_none() {
            return None;
        }

        let legacy_address =
            derive_legacy_tick_array_address(&pool, start_tick_index, &self.program_id);
        if legacy_address == primary_address {
            // Should never happen, but fall back to the standard logging path.
            return self.fetch_tick_array_account(pool, Some(start_tick_index), primary_address);
        }

        let (legacy_result, legacy_error) =
            self.fetch_tick_array_account_attempt(pool, Some(start_tick_index), legacy_address);
        if let Some(array) = legacy_result {
            self.clear_tick_failure(pool, start_tick_index);
            return Some(array);
        }

        if legacy_error.is_none() {
            return None;
        }

        let error = legacy_error.or(primary_error);
        if let Some(err) = error {
            let failures = self.record_tick_failure(pool, start_tick_index);
            self.log_tick_fetch_error(pool, start_tick_index, &legacy_address, &err, failures);
        }
        None
    }

    fn fetch_tick_array_by_address(
        &self,
        pool: Pubkey,
        address: Pubkey,
    ) -> Option<HydratedTickArray> {
        self.fetch_tick_array_account(pool, None, address)
    }

    fn fetch_tick_array_account_attempt(
        &self,
        pool: Pubkey,
        expected_start: Option<i32>,
        address: Pubkey,
    ) -> (Option<HydratedTickArray>, Option<TickArrayFetchError>) {
        if let Some(start) = expected_start {
            if let Some(wait) = self.tick_backoff_delay(pool, start) {
                trace!(
                    target = "adapter::whirlpool",
                    pool = %pool,
                    start = start,
                    wait_ms = wait.as_millis(),
                    "tick array fetch backoff active"
                );
                return (None, None);
            }
        }

        let mut last_error: Option<TickArrayFetchError> = None;
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
                        return (Some(array), None);
                    } else {
                        last_error = Some(TickArrayFetchError::UnexpectedLayout);
                        break;
                    }
                }
                Err(error) => {
                    last_error = Some(classify_tick_array_error(&error));
                }
            }
        }

        (None, last_error)
    }

    fn fetch_tick_array_account(
        &self,
        pool: Pubkey,
        expected_start: Option<i32>,
        address: Pubkey,
    ) -> Option<HydratedTickArray> {
        let (result, error) = self.fetch_tick_array_account_attempt(pool, expected_start, address);
        if let Some(array) = result {
            if let Some(expected) = expected_start {
                self.clear_tick_failure(pool, expected);
            }
            return Some(array);
        }

        let Some(error) = error else {
            return None;
        };

        if let Some(start) = expected_start {
            let failures = self.record_tick_failure(pool, start);
            self.log_tick_fetch_error(pool, start, &address, &error, failures);
        } else {
            warn!(
                target = "adapter::whirlpool",
                pool = %pool,
                %address,
                error = %error,
                "failed to fetch whirlpool tick array"
            );
        }

        None
    }

    fn log_tick_fetch_error(
        &self,
        pool: Pubkey,
        start_tick_index: i32,
        address: &Pubkey,
        error: &TickArrayFetchError,
        failures: u32,
    ) {
        if failures == 1 {
            warn!(
                target = "adapter::whirlpool",
                pool = %pool,
                start = start_tick_index,
                %address,
                error = %error,
                "failed to fetch whirlpool tick array"
            );
        } else {
            debug!(
                target = "adapter::whirlpool",
                pool = %pool,
                start = start_tick_index,
                %address,
                error = %error,
                failures,
                "tick array fetch still failing"
            );
        }
    }
}

#[derive(Debug)]
enum TickArrayFetchError {
    AccountNotFound,
    UnexpectedLayout,
    Rpc(String),
}

impl fmt::Display for TickArrayFetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TickArrayFetchError::AccountNotFound => write!(f, "account not found"),
            TickArrayFetchError::UnexpectedLayout => write!(f, "unexpected account layout"),
            TickArrayFetchError::Rpc(msg) => write!(f, "{msg}"),
        }
    }
}

#[derive(Debug)]
enum FetchError {
    AccountNotFound,
    InvalidLayout(&'static str),
    Rpc(String),
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FetchError::AccountNotFound => write!(f, "account not found"),
            FetchError::InvalidLayout(entity) => write!(f, "unexpected {entity} layout"),
            FetchError::Rpc(msg) => write!(f, "{msg}"),
        }
    }
}

fn classify_tick_array_error(error: &ClientError) -> TickArrayFetchError {
    if is_account_not_found(error) {
        TickArrayFetchError::AccountNotFound
    } else {
        TickArrayFetchError::Rpc(error.to_string())
    }
}

fn fetch_whirlpool_meta(
    program_id: Pubkey,
    clients: &[Arc<RpcClient>],
    pool: Pubkey,
) -> Result<HydratedPoolMeta, FetchError> {
    let mut last_error: Option<FetchError> = None;
    for client in clients {
        match client.get_account(&pool) {
            Ok(account) => {
                if let Some(meta) = parse_whirlpool_account(pool, &program_id, &account.data) {
                    return Ok(meta);
                } else {
                    return Err(FetchError::InvalidLayout("whirlpool pool account"));
                }
            }
            Err(error) => {
                if is_account_not_found(&error) {
                    last_error = Some(FetchError::AccountNotFound);
                } else {
                    last_error = Some(FetchError::Rpc(error.to_string()));
                }
            }
        }
    }

    Err(last_error.unwrap_or(FetchError::AccountNotFound))
}

fn is_account_not_found(error: &ClientError) -> bool {
    error.to_string().contains("AccountNotFound")
}

fn log_pool_fetch_error(pool: Pubkey, error: &FetchError, failures: u32) {
    if failures == 1 {
        warn!(
            target = "adapter::whirlpool",
            pool = %pool,
            error = %error,
            "failed to hydrate whirlpool pool account"
        );
    } else {
        debug!(
            target = "adapter::whirlpool",
            pool = %pool,
            error = %error,
            failures,
            "whirlpool pool hydration still failing"
        );
    }
}

struct WhirlpoolTickArrayCursor<'a> {
    hydrator: &'a WhirlpoolPoolHydrator,
    pool: Pubkey,
    tick_spacing: i32,
    cache: HashMap<i32, HydratedTickArray>,
    used_arrays: Vec<Pubkey>,
    seen_arrays: HashSet<Pubkey>,
    missing_arrays: bool,
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
            used_arrays: Vec::new(),
            seen_arrays: HashSet::new(),
            missing_arrays: false,
        }
    }

    fn get_array(&mut self, start: i32) -> Option<HydratedTickArray> {
        if let Some(entry) = self.cache.get(&start).cloned() {
            self.record_array(entry.address);
            return Some(entry);
        }
        if let Some(entry) = self.hydrator.hydrate_tick_array(self.pool, start) {
            self.record_array(entry.address);
            self.cache.insert(start, entry.clone());
            self.hydrator
                .hydrate_tick_array_window(self.pool, self.tick_spacing, start, 1);
            return Some(entry);
        }
        self.missing_arrays = true;
        None
    }

    fn record_array(&mut self, address: Pubkey) {
        if self.seen_arrays.insert(address) {
            self.used_arrays.push(address);
        }
    }

    fn missing_coverage(&self) -> bool {
        self.missing_arrays
    }

    fn used_arrays(&self) -> Vec<Pubkey> {
        self.used_arrays.clone()
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
const DYNAMIC_TICK_ARRAY_DISCRIMINATOR: [u8; ANCHOR_DISCRIMINATOR_LEN] =
    [17, 216, 246, 142, 225, 199, 218, 56];
const MIN_LEGACY_TICK_ARRAY_ACCOUNT_LEN: usize = ANCHOR_DISCRIMINATOR_LEN
    + 4
    + (WHIRLPOOL_TICK_SERIALIZED_LEN * WHIRLPOOL_TICK_ARRAY_TICK_COUNT)
    + PUBKEY_BYTES;
const MIN_DYNAMIC_TICK_ARRAY_ACCOUNT_LEN_WITHOUT_DISC: usize = 4
    + PUBKEY_BYTES
    + 16 // bitmap
    + WHIRLPOOL_TICK_ARRAY_TICK_COUNT; // minimum discriminants
const MIN_DYNAMIC_TICK_ARRAY_ACCOUNT_LEN_WITH_DISC: usize =
    MIN_DYNAMIC_TICK_ARRAY_ACCOUNT_LEN_WITHOUT_DISC + ANCHOR_DISCRIMINATOR_LEN;
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
    if data.len() < MIN_DYNAMIC_TICK_ARRAY_ACCOUNT_LEN_WITHOUT_DISC {
        return None;
    }

    if let Some(dynamic) = parse_dynamic_tick_array(address, expected_pool, data) {
        return Some(dynamic);
    }
    parse_legacy_tick_array(address, expected_pool, data)
}

fn parse_dynamic_tick_array(
    address: Pubkey,
    expected_pool: Pubkey,
    data: &[u8],
) -> Option<HydratedTickArray> {
    let has_discriminator = data.len() >= MIN_DYNAMIC_TICK_ARRAY_ACCOUNT_LEN_WITH_DISC
        && data
            .get(..ANCHOR_DISCRIMINATOR_LEN)
            .map(|slice| slice == DYNAMIC_TICK_ARRAY_DISCRIMINATOR)
            .unwrap_or(false);

    if has_discriminator {
        if let Some(array) = parse_dynamic_tick_array_with_offset(
            address,
            expected_pool,
            data,
            ANCHOR_DISCRIMINATOR_LEN,
        ) {
            return Some(array);
        }
    }

    parse_dynamic_tick_array_with_offset(address, expected_pool, data, 0)
}

fn parse_dynamic_tick_array_with_offset(
    address: Pubkey,
    expected_pool: Pubkey,
    data: &[u8],
    offset: usize,
) -> Option<HydratedTickArray> {
    if data.len() < offset + MIN_DYNAMIC_TICK_ARRAY_ACCOUNT_LEN_WITHOUT_DISC {
        return None;
    }

    // Dynamic Whirlpool tick arrays (introduced in recent program releases) place the
    // whirlpool pubkey immediately after the optional anchor discriminator + start index,
    // followed by a bitmap and a sequence of Borsh enums indicating whether each slot is
    // initialized. We detect the layout by validating the inline whirlpool pubkey.
    let mut cursor = offset;
    let start_tick_index = read_i32(&mut cursor, data)?;
    let whirlpool = read_pubkey_bytes(&mut cursor, data)?;
    if whirlpool != expected_pool {
        return None;
    }

    // Tick bitmap currently unused but advance cursor to keep decoding aligned.
    let _bitmap = read_u128(&mut cursor, data)?;
    let mut ticks = Vec::with_capacity(WHIRLPOOL_TICK_ARRAY_TICK_COUNT);
    for _ in 0..WHIRLPOOL_TICK_ARRAY_TICK_COUNT {
        let variant = read_u8(&mut cursor, data)?;
        match variant {
            0 => ticks.push(TickEntry {
                initialized: false,
                liquidity_net: 0,
                liquidity_gross: 0,
            }),
            1 => {
                let liquidity_net = read_i128(&mut cursor, data)?;
                let liquidity_gross = read_u128(&mut cursor, data)?;
                skip_bytes(&mut cursor, 16, data)?; // fee growth outside A
                skip_bytes(&mut cursor, 16, data)?; // fee growth outside B
                skip_bytes(&mut cursor, 16 * 3, data)?; // reward growth outside entries
                ticks.push(TickEntry {
                    initialized: liquidity_gross > 0,
                    liquidity_net,
                    liquidity_gross,
                });
            }
            _ => return None,
        }
    }

    Some(HydratedTickArray {
        address,
        start_tick_index,
        ticks,
    })
}

fn parse_legacy_tick_array(
    address: Pubkey,
    expected_pool: Pubkey,
    data: &[u8],
) -> Option<HydratedTickArray> {
    if data.len() < MIN_LEGACY_TICK_ARRAY_ACCOUNT_LEN {
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
        skip_bytes(&mut cursor, 16 * 3, data)?; // reward growth outside entries
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
        address,
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

fn read_u8(cursor: &mut usize, data: &[u8]) -> Option<u8> {
    let byte = *data.get(*cursor)?;
    let next = (*cursor).checked_add(1)?;
    *cursor = next;
    Some(byte)
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
    // Orca switched tick array seeds to use the decimal string form of the start tick index.
    // This aligns with the Anchor instruction seeds seen in initialize_(dynamic_)tick_array.
    let start_seed = start_tick_index.to_string();
    Pubkey::find_program_address(
        &[TICK_ARRAY_SEED, pool.as_ref(), start_seed.as_bytes()],
        program_id,
    )
    .0
}

fn derive_legacy_tick_array_address(
    pool: &Pubkey,
    start_tick_index: i32,
    program_id: &Pubkey,
) -> Pubkey {
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
    pub reserve_a: Option<u128>,
    pub reserve_b: Option<u128>,
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

    pub fn reserve_for_mint(&self, mint: Pubkey) -> Option<u128> {
        if mint == self.token_mint_a {
            self.reserve_a
        } else if mint == self.token_mint_b {
            self.reserve_b
        } else {
            None
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimulationError {
    MissingState,
    MissingTickArrays,
    InsufficientLiquidity,
}

impl From<SimulationError> for QuoteStatus {
    fn from(value: SimulationError) -> Self {
        match value {
            SimulationError::MissingState => QuoteStatus::MissingState,
            SimulationError::MissingTickArrays => QuoteStatus::MissingTickArrays,
            SimulationError::InsufficientLiquidity => QuoteStatus::InsufficientLiquidity,
        }
    }
}

#[derive(Debug, Clone)]
struct SlippageSimulation {
    amount_out: u64,
    consumed_input: u64,
    edge_bps: f64,
    sqrt_price_before: Option<u128>,
    sqrt_price_after: u128,
    tick_arrays: Vec<Pubkey>,
}

fn simulate_constant_liquidity_exact_in(
    state: &WhirlpoolState,
    req: &QuoteRequest,
) -> Result<SlippageSimulation, SimulationError> {
    let liquidity = state.liquidity.ok_or(SimulationError::MissingState)?;
    let sqrt_price_x64 = state.sqrt_price_x64.ok_or(SimulationError::MissingState)?;
    let fee_rate = state.fee_rate.ok_or(SimulationError::MissingState)?;
    if liquidity == 0 || req.amount == 0 {
        return Err(SimulationError::InsufficientLiquidity);
    }

    let direction = resolve_direction(state, req).ok_or(SimulationError::MissingState)?;
    let zero_for_one = matches!(direction, TradeDirection::AToB);
    let fee_bps = fee_bps_from_hundredth_bip(fee_rate);

    let sqrt_price_current = math::to_x96(sqrt_price_x64);
    let sqrt_price_limit = if zero_for_one {
        math::sqrt_price_at_tick_x96(math::MIN_TICK).ok_or(SimulationError::MissingState)?
    } else {
        math::sqrt_price_at_tick_x96(math::MAX_TICK).ok_or(SimulationError::MissingState)?
    };

    let step = math::compute_swap_step(
        sqrt_price_current,
        sqrt_price_limit,
        liquidity,
        req.amount as u128,
        fee_bps,
        zero_for_one,
    )
    .ok_or(SimulationError::MissingState)?;

    let amount_out = step.amount_out.min(u64::MAX as u128) as u64;
    if amount_out == 0 {
        return Err(SimulationError::InsufficientLiquidity);
    }

    let actual_amount_in = step
        .amount_in
        .saturating_add(step.fee_amount)
        .min(req.amount as u128) as u64;
    if actual_amount_in == 0 {
        return Err(SimulationError::InsufficientLiquidity);
    }

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

    Ok(SlippageSimulation {
        amount_out,
        consumed_input: actual_amount_in,
        edge_bps,
        sqrt_price_before: Some(sqrt_price_x64),
        sqrt_price_after,
        tick_arrays: Vec::new(),
    })
}

fn simulate_tick_arrays_exact_in(
    hydrator: &WhirlpoolPoolHydrator,
    state: &WhirlpoolState,
    req: &QuoteRequest,
) -> Result<SlippageSimulation, SimulationError> {
    let tick_spacing = state.tick_spacing.ok_or(SimulationError::MissingState)?;
    let mut current_tick = state
        .tick_current_index
        .ok_or(SimulationError::MissingState)?;
    let mut liquidity = state.liquidity.ok_or(SimulationError::MissingState)?;
    let sqrt_price_x64 = state.sqrt_price_x64.ok_or(SimulationError::MissingState)?;
    let fee_rate = state.fee_rate.ok_or(SimulationError::MissingState)?;
    if liquidity == 0 || req.amount == 0 {
        return Err(SimulationError::InsufficientLiquidity);
    }

    let direction = resolve_direction(state, req).ok_or(SimulationError::MissingState)?;
    let zero_for_one = matches!(direction, TradeDirection::AToB);
    let fee_bps = fee_bps_from_hundredth_bip(fee_rate);

    let mut sqrt_price_x96 = math::to_x96(sqrt_price_x64);
    let sqrt_limit_tick = if zero_for_one {
        math::MIN_TICK
    } else {
        math::MAX_TICK
    };
    let sqrt_price_limit =
        math::sqrt_price_at_tick_x96(sqrt_limit_tick).ok_or(SimulationError::MissingState)?;

    let anchor_start =
        active_tick_array_start(tick_spacing, current_tick).ok_or(SimulationError::MissingState)?;
    let mut cursor =
        WhirlpoolTickArrayCursor::new(hydrator, state.whirlpool, tick_spacing, Some(anchor_start));

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
            let tick_price = math::sqrt_price_at_tick_x96(next_tick.index)
                .ok_or(SimulationError::MissingState)?;
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
        )
        .ok_or(SimulationError::MissingState)?;

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
                liquidity = apply_liquidity_delta(liquidity, init_tick.liquidity_net, zero_for_one)
                    .ok_or(SimulationError::MissingState)?;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    if amount_out_accum == 0 {
        return Err(if cursor.missing_coverage() {
            SimulationError::MissingTickArrays
        } else {
            SimulationError::InsufficientLiquidity
        });
    }

    let consumed_input = (req.amount as u128).saturating_sub(amount_remaining);
    if consumed_input == 0 {
        return Err(SimulationError::InsufficientLiquidity);
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

    let tick_arrays = cursor.used_arrays();

    Ok(SlippageSimulation {
        amount_out,
        consumed_input: actual_amount_in,
        edge_bps,
        sqrt_price_before: state.sqrt_price_x64,
        sqrt_price_after: sqrt_price_x64_from_x96(&sqrt_price_x96),
        tick_arrays,
    })
}

fn simulate_best_exact_in(
    hydrator: Option<&WhirlpoolPoolHydrator>,
    state: &WhirlpoolState,
    req: &QuoteRequest,
    amount: u64,
) -> Result<SlippageSimulation, SimulationError> {
    let mut scoped_req = req.clone();
    scoped_req.amount = amount;
    scoped_req.direction = SwapDirection::ExactIn;
    if let Some(h) = hydrator {
        match simulate_tick_arrays_exact_in(h, state, &scoped_req) {
            Ok(simulation) => return Ok(simulation),
            Err(SimulationError::MissingTickArrays) => {}
            Err(err) => return Err(err),
        }
    }
    simulate_constant_liquidity_exact_in(state, &scoped_req)
}

fn simulate_authoritative_exact_in(
    hydrator: Option<&WhirlpoolPoolHydrator>,
    state: &WhirlpoolState,
    req: &QuoteRequest,
    amount: u64,
) -> Result<SlippageSimulation, SimulationError> {
    let h = hydrator.ok_or(SimulationError::MissingState)?;
    let mut scoped_req = req.clone();
    scoped_req.amount = amount;
    scoped_req.direction = SwapDirection::ExactIn;
    simulate_tick_arrays_exact_in(h, state, &scoped_req)
}

fn find_input_for_exact_out(
    hydrator: Option<&WhirlpoolPoolHydrator>,
    state: &WhirlpoolState,
    req: &QuoteRequest,
    target_out: u64,
) -> Result<(u64, SlippageSimulation), SimulationError> {
    if target_out == 0 {
        return Err(SimulationError::InsufficientLiquidity);
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

    let (mut best_input, mut best_sim) = best.ok_or(SimulationError::InsufficientLiquidity)?;

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

    Ok((best_input, best_sim))
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

fn snapshot_probe_amount(reserve: u128, utilization_bps: u64) -> Option<u64> {
    if reserve == 0 {
        return None;
    }

    let capped = clamp_reserve_to_utilization(reserve, utilization_bps);
    if capped == 0 {
        return None;
    }

    let mut probe = capped / SNAPSHOT_PROBE_DIVISOR;
    if probe == 0 {
        probe = SNAPSHOT_MIN_PROBE_INPUT.min(capped).max(1u128);
    }

    probe = probe.min(capped);
    probe = probe.clamp(1u128, SNAPSHOT_MAX_PROBE_INPUT);
    Some(probe as u64)
}

fn clamp_amount_to_reserve(amount: u64, reserve: u128, utilization_bps: u64) -> u64 {
    if amount == 0 {
        return 0;
    }
    let capped = clamp_reserve_to_utilization(reserve, utilization_bps);
    if capped == 0 {
        return 0;
    }
    amount.min(capped.min(u64::MAX as u128) as u64)
}

fn clamp_reserve_to_utilization(reserve: u128, utilization_bps: u64) -> u128 {
    if utilization_bps >= BPS_DENOMINATOR {
        return reserve;
    }
    reserve
        .saturating_mul(utilization_bps as u128)
        .checked_div(BPS_DENOMINATOR as u128)
        .unwrap_or(0)
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
    use crate::hydration::adapters::{QuoteRequest, SwapDirection};
    use crate::hydration::types::{PoolSnapshot, PoolUpdateExtensions, SnapshotFreshness};
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
            reserve_a: None,
            reserve_b: None,
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
            reserve_a: None,
            reserve_b: None,
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

        assert!(simulate_constant_liquidity_exact_in(&state, &req).is_err());
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
            reserve_a: None,
            reserve_b: None,
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
            reserve_a: None,
            reserve_b: None,
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
            address: Pubkey::new_unique(),
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
            registered_templates: DashMap::new(),
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
            reserve_a: Some(2_000_000u128),
            reserve_b: Some(3_000_000u128),
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
            block_hash: None,
            block_time_unix: None,
            base_mint: base_mint.to_string(),
            quote_mint: quote_mint.to_string(),
            base_post_amount: 2_000_000u128,
            quote_post_amount: 3_000_000u128,
            updated_at: Instant::now(),
            freshness: SnapshotFreshness::LiveCorecast,
            allow_reserve_backfill: true,
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
