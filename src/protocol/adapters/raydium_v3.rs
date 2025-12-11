use std::{cmp::Ordering, collections::HashMap, str::FromStr, sync::Arc, time::Instant};

use dashmap::DashMap;
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};
use tracing::{debug, trace, warn};
use uint::construct_uint;

use crate::bootstrap::{
    HydratedTickArray, InitializedTick, RaydiumStaticSnapshot, StaticPoolRegistry,
    TransferFeeConfigSummary,
};
use crate::protocol::token_fees::{apply_post_fee_amount, apply_pre_fee_amount, TokenTransferFee};
use crate::protocol::transfer_hooks::{
    TransferHookAccountContext,
    TransferHookAccountsResolver,
};
use crate::watch::types::{
    PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate, RaydiumTransferFeeConfig,
    RaydiumV3PoolExtras, RAYDIUM_V3_INPUT_UTILIZATION_BPS,
};
use crate::template::{TemplateContext, TransferFeeConfigHint};

use super::{
    build_quote_response, ProtocolAdapter, QuoteRequest, QuoteResponse, QuoteStatus, SwapDirection,
};

const RAYDIUM_V3_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const POOL_VAULT_SEED: &[u8] = b"pool_vault";
const TICK_ARRAY_TICK_COUNT: usize = 60;
const SPL_TOKEN_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const SPL_TOKEN_2022_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBq2FXk7XJ68");
const MEMO_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

#[allow(clippy::assign_op_pattern, clippy::manual_div_ceil)]
mod u256 {
    use super::construct_uint;

    construct_uint! {
        pub struct U256(4);
    }
}
pub use u256::U256;

#[derive(Debug, Clone, Copy)]
struct ClmmSwapSimulation {
    executed_input_amount: u64,
    net_output_amount: u64,
    sqrt_price_after: u128,
    protocol_fee_amount: u64,
    fund_fee_amount: u64,
}

const REQUIRED_TICK_ARRAYS_PER_SIDE: usize = 3;

#[derive(Clone)]
struct SnapshotStatusLogEntry {
    status: QuoteStatus,
    logged_at: Instant,
}

impl From<RaydiumTransferFeeConfig> for TransferFeeConfigSummary {
    fn from(value: RaydiumTransferFeeConfig) -> Self {
        Self {
            epoch: value.epoch,
            maximum_fee: value.maximum_fee,
            basis_points: value.basis_points,
        }
    }
}

impl From<TransferFeeConfigSummary> for TransferFeeConfigHint {
    fn from(value: TransferFeeConfigSummary) -> Self {
        Self {
            epoch: value.epoch,
            maximum_fee: value.maximum_fee,
            basis_points: value.basis_points,
        }
    }
}

impl From<TransferFeeConfigSummary> for RaydiumTransferFeeConfig {
    fn from(value: TransferFeeConfigSummary) -> Self {
        Self {
            epoch: value.epoch,
            maximum_fee: value.maximum_fee,
            basis_points: value.basis_points,
        }
    }
}

const SNAPSHOT_FAILURE_LOG_INTERVAL_MS: u64 = 15_000;
const STATE_MISS_LOG_INTERVAL_MS: u64 = 15_000;

#[derive(Debug, Clone)]
pub struct RaydiumV3AdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
    pub max_swap_utilization_bps: u64,
}

impl Default for RaydiumV3AdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 15_000,
            max_swap_utilization_bps: RAYDIUM_V3_INPUT_UTILIZATION_BPS,
        }
    }
}

pub struct RaydiumV3Adapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, RaydiumV3State>,
    config: RaydiumV3AdapterConfig,
    registry: Arc<StaticPoolRegistry>,
    transfer_hook_resolver: Option<TransferHookAccountsResolver>,
    snapshot_failure_log: DashMap<(Pubkey, bool), SnapshotStatusLogEntry>,
    missing_state_log: DashMap<Pubkey, Instant>,
}

impl RaydiumV3Adapter {
    pub fn new(
        config: RaydiumV3AdapterConfig,
        registry: Arc<StaticPoolRegistry>,
    ) -> Self {
        let program_id = Pubkey::from_str(RAYDIUM_V3_PROGRAM_ID)
            .expect("static Raydium AMM v3 program id should be valid");
        let transfer_hook_resolver =
            TransferHookAccountsResolver::new(&config.rpc_endpoints, config.rpc_timeout_ms);
        if transfer_hook_resolver.is_none() && !config.rpc_endpoints.is_empty() {
            warn!(
                target = "adapter::raydium_v3",
                "transfer-hook resolver unavailable despite configured RPC endpoints"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            registry,
            transfer_hook_resolver,
            snapshot_failure_log: DashMap::new(),
            missing_state_log: DashMap::new(),
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<RaydiumV3State> {
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
                    target = "adapter::raydium_v3",
                    pool = %state.pool_id,
                    last_slot = state.last_update_slot,
                    "evicting stale pool state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, mut extras: RaydiumV3PoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::raydium_v3",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let Some(base_mint) = parse_pubkey(&update.base_mint) else {
            warn!(target = "adapter::raydium_v3", pool = %update.pool_id, "invalid base mint");
            return;
        };

        let Some(quote_mint) = parse_pubkey(&update.quote_mint) else {
            warn!(target = "adapter::raydium_v3", pool = %update.pool_id, "invalid quote mint");
            return;
        };
        let snapshot = self.registry.raydium_snapshot(&pool_pubkey);
        if let Some(snapshot_ref) = snapshot.as_ref() {
            Self::merge_snapshot_meta(&mut extras, snapshot_ref);
        }
        let RaydiumV3PoolExtras {
            tick_spacing,
            }
            if base_transfer_hook_program.is_none() {
                base_transfer_hook_program = prev.base_transfer_hook_program;
            }
            if quote_transfer_hook_program.is_none() {
                quote_transfer_hook_program = prev.quote_transfer_hook_program;
            }
            if base_token_program == SPL_TOKEN_PROGRAM_ID {
                base_token_program = prev.base_token_program;
            }
            if quote_token_program == SPL_TOKEN_PROGRAM_ID {
                quote_token_program = prev.quote_token_program;
            }
        }

        if let Some(snapshot_ref) = snapshot.as_ref() {
            if let Some(meta) = snapshot_ref.base_mint_meta {
                base_decimals = base_decimals.or(meta.decimals);
                base_token_program = meta.program;
                base_transfer_fee = base_transfer_fee.or(meta.transfer_fee_config);
            }
            if let Some(meta) = snapshot_ref.quote_mint_meta {
                quote_decimals = quote_decimals.or(meta.decimals);
                quote_token_program = meta.program;
                quote_transfer_fee = quote_transfer_fee.or(meta.transfer_fee_config);
            }
            if base_transfer_hook_program.is_none() {
                base_transfer_hook_program = snapshot_ref.base_transfer_hook_program;
            }
            if quote_transfer_hook_program.is_none() {
                quote_transfer_hook_program = snapshot_ref.quote_transfer_hook_program;
            }
        }

        drop(previous_state);

        let state = RaydiumV3State {
            pool_id: pool_pubkey,
            base_mint,
            quote_mint,
            base_token_program,
            quote_token_program,
            base_decimals,
            quote_decimals,
            amm_config,
            observation_state,
            tick_spacing,
            sqrt_price_x64,
            current_tick,
            liquidity,
            vault_base: update.base_post_amount,
            vault_quote: update.quote_post_amount,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
            corecast_slot: update.slot,
            pool_bump,
            observation_bump,
            vault_bumps,
            trade_fee_bps,
            protocol_fee_bps,
            fund_fee_bps,
            tick_array_bitmap_extension,
            pool_status,
            base_transfer_fee,
            quote_transfer_fee,
            base_transfer_hook_program,
            quote_transfer_hook_program,
            snapshot: snapshot.clone(),
        };

        trace!(
            target = "adapter::raydium_v3",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting Raydium pool state"
        );

        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn merge_snapshot_meta(extras: &mut RaydiumV3PoolExtras, snapshot: &RaydiumStaticSnapshot) {
        let meta = &snapshot.meta;
        extras.tick_spacing = extras.tick_spacing.or(Some(meta.tick_spacing));
        extras.sqrt_price_x64 = extras.sqrt_price_x64.or(Some(meta.sqrt_price_x64));
        extras.liquidity = extras.liquidity.or(Some(meta.liquidity));
        extras.current_tick = extras.current_tick.or(Some(meta.current_tick));
        extras.amm_config = extras.amm_config.or(Some(meta.amm_config));
        extras.observation_state = extras.observation_state.or(Some(meta.observation_state));
        extras.pool_bump = extras.pool_bump.or(Some(meta.pool_bump));
        extras.trade_fee_rate = extras.trade_fee_rate.or(meta.trade_fee_rate);
        extras.protocol_fee_rate = extras.protocol_fee_rate.or(meta.protocol_fee_rate);
        extras.fund_fee_rate = extras.fund_fee_rate.or(meta.fund_fee_rate);
        extras.tick_array_bitmap_extension = extras
            .tick_array_bitmap_extension
            .or(Some(meta.tick_array_bitmap_extension));
        extras.pool_status = extras.pool_status.or(Some(meta.pool_status));

        if extras.base_token_program.is_none() {
            extras.base_token_program = snapshot.base_mint_meta.map(|meta| meta.program);
        }
        if extras.quote_token_program.is_none() {
            extras.quote_token_program = snapshot.quote_mint_meta.map(|meta| meta.program);
        }
        if extras.base_transfer_fee_config.is_none() {
            extras.base_transfer_fee_config = snapshot
                .base_mint_meta
                .and_then(|meta| meta.transfer_fee_config)
                .map(RaydiumTransferFeeConfig::from);
        }
        if extras.quote_transfer_fee_config.is_none() {
            extras.quote_transfer_fee_config = snapshot
                .quote_mint_meta
                .and_then(|meta| meta.transfer_fee_config)
                .map(RaydiumTransferFeeConfig::from);
        }
    }

    fn meta_ready(state: &RaydiumV3State) -> bool {
        state.tick_spacing.is_some()
            && state.sqrt_price_x64.is_some()
            && state.liquidity.is_some()
            && state.current_tick.is_some()
    }

    fn log_state_cache_miss(&self, pool: Pubkey, slot: u64) {
        if let Some(entry) = self.missing_state_log.get(&pool) {
            if (entry.value().elapsed().as_millis() as u64) < STATE_MISS_LOG_INTERVAL_MS {
                return;
            }
        }
        self.missing_state_log.insert(pool, Instant::now());
        warn!(
            target = "adapter::raydium_v3",
            pool = %pool,
            slot,
            "raydium state missing for snapshot quote"
        );
    }

    fn clear_snapshot_issue(&self, pool: Pubkey, base_to_quote: bool) {
        self.snapshot_failure_log.remove(&(pool, base_to_quote));
    }

    fn record_snapshot_issue(
        &self,
        state: &RaydiumV3State,
        slot: u64,
        base_to_quote: bool,
        status: QuoteStatus,
        reason: &'static str,
        probe_amount: Option<u64>,
    ) {
        if status == QuoteStatus::Ok {
            self.clear_snapshot_issue(state.pool_id, base_to_quote);
            return;
        }

        let key = (state.pool_id, base_to_quote);
        let should_log = match self.snapshot_failure_log.get(&key) {
            Some(entry) => {
                let cached = entry.value();
                cached.status != status
                    || cached.logged_at.elapsed().as_millis() as u64
                        >= SNAPSHOT_FAILURE_LOG_INTERVAL_MS
            }
            None => true,
        };

        if !should_log {
            return;
        }

        self.snapshot_failure_log.insert(
            key,
            SnapshotStatusLogEntry {
                status,
                logged_at: Instant::now(),
            },
        );

        let tick_array_count = state
            .snapshot_tick_arrays()
            .map(|arrays| arrays.len())
            .unwrap_or(0);

        warn!(
            target = "adapter::raydium_v3",
            pool = %state.pool_id,
            slot,
            direction = if base_to_quote {
                "base_to_quote"
            } else {
                "quote_to_base"
            },
            status = ?status,
            reason,
            probe_amount,
            last_update_slot = state.last_update_slot,
            base_vault = %state.vault_base,
            quote_vault = %state.vault_quote,
            has_tick_spacing = state.tick_spacing.is_some(),
            has_current_tick = state.current_tick.is_some(),
            has_sqrt_price = state.sqrt_price_x64.is_some(),
            has_liquidity = state.liquidity.is_some(),
            snapshot_tick_arrays = tick_array_count,
            "raydium strict price quote unavailable"
        );
    }

    fn decorate_quote_context(
        &self,
        context: &mut TemplateContext,
        state: &RaydiumV3State,
        input_mint: Pubkey,
        output_mint: Pubkey,
    ) {
        context.requires_memo = false;
        context.requires_dont_front = false;
        if context.raydium_tick_array_bitmap_extension.is_none() {
            context.raydium_tick_array_bitmap_extension = state.tick_array_bitmap_extension;
        }
        context.raydium_pool_status = state.pool_status;
        context.raydium_protocol_fee_bps = state.protocol_fee_bps;
        context.raydium_fund_fee_bps = state.fund_fee_bps;
        context.input_transfer_fee_config = Self::transfer_fee_hint_for_mint(state, input_mint);
        context.output_transfer_fee_config = Self::transfer_fee_hint_for_mint(state, output_mint);
        if context.input_token_program.is_none() {
            context.input_token_program = self.mint_program_for(state, input_mint);
        }
        if context.output_token_program.is_none() {
            context.output_token_program = self.mint_program_for(state, output_mint);
        }

        if let Some(zero_for_one) = classify_swap_direction(state, input_mint, output_mint) {
            if let Some(arrays) = self.resolve_tick_arrays(state, zero_for_one) {
                context.whirlpool_tick_arrays = arrays;
            }
        }

        self.attach_transfer_hook_accounts(state, context, input_mint, output_mint);
    }

    fn attach_transfer_hook_accounts(
        &self,
        state: &RaydiumV3State,
        context: &mut TemplateContext,
        input_mint: Pubkey,
        output_mint: Pubkey,
    ) {
        let Some(resolver) = self.transfer_hook_resolver.as_ref() else {
            return;
        };

        if let Some(accounts) = self.collect_transfer_hook_accounts(
            resolver,
            state,
            context,
            input_mint,
            TransferHookFlow::Input,
        ) {
            context.remaining_accounts.extend(accounts);
        }

        if let Some(accounts) = self.collect_transfer_hook_accounts(
            resolver,
            state,
            context,
            output_mint,
            TransferHookFlow::Output,
        ) {
            context.remaining_accounts.extend(accounts);
        }
    }

    fn collect_transfer_hook_accounts(
        &self,
        resolver: &TransferHookAccountsResolver,
        state: &RaydiumV3State,
        context: &TemplateContext,
        mint: Pubkey,
        flow: TransferHookFlow,
    ) -> Option<Vec<AccountMeta>> {
        let program_id = state.transfer_hook_program_for_mint(mint)?;
        let hook_context = self.transfer_hook_account_context(state, context, mint, flow)?;
        resolver.resolve_accounts(program_id, mint, &[hook_context])
    }

    fn transfer_hook_account_context(
        &self,
        state: &RaydiumV3State,
        context: &TemplateContext,
        mint: Pubkey,
        flow: TransferHookFlow,
    ) -> Option<TransferHookAccountContext> {
        match flow {
            TransferHookFlow::Input => {
                let source = context.input_token_account?;
                let destination = self.pool_vault_address(state, mint)?;
                let authority = context.payer?;
                Some(TransferHookAccountContext {
                    source,
                    destination,
                    authority,
                })
            }
            TransferHookFlow::Output => {
                let source = self.pool_vault_address(state, mint)?;
                let destination = context.output_token_account?;
                Some(TransferHookAccountContext {
                    source,
                    destination,
                    authority: state.pool_id,
                })
            }
        }
    }

    fn pool_vault_address(&self, state: &RaydiumV3State, mint: Pubkey) -> Option<Pubkey> {
        if mint != state.base_mint && mint != state.quote_mint {
            return None;
        }
        Some(derive_pool_vault_address(
            &self.program_id,
            &state.pool_id,
            &mint,
        ))
    }

    fn mint_program_for(&self, state: &RaydiumV3State, mint: Pubkey) -> Option<Pubkey> {
        if mint == state.base_mint {
            Some(state.base_token_program)
        } else if mint == state.quote_mint {
            Some(state.quote_token_program)
        } else {
            None
        }
    }

    fn transfer_fee_hint_for_mint(
        state: &RaydiumV3State,
        mint: Pubkey,
    ) -> Option<TransferFeeConfigHint> {
        Self::transfer_fee_summary_for_mint(state, mint).map(TransferFeeConfigHint::from)
    }

    fn transfer_fee_model_for_mint(
        state: &RaydiumV3State,
        mint: Pubkey,
    ) -> Option<TokenTransferFee> {
        Self::transfer_fee_summary_for_mint(state, mint).map(TokenTransferFee::from)
    }

    fn transfer_fee_summary_for_mint(
        state: &RaydiumV3State,
        mint: Pubkey,
    ) -> Option<TransferFeeConfigSummary> {
        if mint == state.base_mint {
            state.base_transfer_fee
        } else if mint == state.quote_mint {
            state.quote_transfer_fee
        } else {
            None
        }
    }

    fn clamp_request_amount(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Result<u64, QuoteStatus> {
        if req.amount == 0 {
            return Ok(0);
        }
        let reserve = if req.input_mint == state.base_mint {
            state.vault_base
        } else if req.input_mint == state.quote_mint {
            state.vault_quote
        } else {
            return Ok(req.amount);
        };
        if reserve == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }
        let utilization_bps = self.config.max_swap_utilization_bps.min(10_000);
        let capped = reserve.saturating_mul(utilization_bps as u128) / 10_000;
        if capped == 0 {
            return Err(QuoteStatus::PriceImpactExceeded);
        }
        let limit = capped.min(u64::MAX as u128) as u64;
        if limit == 0 {
            return Err(QuoteStatus::PriceImpactExceeded);
        }
        Ok(req.amount.min(limit))
    }

    fn clamp_request_output_amount(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Result<u64, QuoteStatus> {
        if req.amount == 0 {
            return Ok(0);
        }
        let reserve = if req.output_mint == state.base_mint {
            state.vault_base
        } else if req.output_mint == state.quote_mint {
            state.vault_quote
        } else {
            return Ok(0);
        };
        if reserve == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }
        let utilization_bps = self.config.max_swap_utilization_bps.min(10_000);
        let capped = reserve.saturating_mul(utilization_bps as u128) / 10_000;
        if capped == 0 {
            return Err(QuoteStatus::PriceImpactExceeded);
        }
        let limit = capped.min(u64::MAX as u128) as u64;
        if limit == 0 {
            return Err(QuoteStatus::PriceImpactExceeded);
        }
        Ok(req.amount.min(limit))
    }

    fn resolve_tick_arrays(
        &self,
        state: &RaydiumV3State,
        zero_for_one: bool,
    ) -> Option<Vec<Pubkey>> {
        let snapshot = state.snapshot.as_ref()?;
        let tick_spacing = state.tick_spacing?;
        let current_tick = state.current_tick?;
        Self::select_tick_array_accounts(
            snapshot.tick_arrays.as_slice(),
            tick_spacing,
            current_tick,
            zero_for_one,
        )
    }

    fn select_tick_arrays<'a>(
        arrays: &'a [HydratedTickArray],
        tick_spacing: i32,
        current_tick: i32,
        zero_for_one: bool,
    ) -> Option<Vec<&'a HydratedTickArray>> {
        let span = tick_array_span(tick_spacing)?;
        if span == 0 || arrays.is_empty() {
            return None;
        }

        let mut ordered: Vec<&HydratedTickArray> = arrays.iter().collect();
        ordered.sort_by_key(|array| array.start_tick_index);

        let anchor_idx = ordered
            .iter()
            .position(|array| tick_in_span(array.start_tick_index, span, current_tick))
            .or_else(|| {
                if zero_for_one {
                    ordered.len().checked_sub(1)
                } else if !ordered.is_empty() {
                    Some(0)
                } else {
                    None
                }
            })?;

        let mut selected = Vec::with_capacity(REQUIRED_TICK_ARRAYS_PER_SIDE);
        let mut min_slot = u64::MAX;
        Self::push_tick_array_ref(&mut selected, &mut min_slot, ordered[anchor_idx]);

        if zero_for_one {
            for step in 1..=2 {
                if let Some(idx) = anchor_idx.checked_sub(step) {
                    Self::push_tick_array_ref(&mut selected, &mut min_slot, ordered[idx]);
                }
            }
        } else {
            for step in 1..=2 {
                let idx = anchor_idx + step;
                if let Some(array) = ordered.get(idx) {
                    Self::push_tick_array_ref(&mut selected, &mut min_slot, array);
                }
            }
        }

        if selected.len() < REQUIRED_TICK_ARRAYS_PER_SIDE {
            for array in ordered {
                if selected.len() == REQUIRED_TICK_ARRAYS_PER_SIDE {
                    break;
                }
                Self::push_tick_array_ref(&mut selected, &mut min_slot, array);
            }
        }

        if selected.len() < REQUIRED_TICK_ARRAYS_PER_SIDE || min_slot == u64::MAX {
            return None;
        }

        Some(selected)
    }

    fn push_tick_array_ref<'a>(
        selected: &mut Vec<&'a HydratedTickArray>,
        min_slot: &mut u64,
        array: &'a HydratedTickArray,
    ) {
        if !selected
            .iter()
            .any(|existing| existing.address == array.address)
        {
            selected.push(array);
        }
        *min_slot = (*min_slot).min(array.hydrated_slot);
    }

    fn select_tick_array_accounts(
        arrays: &[HydratedTickArray],
        tick_spacing: i32,
        current_tick: i32,
        zero_for_one: bool,
    ) -> Option<Vec<Pubkey>> {
        let selected = Self::select_tick_arrays(arrays, tick_spacing, current_tick, zero_for_one)?;
        Some(selected.into_iter().map(|array| array.address).collect())
    }

    fn collect_initialized_ticks(arrays: &[&HydratedTickArray]) -> Vec<InitializedTick> {
        let mut map = HashMap::new();
        for array in arrays {
            for tick in &array.ticks {
                map.entry(tick.tick).or_insert(InitializedTick {
                    index: tick.tick,
                    liquidity_net: tick.liquidity_net,
                });
            }
        }
        let mut ticks: Vec<InitializedTick> = map.into_values().collect();
        ticks.sort_by_key(|tick| tick.index);
        ticks
    }

    fn next_initialized_tick(
        ticks: &[InitializedTick],
        current_tick: i32,
        zero_for_one: bool,
    ) -> Option<InitializedTick> {
        if zero_for_one {
            ticks
                .iter()
                .rev()
                .find(|tick| tick.index <= current_tick)
                .copied()
        } else {
            ticks.iter().find(|tick| tick.index > current_tick).copied()
        }
    }

    fn simulate_clmm_exact_in(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Result<ClmmSwapSimulation, QuoteStatus> {
        let tick_spacing = state.tick_spacing.ok_or(QuoteStatus::MissingState)?;
        let mut current_tick = state.current_tick.ok_or(QuoteStatus::MissingState)?;
        let sqrt_price_x64 = state.sqrt_price_x64.ok_or(QuoteStatus::MissingState)?;
        let mut liquidity = state.liquidity.ok_or(QuoteStatus::MissingState)?;
        if liquidity == 0 || req.amount == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }

        let Some(zero_for_one) = classify_swap_direction(state, req.input_mint, req.output_mint)
        else {
            return Err(QuoteStatus::MissingState);
        };

        let input_transfer_fee = Self::transfer_fee_model_for_mint(state, req.input_mint);
        let output_transfer_fee = Self::transfer_fee_model_for_mint(state, req.output_mint);
        let (swap_budget, _) = apply_post_fee_amount(req.amount, input_transfer_fee)
            .ok_or(QuoteStatus::TokenFeeUnsupported)?;
        if swap_budget == 0 {
            return Err(QuoteStatus::TokenFeeUnsupported);
        }

        let mut sqrt_price_x96 = math::to_x96(sqrt_price_x64);
        let sqrt_limit_tick = if zero_for_one {
            math::MIN_TICK
        } else {
            math::MAX_TICK
        };
        let sqrt_price_limit_x96 =
            math::sqrt_price_at_tick_x96(sqrt_limit_tick).ok_or(QuoteStatus::MissingState)?;

        let snapshot = state.snapshot.as_ref().ok_or(QuoteStatus::MissingState)?;
        let tick_arrays = Self::select_tick_arrays(
            snapshot.tick_arrays.as_slice(),
            tick_spacing,
            current_tick,
            zero_for_one,
        )
        .ok_or(QuoteStatus::MissingState)?;
        let initialized_ticks = Self::collect_initialized_ticks(&tick_arrays);
        let mut amount_remaining = swap_budget as u128;
        let mut pool_input_consumed = 0u128;
        let mut trade_fee_total = 0u128;
        let mut amount_out_accum = 0u128;
        let mut protocol_fee_total = 0u128;
        let mut fund_fee_total = 0u128;
        let fee_bps = state.trade_fee_bps.unwrap_or(DEFAULT_FEE_BPS);
        let protocol_rate = u128::from(state.protocol_fee_bps.unwrap_or(0));
        let fund_rate = u128::from(state.fund_fee_bps.unwrap_or(0));
        let mut steps = 0usize;

        while amount_remaining > 0 && liquidity > 0 && sqrt_price_x96 != sqrt_price_limit_x96 {
            steps += 1;
            if steps > 512 {
                break;
            }

            let next_tick =
                Self::next_initialized_tick(&initialized_ticks, current_tick, zero_for_one);
            let mut target_price = sqrt_price_limit_x96;
            let mut crossing = None;
            if let Some(tick_info) = next_tick {
                let tick_price = math::sqrt_price_at_tick_x96(tick_info.index)
                    .ok_or(QuoteStatus::MissingState)?;
                if zero_for_one {
                    if tick_price > target_price {
                        target_price = tick_price;
                    }
                } else if tick_price < target_price {
                    target_price = tick_price;
                }
                crossing = Some(tick_info);
            }

            let step = math::compute_swap_step(
                sqrt_price_x96,
                target_price,
                liquidity,
                amount_remaining,
                fee_bps,
                zero_for_one,
            )
            .ok_or(QuoteStatus::MissingState)?;

            if step.amount_in == 0 {
                break;
            }

            sqrt_price_x96 = step.sqrt_price_next;
            let total_spent = step.amount_in.saturating_add(step.fee_amount);
            amount_remaining = amount_remaining.saturating_sub(total_spent);
            pool_input_consumed = pool_input_consumed.saturating_add(step.amount_in);
            trade_fee_total = trade_fee_total.saturating_add(step.fee_amount);
            amount_out_accum = amount_out_accum.saturating_add(step.amount_out);

            if protocol_rate > 0 {
                let delta =
                    step.fee_amount.saturating_mul(protocol_rate) / RAYDIUM_FEE_RATE_DENOMINATOR;
                protocol_fee_total = protocol_fee_total.saturating_add(delta);
            }
            if fund_rate > 0 {
                let delta =
                    step.fee_amount.saturating_mul(fund_rate) / RAYDIUM_FEE_RATE_DENOMINATOR;
                fund_fee_total = fund_fee_total.saturating_add(delta);
            }

            if sqrt_price_x96 == target_price {
                if let Some(init_tick) = crossing {
                    current_tick = if zero_for_one {
                        init_tick.index - 1
                    } else {
                        init_tick.index
                    };
                    liquidity =
                        apply_liquidity_delta(liquidity, init_tick.liquidity_net, zero_for_one)
                            .ok_or(QuoteStatus::InsufficientLiquidity)?;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if amount_out_accum == 0 || pool_input_consumed == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }

        let amount_including_fees = pool_input_consumed.saturating_add(trade_fee_total);
        if amount_including_fees == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }
        let amount_including_fees_u64 = amount_including_fees.min(u64::MAX as u128) as u64;
        let (executed_input_amount, _input_fee_amount) =
            apply_pre_fee_amount(amount_including_fees_u64, input_transfer_fee)
                .ok_or(QuoteStatus::TokenFeeUnsupported)?;

        let raw_output = amount_out_accum.min(u64::MAX as u128) as u64;
        let (net_output_amount, _output_fee_amount) =
            apply_post_fee_amount(raw_output, output_transfer_fee)
                .ok_or(QuoteStatus::TokenFeeUnsupported)?;
        if net_output_amount == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }

        let sqrt_price_after = sqrt_price_x64_from_x96(&sqrt_price_x96);
        Ok(ClmmSwapSimulation {
            executed_input_amount,
            net_output_amount,
            sqrt_price_after,
            protocol_fee_amount: protocol_fee_total.min(u64::MAX as u128) as u64,
            fund_fee_amount: fund_fee_total.min(u64::MAX as u128) as u64,
        })
    }

    fn quote_amount_out(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Result<ClmmSwapSimulation, QuoteStatus> {
        self.simulate_clmm_exact_in(state, req)
    }

    fn quote_amount_out_exact_out(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Result<ClmmSwapSimulation, QuoteStatus> {
        if req.amount == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }
        let mut trial_req = QuoteRequest {
            pool_id: req.pool_id,
            input_mint: req.input_mint,
            output_mint: req.output_mint,
            amount: u64::MAX,
            slot: req.slot,
            block_time: req.block_time,
            direction: SwapDirection::ExactIn,
        };

        let max_input = self.clamp_request_amount(state, &trial_req)?;
        if max_input == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }

        trial_req.amount = 0;

        let target = req.amount;
        let mut low = 1u64;
        let mut high = max_input;
        let mut best: Option<ClmmSwapSimulation> = None;
        let mut last_error = QuoteStatus::InsufficientLiquidity;

        while low <= high {
            let mid = low + (high - low) / 2;
            trial_req.amount = mid;
            match self.quote_amount_out(state, &trial_req) {
                Ok(sim) if sim.net_output_amount >= target => {
                    best = Some(sim);
                    if mid == 0 {
                        break;
                    }
                    high = mid.saturating_sub(1);
                }
                Ok(_) => {
                    low = mid.saturating_add(1);
                }
                Err(status) => {
                    last_error = status;
                    if mid == 0 {
                        break;
                    }
                    high = mid.saturating_sub(1);
                }
            }
        }

        let mut simulation = best.ok_or(last_error)?;
        simulation.net_output_amount = target;
        Ok(simulation)
    }

    fn build_snapshot_side(
        &self,
        state: &RaydiumV3State,
        pool_id: Pubkey,
        slot: u64,
        base_to_quote: bool,
        probe_amount: u64,
    ) -> Option<PoolQuoteSide> {
        if probe_amount == 0 {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::InsufficientLiquidity,
                "probe_amount_zero",
                Some(probe_amount),
            );
            return None;
        }

        let fee_bps = state.trade_fee_bps.unwrap_or(DEFAULT_FEE_BPS);
        let template_fee_bps = fee_bps.min(u64::from(u16::MAX)) as u16;
        let (input_mint, output_mint) = if base_to_quote {
            (state.base_mint, state.quote_mint)
        } else {
            (state.quote_mint, state.base_mint)
        };

        if !state.has_snapshot_tick_arrays() {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::MissingState,
                "snapshot_tick_arrays_missing",
                Some(probe_amount),
            );
            return Some(self.unavailable_snapshot_side(
                input_mint,
                output_mint,
                slot,
                fee_bps,
                template_fee_bps,
                QuoteStatus::MissingState,
            ));
        }

        if state.is_disabled() {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::TradingDisabled,
                "pool_disabled",
                Some(probe_amount),
            );
            return Some(self.unavailable_snapshot_side(
                input_mint,
                output_mint,
                slot,
                fee_bps,
                template_fee_bps,
                QuoteStatus::TradingDisabled,
            ));
        }
        if state.is_stale(Some(slot), &self.config) {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::StaleState,
                "state_stale",
                Some(probe_amount),
            );
            return Some(self.unavailable_snapshot_side(
                input_mint,
                output_mint,
                slot,
                fee_bps,
                template_fee_bps,
                QuoteStatus::StaleState,
            ));
        }

        let req = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: probe_amount,
            slot,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let simulation = match self.quote_amount_out(state, &req) {
            Ok(sim) => sim,
            Err(status) => {
                self.record_snapshot_issue(
                    state,
                    slot,
                    base_to_quote,
                    status,
                    "simulation_error",
                    Some(probe_amount),
                );
                return Some(self.unavailable_snapshot_side(
                    input_mint,
                    output_mint,
                    slot,
                    fee_bps,
                    template_fee_bps,
                    status,
                ));
            }
        };
        if simulation.executed_input_amount == 0 {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::InsufficientLiquidity,
                "zero_executed_input",
                Some(probe_amount),
            );
            return None;
        }
        let execution_price =
            simulation.net_output_amount as f64 / simulation.executed_input_amount as f64;

        let reference_price = match state.mid_price() {
            Some(mid) if mid > 0.0 => {
                if base_to_quote {
                    Some(mid)
                } else {
                    Some(1.0 / mid)
                }
            }
            _ => None,
        };

        let slippage = reference_price
            .map(|reference| compute_slippage_bps(reference, execution_price))
            .unwrap_or_default();

        self.record_snapshot_issue(
            state,
            slot,
            base_to_quote,
            QuoteStatus::Ok,
            "ok",
            Some(probe_amount),
        );

        Some(PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional: simulation.executed_input_amount,
            expected_output: simulation.net_output_amount,
            fee_bps,
            template_fee_bps,
            effective_price: execution_price,
            estimated_slippage_bps: slippage,
            sqrt_price_limit: Some(simulation.sqrt_price_after),
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            amount_specified_is_input: true,
            context: {
                let mut context = TemplateContext {
                    compute_unit_limit: Some(RAYDIUM_DEFAULT_CU_LIMIT),
                    compute_unit_price: Some(RAYDIUM_DEFAULT_CU_PRICE),
                    ..Default::default()
                };
                self.decorate_quote_context(&mut context, state, input_mint, output_mint);
                context.expected_fee_amount = Some(
                    simulation
                        .protocol_fee_amount
                        .saturating_add(simulation.fund_fee_amount),
                );
                context
            },
            status: QuoteStatus::Ok,
            sqrt_price_before_x64: None,
            sqrt_price_after_x64: None,
            tick_arrays_used: Vec::new(),
            cache_slot: Some(slot),
        })
    }

    fn unavailable_snapshot_side(
        &self,
        input_mint: Pubkey,
        output_mint: Pubkey,
        slot: u64,
        fee_bps: u64,
        template_fee_bps: u16,
        status: QuoteStatus,
    ) -> PoolQuoteSide {
        PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional: 0,
            expected_output: 0,
            fee_bps,
            template_fee_bps,
            effective_price: 0.0,
            estimated_slippage_bps: 0.0,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            amount_specified_is_input: true,
            context: TemplateContext::default(),
            status,
            sqrt_price_before_x64: None,
            sqrt_price_after_x64: None,
            tick_arrays_used: Vec::new(),
            cache_slot: Some(slot),
        }
    }
}

impl ProtocolAdapter for RaydiumV3Adapter {
    fn name(&self) -> &'static str {
        "raydium_v3"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::raydium_v3",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        self.upsert_state(update, RaydiumV3PoolExtras::default());
    }

    fn refresh_templates(&self, _epoch: u64) {}

    fn refresh_pool_templates(&self, _pool: Pubkey, _epoch: u64) {}

    fn quote_exact_in(&self, mut req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if state.is_disabled() {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::TradingDisabled;
            return Some(response);
        }
        if state.is_stale(Some(req.slot), &self.config) {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::StaleState;
            return Some(response);
        }
        if !state.has_snapshot_tick_arrays() {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::MissingState;
            return Some(response);
        }
        let clamped_input = match self.clamp_request_amount(&state, &req) {
            Ok(value) => value,
            Err(status) => {
                let mut response = build_quote_response(req, 0);
                response.status = status;
                return Some(response);
            }
        };
        if clamped_input == 0 {
            return None;
        }
        req.amount = clamped_input;
        let simulation = match self.quote_amount_out(&state, &req) {
            Ok(sim) => sim,
            Err(status) => {
                let mut response = build_quote_response(req.clone(), 0);
                response.status = status;
                return Some(response);
            }
        };
        let mut response = build_quote_response(req, simulation.net_output_amount);
        response.amount_in = simulation.executed_input_amount;
        response.sqrt_price_limit = Some(simulation.sqrt_price_after);
        response.min_output_amount = Some(simulation.net_output_amount);
        response.context.compute_unit_limit = Some(RAYDIUM_DEFAULT_CU_LIMIT);
        response.context.compute_unit_price = Some(RAYDIUM_DEFAULT_CU_PRICE);
        response.context.expected_fee_amount = Some(
            simulation
                .protocol_fee_amount
                .saturating_add(simulation.fund_fee_amount),
        );
        self.decorate_quote_context(
            &mut response.context,
            &state,
            response.request.input_mint,
            response.request.output_mint,
        );
        Some(response)
    }

    fn quote_exact_out(&self, mut req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactOut {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if state.is_disabled() {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::TradingDisabled;
            return Some(response);
        }
        if state.is_stale(Some(req.slot), &self.config) {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::StaleState;
            return Some(response);
        }
        if !state.has_snapshot_tick_arrays() {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::MissingState;
            return Some(response);
        }

        let clamped_output = match self.clamp_request_output_amount(&state, &req) {
            Ok(value) => value,
            Err(status) => {
                let mut response = build_quote_response(req, 0);
                response.status = status;
                return Some(response);
            }
        };
        if clamped_output == 0 {
            return None;
        }

        req.amount = clamped_output;

        let simulation = match self.quote_amount_out_exact_out(&state, &req) {
            Ok(sim) => sim,
            Err(status) => {
                let mut response = build_quote_response(req.clone(), 0);
                response.status = status;
                return Some(response);
            }
        };
        let requested_output = req.amount;
        let mut response = build_quote_response(req, requested_output);
        response.amount_in = simulation.executed_input_amount;
        response.amount_out = requested_output;
        response.sqrt_price_limit = Some(simulation.sqrt_price_after);
        response.min_output_amount = Some(requested_output);
        response.context.compute_unit_limit = Some(RAYDIUM_DEFAULT_CU_LIMIT);
        response.context.compute_unit_price = Some(RAYDIUM_DEFAULT_CU_PRICE);
        response.context.expected_fee_amount = Some(
            simulation
                .protocol_fee_amount
                .saturating_add(simulation.fund_fee_amount),
        );
        self.decorate_quote_context(
            &mut response.context,
            &state,
            response.request.input_mint,
            response.request.output_mint,
        );
        Some(response)
    }

    fn bidirectional_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
        let state = match self.get_state(&pool_pubkey) {
            Some(state) => state,
            None => {
                self.log_state_cache_miss(pool_pubkey, snapshot.slot);
                return None;
            }
        };

        let base_probe =
            match snapshot_probe_amount(state.vault_base, self.config.max_swap_utilization_bps) {
                Some(value) => value,
                None => {
                    self.record_snapshot_issue(
                        &state,
                        snapshot.slot,
                        true,
                        QuoteStatus::InsufficientLiquidity,
                        "base_reserve_empty",
                        None,
                    );
                    return None;
                }
            };
        let quote_probe =
            match snapshot_probe_amount(state.vault_quote, self.config.max_swap_utilization_bps) {
                Some(value) => value,
                None => {
                    self.record_snapshot_issue(
                        &state,
                        snapshot.slot,
                        false,
                        QuoteStatus::InsufficientLiquidity,
                        "quote_reserve_empty",
                        None,
                    );
                    return None;
                }
            };

        let base_to_quote =
            self.build_snapshot_side(&state, pool_pubkey, snapshot.slot, true, base_probe)?;
        let quote_to_base =
            self.build_snapshot_side(&state, pool_pubkey, snapshot.slot, false, quote_probe)?;

        Some(PoolBidirectionalQuotes {
            base_to_quote,
            quote_to_base,
        })
    }
}

const DEFAULT_FEE_BPS: u64 = 25; // assumed Raydium taker fee (0.25%)
const RAYDIUM_DEFAULT_CU_LIMIT: u32 = 350_000;
const RAYDIUM_DEFAULT_CU_PRICE: u64 = 5_000;
const SNAPSHOT_PROBE_DIVISOR: u128 = 5_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;
const BPS_DENOMINATOR_U128: u128 = 10_000;
const RAYDIUM_FEE_RATE_DENOMINATOR: u128 = 1_000_000;
const RAYDIUM_STATUS_DISABLE_SWAP_BIT: u8 = 1 << 4;

#[derive(Debug, Clone)]
pub struct RaydiumV3State {
    pub pool_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_token_program: Pubkey,
    pub quote_token_program: Pubkey,
    pub base_decimals: Option<u8>,
    pub quote_decimals: Option<u8>,
    pub amm_config: Option<Pubkey>,
    pub observation_state: Option<Pubkey>,
    pub tick_spacing: Option<i32>,
    pub sqrt_price_x64: Option<u128>,
    pub current_tick: Option<i32>,
    pub liquidity: Option<u128>,
    pub vault_base: u128,
    pub vault_quote: u128,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
    pub corecast_slot: u64,
    pub pool_bump: Option<u8>,
    pub observation_bump: Option<u8>,
    pub vault_bumps: Option<[u8; 2]>,
    pub trade_fee_bps: Option<u64>,
    pub protocol_fee_bps: Option<u16>,
    pub fund_fee_bps: Option<u16>,
    pub tick_array_bitmap_extension: Option<Pubkey>,
    pub pool_status: Option<u8>,
    pub base_transfer_fee: Option<TransferFeeConfigSummary>,
    pub quote_transfer_fee: Option<TransferFeeConfigSummary>,
    pub base_transfer_hook_program: Option<Pubkey>,
    pub quote_transfer_hook_program: Option<Pubkey>,
    pub snapshot: Option<Arc<RaydiumStaticSnapshot>>,
}

impl RaydiumV3State {
    pub fn mid_price(&self) -> Option<f64> {
        if self.vault_base == 0 {
            return None;
        }
        Some(self.vault_quote as f64 / self.vault_base as f64)
    }

    pub fn has_snapshot_tick_arrays(&self) -> bool {
        self.snapshot
            .as_ref()
            .map(|snapshot| !snapshot.tick_arrays.is_empty())
            .unwrap_or(false)
    }

    pub fn is_disabled(&self) -> bool {
        self.pool_status
            .map(|status| status & RAYDIUM_STATUS_DISABLE_SWAP_BIT != 0)
            .unwrap_or(false)
    }

    pub fn is_stale(&self, current_slot: Option<u64>, config: &RaydiumV3AdapterConfig) -> bool {
        let slot_fresh = current_slot
            .map(|slot| slot.saturating_sub(self.last_update_slot) <= config.stale_slot_threshold)
            .unwrap_or(true);
        let instant_fresh =
            self.last_update_at.elapsed().as_millis() as u64 <= config.stale_after_ms;
        !(slot_fresh && instant_fresh)
    }

    fn transfer_hook_program_for_mint(&self, mint: Pubkey) -> Option<Pubkey> {
        if mint == self.base_mint {
            self.base_transfer_hook_program
        } else if mint == self.quote_mint {
            self.quote_transfer_hook_program
        } else {
            None
        }
    }

    pub fn snapshot_tick_arrays(&self) -> Option<&[HydratedTickArray]> {
        self.snapshot
            .as_ref()
            .map(|snapshot| snapshot.tick_arrays.as_slice())
    }
}

fn derive_pool_vault_address(program_id: &Pubkey, pool_state: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[POOL_VAULT_SEED, pool_state.as_ref(), mint.as_ref()],
        program_id,
    )
    .0
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
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

fn clamp_reserve_to_utilization(reserve: u128, utilization_bps: u64) -> u128 {
    if utilization_bps >= BPS_DENOMINATOR_U128 as u64 {
        return reserve;
    }
    reserve
        .saturating_mul(utilization_bps as u128)
        .checked_div(BPS_DENOMINATOR_U128)
        .unwrap_or(0)
}

fn sqrt_price_x64_from_x96(value: &U256) -> u128 {
    (value >> 32).low_u128()
}

fn compute_slippage_bps(reference: f64, execution: f64) -> f64 {
    if !reference.is_finite() || reference == 0.0 || !execution.is_finite() {
        return 0.0;
    }
    (((reference - execution) / reference) * 10_000.0).abs()
}

fn tick_array_span(tick_spacing: i32) -> Option<i32> {
    tick_spacing
        .checked_mul(TICK_ARRAY_TICK_COUNT as i32)
        .map(|value| value.abs())
}

fn tick_in_span(start: i32, span: i32, tick: i32) -> bool {
    let start_i64 = start as i64;
    let end_i64 = start_i64 + span as i64;
    let tick_i64 = tick as i64;
    tick_i64 >= start_i64 && tick_i64 < end_i64
}

fn mint_is_less(a: &Pubkey, b: &Pubkey) -> bool {
    a.to_bytes().cmp(&b.to_bytes()) == Ordering::Less
}

fn classify_swap_direction(
    state: &RaydiumV3State,
    input_mint: Pubkey,
    output_mint: Pubkey,
) -> Option<bool> {
    let (token0, token1) = if mint_is_less(&state.base_mint, &state.quote_mint) {
        (state.base_mint, state.quote_mint)
    } else {
        (state.quote_mint, state.base_mint)
    };

    if input_mint == token0 && output_mint == token1 {
        Some(true)
    } else if input_mint == token1 && output_mint == token0 {
        Some(false)
    } else {
        None
    }
}

#[derive(Clone, Copy)]
enum TransferHookFlow {
    Input,
    Output,
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

fn fee_bps_from_hundredth_bip(rate: u32) -> u64 {
    ((rate as u64).saturating_add(50)) / 100
}

fn fee_bps_from_hundredth_bip_u16(rate: u32) -> Option<u16> {
    u16::try_from(fee_bps_from_hundredth_bip(rate)).ok()
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
        let result = div_round_up(
            U256::from(value).checked_mul(U256::from(numerator))?,
            U256::from(denominator),
        );
        result.try_into().ok()
    }

    #[cfg(test)]
    mod tests {
        use super::{sqrt_price_at_tick_x96, U256};

        #[test]
        fn sqrt_price_at_zero_tick_matches_one() {
            let sqrt = sqrt_price_at_tick_x96(0).expect("tick 0");
            assert_eq!(sqrt, U256::one() << 96);
        }
    }
