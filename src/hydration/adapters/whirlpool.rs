use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use dashmap::{mapref::entry::Entry, DashMap};
use solana_client::{client_error::ClientError, rpc_client::RpcClient};
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};
use tracing::{debug, info, trace, warn};
use uint::construct_uint;

use super::hydration::AsyncHydrator;

use crate::builder::{BuilderHandle, WhirlpoolPoolAccounts, WhirlpoolPoolMeta};
use crate::hydration::token_fees::{
    apply_post_fee_amount, apply_pre_fee_amount, TokenTransferFee, TokenTransferFeeHydrator,
};
use crate::hydration::transfer_hooks::{TransferHookAccountContext, TransferHookAccountsResolver};
use crate::hydration::types::{
    HydrationComponent, PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate,
    WhirlpoolAdaptiveFeeConstants, WhirlpoolAdaptiveFeeInfo, WhirlpoolAdaptiveFeeVariables,
    WhirlpoolPoolExtras, WHIRLPOOL_INPUT_UTILIZATION_BPS,
};
use crate::template::{
    TemplateContext, WhirlpoolRemainingAccountsSlice, WhirlpoolRemainingAccountsType,
};

use super::{
    build_quote_response, mul_div_price, quote_from_price, AdapterReadiness, ProtocolAdapter,
    QuoteRequest, QuoteResponse, QuoteStatus, ReadinessBlocker, ReadinessBlockerKind,
    SwapDirection,
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
const SPL_TOKEN_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const MEMO_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

#[allow(clippy::assign_op_pattern, clippy::manual_div_ceil)]
mod u256 {
    use super::construct_uint;

    construct_uint! {
        pub struct U256(4);
    }
}
pub use u256::U256;

const DEFAULT_WHIRLPOOL_HYDRATION_RETRY_MS: u64 = 60_000;
const SNAPSHOT_FAILURE_LOG_INTERVAL_MS: u64 = 15_000;
const STATE_MISS_LOG_INTERVAL_MS: u64 = 15_000;

#[derive(Clone)]
struct SnapshotStatusLogEntry {
    status: QuoteStatus,
    logged_at: Instant,
}

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
    pub prefer_exact_out_quotes: bool,
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
            prefer_exact_out_quotes: false,
        }
    }
}

pub struct WhirlpoolAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, WhirlpoolState>,
    config: WhirlpoolAdapterConfig,
    hydrator: Option<WhirlpoolPoolHydrator>,
    transfer_fee_cache: Option<TokenTransferFeeHydrator>,
    transfer_hook_resolver: Option<TransferHookAccountsResolver>,
    template_builder: Option<BuilderHandle>,
    registered_templates: DashMap<Pubkey, WhirlpoolRegistration>,
    snapshot_failure_log: DashMap<(Pubkey, bool), SnapshotStatusLogEntry>,
    missing_state_log: DashMap<Pubkey, Instant>,
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
        let transfer_fee_cache = TokenTransferFeeHydrator::new(
            &config.rpc_endpoints,
            config.rpc_timeout_ms,
            config.hydration_retry_ms,
        );
        let transfer_hook_resolver =
            TransferHookAccountsResolver::new(&config.rpc_endpoints, config.rpc_timeout_ms);
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
            transfer_fee_cache,
            transfer_hook_resolver,
            template_builder,
            registered_templates: DashMap::new(),
            snapshot_failure_log: DashMap::new(),
            missing_state_log: DashMap::new(),
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

        let mut state = WhirlpoolState {
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
            token_program_a: extras
                .token_program_a
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_program_b: extras
                .token_program_b
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: extras.trade_enable_timestamp,
            adaptive_fee: extras.adaptive_fee,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
        };

        self.attach_transfer_metadata(&mut state);

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
            extras.token_program_a = extras
                .token_program_a
                .or_else(|| Some(meta.token_program_a.to_string()));
            extras.token_program_b = extras
                .token_program_b
                .or_else(|| Some(meta.token_program_b.to_string()));
            extras.tick_spacing = extras.tick_spacing.or(Some(meta.tick_spacing));
            extras.liquidity = extras.liquidity.or(Some(meta.liquidity));
            extras.sqrt_price_x64 = extras.sqrt_price_x64.or(Some(meta.sqrt_price_x64));
            extras.tick_current_index = extras.tick_current_index.or(Some(meta.tick_current_index));
            extras.fee_rate = extras.fee_rate.or(Some(meta.fee_rate));
            extras.protocol_fee_rate = extras.protocol_fee_rate.or(Some(meta.protocol_fee_rate));
            extras.oracle = extras.oracle.or_else(|| Some(meta.oracle.to_string()));
            extras.trade_enable_timestamp = extras
                .trade_enable_timestamp
                .or(meta.trade_enable_timestamp);
            if extras.adaptive_fee.is_none() {
                extras.adaptive_fee = meta.adaptive_fee;
            }
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
            token_program_a: state.token_program_a,
            token_program_b: state.token_program_b,
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

    fn attach_transfer_metadata(&self, state: &mut WhirlpoolState) {
        let Some(cache) = self.transfer_fee_cache.as_ref() else {
            return;
        };
        state.token_a_transfer_fee = cache.hydrate(state.token_mint_a);
        state.token_b_transfer_fee = cache.hydrate(state.token_mint_b);
        state.transfer_hook_program_a = cache.transfer_hook_program(state.token_mint_a);
        state.transfer_hook_program_b = cache.transfer_hook_program(state.token_mint_b);
    }

    fn refreshed_state(&self, state: &WhirlpoolState) -> WhirlpoolState {
        let mut clone = state.clone();
        self.attach_transfer_metadata(&mut clone);
        clone
    }

    fn static_alt_accounts(state: &WhirlpoolState) -> Vec<Pubkey> {
        let mut accounts = Vec::with_capacity(9);
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
        accounts.push(Self::resolve_token_program(state.token_program_a));
        accounts.push(Self::resolve_token_program(state.token_program_b));
        accounts.push(MEMO_PROGRAM_ID);
        Self::dedup_pubkeys(&mut accounts);
        accounts
    }

    fn dedup_pubkeys(accounts: &mut Vec<Pubkey>) {
        let mut seen = HashSet::with_capacity(accounts.len());
        accounts.retain(|pk| seen.insert(*pk));
    }

    fn resolve_token_program(program: Option<Pubkey>) -> Pubkey {
        program.unwrap_or(SPL_TOKEN_PROGRAM_ID)
    }

    fn decorate_template_context(&self, response: &mut QuoteResponse, base_state: &WhirlpoolState) {
        let refreshed_state = self.refreshed_state(base_state);
        let state = &refreshed_state;
        let mut context = self.snapshot_template_context(state);
        let mut preferred_arrays_full = response.context.whirlpool_tick_arrays.clone();
        self.ensure_extended_tick_arrays(state, &response.request, &mut preferred_arrays_full);
        let preferred_arrays = preferred_arrays_full.clone();
        if let Some(arrays) = self.resolve_tick_arrays(state, &response.request, &preferred_arrays)
        {
            context.whirlpool_tick_arrays = arrays;
        } else if context.whirlpool_tick_arrays.is_empty() && !preferred_arrays.is_empty() {
            context.whirlpool_tick_arrays = preferred_arrays.into_iter().take(3).collect();
        }
        self.attach_owner_accounts(
            state,
            &mut context,
            response.request.input_mint,
            response.request.output_mint,
        );
        self.attach_transfer_hook_accounts(state, &mut context, &response.request);
        let supplemental_arrays: Vec<Pubkey> = preferred_arrays_full.into_iter().skip(3).collect();
        self.attach_supplemental_tick_arrays(&mut context, &supplemental_arrays);
        response.context = context;
    }

    fn ensure_extended_tick_arrays(
        &self,
        state: &WhirlpoolState,
        req: &QuoteRequest,
        preferred: &mut Vec<Pubkey>,
    ) {
        const MAX_EXTENDED: usize = 5;
        Self::dedup_pubkeys(preferred);
        if preferred.len() >= MAX_EXTENDED {
            preferred.truncate(MAX_EXTENDED);
            return;
        }
        let Some(direction) = resolve_direction(state, req) else {
            return;
        };
        if let Some((primary, supplemental)) = self.derive_tick_array_window(state, direction) {
            Self::merge_unique_pubkeys(preferred, &primary);
            Self::merge_unique_pubkeys(preferred, &supplemental);
        }
        if preferred.len() > MAX_EXTENDED {
            preferred.truncate(MAX_EXTENDED);
        }
    }

    fn attach_transfer_hook_accounts(
        &self,
        state: &WhirlpoolState,
        context: &mut TemplateContext,
        request: &QuoteRequest,
    ) {
        let Some(resolver) = self.transfer_hook_resolver.as_ref() else {
            return;
        };
        let (input_contexts, input_slice) = self.collect_transfer_hook_accounts(
            resolver,
            state,
            context,
            request.input_mint,
            WhirlpoolRemainingAccountsType::TransferHookInput,
        );
        let (output_contexts, output_slice) = self.collect_transfer_hook_accounts(
            resolver,
            state,
            context,
            request.output_mint,
            WhirlpoolRemainingAccountsType::TransferHookOutput,
        );

        if let Some((accounts, slice)) = input_contexts.zip(input_slice) {
            context.remaining_accounts.extend(accounts);
            context.whirlpool_remaining_accounts.push(slice);
        }
        if let Some((accounts, slice)) = output_contexts.zip(output_slice) {
            context.remaining_accounts.extend(accounts);
            context.whirlpool_remaining_accounts.push(slice);
        }
    }

    fn collect_transfer_hook_accounts(
        &self,
        resolver: &TransferHookAccountsResolver,
        state: &WhirlpoolState,
        context: &TemplateContext,
        mint: Pubkey,
        accounts_type: WhirlpoolRemainingAccountsType,
    ) -> (
        Option<Vec<AccountMeta>>,
        Option<WhirlpoolRemainingAccountsSlice>,
    ) {
        let program_id = match state.transfer_hook_program_for_mint(mint) {
            Some(id) => id,
            None => return (None, None),
        };
        let hook_context =
            match self.transfer_hook_account_context(state, context, mint, accounts_type) {
                Some(ctx) => ctx,
                None => return (None, None),
            };
        self.transfer_hook_contexts(resolver, program_id, mint, hook_context, accounts_type)
    }

    fn transfer_hook_contexts(
        &self,
        resolver: &TransferHookAccountsResolver,
        program_id: Pubkey,
        mint: Pubkey,
        hook_context: TransferHookAccountContext,
        accounts_type: WhirlpoolRemainingAccountsType,
    ) -> (
        Option<Vec<AccountMeta>>,
        Option<WhirlpoolRemainingAccountsSlice>,
    ) {
        let contexts = [hook_context];
        let Some(accounts) = resolver.resolve_accounts(program_id, mint, &contexts) else {
            return (None, None);
        };
        let slice = WhirlpoolRemainingAccountsSlice {
            accounts_type,
            length: accounts.len().min(u8::MAX as usize) as u8,
        };
        (Some(accounts), Some(slice))
    }

    fn transfer_hook_account_context(
        &self,
        state: &WhirlpoolState,
        context: &TemplateContext,
        mint: Pubkey,
        accounts_type: WhirlpoolRemainingAccountsType,
    ) -> Option<TransferHookAccountContext> {
        match accounts_type {
            WhirlpoolRemainingAccountsType::TransferHookInput => {
                let source = context.input_token_account?;
                let destination = state.vault_for_mint(mint)?;
                let authority = context.payer?;
                Some(TransferHookAccountContext {
                    source,
                    destination,
                    authority,
                })
            }
            WhirlpoolRemainingAccountsType::TransferHookOutput => {
                let source = state.vault_for_mint(mint)?;
                let destination = context.output_token_account?;
                Some(TransferHookAccountContext {
                    source,
                    destination,
                    authority: state.whirlpool,
                })
            }
            _ => None,
        }
    }

    fn merge_unique_pubkeys(target: &mut Vec<Pubkey>, source: &[Pubkey]) {
        let mut seen: HashSet<Pubkey> = target.iter().copied().collect();
        for pk in source {
            if seen.insert(*pk) {
                target.push(*pk);
            }
        }
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

    fn attach_supplemental_tick_arrays(
        &self,
        context: &mut TemplateContext,
        supplemental_arrays: &[Pubkey],
    ) {
        const MAX_PER_SLICE: usize = 3;
        const SLICE_TYPES: [WhirlpoolRemainingAccountsType; 3] = [
            WhirlpoolRemainingAccountsType::SupplementalTickArrays,
            WhirlpoolRemainingAccountsType::SupplementalTickArraysOne,
            WhirlpoolRemainingAccountsType::SupplementalTickArraysTwo,
        ];

        if supplemental_arrays.is_empty() {
            return;
        }

        let mut seen = HashSet::with_capacity(
            context.whirlpool_tick_arrays.len() + context.remaining_accounts.len(),
        );
        for pk in &context.whirlpool_tick_arrays {
            seen.insert(*pk);
        }
        for meta in &context.remaining_accounts {
            seen.insert(meta.pubkey);
        }

        let max_accounts = MAX_PER_SLICE * SLICE_TYPES.len();
        let mut extras = Vec::with_capacity(supplemental_arrays.len().min(max_accounts));
        for pk in supplemental_arrays.iter().copied() {
            if pk == Pubkey::default() {
                continue;
            }
            if seen.insert(pk) {
                extras.push(pk);
                if extras.len() == max_accounts {
                    break;
                }
            }
        }

        if extras.is_empty() {
            return;
        }

        context.remaining_accounts.extend(
            extras
                .iter()
                .copied()
                .map(|pk| AccountMeta::new_readonly(pk, false)),
        );

        for (index, chunk) in extras.chunks(MAX_PER_SLICE).enumerate() {
            if index >= SLICE_TYPES.len() {
                break;
            }
            context
                .whirlpool_remaining_accounts
                .push(WhirlpoolRemainingAccountsSlice {
                    accounts_type: SLICE_TYPES[index],
                    length: chunk.len() as u8,
                });
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

    fn schedule_tick_array_windows(&self, state: &WhirlpoolState) {
        let Some(hydrator) = self.hydrator.as_ref() else {
            return;
        };
        let mut seen = HashSet::new();
        let mut hints = Vec::new();
        for direction in [TradeDirection::AToB, TradeDirection::BToA] {
            if let Some((primary, supplemental)) = self.derive_tick_array_window(state, direction)
            {
                for pk in primary.into_iter().chain(supplemental.into_iter()) {
                    if seen.insert(pk) {
                        hints.push(pk);
                    }
                }
            }
        }
        if hints.is_empty() {
            return;
        }
        hydrator.prefetch_tick_arrays(state.whirlpool, hints);
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
        self.derive_tick_array_window(state, direction)
            .map(|(primary, _)| primary)
    }

    fn derive_tick_array_window(
        &self,
        state: &WhirlpoolState,
        direction: TradeDirection,
    ) -> Option<(Vec<Pubkey>, Vec<Pubkey>)> {
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
        let (primary_offsets, supplemental_offsets) = match direction {
            TradeDirection::AToB => ([0, -1, -2], [1, 2]),
            TradeDirection::BToA => ([0, 1, 2], [-1, -2]),
        };

        let mut primary = Vec::with_capacity(primary_offsets.len());
        for offset in primary_offsets {
            let start = Self::compute_tick_array_start(anchor, span, offset)?;
            if start < math::MIN_TICK || start > math::MAX_TICK {
                return None;
            }
            primary.push(derive_tick_array_address(
                &state.whirlpool,
                start,
                &self.program_id,
            ));
        }

        let mut supplemental = Vec::with_capacity(supplemental_offsets.len());
        for offset in supplemental_offsets {
            let Some(start) = Self::compute_tick_array_start(anchor, span, offset) else {
                continue;
            };
            if start < math::MIN_TICK || start > math::MAX_TICK {
                continue;
            }
            supplemental.push(derive_tick_array_address(
                &state.whirlpool,
                start,
                &self.program_id,
            ));
        }

        Some((primary, supplemental))
    }

    fn compute_tick_array_start(anchor: i32, span: i32, offset: i32) -> Option<i32> {
        let step = span.checked_mul(offset)?;
        anchor.checked_add(step)
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

    fn state_stale(&self, state: &WhirlpoolState, request_slot: u64) -> bool {
        let reference_slot = if request_slot == 0 {
            state.last_update_slot
        } else {
            request_slot
        };
        let slot_stale = self.config.stale_slot_threshold > 0
            && reference_slot.saturating_sub(state.last_update_slot)
                > self.config.stale_slot_threshold;
        let time_stale = self.config.stale_after_ms > 0
            && state.last_update_at.elapsed().as_millis() as u64 > self.config.stale_after_ms;
        slot_stale || time_stale
    }

    fn log_state_cache_miss(&self, pool: Pubkey, slot: u64) {
        if let Some(entry) = self.missing_state_log.get(&pool) {
            if (entry.value().elapsed().as_millis() as u64) < STATE_MISS_LOG_INTERVAL_MS {
                return;
            }
        }
        self.missing_state_log.insert(pool, Instant::now());
        warn!(
            target = "adapter::whirlpool",
            pool = %pool,
            slot,
            hydrator_online = self.hydrator.is_some(),
            "whirlpool strict price snapshot requested before hydration"
        );
    }

    fn clear_snapshot_issue(&self, pool: Pubkey, base_to_quote: bool) {
        self.snapshot_failure_log.remove(&(pool, base_to_quote));
    }

    #[allow(clippy::too_many_arguments)]
    fn record_snapshot_issue(
        &self,
        state: &WhirlpoolState,
        slot: u64,
        base_to_quote: bool,
        status: QuoteStatus,
        reason: &'static str,
        probe_amount: Option<u64>,
        amount_specified_is_input: Option<bool>,
        block_time_unix: Option<i64>,
    ) {
        if status == QuoteStatus::Ok {
            self.clear_snapshot_issue(state.whirlpool, base_to_quote);
            return;
        }

        let key = (state.whirlpool, base_to_quote);
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

        let cache_age_ms = state.last_update_at.elapsed().as_millis() as u64;
        let hydrator_clients = self
            .hydrator
            .as_ref()
            .map(|hydrator| hydrator.client_count())
            .unwrap_or(0);
        let normalized_time = normalize_block_time(block_time_unix);
        let trading_enabled = trade_enabled(state, normalized_time);

        warn!(
            target = "adapter::whirlpool",
            pool = %state.whirlpool,
            slot,
            direction = if base_to_quote {
                "base_to_quote"
            } else {
                "quote_to_base"
            },
            status = ?status,
            reason,
            probe_amount,
            amount_specified_is_input,
            block_time_unix,
            last_update_slot = state.last_update_slot,
            cache_age_ms,
            preferred_exact_out = self.config.prefer_exact_out_quotes,
            tick_spacing = ?state.tick_spacing,
            liquidity = ?state.liquidity,
            sqrt_price_x64 = ?state.sqrt_price_x64,
            tick_current_index = ?state.tick_current_index,
            reserve_a = ?state.reserve_a,
            reserve_b = ?state.reserve_b,
            tick_array0 = state.tick_array0.is_some(),
            tick_array1 = state.tick_array1.is_some(),
            tick_array2 = state.tick_array2.is_some(),
            trading_enabled,
            hydrator_clients,
            hydrator_online = self.hydrator.is_some(),
            "whirlpool strict price quote unavailable"
        );
    }

    fn build_snapshot_side(
        &self,
        state: &WhirlpoolState,
        pool_id: Pubkey,
        slot: u64,
        block_time_unix: Option<i64>,
        input_mint: Pubkey,
        output_mint: Pubkey,
        base_to_quote: bool,
        probe_amount: u64,
        amount_specified_is_input: bool,
    ) -> Option<PoolQuoteSide> {
        if probe_amount == 0 {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::InsufficientLiquidity,
                "probe_amount_zero",
                Some(probe_amount),
                Some(amount_specified_is_input),
                block_time_unix,
            );
            return None;
        }

        if input_mint == output_mint {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::MissingState,
                "identical_mints",
                Some(probe_amount),
                Some(amount_specified_is_input),
                block_time_unix,
            );
            return None;
        }

        if self.state_stale(state, slot) {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::StaleState,
                "state_stale",
                Some(probe_amount),
                Some(amount_specified_is_input),
                block_time_unix,
            );
            return None;
        }

        let refreshed_state = self.refreshed_state(state);
        let state = &refreshed_state;

        if !trade_enabled(state, normalize_block_time(block_time_unix)) {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::TradingDisabled,
                "trade_disabled",
                Some(probe_amount),
                Some(amount_specified_is_input),
                block_time_unix,
            );
            return None;
        }

        let mut seen = 0;
        for mint in [state.token_mint_a, state.token_mint_b] {
            if mint == input_mint || mint == output_mint {
                seen += 1;
            }
        }
        if seen < 2 {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::MissingState,
                "mint_mismatch",
                Some(probe_amount),
                Some(amount_specified_is_input),
                block_time_unix,
            );
            return None;
        }

        let req = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: probe_amount,
            slot,
            block_time: block_time_unix,
            direction: if amount_specified_is_input {
                SwapDirection::ExactIn
            } else {
                SwapDirection::ExactOut
            },
        };

        let direction = match resolve_direction(state, &req) {
            Some(direction) => direction,
            None => {
                self.record_snapshot_issue(
                    state,
                    slot,
                    base_to_quote,
                    QuoteStatus::MissingState,
                    "direction_unresolved",
                    Some(probe_amount),
                    Some(amount_specified_is_input),
                    block_time_unix,
                );
                return None;
            }
        };

        let mut preferred_arrays = Vec::new();
        let map_sim_error = |err: SimulationError| -> (QuoteStatus, &'static str) {
            let reason = match err {
                SimulationError::MissingState => "simulation_missing_state",
                SimulationError::MissingTickArrays => "simulation_missing_tick_arrays",
                SimulationError::InsufficientLiquidity => "simulation_insufficient_liquidity",
            };
            (err.into(), reason)
        };

        let simulation_result: Result<(SlippageSimulation, u64), (QuoteStatus, &'static str)> =
            if amount_specified_is_input {
                match simulate_authoritative_exact_in(
                    self.hydrator.as_ref(),
                    state,
                    &req,
                    probe_amount,
                ) {
                    Ok(sim) => Ok((sim, probe_amount)),
                    Err(SimulationError::MissingState | SimulationError::MissingTickArrays) => {
                        match simulate_constant_liquidity_exact_in(state, &req) {
                            Ok(sim) => Ok((sim, probe_amount)),
                            Err(err) => Err(map_sim_error(err)),
                        }
                    }
                    Err(err) => Err(map_sim_error(err)),
                }
            } else {
                match find_input_for_exact_out(self.hydrator.as_ref(), state, &req, probe_amount) {
                    Ok((required_input, sim)) => Ok((sim, required_input)),
                    Err(err) => Err(map_sim_error(err)),
                }
            };

        let (simulation, notional) = match simulation_result {
            Ok(result) => result,
            Err((status, reason)) => {
                self.record_snapshot_issue(
                    state,
                    slot,
                    base_to_quote,
                    status,
                    reason,
                    Some(probe_amount),
                    Some(amount_specified_is_input),
                    block_time_unix,
                );
                return None;
            }
        };

        if !simulation.tick_arrays.is_empty() {
            preferred_arrays = simulation.tick_arrays.clone();
        }

        let expected_output = simulation.amount_out;
        let executed_input = simulation.consumed_input;
        if expected_output == 0 || executed_input == 0 {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::InsufficientLiquidity,
                "simulation_zero_amount",
                Some(probe_amount),
                Some(amount_specified_is_input),
                block_time_unix,
            );
            return None;
        }

        let effective_price = expected_output as f64 / executed_input as f64;
        let slippage_bps = simulation.edge_bps;
        let sqrt_price_limit = Some(simulation.sqrt_price_after);

        let fee_bps = whirlpool_trade_fee_bps(state);
        let template_fee_bps = fee_bps.min(u64::from(u16::MAX)) as u16;

        let mut context = self.snapshot_template_context(state);
        if let Some(arrays) = self.resolve_tick_arrays(state, &req, &preferred_arrays) {
            context.whirlpool_tick_arrays = arrays;
        } else if let Some(arrays) = self.tick_arrays_for_direction(state, direction) {
            context.whirlpool_tick_arrays = arrays;
        }
        self.attach_owner_accounts(state, &mut context, input_mint, output_mint);

        self.record_snapshot_issue(
            state,
            slot,
            base_to_quote,
            QuoteStatus::Ok,
            "ok",
            Some(probe_amount),
            Some(amount_specified_is_input),
            block_time_unix,
        );

        Some(PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional,
            expected_output,
            fee_bps,
            template_fee_bps,
            effective_price,
            estimated_slippage_bps: slippage_bps,
            sqrt_price_limit,
            min_output_amount: Some(expected_output),
            limit_price: None,
            price_impact_limit_bps: None,
            amount_specified_is_input,
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

    fn prefer_exact_out_quotes(&self) -> bool {
        self.config.prefer_exact_out_quotes
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
        snapshot.hydration.require(HydrationComponent::Meta);
        snapshot.hydration.require(HydrationComponent::TickArrays);
        let Some(pool_pubkey) = parse_pubkey(&snapshot.pool_id) else {
            return;
        };
        let Some(state) = self.get_state(&pool_pubkey) else {
            return;
        };
        if state.meta_ready() {
            snapshot
                .hydration
                .mark_ready(HydrationComponent::Meta, state.last_update_slot);
        }
        if state.tick_arrays_ready() {
            snapshot
                .hydration
                .mark_ready(HydrationComponent::TickArrays, state.last_update_slot);
        }
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

    fn warm_components(&self, snapshot: &PoolSnapshot, components: &[HydrationComponent]) {
        let Some(pool_pubkey) = parse_pubkey(&snapshot.pool_id) else {
            return;
        };
        if components.contains(&HydrationComponent::Meta) {
            if let Some(hydrator) = self.hydrator.as_ref() {
                hydrator.hydrate(pool_pubkey);
            }
        }
        if components.contains(&HydrationComponent::TickArrays) {
            if let Some(state) = self.get_state(&pool_pubkey) {
                self.prefetch_tick_array_hints(&state);
                self.schedule_tick_array_windows(&state);
            }
        }
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

        let output_fee = state.transfer_fee_for_mint(request.output_mint);
        let block_time = request.block_time_u64();
        if !trade_enabled(&state, block_time) {
            let fallback_amount = state
                .mid_price()
                .and_then(|price| {
                    quote_from_price(&request, &state.token_mint_a, &state.token_mint_b, price)
                })
                .unwrap_or(0);
            let (net_fallback_out, _) =
                apply_post_fee_amount(fallback_amount, output_fee).unwrap_or((fallback_amount, 0));
            let mut response = build_quote_response(request, net_fallback_out);
            response.status = QuoteStatus::TradingDisabled;
            response.min_output_amount = Some(net_fallback_out);
            self.decorate_template_context(&mut response, &state);
            return Some(response);
        }

        let input_fee = state.transfer_fee_for_mint(request.input_mint);
        let Some((net_budget, _)) = apply_post_fee_amount(request.amount, input_fee) else {
            return None;
        };
        if net_budget == 0 {
            return None;
        }

        match simulate_best_exact_in(self.hydrator.as_ref(), &state, &request, net_budget) {
            Ok(simulation) => {
                let (net_output_amount, _) =
                    apply_post_fee_amount(simulation.amount_out, output_fee)
                        .unwrap_or((simulation.amount_out, 0));
                let (executed_input_amount, _) =
                    apply_pre_fee_amount(simulation.consumed_input, input_fee)
                        .unwrap_or((simulation.consumed_input, 0));
                let mut response = build_quote_response(request, net_output_amount);
                response.amount_in = executed_input_amount;
                response.edge_bps = simulation.edge_bps;
                response.sqrt_price_limit = Some(simulation.sqrt_price_after);
                response.min_output_amount = Some(net_output_amount);
                response.context.expected_fee_amount = Some(simulation.fee_amount);
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
                let (net_fallback_out, _) =
                    apply_post_fee_amount(amount_out, output_fee).unwrap_or((amount_out, 0));
                let mut fallback = build_quote_response(request, net_fallback_out);
                fallback.status = status;
                fallback.min_output_amount = Some(net_fallback_out);
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
        let input_fee = state.transfer_fee_for_mint(request.input_mint);
        let output_fee = state.transfer_fee_for_mint(request.output_mint);
        if !trade_enabled(&state, request.block_time_u64()) {
            let amount_in = quote_exact_out_from_price(&request, &state).unwrap_or(request.amount);
            let (executed_input_amount, _) =
                apply_pre_fee_amount(amount_in, input_fee).unwrap_or((amount_in, 0));
            let (net_amount_out, _) =
                apply_post_fee_amount(request.amount, output_fee).unwrap_or((request.amount, 0));
            let mut response = QuoteResponse {
                amount_in: executed_input_amount,
                amount_out: net_amount_out,
                edge_bps: 0.0,
                sqrt_price_limit: None,
                min_output_amount: Some(net_amount_out),
                status: QuoteStatus::TradingDisabled,
                request,
                context: TemplateContext::default(),
            };
            self.decorate_template_context(&mut response, &state);
            return Some(response);
        }
        let Some((target_out, _)) = apply_pre_fee_amount(request.amount, output_fee) else {
            return None;
        };
        if target_out == 0 {
            return None;
        }
        match find_input_for_exact_out(self.hydrator.as_ref(), &state, &request, target_out) {
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
                let (executed_input_amount, _) =
                    apply_pre_fee_amount(simulation.consumed_input, input_fee)
                        .unwrap_or((simulation.consumed_input, 0));
                let (net_output_amount, _) =
                    apply_post_fee_amount(simulation.amount_out, output_fee)
                        .unwrap_or((simulation.amount_out, 0));
                response.amount_in = executed_input_amount;
                response.amount_out = net_output_amount;
                response.min_output_amount = Some(net_output_amount);
                response.context.expected_fee_amount = Some(simulation.fee_amount);
                if !simulation.tick_arrays.is_empty() {
                    response.context.whirlpool_tick_arrays = simulation.tick_arrays.clone();
                }
                self.decorate_template_context(&mut response, &state);
                Some(response)
            }
            Err(err) => {
                let status: QuoteStatus = err.into();
                let amount_in = quote_exact_out_from_price(&request, &state)?;
                let (executed_input_amount, _) =
                    apply_pre_fee_amount(amount_in, input_fee).unwrap_or((amount_in, 0));
                let (net_amount_out, _) = apply_post_fee_amount(request.amount, output_fee)
                    .unwrap_or((request.amount, 0));
                let mut response = QuoteResponse {
                    amount_in: executed_input_amount,
                    amount_out: net_amount_out,
                    edge_bps: 0.0,
                    sqrt_price_limit: None,
                    min_output_amount: None,
                    status,
                    request,
                    context: TemplateContext::default(),
                };
                response.min_output_amount = Some(net_amount_out);
                self.decorate_template_context(&mut response, &state);
                Some(response)
            }
        }
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
        let base_mint = parse_pubkey(&snapshot.base_mint)?;
        let quote_mint = parse_pubkey(&snapshot.quote_mint)?;
        if base_mint == quote_mint {
            return None;
        }

        let utilization = self.config.max_swap_utilization_bps;
        let base_probe = snapshot_probe_amount(snapshot.base_post_amount, utilization);
        let quote_probe = snapshot_probe_amount(snapshot.quote_post_amount, utilization);
        let prefer_exact_out = self.config.prefer_exact_out_quotes;
        let block_time = snapshot.block_time_unix;

        let log_empty_reserve = |base_to_quote: bool, reason: &'static str| {
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                base_to_quote,
                QuoteStatus::InsufficientLiquidity,
                reason,
                None,
                None,
                block_time,
            );
        };

        let base_to_quote = if prefer_exact_out {
            if let Some(probe) = quote_probe {
                self.build_snapshot_side(
                    &state,
                    pool_pubkey,
                    snapshot.slot,
                    snapshot.block_time_unix,
                    base_mint,
                    quote_mint,
                    true,
                    probe,
                    false,
                )?
            } else if let Some(probe) = base_probe {
                self.build_snapshot_side(
                    &state,
                    pool_pubkey,
                    snapshot.slot,
                    snapshot.block_time_unix,
                    base_mint,
                    quote_mint,
                    true,
                    probe,
                    true,
                )?
            } else {
                log_empty_reserve(true, "base_quote_reserve_empty");
                return None;
            }
        } else {
            let probe = match base_probe {
                Some(probe) => probe,
                None => {
                    log_empty_reserve(true, "base_reserve_empty");
                    return None;
                }
            };
            self.build_snapshot_side(
                &state,
                pool_pubkey,
                snapshot.slot,
                snapshot.block_time_unix,
                base_mint,
                quote_mint,
                true,
                probe,
                true,
            )?
        };

        let quote_to_base = if prefer_exact_out {
            if let Some(probe) = base_probe {
                self.build_snapshot_side(
                    &state,
                    pool_pubkey,
                    snapshot.slot,
                    snapshot.block_time_unix,
                    quote_mint,
                    base_mint,
                    false,
                    probe,
                    false,
                )?
            } else if let Some(probe) = quote_probe {
                self.build_snapshot_side(
                    &state,
                    pool_pubkey,
                    snapshot.slot,
                    snapshot.block_time_unix,
                    quote_mint,
                    base_mint,
                    false,
                    probe,
                    true,
                )?
            } else {
                log_empty_reserve(false, "base_quote_reserve_empty");
                return None;
            }
        } else {
            let probe = match quote_probe {
                Some(probe) => probe,
                None => {
                    log_empty_reserve(false, "quote_reserve_empty");
                    return None;
                }
            };
            self.build_snapshot_side(
                &state,
                pool_pubkey,
                snapshot.slot,
                snapshot.block_time_unix,
                quote_mint,
                base_mint,
                false,
                probe,
                true,
            )?
        };

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

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
    token_program_a: Pubkey,
    token_program_b: Pubkey,
    tick_spacing: i32,
    liquidity: u128,
    sqrt_price_x64: u128,
    tick_current_index: i32,
    fee_rate: u16,
    protocol_fee_rate: u16,
    oracle: Pubkey,
    trade_enable_timestamp: Option<u64>,
    adaptive_fee: Option<WhirlpoolAdaptiveFeeInfo>,
}

#[derive(Debug, Clone, Copy, Default)]
struct HydratedOracleMeta {
    trade_enable_timestamp: Option<u64>,
    adaptive_fee: Option<WhirlpoolAdaptiveFeeInfo>,
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
    tick_backoff: Duration,
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
            tick_backoff,
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
            debug!(
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
            debug!(
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
                if let Some(mut meta) = parse_whirlpool_account(pool, &program_id, &account.data) {
                    let owner_a =
                        match fetch_mint_owner_with_priority(client, clients, &meta.token_mint_a) {
                            Ok(owner) => owner,
                            Err(error) => {
                                last_error = Some(error);
                                continue;
                            }
                        };
                    let owner_b =
                        match fetch_mint_owner_with_priority(client, clients, &meta.token_mint_b) {
                            Ok(owner) => owner,
                            Err(error) => {
                                last_error = Some(error);
                                continue;
                            }
                        };
                    meta.token_program_a = owner_a;
                    meta.token_program_b = owner_b;
                    match fetch_oracle_meta(pool, meta.oracle, clients) {
                        Ok(oracle_meta) => {
                            meta.trade_enable_timestamp = oracle_meta.trade_enable_timestamp;
                            meta.adaptive_fee = oracle_meta.adaptive_fee;
                        }
                        Err(error) => {
                            debug!(
                                target = "adapter::whirlpool",
                                pool = %pool,
                                oracle = %meta.oracle,
                                %error,
                                "failed to hydrate whirlpool oracle account"
                            );
                        }
                    }
                    return Ok(meta);
                } else {
                    return Err(FetchError::InvalidLayout("whirlpool pool account"));
                }
            }
            Err(error) => {
                last_error = Some(classify_fetch_error(&error));
            }
        }
    }

    Err(last_error.unwrap_or(FetchError::AccountNotFound))
}

fn fetch_oracle_meta(
    pool: Pubkey,
    oracle: Pubkey,
    clients: &[Arc<RpcClient>],
) -> Result<HydratedOracleMeta, FetchError> {
    let mut last_error: Option<FetchError> = None;
    for client in clients {
        match client.get_account(&oracle) {
            Ok(account) => {
                if let Some(meta) = parse_oracle_account(pool, &account.data) {
                    return Ok(meta);
                } else {
                    return Err(FetchError::InvalidLayout("whirlpool oracle account"));
                }
            }
            Err(error) => {
                last_error = Some(classify_fetch_error(&error));
            }
        }
    }

    Err(last_error.unwrap_or(FetchError::AccountNotFound))
}

fn is_account_not_found(error: &ClientError) -> bool {
    error.to_string().contains("AccountNotFound")
}

fn classify_fetch_error(error: &ClientError) -> FetchError {
    if is_account_not_found(error) {
        FetchError::AccountNotFound
    } else {
        FetchError::Rpc(error.to_string())
    }
}

fn fetch_mint_owner_with_priority(
    primary: &Arc<RpcClient>,
    clients: &[Arc<RpcClient>],
    mint: &Pubkey,
) -> Result<Pubkey, FetchError> {
    let mut last_error = match primary.get_account(mint) {
        Ok(account) => return Ok(account.owner),
        Err(error) => Some(classify_fetch_error(&error)),
    };

    for candidate in clients {
        if Arc::ptr_eq(candidate, primary) {
            continue;
        }
        match candidate.get_account(mint) {
            Ok(account) => return Ok(account.owner),
            Err(error) => last_error = Some(classify_fetch_error(&error)),
        }
    }

    Err(last_error.unwrap_or(FetchError::AccountNotFound))
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
const ADAPTIVE_FEE_CONSTANTS_LEN: usize = 34;
const ADAPTIVE_FEE_VARIABLES_LEN: usize = 44;
const ORACLE_RESERVED_BYTES: usize = 128;
const ORACLE_ACCOUNT_MIN_LEN: usize = ANCHOR_DISCRIMINATOR_LEN
    + PUBKEY_BYTES // whirlpool
    + 8 // trade enable timestamp
    + ADAPTIVE_FEE_CONSTANTS_LEN
    + ADAPTIVE_FEE_VARIABLES_LEN
    + ORACLE_RESERVED_BYTES;

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
        token_program_a: Pubkey::default(),
        token_program_b: Pubkey::default(),
        tick_spacing,
        liquidity,
        sqrt_price_x64,
        tick_current_index,
        fee_rate,
        protocol_fee_rate,
        oracle,
        trade_enable_timestamp: None,
        adaptive_fee: None,
    })
}

fn parse_oracle_account(pool: Pubkey, data: &[u8]) -> Option<HydratedOracleMeta> {
    if data.len() < ORACLE_ACCOUNT_MIN_LEN {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    let whirlpool = read_pubkey_bytes(&mut cursor, data)?;
    if whirlpool != pool {
        return None;
    }

    let trade_enable_timestamp = read_u64(&mut cursor, data)?;
    let constants = parse_adaptive_fee_constants(&mut cursor, data)?;
    let variables = parse_adaptive_fee_variables(&mut cursor, data)?;
    skip_bytes(&mut cursor, ORACLE_RESERVED_BYTES, data)?;

    let adaptive_fee = if constants.tick_group_size == 0 {
        None
    } else {
        Some(WhirlpoolAdaptiveFeeInfo {
            constants,
            variables,
        })
    };

    Some(HydratedOracleMeta {
        trade_enable_timestamp: Some(trade_enable_timestamp),
        adaptive_fee,
    })
}

fn parse_adaptive_fee_constants(
    cursor: &mut usize,
    data: &[u8],
) -> Option<WhirlpoolAdaptiveFeeConstants> {
    let filter_period = read_u16(cursor, data)?;
    let decay_period = read_u16(cursor, data)?;
    let reduction_factor = read_u16(cursor, data)?;
    let adaptive_fee_control_factor = read_u32(cursor, data)?;
    let max_volatility_accumulator = read_u32(cursor, data)?;
    let tick_group_size = read_u16(cursor, data)?;
    let major_swap_threshold_ticks = read_u16(cursor, data)?;
    let mut reserved = [0u8; 16];
    let end = cursor.checked_add(reserved.len())?;
    reserved.copy_from_slice(data.get(*cursor..end)?);
    *cursor = end;
    Some(WhirlpoolAdaptiveFeeConstants {
        filter_period,
        decay_period,
        reduction_factor,
        adaptive_fee_control_factor,
        max_volatility_accumulator,
        tick_group_size,
        major_swap_threshold_ticks,
        reserved,
    })
}

fn parse_adaptive_fee_variables(
    cursor: &mut usize,
    data: &[u8],
) -> Option<WhirlpoolAdaptiveFeeVariables> {
    let last_reference_update_timestamp = read_u64(cursor, data)?;
    let last_major_swap_timestamp = read_u64(cursor, data)?;
    let volatility_reference = read_u32(cursor, data)?;
    let tick_group_index_reference = read_i32(cursor, data)?;
    let volatility_accumulator = read_u32(cursor, data)?;
    let mut reserved = [0u8; 16];
    let end = cursor.checked_add(reserved.len())?;
    reserved.copy_from_slice(data.get(*cursor..end)?);
    *cursor = end;
    Some(WhirlpoolAdaptiveFeeVariables {
        last_reference_update_timestamp,
        last_major_swap_timestamp,
        volatility_reference,
        tick_group_index_reference,
        volatility_accumulator,
        reserved,
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

fn read_u32(cursor: &mut usize, data: &[u8]) -> Option<u32> {
    let end = cursor.checked_add(4)?;
    let bytes: [u8; 4] = data.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(u32::from_le_bytes(bytes))
}

fn read_u64(cursor: &mut usize, data: &[u8]) -> Option<u64> {
    let end = cursor.checked_add(8)?;
    let bytes: [u8; 8] = data.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(u64::from_le_bytes(bytes))
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
    pub token_program_a: Option<Pubkey>,
    pub token_program_b: Option<Pubkey>,
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
    pub token_a_transfer_fee: Option<TokenTransferFee>,
    pub token_b_transfer_fee: Option<TokenTransferFee>,
    pub transfer_hook_program_a: Option<Pubkey>,
    pub transfer_hook_program_b: Option<Pubkey>,
    pub trade_enable_timestamp: Option<u64>,
    pub adaptive_fee: Option<WhirlpoolAdaptiveFeeInfo>,
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

    pub fn transfer_fee_for_mint(&self, mint: Pubkey) -> Option<TokenTransferFee> {
        if mint == self.token_mint_a {
            self.token_a_transfer_fee
        } else if mint == self.token_mint_b {
            self.token_b_transfer_fee
        } else {
            None
        }
    }

    pub fn transfer_hook_program_for_mint(&self, mint: Pubkey) -> Option<Pubkey> {
        if mint == self.token_mint_a {
            self.transfer_hook_program_a
        } else if mint == self.token_mint_b {
            self.transfer_hook_program_b
        } else {
            None
        }
    }

    pub fn vault_for_mint(&self, mint: Pubkey) -> Option<Pubkey> {
        if mint == self.token_mint_a {
            self.token_vault_a
        } else if mint == self.token_mint_b {
            self.token_vault_b
        } else {
            None
        }
    }

    fn meta_ready(&self) -> bool {
        self.tick_spacing.is_some()
            && self.sqrt_price_x64.is_some()
            && self.liquidity.is_some()
            && self.tick_current_index.is_some()
    }

    fn tick_arrays_ready(&self) -> bool {
        self.tick_array0.is_some() && self.tick_array1.is_some() && self.tick_array2.is_some()
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
    fee_amount: u64,
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
        fee_amount: step.fee_amount.min(u64::MAX as u128) as u64,
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
    let mut amount_in_accum = 0u128;
    let mut fee_accum = 0u128;
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
        amount_in_accum = amount_in_accum.saturating_add(step.amount_in);
        fee_accum = fee_accum.saturating_add(step.fee_amount);

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
        fee_amount: fee_accum.min(u64::MAX as u128) as u64,
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

fn normalize_block_time(timestamp: Option<i64>) -> Option<u64> {
    timestamp.and_then(|value| if value >= 0 { Some(value as u64) } else { None })
}

fn current_unix_timestamp() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn trade_enabled(state: &WhirlpoolState, reference_time: Option<u64>) -> bool {
    match state.trade_enable_timestamp {
        Some(enabled_at) => {
            let now = reference_time.or_else(current_unix_timestamp);
            match now {
                Some(current) => current >= enabled_at,
                None => false,
            }
        }
        None => true,
    }
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
    use crate::hydration::types::{
        HydrationStatus, PoolSnapshot, PoolUpdateExtensions, SnapshotFreshness,
    };
    use dashmap::DashMap;

    #[test]
    fn static_alt_accounts_include_protocol_dependencies() {
        let whirlpool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let vault_a = Pubkey::new_unique();
        let vault_b = Pubkey::new_unique();
        let oracle = Pubkey::new_unique();
        let token_program_a = Pubkey::new_unique();
        let token_program_b = Pubkey::new_unique();
        let state = WhirlpoolState {
            whirlpool,
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            token_vault_a: Some(vault_a),
            token_vault_b: Some(vault_b),
            token_program_a: Some(token_program_a),
            token_program_b: Some(token_program_b),
            reserve_a: None,
            reserve_b: None,
            tick_spacing: None,
            liquidity: None,
            sqrt_price_x64: None,
            tick_current_index: None,
            fee_rate: None,
            protocol_fee_rate: None,
            oracle: Some(oracle),
            tick_array0: None,
            tick_array1: None,
            tick_array2: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };

        let accounts = WhirlpoolAdapter::static_alt_accounts(&state);
        assert!(accounts.contains(&whirlpool));
        assert!(accounts.contains(&mint_a));
        assert!(accounts.contains(&mint_b));
        assert!(accounts.contains(&vault_a));
        assert!(accounts.contains(&vault_b));
        assert!(accounts.contains(&oracle));
        assert!(accounts.contains(&token_program_a));
        assert!(accounts.contains(&token_program_b));
        assert!(accounts.contains(&MEMO_PROGRAM_ID));
    }

    #[test]
    fn static_alt_accounts_fall_back_to_spl_token() {
        let whirlpool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = WhirlpoolState {
            whirlpool,
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            token_vault_a: None,
            token_vault_b: None,
            token_program_a: None,
            token_program_b: None,
            reserve_a: None,
            reserve_b: None,
            tick_spacing: None,
            liquidity: None,
            sqrt_price_x64: None,
            tick_current_index: None,
            fee_rate: None,
            protocol_fee_rate: None,
            oracle: None,
            tick_array0: None,
            tick_array1: None,
            tick_array2: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };

        let accounts = WhirlpoolAdapter::static_alt_accounts(&state);
        assert!(accounts.contains(&SPL_TOKEN_PROGRAM_ID));
    }

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
    fn attaches_supplemental_tick_arrays_in_chunks() {
        let adapter = WhirlpoolAdapter::new(WhirlpoolAdapterConfig::default(), None);
        let base_arrays = vec![
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let supplemental_a = Pubkey::new_unique();
        let supplemental_b = Pubkey::new_unique();
        let supplemental_c = Pubkey::new_unique();
        let supplemental_d = Pubkey::new_unique();
        let supplemental = vec![
            base_arrays[0],
            supplemental_a,
            supplemental_b,
            supplemental_b,
            Pubkey::default(),
            supplemental_c,
            supplemental_d,
        ];
        let mut context = TemplateContext {
            whirlpool_tick_arrays: base_arrays,
            ..TemplateContext::default()
        };

        adapter.attach_supplemental_tick_arrays(&mut context, &supplemental);

        let extras: Vec<Pubkey> = context
            .remaining_accounts
            .iter()
            .map(|meta| meta.pubkey)
            .collect();
        assert_eq!(
            extras,
            vec![
                supplemental_a,
                supplemental_b,
                supplemental_c,
                supplemental_d
            ]
        );
        assert_eq!(context.whirlpool_remaining_accounts.len(), 2);
        assert_eq!(
            context.whirlpool_remaining_accounts,
            vec![
                WhirlpoolRemainingAccountsSlice {
                    accounts_type: WhirlpoolRemainingAccountsType::SupplementalTickArrays,
                    length: 3,
                },
                WhirlpoolRemainingAccountsSlice {
                    accounts_type: WhirlpoolRemainingAccountsType::SupplementalTickArraysOne,
                    length: 1,
                },
            ]
        );
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
            token_program_a: None,
            token_program_b: None,
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: token_a,
            output_mint: token_b,
            amount: 1_000_000,
            slot: 0,
            block_time: None,
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
            token_program_a: None,
            token_program_b: None,
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: Pubkey::new_unique(),
            output_mint: token_b,
            amount: 5,
            slot: 0,
            block_time: None,
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
            token_program_a: None,
            token_program_b: None,
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: token_b,
            output_mint: token_a,
            amount: 50_000,
            slot: 0,
            block_time: None,
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
            token_program_a: None,
            token_program_b: None,
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: token_a,
            output_mint: token_b,
            amount: 10_000,
            slot: 0,
            block_time: None,
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
            transfer_fee_cache: None,
            transfer_hook_resolver: None,
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
            token_program_a: None,
            token_program_b: None,
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
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
            hydration: HydrationStatus::new(1),
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

    #[test]
    fn refreshed_state_rehydrates_transfer_hook_programs() {
        let hook_program = Pubkey::new_unique();
        let token_program = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let fee_cache = TokenTransferFeeHydrator::with_static(vec![(
            mint_a,
            None,
            Some(hook_program),
            token_program,
        )]);
        let adapter = WhirlpoolAdapter {
            program_id: Pubkey::from_str(WHIRLPOOL_PROGRAM_ID).unwrap(),
            state: DashMap::new(),
            config: WhirlpoolAdapterConfig::default(),
            hydrator: None,
            transfer_fee_cache: Some(fee_cache),
            transfer_hook_resolver: None,
            template_builder: None,
            registered_templates: DashMap::new(),
        };
        let state = WhirlpoolState {
            whirlpool: Pubkey::new_unique(),
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            token_vault_a: None,
            token_vault_b: None,
            token_program_a: None,
            token_program_b: None,
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        let refreshed = adapter.refreshed_state(&state);
        assert!(state.transfer_hook_program_for_mint(mint_a).is_none());
        assert_eq!(
            refreshed.transfer_hook_program_for_mint(mint_a),
            Some(hook_program)
        );
    }

    #[test]
    fn quotes_block_before_trade_enable_timestamp() {
        let adapter = WhirlpoolAdapter {
            program_id: Pubkey::from_str(WHIRLPOOL_PROGRAM_ID).unwrap(),
            state: DashMap::new(),
            config: WhirlpoolAdapterConfig::default(),
            hydrator: None,
            transfer_fee_cache: None,
            transfer_hook_resolver: None,
            template_builder: None,
            registered_templates: DashMap::new(),
        };

        let pool = Pubkey::new_unique();
        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        let future_ts = 1_700_000_600u64;
        let block_time = future_ts.saturating_sub(120) as i64;
        let state = WhirlpoolState {
            whirlpool: pool,
            token_mint_a: token_a,
            token_mint_b: token_b,
            token_vault_a: None,
            token_vault_b: None,
            token_program_a: None,
            token_program_b: None,
            reserve_a: Some(5_000_000u128),
            reserve_b: Some(5_000_000u128),
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: Some(future_ts),
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        adapter.state.insert(pool, state);

        let req_in = QuoteRequest {
            pool_id: pool,
            input_mint: token_a,
            output_mint: token_b,
            amount: 1_000,
            slot: 0,
            block_time: Some(block_time),
            direction: SwapDirection::ExactIn,
        };
        let quote_in = adapter.quote_exact_in(req_in).expect("quote");
        assert_eq!(quote_in.status, QuoteStatus::TradingDisabled);

        let req_out = QuoteRequest {
            pool_id: pool,
            input_mint: token_a,
            output_mint: token_b,
            amount: 1_000,
            slot: 0,
            block_time: Some(block_time),
            direction: SwapDirection::ExactOut,
        };
        let quote_out = adapter.quote_exact_out(req_out).expect("quote");
        assert_eq!(quote_out.status, QuoteStatus::TradingDisabled);
    }

    #[test]
    fn snapshot_bidirectional_quotes_honor_exact_out_preference() {
        let mut config = WhirlpoolAdapterConfig::default();
        config.prefer_exact_out_quotes = true;
        let adapter = WhirlpoolAdapter {
            program_id: Pubkey::from_str(WHIRLPOOL_PROGRAM_ID).unwrap(),
            state: DashMap::new(),
            config,
            hydrator: None,
            transfer_fee_cache: None,
            transfer_hook_resolver: None,
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
            token_program_a: None,
            token_program_b: None,
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: None,
            adaptive_fee: None,
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
            hydration: HydrationStatus::new(1),
            extensions: PoolUpdateExtensions::default(),
        };

        let quotes = adapter
            .bidirectional_quotes(&snapshot)
            .expect("quotes missing");

        assert!(!quotes.base_to_quote.amount_specified_is_input);
        assert!(!quotes.quote_to_base.amount_specified_is_input);
        assert!(quotes.base_to_quote.notional > 0);
        assert!(quotes.quote_to_base.notional > 0);
    }

    #[test]
    fn snapshot_quotes_skip_before_trade_enable_timestamp() {
        let adapter = WhirlpoolAdapter {
            program_id: Pubkey::from_str(WHIRLPOOL_PROGRAM_ID).unwrap(),
            state: DashMap::new(),
            config: WhirlpoolAdapterConfig::default(),
            hydrator: None,
            transfer_fee_cache: None,
            transfer_hook_resolver: None,
            template_builder: None,
            registered_templates: DashMap::new(),
        };

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let future_ts = 1_700_000_500u64;
        let state = WhirlpoolState {
            whirlpool: pool,
            token_mint_a: base_mint,
            token_mint_b: quote_mint,
            token_vault_a: None,
            token_vault_b: None,
            token_program_a: None,
            token_program_b: None,
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
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            transfer_hook_program_a: None,
            transfer_hook_program_b: None,
            trade_enable_timestamp: Some(future_ts),
            adaptive_fee: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };
        adapter.state.insert(pool, state);

        let snapshot = PoolSnapshot {
            pool_id: pool.to_string(),
            program_id: adapter.program_id.to_string(),
            slot: 1,
            block_hash: None,
            block_time_unix: Some((future_ts - 60) as i64),
            base_mint: base_mint.to_string(),
            quote_mint: quote_mint.to_string(),
            base_post_amount: 2_000_000u128,
            quote_post_amount: 3_000_000u128,
            updated_at: Instant::now(),
            freshness: SnapshotFreshness::LiveCorecast,
            allow_reserve_backfill: true,
            quotes: None,
            hydration: HydrationStatus::new(1),
            extensions: PoolUpdateExtensions::default(),
        };

        assert!(adapter.bidirectional_quotes(&snapshot).is_none());
    }
}
