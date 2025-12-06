use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022::extension::StateWithExtensionsOwned;
use spl_token_2022::state::Mint as SplToken2022Mint;
use tracing::{debug, info, trace, warn};
use uint::construct_uint;

use super::hydration::{AsyncHydrator, HydratedValue};
use crate::builder::{BuilderHandle, CpAmmPoolAccounts, CpAmmPoolMeta};
use crate::hydration::token_fees::{
    apply_post_fee_amount, apply_pre_fee_amount, TokenTransferFee, TokenTransferFeeHydrator,
};
use crate::hydration::types::{
    CpAmmBaseFeeExtras, CpAmmDynamicFeeExtras, CpAmmPoolExtras, CpAmmPoolFeesExtras,
    HydrationComponent, PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate,
    CP_AMM_INPUT_UTILIZATION_BPS,
};
use crate::template::TemplateContext;
use crate::telemetry;

use super::{
    build_quote_response, ProtocolAdapter, QuoteRequest, QuoteResponse, QuoteStatus, SwapDirection,
};

const BPS_DENOMINATOR: u64 = 10_000;
const FEE_DENOMINATOR: u64 = 1_000_000_000;
const MAX_FEE_NUMERATOR_V0: u64 = 500_000_000;
const MAX_FEE_NUMERATOR_V1: u64 = 990_000_000;
const MAX_FEE_BPS_V0: u64 = 5_000;
const MAX_FEE_BPS_V1: u64 = 9_900;
const MIN_FEE_NUMERATOR: u64 = 100_000;
const CURRENT_POOL_VERSION: u8 = 0;
const CP_AMM_DEFAULT_CU_LIMIT: u32 = 400_000;
const CP_AMM_DEFAULT_CU_PRICE: u64 = 5_000;

const CP_AMM_PROGRAM_ID: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SPL_TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const SNAPSHOT_PROBE_DIVISOR: u128 = 5_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;
const Q64_FACTOR: f64 = 18_446_744_073_709_551_616.0;

const DEFAULT_CP_AMM_HYDRATION_RETRY_MS: u64 = 60_000;
const SNAPSHOT_FAILURE_LOG_INTERVAL_MS: u64 = 15_000;
const STATE_MISS_LOG_INTERVAL_MS: u64 = 15_000;

#[derive(Clone)]
struct SnapshotStatusLogEntry {
    status: QuoteStatus,
    logged_at: Instant,
}

static SPL_TOKEN_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str(SPL_TOKEN_PROGRAM_ID).expect("static spl token program should be valid")
});
static SPL_TOKEN_2022_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str(SPL_TOKEN_2022_PROGRAM_ID)
        .expect("static spl token 2022 program should be valid")
});

construct_uint! {
    pub struct U256(4);
}

#[derive(Debug, Clone)]
pub struct CpAmmAdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
    pub hydration_retry_ms: u64,
    pub hydration_meta_ttl_ms: u64,
    pub max_swap_utilization_bps: u64,
    pub required_partner: Option<Pubkey>,
}

impl Default for CpAmmAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            // initial hydration hits several large accounts; allow generous timeout
            rpc_timeout_ms: 15_000,
            hydration_retry_ms: DEFAULT_CP_AMM_HYDRATION_RETRY_MS,
            hydration_meta_ttl_ms: 600_000,
            max_swap_utilization_bps: CP_AMM_INPUT_UTILIZATION_BPS,
            required_partner: None,
        }
    }
}

pub struct CpAmmAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, CpAmmState>,
    config: CpAmmAdapterConfig,
    hydrator: Option<CpAmmPoolHydrator>,
    transfer_fee_cache: Option<TokenTransferFeeHydrator>,
    mint_metadata_cache: Option<MintMetadataHydrator>,
    template_builder: Option<BuilderHandle>,
    registered_templates: DashMap<Pubkey, RegisteredTemplateSignature>,
    snapshot_failure_log: DashMap<(Pubkey, bool), SnapshotStatusLogEntry>,
    missing_state_log: DashMap<Pubkey, Instant>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RegisteredTemplateSignature {
    alt_accounts: Vec<Pubkey>,
    pool_accounts: CpAmmPoolAccounts,
    fee_tier_bps: u16,
}

impl CpAmmAdapter {
    pub fn new(config: CpAmmAdapterConfig, template_builder: Option<BuilderHandle>) -> Self {
        let program_id =
            Pubkey::from_str(CP_AMM_PROGRAM_ID).expect("static cp_amm program id should be valid");
        let hydrator = CpAmmPoolHydrator::new(&config);
        let transfer_fee_cache = TokenTransferFeeHydrator::new(
            &config.rpc_endpoints,
            config.rpc_timeout_ms,
            config.hydration_retry_ms,
        );
        let mint_metadata_cache = MintMetadataHydrator::new(&config);
        if let Some(hydrator) = hydrator.as_ref() {
            info!(
                target = "adapter::cp_amm",
                endpoints = hydrator.client_count(),
                "cp_amm pool hydrator online"
            );
        } else {
            debug!(
                target = "adapter::cp_amm",
                "cp_amm pool hydrator disabled (no rpc endpoints)"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            hydrator,
            transfer_fee_cache,
            mint_metadata_cache,
            template_builder,
            registered_templates: DashMap::new(),
            snapshot_failure_log: DashMap::new(),
            missing_state_log: DashMap::new(),
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<CpAmmState> {
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
                    target = "adapter::cp_amm",
                    pool = %state.pool,
                    last_slot = state.last_update_slot,
                    "evicting stale pool state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: CpAmmPoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::cp_amm",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let previous_state = self.state.get(&pool_pubkey);
        let extras = self.enrich_extras(pool_pubkey, extras);

        let Some(base_mint) = extras
            .token_a_mint
            .as_deref()
            .and_then(parse_pubkey)
            .or_else(|| parse_pubkey(&update.base_mint))
        else {
            warn!(target = "adapter::cp_amm", pool = %update.pool_id, "invalid base mint");
            return;
        };

        let Some(quote_mint) = extras
            .token_b_mint
            .as_deref()
            .and_then(parse_pubkey)
            .or_else(|| parse_pubkey(&update.quote_mint))
        else {
            warn!(target = "adapter::cp_amm", pool = %update.pool_id, "invalid quote mint");
            return;
        };

        let mut hydrated_meta_slot = extras.hydrated_slot;
        if hydrated_meta_slot
            .map(|slot| slot < update.slot)
            .unwrap_or(true)
        {
            if let Some(existing) = previous_state.as_ref() {
                if existing
                    .hydrated_meta_slot
                    .map(|slot| slot >= update.slot)
                    .unwrap_or(false)
                {
                    hydrated_meta_slot = existing.hydrated_meta_slot;
                }
            }
        }

        if hydrated_meta_slot
            .map(|slot| slot < update.slot)
            .unwrap_or(true)
        {
            if let Some(hydrator) = self.hydrator.as_ref() {
                hydrator.refresh(pool_pubkey);
            }
            hydrated_meta_slot = None;
        }

        let last_hydrated_at = extras
            .hydrated_at
            .or_else(|| previous_state.as_ref().and_then(|existing| existing.last_hydrated_at));

        let mut state = CpAmmState {
            pool: pool_pubkey,
            token_a_mint: base_mint,
            token_b_mint: quote_mint,
            token_a_vault: extras
                .token_a_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_b_vault: extras
                .token_b_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            whitelisted_vault: extras
                .whitelisted_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            partner: extras
                .partner
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            pool_authority: extras
                .pool_authority
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_a_program: extras
                .token_a_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_b_program: extras
                .token_b_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_a_decimals: None,
            token_b_decimals: None,
            amp: extras.amp,
            lp_fee_bps: extras.lp_fee_bps,
            protocol_fee_bps: extras.protocol_fee_bps,
            referral_bps: extras.referral_bps,
            referral_token_account: extras
                .referral_token_account
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            collect_fee_mode: extras.collect_fee_mode,
            activation_point: extras.activation_point,
            activation_type: extras.activation_type,
            pool_status: extras.pool_status,
            pool_type: extras.pool_type,
            pool_version: extras.pool_version,
            token_a_flag: extras.token_a_flag,
            token_b_flag: extras.token_b_flag,
            permanent_lock_liquidity: extras.permanent_lock_liquidity,
            pool_fees: extras.pool_fees.clone(),
            sqrt_price_x64: extras.sqrt_price_x64,
            liquidity: extras.liquidity,
            sqrt_min_price_x64: extras.sqrt_min_price_x64,
            sqrt_max_price_x64: extras.sqrt_max_price_x64,
            vault_a_balance: update.base_post_amount,
            vault_b_balance: update.quote_post_amount,
            corecast_slot: update.slot,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
            last_hydrated_at,
            hydrated_meta_slot,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            last_block_time_unix: normalize_block_time(update.block_time_unix),
        };

        self.attach_mint_metadata(&mut state);
        self.ensure_token_programs(&mut state);
        self.attach_transfer_fees(&mut state);

        trace!(
            target = "adapter::cp_amm",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting cp_amm pool state"
        );

        self.register_templates_with_builder(&state);
        self.record_hydration_metrics(&state);
        drop(previous_state);
        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: CpAmmPoolExtras) -> CpAmmPoolExtras {
        let force_refresh = hydration_meta_expired(&extras, self.config.hydration_meta_ttl_ms);
        if !needs_hydration(&extras) && !force_refresh {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            apply_hydrated_meta(&mut extras, &meta, force_refresh);
        }

        extras
    }

    fn register_templates_with_builder(&self, state: &CpAmmState) {
        let Some(builder) = self.template_builder.as_ref() else {
            return;
        };

        let mut alt_accounts = Vec::with_capacity(10);
        alt_accounts.push(state.pool);
        alt_accounts.push(state.token_a_mint);
        alt_accounts.push(state.token_b_mint);
        if let Some(pk) = state.pool_authority {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.token_a_vault {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.token_b_vault {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.token_a_program {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.token_b_program {
            alt_accounts.push(pk);
        }
        if let Some(pk) = state.referral_token_account {
            alt_accounts.push(pk);
        }

        let token_a_program = state.token_a_program.unwrap_or(*SPL_TOKEN_PROGRAM_PUBKEY);
        let token_b_program = state.token_b_program.unwrap_or(*SPL_TOKEN_PROGRAM_PUBKEY);
        let fee_tier_bps = state.total_fee_bps().min(u64::from(u16::MAX)) as u16;
        let pool_accounts = CpAmmPoolAccounts {
            token_a_mint: state.token_a_mint,
            token_b_mint: state.token_b_mint,
            token_a_vault: state.token_a_vault,
            token_b_vault: state.token_b_vault,
            token_a_program: Some(token_a_program),
            token_b_program: Some(token_b_program),
            referral_token_account: state.referral_token_account,
        };
        if !pool_accounts.is_populated() {
            debug!(
                target = "adapter::cp_amm",
                pool = %state.pool,
                "cp_amm pool metadata incomplete; deferring template registration"
            );
            return;
        }

        let signature = RegisteredTemplateSignature {
            alt_accounts: alt_accounts.clone(),
            pool_accounts: pool_accounts.clone(),
            fee_tier_bps,
        };

        if let Some(existing) = self.registered_templates.get(&state.pool) {
            let alt_missing = builder.alt_manager().handle_for_pool(state.pool).is_none();
            if *existing == signature && !alt_missing {
                return;
            }
            if *existing == signature && alt_missing {
                debug!(
                    target = "adapter::cp_amm",
                    pool = %state.pool,
                    "ALT handle missing; re-registering pool templates"
                );
            }
        }

        builder.register_cp_amm_pool(CpAmmPoolMeta {
            pool_id: state.pool,
            fee_tier_bps,
            alt_accounts: alt_accounts.clone(),
            alt_handle: None,
            accounts: pool_accounts.clone(),
        });

        self.registered_templates.insert(state.pool, signature);
    }

    fn record_hydration_metrics(&self, state: &CpAmmState) {
        let adapter = self.name();
        let meta_slot = state.hydrated_meta_slot.unwrap_or(0);
        let delta = state.corecast_slot.saturating_sub(meta_slot);
        telemetry::record_hydration_component_slot_delta(
            adapter,
            HydrationComponent::Meta.as_str(),
            delta,
        );

        let ready = state.meta_ready_for_slot(state.corecast_slot)
            && !state.hydration_stale(self.config.hydration_meta_ttl_ms);
        let stage = if ready { 1 } else { 0 };
        telemetry::record_hydration_stage(adapter, &state.pool.to_string(), stage);
    }

    fn attach_transfer_fees(&self, state: &mut CpAmmState) {
        state.token_a_transfer_fee =
            self.resolve_transfer_fee(state.token_a_mint, state.token_a_program);
        state.token_b_transfer_fee =
            self.resolve_transfer_fee(state.token_b_mint, state.token_b_program);
    }

    fn attach_mint_metadata(&self, state: &mut CpAmmState) {
        if let Some(meta) = self.mint_metadata(state.token_a_mint) {
            let MintMetadata {
                decimals,
                program_id,
                ..
            } = meta;
            if state.token_a_decimals.is_none() {
                if let Some(value) = decimals {
                    state.token_a_decimals = Some(value);
                }
            }
            if state.token_a_program.is_none() {
                state.token_a_program = Some(program_id);
            }
        }
        if let Some(meta) = self.mint_metadata(state.token_b_mint) {
            let MintMetadata {
                decimals,
                program_id,
                ..
            } = meta;
            if state.token_b_decimals.is_none() {
                if let Some(value) = decimals {
                    state.token_b_decimals = Some(value);
                }
            }
            if state.token_b_program.is_none() {
                state.token_b_program = Some(program_id);
            }
        }
    }

    fn ensure_token_programs(&self, state: &mut CpAmmState) {
        if state.token_a_program.is_none() {
            state.token_a_program = self.resolve_token_program(state.token_a_mint);
        }
        if state.token_b_program.is_none() {
            state.token_b_program = self.resolve_token_program(state.token_b_mint);
        }
    }

    fn mint_metadata(&self, mint: Pubkey) -> Option<MintMetadata> {
        let cache = self.mint_metadata_cache.as_ref()?;
        cache.hydrate(mint)
    }

    fn resolve_transfer_fee(
        &self,
        mint: Pubkey,
        program: Option<Pubkey>,
    ) -> Option<TokenTransferFee> {
        let Some(cache) = self.transfer_fee_cache.as_ref() else {
            return None;
        };
        let program_id = match program {
            Some(program_id) => program_id,
            None => cache.mint_program(mint)?,
        };
        if program_id != *SPL_TOKEN_2022_PROGRAM_PUBKEY {
            return None;
        }
        cache.hydrate(mint)
    }

    fn resolve_token_program(&self, mint: Pubkey) -> Option<Pubkey> {
        if let Some(cache) = self.mint_metadata_cache.as_ref() {
            if let Some(meta) = cache.hydrate(mint) {
                return Some(meta.program_id);
            }
        }
        if let Some(cache) = self.transfer_fee_cache.as_ref() {
            if let Some(program) = cache.mint_program(mint) {
                return Some(program);
            }
        }
        None
    }

    fn decorate_template_context(
        &self,
        state: &CpAmmState,
        context: &mut TemplateContext,
        input_mint: Pubkey,
        output_mint: Pubkey,
    ) {
        if context.input_token_program.is_none() {
            context.input_token_program = Some(token_program_for_mint(state, &input_mint));
        }
        if context.output_token_program.is_none() {
            context.output_token_program = Some(token_program_for_mint(state, &output_mint));
        }

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
                    target = "adapter::cp_amm",
                    pool = %state.pool,
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
                    target = "adapter::cp_amm",
                    pool = %state.pool,
                    mint = %output_mint,
                    "missing configured output token ATA"
                );
            }
        }
    }

    fn clamp_request_amount(&self, state: &CpAmmState, req: &QuoteRequest) -> u64 {
        let reserve = if req.input_mint == state.token_a_mint {
            state.vault_a_balance
        } else if req.input_mint == state.token_b_mint {
            state.vault_b_balance
        } else {
            return req.amount;
        };
        clamp_amount_to_reserve(req.amount, reserve, self.config.max_swap_utilization_bps)
    }

    fn clamp_request_output_amount(&self, state: &CpAmmState, req: &QuoteRequest) -> u64 {
        let reserve = if req.output_mint == state.token_a_mint {
            state.vault_a_balance
        } else if req.output_mint == state.token_b_mint {
            state.vault_b_balance
        } else {
            return req.amount;
        };
        clamp_amount_to_reserve(req.amount, reserve, self.config.max_swap_utilization_bps)
    }

    fn partner_mismatch(&self, state: &CpAmmState) -> bool {
        if !state.has_partner() {
            return false;
        }
        let Some(partner) = state.partner else {
            return true;
        };
        if let Some(required) = self.config.required_partner {
            return partner != required;
        }
        if let Some(builder) = self.template_builder.as_ref() {
            let auth = builder.auth_pubkey();
            let payer = builder.payer_pubkey();
            return partner != auth && partner != payer;
        }
        false
    }

    fn log_state_cache_miss(&self, pool: Pubkey, slot: u64) {
        if let Some(entry) = self.missing_state_log.get(&pool) {
            if (entry.value().elapsed().as_millis() as u64) < STATE_MISS_LOG_INTERVAL_MS {
                return;
            }
        }
        self.missing_state_log.insert(pool, Instant::now());
        warn!(
            target = "adapter::cp_amm",
            pool = %pool,
            slot,
            hydrator_online = self.hydrator.is_some(),
            "cp_amm strict price snapshot requested before hydration"
        );
    }

    fn clear_snapshot_issue(&self, pool: Pubkey, base_to_quote: bool) {
        self.snapshot_failure_log.remove(&(pool, base_to_quote));
    }

    fn record_snapshot_issue(
        &self,
        state: &CpAmmState,
        slot: u64,
        base_to_quote: bool,
        status: QuoteStatus,
        reason: &'static str,
        probe_amount: Option<u64>,
    ) {
        if status == QuoteStatus::Ok {
            self.clear_snapshot_issue(state.pool, base_to_quote);
            return;
        }

        let key = (state.pool, base_to_quote);
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

        let hydration_age_ms = state
            .last_hydrated_at
            .map(|instant| instant.elapsed().as_millis() as u64);
        let cache_age_ms = state.last_update_at.elapsed().as_millis() as u64;
        let hydrator_clients = self
            .hydrator
            .as_ref()
            .map(|hydrator| hydrator.client_count())
            .unwrap_or(0);

        warn!(
            target = "adapter::cp_amm",
            pool = %state.pool,
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
            cache_age_ms,
            hydration_age_ms,
            vault_a = %state.vault_a_balance,
            vault_b = %state.vault_b_balance,
            has_price = state.sqrt_price_x64.is_some(),
            has_liquidity = state.liquidity.is_some(),
            partner = ?state.partner,
            referral = ?state.referral_token_account,
            hydrator_clients,
            "cp_amm strict price quote unavailable"
        );
    }

    fn cache_stale(&self, state: &CpAmmState, current_slot: u64) -> bool {
        let slot_stale = self.config.stale_slot_threshold > 0
            && current_slot.saturating_sub(state.last_update_slot)
                > self.config.stale_slot_threshold;
        let time_stale = self.config.stale_after_ms > 0
            && state.last_update_at.elapsed().as_millis() as u64 > self.config.stale_after_ms;
        slot_stale || time_stale
    }

    fn build_snapshot_side(
        &self,
        state: &CpAmmState,
        pool_id: Pubkey,
        slot: u64,
        block_time: Option<i64>,
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
        if state.hydration_stale(self.config.hydration_meta_ttl_ms) {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::StaleState,
                "hydration_stale",
                Some(probe_amount),
            );
            return None;
        }
        if !state.meta_ready_for_slot(state.corecast_slot) {
            self.request_meta_refresh(state.pool);
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::HydrationPending,
                "meta_pending",
                Some(probe_amount),
            );
            return None;
        }
        let block_time_u64 = normalize_block_time(block_time);
        if !state.swap_enabled(slot, block_time_u64) {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::TradingDisabled,
                "swap_disabled",
                Some(probe_amount),
            );
            return None;
        }
        if self.partner_mismatch(state) {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::MissingPartner,
                "partner_mismatch",
                Some(probe_amount),
            );
            return None;
        }

        let (input_mint, output_mint) = if base_to_quote {
            (state.token_a_mint, state.token_b_mint)
        } else {
            (state.token_b_mint, state.token_a_mint)
        };

        let req = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: probe_amount,
            slot,
            block_time,
            direction: SwapDirection::ExactIn,
        };

        let simulation = match simulate_cp_amm_exact_in(state, &req, true) {
            Some(sim) => sim,
            None => {
                self.record_snapshot_issue(
                    state,
                    slot,
                    base_to_quote,
                    QuoteStatus::InsufficientLiquidity,
                    "simulation_error",
                    Some(probe_amount),
                );
                return None;
            }
        };
        let expected_output = simulation.output_amount;
        let executed_input = simulation.executed_input_amount;
        let direction = if base_to_quote {
            TradeDirection::AToB
        } else {
            TradeDirection::BToA
        };
        let fee_bps = state.total_fee_bps();
        let template_fee_bps = fee_bps.min(u64::from(u16::MAX)) as u16;
        let effective_price =
            scaled_execution_price(state, direction, expected_output, executed_input)
                .unwrap_or(0.0);

        let mut context = TemplateContext {
            compute_unit_limit: Some(CP_AMM_DEFAULT_CU_LIMIT),
            compute_unit_price: Some(CP_AMM_DEFAULT_CU_PRICE),
            requires_memo: state.referral_token_account.is_some(),
            requires_dont_front: false,
            host_fee_account: state.referral_token_account,
            ..Default::default()
        };
        self.decorate_template_context(state, &mut context, input_mint, output_mint);
        if simulation.amount_leftover > 0 {
            context.partial_fill_remaining = Some(simulation.amount_leftover);
        }
        context.expected_fee_amount = Some(simulation.fee_amount);

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
            notional: probe_amount,
            expected_output,
            fee_bps,
            template_fee_bps,
            effective_price,
            estimated_slippage_bps: simulation.slippage_bps,
            sqrt_price_limit: None,
            min_output_amount: Some(expected_output),
            limit_price: None,
            price_impact_limit_bps: None,
            amount_specified_is_input: true,
            context,
            status: QuoteStatus::Ok,
            sqrt_price_before_x64: None,
            sqrt_price_after_x64: None,
            tick_arrays_used: Vec::new(),
            cache_slot: Some(slot),
        })
    }

    fn request_meta_refresh(&self, pool: Pubkey) {
        if let Some(hydrator) = self.hydrator.as_ref() {
            hydrator.refresh(pool);
        }
    }

    fn eager_hydration(
        &self,
        pool: Pubkey,
        base_hint: Option<Pubkey>,
        quote_hint: Option<Pubkey>,
    ) {
        if let Some(hydrator) = self.hydrator.as_ref() {
            hydrator.warm(pool);
        }

        if let Some(state) = self.get_state(&pool) {
            self.warm_mint_artifacts(state.token_a_mint);
            self.warm_mint_artifacts(state.token_b_mint);
            return;
        }

        if let Some(mint) = base_hint {
            self.warm_mint_artifacts(mint);
        }
        if let Some(mint) = quote_hint {
            self.warm_mint_artifacts(mint);
        }
    }

    fn warm_mint_artifacts(&self, mint: Pubkey) {
        if let Some(cache) = self.mint_metadata_cache.as_ref() {
            cache.hydrate(mint);
        }
        if let Some(cache) = self.transfer_fee_cache.as_ref() {
            cache.hydrate(mint);
        }
    }
}

impl ProtocolAdapter for CpAmmAdapter {
    fn name(&self) -> &'static str {
        "cp_amm"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::cp_amm",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        let pool_pubkey = parse_pubkey(&update.pool_id);
        let base_mint = parse_pubkey(&update.base_mint);
        let quote_mint = parse_pubkey(&update.quote_mint);
        let extras = update.extensions.cp_amm.clone().unwrap_or_default();
        self.upsert_state(update, extras);
        if let Some(pool) = pool_pubkey {
            self.eager_hydration(pool, base_mint, quote_mint);
        }
    }

    fn enrich_snapshot(&self, snapshot: &mut PoolSnapshot) {
        snapshot.hydration.require(HydrationComponent::Meta);
        let Some(pool_pubkey) = parse_pubkey(&snapshot.pool_id) else {
            return;
        };
        let Some(state) = self.get_state(&pool_pubkey) else {
            return;
        };
        if !state.hydration_stale(self.config.hydration_meta_ttl_ms)
            && state.meta_ready_for_slot(state.corecast_slot)
        {
            snapshot
                .hydration
                .mark_ready(HydrationComponent::Meta, state.last_update_slot);
        } else {
            self.request_meta_refresh(pool_pubkey);
        }
        let extras = snapshot
            .extensions
            .cp_amm
            .get_or_insert_with(CpAmmPoolExtras::default);

        if extras.token_a_vault.is_none() {
            extras.token_a_vault = state.token_a_vault.map(|pk| pk.to_string());
        }
        if extras.token_b_vault.is_none() {
            extras.token_b_vault = state.token_b_vault.map(|pk| pk.to_string());
        }
        if extras.pool_authority.is_none() {
            extras.pool_authority = state.pool_authority.map(|pk| pk.to_string());
        }
        if extras.token_a_program.is_none() {
            extras.token_a_program = state.token_a_program.map(|pk| pk.to_string());
        }
        if extras.token_b_program.is_none() {
            extras.token_b_program = state.token_b_program.map(|pk| pk.to_string());
        }
        if extras.referral_token_account.is_none() {
            extras.referral_token_account = state.referral_token_account.map(|pk| pk.to_string());
        }
        extras
            .token_a_mint
            .get_or_insert_with(|| state.token_a_mint.to_string());
        extras
            .token_b_mint
            .get_or_insert_with(|| state.token_b_mint.to_string());
    }

    fn warm_components(&self, snapshot: &PoolSnapshot, components: &[HydrationComponent]) {
        if !components.contains(&HydrationComponent::Meta) {
            return;
        }
        let Some(pool_pubkey) = parse_pubkey(&snapshot.pool_id) else {
            return;
        };
        if let Some(hydrator) = self.hydrator.as_ref() {
            hydrator.warm(pool_pubkey);
        }
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if self.cache_stale(&state, req.slot) {
            return Some(quote_status_response(req, QuoteStatus::StaleState));
        }
        if state.hydration_stale(self.config.hydration_meta_ttl_ms) {
            return Some(quote_status_response(req, QuoteStatus::StaleState));
        }
        if !state.meta_ready_for_slot(state.corecast_slot) {
            self.request_meta_refresh(state.pool);
            return Some(quote_status_response(req, QuoteStatus::HydrationPending));
        }
        if !state.swap_enabled(req.slot, req.block_time_u64()) {
            return Some(quote_status_response(req, QuoteStatus::TradingDisabled));
        }
        let mut request = req;
        let capped = self.clamp_request_amount(&state, &request);
        if capped == 0 {
            return None;
        }
        request.amount = capped;
        if referral_missing(&state) {
            let mut missing = build_quote_response(request, 0);
            missing.status = QuoteStatus::MissingReferral;
            return Some(missing);
        }
        if self.partner_mismatch(&state) {
            let mut missing = build_quote_response(request, 0);
            missing.status = QuoteStatus::MissingPartner;
            return Some(missing);
        }
        let simulation = simulate_cp_amm_exact_in(&state, &request, false)?;
        let mut response = build_quote_response(request, simulation.output_amount);
        response.amount_in = simulation.executed_input_amount;
        response.edge_bps = simulation.edge_bps;
        response.min_output_amount = Some(simulation.output_amount);
        response.sqrt_price_limit = Some(simulation.sqrt_price_after);
        response.context.expected_fee_amount = Some(simulation.fee_amount);
        response.context.compute_unit_limit = Some(CP_AMM_DEFAULT_CU_LIMIT);
        response.context.compute_unit_price = Some(CP_AMM_DEFAULT_CU_PRICE);
        response.context.requires_memo = state.referral_token_account.is_some();
        response.context.requires_dont_front = false;
        response.context.host_fee_account = state.referral_token_account;
        self.decorate_template_context(
            &state,
            &mut response.context,
            response.request.input_mint,
            response.request.output_mint,
        );
        Some(response)
    }

    fn quote_exact_out(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactOut {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if self.cache_stale(&state, req.slot) {
            return Some(quote_status_response(req, QuoteStatus::StaleState));
        }
        if state.hydration_stale(self.config.hydration_meta_ttl_ms) {
            return Some(quote_status_response(req, QuoteStatus::StaleState));
        }
        if !state.meta_ready_for_slot(state.corecast_slot) {
            self.request_meta_refresh(state.pool);
            return Some(quote_status_response(req, QuoteStatus::HydrationPending));
        }
        if !state.swap_enabled(req.slot, req.block_time_u64()) {
            return Some(quote_status_response(req, QuoteStatus::TradingDisabled));
        }
        let mut request = req;
        let capped = self.clamp_request_output_amount(&state, &request);
        if capped == 0 {
            return None;
        }
        request.amount = capped;
        if referral_missing(&state) {
            let mut missing = build_quote_response(request, 0);
            missing.status = QuoteStatus::MissingReferral;
            return Some(missing);
        }
        if self.partner_mismatch(&state) {
            let mut missing = build_quote_response(request, 0);
            missing.status = QuoteStatus::MissingPartner;
            return Some(missing);
        }
        let simulation = simulate_cp_amm_exact_out(&state, &request)?;
        let mut response = QuoteResponse {
            amount_in: simulation.executed_input_amount,
            amount_out: simulation.output_amount,
            edge_bps: simulation.edge_bps,
            sqrt_price_limit: Some(simulation.sqrt_price_after),
            min_output_amount: Some(simulation.output_amount),
            status: QuoteStatus::Ok,
            request,
            context: TemplateContext::default(),
        };
        response.context.expected_fee_amount = Some(simulation.fee_amount);
        response.context.compute_unit_limit = Some(CP_AMM_DEFAULT_CU_LIMIT);
        response.context.compute_unit_price = Some(CP_AMM_DEFAULT_CU_PRICE);
        response.context.requires_memo = state.referral_token_account.is_some();
        response.context.requires_dont_front = false;
        response.context.host_fee_account = state.referral_token_account;
        self.decorate_template_context(
            &state,
            &mut response.context,
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
        if state.hydration_stale(self.config.hydration_meta_ttl_ms) {
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                true,
                QuoteStatus::StaleState,
                "hydration_stale",
                None,
            );
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                false,
                QuoteStatus::StaleState,
                "hydration_stale",
                None,
            );
            return None;
        }
        if self.cache_stale(&state, snapshot.slot) {
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                true,
                QuoteStatus::StaleState,
                "cache_stale",
                None,
            );
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                false,
                QuoteStatus::StaleState,
                "cache_stale",
                None,
            );
            return None;
        }
        let snapshot_block_time = normalize_block_time(snapshot.block_time_unix);
        if !state.swap_enabled(snapshot.slot, snapshot_block_time) {
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                true,
                QuoteStatus::TradingDisabled,
                "swap_disabled",
                None,
            );
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                false,
                QuoteStatus::TradingDisabled,
                "swap_disabled",
                None,
            );
            return None;
        }
        if !state.meta_ready_for_slot(state.corecast_slot) {
            self.request_meta_refresh(state.pool);
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                true,
                QuoteStatus::HydrationPending,
                "meta_pending",
                None,
            );
            self.record_snapshot_issue(
                &state,
                snapshot.slot,
                false,
                QuoteStatus::HydrationPending,
                "meta_pending",
                None,
            );
            return None;
        }

        let utilization = self.config.max_swap_utilization_bps;
        let base_probe = match snapshot_probe_amount(state.vault_a_balance, utilization) {
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
        let quote_probe = match snapshot_probe_amount(state.vault_b_balance, utilization) {
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

        let base_to_quote = self.build_snapshot_side(
            &state,
            pool_pubkey,
            snapshot.slot,
            snapshot.block_time_unix,
            true,
            base_probe,
        )?;
        let quote_to_base = self.build_snapshot_side(
            &state,
            pool_pubkey,
            snapshot.slot,
            snapshot.block_time_unix,
            false,
            quote_probe,
        )?;

        Some(PoolBidirectionalQuotes {
            base_to_quote,
            quote_to_base,
        })
    }
}

fn quote_status_response(req: QuoteRequest, status: QuoteStatus) -> QuoteResponse {
    let mut response = build_quote_response(req, 0);
    response.status = status;
    response
}

#[derive(Debug, Clone)]
struct SimulatedSwapQuote {
    pub executed_input_amount: u64,
    #[cfg_attr(not(test), allow(dead_code))]
    pub net_input_amount: u64,
    pub output_amount: u64,
    pub fee_amount: u64,
    #[cfg_attr(not(test), allow(dead_code))]
    pub token_in_fee_amount: u64,
    #[cfg_attr(not(test), allow(dead_code))]
    pub token_out_fee_amount: u64,
    pub sqrt_price_after: u128,
    pub amount_leftover: u64,
    pub edge_bps: f64,
    pub slippage_bps: f64,
}

#[derive(Debug, Clone)]
struct ClmmStep {
    pub amount_out: u64,
    pub net_input_amount: u64,
    #[cfg_attr(not(test), allow(dead_code))]
    pub fee_amount: u64,
    pub sqrt_price_after: u128,
    #[cfg_attr(not(test), allow(dead_code))]
    pub hit_price_limit: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActivationType {
    Slot,
    Timestamp,
}

impl ActivationType {
    fn from_raw(value: Option<u8>) -> Self {
        match value.unwrap_or(0) {
            1 => ActivationType::Timestamp,
            _ => ActivationType::Slot,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PoolStatusFlag {
    Enable = 0,
    Disable = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CollectFeeModeFlag {
    BothToken = 0,
    OnlyB = 1,
}

impl CollectFeeModeFlag {
    fn from_raw(value: Option<u8>) -> Self {
        match value.unwrap_or(0) {
            1 => CollectFeeModeFlag::OnlyB,
            _ => CollectFeeModeFlag::BothToken,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FeeMode {
    fees_on_input: bool,
    has_referral: bool,
}

impl FeeMode {
    fn new(mode: CollectFeeModeFlag, direction: TradeDirection, has_referral: bool) -> Self {
        let fees_on_input = matches!(
            (mode, direction),
            (CollectFeeModeFlag::OnlyB, TradeDirection::BToA)
        );
        Self {
            fees_on_input,
            has_referral,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default)]
struct FeeSplit {
    total_fee: u64,
    trading_fee: u64,
    protocol_fee: u64,
    partner_fee: u64,
    referral_fee: u64,
}

impl FeeSplit {
    fn zero() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug)]
struct FeeConfig {
    kind: FeeConfigKind,
    protocol_fee_percent: u8,
    partner_fee_percent: u8,
    referral_fee_percent: u8,
}

#[derive(Clone, Debug)]
enum FeeConfigKind {
    Pool(PoolFeeModel),
    Legacy(LegacyFeeConfig),
}

#[derive(Clone, Debug)]
struct LegacyFeeConfig {
    trade_fee_numerator: u64,
}

#[derive(Clone, Debug)]
struct PoolFeeModel {
    base_fee: BaseFeeKind,
    dynamic_fee: CpAmmDynamicFeeExtras,
    activation_point: u64,
    pool_version: u8,
    cliff_fee_numerator: u64,
}

#[derive(Clone, Debug)]
enum BaseFeeKind {
    Scheduler(FeeScheduler),
    RateLimiter(FeeRateLimiter),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BaseFeeModeFlag {
    Linear = 0,
    Exponential = 1,
    RateLimiter = 2,
}

impl BaseFeeModeFlag {
    fn from_raw(value: u8) -> Option<Self> {
        match value {
            0 => Some(BaseFeeModeFlag::Linear),
            1 => Some(BaseFeeModeFlag::Exponential),
            2 => Some(BaseFeeModeFlag::RateLimiter),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FeeSchedulerMode {
    Linear,
    Exponential,
}

#[derive(Clone, Debug)]
struct FeeScheduler {
    cliff_fee_numerator: u64,
    number_of_period: u16,
    period_frequency: u64,
    reduction_factor: u64,
    mode: FeeSchedulerMode,
}

#[derive(Clone, Debug)]
struct FeeRateLimiter {
    cliff_fee_numerator: u64,
    fee_increment_bps: u16,
    max_limiter_duration: u32,
    max_fee_bps: u32,
    reference_amount: u64,
}

#[derive(Debug)]
enum FeeComputationError {
    MathOverflow,
    InvalidConfig,
}

type FeeResult<T> = Result<T, FeeComputationError>;

fn referral_missing(state: &CpAmmState) -> bool {
    state.referral_token_account.is_none() && state.fee_config().referral_fee_percent > 0
}

fn direction_mid_price(state: &CpAmmState, direction: TradeDirection) -> Option<f64> {
    match direction {
        TradeDirection::AToB => state.mid_price(),
        TradeDirection::BToA => state
            .mid_price()
            .map(|value| if value == 0.0 { 0.0 } else { 1.0 / value }),
    }
}

fn compute_edge_from_prices(mid_price: f64, execution_price: f64) -> f64 {
    if mid_price <= 0.0
        || execution_price <= 0.0
        || !mid_price.is_finite()
        || !execution_price.is_finite()
    {
        return 0.0;
    }
    ((execution_price / mid_price) - 1.0) * BPS_DENOMINATOR as f64
}

fn scaled_execution_price(
    state: &CpAmmState,
    direction: TradeDirection,
    output_amount: u64,
    executed_input_amount: u64,
) -> Option<f64> {
    if executed_input_amount == 0 || output_amount == 0 {
        return None;
    }
    let base_ratio = output_amount as f64 / executed_input_amount as f64;
    if !base_ratio.is_finite() || base_ratio <= 0.0 {
        return None;
    }
    let scale = state.price_scale(direction);
    let scaled = base_ratio * scale;
    if scaled.is_finite() && scaled > 0.0 {
        Some(scaled)
    } else {
        None
    }
}

fn simulate_cp_amm_exact_in(
    state: &CpAmmState,
    req: &QuoteRequest,
    allow_partial: bool,
) -> Option<SimulatedSwapQuote> {
    let block_time = req.block_time_u64();
    if req.amount == 0 || !state.swap_enabled(req.slot, block_time) {
        return None;
    }

    let direction = resolve_trade_direction(state, &req.input_mint, &req.output_mint)?;
    let (reserve_in, reserve_out) = state.resolve_reserves(&req.input_mint, &req.output_mint)?;
    if reserve_in == 0 || reserve_out == 0 {
        return None;
    }

    let current_point = state.current_point(req.slot, block_time)?;
    let fee_config = state.fee_config();
    let fee_mode = FeeMode::new(
        state.collect_fee_mode(),
        direction,
        state.has_referral_account(),
    );
    let has_partner = state.has_partner();

    let input_fee = state.transfer_fee_for_mint(&req.input_mint);
    let output_fee = state.transfer_fee_for_mint(&req.output_mint);

    let (post_fee_budget, _) = apply_post_fee_amount(req.amount, input_fee)?;
    if post_fee_budget == 0 {
        return None;
    }

    let trade_fee_numerator =
        fee_config.trade_fee_from_included(direction, current_point, post_fee_budget)?;
    let mut fee_split_input = FeeSplit::zero();
    let mut swap_budget = post_fee_budget;
    if fee_mode.fees_on_input {
        let (net, split) = fee_config.apply_fee_to_included_amount(
            post_fee_budget,
            trade_fee_numerator,
            fee_mode.has_referral,
            has_partner,
        )?;
        swap_budget = net;
        fee_split_input = split;
    }
    if swap_budget == 0 {
        return None;
    }

    let step = simulate_clmm_exact_in(state, swap_budget, direction, allow_partial)?;
    let pool_consumed_net = step.net_input_amount;
    if pool_consumed_net == 0 {
        return None;
    }

    let mut pool_consumed_included = pool_consumed_net;
    if fee_mode.fees_on_input {
        let adjusted_numerator =
            fee_config.trade_fee_from_excluded(direction, current_point, pool_consumed_net)?;
        let (included, split) = fee_config.apply_fee_to_excluded_amount(
            pool_consumed_net,
            adjusted_numerator,
            fee_mode.has_referral,
            has_partner,
        )?;
        fee_split_input = split;
        pool_consumed_included = included;
    }

    let (executed_input_amount, token_in_fee_amount) =
        apply_pre_fee_amount(pool_consumed_included, input_fee)?;
    let amount_leftover = req.amount.saturating_sub(executed_input_amount);
    if !allow_partial && amount_leftover > 0 {
        return None;
    }

    let mut gross_output = step.amount_out;
    let mut fee_split = fee_split_input;
    if !fee_mode.fees_on_input {
        let (net_output, split) = fee_config.apply_fee_to_included_amount(
            gross_output,
            trade_fee_numerator,
            fee_mode.has_referral,
            has_partner,
        )?;
        gross_output = net_output;
        fee_split = split;
    }

    let (output_amount, token_out_fee_amount) = apply_post_fee_amount(gross_output, output_fee)?;
    if output_amount == 0 {
        return None;
    }

    let slippage =
        compute_slippage_bps(reserve_in, reserve_out, pool_consumed_net, step.amount_out)
            .unwrap_or(0.0);
    let mid_price = direction_mid_price(state, direction)?;
    let execution_price =
        scaled_execution_price(state, direction, output_amount, executed_input_amount)?;
    let edge_bps = compute_edge_from_prices(mid_price, execution_price);

    Some(SimulatedSwapQuote {
        executed_input_amount,
        net_input_amount: pool_consumed_net,
        output_amount,
        fee_amount: fee_split.total_fee,
        token_in_fee_amount,
        token_out_fee_amount,
        sqrt_price_after: step.sqrt_price_after,
        amount_leftover,
        edge_bps,
        slippage_bps: slippage,
    })
}

fn simulate_cp_amm_exact_out(state: &CpAmmState, req: &QuoteRequest) -> Option<SimulatedSwapQuote> {
    let block_time = req.block_time_u64();
    if req.amount == 0 || !state.swap_enabled(req.slot, block_time) {
        return None;
    }

    let direction = resolve_trade_direction(state, &req.input_mint, &req.output_mint)?;
    let (reserve_in, reserve_out) = state.resolve_reserves(&req.input_mint, &req.output_mint)?;
    if reserve_in == 0 || reserve_out == 0 {
        return None;
    }

    let current_point = state.current_point(req.slot, block_time)?;
    let fee_config = state.fee_config();
    let fee_mode = FeeMode::new(
        state.collect_fee_mode(),
        direction,
        state.has_referral_account(),
    );
    let has_partner = state.has_partner();

    let input_fee = state.transfer_fee_for_mint(&req.input_mint);
    let output_fee = state.transfer_fee_for_mint(&req.output_mint);

    let (desired_after_token_fee, _) = apply_pre_fee_amount(req.amount, output_fee)?;
    if desired_after_token_fee == 0 {
        return None;
    }

    let mut trade_fee_numerator_out = None;
    let mut fee_split_output = FeeSplit::zero();
    let gross_target_out = if fee_mode.fees_on_input {
        desired_after_token_fee
    } else {
        let numerator = fee_config.trade_fee_from_excluded(
            direction,
            current_point,
            desired_after_token_fee,
        )?;
        let (included, split) = fee_config.apply_fee_to_excluded_amount(
            desired_after_token_fee,
            numerator,
            fee_mode.has_referral,
            has_partner,
        )?;
        trade_fee_numerator_out = Some(numerator);
        fee_split_output = split;
        included
    };
    if gross_target_out == 0 || gross_target_out as u128 >= reserve_out {
        return None;
    }

    let step = find_input_for_exact_out(state, direction, gross_target_out)?;
    let pool_input_net = step.net_input_amount;
    if pool_input_net == 0 {
        return None;
    }

    let mut fee_split_input = FeeSplit::zero();
    let mut pool_consumed = pool_input_net;
    if fee_mode.fees_on_input {
        let trade_fee_numerator_input =
            fee_config.trade_fee_from_excluded(direction, current_point, pool_input_net)?;
        let (included_amount, split) = fee_config.apply_fee_to_excluded_amount(
            pool_input_net,
            trade_fee_numerator_input,
            fee_mode.has_referral,
            has_partner,
        )?;
        fee_split_input = split;
        pool_consumed = included_amount;
    }

    let (executed_input_amount, token_in_fee_amount) =
        apply_pre_fee_amount(pool_consumed, input_fee)?;

    let mut post_trade_output = step.amount_out;
    if !fee_mode.fees_on_input {
        let numerator = trade_fee_numerator_out?;
        let (net_output, split) = fee_config.apply_fee_to_included_amount(
            post_trade_output,
            numerator,
            fee_mode.has_referral,
            has_partner,
        )?;
        post_trade_output = net_output;
        fee_split_output = split;
        if post_trade_output < desired_after_token_fee {
            return None;
        }
    }

    let (output_amount, token_out_fee_amount) =
        apply_post_fee_amount(post_trade_output, output_fee)?;
    if output_amount < req.amount {
        return None;
    }

    let fee_split = if fee_mode.fees_on_input {
        fee_split_input
    } else {
        fee_split_output
    };

    let slippage = compute_slippage_bps(reserve_in, reserve_out, pool_input_net, step.amount_out)
        .unwrap_or(0.0);
    let mid_price = direction_mid_price(state, direction)?;
    let execution_price =
        scaled_execution_price(state, direction, output_amount, executed_input_amount)?;
    let edge_bps = compute_edge_from_prices(mid_price, execution_price);

    Some(SimulatedSwapQuote {
        executed_input_amount,
        net_input_amount: pool_input_net,
        output_amount,
        fee_amount: fee_split.total_fee,
        token_in_fee_amount,
        token_out_fee_amount,
        sqrt_price_after: step.sqrt_price_after,
        amount_leftover: 0,
        edge_bps,
        slippage_bps: slippage,
    })
}

fn find_input_for_exact_out(
    state: &CpAmmState,
    direction: TradeDirection,
    target_out: u64,
) -> Option<ClmmStep> {
    let mut hi = target_out.max(1);
    let mut candidate = None;
    for _ in 0..64 {
        let step = simulate_clmm_exact_in(state, hi, direction, false)?;
        if step.amount_out >= target_out {
            candidate = Some(step);
            break;
        }
        if hi >= u64::MAX / 2 {
            return None;
        }
        hi = hi.saturating_mul(2);
    }

    let mut best_input = candidate?;
    let mut low = 1u64;
    let mut high = hi;
    while low < high {
        let mid = low + (high - low) / 2;
        let step = match simulate_clmm_exact_in(state, mid, direction, false) {
            Some(step) => step,
            None => {
                low = mid.saturating_add(1);
                continue;
            }
        };
        if step.amount_out >= target_out {
            high = mid;
            best_input = step;
        } else {
            low = mid.saturating_add(1);
        }
    }
    Some(best_input)
}

fn simulate_clmm_exact_in(
    state: &CpAmmState,
    amount_in: u64,
    direction: TradeDirection,
    allow_price_limit: bool,
) -> Option<ClmmStep> {
    if amount_in == 0 {
        return None;
    }
    let params = state.clmm_params()?;
    if params.liquidity == 0 {
        return None;
    }
    let zero_for_one = matches!(direction, TradeDirection::AToB);
    let sqrt_price_current = math::to_x96(params.sqrt_price_x64);
    let sqrt_price_limit_x64 = if zero_for_one {
        params.sqrt_min_price_x64
    } else {
        params.sqrt_max_price_x64
    };
    let sqrt_price_limit = math::to_x96(sqrt_price_limit_x64);

    if zero_for_one && sqrt_price_limit >= sqrt_price_current {
        return None;
    }
    if !zero_for_one && sqrt_price_limit <= sqrt_price_current {
        return None;
    }

    let step = math::compute_swap_step(
        sqrt_price_current,
        sqrt_price_limit,
        params.liquidity,
        amount_in as u128,
        0,
        zero_for_one,
    )?;
    if step.amount_out == 0 {
        return None;
    }
    let consumed = step.amount_in.saturating_add(step.fee_amount);
    let hit_price_cap = step.sqrt_price_next == sqrt_price_limit;
    if !allow_price_limit && hit_price_cap && consumed < amount_in as u128 {
        return None;
    }

    let sqrt_price_after = from_x96(step.sqrt_price_next)?;
    Some(ClmmStep {
        amount_out: step.amount_out.min(u64::MAX as u128) as u64,
        net_input_amount: step.amount_in.min(u64::MAX as u128) as u64,
        fee_amount: step.fee_amount.min(u64::MAX as u128) as u64,
        sqrt_price_after,
        hit_price_limit: hit_price_cap,
    })
}

fn from_x96(value: U256) -> Option<u128> {
    let shifted = value >> 32;
    shifted.try_into().ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TradeDirection {
    AToB,
    BToA,
}

fn resolve_trade_direction(
    state: &CpAmmState,
    input_mint: &Pubkey,
    output_mint: &Pubkey,
) -> Option<TradeDirection> {
    if input_mint == &state.token_a_mint && output_mint == &state.token_b_mint {
        Some(TradeDirection::AToB)
    } else if input_mint == &state.token_b_mint && output_mint == &state.token_a_mint {
        Some(TradeDirection::BToA)
    } else {
        None
    }
}

fn compute_slippage_bps(
    reserve_in: u128,
    reserve_out: u128,
    amount_in: u64,
    amount_out: u64,
) -> Option<f64> {
    if reserve_in == 0 || reserve_out == 0 || amount_in == 0 || amount_out == 0 {
        return None;
    }

    let spot_price = reserve_out as f64 / reserve_in as f64;
    if !spot_price.is_finite() || spot_price <= 0.0 {
        return None;
    }
    let execution_price = amount_out as f64 / amount_in as f64;
    if !execution_price.is_finite() || execution_price <= 0.0 {
        return None;
    }

    Some(((execution_price / spot_price) - 1.0) * BPS_DENOMINATOR as f64)
}

fn token_program_for_mint(state: &CpAmmState, mint: &Pubkey) -> Pubkey {
    if *mint == state.token_a_mint {
        state.token_a_program.unwrap_or(*SPL_TOKEN_PROGRAM_PUBKEY)
    } else if *mint == state.token_b_mint {
        state.token_b_program.unwrap_or(*SPL_TOKEN_PROGRAM_PUBKEY)
    } else {
        *SPL_TOKEN_PROGRAM_PUBKEY
    }
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

fn current_unix_timestamp() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn normalize_block_time(timestamp: Option<i64>) -> Option<u64> {
    timestamp.and_then(|value| if value >= 0 { Some(value as u64) } else { None })
}

fn needs_hydration(extras: &CpAmmPoolExtras) -> bool {
    extras.token_a_mint.is_none()
        || extras.token_b_mint.is_none()
        || extras.token_a_vault.is_none()
        || extras.token_b_vault.is_none()
        || extras.sqrt_price_x64.is_none()
        || extras.liquidity.is_none()
        || extras.sqrt_min_price_x64.is_none()
        || extras.sqrt_max_price_x64.is_none()
        || extras.collect_fee_mode.is_none()
        || extras.activation_point.is_none()
        || extras.activation_type.is_none()
        || extras.pool_status.is_none()
        || extras.pool_fees.is_none()
}

fn hydration_meta_expired(extras: &CpAmmPoolExtras, ttl_ms: u64) -> bool {
    if ttl_ms == 0 {
        return false;
    }
    match extras.hydrated_at {
        Some(instant) => instant.elapsed().as_millis() as u64 >= ttl_ms,
        None => true,
    }
}

fn apply_hydrated_meta(extras: &mut CpAmmPoolExtras, meta: &HydratedPoolMeta, force_refresh: bool) {
    if force_refresh || extras.token_a_mint.is_none() {
        extras.token_a_mint = Some(meta.token_a_mint.to_string());
    }
    if force_refresh || extras.token_b_mint.is_none() {
        extras.token_b_mint = Some(meta.token_b_mint.to_string());
    }
    if force_refresh || extras.token_a_vault.is_none() {
        extras.token_a_vault = Some(meta.token_a_vault.to_string());
    }
    if force_refresh || extras.token_b_vault.is_none() {
        extras.token_b_vault = Some(meta.token_b_vault.to_string());
    }
    if force_refresh || extras.whitelisted_vault.is_none() {
        extras.whitelisted_vault = optional_pubkey_string(meta.whitelisted_vault);
    }
    if force_refresh || extras.partner.is_none() {
        extras.partner = optional_pubkey_string(meta.partner);
    }
    if force_refresh || extras.sqrt_price_x64.is_none() {
        extras.sqrt_price_x64 = Some(meta.sqrt_price_x64);
    }
    if force_refresh || extras.liquidity.is_none() {
        extras.liquidity = Some(meta.liquidity);
    }
    if force_refresh || extras.sqrt_min_price_x64.is_none() {
        extras.sqrt_min_price_x64 = Some(meta.sqrt_min_price_x64);
    }
    if force_refresh || extras.sqrt_max_price_x64.is_none() {
        extras.sqrt_max_price_x64 = Some(meta.sqrt_max_price_x64);
    }
    if force_refresh || extras.permanent_lock_liquidity.is_none() {
        extras.permanent_lock_liquidity = Some(meta.permanent_lock_liquidity);
    }
    if force_refresh || extras.collect_fee_mode.is_none() {
        extras.collect_fee_mode = Some(meta.collect_fee_mode);
    }
    if force_refresh || extras.activation_point.is_none() {
        extras.activation_point = Some(meta.activation_point);
    }
    if force_refresh || extras.activation_type.is_none() {
        extras.activation_type = Some(meta.activation_type);
    }
    if force_refresh || extras.pool_status.is_none() {
        extras.pool_status = Some(meta.pool_status);
    }
    if force_refresh || extras.pool_type.is_none() {
        extras.pool_type = Some(meta.pool_type);
    }
    if force_refresh || extras.pool_version.is_none() {
        extras.pool_version = Some(meta.pool_version);
    }
    if force_refresh || extras.token_a_flag.is_none() {
        extras.token_a_flag = Some(meta.token_a_flag);
    }
    if force_refresh || extras.token_b_flag.is_none() {
        extras.token_b_flag = Some(meta.token_b_flag);
    }
    if force_refresh || extras.pool_fees.is_none() {
        extras.pool_fees = Some(meta.pool_fees.clone());
    }
    extras.hydrated_at = Some(meta.hydrated_at);
    extras.hydrated_slot = Some(meta.hydrated_slot);
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    token_a_mint: Pubkey,
    token_b_mint: Pubkey,
    token_a_vault: Pubkey,
    token_b_vault: Pubkey,
    whitelisted_vault: Pubkey,
    partner: Pubkey,
    sqrt_price_x64: u128,
    liquidity: u128,
    sqrt_min_price_x64: u128,
    sqrt_max_price_x64: u128,
    permanent_lock_liquidity: u128,
    collect_fee_mode: u8,
    activation_point: u64,
    activation_type: u8,
    pool_status: u8,
    pool_type: u8,
    pool_version: u8,
    token_a_flag: u8,
    token_b_flag: u8,
    pool_fees: CpAmmPoolFeesExtras,
    hydrated_at: Instant,
    hydrated_slot: u64,
}

struct CpAmmPoolHydrator {
    hydrator: AsyncHydrator<Pubkey, HydratedPoolMeta>,
    endpoint_count: usize,
    retry_backoff: Duration,
    last_attempts: DashMap<Pubkey, Instant>,
}

impl CpAmmPoolHydrator {
    fn new(config: &CpAmmAdapterConfig) -> Option<Self> {
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
        let hydrator = AsyncHydrator::new("cp_amm_pool", endpoint_count, move |pool| {
            fetch_cp_amm_meta(fetch_clients.as_ref(), pool)
                .map(|meta| {
                    let slot = Some(meta.hydrated_slot);
                    let fetched_at = meta.hydrated_at;
                    HydratedValue::with_timestamp(meta, slot, fetched_at)
                })
        });

        Some(Self {
            hydrator,
            endpoint_count,
            retry_backoff: Duration::from_millis(config.hydration_retry_ms.max(1)),
            last_attempts: DashMap::new(),
        })
    }

    fn client_count(&self) -> usize {
        self.endpoint_count
    }

    fn hydrate(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        if let Some(meta) = self.hydrator.cached(&pool) {
            if meta.hydrated_at.elapsed() >= self.retry_backoff {
                if self.should_schedule(pool) {
                    self.hydrator.schedule(pool);
                }
            }
            return Some(meta);
        }
        if self.should_schedule(pool) {
            self.hydrator.schedule(pool);
        }
        None
    }

    fn warm(&self, pool: Pubkey) {
        if self.should_schedule(pool) {
            self.hydrator.schedule(pool);
        }
    }

    fn refresh(&self, pool: Pubkey) {
        self.hydrator.refresh(pool);
    }

    fn should_schedule(&self, pool: Pubkey) -> bool {
        let now = Instant::now();
        if let Some(last) = self.last_attempts.get(&pool) {
            if now.duration_since(*last.value()) < self.retry_backoff {
                return false;
            }
        }
        self.last_attempts.insert(pool, now);
        true
    }
}

fn fetch_cp_amm_meta(clients: &[Arc<RpcClient>], pool: Pubkey) -> Option<HydratedPoolMeta> {
    for client in clients {
        match client.get_account(&pool) {
            Ok(account) => {
                if let Some(mut meta) = parse_pool_account(&account.data) {
                    meta.hydrated_slot = client.get_slot().unwrap_or_default();
                    meta.hydrated_at = Instant::now();
                    return Some(meta);
                } else {
                    warn!(
                        target = "adapter::cp_amm",
                        pool = %pool,
                        "unexpected cp_amm pool account layout"
                    );
                }
            }
            Err(error) => {
                warn!(
                    target = "adapter::cp_amm",
                    pool = %pool,
                    %error,
                    "failed to fetch cp_amm pool account"
                );
            }
        }
    }

    None
}

#[derive(Clone, Debug)]
struct MintMetadata {
    decimals: Option<u8>,
    program_id: Pubkey,
    hydrated_at: Instant,
}

struct MintMetadataHydrator {
    hydrator: AsyncHydrator<Pubkey, MintMetadata>,
    retry_backoff: Duration,
    last_attempts: DashMap<Pubkey, Instant>,
}

impl MintMetadataHydrator {
    fn new(config: &CpAmmAdapterConfig) -> Option<Self> {
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
        let hydrator = AsyncHydrator::new("mint_meta", endpoint_count, move |mint| {
            fetch_mint_metadata(fetch_clients.as_ref(), mint)
                .map(|meta| HydratedValue::new(meta, None))
        });

        Some(Self {
            hydrator,
            retry_backoff: Duration::from_millis(config.hydration_retry_ms.max(1)),
            last_attempts: DashMap::new(),
        })
    }

    fn hydrate(&self, mint: Pubkey) -> Option<MintMetadata> {
        if let Some(meta) = self.hydrator.cached(&mint) {
            if meta.hydrated_at.elapsed() >= self.retry_backoff {
                if self.should_schedule(mint) {
                    self.hydrator.schedule(mint);
                }
            }
            return Some(meta);
        }
        if self.should_schedule(mint) {
            self.hydrator.schedule(mint);
        }
        None
    }

    fn should_schedule(&self, mint: Pubkey) -> bool {
        let now = Instant::now();
        if let Some(last) = self.last_attempts.get(&mint) {
            if now.duration_since(*last.value()) < self.retry_backoff {
                return false;
            }
        }
        self.last_attempts.insert(mint, now);
        true
    }
}

fn fetch_mint_metadata(clients: &[Arc<RpcClient>], mint: Pubkey) -> Option<MintMetadata> {
    for client in clients {
        match client.get_account(&mint) {
            Ok(account) => {
                let owner = account.owner;
                let decimals = parse_mint_decimals(&owner, &account.data);
                if decimals.is_none() {
                    warn!(
                        target = "adapter::cp_amm",
                        mint = %mint,
                        program = %owner,
                        "unsupported mint program for metadata hydration"
                    );
                }
                return Some(MintMetadata {
                    decimals,
                    program_id: owner,
                    hydrated_at: Instant::now(),
                });
            }
            Err(error) => {
                warn!(
                    target = "adapter::cp_amm",
                    mint = %mint,
                    %error,
                    "failed to fetch mint account for metadata"
                );
            }
        }
    }

    None
}

fn parse_mint_decimals(owner: &Pubkey, data: &[u8]) -> Option<u8> {
    if owner == &*SPL_TOKEN_PROGRAM_PUBKEY {
        return data.get(44).copied();
    }
    if owner == &*SPL_TOKEN_2022_PROGRAM_PUBKEY {
        return StateWithExtensionsOwned::<SplToken2022Mint>::unpack(data.to_vec())
            .ok()
            .map(|mint| mint.base.decimals);
    }
    None
}

const POOL_FEES_STRUCT_LEN: usize = 160;
const PUBKEY_BYTES: usize = 32;
const U64_BYTES: usize = 8;
const U128_BYTES: usize = 16;
const CP_AMM_POOL_ACCOUNT_HEADER_LEN: usize = 8 + POOL_FEES_STRUCT_LEN;
const CP_AMM_POOL_ACCOUNT_MIN_LEN: usize = CP_AMM_POOL_ACCOUNT_HEADER_LEN
    + (PUBKEY_BYTES * 6)
    + (U128_BYTES * 6)
    + (U64_BYTES * 5)
    + 8
    + (32 * 2);

fn parse_pool_account(data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < CP_AMM_POOL_ACCOUNT_MIN_LEN {
        return None;
    }

    let mut cursor = 8; // anchor account discriminator
    let pool_fees = read_pool_fees_struct(data, &mut cursor)?;
    let token_a_mint = read_pubkey_from_slice(data, &mut cursor)?;
    let token_b_mint = read_pubkey_from_slice(data, &mut cursor)?;
    let token_a_vault = read_pubkey_from_slice(data, &mut cursor)?;
    let token_b_vault = read_pubkey_from_slice(data, &mut cursor)?;
    let whitelisted_vault = read_pubkey_from_slice(data, &mut cursor)?;
    let partner = read_pubkey_from_slice(data, &mut cursor)?;
    let liquidity = read_u128_from_slice(data, &mut cursor)?;
    let _padding = read_u128_from_slice(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, U64_BYTES.saturating_mul(4))?; // fee accruals
    let sqrt_min_price_x64 = read_u128_from_slice(data, &mut cursor)?;
    let sqrt_max_price_x64 = read_u128_from_slice(data, &mut cursor)?;
    let sqrt_price_x64 = read_u128_from_slice(data, &mut cursor)?;
    let activation_point = read_u64_from_slice(data, &mut cursor)?;
    let activation_type = read_u8_from_slice(data, &mut cursor)?;
    let pool_status = read_u8_from_slice(data, &mut cursor)?;
    let token_a_flag = read_u8_from_slice(data, &mut cursor)?;
    let token_b_flag = read_u8_from_slice(data, &mut cursor)?;
    let collect_fee_mode = read_u8_from_slice(data, &mut cursor)?;
    let pool_type = read_u8_from_slice(data, &mut cursor)?;
    let pool_version = read_u8_from_slice(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 1)?; // padding
    skip_bytes(data, &mut cursor, 32 * 2)?; // fee per liquidity accumulators
    let permanent_lock_liquidity = read_u128_from_slice(data, &mut cursor)?;

    Some(HydratedPoolMeta {
        token_a_mint,
        token_b_mint,
        token_a_vault,
        token_b_vault,
        whitelisted_vault,
        partner,
        sqrt_price_x64,
        liquidity,
        sqrt_min_price_x64,
        sqrt_max_price_x64,
        permanent_lock_liquidity,
        collect_fee_mode,
        activation_point,
        activation_type,
        pool_status,
        pool_type,
        pool_version,
        token_a_flag,
        token_b_flag,
        pool_fees,
        hydrated_at: Instant::now(),
        hydrated_slot: 0,
    })
}

fn read_pubkey_from_slice(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    let end = cursor.checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

fn read_u64_from_slice(data: &[u8], cursor: &mut usize) -> Option<u64> {
    let end = cursor.checked_add(U64_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; U64_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u64::from_le_bytes(bytes))
}

fn read_u128_from_slice(data: &[u8], cursor: &mut usize) -> Option<u128> {
    let end = cursor.checked_add(U128_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; U128_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u128::from_le_bytes(bytes))
}

fn read_u32_from_slice(data: &[u8], cursor: &mut usize) -> Option<u32> {
    let end = cursor.checked_add(4)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u32::from_le_bytes(bytes))
}

fn read_u16_from_slice(data: &[u8], cursor: &mut usize) -> Option<u16> {
    let end = cursor.checked_add(2)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u16::from_le_bytes(bytes))
}

fn read_u8_from_slice(data: &[u8], cursor: &mut usize) -> Option<u8> {
    let byte = *data.get(*cursor)?;
    *cursor = cursor.checked_add(1)?;
    Some(byte)
}

fn read_bytes<const N: usize>(data: &[u8], cursor: &mut usize) -> Option<[u8; N]> {
    let end = cursor.checked_add(N)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; N];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(bytes)
}

fn skip_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<()> {
    let end = cursor.checked_add(len)?;
    data.get(*cursor..end)?;
    *cursor = end;
    Some(())
}

fn read_base_fee_struct(data: &[u8], cursor: &mut usize) -> Option<CpAmmBaseFeeExtras> {
    let mut base = CpAmmBaseFeeExtras::default();
    base.cliff_fee_numerator = read_u64_from_slice(data, cursor)?;
    base.base_fee_mode = read_u8_from_slice(data, cursor)?;
    skip_bytes(data, cursor, 5)?; // padding
    base.first_factor = read_u16_from_slice(data, cursor)?;
    base.second_factor = read_bytes::<8>(data, cursor)?;
    base.third_factor = read_u64_from_slice(data, cursor)?;
    skip_bytes(data, cursor, 8)?; // padding_1
    Some(base)
}

fn read_dynamic_fee_struct(data: &[u8], cursor: &mut usize) -> Option<CpAmmDynamicFeeExtras> {
    let mut dyn_fee = CpAmmDynamicFeeExtras::default();
    dyn_fee.initialized = read_u8_from_slice(data, cursor)?;
    skip_bytes(data, cursor, 7)?; // padding
    dyn_fee.max_volatility_accumulator = read_u32_from_slice(data, cursor)?;
    dyn_fee.variable_fee_control = read_u32_from_slice(data, cursor)?;
    dyn_fee.bin_step = read_u16_from_slice(data, cursor)?;
    dyn_fee.filter_period = read_u16_from_slice(data, cursor)?;
    dyn_fee.decay_period = read_u16_from_slice(data, cursor)?;
    dyn_fee.reduction_factor = read_u16_from_slice(data, cursor)?;
    dyn_fee.last_update_timestamp = read_u64_from_slice(data, cursor)?;
    dyn_fee.bin_step_u128 = read_u128_from_slice(data, cursor)?;
    dyn_fee.sqrt_price_reference = read_u128_from_slice(data, cursor)?;
    dyn_fee.volatility_accumulator = read_u128_from_slice(data, cursor)?;
    dyn_fee.volatility_reference = read_u128_from_slice(data, cursor)?;
    Some(dyn_fee)
}

fn read_pool_fees_struct(data: &[u8], cursor: &mut usize) -> Option<CpAmmPoolFeesExtras> {
    let mut fees = CpAmmPoolFeesExtras::default();
    fees.base_fee = read_base_fee_struct(data, cursor)?;
    fees.protocol_fee_percent = read_u8_from_slice(data, cursor)?;
    fees.partner_fee_percent = read_u8_from_slice(data, cursor)?;
    fees.referral_fee_percent = read_u8_from_slice(data, cursor)?;
    skip_bytes(data, cursor, 5)?; // padding
    fees.dynamic_fee = read_dynamic_fee_struct(data, cursor)?;
    skip_bytes(data, cursor, 16)?; // padding_1 (two u64)
    Some(fees)
}

#[derive(Debug, Clone)]
pub struct CpAmmState {
    pub pool: Pubkey,
    pub pool_authority: Option<Pubkey>,
    pub token_a_vault: Option<Pubkey>,
    pub token_b_vault: Option<Pubkey>,
    pub whitelisted_vault: Option<Pubkey>,
    pub partner: Option<Pubkey>,
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    pub token_a_program: Option<Pubkey>,
    pub token_b_program: Option<Pubkey>,
    pub token_a_decimals: Option<u8>,
    pub token_b_decimals: Option<u8>,
    pub token_a_transfer_fee: Option<TokenTransferFee>,
    pub token_b_transfer_fee: Option<TokenTransferFee>,
    pub amp: Option<u64>,
    pub lp_fee_bps: Option<u16>,
    pub protocol_fee_bps: Option<u16>,
    pub referral_bps: Option<u16>,
    pub referral_token_account: Option<Pubkey>,
    pub collect_fee_mode: Option<u8>,
    pub activation_point: Option<u64>,
    pub activation_type: Option<u8>,
    pub pool_status: Option<u8>,
    pub pool_type: Option<u8>,
    pub pool_version: Option<u8>,
    pub token_a_flag: Option<u8>,
    pub token_b_flag: Option<u8>,
    pub permanent_lock_liquidity: Option<u128>,
    pub pool_fees: Option<CpAmmPoolFeesExtras>,
    pub sqrt_price_x64: Option<u128>,
    pub liquidity: Option<u128>,
    pub sqrt_min_price_x64: Option<u128>,
    pub sqrt_max_price_x64: Option<u128>,
    pub vault_a_balance: u128,
    pub vault_b_balance: u128,
    pub corecast_slot: u64,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
    pub last_hydrated_at: Option<Instant>,
    pub hydrated_meta_slot: Option<u64>,
    pub last_block_time_unix: Option<u64>,
}

impl CpAmmState {
    pub fn mid_price(&self) -> Option<f64> {
        if let Some(sqrt_price_x64) = self.sqrt_price_x64 {
            let sqrt_price = sqrt_price_x64 as f64 / Q64_FACTOR;
            if sqrt_price.is_finite() && sqrt_price > 0.0 {
                let mut price = sqrt_price * sqrt_price;
                price *= self.decimals_scale();
                if price.is_finite() && price > 0.0 {
                    return Some(price);
                }
            }
        }
        if self.vault_a_balance == 0 {
            None
        } else {
            Some(self.vault_b_balance as f64 / self.vault_a_balance as f64)
        }
    }

    fn decimals_scale(&self) -> f64 {
        match (self.token_a_decimals, self.token_b_decimals) {
            (Some(a), Some(b)) => 10_f64.powi((b as i32) - (a as i32)),
            _ => 1.0,
        }
    }

    fn price_scale(&self, direction: TradeDirection) -> f64 {
        let base = self.decimals_scale();
        match direction {
            TradeDirection::AToB => base,
            TradeDirection::BToA => {
                if base == 0.0 {
                    1.0
                } else {
                    1.0 / base
                }
            }
        }
    }

    fn resolve_reserves(&self, input_mint: &Pubkey, output_mint: &Pubkey) -> Option<(u128, u128)> {
        if *input_mint == self.token_a_mint && *output_mint == self.token_b_mint {
            Some((self.vault_a_balance, self.vault_b_balance))
        } else if *input_mint == self.token_b_mint && *output_mint == self.token_a_mint {
            Some((self.vault_b_balance, self.vault_a_balance))
        } else {
            None
        }
    }

    fn total_fee_bps(&self) -> u64 {
        self.fee_config().trade_fee_bps()
    }

    fn clmm_params(&self) -> Option<ClmmParams> {
        Some(ClmmParams {
            liquidity: self.liquidity?,
            sqrt_price_x64: self.sqrt_price_x64?,
            sqrt_min_price_x64: self.sqrt_min_price_x64?,
            sqrt_max_price_x64: self.sqrt_max_price_x64?,
        })
    }

    fn transfer_fee_for_mint(&self, mint: &Pubkey) -> Option<TokenTransferFee> {
        if *mint == self.token_a_mint {
            self.token_a_transfer_fee
        } else if *mint == self.token_b_mint {
            self.token_b_transfer_fee
        } else {
            None
        }
    }

    fn meta_ready(&self) -> bool {
        self.sqrt_price_x64.is_some()
            && self.liquidity.is_some()
            && self.sqrt_min_price_x64.is_some()
            && self.sqrt_max_price_x64.is_some()
    }

    fn meta_ready_for_slot(&self, slot: u64) -> bool {
        if !self.meta_ready() {
            return false;
        }
        self.hydrated_meta_slot
            .map(|hydrated_slot| hydrated_slot >= slot)
            .unwrap_or(false)
    }

    fn hydration_stale(&self, ttl_ms: u64) -> bool {
        if ttl_ms == 0 {
            return false;
        }
        match self.last_hydrated_at {
            Some(instant) => instant.elapsed().as_millis() as u64 > ttl_ms,
            None => true,
        }
    }

    fn has_referral_account(&self) -> bool {
        self.referral_token_account.is_some()
    }

    fn has_partner(&self) -> bool {
        matches!(self.partner, Some(pk) if pk != Pubkey::default())
    }

    fn activation_kind(&self) -> ActivationType {
        ActivationType::from_raw(self.activation_type)
    }

    fn current_point(&self, slot: u64, block_time: Option<u64>) -> Option<u64> {
        match self.activation_kind() {
            ActivationType::Slot => Some(slot),
            ActivationType::Timestamp => block_time
                .or(self.last_block_time_unix)
                .or_else(current_unix_timestamp)
                .or(Some(slot)),
        }
    }

    fn collect_fee_mode(&self) -> CollectFeeModeFlag {
        CollectFeeModeFlag::from_raw(self.collect_fee_mode)
    }

    fn pool_status_flag(&self) -> PoolStatusFlag {
        match self.pool_status.unwrap_or(PoolStatusFlag::Enable as u8) {
            1 => PoolStatusFlag::Disable,
            _ => PoolStatusFlag::Enable,
        }
    }

    fn fee_config(&self) -> FeeConfig {
        FeeConfig::from_state(self)
    }

    fn swap_enabled(&self, slot: u64, block_time: Option<u64>) -> bool {
        if self.pool_status_flag() != PoolStatusFlag::Enable {
            return false;
        }
        let activation_point = self.activation_point.unwrap_or(0);
        if activation_point == 0 {
            return true;
        }
        match self.activation_kind() {
            ActivationType::Slot => slot >= activation_point,
            ActivationType::Timestamp => block_time
                .or(self.last_block_time_unix)
                .or_else(current_unix_timestamp)
                .map(|ts| ts >= activation_point)
                .unwrap_or(true),
        }
    }
}

impl FeeConfig {
    fn from_state(state: &CpAmmState) -> Self {
        if let Some(mut fees) = state.pool_fees.clone() {
            let pool_version = state.pool_version.unwrap_or(CURRENT_POOL_VERSION);
            if sanitize_pool_fees(&mut fees, pool_version) {
                if let Some(base_fee) = BaseFeeKind::from_extras(&fees.base_fee) {
                    if fee_schedule_valid(
                        &base_fee,
                        &fees.dynamic_fee,
                        state.activation_point.unwrap_or(0),
                        pool_version,
                    ) {
                        return Self {
                            kind: FeeConfigKind::Pool(PoolFeeModel {
                                base_fee,
                                dynamic_fee: fees.dynamic_fee.clone(),
                                activation_point: state.activation_point.unwrap_or(0),
                                pool_version,
                                cliff_fee_numerator: fees
                                    .base_fee
                                    .cliff_fee_numerator
                                    .min(FEE_DENOMINATOR),
                            }),
                            protocol_fee_percent: fees.protocol_fee_percent,
                            partner_fee_percent: fees.partner_fee_percent,
                            referral_fee_percent: fees.referral_fee_percent,
                        };
                    } else {
                        debug!(
                            target = "adapter::cp_amm",
                            pool = %state.pool,
                            "cp_amm fee schedule failed validation; using fallback fees"
                        );
                    }
                } else {
                    debug!(
                        target = "adapter::cp_amm",
                        pool = %state.pool,
                        "cp_amm fee schedule missing base mode; using fallback fees"
                    );
                }
            } else {
                debug!(
                    target = "adapter::cp_amm",
                    pool = %state.pool,
                    "cp_amm fee schedule rejected; using fallback fees"
                );
            }
        }

        let fallback_trade_bps = state
            .lp_fee_bps
            .unwrap_or(0)
            .saturating_add(state.protocol_fee_bps.unwrap_or(0))
            .saturating_add(state.referral_bps.unwrap_or(0))
            as u64;
        let trade_fee_numerator = bps_to_numerator(fallback_trade_bps);
        let protocol_bps = state.protocol_fee_bps.unwrap_or(0) as u64;
        let referral_bps = state.referral_bps.unwrap_or(0) as u64;
        let protocol_percent = percent_from_components(protocol_bps, fallback_trade_bps);
        let referral_percent = if protocol_bps == 0 {
            0
        } else {
            percent_from_components(referral_bps, protocol_bps)
        };
        Self {
            kind: FeeConfigKind::Legacy(LegacyFeeConfig {
                trade_fee_numerator,
            }),
            protocol_fee_percent: protocol_percent,
            partner_fee_percent: 0,
            referral_fee_percent: referral_percent,
        }
    }

    fn trade_fee_bps(&self) -> u64 {
        match &self.kind {
            FeeConfigKind::Pool(model) => numerator_to_bps(model.cliff_fee_numerator),
            FeeConfigKind::Legacy(config) => numerator_to_bps(config.trade_fee_numerator),
        }
    }

    fn trade_fee_from_included(
        &self,
        direction: TradeDirection,
        current_point: u64,
        included_amount: u64,
    ) -> Option<u64> {
        match &self.kind {
            FeeConfigKind::Pool(model) => model
                .total_fee_from_included(direction, current_point, included_amount)
                .ok(),
            FeeConfigKind::Legacy(config) => Some(config.trade_fee_numerator),
        }
    }

    fn trade_fee_from_excluded(
        &self,
        direction: TradeDirection,
        current_point: u64,
        excluded_amount: u64,
    ) -> Option<u64> {
        match &self.kind {
            FeeConfigKind::Pool(model) => model
                .total_fee_from_excluded(direction, current_point, excluded_amount)
                .ok(),
            FeeConfigKind::Legacy(config) => Some(config.trade_fee_numerator),
        }
    }

    fn apply_fee_to_included_amount(
        &self,
        included_amount: u64,
        trade_fee_numerator: u64,
        has_referral: bool,
        has_partner: bool,
    ) -> Option<(u64, FeeSplit)> {
        if included_amount == 0 || trade_fee_numerator == 0 {
            return Some((included_amount, FeeSplit::zero()));
        }
        let (net, fee_amount) = compute_excluded_fee_amount(trade_fee_numerator, included_amount)?;
        let split = self.split_fee(fee_amount, has_referral, has_partner);
        Some((net, split))
    }

    fn apply_fee_to_excluded_amount(
        &self,
        excluded_amount: u64,
        trade_fee_numerator: u64,
        has_referral: bool,
        has_partner: bool,
    ) -> Option<(u64, FeeSplit)> {
        if excluded_amount == 0 || trade_fee_numerator == 0 {
            return Some((excluded_amount, FeeSplit::zero()));
        }
        let (included, fee_amount) =
            compute_included_fee_amount(trade_fee_numerator, excluded_amount)?;
        let split = self.split_fee(fee_amount, has_referral, has_partner);
        Some((included, split))
    }

    fn split_fee(&self, fee_amount: u64, has_referral: bool, has_partner: bool) -> FeeSplit {
        if fee_amount == 0 {
            return FeeSplit::zero();
        }
        let protocol_fee = mul_div_floor_u64(fee_amount, self.protocol_fee_percent.into(), 100);
        let trading_fee = fee_amount.saturating_sub(protocol_fee);
        let referral_fee = if has_referral {
            mul_div_floor_u64(protocol_fee, self.referral_fee_percent.into(), 100)
        } else {
            0
        };
        let protocol_after_referral = protocol_fee.saturating_sub(referral_fee);
        let partner_fee = if has_partner {
            mul_div_floor_u64(
                protocol_after_referral,
                self.partner_fee_percent.into(),
                100,
            )
        } else {
            0
        };
        let protocol_fee_final = protocol_after_referral.saturating_sub(partner_fee);
        FeeSplit {
            total_fee: fee_amount,
            trading_fee,
            protocol_fee: protocol_fee_final,
            partner_fee,
            referral_fee,
        }
    }
}

fn sanitize_pool_fees(fees: &mut CpAmmPoolFeesExtras, pool_version: u8) -> bool {
    let max_fee_numerator =
        max_fee_numerator_for_version(pool_version).unwrap_or(MAX_FEE_NUMERATOR_V1);
    if max_fee_numerator < MIN_FEE_NUMERATOR {
        return false;
    }

    if fees.base_fee.cliff_fee_numerator == 0 {
        fees.base_fee.cliff_fee_numerator = MIN_FEE_NUMERATOR;
    }
    fees.base_fee.cliff_fee_numerator = fees
        .base_fee
        .cliff_fee_numerator
        .clamp(MIN_FEE_NUMERATOR, max_fee_numerator);

    fees.protocol_fee_percent = fees.protocol_fee_percent.min(100);
    fees.partner_fee_percent = fees.partner_fee_percent.min(100);
    fees.referral_fee_percent = fees.referral_fee_percent.min(100);

    if fees.dynamic_fee.initialized == 0 {
        fees.dynamic_fee.volatility_accumulator = 0;
        fees.dynamic_fee.volatility_reference = 0;
    } else if fees.dynamic_fee.max_volatility_accumulator > 0 {
        let cap = u128::from(fees.dynamic_fee.max_volatility_accumulator);
        if fees.dynamic_fee.volatility_accumulator > cap {
            fees.dynamic_fee.volatility_accumulator = cap;
        }
        if fees.dynamic_fee.volatility_reference > cap {
            fees.dynamic_fee.volatility_reference = cap;
        }
    }

    match BaseFeeModeFlag::from_raw(fees.base_fee.base_fee_mode) {
        Some(mode) => match mode {
            BaseFeeModeFlag::Linear | BaseFeeModeFlag::Exponential => {
                sanitize_scheduler_extras(&mut fees.base_fee, mode)
            }
            BaseFeeModeFlag::RateLimiter => {
                sanitize_rate_limiter_extras(&mut fees.base_fee, pool_version)
            }
        },
        None => false,
    }
}

fn sanitize_scheduler_extras(base: &mut CpAmmBaseFeeExtras, mode: BaseFeeModeFlag) -> bool {
    match mode {
        BaseFeeModeFlag::Linear => {
            if base.third_factor > base.cliff_fee_numerator {
                return false;
            }
            true
        }
        BaseFeeModeFlag::Exponential => {
            base.third_factor = base.third_factor.min(10_000);
            true
        }
        BaseFeeModeFlag::RateLimiter => false,
    }
}

fn sanitize_rate_limiter_extras(base: &mut CpAmmBaseFeeExtras, pool_version: u8) -> bool {
    if base.first_factor == 0 {
        return false;
    }

    let mut max_fee_bytes = [0u8; 4];
    max_fee_bytes.copy_from_slice(&base.second_factor[4..8]);
    let mut max_fee_bps = u32::from_le_bytes(max_fee_bytes);
    if max_fee_bps == 0 {
        return false;
    }

    let max_allowed_bps = max_fee_bps_for_version(pool_version) as u32;
    if max_fee_bps > max_allowed_bps {
        max_fee_bps = max_allowed_bps;
    }

    let max_numerator = match bps_to_numerator_u64(u64::from(max_fee_bps)) {
        Ok(value) => value,
        Err(_) => return false,
    };

    if max_numerator < base.cliff_fee_numerator {
        return false;
    }

    base.second_factor[4..8].copy_from_slice(&max_fee_bps.to_le_bytes());
    true
}

fn fee_schedule_valid(
    base_fee: &BaseFeeKind,
    dynamic_fee: &CpAmmDynamicFeeExtras,
    activation_point: u64,
    pool_version: u8,
) -> bool {
    const VALIDATION_AMOUNTS: [u64; 3] = [1, 10_000, 5_000_000];
    let model = PoolFeeModel {
        base_fee: base_fee.clone(),
        dynamic_fee: dynamic_fee.clone(),
        activation_point,
        pool_version,
        cliff_fee_numerator: base_fee.cliff_fee().min(FEE_DENOMINATOR),
    };
    let limiter_point = activation_point.saturating_add(1);
    for &amount in &VALIDATION_AMOUNTS {
        if model
            .total_fee_from_included(TradeDirection::AToB, activation_point, amount)
            .is_err()
        {
            return false;
        }
        if model
            .total_fee_from_included(TradeDirection::BToA, limiter_point, amount)
            .is_err()
        {
            return false;
        }
    }
    true
}

fn max_fee_bps_for_version(version: u8) -> u64 {
    match version {
        0 => MAX_FEE_BPS_V0,
        1 => MAX_FEE_BPS_V1,
        _ => MAX_FEE_BPS_V1,
    }
}

fn numerator_to_bps(numerator: u64) -> u64 {
    numerator
        .saturating_mul(BPS_DENOMINATOR)
        .checked_div(FEE_DENOMINATOR)
        .unwrap_or(0)
}

fn bps_to_numerator_u64(bps: u64) -> FeeResult<u64> {
    if bps == 0 {
        return Ok(0);
    }
    let numerator = bps
        .saturating_mul(FEE_DENOMINATOR)
        .checked_div(BPS_DENOMINATOR)
        .ok_or(FeeComputationError::InvalidConfig)?;
    Ok(numerator.min(FEE_DENOMINATOR))
}

fn max_fee_numerator_for_version(version: u8) -> Option<u64> {
    match version {
        0 => Some(MAX_FEE_NUMERATOR_V0),
        1 => Some(MAX_FEE_NUMERATOR_V1),
        _ => None,
    }
}

fn sqrt_u256(value: U256) -> Option<U256> {
    if value.is_zero() {
        return Some(U256::zero());
    }
    let mut z = value;
    let mut x = (value + U256::one()) >> 1;
    while x < z {
        z = x;
        x = (value / x + x) >> 1;
    }
    Some(z)
}

fn dynamic_fee_numerator(fee: &CpAmmDynamicFeeExtras) -> FeeResult<u64> {
    if fee.initialized == 0 {
        return Ok(0);
    }
    let accumulator = fee.volatility_accumulator;
    let bin_step = u128::from(fee.bin_step);
    let scaled = accumulator
        .checked_mul(bin_step)
        .ok_or(FeeComputationError::MathOverflow)?;
    let squared = scaled
        .checked_pow(2)
        .ok_or(FeeComputationError::MathOverflow)?;
    let control = u128::from(fee.variable_fee_control);
    let v_fee = squared
        .checked_mul(control)
        .ok_or(FeeComputationError::MathOverflow)?;
    let scaled = v_fee
        .checked_add(99_999_999_999)
        .ok_or(FeeComputationError::MathOverflow)?
        .checked_div(100_000_000_000)
        .ok_or(FeeComputationError::InvalidConfig)?;
    let capped = scaled.min(u128::from(FEE_DENOMINATOR));
    Ok(capped as u64)
}

fn compute_excluded_fee_amount(
    trade_fee_numerator: u64,
    included_fee_amount: u64,
) -> Option<(u64, u64)> {
    let fee_amount =
        mul_div_round_up_u64(included_fee_amount, trade_fee_numerator, FEE_DENOMINATOR)?;
    let excluded = included_fee_amount.checked_sub(fee_amount)?;
    Some((excluded, fee_amount))
}

fn compute_included_fee_amount(
    trade_fee_numerator: u64,
    excluded_fee_amount: u64,
) -> Option<(u64, u64)> {
    if trade_fee_numerator >= FEE_DENOMINATOR {
        return None;
    }
    let denominator = FEE_DENOMINATOR.saturating_sub(trade_fee_numerator);
    let included = mul_div_round_up_u64(excluded_fee_amount, FEE_DENOMINATOR, denominator)?;
    let fee_amount = included.saturating_sub(excluded_fee_amount);
    Some((included, fee_amount))
}

impl FeeRateLimiter {
    fn from_extras(extras: &CpAmmBaseFeeExtras) -> Option<Self> {
        let duration_bytes: [u8; 4] = extras.second_factor[0..4].try_into().ok()?;
        let max_fee_bytes: [u8; 4] = extras.second_factor[4..8].try_into().ok()?;
        Some(Self {
            cliff_fee_numerator: extras.cliff_fee_numerator,
            fee_increment_bps: extras.first_factor,
            max_limiter_duration: u32::from_le_bytes(duration_bytes),
            max_fee_bps: u32::from_le_bytes(max_fee_bytes),
            reference_amount: extras.third_factor,
        })
    }

    fn is_rate_limiter_applied(
        &self,
        current_point: u64,
        activation_point: u64,
        trade_direction: TradeDirection,
    ) -> bool {
        if self.is_zero_rate_limiter() {
            return false;
        }
        if trade_direction == TradeDirection::AToB {
            return false;
        }
        if current_point < activation_point {
            return false;
        }
        let last_effective_point = (activation_point as u128)
            .checked_add(self.max_limiter_duration as u128)
            .unwrap_or(u128::MAX);
        u128::from(current_point) <= last_effective_point
    }

    fn is_zero_rate_limiter(&self) -> bool {
        self.reference_amount == 0
            && self.max_limiter_duration == 0
            && self.max_fee_bps == 0
            && self.fee_increment_bps == 0
    }

    fn get_max_index(&self) -> FeeResult<u64> {
        let max_fee = bps_to_numerator_u64(u64::from(self.max_fee_bps))?;
        let delta = max_fee
            .checked_sub(self.cliff_fee_numerator)
            .ok_or(FeeComputationError::InvalidConfig)?;
        let increment = bps_to_numerator_u64(u64::from(self.fee_increment_bps))?;
        if increment == 0 {
            return Err(FeeComputationError::InvalidConfig);
        }
        Ok(delta / increment)
    }

    fn fee_numerator_from_included_core(&self, included_fee_amount: u64) -> FeeResult<u64> {
        if self.reference_amount == 0 || included_fee_amount == 0 {
            return Ok(self.cliff_fee_numerator);
        }
        if included_fee_amount <= self.reference_amount {
            return Ok(self.cliff_fee_numerator);
        }
        let max_fee_numerator = bps_to_numerator_u64(u64::from(self.max_fee_bps))?;
        let reference = U256::from(self.reference_amount);
        let delta = U256::from(included_fee_amount - self.reference_amount);
        let (a, b) = if reference.is_zero() {
            (U256::zero(), delta)
        } else {
            (delta / reference.clone(), delta % reference.clone())
        };
        let max_index = U256::from(self.get_max_index()?);
        let c = U256::from(self.cliff_fee_numerator);
        let increment = U256::from(bps_to_numerator_u64(u64::from(self.fee_increment_bps))?);
        let x0 = reference;
        let one = U256::one();
        let two = U256::from(2u8);

        let trading_fee_numerator = if a < max_index {
            let numerator_1 = c + c * a + increment * a * (a + one.clone()) / two;
            let numerator_2 = c + increment * (a + one.clone());
            x0.clone() * numerator_1 + b * numerator_2
        } else {
            let numerator_1 =
                c + c * max_index + increment * max_index * (max_index + one.clone()) / two;
            let numerator_2 = U256::from(max_fee_numerator);
            let d = a - max_index;
            let left_amount = d * x0.clone() + b;
            x0 * numerator_1 + left_amount * numerator_2
        };

        let denominator = U256::from(FEE_DENOMINATOR);
        let trading_fee = (trading_fee_numerator + denominator.clone() - one) / denominator;
        let trading_fee_u64: u64 = trading_fee
            .try_into()
            .map_err(|_| FeeComputationError::InvalidConfig)?;

        mul_div_round_up_u64(trading_fee_u64, FEE_DENOMINATOR, included_fee_amount)
            .ok_or(FeeComputationError::InvalidConfig)
    }

    fn get_fee_numerator_from_included_fee_amount(
        &self,
        current_point: u64,
        activation_point: u64,
        trade_direction: TradeDirection,
        included_fee_amount: u64,
    ) -> FeeResult<u64> {
        if !self.is_rate_limiter_applied(current_point, activation_point, trade_direction) {
            return Ok(self.cliff_fee_numerator);
        }
        self.fee_numerator_from_included_core(included_fee_amount)
    }

    fn get_checked_amounts(&self) -> FeeResult<(u64, u64, bool)> {
        let max_index = U256::from(self.get_max_index()?);
        let x0 = U256::from(self.reference_amount);
        let one = U256::one();
        let max_index_input_amount = (max_index + one.clone()) * x0.clone();
        if max_index_input_amount <= U256::from(u64::MAX) {
            let included: u64 = max_index_input_amount
                .try_into()
                .map_err(|_| FeeComputationError::InvalidConfig)?;
            let excluded = self.get_excluded_fee_amount(included)?;
            Ok((excluded, included, false))
        } else {
            let excluded = self.get_excluded_fee_amount(u64::MAX)?;
            Ok((excluded, u64::MAX, true))
        }
    }

    fn get_excluded_fee_amount(&self, included_fee_amount: u64) -> FeeResult<u64> {
        let fee_numerator = self.fee_numerator_from_included_core(included_fee_amount)?;
        let (excluded, _) = compute_excluded_fee_amount(fee_numerator, included_fee_amount)
            .ok_or(FeeComputationError::InvalidConfig)?;
        Ok(excluded)
    }

    fn fee_numerator_from_excluded_core(&self, excluded_fee_amount: u64) -> FeeResult<u64> {
        if self.reference_amount == 0 || excluded_fee_amount == 0 {
            return Ok(self.cliff_fee_numerator);
        }

        let reference_excluded = self.get_excluded_fee_amount(self.reference_amount)?;
        if excluded_fee_amount <= reference_excluded {
            return Ok(self.cliff_fee_numerator);
        }

        let (checked_excluded, checked_included, overflowed) = self.get_checked_amounts()?;
        if excluded_fee_amount == checked_excluded {
            return self.fee_numerator_from_included_core(checked_included);
        }

        if excluded_fee_amount < checked_excluded {
            self.solve_quadratic_fee(excluded_fee_amount)
        } else {
            if overflowed {
                return Err(FeeComputationError::MathOverflow);
            }
            let remaining = excluded_fee_amount
                .checked_sub(checked_excluded)
                .ok_or(FeeComputationError::MathOverflow)?;
            let max_fee_numerator = bps_to_numerator_u64(u64::from(self.max_fee_bps))?;
            let (included_remaining, _) = compute_included_fee_amount(max_fee_numerator, remaining)
                .ok_or(FeeComputationError::InvalidConfig)?;
            let total_included = included_remaining
                .checked_add(checked_included)
                .ok_or(FeeComputationError::MathOverflow)?;
            self.fee_numerator_from_included_core(total_included)
        }
    }

    fn get_fee_numerator_from_excluded_fee_amount(
        &self,
        current_point: u64,
        activation_point: u64,
        trade_direction: TradeDirection,
        excluded_fee_amount: u64,
    ) -> FeeResult<u64> {
        if !self.is_rate_limiter_applied(current_point, activation_point, trade_direction) {
            return Ok(self.cliff_fee_numerator);
        }
        self.fee_numerator_from_excluded_core(excluded_fee_amount)
    }

    fn solve_quadratic_fee(&self, excluded_fee_amount: u64) -> FeeResult<u64> {
        let two = U256::from(2u8);
        let four = U256::from(4u8);
        let increment = U256::from(bps_to_numerator_u64(u64::from(self.fee_increment_bps))?);
        let reference = U256::from(self.reference_amount);
        let denominator = U256::from(FEE_DENOMINATOR);
        let cliff = U256::from(self.cliff_fee_numerator);
        let excluded = U256::from(excluded_fee_amount);

        let x = increment;
        if x.is_zero() {
            return Ok(self.cliff_fee_numerator);
        }
        let y = two.clone() * denominator.clone() * reference.clone()
            + increment.clone() * reference.clone()
            - two.clone() * cliff.clone() * reference.clone();
        let z = two.clone() * excluded.clone() * denominator.clone() * reference.clone();

        let disc = y
            .clone()
            .checked_mul(y.clone())
            .ok_or(FeeComputationError::MathOverflow)?
            .checked_sub(four.clone() * x.clone() * z.clone())
            .ok_or(FeeComputationError::MathOverflow)?;
        let sqrt_disc = sqrt_u256(disc).ok_or(FeeComputationError::InvalidConfig)?;
        let included_fee_amount = (y - sqrt_disc) / (two.clone() * x.clone());
        let a_plus_one = included_fee_amount
            .checked_div(reference.clone())
            .ok_or(FeeComputationError::InvalidConfig)?;

        let first_excluded = self.get_excluded_fee_amount(
            included_fee_amount
                .try_into()
                .map_err(|_| FeeComputationError::InvalidConfig)?,
        )?;
        let remaining_excluded = excluded_fee_amount
            .checked_sub(first_excluded)
            .ok_or(FeeComputationError::MathOverflow)?;

        let remaining_fee_numerator = cliff + increment * a_plus_one;
        let (included_remaining, _) = compute_included_fee_amount(
            remaining_fee_numerator
                .try_into()
                .map_err(|_| FeeComputationError::InvalidConfig)?,
            remaining_excluded,
        )
        .ok_or(FeeComputationError::InvalidConfig)?;

        let total_included = included_fee_amount
            .checked_add(U256::from(included_remaining))
            .ok_or(FeeComputationError::MathOverflow)?;
        let total_u64: u64 = total_included
            .try_into()
            .map_err(|_| FeeComputationError::InvalidConfig)?;
        self.fee_numerator_from_included_core(total_u64)
    }
}

impl PoolFeeModel {
    fn total_fee_from_included(
        &self,
        direction: TradeDirection,
        current_point: u64,
        included_amount: u64,
    ) -> FeeResult<u64> {
        let base = self.base_fee.base_fee_from_included(
            current_point,
            self.activation_point,
            direction,
            included_amount,
        )?;
        self.combine_with_dynamic(base)
    }

    fn total_fee_from_excluded(
        &self,
        direction: TradeDirection,
        current_point: u64,
        excluded_amount: u64,
    ) -> FeeResult<u64> {
        let base = self.base_fee.base_fee_from_excluded(
            current_point,
            self.activation_point,
            direction,
            excluded_amount,
        )?;
        self.combine_with_dynamic(base)
    }

    fn combine_with_dynamic(&self, base_fee_numerator: u64) -> FeeResult<u64> {
        let dynamic = dynamic_fee_numerator(&self.dynamic_fee)?;
        let total = (base_fee_numerator as u128)
            .checked_add(dynamic as u128)
            .ok_or(FeeComputationError::MathOverflow)?;
        let max = max_fee_numerator_for_version(self.pool_version)
            .ok_or(FeeComputationError::InvalidConfig)?;
        Ok(total.min(max as u128) as u64)
    }
}

impl BaseFeeKind {
    fn from_extras(extras: &CpAmmBaseFeeExtras) -> Option<Self> {
        let mode = BaseFeeModeFlag::from_raw(extras.base_fee_mode)?;
        match mode {
            BaseFeeModeFlag::Linear | BaseFeeModeFlag::Exponential => {
                let scheduler = FeeScheduler::from_extras(extras, mode)?;
                Some(BaseFeeKind::Scheduler(scheduler))
            }
            BaseFeeModeFlag::RateLimiter => {
                let limiter = FeeRateLimiter::from_extras(extras)?;
                Some(BaseFeeKind::RateLimiter(limiter))
            }
        }
    }

    fn base_fee_from_included(
        &self,
        current_point: u64,
        activation_point: u64,
        direction: TradeDirection,
        included_amount: u64,
    ) -> FeeResult<u64> {
        match self {
            BaseFeeKind::Scheduler(scheduler) => {
                scheduler.base_fee(current_point, activation_point)
            }
            BaseFeeKind::RateLimiter(limiter) => limiter
                .get_fee_numerator_from_included_fee_amount(
                    current_point,
                    activation_point,
                    direction,
                    included_amount,
                ),
        }
    }

    fn base_fee_from_excluded(
        &self,
        current_point: u64,
        activation_point: u64,
        direction: TradeDirection,
        excluded_amount: u64,
    ) -> FeeResult<u64> {
        match self {
            BaseFeeKind::Scheduler(scheduler) => {
                scheduler.base_fee(current_point, activation_point)
            }
            BaseFeeKind::RateLimiter(limiter) => limiter
                .get_fee_numerator_from_excluded_fee_amount(
                    current_point,
                    activation_point,
                    direction,
                    excluded_amount,
                ),
        }
    }

    fn cliff_fee(&self) -> u64 {
        match self {
            BaseFeeKind::Scheduler(scheduler) => scheduler.cliff_fee_numerator,
            BaseFeeKind::RateLimiter(limiter) => limiter.cliff_fee_numerator,
        }
    }
}

impl FeeScheduler {
    fn from_extras(extras: &CpAmmBaseFeeExtras, mode: BaseFeeModeFlag) -> Option<Self> {
        let scheduler_mode = match mode {
            BaseFeeModeFlag::Linear => FeeSchedulerMode::Linear,
            BaseFeeModeFlag::Exponential => FeeSchedulerMode::Exponential,
            BaseFeeModeFlag::RateLimiter => return None,
        };
        Some(Self {
            cliff_fee_numerator: extras.cliff_fee_numerator,
            number_of_period: extras.first_factor,
            period_frequency: u64::from_le_bytes(extras.second_factor),
            reduction_factor: extras.third_factor,
            mode: scheduler_mode,
        })
    }

    fn base_fee(&self, current_point: u64, activation_point: u64) -> FeeResult<u64> {
        if self.period_frequency == 0 {
            return Ok(self.cliff_fee_numerator.min(FEE_DENOMINATOR));
        }
        let period = if current_point < activation_point {
            u64::from(self.number_of_period)
        } else {
            current_point
                .checked_sub(activation_point)
                .ok_or(FeeComputationError::MathOverflow)?
                .checked_div(self.period_frequency)
                .ok_or(FeeComputationError::MathOverflow)?
                .min(u64::from(self.number_of_period))
        };
        self.fee_by_period(period)
    }

    fn fee_by_period(&self, period: u64) -> FeeResult<u64> {
        let bounded_period = period.min(u64::from(self.number_of_period));
        match self.mode {
            FeeSchedulerMode::Linear => {
                let reduction = self
                    .reduction_factor
                    .checked_mul(bounded_period)
                    .ok_or(FeeComputationError::MathOverflow)?
                    .min(self.cliff_fee_numerator);
                Ok(self.cliff_fee_numerator.saturating_sub(reduction))
            }
            FeeSchedulerMode::Exponential => exponential_fee(
                self.cliff_fee_numerator,
                self.reduction_factor,
                bounded_period,
            ),
        }
    }
}

fn exponential_fee(cliff: u64, reduction_factor: u64, period: u64) -> FeeResult<u64> {
    if period == 0 {
        return Ok(cliff);
    }
    let mut fee = U256::from(cliff);
    let reduction = reduction_factor.min(10_000);
    let numerator = U256::from(10_000u64.saturating_sub(reduction));
    let denominator = U256::from(10_000u64);
    for _ in 0..period {
        fee = fee
            .checked_mul(numerator)
            .ok_or(FeeComputationError::MathOverflow)?
            .checked_div(denominator)
            .ok_or(FeeComputationError::InvalidConfig)?;
    }
    fee.try_into()
        .map_err(|_| FeeComputationError::InvalidConfig)
}

fn percent_from_components(part: u64, whole: u64) -> u8 {
    if part == 0 || whole == 0 {
        return 0;
    }
    let percent = part
        .saturating_mul(100)
        .checked_div(whole)
        .unwrap_or(0)
        .min(100);
    percent as u8
}

fn bps_to_numerator(bps: u64) -> u64 {
    if bps == 0 {
        return 0;
    }
    bps.saturating_mul(FEE_DENOMINATOR)
        .checked_div(BPS_DENOMINATOR)
        .unwrap_or(0)
        .min(FEE_DENOMINATOR)
}

fn mul_div_round_up_u64(value: u64, numerator: u64, denominator: u64) -> Option<u64> {
    if denominator == 0 {
        return None;
    }
    let value = value as u128;
    let numerator = numerator as u128;
    let denominator = denominator as u128;
    let result = (value
        .checked_mul(numerator)?
        .checked_add(denominator.saturating_sub(1))?)
    .checked_div(denominator)?;
    Some(result.min(u128::from(u64::MAX)) as u64)
}

fn mul_div_floor_u64(value: u64, numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 || numerator == 0 || value == 0 {
        return 0;
    }
    let result = (value as u128)
        .checked_mul(numerator as u128)
        .unwrap_or(u128::from(u64::MAX))
        .checked_div(denominator as u128)
        .unwrap_or(u128::from(u64::MAX));
    result.min(u128::from(u64::MAX)) as u64
}

#[derive(Clone, Copy)]
struct ClmmParams {
    liquidity: u128,
    sqrt_price_x64: u128,
    sqrt_min_price_x64: u128,
    sqrt_max_price_x64: u128,
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

fn optional_pubkey_string(pk: Pubkey) -> Option<String> {
    if pk == Pubkey::default() {
        None
    } else {
        Some(pk.to_string())
    }
}

mod math {
    use super::{BPS_DENOMINATOR, U256};

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

        let fee_denominator = BPS_DENOMINATOR as u128;
        let fee_bps = fee_bps as u128;
        if fee_bps >= fee_denominator {
            return None;
        }

        let amount_less_fee = amount_remaining
            .checked_mul(fee_denominator - fee_bps)?
            .checked_div(fee_denominator)?;

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

        let fee_amount = mul_div_round_up(amount_in, fee_bps, fee_denominator - fee_bps)?;

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
    use crate::hydration::types::{
        HydrationStatus, PoolSnapshot, PoolUpdateExtensions, SnapshotFreshness,
    };
    use spl_pod::primitives::{PodU16, PodU64};
    use spl_token_2022::extension::transfer_fee::TransferFee;

    fn fixture_transfer_fee(bps: u16, max_fee: u64) -> TokenTransferFee {
        let fee = TransferFee {
            epoch: PodU64::from(0),
            maximum_fee: PodU64::from(max_fee),
            transfer_fee_basis_points: PodU16::from(bps),
        };
        TokenTransferFee::new(fee)
    }

    fn fixture_rate_limiter_state() -> CpAmmState {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let mut second_factor = [0u8; 8];
        second_factor[..4].copy_from_slice(&100u32.to_le_bytes());
        second_factor[4..].copy_from_slice(&2_000u32.to_le_bytes());
        let pool_fees = CpAmmPoolFeesExtras {
            base_fee: CpAmmBaseFeeExtras {
                cliff_fee_numerator: 10_000_000,
                base_fee_mode: BaseFeeModeFlag::RateLimiter as u8,
                first_factor: 100,
                second_factor,
                third_factor: 500_000,
            },
            protocol_fee_percent: 0,
            partner_fee_percent: 0,
            referral_fee_percent: 0,
            dynamic_fee: CpAmmDynamicFeeExtras::default(),
        };

        CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: None,
            token_b_decimals: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: Some(CollectFeeModeFlag::OnlyB as u8),
            activation_point: Some(0),
            activation_type: Some(ActivationType::Slot as u8),
            pool_status: Some(PoolStatusFlag::Enable as u8),
            pool_type: None,
            pool_version: Some(0),
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: Some(pool_fees),
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(5_500_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 3_000_000,
            vault_b_balance: 3_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        }
    }

    #[test]
    fn apply_meta_refreshes_dynamic_fee() {
        let old_mint = Pubkey::new_unique();
        let old_mint_string = old_mint.to_string();
        let mut extras = CpAmmPoolExtras {
            token_a_mint: Some(old_mint_string.clone()),
            pool_fees: Some(CpAmmPoolFeesExtras {
                base_fee: CpAmmBaseFeeExtras {
                    cliff_fee_numerator: 5,
                    ..Default::default()
                },
                dynamic_fee: CpAmmDynamicFeeExtras {
                    initialized: 1,
                    volatility_accumulator: 10,
                    ..Default::default()
                },
                ..Default::default()
            }),
            hydrated_at: Some(Instant::now()),
            ..Default::default()
        };

        let refreshed_pool_fees = CpAmmPoolFeesExtras {
            base_fee: CpAmmBaseFeeExtras {
                cliff_fee_numerator: 500,
                base_fee_mode: BaseFeeModeFlag::RateLimiter as u8,
                ..Default::default()
            },
            dynamic_fee: CpAmmDynamicFeeExtras {
                initialized: 1,
                volatility_accumulator: 77,
                ..Default::default()
            },
            protocol_fee_percent: 2,
            partner_fee_percent: 1,
            referral_fee_percent: 1,
        };

        let meta = HydratedPoolMeta {
            token_a_mint: Pubkey::new_unique(),
            token_b_mint: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            whitelisted_vault: Pubkey::default(),
            partner: Pubkey::default(),
            sqrt_price_x64: 42,
            liquidity: 500_000,
            sqrt_min_price_x64: 21,
            sqrt_max_price_x64: 84,
            permanent_lock_liquidity: 9,
            collect_fee_mode: CollectFeeModeFlag::OnlyB as u8,
            activation_point: 7,
            activation_type: 0,
            pool_status: PoolStatusFlag::Enable as u8,
            pool_type: 0,
            pool_version: 0,
            token_a_flag: 1,
            token_b_flag: 2,
            pool_fees: refreshed_pool_fees.clone(),
            hydrated_at: Instant::now(),
            hydrated_slot: 0,
        };

        apply_hydrated_meta(&mut extras, &meta, false);
        let after_no_refresh = extras.pool_fees.as_ref().expect("fees present");
        assert_eq!(after_no_refresh.dynamic_fee.volatility_accumulator, 10);
        assert_eq!(
            extras.token_a_mint.as_deref(),
            Some(old_mint_string.as_str())
        );

        let refreshed_mint = meta.token_a_mint.to_string();
        apply_hydrated_meta(&mut extras, &meta, true);
        let refreshed = extras.pool_fees.as_ref().expect("fees refreshed");
        assert_eq!(
            refreshed.dynamic_fee.volatility_accumulator,
            refreshed_pool_fees.dynamic_fee.volatility_accumulator
        );
        assert_eq!(refreshed.base_fee.cliff_fee_numerator, 500);
        assert_eq!(
            extras.token_a_mint.as_deref(),
            Some(refreshed_mint.as_str())
        );
        assert!(extras.hydrated_at.is_some());
    }

    #[test]
    fn decorate_template_context_sets_token_programs_without_builder() {
        let adapter = CpAmmAdapter::new(CpAmmAdapterConfig::default(), None);
        let mut state = fixture_rate_limiter_state();
        state.token_a_program = Some(*SPL_TOKEN_2022_PROGRAM_PUBKEY);
        state.token_b_program = Some(*SPL_TOKEN_PROGRAM_PUBKEY);

        let mut context = TemplateContext::default();
        adapter.decorate_template_context(
            &state,
            &mut context,
            state.token_a_mint,
            state.token_b_mint,
        );

        assert_eq!(context.input_token_program, state.token_a_program);
        assert_eq!(context.output_token_program, state.token_b_program);
    }

    #[test]
    fn invalid_fee_schedule_falls_back_to_legacy() {
        let mut state = fixture_rate_limiter_state();
        state.lp_fee_bps = Some(11);
        state.protocol_fee_bps = Some(7);
        state.referral_bps = Some(2);
        if let Some(fees) = state.pool_fees.as_mut() {
            let mut second_factor = [0u8; 8];
            second_factor[..4].copy_from_slice(&1u32.to_le_bytes());
            second_factor[4..].copy_from_slice(&50u32.to_le_bytes());
            fees.base_fee.second_factor = second_factor;
            fees.base_fee.cliff_fee_numerator = bps_to_numerator_u64(400)
                .expect("cliff conversion")
                .min(FEE_DENOMINATOR);
        }

        let config = state.fee_config();
        assert!(matches!(config.kind, FeeConfigKind::Legacy(_)));
        assert_eq!(config.trade_fee_bps(), 20);
    }

    #[test]
    fn sanitized_fee_schedule_clamps_to_bounds() {
        let mut state = fixture_rate_limiter_state();
        state.pool_version = Some(0);
        if let Some(fees) = state.pool_fees.as_mut() {
            fees.base_fee.cliff_fee_numerator = FEE_DENOMINATOR * 2;
            let mut second_factor = fees.base_fee.second_factor;
            let over_max = (MAX_FEE_BPS_V0 + 1_000) as u32;
            second_factor[4..].copy_from_slice(&over_max.to_le_bytes());
            fees.base_fee.second_factor = second_factor;
        }

        let config = state.fee_config();
        assert!(matches!(config.kind, FeeConfigKind::Pool(_)));
        assert_eq!(config.trade_fee_bps(), MAX_FEE_BPS_V0);
    }

    #[test]
    fn parses_pool_account_prefix() {
        let token_a_mint = Pubkey::new_unique();
        let token_b_mint = Pubkey::new_unique();
        let token_a_vault = Pubkey::new_unique();
        let token_b_vault = Pubkey::new_unique();
        let whitelisted_vault = Pubkey::new_unique();
        let partner = Pubkey::new_unique();
        let liquidity: u128 = 42_000;
        let sqrt_price: u128 = 1_234_567_890;

        let mut data = vec![0u8; CP_AMM_POOL_ACCOUNT_MIN_LEN];
        let mut cursor = CP_AMM_POOL_ACCOUNT_HEADER_LEN;
        for key in [
            token_a_mint,
            token_b_mint,
            token_a_vault,
            token_b_vault,
            whitelisted_vault,
            partner,
        ] {
            data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(key.as_ref());
            cursor += PUBKEY_BYTES;
        }

        let liquidity_bytes = liquidity.to_le_bytes();
        data[cursor..cursor + U128_BYTES].copy_from_slice(&liquidity_bytes);
        cursor += U128_BYTES;
        cursor += U128_BYTES; // padding
        cursor += U64_BYTES * 4; // protocol + partner fee accumulators
        cursor += U128_BYTES; // sqrt_min_price
        cursor += U128_BYTES; // sqrt_max_price
        let sqrt_price_bytes = sqrt_price.to_le_bytes();
        data[cursor..cursor + U128_BYTES].copy_from_slice(&sqrt_price_bytes);

        let meta = parse_pool_account(&data).expect("pool account should hydrate");
        assert_eq!(meta.token_a_mint, token_a_mint);
        assert_eq!(meta.token_b_mint, token_b_mint);
        assert_eq!(meta.token_a_vault, token_a_vault);
        assert_eq!(meta.token_b_vault, token_b_vault);
        assert_eq!(meta.liquidity, liquidity);
        assert_eq!(meta.sqrt_price_x64, sqrt_price);
    }

    #[test]
    fn simulate_cp_amm_exact_in_respects_slippage() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: mint_a,
            output_mint: mint_b,
            amount: 100_000,
            slot: 1,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let mut state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: None,
            token_b_decimals: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: None,
            activation_type: None,
            pool_status: None,
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(5_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 1_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        let simulation = simulate_cp_amm_exact_in(&state, &req, false).expect("swap should quote");
        assert!(simulation.output_amount > 0);
        assert!(simulation.edge_bps.is_sign_negative());
    }

    #[test]
    fn rate_limiter_escalates_fee() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let mut second_factor = [0u8; 8];
        second_factor[..4].copy_from_slice(&100u32.to_le_bytes());
        second_factor[4..].copy_from_slice(&2_000u32.to_le_bytes());
        let pool_fees = CpAmmPoolFeesExtras {
            base_fee: CpAmmBaseFeeExtras {
                cliff_fee_numerator: 10_000_000,
                base_fee_mode: BaseFeeModeFlag::RateLimiter as u8,
                first_factor: 100,
                second_factor,
                third_factor: 500_000,
            },
            protocol_fee_percent: 0,
            partner_fee_percent: 0,
            referral_fee_percent: 0,
            dynamic_fee: CpAmmDynamicFeeExtras::default(),
        };

        let mut state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: None,
            token_b_decimals: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: Some(CollectFeeModeFlag::OnlyB as u8),
            activation_point: Some(1),
            activation_type: Some(ActivationType::Slot as u8),
            pool_status: Some(PoolStatusFlag::Enable as u8),
            pool_type: None,
            pool_version: Some(0),
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: Some(pool_fees),
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(5_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 1_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        let config = state.fee_config();
        let current_point = 10;
        let small = config
            .trade_fee_from_included(TradeDirection::BToA, current_point, 100_000)
            .expect("small trade fee");
        let large = config
            .trade_fee_from_included(TradeDirection::BToA, current_point, 5_000_000)
            .expect("large trade fee");
        assert!(large > small);

        let unaffected = config
            .trade_fee_from_included(TradeDirection::AToB, current_point, 5_000_000)
            .expect("a to b fee");
        assert_eq!(unaffected, small);
    }

    #[test]
    fn rate_limiter_exact_in_recomputes_executed_fee() {
        let state = fixture_rate_limiter_state();
        let req = QuoteRequest {
            pool_id: state.pool,
            input_mint: state.token_b_mint,
            output_mint: state.token_a_mint,
            amount: 120_000,
            slot: 5,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let simulation = simulate_cp_amm_exact_in(&state, &req, false).expect("rate-limited quote");
        assert!(simulation.fee_amount > 0);
        let direction =
            resolve_trade_direction(&state, &req.input_mint, &req.output_mint).expect("direction");
        assert_eq!(direction, TradeDirection::BToA);
        let fee_mode = FeeMode::new(
            state.collect_fee_mode(),
            direction,
            state.has_referral_account(),
        );
        assert!(fee_mode.fees_on_input);
        let current_point = state
            .current_point(req.slot, req.block_time_u64())
            .expect("point");
        let fee_config = state.fee_config();
        let numerator = fee_config
            .trade_fee_from_excluded(direction, current_point, simulation.net_input_amount)
            .expect("limiter numerator");
        let (_, split) = fee_config
            .apply_fee_to_excluded_amount(
                simulation.net_input_amount,
                numerator,
                fee_mode.has_referral,
                state.has_partner(),
            )
            .expect("fee split");
        assert_eq!(split.total_fee, simulation.fee_amount);
    }

    #[test]
    fn rate_limiter_exact_out_recomputes_executed_fee() {
        let state = fixture_rate_limiter_state();
        let req = QuoteRequest {
            pool_id: state.pool,
            input_mint: state.token_b_mint,
            output_mint: state.token_a_mint,
            amount: 80_000,
            slot: 7,
            block_time: None,
            direction: SwapDirection::ExactOut,
        };

        let simulation = simulate_cp_amm_exact_out(&state, &req).expect("rate-limited exact out");
        assert!(simulation.fee_amount > 0);
        let direction =
            resolve_trade_direction(&state, &req.input_mint, &req.output_mint).expect("direction");
        assert_eq!(direction, TradeDirection::BToA);
        let fee_mode = FeeMode::new(
            state.collect_fee_mode(),
            direction,
            state.has_referral_account(),
        );
        assert!(fee_mode.fees_on_input);
        let current_point = state
            .current_point(req.slot, req.block_time_u64())
            .expect("point");
        let fee_config = state.fee_config();
        let numerator = fee_config
            .trade_fee_from_excluded(direction, current_point, simulation.net_input_amount)
            .expect("limiter numerator");
        let (_, split) = fee_config
            .apply_fee_to_excluded_amount(
                simulation.net_input_amount,
                numerator,
                fee_mode.has_referral,
                state.has_partner(),
            )
            .expect("fee split");
        assert_eq!(split.total_fee, simulation.fee_amount);
        assert_eq!(simulation.output_amount, req.amount);
    }

    #[test]
    fn partial_fill_matches_price_cap_behavior() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: mint_a,
            output_mint: mint_b,
            amount: u64::MAX,
            slot: 10,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let mut state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: None,
            token_b_decimals: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: Some(CollectFeeModeFlag::BothToken as u8),
            activation_point: Some(0),
            activation_type: Some(ActivationType::Slot as u8),
            pool_status: Some(PoolStatusFlag::Enable as u8),
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(5_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 1_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        assert!(simulate_cp_amm_exact_in(&state, &req, false).is_none());
        let partial = simulate_cp_amm_exact_in(&state, &req, true).expect("partial fill");
        assert!(partial.amount_leftover > 0);
        assert!(partial.output_amount > 0);
    }

    #[test]
    fn simulate_cp_amm_exact_out_tracks_inverse() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let exact_in = QuoteRequest {
            pool_id: pool,
            input_mint: mint_a,
            output_mint: mint_b,
            amount: 50_000,
            slot: 1,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let mut state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: None,
            token_b_decimals: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: None,
            activation_type: None,
            pool_status: None,
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(8_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 2_000_000,
            vault_b_balance: 3_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        let expected_out = simulate_cp_amm_exact_in(&state, &exact_in, false)
            .map(|sim| sim.output_amount)
            .expect("exact-in quote should succeed");

        let exact_out = QuoteRequest {
            amount: expected_out,
            direction: SwapDirection::ExactOut,
            ..exact_in
        };
        let amount_in = simulate_cp_amm_exact_out(&state, &exact_out)
            .map(|sim| sim.executed_input_amount)
            .expect("inverse quote should succeed");
        assert!(amount_in >= exact_in.amount);
        assert!(amount_in - exact_in.amount <= 5);
    }

    #[test]
    fn simulate_cp_amm_applies_token_transfer_fees() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: mint_a,
            output_mint: mint_b,
            amount: 200_000,
            slot: 1,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let mut state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: None,
            token_b_decimals: None,
            token_a_transfer_fee: Some(fixture_transfer_fee(150, 5_000)),
            token_b_transfer_fee: Some(fixture_transfer_fee(100, 3_000)),
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: None,
            activation_type: None,
            pool_status: None,
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(7_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 3_000_000,
            vault_b_balance: 5_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        let simulation =
            simulate_cp_amm_exact_in(&state, &req, false).expect("quote should succeed");
        assert!(simulation.token_in_fee_amount > 0);
        assert!(simulation.token_out_fee_amount > 0);
        let direction = resolve_trade_direction(&state, &req.input_mint, &req.output_mint).unwrap();
        let fee_mode = FeeMode::new(state.collect_fee_mode(), direction, false);
        let mut expected_consumed = simulation
            .net_input_amount
            .saturating_add(simulation.token_in_fee_amount);
        if fee_mode.fees_on_input {
            expected_consumed = expected_consumed.saturating_add(simulation.fee_amount);
        }
        assert!(simulation.executed_input_amount.abs_diff(expected_consumed) <= 1);
        assert!(simulation.output_amount < req.amount);

        // ensure state without fees reverts to original behavior
        state.token_a_transfer_fee = None;
        state.token_b_transfer_fee = None;
        let baseline = simulate_cp_amm_exact_in(&state, &req, false).expect("baseline quote");
        assert!(baseline.token_in_fee_amount == 0);
        assert!(baseline.token_out_fee_amount == 0);
        assert!(simulation.executed_input_amount >= baseline.executed_input_amount);
        assert!(baseline.output_amount > simulation.output_amount);
    }

    #[test]
    fn resolve_reserves_fails_for_unknown_direction() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let mut state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: None,
            token_b_decimals: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: None,
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: None,
            activation_type: None,
            pool_status: None,
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: None,
            liquidity: None,
            sqrt_min_price_x64: None,
            sqrt_max_price_x64: None,
            vault_a_balance: 1,
            vault_b_balance: 1,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: None,
            hydrated_meta_slot: None,
            last_block_time_unix: None,
        };

        let other_mint = Pubkey::new_unique();
        assert!(state.resolve_reserves(&other_mint, &mint_b).is_none());
        state.lp_fee_bps = Some(9_999);
        state.protocol_fee_bps = Some(9_999);
        assert_eq!(state.total_fee_bps(), 10_000);
    }

    #[test]
    fn mid_price_prefers_sqrt_price_with_decimals() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let sqrt_price = 2_f64.sqrt();
        let sqrt_price_x64 = (sqrt_price * Q64_FACTOR).round() as u128;

        let state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: Some(6),
            token_b_decimals: Some(6),
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: None,
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: None,
            activation_type: None,
            pool_status: None,
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(sqrt_price_x64),
            liquidity: Some(9_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 2_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        let price = state.mid_price().expect("mid price should compute");
        assert!((price - 2.0).abs() < 1e-6);
    }

    #[test]
    fn mid_price_applies_decimal_adjustment() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let sqrt_price_x64 = (1_f64 * Q64_FACTOR).round() as u128;

        let state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: Some(9),
            token_b_decimals: Some(6),
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: None,
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: None,
            activation_type: None,
            pool_status: None,
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(sqrt_price_x64),
            liquidity: Some(9_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000,
            vault_b_balance: 1_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        let price = state.mid_price().expect("mid price should compute");
        assert!((price - 0.001).abs() < 1e-9);
    }

    #[test]
    fn mid_price_falls_back_to_vault_ratio() {
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();

        let state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: Some(6),
            token_b_decimals: Some(6),
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: None,
            protocol_fee_bps: None,
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: None,
            activation_type: None,
            pool_status: None,
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: None,
            liquidity: None,
            sqrt_min_price_x64: None,
            sqrt_max_price_x64: None,
            vault_a_balance: 500,
            vault_b_balance: 1_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: None,
            hydrated_meta_slot: None,
            last_block_time_unix: None,
        };

        let price = state.mid_price().expect("mid price should fall back");
        assert!((price - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn bidirectional_quotes_generate_snapshot_sides() {
        let adapter = CpAmmAdapter::new(CpAmmAdapterConfig::default(), None);
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();

        let state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: None,
            token_b_decimals: None,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: Some(5),
            referral_bps: Some(5),
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: None,
            activation_type: None,
            pool_status: None,
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(9_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 2_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        adapter.state.insert(pool, state.clone());

        let snapshot = PoolSnapshot {
            pool_id: pool.to_string(),
            program_id: CP_AMM_PROGRAM_ID.to_string(),
            slot: 42,
            block_hash: None,
            block_time_unix: None,
            base_mint: mint_a.to_string(),
            quote_mint: mint_b.to_string(),
            base_post_amount: state.vault_a_balance,
            quote_post_amount: state.vault_b_balance,
            updated_at: Instant::now(),
            freshness: SnapshotFreshness::LiveCorecast,
            allow_reserve_backfill: true,
            quotes: None,
            hydration: HydrationStatus::new(42),
            extensions: PoolUpdateExtensions::default(),
        };

        let quotes = adapter
            .bidirectional_quotes(&snapshot)
            .expect("quotes should be generated");

        let expected_base_probe =
            snapshot_probe_amount(state.vault_a_balance, CP_AMM_INPUT_UTILIZATION_BPS).unwrap();
        let expected_quote_probe =
            snapshot_probe_amount(state.vault_b_balance, CP_AMM_INPUT_UTILIZATION_BPS).unwrap();

        assert!(quotes.base_to_quote.notional <= expected_base_probe);
        assert!(quotes.quote_to_base.notional <= expected_quote_probe);
        assert_eq!(quotes.base_to_quote.fee_bps, state.total_fee_bps());
        assert!(quotes.base_to_quote.expected_output > 0);
        assert!(quotes.quote_to_base.expected_output > 0);
    }

    #[test]
    fn quote_exact_in_rejects_partner_mismatch() {
        let required_partner = Pubkey::new_unique();
        let mut adapter_config = CpAmmAdapterConfig::default();
        adapter_config.required_partner = Some(required_partner);
        let adapter = CpAmmAdapter::new(adapter_config, None);

        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: mint_a,
            output_mint: mint_b,
            amount: 100_000,
            slot: 5,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let mut state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: Some(Pubkey::new_unique()),
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: Some(6),
            token_b_decimals: Some(6),
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: Some(5),
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: Some(0),
            activation_type: Some(ActivationType::Slot as u8),
            pool_status: Some(PoolStatusFlag::Enable as u8),
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(5_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 1_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        adapter.state.insert(pool, state);
        let quote = adapter.quote_exact_in(req).expect("quote produced");
        assert_eq!(quote.status, QuoteStatus::MissingPartner);
    }

    #[test]
    fn quote_exact_in_allows_matching_partner() {
        let required_partner = Pubkey::new_unique();
        let mut adapter_config = CpAmmAdapterConfig::default();
        adapter_config.required_partner = Some(required_partner);
        let adapter = CpAmmAdapter::new(adapter_config, None);

        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: mint_a,
            output_mint: mint_b,
            amount: 200_000,
            slot: 8,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: Some(required_partner),
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: Some(6),
            token_b_decimals: Some(6),
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: Some(5),
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: None,
            activation_point: Some(0),
            activation_type: Some(ActivationType::Slot as u8),
            pool_status: Some(PoolStatusFlag::Enable as u8),
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(6_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 2_000_000,
            vault_b_balance: 2_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        adapter.state.insert(pool, state);
        let quote = adapter.quote_exact_in(req).expect("quote produced");
        assert_eq!(quote.status, QuoteStatus::Ok);
        assert!(quote.amount_out > 0);
    }

    #[test]
    fn quote_exact_in_reports_stale_state_status() {
        let mut config = CpAmmAdapterConfig::default();
        config.hydration_meta_ttl_ms = 1;
        let adapter = CpAmmAdapter::new(config, None);

        let mut state = fixture_rate_limiter_state();
        state.last_hydrated_at = Some(Instant::now() - Duration::from_millis(10));
        adapter.state.insert(state.pool, state.clone());

        let req = QuoteRequest {
            pool_id: state.pool,
            input_mint: state.token_a_mint,
            output_mint: state.token_b_mint,
            amount: 1_000,
            slot: 5,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let quote = adapter.quote_exact_in(req).expect("quote produced");
        assert_eq!(quote.status, QuoteStatus::StaleState);
        assert_eq!(quote.amount_out, 0);
    }

    #[test]
    fn quote_exact_in_reports_slot_staleness() {
        let mut config = CpAmmAdapterConfig::default();
        config.stale_slot_threshold = 1;
        let adapter = CpAmmAdapter::new(config, None);

        let mut state = fixture_rate_limiter_state();
        state.last_update_slot = 5;
        state.corecast_slot = state.last_update_slot;
        state.hydrated_meta_slot = Some(state.corecast_slot);
        adapter.state.insert(state.pool, state.clone());

        let req = QuoteRequest {
            pool_id: state.pool,
            input_mint: state.token_a_mint,
            output_mint: state.token_b_mint,
            amount: 2_000,
            slot: 10,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let quote = adapter.quote_exact_in(req).expect("quote produced");
        assert_eq!(quote.status, QuoteStatus::StaleState);
        assert_eq!(quote.amount_out, 0);
    }

    #[test]
    fn quote_exact_out_sets_min_output_amount() {
        let adapter = CpAmmAdapter::new(CpAmmAdapterConfig::default(), None);

        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: mint_a,
            output_mint: mint_b,
            amount: 150_000,
            slot: 12,
            block_time: None,
            direction: SwapDirection::ExactOut,
        };

        let state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
            whitelisted_vault: None,
            partner: None,
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            token_a_program: None,
            token_b_program: None,
            token_a_decimals: Some(6),
            token_b_decimals: Some(6),
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
            amp: None,
            lp_fee_bps: Some(30),
            protocol_fee_bps: Some(5),
            referral_bps: None,
            referral_token_account: None,
            collect_fee_mode: Some(CollectFeeModeFlag::BothToken as u8),
            activation_point: Some(0),
            activation_type: Some(ActivationType::Slot as u8),
            pool_status: Some(PoolStatusFlag::Enable as u8),
            pool_type: None,
            pool_version: None,
            token_a_flag: None,
            token_b_flag: None,
            permanent_lock_liquidity: None,
            pool_fees: None,
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(6_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 2_000_000,
            vault_b_balance: 2_000_000,
            corecast_slot: 0,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
            hydrated_meta_slot: Some(0),
            last_block_time_unix: None,
        };

        adapter.state.insert(pool, state);
        let quote = adapter.quote_exact_out(req).expect("quote produced");
        assert_eq!(quote.status, QuoteStatus::Ok);
        assert!(quote.amount_out > 0);
        assert_eq!(quote.min_output_amount, Some(quote.amount_out));
    }

    #[test]
    fn quote_exact_out_reports_trading_disabled_status() {
        let adapter = CpAmmAdapter::new(CpAmmAdapterConfig::default(), None);

        let mut state = fixture_rate_limiter_state();
        state.pool_status = Some(PoolStatusFlag::Disable as u8);
        adapter.state.insert(state.pool, state.clone());

        let requested_amount = 5_000;
        let req = QuoteRequest {
            pool_id: state.pool,
            input_mint: state.token_a_mint,
            output_mint: state.token_b_mint,
            amount: requested_amount,
            slot: 10,
            block_time: None,
            direction: SwapDirection::ExactOut,
        };

        let quote = adapter.quote_exact_out(req).expect("quote produced");
        assert_eq!(quote.status, QuoteStatus::TradingDisabled);
        assert_eq!(quote.amount_out, 0);
        assert_eq!(quote.amount_in, requested_amount);
    }

    #[test]
    fn quote_exact_out_reports_time_staleness() {
        let mut config = CpAmmAdapterConfig::default();
        config.stale_after_ms = 1;
        let adapter = CpAmmAdapter::new(config, None);

        let mut state = fixture_rate_limiter_state();
        state.last_update_at = Instant::now() - Duration::from_millis(10);
        adapter.state.insert(state.pool, state.clone());

        let req = QuoteRequest {
            pool_id: state.pool,
            input_mint: state.token_a_mint,
            output_mint: state.token_b_mint,
            amount: 4_000,
            slot: 2,
            block_time: None,
            direction: SwapDirection::ExactOut,
        };

        let quote = adapter.quote_exact_out(req).expect("quote produced");
        assert_eq!(quote.status, QuoteStatus::StaleState);
        assert_eq!(quote.amount_out, 0);
    }
}
