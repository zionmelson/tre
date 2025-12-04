use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022::extension::{
    transfer_fee::{TransferFee, TransferFeeConfig},
    BaseStateWithExtensions, StateWithExtensionsOwned,
};
use spl_token_2022::state::Mint as SplToken2022Mint;
use tracing::{debug, info, trace, warn};
use uint::construct_uint;

use super::hydration::AsyncHydrator;
use crate::builder::{BuilderHandle, CpAmmPoolAccounts, CpAmmPoolMeta};
use crate::hydration::types::{
    CpAmmPoolExtras, PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate,
    CP_AMM_INPUT_UTILIZATION_BPS,
};
use crate::template::TemplateContext;

use super::{
    build_quote_response, ProtocolAdapter, QuoteRequest, QuoteResponse, QuoteStatus, SwapDirection,
};

const BPS_DENOMINATOR: u64 = 10_000;
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
        let transfer_fee_cache = TokenTransferFeeHydrator::new(&config);
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
            sqrt_price_x64: extras.sqrt_price_x64,
            liquidity: extras.liquidity,
            sqrt_min_price_x64: extras.sqrt_min_price_x64,
            sqrt_max_price_x64: extras.sqrt_max_price_x64,
            vault_a_balance: update.base_post_amount,
            vault_b_balance: update.quote_post_amount,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
            last_hydrated_at: extras.hydrated_at,
            token_a_transfer_fee: None,
            token_b_transfer_fee: None,
        };

        self.attach_transfer_fees(&mut state);
        self.attach_mint_metadata(&mut state);

        trace!(
            target = "adapter::cp_amm",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting cp_amm pool state"
        );

        self.register_templates_with_builder(&state);
        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: CpAmmPoolExtras) -> CpAmmPoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.token_a_mint = extras
                .token_a_mint
                .or_else(|| Some(meta.token_a_mint.to_string()));
            extras.token_b_mint = extras
                .token_b_mint
                .or_else(|| Some(meta.token_b_mint.to_string()));
            extras.token_a_vault = extras
                .token_a_vault
                .or_else(|| Some(meta.token_a_vault.to_string()));
            extras.token_b_vault = extras
                .token_b_vault
                .or_else(|| Some(meta.token_b_vault.to_string()));
            extras.sqrt_price_x64 = extras.sqrt_price_x64.or_else(|| Some(meta.sqrt_price_x64));
            extras.liquidity = extras.liquidity.or_else(|| Some(meta.liquidity));
            extras.sqrt_min_price_x64 = extras
                .sqrt_min_price_x64
                .or_else(|| Some(meta.sqrt_min_price_x64));
            extras.sqrt_max_price_x64 = extras
                .sqrt_max_price_x64
                .or_else(|| Some(meta.sqrt_max_price_x64));
            extras.hydrated_at = extras.hydrated_at.or(Some(meta.hydrated_at));
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

    fn attach_transfer_fees(&self, state: &mut CpAmmState) {
        state.token_a_transfer_fee =
            self.resolve_transfer_fee(state.token_a_mint, state.token_a_program);
        state.token_b_transfer_fee =
            self.resolve_transfer_fee(state.token_b_mint, state.token_b_program);
    }

    fn attach_mint_metadata(&self, state: &mut CpAmmState) {
        if state.token_a_decimals.is_none() {
            state.token_a_decimals = self.resolve_mint_decimals(state.token_a_mint);
        }
        if state.token_b_decimals.is_none() {
            state.token_b_decimals = self.resolve_mint_decimals(state.token_b_mint);
        }
    }

    fn resolve_transfer_fee(
        &self,
        mint: Pubkey,
        program: Option<Pubkey>,
    ) -> Option<TokenTransferFee> {
        let Some(cache) = self.transfer_fee_cache.as_ref() else {
            return None;
        };
        let program_id = program.unwrap_or(*SPL_TOKEN_PROGRAM_PUBKEY);
        if program_id != *SPL_TOKEN_2022_PROGRAM_PUBKEY {
            return None;
        }
        cache.hydrate(mint).and_then(|meta| meta.fee)
    }

    fn resolve_mint_decimals(&self, mint: Pubkey) -> Option<u8> {
        let Some(cache) = self.mint_metadata_cache.as_ref() else {
            return None;
        };
        cache.hydrate(mint).map(|meta| meta.decimals)
    }

    fn decorate_template_context(
        &self,
        state: &CpAmmState,
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

    fn build_snapshot_side(
        &self,
        state: &CpAmmState,
        pool_id: Pubkey,
        slot: u64,
        base_to_quote: bool,
        probe_amount: u64,
    ) -> Option<PoolQuoteSide> {
        if probe_amount == 0 {
            return None;
        }
        if state.hydration_stale(self.config.hydration_meta_ttl_ms) {
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
            direction: SwapDirection::ExactIn,
        };

        let simulation = simulate_cp_amm_exact_in(state, &req, true)?;
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
            context,
            status: QuoteStatus::Ok,
            sqrt_price_before_x64: None,
            sqrt_price_after_x64: None,
            tick_arrays_used: Vec::new(),
            cache_slot: Some(slot),
        })
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

        let extras = update.extensions.cp_amm.clone().unwrap_or_default();
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

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if state.hydration_stale(self.config.hydration_meta_ttl_ms) {
            return None;
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
        if state.hydration_stale(self.config.hydration_meta_ttl_ms) {
            return None;
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
        let simulation = simulate_cp_amm_exact_out(&state, &request)?;
        let mut response = QuoteResponse {
            amount_in: simulation.executed_input_amount,
            amount_out: simulation.output_amount,
            edge_bps: simulation.edge_bps,
            sqrt_price_limit: Some(simulation.sqrt_price_after),
            min_output_amount: None,
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
        let state = self.get_state(&pool_pubkey)?;
        if state.hydration_stale(self.config.hydration_meta_ttl_ms) {
            return None;
        }

        let utilization = self.config.max_swap_utilization_bps;
        let base_probe = snapshot_probe_amount(state.vault_a_balance, utilization)?;
        let quote_probe = snapshot_probe_amount(state.vault_b_balance, utilization)?;

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

#[derive(Debug, Clone)]
struct SimulatedSwapQuote {
    pub executed_input_amount: u64,
    pub net_input_amount: u64,
    pub output_amount: u64,
    pub fee_amount: u64,
    pub token_in_fee_amount: u64,
    pub token_out_fee_amount: u64,
    pub sqrt_price_after: u128,
    pub amount_leftover: u64,
    pub edge_bps: f64,
    pub slippage_bps: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct TokenTransferFee {
    fee: TransferFee,
}

impl TokenTransferFee {
    fn new(fee: TransferFee) -> Self {
        Self { fee }
    }

    fn post_fee_amount(&self, pre_fee_amount: u64) -> Option<(u64, u64)> {
        let fee_amount = self.fee.calculate_fee(pre_fee_amount)?;
        let net = pre_fee_amount.checked_sub(fee_amount)?;
        Some((net, fee_amount))
    }

    fn pre_fee_amount(&self, post_fee_amount: u64) -> Option<(u64, u64)> {
        let gross = self.fee.calculate_pre_fee_amount(post_fee_amount)?;
        let fee_amount = gross.checked_sub(post_fee_amount)?;
        Some((gross, fee_amount))
    }
}

fn apply_post_fee_amount(amount: u64, fee: Option<TokenTransferFee>) -> Option<(u64, u64)> {
    match fee {
        Some(fee) => fee.post_fee_amount(amount),
        None => Some((amount, 0)),
    }
}

fn apply_pre_fee_amount(amount: u64, fee: Option<TokenTransferFee>) -> Option<(u64, u64)> {
    match fee {
        Some(fee) => fee.pre_fee_amount(amount),
        None => Some((amount, 0)),
    }
}

#[derive(Debug, Clone)]
struct ClmmStep {
    pub amount_out: u64,
    pub net_input_amount: u64,
    pub fee_amount: u64,
    pub sqrt_price_after: u128,
    pub hit_price_limit: bool,
}

fn referral_missing(state: &CpAmmState) -> bool {
    state.referral_token_account.is_none() && state.referral_bps.unwrap_or(0) > 0
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
    if req.amount == 0 {
        return None;
    }

    let direction = resolve_trade_direction(state, &req.input_mint, &req.output_mint)?;
    let (reserve_in, reserve_out) = state.resolve_reserves(&req.input_mint, &req.output_mint)?;
    if reserve_in == 0 || reserve_out == 0 {
        return None;
    }

    let input_fee = state.transfer_fee_for_mint(&req.input_mint);
    let output_fee = state.transfer_fee_for_mint(&req.output_mint);

    let (post_fee_budget, _) = apply_post_fee_amount(req.amount, input_fee)?;
    if post_fee_budget == 0 {
        return None;
    }

    let step = simulate_clmm_exact_in(state, post_fee_budget, direction)?;
    let pool_consumed = step.net_input_amount.saturating_add(step.fee_amount);

    let (executed_input_amount, token_in_fee_amount) =
        apply_pre_fee_amount(pool_consumed, input_fee)?;
    let amount_leftover = req.amount.saturating_sub(executed_input_amount);
    if !allow_partial && amount_leftover > 0 {
        return None;
    }

    let (output_amount, token_out_fee_amount) = apply_post_fee_amount(step.amount_out, output_fee)?;
    if output_amount == 0 {
        return None;
    }

    let slippage = compute_slippage_bps(reserve_in, reserve_out, pool_consumed, step.amount_out)
        .unwrap_or(0.0);
    let mid_price = direction_mid_price(state, direction)?;
    let execution_price =
        scaled_execution_price(state, direction, output_amount, executed_input_amount)?;
    let edge_bps = compute_edge_from_prices(mid_price, execution_price);

    Some(SimulatedSwapQuote {
        executed_input_amount,
        net_input_amount: step.net_input_amount,
        output_amount,
        fee_amount: step.fee_amount,
        token_in_fee_amount,
        token_out_fee_amount,
        sqrt_price_after: step.sqrt_price_after,
        amount_leftover,
        edge_bps,
        slippage_bps: slippage,
    })
}

fn simulate_cp_amm_exact_out(state: &CpAmmState, req: &QuoteRequest) -> Option<SimulatedSwapQuote> {
    if req.amount == 0 {
        return None;
    }

    let direction = resolve_trade_direction(state, &req.input_mint, &req.output_mint)?;
    let (reserve_in, reserve_out) = state.resolve_reserves(&req.input_mint, &req.output_mint)?;
    if reserve_in == 0 || reserve_out == 0 {
        return None;
    }
    let input_fee = state.transfer_fee_for_mint(&req.input_mint);
    let output_fee = state.transfer_fee_for_mint(&req.output_mint);

    let (gross_target_out, _) = apply_pre_fee_amount(req.amount, output_fee)?;
    if gross_target_out == 0 || gross_target_out as u128 >= reserve_out {
        return None;
    }

    let step = find_input_for_exact_out(state, direction, gross_target_out)?;
    let pool_consumed = step.net_input_amount.saturating_add(step.fee_amount);

    let (executed_input_amount, token_in_fee_amount) =
        apply_pre_fee_amount(pool_consumed, input_fee)?;
    let (output_amount, token_out_fee_amount) = apply_post_fee_amount(step.amount_out, output_fee)?;
    if output_amount < req.amount {
        return None;
    }

    let slippage = compute_slippage_bps(reserve_in, reserve_out, pool_consumed, step.amount_out)
        .unwrap_or(0.0);
    let mid_price = direction_mid_price(state, direction)?;
    let execution_price =
        scaled_execution_price(state, direction, output_amount, executed_input_amount)?;
    let edge_bps = compute_edge_from_prices(mid_price, execution_price);

    Some(SimulatedSwapQuote {
        executed_input_amount,
        net_input_amount: step.net_input_amount,
        output_amount,
        fee_amount: step.fee_amount,
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
        let step = simulate_clmm_exact_in(state, hi, direction)?;
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
        let step = match simulate_clmm_exact_in(state, mid, direction) {
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
        state.total_fee_bps(),
        zero_for_one,
    )?;
    if step.amount_out == 0 {
        return None;
    }
    let consumed = step.amount_in.saturating_add(step.fee_amount);
    let hit_price_cap = step.sqrt_price_next == sqrt_price_limit;
    if hit_price_cap && consumed < amount_in as u128 {
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

#[derive(Clone, Copy, Debug)]
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

fn needs_hydration(extras: &CpAmmPoolExtras) -> bool {
    extras.token_a_mint.is_none()
        || extras.token_b_mint.is_none()
        || extras.token_a_vault.is_none()
        || extras.token_b_vault.is_none()
        || extras.sqrt_price_x64.is_none()
        || extras.liquidity.is_none()
        || extras.sqrt_min_price_x64.is_none()
        || extras.sqrt_max_price_x64.is_none()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    token_a_mint: Pubkey,
    token_b_mint: Pubkey,
    token_a_vault: Pubkey,
    token_b_vault: Pubkey,
    sqrt_price_x64: u128,
    liquidity: u128,
    sqrt_min_price_x64: u128,
    sqrt_max_price_x64: u128,
    hydrated_at: Instant,
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
                if let Some(meta) = parse_pool_account(&account.data) {
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
struct TransferFeeMeta {
    fee: Option<TokenTransferFee>,
    hydrated_at: Instant,
}

struct TokenTransferFeeHydrator {
    hydrator: AsyncHydrator<Pubkey, TransferFeeMeta>,
    retry_backoff: Duration,
    last_attempts: DashMap<Pubkey, Instant>,
}

impl TokenTransferFeeHydrator {
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
        let hydrator = AsyncHydrator::new("token_fee", endpoint_count, move |mint| {
            fetch_token_transfer_fee_meta(fetch_clients.as_ref(), mint)
        });

        Some(Self {
            hydrator,
            retry_backoff: Duration::from_millis(config.hydration_retry_ms.max(1)),
            last_attempts: DashMap::new(),
        })
    }

    fn hydrate(&self, mint: Pubkey) -> Option<TransferFeeMeta> {
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

fn fetch_token_transfer_fee_meta(
    clients: &[Arc<RpcClient>],
    mint: Pubkey,
) -> Option<TransferFeeMeta> {
    for client in clients {
        match client.get_account(&mint) {
            Ok(account) => {
                if account.owner != *SPL_TOKEN_2022_PROGRAM_PUBKEY {
                    return Some(TransferFeeMeta {
                        fee: None,
                        hydrated_at: Instant::now(),
                    });
                }
                match client.get_epoch_info() {
                    Ok(epoch_info) => {
                        let fee = parse_transfer_fee_extension(&account.data, epoch_info.epoch);
                        return Some(TransferFeeMeta {
                            fee,
                            hydrated_at: Instant::now(),
                        });
                    }
                    Err(error) => {
                        warn!(
                            target = "adapter::cp_amm",
                            mint = %mint,
                            %error,
                            "failed to fetch epoch info for token fee metadata"
                        );
                    }
                }
            }
            Err(error) => {
                warn!(
                    target = "adapter::cp_amm",
                    mint = %mint,
                    %error,
                    "failed to fetch mint account for token fee metadata"
                );
            }
        }
    }

    None
}

fn parse_transfer_fee_extension(data: &[u8], epoch: u64) -> Option<TokenTransferFee> {
    let state = StateWithExtensionsOwned::<SplToken2022Mint>::unpack(data.to_vec()).ok()?;
    let config = state.get_extension::<TransferFeeConfig>().ok()?;
    let fee = *config.get_epoch_fee(epoch);
    Some(TokenTransferFee::new(fee))
}

#[derive(Clone, Debug)]
struct MintMetadata {
    decimals: u8,
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
                if let Some(decimals) = parse_mint_decimals(&owner, &account.data) {
                    return Some(MintMetadata {
                        decimals,
                        hydrated_at: Instant::now(),
                    });
                } else {
                    warn!(
                        target = "adapter::cp_amm",
                        mint = %mint,
                        program = %owner,
                        "unsupported mint program for metadata hydration"
                    );
                }
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
const CP_AMM_POOL_ACCOUNT_MIN_LEN: usize =
    CP_AMM_POOL_ACCOUNT_HEADER_LEN + (PUBKEY_BYTES * 6) + (U128_BYTES * 5) + (U64_BYTES * 4);

fn parse_pool_account(data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < CP_AMM_POOL_ACCOUNT_MIN_LEN {
        return None;
    }

    let mut cursor = CP_AMM_POOL_ACCOUNT_HEADER_LEN;
    let token_a_mint = read_pubkey_from_slice(data, &mut cursor)?;
    let token_b_mint = read_pubkey_from_slice(data, &mut cursor)?;
    let token_a_vault = read_pubkey_from_slice(data, &mut cursor)?;
    let token_b_vault = read_pubkey_from_slice(data, &mut cursor)?;
    let _whitelisted_vault = read_pubkey_from_slice(data, &mut cursor)?;
    let _partner = read_pubkey_from_slice(data, &mut cursor)?;
    let liquidity = read_u128_from_slice(data, &mut cursor)?;
    let _padding = read_u128_from_slice(data, &mut cursor)?;
    // Skip protocol + partner fee accruals
    read_u64_from_slice(data, &mut cursor)?;
    read_u64_from_slice(data, &mut cursor)?;
    read_u64_from_slice(data, &mut cursor)?;
    read_u64_from_slice(data, &mut cursor)?;
    let sqrt_min_price_x64 = read_u128_from_slice(data, &mut cursor)?;
    let sqrt_max_price_x64 = read_u128_from_slice(data, &mut cursor)?;
    let sqrt_price_x64 = read_u128_from_slice(data, &mut cursor)?;

    Some(HydratedPoolMeta {
        token_a_mint,
        token_b_mint,
        token_a_vault,
        token_b_vault,
        sqrt_price_x64,
        liquidity,
        sqrt_min_price_x64,
        sqrt_max_price_x64,
        hydrated_at: Instant::now(),
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

#[derive(Debug, Clone)]
pub struct CpAmmState {
    pub pool: Pubkey,
    pub pool_authority: Option<Pubkey>,
    pub token_a_vault: Option<Pubkey>,
    pub token_b_vault: Option<Pubkey>,
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
    pub sqrt_price_x64: Option<u128>,
    pub liquidity: Option<u128>,
    pub sqrt_min_price_x64: Option<u128>,
    pub sqrt_max_price_x64: Option<u128>,
    pub vault_a_balance: u128,
    pub vault_b_balance: u128,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
    pub last_hydrated_at: Option<Instant>,
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
        let lp = self.lp_fee_bps.unwrap_or(0) as u64;
        let protocol = self.protocol_fee_bps.unwrap_or(0) as u64;
        let referral = self.referral_bps.unwrap_or(0) as u64;
        lp.saturating_add(protocol)
            .saturating_add(referral)
            .min(BPS_DENOMINATOR)
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

    fn hydration_stale(&self, ttl_ms: u64) -> bool {
        if ttl_ms == 0 {
            return false;
        }
        match self.last_hydrated_at {
            Some(instant) => instant.elapsed().as_millis() as u64 > ttl_ms,
            None => true,
        }
    }
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
    use crate::hydration::types::{PoolSnapshot, PoolUpdateExtensions, SnapshotFreshness};
    use spl_pod::primitives::{PodU16, PodU64};

    fn fixture_transfer_fee(bps: u16, max_fee: u64) -> TokenTransferFee {
        let fee = TransferFee {
            epoch: PodU64::from(0),
            maximum_fee: PodU64::from(max_fee),
            transfer_fee_basis_points: PodU16::from(bps),
        };
        TokenTransferFee::new(fee)
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
            direction: SwapDirection::ExactIn,
        };

        let state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
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
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(5_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 1_000_000,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
        };

        let simulation = simulate_cp_amm_exact_in(&state, &req, false).expect("swap should quote");
        assert!(simulation.output_amount > 0);
        assert!(simulation.edge_bps.is_sign_negative());
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
            direction: SwapDirection::ExactIn,
        };

        let state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
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
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(8_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 2_000_000,
            vault_b_balance: 3_000_000,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
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
            direction: SwapDirection::ExactIn,
        };

        let mut state = CpAmmState {
            pool,
            pool_authority: None,
            token_a_vault: None,
            token_b_vault: None,
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
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(7_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 3_000_000,
            vault_b_balance: 5_000_000,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
        };

        let simulation =
            simulate_cp_amm_exact_in(&state, &req, false).expect("quote should succeed");
        assert!(simulation.token_in_fee_amount > 0);
        assert!(simulation.token_out_fee_amount > 0);
        let total_reported = simulation
            .net_input_amount
            .saturating_add(simulation.fee_amount)
            .saturating_add(simulation.token_in_fee_amount);
        assert_eq!(simulation.executed_input_amount, total_reported);
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
            sqrt_price_x64: None,
            liquidity: None,
            sqrt_min_price_x64: None,
            sqrt_max_price_x64: None,
            vault_a_balance: 1,
            vault_b_balance: 1,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: None,
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
            sqrt_price_x64: Some(sqrt_price_x64),
            liquidity: Some(9_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 2_000_000,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
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
            sqrt_price_x64: Some(sqrt_price_x64),
            liquidity: Some(9_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000,
            vault_b_balance: 1_000,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
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
            sqrt_price_x64: None,
            liquidity: None,
            sqrt_min_price_x64: None,
            sqrt_max_price_x64: None,
            vault_a_balance: 500,
            vault_b_balance: 1_000,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: None,
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
            sqrt_price_x64: Some(1_u128 << 64),
            liquidity: Some(9_000_000_000),
            sqrt_min_price_x64: Some(1_u128 << 60),
            sqrt_max_price_x64: Some(1_u128 << 70),
            vault_a_balance: 1_000_000,
            vault_b_balance: 2_000_000,
            last_update_slot: 0,
            last_update_at: Instant::now(),
            last_hydrated_at: Some(Instant::now()),
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
}
