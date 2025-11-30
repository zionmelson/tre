use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, trace, warn};

use crate::builder::{
    templates::SwapTemplateDirection, BuilderHandle, LifinityPoolAccounts, LifinityPoolMeta,
};
use crate::stages::types::{
    LifinityPoolExtras, PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate,
    LIFINITY_V2_INPUT_UTILIZATION_BPS,
};
use crate::template::TemplateContext;

use super::{
    build_quote_response, hydration::AsyncHydrator, ProtocolAdapter, QuoteRequest, QuoteResponse,
    SwapDirection,
};

const LIFINITY_PROGRAM_ID: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const BPS_DENOMINATOR: u64 = 10_000;
const SNAPSHOT_PROBE_DIVISOR: u128 = 5_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;
const LIFINITY_DEFAULT_CU_LIMIT: u32 = 320_000;
const LIFINITY_DEFAULT_CU_PRICE: u64 = 5_000;

const DEFAULT_LIFINITY_HYDRATION_RETRY_MS: u64 = 60_000;

static SPL_TOKEN_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str(SPL_TOKEN_PROGRAM_ID).expect("static spl token program should be valid")
});

#[derive(Debug, Clone)]
pub struct LifinityAdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
    pub oracle_stale_after_ms: u64,
    pub hydration_retry_ms: u64,
    pub max_swap_utilization_bps: u64,
}

impl Default for LifinityAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 15_000,
            oracle_stale_after_ms: 1_000,
            hydration_retry_ms: DEFAULT_LIFINITY_HYDRATION_RETRY_MS,
            max_swap_utilization_bps: LIFINITY_V2_INPUT_UTILIZATION_BPS,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LifinityRegistrationState {
    fingerprint: u64,
}

pub struct LifinityAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, LifinityPoolState>,
    config: LifinityAdapterConfig,
    hydrator: Option<LifinityPoolHydrator>,
    oracle_cache: Option<LifinityOracleCache>,
    template_builder: Option<BuilderHandle>,
    registered_templates: DashMap<Pubkey, LifinityRegistrationState>,
}

impl LifinityAdapter {
    pub fn new(config: LifinityAdapterConfig, template_builder: Option<BuilderHandle>) -> Self {
        let program_id = Pubkey::from_str(LIFINITY_PROGRAM_ID)
            .expect("static Lifinity AMM v2 program id should be valid");
        let hydrator = LifinityPoolHydrator::new(program_id, &config);
        let oracle_cache = LifinityOracleCache::new(&config);
        if let Some(hydrator) = hydrator.as_ref() {
            info!(
                target = "adapter::lifinity",
                endpoints = hydrator.client_count(),
                "lifinity hydrator online"
            );
        } else {
            debug!(
                target = "adapter::lifinity",
                "lifinity hydrator disabled (no rpc endpoints)"
            );
        }
        if oracle_cache.is_some() {
            info!(target = "adapter::lifinity", "lifinity oracle cache online");
        } else {
            debug!(
                target = "adapter::lifinity",
                "lifinity oracle cache disabled (no rpc endpoints)"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            hydrator,
            oracle_cache,
            template_builder,
            registered_templates: DashMap::new(),
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<LifinityPoolState> {
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
                    target = "adapter::lifinity",
                    pool = %state.amm,
                    last_slot = state.last_update_slot,
                    "evicting stale Lifinity state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: LifinityPoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::lifinity",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let extras = self.enrich_extras(pool_pubkey, extras);

        let Some(base_mint) = parse_pubkey(&update.base_mint) else {
            warn!(target = "adapter::lifinity", pool = %update.pool_id, "invalid base mint");
            return;
        };

        let Some(quote_mint) = parse_pubkey(&update.quote_mint) else {
            warn!(target = "adapter::lifinity", pool = %update.pool_id, "invalid quote mint");
            return;
        };

        let state = LifinityPoolState {
            amm: pool_pubkey,
            base_mint,
            quote_mint,
            authority: extras
                .authority
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_a_vault: extras
                .token_a_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_b_vault: extras
                .token_b_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            pool_mint: extras
                .pool_mint
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            fee_account: extras
                .fee_account
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_program: extras
                .token_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            oracle_main: extras
                .oracle_main
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            oracle_sub: extras
                .oracle_sub
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            oracle_pc: extras
                .oracle_pc
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            fees_bps: extras.fees_bps,
            owner_fee_bps: extras.owner_fee_bps,
            host_fee_bps: extras.host_fee_bps,
            std_spread_bps: extras.std_spread_bps,
            spread_buffer_bps: extras.spread_buffer_bps,
            spread_coefficient_bps: extras.spread_coefficient_bps,
            price_buffer_coin_bps: extras.price_buffer_coin_bps,
            price_buffer_pc_bps: extras.price_buffer_pc_bps,
            fee_trade_bps: extras.fee_trade_bps,
            fee_platform_bps: extras.fee_platform_bps,
            oracle_main_slot_limit: extras.oracle_main_slot_limit,
            oracle_sub_confidence_limit: extras.oracle_sub_confidence_limit,
            oracle_sub_slot_limit: extras.oracle_sub_slot_limit,
            oracle_pc_confidence_limit: extras.oracle_pc_confidence_limit,
            oracle_pc_slot_limit: extras.oracle_pc_slot_limit,
            vault_a_balance: extras.vault_a_balance,
            vault_b_balance: extras.vault_b_balance,
            freeze_trade: extras.freeze_trade,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
        };

        trace!(
            target = "adapter::lifinity",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting Lifinity pool state"
        );

        self.register_templates_with_builder(&state);
        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: LifinityPoolExtras) -> LifinityPoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.authority = extras
                .authority
                .or_else(|| Some(meta.authority.to_string()));
            extras.token_program = extras
                .token_program
                .or_else(|| Some(meta.token_program.to_string()));
            extras.token_a_vault = extras
                .token_a_vault
                .or_else(|| Some(meta.token_a_vault.to_string()));
            extras.token_b_vault = extras
                .token_b_vault
                .or_else(|| Some(meta.token_b_vault.to_string()));
            extras.pool_mint = extras
                .pool_mint
                .or_else(|| Some(meta.pool_mint.to_string()));
            extras.fee_account = extras
                .fee_account
                .or_else(|| Some(meta.fee_account.to_string()));
            extras.oracle_main = extras
                .oracle_main
                .or_else(|| Some(meta.oracle_main.to_string()));
            extras.oracle_sub = extras
                .oracle_sub
                .or_else(|| Some(meta.oracle_sub.to_string()));
            extras.oracle_pc = extras
                .oracle_pc
                .or_else(|| Some(meta.oracle_pc.to_string()));
            extras.fees_bps = extras.fees_bps.or(meta.fees_bps);
            extras.owner_fee_bps = extras.owner_fee_bps.or(meta.owner_fee_bps);
            extras.host_fee_bps = extras.host_fee_bps.or(meta.host_fee_bps);
            extras.std_spread_bps = extras.std_spread_bps.or(meta.std_spread_bps);
            extras.spread_buffer_bps = extras.spread_buffer_bps.or(meta.spread_buffer_bps);
            extras.spread_coefficient_bps = extras
                .spread_coefficient_bps
                .or(meta.spread_coefficient_bps);
            extras.price_buffer_coin_bps =
                extras.price_buffer_coin_bps.or(meta.price_buffer_coin_bps);
            extras.price_buffer_pc_bps = extras.price_buffer_pc_bps.or(meta.price_buffer_pc_bps);
            extras.fee_trade_bps = extras.fee_trade_bps.or(meta.fee_trade_bps);
            extras.fee_platform_bps = extras.fee_platform_bps.or(meta.fee_platform_bps);
            extras.oracle_main_slot_limit = extras
                .oracle_main_slot_limit
                .or(meta.oracle_main_slot_limit);
            extras.oracle_sub_confidence_limit = extras
                .oracle_sub_confidence_limit
                .or(meta.oracle_sub_confidence_limit);
            extras.oracle_sub_slot_limit =
                extras.oracle_sub_slot_limit.or(meta.oracle_sub_slot_limit);
            extras.oracle_pc_confidence_limit = extras
                .oracle_pc_confidence_limit
                .or(meta.oracle_pc_confidence_limit);
            extras.oracle_pc_slot_limit = extras.oracle_pc_slot_limit.or(meta.oracle_pc_slot_limit);
            extras.freeze_trade = extras.freeze_trade.or(Some(meta.freeze_trade));
        }

        extras
    }

    fn register_templates_with_builder(&self, state: &LifinityPoolState) {
        let Some(builder) = self.template_builder.as_ref() else {
            return;
        };
        let (meta, fingerprint) = match self.build_lifinity_meta(state) {
            Some(value) => value,
            None => return,
        };

        let template_ready = self.templates_ready(builder, state.amm);
        if let Some(entry) = self.registered_templates.get(&state.amm) {
            if entry.fingerprint == fingerprint && template_ready {
                return;
            }
        }

        builder.register_lifinity_pool(meta);
        self.registered_templates
            .insert(state.amm, LifinityRegistrationState { fingerprint });
    }

    fn build_lifinity_meta(&self, state: &LifinityPoolState) -> Option<(LifinityPoolMeta, u64)> {
        macro_rules! require {
            ($value:expr, $label:literal) => {
                match $value {
                    Some(entry) => entry,
                    None => {
                        warn!(
                            target = "adapter::lifinity",
                            pool = %state.amm,
                            concat!(
                                "lifinity pool missing ",
                                $label,
                                " metadata; skipping registration"
                            )
                        );
                        return None;
                    }
                }
            };
        }

        let authority = require!(state.authority, "authority");
        let token_program = require!(state.token_program, "token_program");
        let token_a_vault = require!(state.token_a_vault, "token_a_vault");
        let token_b_vault = require!(state.token_b_vault, "token_b_vault");
        let pool_mint = require!(state.pool_mint, "pool_mint");
        let fee_account = require!(state.fee_account, "fee_account");
        let oracle_main = require!(state.oracle_main, "oracle_main");
        let oracle_sub = require!(state.oracle_sub, "oracle_sub");
        let oracle_pc = require!(state.oracle_pc, "oracle_pc");

        let mut alt_accounts = Vec::with_capacity(12);
        alt_accounts.push(state.amm);
        alt_accounts.push(state.base_mint);
        alt_accounts.push(state.quote_mint);
        alt_accounts.push(authority);
        alt_accounts.push(token_a_vault);
        alt_accounts.push(token_b_vault);
        alt_accounts.push(pool_mint);
        alt_accounts.push(fee_account);
        alt_accounts.push(token_program);
        alt_accounts.push(oracle_main);
        alt_accounts.push(oracle_sub);
        alt_accounts.push(oracle_pc);

        let fee_tier_bps = base_fee_bps(state).min(u64::from(u16::MAX)) as u16;
        let accounts = LifinityPoolAccounts {
            base_mint: state.base_mint,
            quote_mint: state.quote_mint,
            authority: Some(authority),
            token_program: Some(token_program),
            token_a_vault: Some(token_a_vault),
            token_b_vault: Some(token_b_vault),
            pool_mint: Some(pool_mint),
            fee_account: Some(fee_account),
            oracle_main: Some(oracle_main),
            oracle_sub: Some(oracle_sub),
            oracle_pc: Some(oracle_pc),
        };
        let fingerprint =
            Self::lifinity_registration_fingerprint(state.amm, &accounts, fee_tier_bps);

        Some((
            LifinityPoolMeta {
                pool_id: state.amm,
                fee_tier_bps,
                alt_accounts,
                alt_handle: None,
                accounts,
            },
            fingerprint,
        ))
    }

    fn templates_ready(&self, builder: &BuilderHandle, pool: Pubkey) -> bool {
        let forward = builder.template_readiness(&pool, SwapTemplateDirection::BaseIn);
        let reverse = builder.template_readiness(&pool, SwapTemplateDirection::BaseOut);
        forward.template_ready && forward.alt_ready && reverse.template_ready && reverse.alt_ready
    }

    fn lifinity_registration_fingerprint(
        pool: Pubkey,
        accounts: &LifinityPoolAccounts,
        fee_tier_bps: u16,
    ) -> u64 {
        let mut hasher = DefaultHasher::new();
        pool.hash(&mut hasher);
        fee_tier_bps.hash(&mut hasher);
        accounts.base_mint.hash(&mut hasher);
        accounts.quote_mint.hash(&mut hasher);
        accounts.authority.hash(&mut hasher);
        accounts.token_program.hash(&mut hasher);
        accounts.token_a_vault.hash(&mut hasher);
        accounts.token_b_vault.hash(&mut hasher);
        accounts.pool_mint.hash(&mut hasher);
        accounts.fee_account.hash(&mut hasher);
        accounts.oracle_main.hash(&mut hasher);
        accounts.oracle_sub.hash(&mut hasher);
        accounts.oracle_pc.hash(&mut hasher);
        hasher.finish()
    }
    fn decorate_template_context(
        &self,
        state: &LifinityPoolState,
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
                    target = "adapter::lifinity",
                    pool = %state.amm,
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
                    target = "adapter::lifinity",
                    pool = %state.amm,
                    mint = %output_mint,
                    "missing configured output token ATA"
                );
            }
        }
    }

    fn clamp_request_amount(&self, ctx: &SwapContext, amount: u64) -> u64 {
        clamp_amount_to_reserve(
            amount,
            ctx.input_reserve,
            self.config.max_swap_utilization_bps,
        )
    }

    fn ensure_oracles_healthy(&self, state: &LifinityPoolState, current_slot: u64) -> bool {
        let Some(cache) = self.oracle_cache.as_ref() else {
            return true;
        };

        let Some(main_key) = state.oracle_main else {
            trace!(target = "adapter::lifinity", pool = %state.amm, "missing oracle_main key");
            return false;
        };
        let Some(main_snapshot) = cache.snapshot(main_key) else {
            trace!(target = "adapter::lifinity", pool = %state.amm, oracle = %main_key, "missing oracle_main snapshot");
            return false;
        };
        if !slot_within_limit(
            current_slot,
            main_snapshot.publish_slot,
            state.oracle_main_slot_limit,
        ) {
            trace!(
                target = "adapter::lifinity",
                pool = %state.amm,
                oracle = %main_key,
                current_slot,
                publish_slot = main_snapshot.publish_slot,
                limit = ?state.oracle_main_slot_limit,
                "oracle_main slot limit exceeded"
            );
            return false;
        }

        let Some(sub_key) = state.oracle_sub else {
            trace!(target = "adapter::lifinity", pool = %state.amm, "missing oracle_sub key");
            return false;
        };
        let Some(sub_snapshot) = cache.snapshot(sub_key) else {
            trace!(target = "adapter::lifinity", pool = %state.amm, oracle = %sub_key, "missing oracle_sub snapshot");
            return false;
        };
        if !slot_within_limit(
            current_slot,
            sub_snapshot.publish_slot,
            state.oracle_sub_slot_limit,
        ) {
            trace!(
                target = "adapter::lifinity",
                pool = %state.amm,
                oracle = %sub_key,
                current_slot,
                publish_slot = sub_snapshot.publish_slot,
                limit = ?state.oracle_sub_slot_limit,
                "oracle_sub slot limit exceeded"
            );
            return false;
        }
        if !confidence_within_limit(sub_snapshot.confidence, state.oracle_sub_confidence_limit) {
            trace!(
                target = "adapter::lifinity",
                pool = %state.amm,
                oracle = %sub_key,
                confidence = sub_snapshot.confidence,
                limit = ?state.oracle_sub_confidence_limit,
                "oracle_sub confidence limit exceeded"
            );
            return false;
        }

        let Some(pc_key) = state.oracle_pc else {
            trace!(target = "adapter::lifinity", pool = %state.amm, "missing oracle_pc key");
            return false;
        };
        let Some(pc_snapshot) = cache.snapshot(pc_key) else {
            trace!(target = "adapter::lifinity", pool = %state.amm, oracle = %pc_key, "missing oracle_pc snapshot");
            return false;
        };
        if !slot_within_limit(
            current_slot,
            pc_snapshot.publish_slot,
            state.oracle_pc_slot_limit,
        ) {
            trace!(
                target = "adapter::lifinity",
                pool = %state.amm,
                oracle = %pc_key,
                current_slot,
                publish_slot = pc_snapshot.publish_slot,
                limit = ?state.oracle_pc_slot_limit,
                "oracle_pc slot limit exceeded"
            );
            return false;
        }
        if !confidence_within_limit(pc_snapshot.confidence, state.oracle_pc_confidence_limit) {
            trace!(
                target = "adapter::lifinity",
                pool = %state.amm,
                oracle = %pc_key,
                confidence = pc_snapshot.confidence,
                limit = ?state.oracle_pc_confidence_limit,
                "oracle_pc confidence limit exceeded"
            );
            return false;
        }

        true
    }

    fn build_snapshot_side(
        &self,
        state: &LifinityPoolState,
        pool_id: Pubkey,
        slot: u64,
        base_to_quote: bool,
        probe_amount: u64,
    ) -> Option<PoolQuoteSide> {
        if probe_amount == 0 {
            return None;
        }

        let (input_mint, output_mint) = if base_to_quote {
            (state.base_mint, state.quote_mint)
        } else {
            (state.quote_mint, state.base_mint)
        };

        let req = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: probe_amount,
            slot,
            direction: SwapDirection::ExactIn,
        };

        let ctx = select_reserves(state, &req)?;
        let expected_output = lifinity_amount_out(probe_amount, ctx, state)?;
        let fee_bps = total_effective_fee_bps(probe_amount, &ctx, state);
        let template_fee_bps = base_fee_bps(state).min(u64::from(u16::MAX)) as u16;

        let execution_price = if probe_amount == 0 {
            0.0
        } else {
            expected_output as f64 / probe_amount as f64
        };

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

        let slippage_bps = reference_price
            .map(|reference| compute_slippage_bps(reference, execution_price))
            .unwrap_or_default();

        Some(PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional: probe_amount,
            expected_output,
            fee_bps,
            template_fee_bps,
            effective_price: execution_price,
            estimated_slippage_bps: slippage_bps,
            sqrt_price_limit: None,
            min_output_amount: Some(expected_output),
            limit_price: None,
            price_impact_limit_bps: None,
            context: {
                let mut ctx = TemplateContext {
                    compute_unit_limit: Some(LIFINITY_DEFAULT_CU_LIMIT),
                    compute_unit_price: Some(LIFINITY_DEFAULT_CU_PRICE),
                    ..Default::default()
                };
                self.decorate_template_context(state, &mut ctx, input_mint, output_mint);
                ctx
            },
        })
    }
}

impl ProtocolAdapter for LifinityAdapter {
    fn name(&self) -> &'static str {
        "lifinity_amm_v2"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::lifinity",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        let extras = update.extensions.lifinity_v2.clone().unwrap_or_default();
        self.upsert_state(update, extras);
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if matches!(state.freeze_trade, Some(value) if value != 0) {
            return None;
        }

        if !self.ensure_oracles_healthy(&state, req.slot) {
            return None;
        }

        let ctx = select_reserves(&state, &req)?;
        let capped_amount = self.clamp_request_amount(&ctx, req.amount);
        if capped_amount == 0 {
            return None;
        }
        let mut request = req;
        request.amount = capped_amount;
        let input_mint = request.input_mint;
        let output_mint = request.output_mint;
        let amount_out = lifinity_amount_out(capped_amount, ctx, &state)?;
        let mut response = build_quote_response(request, amount_out);
        response.min_output_amount = Some(amount_out);
        response.context.compute_unit_limit = Some(LIFINITY_DEFAULT_CU_LIMIT);
        response.context.compute_unit_price = Some(LIFINITY_DEFAULT_CU_PRICE);
        self.decorate_template_context(&state, &mut response.context, input_mint, output_mint);
        Some(response)
    }

    fn bidirectional_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
        let state = self.get_state(&pool_pubkey)?;

        if !self.ensure_oracles_healthy(&state, snapshot.slot) {
            return None;
        }

        let base_reserve = state.vault_a_balance?;
        let quote_reserve = state.vault_b_balance?;

        let utilization = self.config.max_swap_utilization_bps;
        let base_probe = snapshot_probe_amount(base_reserve, utilization)?;
        let quote_probe = snapshot_probe_amount(quote_reserve, utilization)?;

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
pub struct LifinityPoolState {
    pub amm: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub authority: Option<Pubkey>,
    pub token_a_vault: Option<Pubkey>,
    pub token_b_vault: Option<Pubkey>,
    pub pool_mint: Option<Pubkey>,
    pub fee_account: Option<Pubkey>,
    pub token_program: Option<Pubkey>,
    pub oracle_main: Option<Pubkey>,
    pub oracle_sub: Option<Pubkey>,
    pub oracle_pc: Option<Pubkey>,
    pub fees_bps: Option<u16>,
    pub owner_fee_bps: Option<u16>,
    pub host_fee_bps: Option<u16>,
    pub std_spread_bps: Option<u16>,
    pub spread_buffer_bps: Option<u16>,
    pub spread_coefficient_bps: Option<u32>,
    pub price_buffer_coin_bps: Option<i32>,
    pub price_buffer_pc_bps: Option<i32>,
    pub fee_trade_bps: Option<u16>,
    pub fee_platform_bps: Option<u16>,
    pub oracle_main_slot_limit: Option<u64>,
    pub oracle_sub_confidence_limit: Option<u64>,
    pub oracle_sub_slot_limit: Option<u64>,
    pub oracle_pc_confidence_limit: Option<u64>,
    pub oracle_pc_slot_limit: Option<u64>,
    pub vault_a_balance: Option<u128>,
    pub vault_b_balance: Option<u128>,
    pub freeze_trade: Option<u8>,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
}

impl LifinityPoolState {
    pub fn mid_price(&self) -> Option<f64> {
        match (self.vault_a_balance, self.vault_b_balance) {
            (Some(a), Some(b)) if a > 0 => Some(b as f64 / a as f64),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwapSide {
    BaseIn,
    QuoteIn,
}

#[derive(Debug, Clone, Copy)]
struct SwapContext {
    input_reserve: u128,
    output_reserve: u128,
    side: SwapSide,
}

fn select_reserves(state: &LifinityPoolState, req: &QuoteRequest) -> Option<SwapContext> {
    let (Some(vault_a), Some(vault_b)) = (state.vault_a_balance, state.vault_b_balance) else {
        return None;
    };

    if req.input_mint == state.base_mint && req.output_mint == state.quote_mint {
        Some(SwapContext {
            input_reserve: vault_a,
            output_reserve: vault_b,
            side: SwapSide::BaseIn,
        })
    } else if req.input_mint == state.quote_mint && req.output_mint == state.base_mint {
        Some(SwapContext {
            input_reserve: vault_b,
            output_reserve: vault_a,
            side: SwapSide::QuoteIn,
        })
    } else {
        None
    }
}

fn lifinity_amount_out(amount_in: u64, ctx: SwapContext, state: &LifinityPoolState) -> Option<u64> {
    if ctx.input_reserve == 0 || ctx.output_reserve == 0 {
        return None;
    }

    if amount_in == 0 {
        return Some(0);
    }

    let total_fee_bps = total_effective_fee_bps(amount_in, &ctx, state);

    let net_input = apply_bps_cut(amount_in as u128, total_fee_bps);
    if net_input == 0 {
        return Some(0);
    }

    let numerator = net_input.saturating_mul(ctx.output_reserve);
    let denominator = ctx.input_reserve.saturating_add(net_input);
    if denominator == 0 {
        return None;
    }

    let amount_out = numerator / denominator;
    Some(amount_out.min(u64::MAX as u128) as u64)
}

fn base_fee_bps(state: &LifinityPoolState) -> u64 {
    let mut total = 0u64;
    for value in [
        state.fees_bps.map(|value| value as u64),
        state.owner_fee_bps.map(|value| value as u64),
        state.host_fee_bps.map(|value| value as u64),
        state.fee_trade_bps.map(|value| value as u64),
        state.fee_platform_bps.map(|value| value as u64),
    ]
    .into_iter()
    .flatten()
    {
        total = total.saturating_add(value);
    }

    total.min(BPS_DENOMINATOR - 1)
}

fn oracle_spread_bps(
    amount_in: u64,
    input_reserve: u128,
    side: SwapSide,
    state: &LifinityPoolState,
) -> u64 {
    let mut spread = 0i64;
    spread += state.std_spread_bps.map(|value| value as i64).unwrap_or(0);
    spread += state
        .spread_buffer_bps
        .map(|value| value as i64)
        .unwrap_or(0);
    spread += price_buffer_component(side, state);

    if let Some(coefficient) = state.spread_coefficient_bps {
        let trade_share = trade_share_bps(amount_in, input_reserve) as i64;
        spread += (coefficient as i64).saturating_mul(trade_share) / (BPS_DENOMINATOR as i64);
    }

    spread.clamp(0, (BPS_DENOMINATOR - 1) as i64) as u64
}

fn price_buffer_component(side: SwapSide, state: &LifinityPoolState) -> i64 {
    match side {
        SwapSide::BaseIn => state
            .price_buffer_coin_bps
            .map(|value| value as i64)
            .unwrap_or(0),
        SwapSide::QuoteIn => state
            .price_buffer_pc_bps
            .map(|value| value as i64)
            .unwrap_or(0),
    }
}

fn trade_share_bps(amount_in: u64, input_reserve: u128) -> u64 {
    if input_reserve == 0 {
        return 0;
    }

    ((amount_in as u128).saturating_mul(BPS_DENOMINATOR as u128) / input_reserve)
        .min(BPS_DENOMINATOR as u128) as u64
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

fn apply_bps_cut(amount: u128, fee_bps: u64) -> u128 {
    if amount == 0 {
        return 0;
    }

    if fee_bps == 0 {
        return amount;
    }

    let retained_bps = BPS_DENOMINATOR.saturating_sub(fee_bps) as u128;
    if retained_bps == 0 {
        return 0;
    }

    amount.saturating_mul(retained_bps) / (BPS_DENOMINATOR as u128)
}

fn slot_within_limit(current_slot: u64, snapshot_slot: u64, limit: Option<u64>) -> bool {
    let Some(limit_slot) = limit else {
        return true;
    };

    current_slot.saturating_sub(snapshot_slot) <= limit_slot
}

fn confidence_within_limit(confidence: u64, limit: Option<u64>) -> bool {
    let Some(limit_conf) = limit else {
        return true;
    };

    confidence <= limit_conf
}

fn compute_slippage_bps(reference: f64, execution: f64) -> f64 {
    if !reference.is_finite() || reference == 0.0 || !execution.is_finite() {
        return 0.0;
    }
    (((reference - execution) / reference) * 10_000.0).abs()
}

fn total_effective_fee_bps(amount_in: u64, ctx: &SwapContext, state: &LifinityPoolState) -> u64 {
    let spread_bps = oracle_spread_bps(amount_in, ctx.input_reserve, ctx.side, state);
    base_fee_bps(state)
        .saturating_add(spread_bps)
        .min(BPS_DENOMINATOR - 1)
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

fn needs_hydration(extras: &LifinityPoolExtras) -> bool {
    extras.authority.is_none()
        || extras.token_a_vault.is_none()
        || extras.token_b_vault.is_none()
        || extras.pool_mint.is_none()
        || extras.fee_account.is_none()
        || extras.token_program.is_none()
        || extras.oracle_main.is_none()
        || extras.oracle_sub.is_none()
        || extras.oracle_pc.is_none()
        || extras.oracle_main_slot_limit.is_none()
        || extras.oracle_sub_confidence_limit.is_none()
        || extras.oracle_sub_slot_limit.is_none()
        || extras.oracle_pc_confidence_limit.is_none()
        || extras.oracle_pc_slot_limit.is_none()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    authority: Pubkey,
    token_program: Pubkey,
    token_a_vault: Pubkey,
    token_b_vault: Pubkey,
    pool_mint: Pubkey,
    fee_account: Pubkey,
    oracle_main: Pubkey,
    oracle_sub: Pubkey,
    oracle_pc: Pubkey,
    fees_bps: Option<u16>,
    owner_fee_bps: Option<u16>,
    host_fee_bps: Option<u16>,
    std_spread_bps: Option<u16>,
    spread_buffer_bps: Option<u16>,
    spread_coefficient_bps: Option<u32>,
    price_buffer_coin_bps: Option<i32>,
    price_buffer_pc_bps: Option<i32>,
    fee_trade_bps: Option<u16>,
    fee_platform_bps: Option<u16>,
    oracle_main_slot_limit: Option<u64>,
    oracle_sub_confidence_limit: Option<u64>,
    oracle_sub_slot_limit: Option<u64>,
    oracle_pc_confidence_limit: Option<u64>,
    oracle_pc_slot_limit: Option<u64>,
    freeze_trade: u8,
}

struct LifinityPoolHydrator {
    program_id: Pubkey,
    hydrator: AsyncHydrator<Pubkey, HydratedPoolMeta>,
    endpoint_count: usize,
}

impl LifinityPoolHydrator {
    fn new(program_id: Pubkey, config: &LifinityAdapterConfig) -> Option<Self> {
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
        let hydrator = AsyncHydrator::new("lifinity_pool", endpoint_count, move |pool| {
            fetch_lifinity_meta(program_id, fetch_clients.as_ref(), pool)
        });

        Some(Self {
            program_id,
            hydrator,
            endpoint_count,
        })
    }

    fn client_count(&self) -> usize {
        self.endpoint_count
    }

    fn hydrate(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        self.hydrator.get_or_schedule(pool)
    }
}

fn fetch_lifinity_meta(
    program_id: Pubkey,
    clients: &[Arc<RpcClient>],
    pool: Pubkey,
) -> Option<HydratedPoolMeta> {
    for client in clients {
        match client.get_account(&pool) {
            Ok(account) => {
                if let Some(meta) = parse_amm_account(&program_id, pool, &account.data) {
                    return Some(meta);
                } else {
                    warn!(
                        target = "adapter::lifinity",
                        pool = %pool,
                        "unexpected Lifinity Amm account layout"
                    );
                }
            }
            Err(error) => {
                warn!(
                    target = "adapter::lifinity",
                    pool = %pool,
                    %error,
                    "failed to fetch Lifinity Amm account"
                );
            }
        }
    }

    None
}

struct LifinityOracleCache {
    clients: Vec<Arc<RpcClient>>,
    cache: DashMap<Pubkey, OracleSnapshot>,
    ttl: Duration,
}

impl LifinityOracleCache {
    fn new(config: &LifinityAdapterConfig) -> Option<Self> {
        if config.rpc_endpoints.is_empty() {
            return None;
        }

        let timeout = Duration::from_millis(config.rpc_timeout_ms.max(100));
        let clients: Vec<Arc<RpcClient>> = config
            .rpc_endpoints
            .iter()
            .map(|url| Arc::new(RpcClient::new_with_timeout(url.clone(), timeout)))
            .collect();

        if clients.is_empty() {
            return None;
        }

        Some(Self {
            clients,
            cache: DashMap::new(),
            ttl: Duration::from_millis(config.oracle_stale_after_ms.max(100)),
        })
    }

    fn snapshot(&self, oracle: Pubkey) -> Option<OracleSnapshot> {
        if let Some(entry) = self.cache.get(&oracle) {
            if entry.is_fresh(self.ttl) {
                return Some(entry.clone());
            }
        }
        self.fetch_from_rpc(oracle)
    }

    fn fetch_from_rpc(&self, oracle: Pubkey) -> Option<OracleSnapshot> {
        for client in &self.clients {
            match client.get_account(&oracle) {
                Ok(account) => {
                    if let Some(parsed) = parse_pyth_price_account(&account.data) {
                        let snapshot = OracleSnapshot::from_parsed(parsed);
                        self.cache.insert(oracle, snapshot.clone());
                        return Some(snapshot);
                    } else {
                        warn!(
                            target = "adapter::lifinity",
                            oracle = %oracle,
                            "unexpected oracle account layout"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        target = "adapter::lifinity",
                        oracle = %oracle,
                        %error,
                        "failed to fetch oracle account"
                    );
                }
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
struct OracleSnapshot {
    _price: i64,
    confidence: u64,
    _exponent: i32,
    _valid_slot: u64,
    publish_slot: u64,
    cached_at: Instant,
}

impl OracleSnapshot {
    fn from_parsed(parsed: ParsedOracleData) -> Self {
        Self {
            _price: parsed.price,
            confidence: parsed.confidence,
            _exponent: parsed.exponent,
            _valid_slot: parsed.valid_slot,
            publish_slot: parsed.publish_slot,
            cached_at: Instant::now(),
        }
    }

    fn is_fresh(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() <= ttl
    }
}

#[derive(Clone, Copy, Debug)]
struct ParsedOracleData {
    price: i64,
    confidence: u64,
    exponent: i32,
    valid_slot: u64,
    publish_slot: u64,
}

const PYTH_MAGIC: u32 = 0xA1B2C3D4;
const PYTH_PRICE_ACCOUNT_TYPE: u32 = 3;

fn parse_pyth_price_account(data: &[u8]) -> Option<ParsedOracleData> {
    let mut cursor = 0usize;
    let magic = read_u32(data, &mut cursor)?;
    if magic != PYTH_MAGIC {
        return None;
    }

    let _version = read_u32(data, &mut cursor)?;
    let account_type = read_u32(data, &mut cursor)?;
    if account_type != PYTH_PRICE_ACCOUNT_TYPE {
        return None;
    }
    let _size = read_u32(data, &mut cursor)?;

    let _price_type = read_i32(data, &mut cursor)?;
    let exponent = read_i32(data, &mut cursor)?;
    read_u32(data, &mut cursor)?; // num_components
    read_u32(data, &mut cursor)?; // num_quoters
    read_u64(data, &mut cursor)?; // last_slot
    let valid_slot = read_u64(data, &mut cursor)?;

    for _ in 0..3 {
        read_pubkey(data, &mut cursor)?;
    }

    let price = read_i64(data, &mut cursor)?;
    let confidence = read_u64(data, &mut cursor)?;
    read_u32(data, &mut cursor)?; // status
    read_u32(data, &mut cursor)?; // corp_action
    let publish_slot = read_u64(data, &mut cursor)?;

    Some(ParsedOracleData {
        price,
        confidence,
        exponent,
        valid_slot,
        publish_slot,
    })
}

const ANCHOR_DISCRIMINATOR_LEN: usize = 8;
const PUBKEY_BYTES: usize = 32;

fn parse_amm_account(program_id: &Pubkey, pool: Pubkey, data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < ANCHOR_DISCRIMINATOR_LEN + PUBKEY_BYTES * 3 {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;

    // Skip initializer metadata (3 pubkeys + 2 u64)
    for _ in 0..3 {
        read_pubkey(data, &mut cursor)?;
    }
    read_u64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?;

    // Flags + bumps
    let _ = read_u8(data, &mut cursor)?; // is_initialized
    let bump_seed = read_u8(data, &mut cursor)?;
    let freeze_trade = read_u8(data, &mut cursor)?;
    read_u8(data, &mut cursor)?; // freeze_deposit
    read_u8(data, &mut cursor)?; // freeze_withdraw
    read_u8(data, &mut cursor)?; // base_decimals

    let token_program = read_pubkey(data, &mut cursor)?;
    let token_a_vault = read_pubkey(data, &mut cursor)?;
    let token_b_vault = read_pubkey(data, &mut cursor)?;
    let pool_mint = read_pubkey(data, &mut cursor)?;
    read_pubkey(data, &mut cursor)?; // token_a_mint
    read_pubkey(data, &mut cursor)?; // token_b_mint
    let fee_account = read_pubkey(data, &mut cursor)?;
    let oracle_main = read_pubkey(data, &mut cursor)?;
    let oracle_sub = read_pubkey(data, &mut cursor)?;
    let oracle_pc = read_pubkey(data, &mut cursor)?;

    // Fees
    let trade_fee_num = read_u64(data, &mut cursor)?;
    let trade_fee_den = read_u64(data, &mut cursor)?;
    let owner_fee_num = read_u64(data, &mut cursor)?;
    let owner_fee_den = read_u64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?; // owner withdraw numerator
    read_u64(data, &mut cursor)?; // owner withdraw denominator
    let host_fee_num = read_u64(data, &mut cursor)?;
    let host_fee_den = read_u64(data, &mut cursor)?;

    // Curve
    read_u8(data, &mut cursor)?; // curve type
    read_u64(data, &mut cursor)?; // curve parameters

    // Config header
    read_u64(data, &mut cursor)?; // last_price
    read_u64(data, &mut cursor)?; // last_balanced_price
    let config_denominator = read_u64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?; // volume_x
    read_u64(data, &mut cursor)?; // volume_y
    read_u64(data, &mut cursor)?; // volume_x_in_y
    read_u64(data, &mut cursor)?; // deposit_cap
    read_u64(data, &mut cursor)?; // regression_target
    let _oracle_type = read_u64(data, &mut cursor)?;
    let _oracle_status = read_u64(data, &mut cursor)?;
    let oracle_main_slot_limit = read_u64(data, &mut cursor)?;
    let oracle_sub_confidence_limit = read_u64(data, &mut cursor)?;
    let oracle_sub_slot_limit = read_u64(data, &mut cursor)?;
    let oracle_pc_confidence_limit = read_u64(data, &mut cursor)?;
    let oracle_pc_slot_limit = read_u64(data, &mut cursor)?;
    let std_spread = read_u64(data, &mut cursor)?;
    let std_spread_buffer = read_u64(data, &mut cursor)?;
    let spread_coefficient = read_u64(data, &mut cursor)?;
    let price_buffer_coin = read_i64(data, &mut cursor)?;
    let price_buffer_pc = read_i64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?; // rebalance ratio
    let fee_trade = read_u64(data, &mut cursor)?;
    let fee_platform = read_u64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?; // oracle main slot buffer
    for _ in 0..5 {
        read_u64(data, &mut cursor)?; // config temps
    }

    // Temp pubkeys (unused)
    for _ in 0..5 {
        read_pubkey(data, &mut cursor)?;
    }

    let authority =
        Pubkey::create_program_address(&[pool.as_ref(), &[bump_seed]], program_id).ok()?;

    let std_spread_bps = ratio_to_bps(std_spread, config_denominator);
    let spread_buffer_bps = ratio_to_bps(std_spread_buffer, config_denominator);
    let spread_coefficient_bps = ratio_to_bps_u32(spread_coefficient, config_denominator);
    let price_buffer_coin_bps = ratio_to_signed_bps(price_buffer_coin, config_denominator);
    let price_buffer_pc_bps = ratio_to_signed_bps(price_buffer_pc, config_denominator);
    let fee_trade_bps = ratio_to_bps(fee_trade, config_denominator);
    let fee_platform_bps = ratio_to_bps(fee_platform, config_denominator);

    Some(HydratedPoolMeta {
        authority,
        token_program,
        token_a_vault,
        token_b_vault,
        pool_mint,
        fee_account,
        oracle_main,
        oracle_sub,
        oracle_pc,
        fees_bps: ratio_to_bps(trade_fee_num, trade_fee_den),
        owner_fee_bps: ratio_to_bps(owner_fee_num, owner_fee_den),
        host_fee_bps: ratio_to_bps(host_fee_num, host_fee_den),
        std_spread_bps,
        spread_buffer_bps,
        spread_coefficient_bps,
        price_buffer_coin_bps,
        price_buffer_pc_bps,
        fee_trade_bps,
        fee_platform_bps,
        oracle_main_slot_limit: Some(oracle_main_slot_limit),
        oracle_sub_confidence_limit: Some(oracle_sub_confidence_limit),
        oracle_sub_slot_limit: Some(oracle_sub_slot_limit),
        oracle_pc_confidence_limit: Some(oracle_pc_confidence_limit),
        oracle_pc_slot_limit: Some(oracle_pc_slot_limit),
        freeze_trade,
    })
}

fn read_pubkey(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    let end = cursor.checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Option<u64> {
    let end = cursor.checked_add(8)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u64::from_le_bytes(bytes))
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Option<u32> {
    let end = cursor.checked_add(4)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u32::from_le_bytes(bytes))
}

fn read_i64(data: &[u8], cursor: &mut usize) -> Option<i64> {
    let end = cursor.checked_add(8)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(i64::from_le_bytes(bytes))
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Option<i32> {
    let end = cursor.checked_add(4)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(i32::from_le_bytes(bytes))
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    let byte = *data.get(*cursor)?;
    *cursor = (*cursor).checked_add(1)?;
    Some(byte)
}

fn ratio_to_bps(num: u64, denom: u64) -> Option<u16> {
    if denom == 0 {
        return None;
    }
    let value = (num as u128).saturating_mul(10_000) / denom as u128;
    Some(value.min(u16::MAX as u128) as u16)
}

fn ratio_to_bps_u32(num: u64, denom: u64) -> Option<u32> {
    if denom == 0 {
        return None;
    }
    let value = (num as u128).saturating_mul(10_000) / denom as u128;
    Some(value.min(u32::MAX as u128) as u32)
}

fn ratio_to_signed_bps(num: i64, denom: u64) -> Option<i32> {
    if denom == 0 {
        return None;
    }
    let scaled = (num as i128).saturating_mul(10_000i128) / denom as i128;
    let clamped = scaled.clamp(i32::MIN as i128, i32::MAX as i128);
    Some(clamped as i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::types::{PoolSnapshot, PoolUpdateExtensions};

    fn push_pubkey(buf: &mut Vec<u8>, key: &Pubkey) {
        buf.extend_from_slice(key.as_ref());
    }

    fn push_u64(buf: &mut Vec<u8>, value: u64) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn push_i64(buf: &mut Vec<u8>, value: i64) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u32(buf: &mut Vec<u8>, value: u32) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn push_i32(buf: &mut Vec<u8>, value: i32) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn minimal_state(
        base_mint: Pubkey,
        quote_mint: Pubkey,
        vault_a: u128,
        vault_b: u128,
    ) -> LifinityPoolState {
        LifinityPoolState {
            amm: Pubkey::new_unique(),
            base_mint,
            quote_mint,
            authority: None,
            token_a_vault: None,
            token_b_vault: None,
            pool_mint: None,
            fee_account: None,
            token_program: None,
            oracle_main: None,
            oracle_sub: None,
            oracle_pc: None,
            fees_bps: None,
            owner_fee_bps: None,
            host_fee_bps: None,
            std_spread_bps: None,
            spread_buffer_bps: None,
            spread_coefficient_bps: None,
            price_buffer_coin_bps: None,
            price_buffer_pc_bps: None,
            fee_trade_bps: None,
            fee_platform_bps: None,
            oracle_main_slot_limit: None,
            oracle_sub_confidence_limit: None,
            oracle_sub_slot_limit: None,
            oracle_pc_confidence_limit: None,
            oracle_pc_slot_limit: None,
            vault_a_balance: Some(vault_a),
            vault_b_balance: Some(vault_b),
            freeze_trade: Some(0),
            last_update_slot: 0,
            last_update_at: Instant::now(),
        }
    }

    fn swap_ctx(input: u128, output: u128, side: SwapSide) -> SwapContext {
        SwapContext {
            input_reserve: input,
            output_reserve: output,
            side,
        }
    }

    #[test]
    fn parses_lifinity_amm_account() {
        let program_id = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let (expected_authority, bump) =
            Pubkey::find_program_address(&[pool.as_ref()], &program_id);

        let token_program = Pubkey::new_unique();
        let token_a_vault = Pubkey::new_unique();
        let token_b_vault = Pubkey::new_unique();
        let pool_mint = Pubkey::new_unique();
        let token_a_mint = Pubkey::new_unique();
        let token_b_mint = Pubkey::new_unique();
        let fee_account = Pubkey::new_unique();
        let oracle_main = Pubkey::new_unique();
        let oracle_sub = Pubkey::new_unique();
        let oracle_pc = Pubkey::new_unique();

        let mut data = vec![0u8; ANCHOR_DISCRIMINATOR_LEN];

        for _ in 0..3 {
            push_pubkey(&mut data, &Pubkey::new_unique());
        }
        push_u64(&mut data, 0);
        push_u64(&mut data, 0);
        data.push(1); // is_initialized
        data.push(bump);
        data.push(1); // freeze_trade
        data.push(0);
        data.push(0);
        data.push(6); // base decimals
        push_pubkey(&mut data, &token_program);
        push_pubkey(&mut data, &token_a_vault);
        push_pubkey(&mut data, &token_b_vault);
        push_pubkey(&mut data, &pool_mint);
        push_pubkey(&mut data, &token_a_mint);
        push_pubkey(&mut data, &token_b_mint);
        push_pubkey(&mut data, &fee_account);
        push_pubkey(&mut data, &oracle_main);
        push_pubkey(&mut data, &oracle_sub);
        push_pubkey(&mut data, &oracle_pc);

        // Fees (trade=25 bps, owner=5 bps, host=10 bps)
        push_u64(&mut data, 25);
        push_u64(&mut data, 10_000);
        push_u64(&mut data, 5);
        push_u64(&mut data, 10_000);
        push_u64(&mut data, 0);
        push_u64(&mut data, 1);
        push_u64(&mut data, 10);
        push_u64(&mut data, 1_000);

        data.push(1); // curve type
        push_u64(&mut data, 0);

        // Config values
        push_u64(&mut data, 0); // last_price
        push_u64(&mut data, 0); // last_balanced_price
        push_u64(&mut data, 1_000_000); // config_denominator
        for _ in 0..12 {
            push_u64(&mut data, 0);
        }
        push_u64(&mut data, 25_000); // std_spread raw = 250 bps
        push_u64(&mut data, 10_000); // spread buffer raw = 100 bps
        push_u64(&mut data, 0);
        push_i64(&mut data, 0);
        push_i64(&mut data, 0);
        for _ in 0..4 {
            push_u64(&mut data, 0);
        }
        for _ in 0..5 {
            push_u64(&mut data, 0);
        }

        for _ in 0..5 {
            push_pubkey(&mut data, &Pubkey::new_unique());
        }

        let meta = parse_amm_account(&program_id, pool, &data).expect("should parse");

        assert_eq!(meta.authority, expected_authority);
        assert_eq!(meta.token_program, token_program);
        assert_eq!(meta.token_a_vault, token_a_vault);
        assert_eq!(meta.token_b_vault, token_b_vault);
        assert_eq!(meta.pool_mint, pool_mint);
        assert_eq!(meta.fee_account, fee_account);
        assert_eq!(meta.oracle_main, oracle_main);
        assert_eq!(meta.oracle_sub, oracle_sub);
        assert_eq!(meta.oracle_pc, oracle_pc);
        assert_eq!(meta.fees_bps, Some(25));
        assert_eq!(meta.owner_fee_bps, Some(5));
        assert_eq!(meta.host_fee_bps, Some(100));
        assert_eq!(meta.std_spread_bps, Some(250));
        assert_eq!(meta.spread_buffer_bps, Some(100));
        assert_eq!(meta.freeze_trade, 1);
    }

    #[test]
    fn lifinity_math_captures_slippage() {
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let state = minimal_state(base_mint, quote_mint, 1_000_000, 1_000_000);

        let ctx = swap_ctx(1_000_000, 1_000_000, SwapSide::BaseIn);
        let amount_out = lifinity_amount_out(100_000, ctx, &state).unwrap();

        assert!(
            amount_out < 100_000,
            "xyk slippage should be below mid-price output"
        );
    }

    #[test]
    fn lifinity_math_applies_fees() {
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let mut fee_state = minimal_state(base_mint, quote_mint, 2_000_000, 1_000_000);
        fee_state.fees_bps = Some(30);
        fee_state.owner_fee_bps = Some(20);

        let no_fee_state = minimal_state(base_mint, quote_mint, 2_000_000, 1_000_000);

        let ctx = swap_ctx(2_000_000, 1_000_000, SwapSide::BaseIn);
        let with_fee = lifinity_amount_out(50_000, ctx, &fee_state).unwrap();
        let without_fee = lifinity_amount_out(50_000, ctx, &no_fee_state).unwrap();

        assert!(
            with_fee < without_fee,
            "effective fee bps should reduce output amount"
        );
    }

    #[test]
    fn lifinity_math_oracle_spread_respects_price_buffers() {
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let ctx = swap_ctx(5_000_000, 4_000_000, SwapSide::BaseIn);

        let mut buffered = minimal_state(base_mint, quote_mint, 5_000_000, 4_000_000);
        buffered.price_buffer_coin_bps = Some(250); // 2.5%

        let neutral = minimal_state(base_mint, quote_mint, 5_000_000, 4_000_000);

        let buffered_out = lifinity_amount_out(200_000, ctx, &buffered).unwrap();
        let neutral_out = lifinity_amount_out(200_000, ctx, &neutral).unwrap();

        assert!(
            buffered_out < neutral_out,
            "positive coin buffer should widen spread for base-in trades"
        );
    }

    #[test]
    fn lifinity_math_oracle_spread_scales_with_trade_share() {
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let ctx = swap_ctx(10_000_000, 9_000_000, SwapSide::BaseIn);

        let mut adaptive = minimal_state(base_mint, quote_mint, 10_000_000, 9_000_000);
        adaptive.spread_coefficient_bps = Some(1_000); // 10%

        let reference = minimal_state(base_mint, quote_mint, 10_000_000, 9_000_000);

        let adaptive_out = lifinity_amount_out(500_000, ctx, &adaptive).unwrap();
        let reference_out = lifinity_amount_out(500_000, ctx, &reference).unwrap();

        assert!(
            adaptive_out < reference_out,
            "spread coefficient should lower output as trade share grows"
        );
    }

    #[test]
    fn bidirectional_quotes_generate_snapshot_sides() {
        let adapter = LifinityAdapter::new(LifinityAdapterConfig::default(), None);
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();

        let mut state = minimal_state(base_mint, quote_mint, 5_000_000, 7_500_000);
        state.amm = pool;
        state.fees_bps = Some(20);
        state.owner_fee_bps = Some(5);
        state.host_fee_bps = Some(5);
        adapter.state.insert(pool, state.clone());

        let snapshot = PoolSnapshot {
            pool_id: pool.to_string(),
            program_id: LIFINITY_PROGRAM_ID.to_string(),
            slot: 123,
            base_mint: base_mint.to_string(),
            quote_mint: quote_mint.to_string(),
            base_post_amount: state.vault_a_balance.unwrap(),
            quote_post_amount: state.vault_b_balance.unwrap(),
            updated_at: Instant::now(),
            quotes: None,
            extensions: PoolUpdateExtensions::default(),
        };

        let quotes = adapter
            .bidirectional_quotes(&snapshot)
            .expect("quotes should be generated");

        let utilization = adapter.config.max_swap_utilization_bps;
        let expected_base_probe =
            snapshot_probe_amount(state.vault_a_balance.unwrap(), utilization).expect("base probe");
        let expected_quote_probe =
            snapshot_probe_amount(state.vault_b_balance.unwrap(), utilization)
                .expect("quote probe");

        assert_eq!(quotes.base_to_quote.notional, expected_base_probe);
        assert_eq!(quotes.quote_to_base.notional, expected_quote_probe);
        assert_eq!(quotes.base_to_quote.fee_bps, 30);
        assert!(quotes.base_to_quote.expected_output > 0);
        assert!(quotes.quote_to_base.expected_output > 0);
        assert!(quotes.base_to_quote.estimated_slippage_bps >= 0.0);
    }

    #[test]
    fn parses_pyth_price_account() {
        let mut data = Vec::new();
        push_u32(&mut data, PYTH_MAGIC);
        push_u32(&mut data, 2); // version
        push_u32(&mut data, PYTH_PRICE_ACCOUNT_TYPE);
        push_u32(&mut data, 0); // size placeholder
        push_i32(&mut data, 1); // price type
        push_i32(&mut data, -8); // exponent
        push_u32(&mut data, 1); // num components
        push_u32(&mut data, 1); // num quoters
        push_u64(&mut data, 1_000);
        push_u64(&mut data, 1_010);
        for _ in 0..3 {
            push_pubkey(&mut data, &Pubkey::new_unique());
        }
        push_i64(&mut data, 123_456_789); // price
        push_u64(&mut data, 1_000); // confidence
        push_u32(&mut data, 0); // status
        push_u32(&mut data, 0); // corp action
        push_u64(&mut data, 1_005); // publish slot

        let parsed = parse_pyth_price_account(&data).expect("should parse Pyth account");
        assert_eq!(parsed.price, 123_456_789);
        assert_eq!(parsed.confidence, 1_000);
        assert_eq!(parsed.exponent, -8);
        assert_eq!(parsed.valid_slot, 1_010);
        assert_eq!(parsed.publish_slot, 1_005);
    }
}
