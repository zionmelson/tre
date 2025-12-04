use std::{
    collections::BTreeMap,
    convert::TryFrom,
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use dashmap::{DashMap, DashSet};
use once_cell::sync::Lazy;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, UiAccountEncoding,
};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_client::rpc_response::{UiAccount, UiAccountData};
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};
use tracing::{debug, info, trace, warn};
use uint::construct_uint;

use crate::builder::{BuilderHandle, LbClmmPoolAccounts, LbClmmPoolMeta};
use crate::hydration::types::{
    compute_edge_bps, LbClmmBinArrayExtras, LbClmmBinLiquidityExtras, LbClmmBitmapExtras,
    LbClmmPoolExtras, PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate,
    LB_CLMM_INPUT_UTILIZATION_BPS,
};
use crate::template::TemplateContext;

use super::{
    build_quote_response, hydration::AsyncHydrator, ProtocolAdapter, QuoteRequest, QuoteResponse,
    QuoteStatus, SwapDirection,
};

const LB_CLMM_PROGRAM_ID: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SPL_TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const BPS_DENOMINATOR: u64 = 10_000;
const DEFAULT_DYNAMIC_FEE_BPS: u16 = 30;
const MAX_BIN_PER_ARRAY: i32 = 70;
const BIN_STRUCT_LEN: usize = 144;
const BIN_ARRAY_HEADER_LEN: usize = 8 /* index */ + 1 /* version */ + 7 /* padding */ + 32;
const BIN_ARRAY_ACCOUNT_LEN: usize =
    BIN_ARRAY_HEADER_LEN + (MAX_BIN_PER_ARRAY as usize) * BIN_STRUCT_LEN;
const BIN_ARRAY_LB_PAIR_OFFSET: usize = 16;
const BIN_ARRAY_INDEX_OFFSET: usize = 0;
const BITMAP_EXTENSION_ACCOUNT_LEN: usize = 32 + (2 * 12 * 8 * 8);
const SNAPSHOT_PROBE_DIVISOR: u128 = 10_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;
const PRICE_IMPACT_FLOOR_BPS: u16 = 25;
const PRICE_IMPACT_PADDING_BPS: u16 = 10;
const LB_CLMM_DEFAULT_CU_LIMIT: u32 = 420_000;
const LB_CLMM_DEFAULT_CU_PRICE: u64 = 5_000;
const DEFAULT_LB_CLMM_HYDRATION_RETRY_MS: u64 = 60_000;
const TARGETED_BIN_ARRAY_RADIUS: i64 = 4;
const BIN_WINDOW_EDGE_BUFFER_BINS: i32 = MAX_BIN_PER_ARRAY * 2;
const DEFAULT_MAX_BIN_WALK: usize = 800;
const DEFAULT_MAX_PRICE_IMPACT_BPS: u16 = 500;
const HOST_FEE_SHARE_BPS: u64 = 2_000;
const BIN_ARRAY_BITMAP_SIZE_I32: i32 = 1024;
const DEFAULT_BITMAP_WORDS: usize = 16; // 1024 bits
const BITMAP_SEGMENT_WORDS: usize = 8; // 512 bits
const BITMAP_SEGMENT_BYTES: usize = BITMAP_SEGMENT_WORDS * 8;
const Q64_SCALE_OFFSET: u32 = 64;
const Q64_ONE: u128 = 1u128 << Q64_SCALE_OFFSET;
const MAX_Q64_EXPONENT: u32 = 0x80000;

construct_uint! {
    pub struct U256(4);
}

static SPL_TOKEN_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str(SPL_TOKEN_PROGRAM_ID).expect("static spl token program id should be valid")
});
static SPL_TOKEN_2022_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str(SPL_TOKEN_2022_PROGRAM_ID)
        .expect("static spl token 2022 program id should be valid")
});

#[derive(Debug, Clone)]
pub struct LbClmmAdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
    pub hydration_retry_ms: u64,
    pub max_swap_utilization_bps: u64,
    pub max_bin_walk: usize,
    pub max_price_impact_bps: u16,
}

impl Default for LbClmmAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            // lb_clmm bin array/bitmap fetches can be slow; give RPC plenty of time.
            rpc_timeout_ms: 15_000,
            hydration_retry_ms: DEFAULT_LB_CLMM_HYDRATION_RETRY_MS,
            max_swap_utilization_bps: LB_CLMM_INPUT_UTILIZATION_BPS,
            max_bin_walk: DEFAULT_MAX_BIN_WALK,
            max_price_impact_bps: DEFAULT_MAX_PRICE_IMPACT_BPS,
        }
    }
}

pub struct LbClmmAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, LbClmmState>,
    config: LbClmmAdapterConfig,
    hydrator: Option<LbClmmPoolHydrator>,
    template_builder: Option<BuilderHandle>,
    registered_templates: DashSet<Pubkey>,
}

impl LbClmmAdapter {
    pub fn new(config: LbClmmAdapterConfig, template_builder: Option<BuilderHandle>) -> Self {
        let program_id = Pubkey::from_str(LB_CLMM_PROGRAM_ID)
            .expect("static lb_clmm program id should be valid");
        let hydrator = LbClmmPoolHydrator::new(program_id, &config);
        if let Some(hydrator_ref) = hydrator.as_ref() {
            info!(
                target = "adapter::lb_clmm",
                endpoints = hydrator_ref.client_count(),
                "lb_clmm pool hydrator online"
            );
        } else {
            debug!(
                target = "adapter::lb_clmm",
                "lb_clmm pool hydrator disabled (no rpc endpoints)"
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

    pub fn get_state(&self, pool: &Pubkey) -> Option<LbClmmState> {
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
                    target = "adapter::lb_clmm",
                    pool = %state.lb_pair,
                    last_slot = state.last_update_slot,
                    "evicting stale lb_clmm state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: LbClmmPoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::lb_clmm",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let extras = self.enrich_extras(pool_pubkey, extras);
        let Some(token_x_mint) = extras
            .token_x_mint
            .as_ref()
            .and_then(|value| parse_pubkey(value))
        else {
            warn!(target = "adapter::lb_clmm", pool = %update.pool_id, "missing token_x mint");
            return;
        };

        let Some(token_y_mint) = extras
            .token_y_mint
            .as_ref()
            .and_then(|value| parse_pubkey(value))
        else {
            warn!(target = "adapter::lb_clmm", pool = %update.pool_id, "missing token_y mint");
            return;
        };

        let mut active_bitmap = extras.active_bitmap.clone().map(LbClmmBinBitmap::from);
        if let (Some(active_bin), Some(bitmap)) = (extras.active_bin_id, active_bitmap.as_mut()) {
            bitmap.anchor_with_bin(active_bin);
        }

        let state = LbClmmState {
            lb_pair: pool_pubkey,
            token_x_mint,
            token_y_mint,
            token_x_program: extras
                .token_x_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_y_program: extras
                .token_y_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            reserve_x: extras
                .reserve_x
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            reserve_y: extras
                .reserve_y
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            oracle: extras.oracle.as_ref().and_then(|value| parse_pubkey(value)),
            bin_array_bitmap_extension: extras
                .bin_array_bitmap_extension
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            host_fee_account: extras
                .host_fee_account
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            bin_step: extras.bin_step,
            active_bin_id: extras.active_bin_id,
            dynamic_fee_bps: extras.dynamic_fee_bps,
            protocol_fee_bps: extras.protocol_fee_bps,
            reserve_x_balance: extras.reserve_x_balance,
            reserve_y_balance: extras.reserve_y_balance,
            volatility_bps: extras.volatility_bps,
            bin_liquidity: build_bin_liquidity_map(&extras),
            active_bitmap,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
        };
        trace!(
            target = "adapter::lb_clmm",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting lb_clmm pool state"
        );

        self.register_templates_with_builder(&state);
        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: LbClmmPoolExtras) -> LbClmmPoolExtras {
        let hydrator_ref = self.hydrator.as_ref();
        if !needs_hydration(&extras) {
            if let (Some(hydrator), Some(active_bin_id)) = (hydrator_ref, extras.active_bin_id) {
                hydrator.ensure_window(pool, active_bin_id);
            }
            return extras;
        }

        let Some(hydrator) = hydrator_ref else {
            return extras;
        };

        let meta = hydrator
            .hydrate(pool)
            .or_else(|| hydrator.hydrate_blocking(pool));

        if let Some(meta) = meta {
            extras.token_x_mint = extras
                .token_x_mint
                .or_else(|| Some(meta.token_x_mint.to_string()));
            extras.token_y_mint = extras
                .token_y_mint
                .or_else(|| Some(meta.token_y_mint.to_string()));
            extras.reserve_x = extras
                .reserve_x
                .or_else(|| Some(meta.reserve_x.to_string()));
            extras.reserve_y = extras
                .reserve_y
                .or_else(|| Some(meta.reserve_y.to_string()));
            extras.oracle = extras.oracle.or_else(|| Some(meta.oracle.to_string()));
            extras.token_x_program = extras
                .token_x_program
                .or_else(|| meta.token_x_program.map(|pk| pk.to_string()));
            extras.token_y_program = extras
                .token_y_program
                .or_else(|| meta.token_y_program.map(|pk| pk.to_string()));
            extras.bin_step = extras.bin_step.or(Some(meta.bin_step));
            extras.active_bin_id = extras.active_bin_id.or(Some(meta.active_bin_id));
            if extras.bin_arrays.is_empty() && !meta.bin_arrays.is_empty() {
                extras.bin_arrays = meta.bin_arrays.clone();
            }
            if extras.active_bitmap.is_none() {
                extras.active_bitmap = Some(meta.active_bitmap.clone());
            }
        }

        if let Some(active_bin_id) = extras.active_bin_id {
            hydrator.ensure_window(pool, active_bin_id);
        }

        extras
    }

    fn register_templates_with_builder(&self, state: &LbClmmState) {
        let Some(builder) = self.template_builder.as_ref() else {
            return;
        };

        let already_registered = self.registered_templates.contains(&state.lb_pair);
        if already_registered {
            let alt_missing = builder
                .alt_manager()
                .handle_for_pool(state.lb_pair)
                .is_none();
            if !alt_missing {
                return;
            }
            debug!(
                target = "adapter::lb_clmm",
                pool = %state.lb_pair,
                "ALT handle missing; re-registering pool templates"
            );
        }

        let (Some(token_x_program), Some(token_y_program)) =
            (state.token_x_program, state.token_y_program)
        else {
            warn!(
                target = "adapter::lb_clmm",
                pool = %state.lb_pair,
                "lb_clmm pool missing token program metadata; skipping registration"
            );
            return;
        };
        let alt_accounts = build_lb_clmm_alt_accounts(state, token_x_program, token_y_program);

        let fee_tier_bps = state.taker_fee_bps().min(u64::from(u16::MAX)) as u16;
        let pool_accounts = LbClmmPoolAccounts {
            reserve_x: state.reserve_x,
            reserve_y: state.reserve_y,
            token_x_mint: state.token_x_mint,
            token_y_mint: state.token_y_mint,
            oracle: state.oracle,
            bin_array_bitmap_extension: state.bin_array_bitmap_extension,
            token_x_program,
            token_y_program,
            host_fee_in: state.host_fee_account,
        };
        builder.register_lb_clmm_pool(LbClmmPoolMeta {
            pool_id: state.lb_pair,
            fee_tier_bps,
            alt_accounts,
            alt_handle: None,
            accounts: pool_accounts,
        });

        self.registered_templates.insert(state.lb_pair);
    }

    fn decorate_swap_context(
        &self,
        state: &LbClmmState,
        context: &mut TemplateContext,
        input_mint: Pubkey,
        output_mint: Pubkey,
    ) {
        let input_program = token_program_for_mint(state, &input_mint);
        let output_program = token_program_for_mint(state, &output_mint);
        if context.input_token_program.is_none() {
            context.input_token_program = Some(input_program);
        }
        if context.output_token_program.is_none() {
            context.output_token_program = Some(output_program);
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
                    target = "adapter::lb_clmm",
                    pool = %state.lb_pair,
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
                    target = "adapter::lb_clmm",
                    pool = %state.lb_pair,
                    mint = %output_mint,
                    "missing configured output token ATA"
                );
            }
        }

        if context.host_fee_account.is_none() {
            if let Some(host_fee) = builder.host_fee_account_for_mint(&input_mint) {
                context.host_fee_account = Some(host_fee);
            }
        }
    }

    fn refresh_window(&self, state: &LbClmmState, target_bin: Option<i32>) {
        let Some(bin_id) = target_bin.or(state.active_bin_id) else {
            return;
        };
        if let Some(hydrator) = self.hydrator.as_ref() {
            hydrator.ensure_window(state.lb_pair, bin_id);
        }
    }

    fn clamp_request_amount(&self, state: &LbClmmState, req: &QuoteRequest) -> u64 {
        match state.resolve_reserves(&req.input_mint, &req.output_mint) {
            Some((reserve, _)) => {
                clamp_amount_to_reserve(req.amount, reserve, self.config.max_swap_utilization_bps)
            }
            None => req.amount,
        }
    }

    fn build_snapshot_side(
        &self,
        state: &LbClmmState,
        pool_id: Pubkey,
        slot: u64,
        base_to_quote: bool,
        probe_amount: u64,
    ) -> Option<PoolQuoteSide> {
        if probe_amount == 0 {
            return None;
        }

        let (input_mint, output_mint) = if base_to_quote {
            (state.token_x_mint, state.token_y_mint)
        } else {
            (state.token_y_mint, state.token_x_mint)
        };

        let req = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: probe_amount,
            slot,
            direction: SwapDirection::ExactIn,
        };

        let fee_bps = state.taker_fee_bps();
        let template_fee_bps = fee_bps.min(u64::from(u16::MAX)) as u16;
        let limit_price = state.active_bin_id.map(|id| id as i64);
        let mut ctx = TemplateContext::default();
        ctx.compute_unit_limit = Some(LB_CLMM_DEFAULT_CU_LIMIT);
        ctx.compute_unit_price = Some(LB_CLMM_DEFAULT_CU_PRICE);
        if let Some(host_fee) = state.host_fee_account {
            ctx.host_fee_account = Some(host_fee);
        }
        if let Some(extension) = state.bin_array_bitmap_extension {
            ctx.remaining_accounts
                .push(AccountMeta::new_readonly(extension, false));
        }
        self.decorate_swap_context(state, &mut ctx, input_mint, output_mint);

        match simulate_lb_clmm_exact_in(&self.config, state, &req) {
            Ok(simulation) => {
                ctx.expected_fee_amount = Some(simulation.taker_fee_amount);
                let effective_price = if probe_amount == 0 {
                    0.0
                } else {
                    simulation.amount_out as f64 / probe_amount as f64
                };
                let price_impact_limit_bps = Some(derive_price_impact_limit(
                    simulation.price_impact_bps,
                    state.volatility_bps,
                ));
                Some(PoolQuoteSide {
                    input_mint: input_mint.to_string(),
                    output_mint: output_mint.to_string(),
                    notional: probe_amount,
                    expected_output: simulation.amount_out,
                    fee_bps,
                    template_fee_bps,
                    effective_price,
                    estimated_slippage_bps: simulation.price_impact_bps.abs(),
                    sqrt_price_limit: None,
                    min_output_amount: None,
                    limit_price,
                    price_impact_limit_bps,
                    context: ctx,
                    status: QuoteStatus::Ok,
                    sqrt_price_before_x64: None,
                    sqrt_price_after_x64: None,
                    tick_arrays_used: Vec::new(),
                    cache_slot: Some(slot),
                })
            }
            Err(err) => {
                if matches!(err, LbClmmSimulationError::InsufficientBins { .. }) {
                    self.refresh_window(state, err.missing_bin_id());
                }
                Some(PoolQuoteSide {
                    input_mint: input_mint.to_string(),
                    output_mint: output_mint.to_string(),
                    notional: probe_amount,
                    expected_output: 0,
                    fee_bps,
                    template_fee_bps,
                    effective_price: 0.0,
                    estimated_slippage_bps: err.observed_slippage_bps(),
                    sqrt_price_limit: None,
                    min_output_amount: None,
                    limit_price,
                    price_impact_limit_bps: None,
                    context: ctx,
                    status: err.into(),
                    sqrt_price_before_x64: None,
                    sqrt_price_after_x64: None,
                    tick_arrays_used: Vec::new(),
                    cache_slot: Some(slot),
                })
            }
        }
    }
}

impl ProtocolAdapter for LbClmmAdapter {
    fn name(&self) -> &'static str {
        "lb_clmm"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::lb_clmm",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        let extras = update.extensions.lb_clmm.clone().unwrap_or_default();
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
            .lb_clmm
            .get_or_insert_with(LbClmmPoolExtras::default);

        if extras.reserve_x.is_none() {
            extras.reserve_x = state.reserve_x.map(|pk| pk.to_string());
        }
        if extras.reserve_y.is_none() {
            extras.reserve_y = state.reserve_y.map(|pk| pk.to_string());
        }
        if extras.token_x_program.is_none() {
            extras.token_x_program = state.token_x_program.map(|pk| pk.to_string());
        }
        if extras.token_y_program.is_none() {
            extras.token_y_program = state.token_y_program.map(|pk| pk.to_string());
        }
        if extras.oracle.is_none() {
            extras.oracle = state.oracle.map(|pk| pk.to_string());
        }
        if extras.bin_array_bitmap_extension.is_none() {
            extras.bin_array_bitmap_extension =
                state.bin_array_bitmap_extension.map(|pk| pk.to_string());
        }
        if extras.host_fee_account.is_none() {
            extras.host_fee_account = state.host_fee_account.map(|pk| pk.to_string());
        }
        extras
            .token_x_mint
            .get_or_insert_with(|| state.token_x_mint.to_string());
        extras
            .token_y_mint
            .get_or_insert_with(|| state.token_y_mint.to_string());
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let mut request = req;
        let capped = self.clamp_request_amount(&state, &request);
        if capped == 0 {
            return None;
        }
        request.amount = capped;
        let simulation = match simulate_lb_clmm_exact_in(&self.config, &state, &request) {
            Ok(outcome) => outcome,
            Err(err) => {
                if matches!(err, LbClmmSimulationError::InsufficientBins { .. }) {
                    self.refresh_window(&state, err.missing_bin_id());
                }
                let mut response = build_quote_response(request, 0);
                response.status = err.into();
                return Some(response);
            }
        };
        let mut response = build_quote_response(request.clone(), simulation.amount_out);
        response.edge_bps = simulation.price_impact_bps;
        response.min_output_amount = Some(simulation.amount_out);
        response.context.compute_unit_limit = Some(LB_CLMM_DEFAULT_CU_LIMIT);
        response.context.compute_unit_price = Some(LB_CLMM_DEFAULT_CU_PRICE);
        response.context.expected_fee_amount = Some(simulation.taker_fee_amount);
        if let Some(host_fee) = state.host_fee_account {
            response.context.host_fee_account = Some(host_fee);
        }
        if let Some(extension) = state.bin_array_bitmap_extension {
            response
                .context
                .remaining_accounts
                .push(AccountMeta::new_readonly(extension, false));
        }
        self.decorate_swap_context(
            &state,
            &mut response.context,
            request.input_mint,
            request.output_mint,
        );
        Some(response)
    }

    fn bidirectional_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
        let state = self.get_state(&pool_pubkey)?;

        let base_reserve = state.reserve_x_balance.or(Some(snapshot.base_post_amount));
        let quote_reserve = state.reserve_y_balance.or(Some(snapshot.quote_post_amount));

        let utilization = self.config.max_swap_utilization_bps;
        let base_probe =
            base_reserve.and_then(|reserve| snapshot_probe_amount(reserve, utilization))?;
        let quote_probe =
            quote_reserve.and_then(|reserve| snapshot_probe_amount(reserve, utilization))?;

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

fn build_lb_clmm_alt_accounts(
    state: &LbClmmState,
    token_x_program: Pubkey,
    token_y_program: Pubkey,
) -> Vec<Pubkey> {
    let mut accounts = Vec::with_capacity(12);
    push_unique(&mut accounts, state.lb_pair);
    push_unique(&mut accounts, state.token_x_mint);
    push_unique(&mut accounts, state.token_y_mint);
    push_unique(&mut accounts, token_x_program);
    push_unique(&mut accounts, token_y_program);
    if let Some(pk) = state.reserve_x {
        push_unique(&mut accounts, pk);
    }
    if let Some(pk) = state.reserve_y {
        push_unique(&mut accounts, pk);
    }
    if let Some(pk) = state.oracle {
        push_unique(&mut accounts, pk);
    }
    if let Some(pk) = state.bin_array_bitmap_extension {
        push_unique(&mut accounts, pk);
    }
    if let Some(pk) = state.host_fee_account {
        push_unique(&mut accounts, pk);
    }
    accounts
}

fn push_unique(accounts: &mut Vec<Pubkey>, value: Pubkey) {
    if !accounts.iter().any(|existing| existing == &value) {
        accounts.push(value);
    }
}

fn needs_hydration(extras: &LbClmmPoolExtras) -> bool {
    extras.token_x_mint.is_none()
        || extras.token_y_mint.is_none()
        || extras.reserve_x.is_none()
        || extras.reserve_y.is_none()
        || extras.oracle.is_none()
        || extras.token_x_program.is_none()
        || extras.token_y_program.is_none()
        || extras.bin_step.is_none()
        || extras.active_bin_id.is_none()
}

fn token_program_for_mint(state: &LbClmmState, mint: &Pubkey) -> Pubkey {
    if *mint == state.token_x_mint {
        state.token_x_program.unwrap_or(*SPL_TOKEN_PROGRAM_PUBKEY)
    } else if *mint == state.token_y_mint {
        state.token_y_program.unwrap_or(*SPL_TOKEN_PROGRAM_PUBKEY)
    } else {
        *SPL_TOKEN_PROGRAM_PUBKEY
    }
}

fn build_bin_liquidity_map(extras: &LbClmmPoolExtras) -> BTreeMap<i32, LbClmmBinSnapshot> {
    let mut map = BTreeMap::new();
    for array in &extras.bin_arrays {
        for bin in &array.bins {
            map.insert(
                bin.bin_id,
                LbClmmBinSnapshot {
                    bin_id: bin.bin_id,
                    liquidity_x: bin.liquidity_x,
                    liquidity_y: bin.liquidity_y,
                },
            );
        }
    }
    map
}

#[derive(Debug, Clone)]
pub struct LbClmmState {
    pub lb_pair: Pubkey,
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub token_x_program: Option<Pubkey>,
    pub token_y_program: Option<Pubkey>,
    pub reserve_x: Option<Pubkey>,
    pub reserve_y: Option<Pubkey>,
    pub oracle: Option<Pubkey>,
    pub bin_array_bitmap_extension: Option<Pubkey>,
    pub host_fee_account: Option<Pubkey>,
    pub bin_step: Option<u16>,
    pub active_bin_id: Option<i32>,
    pub dynamic_fee_bps: Option<u16>,
    pub protocol_fee_bps: Option<u16>,
    pub reserve_x_balance: Option<u128>,
    pub reserve_y_balance: Option<u128>,
    pub volatility_bps: Option<u16>,
    pub bin_liquidity: BTreeMap<i32, LbClmmBinSnapshot>,
    pub active_bitmap: Option<LbClmmBinBitmap>,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
}

impl LbClmmState {
    pub fn mid_price(&self) -> Option<f64> {
        match (self.reserve_x_balance, self.reserve_y_balance) {
            (Some(x), Some(y)) if x > 0 => Some(y as f64 / x as f64),
            _ => None,
        }
    }

    fn resolve_reserves(&self, input_mint: &Pubkey, output_mint: &Pubkey) -> Option<(u128, u128)> {
        let (reserve_x, reserve_y) = (self.reserve_x_balance?, self.reserve_y_balance?);
        if *input_mint == self.token_x_mint && *output_mint == self.token_y_mint {
            Some((reserve_x, reserve_y))
        } else if *input_mint == self.token_y_mint && *output_mint == self.token_x_mint {
            Some((reserve_y, reserve_x))
        } else {
            None
        }
    }

    fn taker_fee_bps(&self) -> u64 {
        let dynamic = self.dynamic_fee_bps.unwrap_or(DEFAULT_DYNAMIC_FEE_BPS) as u64;
        let volatility = self.volatility_bps.unwrap_or(0) as u64;
        dynamic.saturating_add(volatility).min(BPS_DENOMINATOR)
    }

    fn bin_array_has_liquidity(&self, bin_id: i32) -> Option<bool> {
        let bitmap = self.active_bitmap.as_ref()?;
        let array_index = array_index_for_bin(bin_id);
        let idx = i32::try_from(array_index).ok()?;
        bitmap.bit(idx)
    }
}

#[derive(Debug, Clone)]
pub struct LbClmmBinSnapshot {
    pub bin_id: i32,
    pub liquidity_x: u128,
    pub liquidity_y: u128,
}

#[derive(Debug, Clone)]
pub struct LbClmmBinBitmap {
    anchor_bin_id: i32,
    raw_bitmap: Vec<u8>,
    default_bitmap: [u64; DEFAULT_BITMAP_WORDS],
    negative_extension: Vec<[u64; BITMAP_SEGMENT_WORDS]>,
    positive_extension: Vec<[u64; BITMAP_SEGMENT_WORDS]>,
}

impl From<LbClmmBitmapExtras> for LbClmmBinBitmap {
    fn from(value: LbClmmBitmapExtras) -> Self {
        let mut bitmap = Self {
            anchor_bin_id: value.anchor_bin_id,
            raw_bitmap: value.bitmap,
            default_bitmap: [0u64; DEFAULT_BITMAP_WORDS],
            negative_extension: Vec::new(),
            positive_extension: Vec::new(),
        };
        bitmap.rebuild_segments();
        bitmap
    }
}

impl LbClmmBinBitmap {
    fn rebuild_segments(&mut self) {
        self.default_bitmap = [0u64; DEFAULT_BITMAP_WORDS];
        self.negative_extension.clear();
        self.positive_extension.clear();

        let default_bytes = DEFAULT_BITMAP_WORDS * 8;
        for (idx, chunk) in self
            .raw_bitmap
            .get(0..default_bytes)
            .unwrap_or(&[])
            .chunks_exact(8)
            .enumerate()
        {
            let mut word = [0u8; 8];
            word.copy_from_slice(chunk);
            self.default_bitmap[idx] = u64::from_le_bytes(word);
        }

        if self.raw_bitmap.len() > default_bytes {
            let remaining = &self.raw_bitmap[default_bytes..];
            let half = remaining.len() / 2;
            let (negative_bytes, positive_bytes) = remaining.split_at(half);
            self.negative_extension = parse_extension_segments(negative_bytes);
            self.positive_extension = parse_extension_segments(positive_bytes);
        }
    }

    fn anchor_with_bin(&mut self, bin_id: i32) {
        if let Ok(index) = i32::try_from(array_index_for_bin(bin_id)) {
            self.anchor_bin_id = index;
        }
    }

    fn bit(&self, array_index: i32) -> Option<bool> {
        let relative = array_index - self.anchor_bin_id;
        let half_window = BIN_ARRAY_BITMAP_SIZE_I32 / 2;
        let segment_span = (BITMAP_SEGMENT_WORDS * 64) as i32;

        if (-half_window..half_window).contains(&relative) {
            let idx = (relative + half_window) as usize;
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            return self
                .default_bitmap
                .get(word_idx)
                .map(|word| (*word & (1u64 << bit_idx)) != 0);
        }

        if relative >= half_window {
            let offset = relative - half_window;
            let segment_idx = (offset / segment_span) as usize;
            let within_segment = (offset % segment_span) as usize;
            return self
                .positive_extension
                .get(segment_idx)
                .and_then(|segment| bit_from_segment(segment, within_segment));
        }

        let offset = (-relative) - half_window;
        if offset < 0 {
            return None;
        }
        let segment_idx = (offset / segment_span) as usize;
        let within_segment = (offset % segment_span) as usize;
        self.negative_extension
            .get(segment_idx)
            .and_then(|segment| bit_from_segment(segment, within_segment))
    }
}

fn bit_from_segment(segment: &[u64; BITMAP_SEGMENT_WORDS], bit_index: usize) -> Option<bool> {
    let word_idx = bit_index / 64;
    let within_word = bit_index % 64;
    segment
        .get(word_idx)
        .map(|word| (*word & (1u64 << within_word)) != 0)
}

fn parse_extension_segments(bytes: &[u8]) -> Vec<[u64; BITMAP_SEGMENT_WORDS]> {
    bytes
        .chunks_exact(BITMAP_SEGMENT_BYTES)
        .map(|chunk| {
            let mut words = [0u64; BITMAP_SEGMENT_WORDS];
            for (idx, word_bytes) in chunk.chunks_exact(8).enumerate() {
                let mut word = [0u8; 8];
                word.copy_from_slice(word_bytes);
                words[idx] = u64::from_le_bytes(word);
            }
            words
        })
        .collect()
}

#[derive(Clone, Copy)]
enum Rounding {
    Down,
    Up,
}

#[derive(Clone, Copy)]
struct LbClmmBinState {
    bin_id: i32,
    amount_x: u64,
    amount_y: u64,
}

impl LbClmmBinState {
    fn from_snapshot(snapshot: &LbClmmBinSnapshot) -> Self {
        Self {
            bin_id: snapshot.bin_id,
            amount_x: snapshot.liquidity_x.min(u64::MAX as u128) as u64,
            amount_y: snapshot.liquidity_y.min(u64::MAX as u128) as u64,
        }
    }

    fn available_out(&self, swap_x_to_y: bool) -> u128 {
        if swap_x_to_y {
            self.amount_y as u128
        } else {
            self.amount_x as u128
        }
    }
}

struct BinSwapOutcome {
    gross_input: u128,
    net_input: u128,
    amount_out: u128,
    bin_drained: bool,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct LbClmmSimulationOutcome {
    amount_out: u64,
    price_impact_bps: f64,
    taker_fee_amount: u64,
    protocol_fee_amount: u64,
    host_fee_amount: u64,
    lp_fee_amount: u64,
}

#[derive(Debug, Clone, Copy)]
enum LbClmmSimulationError {
    MissingState,
    InsufficientLiquidity,
    InsufficientBins { missing_bin_id: Option<i32> },
    PriceImpactExceeded { observed_bps: f64 },
}

impl From<LbClmmSimulationError> for QuoteStatus {
    fn from(value: LbClmmSimulationError) -> Self {
        match value {
            LbClmmSimulationError::MissingState => QuoteStatus::MissingState,
            LbClmmSimulationError::InsufficientLiquidity => QuoteStatus::InsufficientLiquidity,
            LbClmmSimulationError::InsufficientBins { .. } => QuoteStatus::InsufficientBins,
            LbClmmSimulationError::PriceImpactExceeded { .. } => QuoteStatus::PriceImpactExceeded,
        }
    }
}

impl LbClmmSimulationError {
    fn observed_slippage_bps(&self) -> f64 {
        match self {
            LbClmmSimulationError::PriceImpactExceeded { observed_bps } => observed_bps.abs(),
            _ => 0.0,
        }
    }

    fn missing_bin_id(&self) -> Option<i32> {
        match self {
            LbClmmSimulationError::InsufficientBins { missing_bin_id } => *missing_bin_id,
            _ => None,
        }
    }
}

fn simulate_lb_clmm_exact_in(
    config: &LbClmmAdapterConfig,
    state: &LbClmmState,
    req: &QuoteRequest,
) -> Result<LbClmmSimulationOutcome, LbClmmSimulationError> {
    if req.amount == 0 {
        return Err(LbClmmSimulationError::InsufficientLiquidity);
    }

    let swap_x_to_y = req.input_mint == state.token_x_mint;
    let fee_bps = state.taker_fee_bps();
    if fee_bps >= BPS_DENOMINATOR {
        return Err(LbClmmSimulationError::MissingState);
    }

    let bin_step = state.bin_step.ok_or(LbClmmSimulationError::MissingState)?;
    let active_bin = state
        .active_bin_id
        .ok_or(LbClmmSimulationError::MissingState)?;
    if state.bin_liquidity.is_empty() {
        return Err(LbClmmSimulationError::InsufficientBins {
            missing_bin_id: Some(active_bin),
        });
    }

    let mut remaining_gross = u128::from(req.amount);
    let mut total_out: u128 = 0;
    let mut total_net_in: u128 = 0;
    let mut total_fee_amount: u128 = 0;
    let mut last_price: Option<u128> = None;
    let mut current_bin = active_bin;
    let mut bins_wandered: usize = 0;

    while remaining_gross > 0 {
        if bins_wandered >= config.max_bin_walk {
            return Err(LbClmmSimulationError::InsufficientBins {
                missing_bin_id: Some(current_bin),
            });
        }

        let snapshot = match state.bin_liquidity.get(&current_bin) {
            Some(snapshot) => snapshot.clone(),
            None => match state.bin_array_has_liquidity(current_bin) {
                Some(true) => {
                    return Err(LbClmmSimulationError::InsufficientBins {
                        missing_bin_id: Some(current_bin),
                    });
                }
                Some(false) => {
                    bins_wandered += 1;
                    current_bin = step_bin_id(current_bin, swap_x_to_y)
                        .ok_or(LbClmmSimulationError::MissingState)?;
                    continue;
                }
                None => {
                    return Err(LbClmmSimulationError::InsufficientBins {
                        missing_bin_id: Some(current_bin),
                    });
                }
            },
        };

        let mut bin_state = LbClmmBinState::from_snapshot(&snapshot);
        if bin_state.available_out(swap_x_to_y) == 0 {
            bins_wandered += 1;
            current_bin =
                step_bin_id(current_bin, swap_x_to_y).ok_or(LbClmmSimulationError::MissingState)?;
            continue;
        }

        let price = get_price_from_id(bin_state.bin_id, bin_step)
            .ok_or(LbClmmSimulationError::MissingState)?;
        last_price = Some(price);
        let outcome =
            execute_bin_swap(&mut bin_state, price, remaining_gross, swap_x_to_y, fee_bps)
                .ok_or(LbClmmSimulationError::InsufficientLiquidity)?;
        if outcome.amount_out == 0 || outcome.net_input == 0 {
            break;
        }

        total_out = total_out.saturating_add(outcome.amount_out);
        total_net_in = total_net_in.saturating_add(outcome.net_input);
        total_fee_amount =
            total_fee_amount.saturating_add(outcome.gross_input.saturating_sub(outcome.net_input));
        remaining_gross = remaining_gross.saturating_sub(outcome.gross_input);

        bins_wandered += 1;
        if remaining_gross == 0 {
            break;
        }
        if outcome.bin_drained {
            current_bin =
                step_bin_id(current_bin, swap_x_to_y).ok_or(LbClmmSimulationError::MissingState)?;
            continue;
        }
        // Partial drain satisfied request; exit loop.
        break;
    }

    if remaining_gross > 0 {
        return Err(LbClmmSimulationError::InsufficientLiquidity);
    }

    if total_out == 0 || total_net_in == 0 {
        return Err(LbClmmSimulationError::InsufficientLiquidity);
    }

    let trade_price = if swap_x_to_y {
        total_out as f64 / total_net_in as f64
    } else {
        total_net_in as f64 / total_out as f64
    };
    if !trade_price.is_finite() || trade_price <= 0.0 {
        return Err(LbClmmSimulationError::InsufficientLiquidity);
    }

    let reference_price =
        reference_price_for_state(state, last_price).ok_or(LbClmmSimulationError::MissingState)?;
    let price_impact_bps = compute_edge_bps(trade_price, reference_price);
    if price_impact_bps.abs() > f64::from(config.max_price_impact_bps) {
        return Err(LbClmmSimulationError::PriceImpactExceeded {
            observed_bps: price_impact_bps,
        });
    }

    let protocol_share_bps = state.protocol_fee_bps.unwrap_or(0) as u128;
    let protocol_fee =
        total_fee_amount.saturating_mul(protocol_share_bps) / (BPS_DENOMINATOR as u128);
    let host_fee = if state.host_fee_account.is_some() {
        protocol_fee.saturating_mul(HOST_FEE_SHARE_BPS as u128) / (BPS_DENOMINATOR as u128)
    } else {
        0
    };
    let lp_fee = total_fee_amount.saturating_sub(protocol_fee);
    let net_protocol_fee = protocol_fee.saturating_sub(host_fee);

    Ok(LbClmmSimulationOutcome {
        amount_out: total_out.min(u64::MAX as u128) as u64,
        price_impact_bps,
        taker_fee_amount: total_fee_amount.min(u64::MAX as u128) as u64,
        protocol_fee_amount: net_protocol_fee.min(u64::MAX as u128) as u64,
        host_fee_amount: host_fee.min(u64::MAX as u128) as u64,
        lp_fee_amount: lp_fee.min(u64::MAX as u128) as u64,
    })
}

fn derive_price_impact_limit(slippage_bps: f64, volatility_bps: Option<u16>) -> u16 {
    let abs_slippage = if slippage_bps.is_finite() {
        slippage_bps.abs()
    } else {
        0.0
    };
    let floor = f64::from(PRICE_IMPACT_FLOOR_BPS);
    let base = abs_slippage.max(floor).ceil() as u64;
    let headroom = PRICE_IMPACT_PADDING_BPS as u64;
    let volatility = volatility_bps.unwrap_or(0) as u64;
    let total = base.saturating_add(volatility).saturating_add(headroom);
    total.min(u16::MAX as u64) as u16
}

fn execute_bin_swap(
    bin: &mut LbClmmBinState,
    price: u128,
    remaining_gross: u128,
    swap_x_to_y: bool,
    fee_bps: u64,
) -> Option<BinSwapOutcome> {
    let available_out = bin.available_out(swap_x_to_y);
    if available_out == 0 {
        return None;
    }

    let target_out_u64 = available_out.min(u64::MAX as u128) as u64;
    let net_for_full = amount_in_for_output(target_out_u64, price, swap_x_to_y)? as u128;
    if net_for_full == 0 {
        return None;
    }
    let gross_for_full = restore_from_fee(net_for_full, fee_bps);

    if remaining_gross >= gross_for_full {
        return Some(BinSwapOutcome {
            gross_input: gross_for_full,
            net_input: net_for_full,
            amount_out: available_out,
            bin_drained: true,
        });
    }

    let net_from_partial = apply_fee(remaining_gross, fee_bps)?;
    if net_from_partial == 0 {
        return None;
    }
    let partial_in = net_from_partial.min(u64::MAX as u128) as u64;
    let mut amount_out = amount_out_from_input(partial_in, price, swap_x_to_y)? as u128;
    if amount_out > available_out {
        amount_out = available_out;
    }
    Some(BinSwapOutcome {
        gross_input: remaining_gross,
        net_input: net_from_partial,
        amount_out,
        bin_drained: amount_out == available_out,
    })
}

fn amount_out_from_input(amount_in: u64, price: u128, swap_x_to_y: bool) -> Option<u64> {
    let raw = if swap_x_to_y {
        shl_div_q64(amount_in as u128, price, Rounding::Down)?
    } else {
        mul_shr_q64(price, amount_in as u128, Rounding::Down)?
    };
    Some(raw.min(u64::MAX as u128) as u64)
}

fn amount_in_for_output(amount_out: u64, price: u128, swap_x_to_y: bool) -> Option<u64> {
    let raw = if swap_x_to_y {
        mul_shr_q64(price, amount_out as u128, Rounding::Up)?
    } else {
        shl_div_q64(amount_out as u128, price, Rounding::Up)?
    };
    Some(raw.min(u64::MAX as u128) as u64)
}

fn reference_price_for_state(state: &LbClmmState, cached_price: Option<u128>) -> Option<f64> {
    if let Some(value) = cached_price {
        let inverted = invert_q64(value, Rounding::Down)?;
        return Some(q64_to_f64(inverted));
    }
    let price = get_price_from_id(state.active_bin_id?, state.bin_step?)?;
    let inverted = invert_q64(price, Rounding::Down)?;
    Some(q64_to_f64(inverted))
}

fn q64_to_f64(value: u128) -> f64 {
    value as f64 / (Q64_ONE as f64)
}

fn apply_fee(amount: u128, fee_bps: u64) -> Option<u128> {
    if fee_bps >= BPS_DENOMINATOR {
        return None;
    }
    let numerator = (BPS_DENOMINATOR - fee_bps) as u128;
    let denominator = BPS_DENOMINATOR as u128;
    Some(amount.saturating_mul(numerator) / denominator)
}

fn restore_from_fee(net: u128, fee_bps: u64) -> u128 {
    let denominator = (BPS_DENOMINATOR - fee_bps) as u128;
    if denominator == 0 {
        return net;
    }
    (net.saturating_mul(BPS_DENOMINATOR as u128) + denominator - 1) / denominator
}

fn get_price_from_id(bin_id: i32, bin_step: u16) -> Option<u128> {
    let scaled_step = u128::from(bin_step)
        .checked_shl(Q64_SCALE_OFFSET)?
        .checked_div(BPS_DENOMINATOR as u128)?;
    let base = Q64_ONE.checked_add(scaled_step)?;
    pow_q64(base, bin_id)
}

fn pow_q64(base: u128, exp: i32) -> Option<u128> {
    if exp == 0 {
        return Some(Q64_ONE);
    }
    let mut exponent = exp.unsigned_abs();
    if exponent >= MAX_Q64_EXPONENT {
        return None;
    }
    let mut result = Q64_ONE;
    let mut current = base;
    while exponent > 0 {
        if (exponent & 1) == 1 {
            result = mul_shr_q64(result, current, Rounding::Down)?;
        }
        exponent >>= 1;
        if exponent > 0 {
            current = mul_shr_q64(current, current, Rounding::Down)?;
        }
    }
    if exp.is_negative() {
        return invert_q64(result, Rounding::Down);
    }
    Some(result)
}

fn invert_q64(value: u128, rounding: Rounding) -> Option<u128> {
    if value == 0 {
        return None;
    }
    shl_div_q64(Q64_ONE, value, rounding)
}

fn mul_shr_q64(x: u128, y: u128, rounding: Rounding) -> Option<u128> {
    let denominator = 1u128.checked_shl(Q64_SCALE_OFFSET)?;
    mul_div_u128(x, y, denominator, rounding)
}

fn shl_div_q64(x: u128, y: u128, rounding: Rounding) -> Option<u128> {
    if y == 0 {
        return None;
    }
    let numerator = U256::from(x) << Q64_SCALE_OFFSET;
    let denom = U256::from(y);
    let quotient = numerator / denom;
    let remainder = numerator % denom;
    let adjusted = match rounding {
        Rounding::Down => quotient,
        Rounding::Up => {
            if remainder.is_zero() {
                quotient
            } else {
                quotient.checked_add(U256::one())?
            }
        }
    };
    adjusted.try_into().ok()
}

fn mul_div_u128(x: u128, y: u128, denominator: u128, rounding: Rounding) -> Option<u128> {
    if denominator == 0 {
        return None;
    }
    let product = U256::from(x).checked_mul(U256::from(y))?;
    let denom = U256::from(denominator);
    let quotient = product / denom;
    let remainder = product % denom;
    let adjusted = match rounding {
        Rounding::Down => quotient,
        Rounding::Up => {
            if remainder.is_zero() {
                quotient
            } else {
                quotient.checked_add(U256::one())?
            }
        }
    };
    adjusted.try_into().ok()
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

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    token_x_mint: Pubkey,
    token_y_mint: Pubkey,
    reserve_x: Pubkey,
    reserve_y: Pubkey,
    oracle: Pubkey,
    token_x_program: Option<Pubkey>,
    token_y_program: Option<Pubkey>,
    bin_step: u16,
    active_bin_id: i32,
    bin_arrays: Vec<LbClmmBinArrayExtras>,
    active_bitmap: LbClmmBitmapExtras,
}

#[derive(Debug, Clone, Copy)]
struct BinWindow {
    start_index: i64,
    end_index: i64,
    start_bin_id: i32,
    end_bin_id: i32,
}

impl BinWindow {
    fn around(center_bin_id: i32) -> Self {
        let center_index = array_index_for_bin(center_bin_id);
        let start_index = center_index.saturating_sub(TARGETED_BIN_ARRAY_RADIUS);
        let end_index = center_index.saturating_add(TARGETED_BIN_ARRAY_RADIUS);
        Self::from_indices(start_index, end_index)
    }

    fn from_arrays(arrays: &[LbClmmBinArrayExtras]) -> Option<Self> {
        if arrays.is_empty() {
            return None;
        }
        let start_bin = arrays.iter().map(|array| array.start_bin_id).min()?;
        let end_bin = arrays
            .iter()
            .map(|array| array.start_bin_id.saturating_add(MAX_BIN_PER_ARRAY - 1))
            .max()?;
        let start_index = array_index_for_bin(start_bin);
        let end_index = array_index_for_bin(end_bin);
        Some(Self::from_indices(start_index, end_index))
    }

    fn from_indices(start_index: i64, end_index: i64) -> Self {
        let start_bin_id = start_bin_id_from_index(start_index).unwrap_or(i32::MIN);
        let end_start = start_bin_id_from_index(end_index).unwrap_or(i32::MAX);
        let end_bin_id = end_start.saturating_add(MAX_BIN_PER_ARRAY - 1);
        Self {
            start_index,
            end_index,
            start_bin_id,
            end_bin_id,
        }
    }

    fn contains(&self, bin_id: i32) -> bool {
        bin_id >= self.start_bin_id && bin_id <= self.end_bin_id
    }

    fn needs_refresh(&self, active_bin_id: i32) -> bool {
        if !self.contains(active_bin_id) {
            return true;
        }
        let left_margin = active_bin_id.saturating_sub(self.start_bin_id);
        let right_margin = self.end_bin_id.saturating_sub(active_bin_id);
        left_margin < BIN_WINDOW_EDGE_BUFFER_BINS || right_margin < BIN_WINDOW_EDGE_BUFFER_BINS
    }

    fn indices(&self) -> impl Iterator<Item = i64> {
        self.start_index..=self.end_index
    }
}

#[derive(Debug, Clone, Copy)]
enum BinFetchPlan {
    Full,
    Window(BinWindow),
}

struct LbClmmPoolHydrator {
    program_id: Pubkey,
    hydrator: AsyncHydrator<Pubkey, HydratedPoolMeta>,
    cache: Arc<DashMap<Pubkey, HydratedPoolMeta>>,
    extensions_pending: Arc<DashSet<Pubkey>>,
    window_cache: Arc<DashMap<Pubkey, BinWindow>>,
    clients: Arc<Vec<Arc<RpcClient>>>,
    endpoint_count: usize,
}

impl LbClmmPoolHydrator {
    fn new(program_id: Pubkey, config: &LbClmmAdapterConfig) -> Option<Self> {
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

        let endpoint_count = clients.len();
        let clients = Arc::new(clients);
        let fetch_clients = clients.clone();
        let cache = Arc::new(DashMap::new());
        let cache_for_fetch = cache.clone();
        let extensions_pending = Arc::new(DashSet::new());
        let pending_for_fetch = extensions_pending.clone();
        let window_cache = Arc::new(DashMap::new());
        let window_cache_for_fetch = window_cache.clone();
        let hydrator = AsyncHydrator::with_cache(
            "lb_clmm_pool",
            endpoint_count,
            move |pool| {
                fetch_lb_clmm_meta(
                    program_id,
                    fetch_clients.as_ref(),
                    pool,
                    cache_for_fetch.clone(),
                    pending_for_fetch.clone(),
                    window_cache_for_fetch.clone(),
                )
            },
            cache.clone(),
        );

        Some(Self {
            program_id,
            hydrator,
            cache,
            extensions_pending,
            window_cache,
            clients,
            endpoint_count,
        })
    }

    fn client_count(&self) -> usize {
        self.endpoint_count
    }

    fn hydrate(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        if let Some(entry) = self.hydrator.cached(&pool) {
            if entry.bin_arrays.is_empty() {
                self.hydrator.schedule(pool);
            }
            return Some(entry);
        }
        self.hydrator.get_or_schedule(pool)
    }

    fn hydrate_blocking(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        if let Some(entry) = self.cache.get(&pool) {
            return Some(entry.clone());
        }

        let clients: &[Arc<RpcClient>] = self.clients.as_ref().as_slice();
        fetch_lb_clmm_meta(
            self.program_id,
            clients,
            pool,
            self.cache.clone(),
            self.extensions_pending.clone(),
            self.window_cache.clone(),
        )
    }

    fn ensure_window(&self, pool: Pubkey, target_bin_id: i32) {
        if self.extensions_pending.contains(&pool) {
            return;
        }
        let desired = BinWindow::around(target_bin_id);
        if let Some(existing) = self.window_cache.get(&pool) {
            let entry = existing.value();
            if entry.contains(target_bin_id) && !entry.needs_refresh(target_bin_id) {
                return;
            }
        }
        let clients = self.clients.iter().cloned().collect();
        stream_lb_clmm_extensions(
            self.program_id,
            clients,
            pool,
            self.cache.clone(),
            self.extensions_pending.clone(),
            self.window_cache.clone(),
            BinFetchPlan::Window(desired),
        );
    }
}

fn fetch_lb_clmm_meta(
    program_id: Pubkey,
    clients: &[Arc<RpcClient>],
    pool: Pubkey,
    cache: Arc<DashMap<Pubkey, HydratedPoolMeta>>,
    pending_extensions: Arc<DashSet<Pubkey>>,
    window_cache: Arc<DashMap<Pubkey, BinWindow>>,
) -> Option<HydratedPoolMeta> {
    for client in clients {
        match client.get_account(&pool) {
            Ok(account) => {
                if let Some(meta) = parse_lb_pair_account(&account.data) {
                    cache.insert(pool, meta.clone());
                    stream_lb_clmm_extensions(
                        program_id,
                        clients.iter().cloned().collect(),
                        pool,
                        cache.clone(),
                        pending_extensions.clone(),
                        window_cache.clone(),
                        BinFetchPlan::Full,
                    );
                    return Some(meta);
                } else {
                    warn!(
                        target = "adapter::lb_clmm",
                        pool = %pool,
                        "unexpected lb_clmm pool account layout"
                    );
                }
            }
            Err(error) => {
                warn!(
                    target = "adapter::lb_clmm",
                    pool = %pool,
                    %error,
                    "failed to fetch lb_clmm pool account"
                );
            }
        }
    }

    None
}

fn stream_lb_clmm_extensions(
    program_id: Pubkey,
    clients: Vec<Arc<RpcClient>>,
    pool: Pubkey,
    cache: Arc<DashMap<Pubkey, HydratedPoolMeta>>,
    pending_extensions: Arc<DashSet<Pubkey>>,
    window_cache: Arc<DashMap<Pubkey, BinWindow>>,
    plan: BinFetchPlan,
) {
    if !pending_extensions.insert(pool) {
        return;
    }

    debug!(
        target = "adapter::lb_clmm",
        pool = %pool,
        ?plan,
        "scheduling async lb_clmm bin hydration"
    );

    thread::spawn(move || {
        for client in &clients {
            let mut updated = match cache.get(&pool) {
                Some(entry) => entry.clone(),
                None => continue,
            };
            let mut staged = false;

            match plan {
                BinFetchPlan::Window(window) => {
                    let arrays = fetch_targeted_bin_window(program_id, client, pool, &window);
                    if !arrays.is_empty() {
                        updated.bin_arrays = merge_bin_array_sets(updated.bin_arrays, arrays);
                        window_cache.insert(pool, window);
                        staged = true;
                    }
                }
                BinFetchPlan::Full => {
                    let window = BinWindow::around(updated.active_bin_id);
                    let targeted = fetch_targeted_bin_window(program_id, client, pool, &window);
                    if !targeted.is_empty() {
                        updated.bin_arrays = merge_bin_array_sets(updated.bin_arrays, targeted);
                        window_cache.insert(pool, window);
                        cache.insert(pool, updated.clone());
                        staged = true;
                    }

                    let bin_arrays = fetch_bin_arrays(program_id, client, pool);
                    if !bin_arrays.is_empty() {
                        updated.bin_arrays = merge_bin_array_sets(Vec::new(), bin_arrays);
                        if let Some(full_window) = BinWindow::from_arrays(&updated.bin_arrays) {
                            window_cache.insert(pool, full_window);
                        }
                        staged = true;
                    }

                    if let Some(extension) = fetch_bitmap_extension(program_id, client, pool) {
                        updated.active_bitmap.bitmap.extend(extension.bitmap);
                        staged = true;
                    }
                }
            }

            if staged {
                cache.insert(pool, updated);
                break;
            }
        }
        pending_extensions.remove(&pool);
    });
}

fn fetch_bin_arrays(
    program_id: Pubkey,
    client: &RpcClient,
    pool: Pubkey,
) -> Vec<LbClmmBinArrayExtras> {
    let filters = Some(vec![
        RpcFilterType::DataSize(BIN_ARRAY_ACCOUNT_LEN as u64),
        RpcFilterType::Memcmp(Memcmp::new(
            BIN_ARRAY_LB_PAIR_OFFSET,
            MemcmpEncodedBytes::Bytes(pool.to_bytes().to_vec()),
        )),
    ]);
    let mut account_config = RpcAccountInfoConfig::default();
    account_config.encoding = Some(UiAccountEncoding::Base64);
    let config = RpcProgramAccountsConfig {
        filters,
        account_config,
        with_context: None,
        sort_results: None,
    };

    match client.get_program_ui_accounts_with_config(&program_id, config) {
        Ok(accounts) => accounts
            .into_iter()
            .filter_map(|(_, account)| {
                let data = decode_ui_account_bytes(&account)?;
                parse_bin_array_account(pool, &data)
            })
            .collect(),
        Err(error) => {
            warn!(
                target = "adapter::lb_clmm",
                pool = %pool,
                %error,
                "failed to fetch lb_clmm bin arrays"
            );
            Vec::new()
        }
    }
}

fn fetch_targeted_bin_window(
    program_id: Pubkey,
    client: &RpcClient,
    pool: Pubkey,
    window: &BinWindow,
) -> Vec<LbClmmBinArrayExtras> {
    let mut arrays = Vec::new();
    for index in window.indices() {
        if let Some(array) = fetch_bin_array_by_index(program_id, client, pool, index) {
            arrays.push(array);
        }
    }
    arrays
}

fn fetch_bin_array_by_index(
    program_id: Pubkey,
    client: &RpcClient,
    pool: Pubkey,
    index: i64,
) -> Option<LbClmmBinArrayExtras> {
    let mut account_config = RpcAccountInfoConfig::default();
    account_config.encoding = Some(UiAccountEncoding::Base64);
    let filters = Some(vec![
        RpcFilterType::DataSize(BIN_ARRAY_ACCOUNT_LEN as u64),
        RpcFilterType::Memcmp(Memcmp::new(
            BIN_ARRAY_INDEX_OFFSET,
            MemcmpEncodedBytes::Bytes(index.to_le_bytes().to_vec()),
        )),
        RpcFilterType::Memcmp(Memcmp::new(
            BIN_ARRAY_LB_PAIR_OFFSET,
            MemcmpEncodedBytes::Bytes(pool.to_bytes().to_vec()),
        )),
    ]);
    let config = RpcProgramAccountsConfig {
        filters,
        account_config,
        with_context: None,
        sort_results: None,
    };

    match client.get_program_ui_accounts_with_config(&program_id, config) {
        Ok(mut accounts) => accounts.drain(..).find_map(|(_, account)| {
            let data = decode_ui_account_bytes(&account)?;
            parse_bin_array_account(pool, &data)
        }),
        Err(error) => {
            warn!(
                target = "adapter::lb_clmm",
                pool = %pool,
                index,
                %error,
                "failed to fetch targeted lb_clmm bin array"
            );
            None
        }
    }
}

fn merge_bin_array_sets(
    current: Vec<LbClmmBinArrayExtras>,
    additions: Vec<LbClmmBinArrayExtras>,
) -> Vec<LbClmmBinArrayExtras> {
    if additions.is_empty() {
        return current;
    }
    if current.is_empty() {
        let mut arrays = additions;
        arrays.sort_by_key(|array| array.start_bin_id);
        return arrays;
    }
    let mut merged = BTreeMap::new();
    for array in current.into_iter().chain(additions.into_iter()) {
        merged.insert(array.start_bin_id, array);
    }
    merged.into_values().collect()
}

fn fetch_bitmap_extension(
    program_id: Pubkey,
    client: &RpcClient,
    pool: Pubkey,
) -> Option<LbClmmBitmapExtras> {
    let filters = Some(vec![
        RpcFilterType::DataSize(BITMAP_EXTENSION_ACCOUNT_LEN as u64),
        RpcFilterType::Memcmp(Memcmp::new(
            0,
            MemcmpEncodedBytes::Bytes(pool.to_bytes().to_vec()),
        )),
    ]);
    let mut account_config = RpcAccountInfoConfig::default();
    account_config.encoding = Some(UiAccountEncoding::Base64);
    let config = RpcProgramAccountsConfig {
        filters,
        account_config,
        with_context: None,
        sort_results: None,
    };

    match client.get_program_ui_accounts_with_config(&program_id, config) {
        Ok(mut accounts) => accounts.drain(..).find_map(|(_, account)| {
            let data = decode_ui_account_bytes(&account)?;
            parse_bitmap_extension_account(pool, &data)
        }),
        Err(error) => {
            warn!(
                target = "adapter::lb_clmm",
                pool = %pool,
                %error,
                "failed to fetch lb_clmm bitmap extension"
            );
            None
        }
    }
}

fn decode_ui_account_bytes(account: &UiAccount) -> Option<Vec<u8>> {
    match &account.data {
        UiAccountData::Binary(data, UiAccountEncoding::Base64)
        | UiAccountData::LegacyBinary(data) => BASE64_STANDARD.decode(data).ok(),
        _ => None,
    }
}

const STATIC_PARAMETERS_LEN: usize = 32;
const VARIABLE_PARAMETERS_LEN: usize = 32;
const PROTOCOL_FEE_LEN: usize = 16;
const REWARD_INFO_LEN: usize = 144;
const REWARD_INFO_COUNT: usize = 2;
const BIN_ARRAY_BITMAP_BYTES: usize = 16 * 8;
const PUBKEY_BYTES: usize = 32;
const LB_PAIR_ACCOUNT_MIN_LEN: usize = STATIC_PARAMETERS_LEN
        + VARIABLE_PARAMETERS_LEN
        + 1 // bump_seed
        + 2 // bin_step_seed
        + 1 // pair_type
        + 4 // active_id
        + 2 // bin_step
        + 1 // status
        + 1 // require_base_factor_seed
        + 2 // base_factor_seed
        + 1 // activation_type
        + 1 // creator_pool_on_off_control
        + PUBKEY_BYTES * 4 // token mints + reserves
        + PROTOCOL_FEE_LEN
        + 32 // _padding_1
        + REWARD_INFO_LEN * REWARD_INFO_COUNT
        + PUBKEY_BYTES // oracle
        + BIN_ARRAY_BITMAP_BYTES
        + 8 // last_updated_at
        + 32 // _padding_2
        + PUBKEY_BYTES // pre_activation_swap_address
        + PUBKEY_BYTES // base_key
        + 8 // activation_point
        + 8 // pre_activation_duration
        + 8 // _padding_3
        + 8 // _padding_4
        + PUBKEY_BYTES // creator
        + 1 // token_mint_x_program_flag
        + 1 // token_mint_y_program_flag
        + 22; // _reserved

fn parse_lb_pair_account(data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < LB_PAIR_ACCOUNT_MIN_LEN {
        return None;
    }

    let mut cursor = STATIC_PARAMETERS_LEN + VARIABLE_PARAMETERS_LEN;
    skip_bytes(data, &mut cursor, 1)?; // bump_seed
    skip_bytes(data, &mut cursor, 2)?; // bin_step_seed
    skip_bytes(data, &mut cursor, 1)?; // pair_type
    let active_bin_id = read_i32(data, &mut cursor)?;
    let bin_step = read_u16(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 1)?; // status
    skip_bytes(data, &mut cursor, 1)?; // require_base_factor_seed
    skip_bytes(data, &mut cursor, 2)?; // base_factor_seed
    skip_bytes(data, &mut cursor, 1)?; // activation_type
    skip_bytes(data, &mut cursor, 1)?; // creator_pool_on_off_control

    let token_x_mint = read_pubkey_bytes(data, &mut cursor)?;
    let token_y_mint = read_pubkey_bytes(data, &mut cursor)?;
    let reserve_x = read_pubkey_bytes(data, &mut cursor)?;
    let reserve_y = read_pubkey_bytes(data, &mut cursor)?;

    skip_bytes(data, &mut cursor, PROTOCOL_FEE_LEN)?;
    skip_bytes(data, &mut cursor, 32)?; // _padding_1
    skip_bytes(data, &mut cursor, REWARD_INFO_LEN * REWARD_INFO_COUNT)?;
    let oracle = read_pubkey_bytes(data, &mut cursor)?;
    let inline_bitmap = read_bytes(data, &mut cursor, BIN_ARRAY_BITMAP_BYTES)?;
    skip_bytes(data, &mut cursor, 8)?; // last_updated_at
    skip_bytes(data, &mut cursor, 32)?; // _padding_2
    skip_bytes(data, &mut cursor, PUBKEY_BYTES)?; // pre_activation_swap_address
    skip_bytes(data, &mut cursor, PUBKEY_BYTES)?; // base_key
    skip_bytes(data, &mut cursor, 8)?; // activation_point
    skip_bytes(data, &mut cursor, 8)?; // pre_activation_duration
    skip_bytes(data, &mut cursor, 8)?; // _padding_3
    skip_bytes(data, &mut cursor, 8)?; // _padding_4
    skip_bytes(data, &mut cursor, PUBKEY_BYTES)?; // creator
    let token_x_flag = read_u8(data, &mut cursor)?;
    let token_y_flag = read_u8(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 22)?; // _reserved

    Some(HydratedPoolMeta {
        token_x_mint,
        token_y_mint,
        reserve_x,
        reserve_y,
        oracle,
        token_x_program: map_token_program_flag(token_x_flag),
        token_y_program: map_token_program_flag(token_y_flag),
        bin_step,
        active_bin_id,
        bin_arrays: Vec::new(),
        active_bitmap: LbClmmBitmapExtras {
            anchor_bin_id: 0,
            bitmap: inline_bitmap,
        },
    })
}

fn parse_bin_array_account(pool: Pubkey, data: &[u8]) -> Option<LbClmmBinArrayExtras> {
    if data.len() < BIN_ARRAY_ACCOUNT_LEN {
        return None;
    }

    let mut cursor = 0usize;
    let index = read_i64(data, &mut cursor)?;
    let _version = read_u8(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 7)?;
    let owner = read_pubkey_bytes(data, &mut cursor)?;
    if owner != pool {
        return None;
    }

    let start_bin_id = start_bin_id_from_index(index)?;
    let mut bins = Vec::new();
    for offset in 0..(MAX_BIN_PER_ARRAY as usize) {
        let amount_x = read_u64(data, &mut cursor)? as u128;
        let amount_y = read_u64(data, &mut cursor)? as u128;
        skip_bytes(data, &mut cursor, BIN_STRUCT_LEN - 16)?;
        if amount_x == 0 && amount_y == 0 {
            continue;
        }
        let bin_id = start_bin_id.saturating_add(offset as i32);
        bins.push(LbClmmBinLiquidityExtras {
            bin_id,
            liquidity_x: amount_x,
            liquidity_y: amount_y,
        });
    }

    if bins.is_empty() {
        return None;
    }

    Some(LbClmmBinArrayExtras { start_bin_id, bins })
}

fn parse_bitmap_extension_account(pool: Pubkey, data: &[u8]) -> Option<LbClmmBitmapExtras> {
    if data.len() < BITMAP_EXTENSION_ACCOUNT_LEN {
        return None;
    }

    let mut cursor = 0usize;
    let owner = read_pubkey_bytes(data, &mut cursor)?;
    if owner != pool {
        return None;
    }
    let remaining = data.len().checked_sub(cursor)?;
    let bitmap = read_bytes(data, &mut cursor, remaining)?;
    Some(LbClmmBitmapExtras {
        anchor_bin_id: 0,
        bitmap,
    })
}

fn start_bin_id_from_index(index: i64) -> Option<i32> {
    let base = index.checked_mul(i64::from(MAX_BIN_PER_ARRAY))?;
    i32::try_from(base).ok()
}

fn array_index_for_bin(bin_id: i32) -> i64 {
    let step = i64::from(MAX_BIN_PER_ARRAY);
    let bin = i64::from(bin_id);
    let mut index = bin / step;
    if bin < 0 && bin % step != 0 {
        index -= 1;
    }
    index
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Option<u16> {
    let end = (*cursor).checked_add(2)?;
    let bytes = data.get(*cursor..end)?;
    *cursor = end;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Option<i32> {
    let end = (*cursor).checked_add(4)?;
    let bytes = data.get(*cursor..end)?;
    *cursor = end;
    Some(i32::from_le_bytes(bytes.try_into().ok()?))
}

fn read_i64(data: &[u8], cursor: &mut usize) -> Option<i64> {
    let end = (*cursor).checked_add(8)?;
    let bytes = data.get(*cursor..end)?;
    *cursor = end;
    Some(i64::from_le_bytes(bytes.try_into().ok()?))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Option<u64> {
    let end = (*cursor).checked_add(8)?;
    let bytes = data.get(*cursor..end)?;
    *cursor = end;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

fn read_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<Vec<u8>> {
    let end = (*cursor).checked_add(len)?;
    let bytes = data.get(*cursor..end)?;
    *cursor = end;
    Some(bytes.to_vec())
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    let value = *data.get(*cursor)?;
    *cursor = (*cursor).checked_add(1)?;
    Some(value)
}

fn skip_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<()> {
    let end = (*cursor).checked_add(len)?;
    if end > data.len() {
        return None;
    }
    *cursor = end;
    Some(())
}

fn read_pubkey_bytes(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    let end = (*cursor).checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

fn step_bin_id(current: i32, swap_x_to_y: bool) -> Option<i32> {
    if swap_x_to_y {
        current.checked_add(1)
    } else {
        current.checked_sub(1)
    }
}

fn map_token_program_flag(flag: u8) -> Option<Pubkey> {
    match flag {
        0 => Some(*SPL_TOKEN_PROGRAM_PUBKEY),
        1 => Some(*SPL_TOKEN_2022_PROGRAM_PUBKEY),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn parses_lb_pair_account_static_fields() {
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::new_unique();
        let reserve_x = Pubkey::new_unique();
        let reserve_y = Pubkey::new_unique();
        let oracle = Pubkey::new_unique();
        let active_bin_id = 42;
        let bin_step = 25;

        let mut data = vec![0u8; LB_PAIR_ACCOUNT_MIN_LEN];
        let mut cursor = STATIC_PARAMETERS_LEN + VARIABLE_PARAMETERS_LEN;
        cursor += 1 + 2 + 1; // bump, seed, pair_type
        write_i32(&mut data, &mut cursor, active_bin_id);
        write_u16(&mut data, &mut cursor, bin_step);
        cursor += 1 + 1 + 2 + 1 + 1; // status + require + base_factor_seed + activation + control
        write_pubkey(&mut data, &mut cursor, token_x);
        write_pubkey(&mut data, &mut cursor, token_y);
        write_pubkey(&mut data, &mut cursor, reserve_x);
        write_pubkey(&mut data, &mut cursor, reserve_y);
        cursor += PROTOCOL_FEE_LEN + 32 + REWARD_INFO_LEN * REWARD_INFO_COUNT;
        write_pubkey(&mut data, &mut cursor, oracle);
        cursor += BIN_ARRAY_BITMAP_BYTES + 8 + 32 + PUBKEY_BYTES * 2 + 8 + 8 + 8 + 8 + PUBKEY_BYTES;
        data[cursor] = 0;
        cursor += 1;
        data[cursor] = 1;

        let meta = parse_lb_pair_account(&data).expect("should parse hydrated meta");
        assert_eq!(meta.token_x_mint, token_x);
        assert_eq!(meta.token_y_mint, token_y);
        assert_eq!(meta.reserve_x, reserve_x);
        assert_eq!(meta.reserve_y, reserve_y);
        assert_eq!(meta.oracle, oracle);
        assert_eq!(meta.active_bin_id, active_bin_id);
        assert_eq!(meta.bin_step, bin_step);
        assert_eq!(meta.token_x_program, Some(*SPL_TOKEN_PROGRAM_PUBKEY));
        assert_eq!(meta.token_y_program, Some(*SPL_TOKEN_2022_PROGRAM_PUBKEY));
    }

    #[test]
    fn simulate_lb_clmm_exact_in_applies_slippage() {
        let pool = Pubkey::new_unique();
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::new_unique();
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: token_x,
            output_mint: token_y,
            amount: 150,
            slot: 0,
            direction: SwapDirection::ExactIn,
        };

        let mut bin_liquidity = BTreeMap::new();
        bin_liquidity.insert(
            0,
            LbClmmBinSnapshot {
                bin_id: 0,
                liquidity_x: 1_000_000,
                liquidity_y: 100,
            },
        );
        bin_liquidity.insert(
            1,
            LbClmmBinSnapshot {
                bin_id: 1,
                liquidity_x: 1_000_000,
                liquidity_y: 200,
            },
        );

        let state = LbClmmState {
            lb_pair: pool,
            token_x_mint: token_x,
            token_y_mint: token_y,
            token_x_program: None,
            token_y_program: None,
            reserve_x: None,
            reserve_y: None,
            oracle: None,
            bin_array_bitmap_extension: None,
            host_fee_account: None,
            bin_step: Some(100),
            active_bin_id: Some(0),
            dynamic_fee_bps: Some(0),
            protocol_fee_bps: None,
            reserve_x_balance: Some(1_000_000u128),
            reserve_y_balance: Some(1_000_000u128),
            volatility_bps: Some(0),
            bin_liquidity,
            active_bitmap: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };

        let config = LbClmmAdapterConfig::default();
        let outcome =
            simulate_lb_clmm_exact_in(&config, &state, &req).expect("quote should be produced");
        assert!(outcome.amount_out > 0 && outcome.amount_out < req.amount);
        assert!(outcome.price_impact_bps.is_finite());
    }

    #[test]
    fn bin_walk_handles_reverse_direction() {
        let pool = Pubkey::new_unique();
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::new_unique();
        let req = QuoteRequest {
            pool_id: pool,
            input_mint: token_y,
            output_mint: token_x,
            amount: 200,
            slot: 0,
            direction: SwapDirection::ExactIn,
        };

        let mut bin_liquidity = BTreeMap::new();
        bin_liquidity.insert(
            0,
            LbClmmBinSnapshot {
                bin_id: 0,
                liquidity_x: 0,
                liquidity_y: 500,
            },
        );
        bin_liquidity.insert(
            -1,
            LbClmmBinSnapshot {
                bin_id: -1,
                liquidity_x: 1_000_000,
                liquidity_y: 500,
            },
        );

        let state = LbClmmState {
            lb_pair: pool,
            token_x_mint: token_x,
            token_y_mint: token_y,
            token_x_program: None,
            token_y_program: None,
            reserve_x: None,
            reserve_y: None,
            oracle: None,
            bin_array_bitmap_extension: None,
            host_fee_account: None,
            bin_step: Some(100),
            active_bin_id: Some(0),
            dynamic_fee_bps: Some(0),
            protocol_fee_bps: None,
            reserve_x_balance: Some(1_000_000u128),
            reserve_y_balance: Some(1_000_000u128),
            volatility_bps: Some(0),
            bin_liquidity,
            active_bitmap: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };

        let mut config = LbClmmAdapterConfig::default();
        config.max_price_impact_bps = u16::MAX;
        let result = simulate_lb_clmm_exact_in(&config, &state, &req);
        let amount_out = result.expect("quote produced").amount_out;
        assert!(amount_out > 0, "should aggregate liquidity from lower bins");
    }

    #[test]
    fn taker_fee_combines_dynamic_and_volatility() {
        let state = LbClmmState {
            lb_pair: Pubkey::new_unique(),
            token_x_mint: Pubkey::new_unique(),
            token_y_mint: Pubkey::new_unique(),
            token_x_program: None,
            token_y_program: None,
            reserve_x: None,
            reserve_y: None,
            oracle: None,
            bin_array_bitmap_extension: None,
            host_fee_account: None,
            bin_step: None,
            active_bin_id: None,
            dynamic_fee_bps: Some(9_900),
            protocol_fee_bps: None,
            reserve_x_balance: Some(1u128),
            reserve_y_balance: Some(1u128),
            volatility_bps: Some(500),
            bin_liquidity: BTreeMap::new(),
            active_bitmap: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        };

        assert_eq!(state.taker_fee_bps(), BPS_DENOMINATOR);
    }

    #[test]
    fn parse_bin_array_account_filters_liquidity() {
        let pool = Pubkey::new_unique();
        let mut data = vec![0u8; BIN_ARRAY_ACCOUNT_LEN];
        let mut cursor = 0usize;
        write_i64(&mut data, &mut cursor, 1);
        data[cursor] = 1; // version
        cursor += 1 + 7; // version + padding
        write_pubkey(&mut data, &mut cursor, pool);

        for idx in 0..MAX_BIN_PER_ARRAY as usize {
            let amount_x = if idx % 10 == 0 { (idx as u64) + 1 } else { 0 };
            write_u64(&mut data, &mut cursor, amount_x);
            write_u64(&mut data, &mut cursor, 0);
            cursor += BIN_STRUCT_LEN - 16;
        }

        let extras =
            parse_bin_array_account(pool, &data).expect("should parse populated bin array");
        assert_eq!(extras.start_bin_id, MAX_BIN_PER_ARRAY);
        assert_eq!(extras.bins.len(), (MAX_BIN_PER_ARRAY as usize + 9) / 10);
        assert_eq!(extras.bins[0].bin_id, MAX_BIN_PER_ARRAY);
        assert_eq!(extras.bins[0].liquidity_x, 1);
    }

    #[test]
    fn parse_bitmap_extension_handles_payload() {
        let pool = Pubkey::new_unique();
        let mut data = vec![0u8; BITMAP_EXTENSION_ACCOUNT_LEN];
        let mut cursor = 0usize;
        write_pubkey(&mut data, &mut cursor, pool);
        for (idx, byte) in data[cursor..].iter_mut().enumerate() {
            *byte = (idx % 255) as u8;
        }

        let extras =
            parse_bitmap_extension_account(pool, &data).expect("should parse bitmap extension");
        assert_eq!(extras.bitmap.len(), BITMAP_EXTENSION_ACCOUNT_LEN - 32);
        assert_eq!(extras.bitmap[0], 0);
        assert_eq!(extras.bitmap[1], 1);
    }

    #[test]
    fn bitmap_alignment_tracks_active_array() {
        let word_idx = (BIN_ARRAY_BITMAP_SIZE_I32 / 2 / 64) as usize;
        let byte_offset = word_idx * 8;
        let mut bytes = vec![0u8; BIN_ARRAY_BITMAP_BYTES];
        bytes[byte_offset] = 1; // mark anchor array occupied

        let mut bitmap = LbClmmBinBitmap::from(LbClmmBitmapExtras {
            anchor_bin_id: 0,
            bitmap: bytes,
        });
        bitmap.anchor_with_bin(0);

        let mut state = default_state();
        state.active_bitmap = Some(bitmap);
        assert_eq!(state.bin_array_has_liquidity(0), Some(true));
    }

    #[test]
    fn lb_clmm_alt_accounts_are_deduplicated() {
        let mut state = default_state();
        state.reserve_x = Some(state.token_x_mint);
        state.reserve_y = Some(state.token_y_mint);
        state.oracle = Some(state.token_x_mint);
        let accounts = build_lb_clmm_alt_accounts(
            &state,
            state.token_x_program.unwrap(),
            state.token_y_program.unwrap(),
        );
        let mut seen = HashSet::new();
        for pk in accounts {
            assert!(seen.insert(pk), "duplicate {pk}");
        }
    }

    #[test]
    fn decorate_swap_context_sets_token_programs_without_builder() {
        let adapter = LbClmmAdapter::new(LbClmmAdapterConfig::default(), None);
        let state = default_state();
        let mut context = TemplateContext::default();
        adapter.decorate_swap_context(&state, &mut context, state.token_x_mint, state.token_y_mint);
        assert_eq!(context.input_token_program, state.token_x_program);
        assert_eq!(context.output_token_program, state.token_y_program);
        assert!(context.input_token_account.is_none());
        assert!(context.output_token_account.is_none());
    }

    fn default_state() -> LbClmmState {
        LbClmmState {
            lb_pair: Pubkey::new_unique(),
            token_x_mint: Pubkey::new_unique(),
            token_y_mint: Pubkey::new_unique(),
            token_x_program: Some(*SPL_TOKEN_PROGRAM_PUBKEY),
            token_y_program: Some(*SPL_TOKEN_2022_PROGRAM_PUBKEY),
            reserve_x: None,
            reserve_y: None,
            oracle: None,
            bin_array_bitmap_extension: None,
            host_fee_account: None,
            bin_step: Some(10),
            active_bin_id: Some(0),
            dynamic_fee_bps: None,
            protocol_fee_bps: None,
            reserve_x_balance: Some(1_000),
            reserve_y_balance: Some(1_000),
            volatility_bps: Some(0),
            bin_liquidity: BTreeMap::new(),
            active_bitmap: None,
            last_update_slot: 0,
            last_update_at: Instant::now(),
        }
    }

    fn write_pubkey(data: &mut [u8], cursor: &mut usize, key: Pubkey) {
        let end = *cursor + PUBKEY_BYTES;
        data[*cursor..end].copy_from_slice(key.as_ref());
        *cursor = end;
    }

    fn write_u16(data: &mut [u8], cursor: &mut usize, value: u16) {
        let end = *cursor + 2;
        data[*cursor..end].copy_from_slice(&value.to_le_bytes());
        *cursor = end;
    }

    fn write_i32(data: &mut [u8], cursor: &mut usize, value: i32) {
        let end = *cursor + 4;
        data[*cursor..end].copy_from_slice(&value.to_le_bytes());
        *cursor = end;
    }

    fn write_i64(data: &mut [u8], cursor: &mut usize, value: i64) {
        let end = *cursor + 8;
        data[*cursor..end].copy_from_slice(&value.to_le_bytes());
        *cursor = end;
    }

    fn write_u64(data: &mut [u8], cursor: &mut usize, value: u64) {
        let end = *cursor + 8;
        data[*cursor..end].copy_from_slice(&value.to_le_bytes());
        *cursor = end;
    }
}
