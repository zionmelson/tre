use std::{
    collections::BTreeMap,
    convert::TryFrom,
    ops::Bound::{Included, Unbounded},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, UiAccountEncoding,
};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_client::rpc_response::{UiAccount, UiAccountData};
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, trace, warn};

use crate::stages::types::{
    compute_edge_bps, LbClmmBinArrayExtras, LbClmmBinLiquidityExtras, LbClmmBitmapExtras,
    LbClmmPoolExtras, PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate,
};

use super::{build_quote_response, ProtocolAdapter, QuoteRequest, QuoteResponse, SwapDirection};

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
const BITMAP_EXTENSION_ACCOUNT_LEN: usize = 32 + (2 * 12 * 8 * 8);
const SNAPSHOT_PROBE_DIVISOR: u128 = 5_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;

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
}

impl Default for LbClmmAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 1_000,
        }
    }
}

pub struct LbClmmAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, LbClmmState>,
    config: LbClmmAdapterConfig,
    hydrator: Option<LbClmmPoolHydrator>,
}

impl LbClmmAdapter {
    pub fn new(config: LbClmmAdapterConfig) -> Self {
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
            active_bitmap: extras.active_bitmap.map(LbClmmBinBitmap::from),
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

        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: LbClmmPoolExtras) -> LbClmmPoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
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

        extras
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

        let (expected_output, slippage_bps) = simulate_lb_clmm_exact_in(state, &req)?;
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
            fee_bps: state.taker_fee_bps(),
            effective_price,
            estimated_slippage_bps: slippage_bps.abs(),
        })
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

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let (amount_out, edge_bps) = simulate_lb_clmm_exact_in(&state, &req)?;
        let mut response = build_quote_response(req, amount_out);
        response.edge_bps = edge_bps;
        Some(response)
    }

    fn bidirectional_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
        let state = self.get_state(&pool_pubkey)?;

        let base_reserve = state.reserve_x_balance.or(Some(snapshot.base_post_amount));
        let quote_reserve = state.reserve_y_balance.or(Some(snapshot.quote_post_amount));

        let base_probe = base_reserve.and_then(snapshot_probe_amount)?;
        let quote_probe = quote_reserve.and_then(snapshot_probe_amount)?;

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
        || extras.bin_arrays.is_empty()
        || extras.active_bitmap.is_none()
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
}

#[derive(Debug, Clone)]
pub struct LbClmmBinSnapshot {
    pub bin_id: i32,
    pub liquidity_x: u128,
    pub liquidity_y: u128,
}

#[derive(Debug, Clone)]
pub struct LbClmmBinBitmap {
    pub anchor_bin_id: i32,
    pub bitmap: Vec<u8>,
}

impl From<LbClmmBitmapExtras> for LbClmmBinBitmap {
    fn from(value: LbClmmBitmapExtras) -> Self {
        Self {
            anchor_bin_id: value.anchor_bin_id,
            bitmap: value.bitmap,
        }
    }
}

fn simulate_lb_clmm_exact_in(state: &LbClmmState, req: &QuoteRequest) -> Option<(u64, f64)> {
    if req.amount == 0 {
        return None;
    }

    let swap_x_to_y = req.input_mint == state.token_x_mint;
    let fee_bps = state.taker_fee_bps();
    let effective_in = apply_fee(req.amount as u128, fee_bps)?;
    if effective_in == 0 {
        return None;
    }

    if state.bin_liquidity.is_empty() {
        return simulate_xyk_fallback(state, req);
    }

    let (bin_step, active_bin) = match (state.bin_step, state.active_bin_id) {
        (Some(step), Some(active)) => (step, active),
        _ => return simulate_xyk_fallback(state, req),
    };

    let (amount_out_u128, spent_effective) = walk_bins(
        &state.bin_liquidity,
        bin_step,
        active_bin,
        effective_in,
        swap_x_to_y,
    )?;

    if amount_out_u128 == 0 {
        return simulate_xyk_fallback(state, req);
    }

    let gross_in = restore_from_fee(spent_effective, fee_bps);
    let amount_out = amount_out_u128.min(u128::from(u64::MAX)) as u64;
    let edge_bps = compute_bin_walk_edge(state, gross_in, amount_out, swap_x_to_y);
    Some((amount_out, edge_bps))
}

fn simulate_xyk_fallback(state: &LbClmmState, req: &QuoteRequest) -> Option<(u64, f64)> {
    let (reserve_in, reserve_out) = state.resolve_reserves(&req.input_mint, &req.output_mint)?;
    let amount_out = xyk_amount_out(reserve_in, reserve_out, req.amount, state.taker_fee_bps())?;
    let edge_bps =
        compute_slippage_bps(reserve_in, reserve_out, req.amount, amount_out).unwrap_or(0.0);
    Some((amount_out, edge_bps))
}

fn walk_bins(
    bins: &BTreeMap<i32, LbClmmBinSnapshot>,
    bin_step: u16,
    active_bin: i32,
    mut remaining_in: u128,
    swap_x_to_y: bool,
) -> Option<(u128, u128)> {
    if remaining_in == 0 {
        return None;
    }

    let mut produced_out = 0u128;
    let mut spent_effective = 0u128;

    if swap_x_to_y {
        for (&bin_id, bin) in bins.range((Included(active_bin), Unbounded)) {
            if remaining_in == 0 {
                break;
            }
            if bin.liquidity_y == 0 {
                continue;
            }
            let Some(price) = bin_price_x_per_y(bin_id, bin_step) else {
                continue;
            };
            let max_out_from_input = div_amount_by_price(remaining_in, price);
            if max_out_from_input == 0 {
                break;
            }
            let out = bin.liquidity_y.min(max_out_from_input);
            let spent = mul_amount_by_price(out, price);
            if spent == 0 {
                break;
            }
            produced_out = produced_out.saturating_add(out);
            spent_effective = spent_effective.saturating_add(spent);
            remaining_in = remaining_in.saturating_sub(spent);
        }
    } else {
        for (&bin_id, bin) in bins.range((Unbounded, Included(active_bin))).rev() {
            if remaining_in == 0 {
                break;
            }
            if bin.liquidity_x == 0 {
                continue;
            }
            let Some(price) = bin_price_x_per_y(bin_id, bin_step) else {
                continue;
            };
            let max_out_from_input = div_amount_by_price(remaining_in, price);
            if max_out_from_input == 0 {
                break;
            }
            let out = bin.liquidity_x.min(max_out_from_input);
            let spent = mul_amount_by_price(out, price);
            if spent == 0 {
                break;
            }
            produced_out = produced_out.saturating_add(out);
            spent_effective = spent_effective.saturating_add(spent);
            remaining_in = remaining_in.saturating_sub(spent);
        }
    }

    if produced_out == 0 {
        None
    } else {
        Some((produced_out, spent_effective))
    }
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

fn bin_price_x_per_y(bin_id: i32, bin_step: u16) -> Option<f64> {
    let step = 1.0 + (bin_step as f64) / BPS_DENOMINATOR as f64;
    if !step.is_finite() || step <= 0.0 {
        return None;
    }
    let price = step.powi(bin_id);
    if price.is_finite() && price > 0.0 {
        Some(price)
    } else {
        None
    }
}

fn div_amount_by_price(amount: u128, price: f64) -> u128 {
    if amount == 0 || price <= 0.0 || !price.is_finite() {
        return 0;
    }
    let value = (amount as f64) / price;
    f64_to_u128(value.floor())
}

fn mul_amount_by_price(amount: u128, price: f64) -> u128 {
    if amount == 0 || price <= 0.0 || !price.is_finite() {
        return 0;
    }
    let value = (amount as f64) * price;
    f64_to_u128(value.ceil())
}

fn f64_to_u128(value: f64) -> u128 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    if value >= u128::MAX as f64 {
        return u128::MAX;
    }
    value as u128
}

fn compute_bin_walk_edge(
    state: &LbClmmState,
    gross_in: u128,
    amount_out: u64,
    swap_x_to_y: bool,
) -> f64 {
    if gross_in == 0 || amount_out == 0 {
        return 0.0;
    }
    let gross_in_f64 = gross_in as f64;
    let amount_out_f64 = amount_out as f64;
    if gross_in_f64 <= 0.0 || amount_out_f64 <= 0.0 {
        return 0.0;
    }
    let trade_price = if swap_x_to_y {
        amount_out_f64 / gross_in_f64
    } else {
        gross_in_f64 / amount_out_f64
    };
    let pool_price = state.mid_price().unwrap_or(trade_price);
    compute_edge_bps(trade_price, pool_price)
}

fn xyk_amount_out(
    reserve_in: u128,
    reserve_out: u128,
    amount_in: u64,
    fee_bps: u64,
) -> Option<u64> {
    if reserve_in == 0 || reserve_out == 0 {
        return None;
    }

    let fee_bps = fee_bps.min(BPS_DENOMINATOR);
    let amount_less_fee = (amount_in as u128)
        .saturating_mul((BPS_DENOMINATOR - fee_bps) as u128)
        .checked_div(BPS_DENOMINATOR as u128)?;
    if amount_less_fee == 0 {
        return None;
    }

    let numerator = amount_less_fee.saturating_mul(reserve_out);
    let denominator = reserve_in.saturating_add(amount_less_fee);
    if denominator == 0 {
        return None;
    }

    Some((numerator / denominator).min(u64::MAX as u128) as u64)
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

fn snapshot_probe_amount(reserve: u128) -> Option<u64> {
    if reserve == 0 {
        return None;
    }

    let mut probe = reserve / SNAPSHOT_PROBE_DIVISOR;
    if probe == 0 {
        probe = SNAPSHOT_MIN_PROBE_INPUT.min(reserve).max(1u128);
    }

    probe = probe.min(reserve);
    probe = probe.max(1u128).min(SNAPSHOT_MAX_PROBE_INPUT);
    Some(probe as u64)
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

struct LbClmmPoolHydrator {
    program_id: Pubkey,
    clients: Vec<Arc<RpcClient>>,
    cache: DashMap<Pubkey, HydratedPoolMeta>,
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

        Some(Self {
            program_id,
            clients,
            cache: DashMap::new(),
        })
    }

    fn client_count(&self) -> usize {
        self.clients.len()
    }

    fn hydrate(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        if let Some(entry) = self.cache.get(&pool) {
            if entry.bin_arrays.is_empty() {
                drop(entry);
                if let Some(fresh) = self.fetch_from_rpc(pool) {
                    return Some(fresh);
                }
                return self.cache.get(&pool).map(|value| value.clone());
            }
            return Some(entry.clone());
        }
        self.fetch_from_rpc(pool)
    }

    fn fetch_from_rpc(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        for client in &self.clients {
            match client.get_account(&pool) {
                Ok(account) => {
                    if let Some(mut meta) = parse_lb_pair_account(&account.data) {
                        let bin_arrays = self.fetch_bin_arrays(client, pool);
                        if !bin_arrays.is_empty() {
                            meta.bin_arrays = bin_arrays;
                        }
                        if let Some(extension) = self.fetch_bitmap_extension(client, pool) {
                            meta.active_bitmap.bitmap.extend(extension.bitmap);
                        }
                        self.cache.insert(pool, meta.clone());
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

    fn fetch_bin_arrays(&self, client: &RpcClient, pool: Pubkey) -> Vec<LbClmmBinArrayExtras> {
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

        match client.get_program_ui_accounts_with_config(&self.program_id, config) {
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

    fn fetch_bitmap_extension(
        &self,
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

        match client.get_program_ui_accounts_with_config(&self.program_id, config) {
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

        let (amount_out, edge_bps) =
            simulate_lb_clmm_exact_in(&state, &req).expect("quote should be produced");
        assert_eq!(amount_out, 149, "bin walk should span multiple bins");
        assert!(edge_bps.is_finite());
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
                liquidity_x: 50,
                liquidity_y: 500,
            },
        );
        bin_liquidity.insert(
            -1,
            LbClmmBinSnapshot {
                bin_id: -1,
                liquidity_x: 100,
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

        let (amount_out, _) = simulate_lb_clmm_exact_in(&state, &req).expect("quote produced");
        assert_eq!(
            amount_out, 150,
            "should aggregate liquidity from lower bins"
        );
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
