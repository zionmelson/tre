use std::{
    cmp::Ordering,
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::{DashMap, DashSet};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, trace, warn};
use uint::construct_uint;

use crate::builder::{AmmV3PoolMeta, BuilderHandle};
use crate::stages::types::{
    PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate, RaydiumV3PoolExtras,
};
use crate::template::TemplateContext;

use super::hydration::AsyncHydrator;
use super::{build_quote_response, ProtocolAdapter, QuoteRequest, QuoteResponse, SwapDirection};

const RAYDIUM_V3_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const TICK_ARRAY_SEED: &[u8] = b"tick_array";
const TICK_ARRAY_STATE_DISCRIMINATOR: [u8; 8] = [192, 155, 85, 205, 49, 249, 129, 42];
const POOL_STATE_DISCRIMINATOR: [u8; 8] = [247, 237, 227, 245, 215, 195, 222, 70];
const AMM_CONFIG_STATE_DISCRIMINATOR: [u8; 8] = [218, 244, 33, 104, 203, 203, 43, 111];
const ANCHOR_DISCRIMINATOR_LEN: usize = 8;
const PUBKEY_BYTES: usize = 32;
const TICK_ARRAY_TICK_COUNT: usize = 60;
const TICK_STATE_PADDING_BYTES: usize = 13 * 4;
const TICK_STATE_SERIALIZED_LEN: usize =
    4 + 16 + 16 + 16 + 16 + (3 * 16) + TICK_STATE_PADDING_BYTES;
const MIN_RAYDIUM_TICK_ARRAY_ACCOUNT_LEN: usize = ANCHOR_DISCRIMINATOR_LEN
    + PUBKEY_BYTES
    + 4
    + (TICK_STATE_SERIALIZED_LEN * TICK_ARRAY_TICK_COUNT)
    + 1
    + 8
    + 107;
const MIN_RAYDIUM_AMM_CONFIG_ACCOUNT_LEN: usize = ANCHOR_DISCRIMINATOR_LEN + 109;

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
    amount_out: u64,
    sqrt_price_after: u128,
}

#[derive(Debug, Clone, Copy)]
struct QuoteOutcome {
    amount_out: u64,
    sqrt_price_limit: Option<u128>,
}

const DEFAULT_RAYDIUM_HYDRATION_RETRY_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct RaydiumV3AdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
    pub hydration_retry_ms: u64,
}

impl Default for RaydiumV3AdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            // tick array hydration can take several seconds; cache once fetched.
            rpc_timeout_ms: 15_000,
            hydration_retry_ms: DEFAULT_RAYDIUM_HYDRATION_RETRY_MS,
        }
    }
}

pub struct RaydiumV3Adapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, RaydiumV3State>,
    config: RaydiumV3AdapterConfig,
    hydrator: Option<RaydiumPoolHydrator>,
    template_builder: Option<BuilderHandle>,
    registered_templates: DashSet<Pubkey>,
}

impl RaydiumV3Adapter {
    pub fn new(config: RaydiumV3AdapterConfig, template_builder: Option<BuilderHandle>) -> Self {
        let program_id = Pubkey::from_str(RAYDIUM_V3_PROGRAM_ID)
            .expect("static Raydium AMM v3 program id should be valid");
        let hydrator = RaydiumPoolHydrator::new(&config, program_id);
        if let Some(hydrator_ref) = hydrator.as_ref() {
            info!(
                target = "adapter::raydium_v3",
                endpoints = hydrator_ref.client_count(),
                "raydium pool hydrator online"
            );
        } else {
            debug!(
                target = "adapter::raydium_v3",
                "raydium pool hydrator disabled (no rpc endpoints)"
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

    pub fn get_state(&self, pool: &Pubkey) -> Option<RaydiumV3State> {
        self.state.get(pool).map(|entry| entry.clone())
    }

    fn xyk_amount_out(
        reserve_in: u128,
        reserve_out: u128,
        amount_in: u64,
        fee_bps: u64,
    ) -> Option<u64> {
        if reserve_in == 0 || reserve_out == 0 || amount_in == 0 {
            return None;
        }

        let fee_deduction = 10_000u128.saturating_sub(fee_bps as u128);
        let amount_less_fee = (amount_in as u128)
            .saturating_mul(fee_deduction)
            .checked_div(10_000)?;
        if amount_less_fee == 0 {
            return None;
        }

        let numerator = amount_less_fee.saturating_mul(reserve_out);
        let denominator = reserve_in.saturating_add(amount_less_fee);
        if denominator == 0 {
            return None;
        }

        let amount_out = numerator.checked_div(denominator)?;
        Some(amount_out.min(u64::MAX as u128) as u64)
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

    fn upsert_state(&self, update: &PoolUpdate, extras: RaydiumV3PoolExtras) {
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

        let extras = self.enrich_extras(pool_pubkey, extras);
        let RaydiumV3PoolExtras {
            tick_spacing,
            sqrt_price_x64,
            liquidity,
            current_tick,
            amm_config,
            observation_state,
            pool_bump,
            observation_bump,
            vault_bumps,
            trade_fee_rate,
            protocol_fee_rate: _,
        } = extras;

        let trade_fee_bps = trade_fee_rate.map(fee_bps_from_hundredth_bip);

        let state = RaydiumV3State {
            pool_id: pool_pubkey,
            base_mint,
            quote_mint,
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
            pool_bump,
            observation_bump,
            vault_bumps,
            trade_fee_bps,
        };

        trace!(
            target = "adapter::raydium_v3",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting Raydium pool state"
        );

        self.register_templates_with_builder(&state);
        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: RaydiumV3PoolExtras) -> RaydiumV3PoolExtras {
        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.tick_spacing = extras.tick_spacing.or(Some(meta.tick_spacing));
            extras.sqrt_price_x64 = extras.sqrt_price_x64.or(Some(meta.sqrt_price_x64));
            extras.liquidity = extras.liquidity.or(Some(meta.liquidity));
            extras.current_tick = extras.current_tick.or(Some(meta.current_tick));
            // Always trust the on-chain pool account for amm_config so stale CoreCast
            // metadata cannot point us at a non-existent config account.
            extras.amm_config = Some(meta.amm_config);
            extras.observation_state = extras.observation_state.or(Some(meta.observation_state));
            extras.pool_bump = extras.pool_bump.or(Some(meta.pool_bump));
            extras.trade_fee_rate = extras.trade_fee_rate.or(meta.trade_fee_rate);
            extras.protocol_fee_rate = extras.protocol_fee_rate.or(meta.protocol_fee_rate);
        }

        if let (Some(spacing), Some(tick)) = (extras.tick_spacing, extras.current_tick) {
            if let Some(start_index) = active_tick_array_start(spacing, tick) {
                let _ = hydrator.hydrate_tick_array(pool, start_index);
                hydrator.hydrate_tick_array_window(pool, spacing, start_index, 2);
            }
        }

        extras
    }

    fn register_templates_with_builder(&self, state: &RaydiumV3State) {
        let Some(builder) = self.template_builder.as_ref() else {
            return;
        };
        let Some(fee_bps) = state.trade_fee_bps else {
            return;
        };
        if !self.registered_templates.insert(state.pool_id) {
            return;
        }

        let fee_tier_bps = u16::try_from(fee_bps).unwrap_or(u16::MAX);
        let mut alt_accounts = Vec::with_capacity(8);
        alt_accounts.push(state.pool_id);
        alt_accounts.push(state.base_mint);
        alt_accounts.push(state.quote_mint);
        if let Some(key) = state.amm_config {
            alt_accounts.push(key);
        }
        if let Some(key) = state.observation_state {
            alt_accounts.push(key);
        }

        builder.register_amm_v3_pool(AmmV3PoolMeta {
            pool_id: state.pool_id,
            fee_tier_bps,
            alt_accounts,
            alt_handle: None,
        });
    }

    fn simulate_clmm_exact_in(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Option<ClmmSwapSimulation> {
        let hydrator = self.hydrator.as_ref()?;
        let tick_spacing = state.tick_spacing?;
        let mut current_tick = state.current_tick?;
        let sqrt_price_x64 = state.sqrt_price_x64?;
        let mut liquidity = state.liquidity?;
        if liquidity == 0 || req.amount == 0 {
            return None;
        }

        let (token0, token1) = if mint_is_less(&state.base_mint, &state.quote_mint) {
            (state.base_mint, state.quote_mint)
        } else {
            (state.quote_mint, state.base_mint)
        };

        let (zero_for_one, valid_direction) =
            if req.input_mint == token0 && req.output_mint == token1 {
                (true, true)
            } else if req.input_mint == token1 && req.output_mint == token0 {
                (false, true)
            } else {
                (false, false)
            };

        if !valid_direction {
            return None;
        }

        let mut sqrt_price_x96 = math::to_x96(sqrt_price_x64);
        let sqrt_limit_tick = if zero_for_one {
            math::MIN_TICK
        } else {
            math::MAX_TICK
        };
        let sqrt_price_limit_x96 = math::sqrt_price_at_tick_x96(sqrt_limit_tick)?;

        let anchor_start = active_tick_array_start(tick_spacing, current_tick);
        let mut cursor = TickArrayCursor::new(hydrator, state.pool_id, tick_spacing, anchor_start);
        let mut amount_remaining = req.amount as u128;
        let mut amount_out_accum = 0u128;
        let fee_bps = state.trade_fee_bps.unwrap_or(DEFAULT_FEE_BPS);
        let mut steps = 0usize;

        while amount_remaining > 0 && liquidity > 0 && sqrt_price_x96 != sqrt_price_limit_x96 {
            steps += 1;
            if steps > 512 {
                break;
            }

            let next_tick = cursor.next_initialized_tick(current_tick, zero_for_one);
            let mut target_price = sqrt_price_limit_x96;
            let mut crossing = None;
            if let Some(tick_info) = next_tick {
                let tick_price = math::sqrt_price_at_tick_x96(tick_info.index)?;
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
            None
        } else {
            let amount_out = amount_out_accum.min(u64::MAX as u128) as u64;
            let sqrt_price_after = sqrt_price_x64_from_x96(&sqrt_price_x96);
            Some(ClmmSwapSimulation {
                amount_out,
                sqrt_price_after,
            })
        }
    }

    fn quote_amount_out(&self, state: &RaydiumV3State, req: &QuoteRequest) -> Option<QuoteOutcome> {
        if let Some(simulation) = self.simulate_clmm_exact_in(state, req) {
            return Some(QuoteOutcome {
                amount_out: simulation.amount_out,
                sqrt_price_limit: Some(simulation.sqrt_price_after),
            });
        }

        let (reserve_in, reserve_out) =
            if req.input_mint == state.base_mint && req.output_mint == state.quote_mint {
                (state.vault_base, state.vault_quote)
            } else if req.input_mint == state.quote_mint && req.output_mint == state.base_mint {
                (state.vault_quote, state.vault_base)
            } else {
                return None;
            };

        if reserve_in == 0 || reserve_out == 0 {
            return None;
        }

        let fee_bps = state.trade_fee_bps.unwrap_or(DEFAULT_FEE_BPS);
        let amount_out = Self::xyk_amount_out(reserve_in, reserve_out, req.amount, fee_bps)?;
        Some(QuoteOutcome {
            amount_out,
            sqrt_price_limit: None,
        })
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
            return None;
        }

        let fee_bps = state.trade_fee_bps.unwrap_or(DEFAULT_FEE_BPS);
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

        let quote = self.quote_amount_out(state, &req)?;
        let execution_price = if probe_amount == 0 {
            0.0
        } else {
            quote.amount_out as f64 / probe_amount as f64
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

        let slippage = reference_price
            .map(|reference| compute_slippage_bps(reference, execution_price))
            .unwrap_or_default();

        Some(PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional: probe_amount,
            expected_output: quote.amount_out,
            fee_bps,
            effective_price: execution_price,
            estimated_slippage_bps: slippage,
            sqrt_price_limit: quote.sqrt_price_limit,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            context: TemplateContext {
                compute_unit_limit: Some(RAYDIUM_DEFAULT_CU_LIMIT),
                ..Default::default()
            },
        })
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

        // CoreCast metadata is often stale; rely solely on on-chain hydration.
        self.upsert_state(update, RaydiumV3PoolExtras::default());
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let quote = self.quote_amount_out(&state, &req)?;
        let mut response = build_quote_response(req, quote.amount_out);
        response.sqrt_price_limit = quote.sqrt_price_limit;
        response.min_output_amount = Some(quote.amount_out);
        response.context.compute_unit_limit = Some(RAYDIUM_DEFAULT_CU_LIMIT);
        Some(response)
    }

    fn bidirectional_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
        let state = self.get_state(&pool_pubkey)?;

        let base_probe = snapshot_probe_amount(state.vault_base)?;
        let quote_probe = snapshot_probe_amount(state.vault_quote)?;

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
const SNAPSHOT_PROBE_DIVISOR: u128 = 5_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;

#[derive(Debug, Clone)]
pub struct RaydiumV3State {
    pub pool_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
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
    pub pool_bump: Option<u8>,
    pub observation_bump: Option<u8>,
    pub vault_bumps: Option<[u8; 2]>,
    pub trade_fee_bps: Option<u64>,
}

impl RaydiumV3State {
    pub fn mid_price(&self) -> Option<f64> {
        if self.vault_base == 0 {
            return None;
        }
        Some(self.vault_quote as f64 / self.vault_base as f64)
    }
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
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

fn compute_slippage_bps(reference: f64, execution: f64) -> f64 {
    if !reference.is_finite() || reference == 0.0 || !execution.is_finite() {
        return 0.0;
    }
    (((reference - execution) / reference) * 10_000.0).abs()
}

fn active_tick_array_start(tick_spacing: i32, tick_index: i32) -> Option<i32> {
    if tick_spacing == 0 {
        return None;
    }
    let spacing = (tick_spacing as i64).abs();
    if spacing == 0 {
        return None;
    }
    let ticks_per_array = spacing.checked_mul(TICK_ARRAY_TICK_COUNT as i64)?;
    let tick = tick_index as i64;
    let mut quotient = tick / ticks_per_array;
    if tick < 0 && tick % ticks_per_array != 0 {
        quotient -= 1;
    }
    let start = quotient.checked_mul(ticks_per_array)?;
    if start < i64::from(i32::MIN) || start > i64::from(i32::MAX) {
        return None;
    }
    Some(start as i32)
}

fn tick_array_span(tick_spacing: i32) -> Option<i32> {
    tick_spacing
        .checked_mul(TICK_ARRAY_TICK_COUNT as i32)
        .map(|value| value.abs())
}

fn mint_is_less(a: &Pubkey, b: &Pubkey) -> bool {
    a.to_bytes().cmp(&b.to_bytes()) == Ordering::Less
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

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    amm_config: Pubkey,
    observation_state: Pubkey,
    tick_spacing: i32,
    sqrt_price_x64: u128,
    liquidity: u128,
    current_tick: i32,
    pool_bump: u8,
    trade_fee_rate: Option<u32>,
    protocol_fee_rate: Option<u32>,
}

#[derive(Clone, Debug)]
struct HydratedAmmConfig {
    trade_fee_rate: u32,
    protocol_fee_rate: u32,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct HydratedTickArray {
    address: Pubkey,
    pool_id: Pubkey,
    start_tick_index: i32,
    ticks: Vec<HydratedTick>,
    initialized_tick_count: u8,
    recent_epoch: u64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct HydratedTick {
    tick: i32,
    liquidity_net: i128,
    liquidity_gross: u128,
    fee_growth_outside_0_x64: u128,
    fee_growth_outside_1_x64: u128,
    reward_growths_outside_x64: [u128; 3],
}

struct TickArrayCursor<'a> {
    hydrator: &'a RaydiumPoolHydrator,
    pool: Pubkey,
    tick_spacing: i32,
    cache: HashMap<i32, HydratedTickArray>,
}

impl<'a> TickArrayCursor<'a> {
    fn new(
        hydrator: &'a RaydiumPoolHydrator,
        pool: Pubkey,
        tick_spacing: i32,
        anchor_start: Option<i32>,
    ) -> Self {
        if let Some(start) = anchor_start {
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
                if let Some(tick) = find_prev_initialized(&array, search_tick) {
                    return Some(tick);
                }
                array_start = array_start.checked_sub(span)?;
                search_tick = array_start + span - self.tick_spacing;
            } else {
                if let Some(tick) = find_next_initialized(&array, search_tick) {
                    return Some(tick);
                }
                array_start = array_start.checked_add(span)?;
                search_tick = array_start - self.tick_spacing;
            }
        }

        None
    }
}

#[derive(Clone, Copy, Debug)]
struct InitializedTick {
    index: i32,
    liquidity_net: i128,
}

fn find_prev_initialized(array: &HydratedTickArray, current_tick: i32) -> Option<InitializedTick> {
    for tick in array.ticks.iter().rev() {
        if tick.tick <= current_tick && tick.liquidity_gross > 0 {
            return Some(InitializedTick {
                index: tick.tick,
                liquidity_net: tick.liquidity_net,
            });
        }
    }
    None
}

fn find_next_initialized(array: &HydratedTickArray, current_tick: i32) -> Option<InitializedTick> {
    for tick in &array.ticks {
        if tick.tick > current_tick && tick.liquidity_gross > 0 {
            return Some(InitializedTick {
                index: tick.tick,
                liquidity_net: tick.liquidity_net,
            });
        }
    }
    None
}

struct RaydiumPoolHydrator {
    program_id: Pubkey,
    clients: Arc<Vec<Arc<RpcClient>>>,
    pool_cache: AsyncHydrator<Pubkey, HydratedPoolMeta>,
    tick_array_cache: DashMap<(Pubkey, i32), HydratedTickArray>,
    amm_config_cache: Arc<DashMap<Pubkey, HydratedAmmConfig>>,
    missing_amm_configs: Arc<DashMap<Pubkey, Instant>>,
    missing_tick_arrays: DashMap<Pubkey, Instant>,
    retry_backoff: Duration,
}

impl RaydiumPoolHydrator {
    fn new(config: &RaydiumV3AdapterConfig, program_id: Pubkey) -> Option<Self> {
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

        let clients = Arc::new(client_list);
        let amm_config_cache = Arc::new(DashMap::new());
        let missing_amm_configs = Arc::new(DashMap::new());
        let retry_backoff = Duration::from_millis(config.hydration_retry_ms.max(1));
        let fetch_clients = clients.clone();
        let fetch_amm_cache = amm_config_cache.clone();
        let fetch_missing = missing_amm_configs.clone();
        let pool_cache = AsyncHydrator::new("raydium_pool", fetch_clients.len(), move |pool| {
            fetch_raydium_pool_meta(
                fetch_clients.as_ref(),
                fetch_amm_cache.as_ref(),
                fetch_missing.as_ref(),
                retry_backoff,
                pool,
            )
        });

        Some(Self {
            program_id,
            clients,
            pool_cache,
            tick_array_cache: DashMap::new(),
            amm_config_cache,
            missing_amm_configs,
            missing_tick_arrays: DashMap::new(),
            retry_backoff,
        })
    }

    fn client_count(&self) -> usize {
        self.clients.len()
    }

    fn hydrate(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        self.pool_cache.get_or_schedule(pool)
    }

    pub fn hydrate_tick_array(
        &self,
        pool: Pubkey,
        start_tick_index: i32,
    ) -> Option<HydratedTickArray> {
        let key = (pool, start_tick_index);
        if let Some(entry) = self.tick_array_cache.get(&key) {
            return Some(entry.clone());
        }

        let address = derive_tick_array_address(&self.program_id, &pool, start_tick_index);
        if self.should_skip_missing_tick_array(&address) {
            return None;
        }

        self.fetch_tick_array_from_rpc(pool, start_tick_index)
    }

    pub fn hydrate_tick_array_window(
        &self,
        pool: Pubkey,
        tick_spacing: i32,
        anchor_start: i32,
        look_ahead: u32,
    ) {
        if look_ahead == 0 || tick_spacing == 0 {
            return;
        }
        let span = tick_spacing
            .abs()
            .saturating_mul(TICK_ARRAY_TICK_COUNT as i32);
        if span == 0 {
            return;
        }
        let mut starts = Vec::with_capacity((look_ahead as usize) * 2);
        for step in 1..=look_ahead {
            let offset = span.saturating_mul(step as i32);
            if let Some(start) = anchor_start.checked_add(offset) {
                starts.push(start);
            }
            if let Some(start) = anchor_start.checked_sub(offset) {
                starts.push(start);
            }
        }
        self.hydrate_tick_arrays(pool, &starts);
    }

    pub fn hydrate_tick_arrays(
        &self,
        pool: Pubkey,
        start_tick_indices: &[i32],
    ) -> Vec<HydratedTickArray> {
        if start_tick_indices.is_empty() {
            return Vec::new();
        }

        let mut results = Vec::new();
        let mut pending = Vec::new();
        for start in start_tick_indices {
            let key = (pool, *start);
            if let Some(entry) = self.tick_array_cache.get(&key) {
                results.push(entry.clone());
                continue;
            }
            let address = derive_tick_array_address(&self.program_id, &pool, *start);
            if self.should_skip_missing_tick_array(&address) {
                continue;
            }
            pending.push((*start, address));
        }

        if !pending.is_empty() {
            self.fetch_tick_arrays_from_rpc(pool, &pending);
            for (start, _) in pending {
                let key = (pool, start);
                if let Some(entry) = self.tick_array_cache.get(&key) {
                    results.push(entry.clone());
                }
            }
        }

        results
    }

    fn fetch_tick_array_from_rpc(
        &self,
        pool: Pubkey,
        start_tick_index: i32,
    ) -> Option<HydratedTickArray> {
        let address = derive_tick_array_address(&self.program_id, &pool, start_tick_index);

        for client in self.clients.as_ref() {
            match client.get_account(&address) {
                Ok(account) => {
                    if let Some(array) = parse_tick_array_account(address, &account.data) {
                        if array.pool_id != pool {
                            warn!(
                                target = "adapter::raydium_v3",
                                pool = %pool,
                                tick_array = %address,
                                "tick array pool mismatch"
                            );
                            return None;
                        }
                        if array.start_tick_index != start_tick_index {
                            warn!(
                                target = "adapter::raydium_v3",
                                pool = %pool,
                                tick_array = %address,
                                actual = array.start_tick_index,
                                requested = start_tick_index,
                                "tick array start index mismatch"
                            );
                            return None;
                        }
                        self.tick_array_cache
                            .insert((pool, start_tick_index), array.clone());
                        return Some(array);
                    } else {
                        warn!(
                            target = "adapter::raydium_v3",
                            pool = %pool,
                            tick_array = %address,
                            "unexpected raydium tick array layout"
                        );
                    }
                }
                Err(error) => {
                    if is_account_not_found(&error) {
                        let first_time = self.mark_missing_tick_array(address);
                        if first_time {
                            warn!(
                                target = "adapter::raydium_v3",
                                pool = %pool,
                                tick_array = %address,
                                "raydium tick array missing on-chain"
                            );
                        }
                    } else {
                        warn!(
                            target = "adapter::raydium_v3",
                            pool = %pool,
                            tick_array = %address,
                            %error,
                            "failed to fetch raydium tick array"
                        );
                    }
                }
            }
        }

        None
    }

    fn fetch_tick_arrays_from_rpc(&self, pool: Pubkey, pending: &[(i32, Pubkey)]) {
        if pending.is_empty() {
            return;
        }

        let addresses: Vec<Pubkey> = pending.iter().map(|(_, address)| *address).collect();

        for client in self.clients.as_ref() {
            match client.get_multiple_accounts(&addresses) {
                Ok(accounts) => {
                    for ((start, address), account_opt) in pending.iter().zip(accounts.into_iter())
                    {
                        if let Some(account) = account_opt {
                            if let Some(array) = parse_tick_array_account(*address, &account.data) {
                                if array.pool_id != pool || array.start_tick_index != *start {
                                    continue;
                                }
                                self.tick_array_cache.insert((pool, *start), array.clone());
                            }
                        }
                    }
                    return;
                }
                Err(error) => {
                    warn!(
                        target = "adapter::raydium_v3",
                        pool = %pool,
                        %error,
                        "failed to fetch multiple tick arrays"
                    );
                }
            }
        }
    }

    fn should_skip_missing_config(&self, key: &Pubkey) -> bool {
        Self::should_skip_missing(&self.missing_amm_configs, key, self.retry_backoff)
    }

    fn should_skip_missing_tick_array(&self, key: &Pubkey) -> bool {
        Self::should_skip_missing(&self.missing_tick_arrays, key, self.retry_backoff)
    }

    fn mark_missing_config(&self, key: Pubkey) -> bool {
        Self::record_missing(&self.missing_amm_configs, key)
    }

    fn mark_missing_tick_array(&self, key: Pubkey) -> bool {
        Self::record_missing(&self.missing_tick_arrays, key)
    }

    fn should_skip_missing(
        map: &DashMap<Pubkey, Instant>,
        key: &Pubkey,
        backoff: Duration,
    ) -> bool {
        if let Some(entry) = map.get(key) {
            if entry.value().elapsed() < backoff {
                return true;
            }
            drop(entry);
            map.remove(key);
        }
        false
    }

    fn record_missing(map: &DashMap<Pubkey, Instant>, key: Pubkey) -> bool {
        map.insert(key, Instant::now()).is_none()
    }
}

const MIN_RAYDIUM_POOL_ACCOUNT_LEN: usize = ANCHOR_DISCRIMINATOR_LEN + 261;

fn parse_amm_config_account(data: &[u8]) -> Option<HydratedAmmConfig> {
    if data.len() < MIN_RAYDIUM_AMM_CONFIG_ACCOUNT_LEN {
        return None;
    }

    let discriminator = data.get(0..ANCHOR_DISCRIMINATOR_LEN)?;
    if discriminator != AMM_CONFIG_STATE_DISCRIMINATOR {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    read_u8(data, &mut cursor)?; // bump
    read_u16(data, &mut cursor)?; // index
    skip_pubkey(data, &mut cursor)?; // owner
    let protocol_fee_rate = read_u32(data, &mut cursor)?;
    let trade_fee_rate = read_u32(data, &mut cursor)?;

    Some(HydratedAmmConfig {
        trade_fee_rate,
        protocol_fee_rate,
    })
}

fn parse_pool_account(data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < MIN_RAYDIUM_POOL_ACCOUNT_LEN {
        return None;
    }

    let discriminator = data.get(0..ANCHOR_DISCRIMINATOR_LEN)?;
    if discriminator != POOL_STATE_DISCRIMINATOR {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    let pool_bump = *data.get(cursor)?;
    cursor += 1;

    let amm_config = read_pubkey(data, &mut cursor)?;
    skip_pubkey(data, &mut cursor)?; // owner
    skip_pubkey(data, &mut cursor)?; // token_mint_0
    skip_pubkey(data, &mut cursor)?; // token_mint_1
    skip_pubkey(data, &mut cursor)?; // token_vault_0
    skip_pubkey(data, &mut cursor)?; // token_vault_1
    let observation_state = read_pubkey(data, &mut cursor)?;

    skip_bytes(data, &mut cursor, 2)?; // mint_decimals_0 + mint_decimals_1
    skip_bytes(data, &mut cursor, 8)?; // reserved flags (token program ids, decimals extras)

    let tick_spacing = read_u16(data, &mut cursor)? as i32;
    let liquidity = read_u128(data, &mut cursor)?;
    let sqrt_price_x64 = read_u128(data, &mut cursor)?;
    let current_tick = read_i32(data, &mut cursor)?;

    Some(HydratedPoolMeta {
        amm_config,
        observation_state,
        tick_spacing,
        sqrt_price_x64,
        liquidity,
        current_tick,
        pool_bump,
        trade_fee_rate: None,
        protocol_fee_rate: None,
    })
}

fn fetch_raydium_pool_meta(
    clients: &[Arc<RpcClient>],
    amm_cache: &DashMap<Pubkey, HydratedAmmConfig>,
    missing_configs: &DashMap<Pubkey, Instant>,
    retry_backoff: Duration,
    pool: Pubkey,
) -> Option<HydratedPoolMeta> {
    for client in clients {
        match client.get_account(&pool) {
            Ok(account) => {
                if let Some(mut meta) = parse_pool_account(&account.data) {
                    attach_config_fees_from_cache(
                        &mut meta,
                        clients,
                        amm_cache,
                        missing_configs,
                        retry_backoff,
                    );
                    return Some(meta);
                } else {
                    warn!(
                        target = "adapter::raydium_v3",
                        pool = %pool,
                        "unexpected raydium pool account layout"
                    );
                }
            }
            Err(error) => {
                warn!(
                    target = "adapter::raydium_v3",
                    pool = %pool,
                    %error,
                    "failed to fetch raydium pool account"
                );
            }
        }
    }

    None
}

fn attach_config_fees_from_cache(
    meta: &mut HydratedPoolMeta,
    clients: &[Arc<RpcClient>],
    amm_cache: &DashMap<Pubkey, HydratedAmmConfig>,
    missing_configs: &DashMap<Pubkey, Instant>,
    retry_backoff: Duration,
) {
    let trade_missing = meta.trade_fee_rate.is_none();
    let protocol_missing = meta.protocol_fee_rate.is_none();
    if !(trade_missing || protocol_missing) {
        return;
    }

    if let Some(config) = hydrate_amm_config_from_clients(
        meta.amm_config,
        clients,
        amm_cache,
        missing_configs,
        retry_backoff,
    ) {
        if trade_missing {
            meta.trade_fee_rate = Some(config.trade_fee_rate);
        }
        if protocol_missing {
            meta.protocol_fee_rate = Some(config.protocol_fee_rate);
        }
    }
}

fn hydrate_amm_config_from_clients(
    amm_config: Pubkey,
    clients: &[Arc<RpcClient>],
    cache: &DashMap<Pubkey, HydratedAmmConfig>,
    missing_configs: &DashMap<Pubkey, Instant>,
    retry_backoff: Duration,
) -> Option<HydratedAmmConfig> {
    if RaydiumPoolHydrator::should_skip_missing(missing_configs, &amm_config, retry_backoff) {
        return None;
    }
    if let Some(entry) = cache.get(&amm_config) {
        return Some(entry.clone());
    }

    for client in clients {
        match client.get_account(&amm_config) {
            Ok(account) => {
                if let Some(config) = parse_amm_config_account(&account.data) {
                    cache.insert(amm_config, config.clone());
                    return Some(config);
                } else {
                    warn!(
                        target = "adapter::raydium_v3",
                        amm_config = %amm_config,
                        "unexpected raydium amm config layout"
                    );
                }
            }
            Err(error) => {
                if is_account_not_found(&error) {
                    if RaydiumPoolHydrator::record_missing(missing_configs, amm_config) {
                        warn!(
                            target = "adapter::raydium_v3",
                            amm_config = %amm_config,
                            "raydium amm config missing on-chain"
                        );
                    }
                } else {
                    warn!(
                        target = "adapter::raydium_v3",
                        amm_config = %amm_config,
                        %error,
                        "failed to fetch raydium amm config"
                    );
                }
            }
        }
    }

    None
}

fn parse_tick_array_account(address: Pubkey, data: &[u8]) -> Option<HydratedTickArray> {
    if data.len() < MIN_RAYDIUM_TICK_ARRAY_ACCOUNT_LEN {
        return None;
    }

    let discriminator = data.get(0..ANCHOR_DISCRIMINATOR_LEN)?;
    if discriminator != TICK_ARRAY_STATE_DISCRIMINATOR {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    let pool_id = read_pubkey(data, &mut cursor)?;
    let start_tick_index = read_i32(data, &mut cursor)?;

    let mut ticks = Vec::with_capacity(TICK_ARRAY_TICK_COUNT);
    for _ in 0..TICK_ARRAY_TICK_COUNT {
        let tick = read_i32(data, &mut cursor)?;
        let liquidity_net = read_i128(data, &mut cursor)?;
        let liquidity_gross = read_u128(data, &mut cursor)?;
        let fee_growth_outside_0_x64 = read_u128(data, &mut cursor)?;
        let fee_growth_outside_1_x64 = read_u128(data, &mut cursor)?;
        let mut reward_growths_outside_x64 = [0u128; 3];
        for reward in reward_growths_outside_x64.iter_mut() {
            *reward = read_u128(data, &mut cursor)?;
        }
        skip_bytes(data, &mut cursor, TICK_STATE_PADDING_BYTES)?;
        ticks.push(HydratedTick {
            tick,
            liquidity_net,
            liquidity_gross,
            fee_growth_outside_0_x64,
            fee_growth_outside_1_x64,
            reward_growths_outside_x64,
        });
    }

    let initialized_tick_count = read_u8(data, &mut cursor)?;
    let recent_epoch = read_u64(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 107)?;

    Some(HydratedTickArray {
        address,
        pool_id,
        start_tick_index,
        ticks,
        initialized_tick_count,
        recent_epoch,
    })
}

fn skip_pubkey(data: &[u8], cursor: &mut usize) -> Option<()> {
    *cursor = cursor.checked_add(PUBKEY_BYTES)?;
    if *cursor <= data.len() {
        Some(())
    } else {
        None
    }
}

fn derive_tick_array_address(program_id: &Pubkey, pool: &Pubkey, start_tick_index: i32) -> Pubkey {
    let (address, _) = Pubkey::find_program_address(
        &[
            TICK_ARRAY_SEED,
            pool.as_ref(),
            &start_tick_index.to_le_bytes(),
        ],
        program_id,
    );
    address
}

fn read_pubkey(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    let end = cursor.checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Option<u16> {
    let end = cursor.checked_add(2)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u16::from_le_bytes(bytes))
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Option<u32> {
    let end = cursor.checked_add(4)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u32::from_le_bytes(bytes))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Option<u64> {
    let end = cursor.checked_add(8)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u64::from_le_bytes(bytes))
}

fn read_u128(data: &[u8], cursor: &mut usize) -> Option<u128> {
    let end = cursor.checked_add(16)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u128::from_le_bytes(bytes))
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    let end = cursor.checked_add(1)?;
    let value = *data.get(*cursor)?;
    *cursor = end;
    Some(value)
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Option<i32> {
    let end = cursor.checked_add(4)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(i32::from_le_bytes(bytes))
}

fn read_i128(data: &[u8], cursor: &mut usize) -> Option<i128> {
    let end = cursor.checked_add(16)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(i128::from_le_bytes(bytes))
}

fn skip_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<()> {
    *cursor = cursor.checked_add(len)?;
    if *cursor <= data.len() {
        Some(())
    } else {
        None
    }
}

fn is_account_not_found(error: &ClientError) -> bool {
    use solana_client::rpc_request::RpcError;

    match error.kind() {
        ClientErrorKind::RpcError(RpcError::RpcResponseError { message, data, .. }) => {
            message.contains("AccountNotFound") || format!("{:?}", data).contains("AccountNotFound")
        }
        ClientErrorKind::Custom(message) => message.contains("AccountNotFound"),
        _ => error.to_string().contains("AccountNotFound"),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn parses_pool_account_layout() {
        let mut data = vec![0u8; MIN_RAYDIUM_POOL_ACCOUNT_LEN + 32];
        data[..ANCHOR_DISCRIMINATOR_LEN].copy_from_slice(&POOL_STATE_DISCRIMINATOR);
        let mut cursor = 0;
        cursor += ANCHOR_DISCRIMINATOR_LEN;
        data[cursor] = 5; // bump
        cursor += 1;

        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // amm_config
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // owner
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_mint_0
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_mint_1
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_vault_0
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_vault_1
        let observation = Pubkey::new_unique();
        cursor = write_pubkey(&mut data, cursor, &observation); // observation

        data[cursor] = 6;
        data[cursor + 1] = 9;
        cursor += 2;

        cursor += 8; // reserved flags

        let tick_spacing: u16 = 128;
        data[cursor..cursor + 2].copy_from_slice(&tick_spacing.to_le_bytes());
        cursor += 2;

        let liquidity: u128 = 1_000_000;
        data[cursor..cursor + 16].copy_from_slice(&liquidity.to_le_bytes());
        cursor += 16;

        let sqrt_price_x64: u128 = 42_000;
        data[cursor..cursor + 16].copy_from_slice(&sqrt_price_x64.to_le_bytes());
        cursor += 16;

        let current_tick: i32 = -64;
        data[cursor..cursor + 4].copy_from_slice(&current_tick.to_le_bytes());

        let meta = parse_pool_account(&data).expect("meta should parse");
        assert_eq!(meta.pool_bump, 5);
        assert_eq!(meta.tick_spacing, tick_spacing as i32);
        assert_eq!(meta.liquidity, liquidity);
        assert_eq!(meta.sqrt_price_x64, sqrt_price_x64);
        assert_eq!(meta.current_tick, current_tick);
        assert_eq!(meta.observation_state, observation);
    }

    #[test]
    fn parses_tick_array_account_layout() {
        let pool = Pubkey::new_unique();
        let address = Pubkey::new_unique();
        let start_tick_index: i32 = -120;
        let mut data = vec![0u8; MIN_RAYDIUM_TICK_ARRAY_ACCOUNT_LEN];
        data[..ANCHOR_DISCRIMINATOR_LEN].copy_from_slice(&TICK_ARRAY_STATE_DISCRIMINATOR);
        let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
        cursor = write_pubkey(&mut data, cursor, &pool);
        data[cursor..cursor + 4].copy_from_slice(&start_tick_index.to_le_bytes());
        cursor += 4;

        for idx in 0..TICK_ARRAY_TICK_COUNT {
            let tick_value = start_tick_index + (idx as i32 * 2);
            data[cursor..cursor + 4].copy_from_slice(&tick_value.to_le_bytes());
            cursor += 4;

            let liquidity_net: i128 = idx as i128 - 30;
            data[cursor..cursor + 16].copy_from_slice(&liquidity_net.to_le_bytes());
            cursor += 16;

            let liquidity_gross: u128 = (idx as u128) * 1_000;
            data[cursor..cursor + 16].copy_from_slice(&liquidity_gross.to_le_bytes());
            cursor += 16;

            let fee0: u128 = 1_000 + idx as u128;
            data[cursor..cursor + 16].copy_from_slice(&fee0.to_le_bytes());
            cursor += 16;

            let fee1: u128 = 2_000 + idx as u128;
            data[cursor..cursor + 16].copy_from_slice(&fee1.to_le_bytes());
            cursor += 16;

            for reward in 0..3 {
                let reward_value: u128 = (reward as u128) << 32 | idx as u128;
                data[cursor..cursor + 16].copy_from_slice(&reward_value.to_le_bytes());
                cursor += 16;
            }

            cursor += TICK_STATE_PADDING_BYTES;
        }

        let initialized_count = 12u8;
        data[cursor] = initialized_count;
        cursor += 1;

        let recent_epoch: u64 = 99;
        data[cursor..cursor + 8].copy_from_slice(&recent_epoch.to_le_bytes());

        let array = parse_tick_array_account(address, &data).expect("tick array should parse");
        assert_eq!(array.address, address);
        assert_eq!(array.pool_id, pool);
        assert_eq!(array.start_tick_index, start_tick_index);
        assert_eq!(array.initialized_tick_count, initialized_count);
        assert_eq!(array.recent_epoch, recent_epoch);
        assert_eq!(array.ticks.len(), TICK_ARRAY_TICK_COUNT);
        let first = &array.ticks[0];
        assert_eq!(first.tick, start_tick_index);
        assert_eq!(first.liquidity_net, -30);
        assert_eq!(first.liquidity_gross, 0);
        assert_eq!(first.reward_growths_outside_x64[0], 0);
        let last = &array.ticks[TICK_ARRAY_TICK_COUNT - 1];
        assert_eq!(
            last.tick,
            start_tick_index + ((TICK_ARRAY_TICK_COUNT as i32 - 1) * 2)
        );
        assert_eq!(
            last.fee_growth_outside_1_x64,
            2_000 + (TICK_ARRAY_TICK_COUNT as u128 - 1)
        );
    }

    fn write_pubkey(buffer: &mut [u8], cursor: usize, key: &Pubkey) -> usize {
        let end = cursor + PUBKEY_BYTES;
        buffer[cursor..end].copy_from_slice(key.as_ref());
        end
    }
}
