use crate::math;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use std::collections::BTreeMap;
use std::convert::TryInto;

pub const RAYDIUM_CLMM_PROGRAM_ID: Pubkey = pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK");

const TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const TOKEN_PROGRAM_2022_ID: Pubkey = pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
const MEMO_PROGRAM_ID: Pubkey = pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const SWAP_V2_DISCRIMINATOR: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];
const TICK_ARRAY_SIZE: i32 = 60;
const TICK_ARRAY_SEED: &[u8] = b"tick_array";
const TICK_ARRAY_BITMAP_SEED: &[u8] = b"tick_array_bitmap";
const MIN_SQRT_PRICE_X64: u128 = 1; // Technically > 0; practical floor for Raydium
const MAX_SQRT_PRICE_X64: u128 = u128::MAX - 1;

/// Minimal subset of Raydium CLMM pool state required for fast simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaydiumPoolParams {
    pub amm_config: Pubkey,
    pub token_mint_0: Pubkey,
    pub token_mint_1: Pubkey,
    pub token_vault_0: Pubkey,
    pub token_vault_1: Pubkey,
    pub observation_key: Pubkey,
    pub mint_decimals_0: u8,
    pub mint_decimals_1: u8,
    pub tick_spacing: u16,
    pub liquidity: u128,
    pub sqrt_price_x64: u128,
    pub tick_current: i32,
    pub status: u8,
}

impl RaydiumPoolParams {
    /// Attempts to decode the Raydium `PoolState` struct directly from account data.
    ///
    /// Layout derived from `idls/amm_v3.json` (`PoolState` definition, packed C struct).
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut cursor = 0usize;
        skip_bytes(data, &mut cursor, 1)?; // bump
        let amm_config = read_pubkey(data, &mut cursor)?;
        skip_pubkey(data, &mut cursor)?; // owner
        let token_mint_0 = read_pubkey(data, &mut cursor)?;
        let token_mint_1 = read_pubkey(data, &mut cursor)?;
        let token_vault_0 = read_pubkey(data, &mut cursor)?;
        let token_vault_1 = read_pubkey(data, &mut cursor)?;
        let observation_key = read_pubkey(data, &mut cursor)?;
        let mint_decimals_0 = read_u8(data, &mut cursor)?;
        let mint_decimals_1 = read_u8(data, &mut cursor)?;
        let tick_spacing = read_u16(data, &mut cursor)?;
        let liquidity = read_u128(data, &mut cursor)?;
        let sqrt_price_x64 = read_u128(data, &mut cursor)?;
        let tick_current = read_i32(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 2)?; // padding3
        skip_bytes(data, &mut cursor, 2)?; // padding4
        skip_bytes(data, &mut cursor, 16 * 2)?; // fee_growth globals
        skip_bytes(data, &mut cursor, 8 * 2)?; // protocol fees
        skip_bytes(data, &mut cursor, 16 * 4)?; // swap counters
        let status = read_u8(data, &mut cursor)?;
        // The remaining padding / arrays are not needed for planning; no need to parse further.

        Some(Self {
            amm_config,
            token_mint_0,
            token_mint_1,
            token_vault_0,
            token_vault_1,
            observation_key,
            mint_decimals_0,
            mint_decimals_1,
            tick_spacing,
            liquidity,
            sqrt_price_x64,
            tick_current,
            status,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaydiumTickState {
    pub tick: i32,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaydiumTickArray {
    pub pool_id: Pubkey,
    pub start_tick_index: i32,
    pub ticks: Vec<RaydiumTickState>,
    pub initialized_tick_count: u8,
    pub recent_epoch: u64,
}

impl RaydiumTickArray {
    pub fn initialized_ticks(&self) -> impl Iterator<Item = &RaydiumTickState> {
        self.ticks.iter().filter(|tick| tick.liquidity_gross > 0)
    }
}

/// Decodes a Raydium CLMM tick-array account into a lightweight struct useful for planning.
pub fn decode_tick_array(data: &[u8]) -> Option<RaydiumTickArray> {
    let mut cursor = 0usize;
    let pool_id = read_pubkey(data, &mut cursor)?;
    let start_tick_index = read_i32(data, &mut cursor)?;

    let mut ticks = Vec::with_capacity(TICK_ARRAY_SIZE as usize);
    for _ in 0..TICK_ARRAY_SIZE {
        let tick_index = read_i32(data, &mut cursor)?;
        let liquidity_net = read_i128(data, &mut cursor)?;
        let liquidity_gross = read_u128(data, &mut cursor)?;
        // Skip fee growth + reward growth fields we do not currently use.
        skip_bytes(data, &mut cursor, 16)?; // fee_growth_outside_0_x64
        skip_bytes(data, &mut cursor, 16)?; // fee_growth_outside_1_x64
        skip_bytes(data, &mut cursor, 16 * 3)?; // reward_growths_outside_x64
        skip_bytes(data, &mut cursor, 4 * 13)?; // padding

        ticks.push(RaydiumTickState {
            tick: tick_index,
            liquidity_net,
            liquidity_gross,
        });
    }

    let initialized_tick_count = read_u8(data, &mut cursor)?;
    let recent_epoch = read_u64(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 107)?; // trailing padding

    Some(RaydiumTickArray {
        pool_id,
        start_tick_index,
        ticks,
        initialized_tick_count,
        recent_epoch,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaydiumSwapAccounts {
    pub params: RaydiumPoolParams,
    pub supplemental_tick_arrays: Vec<Pubkey>,
    pub include_bitmap_extension: bool,
}

impl RaydiumSwapAccounts {
    pub fn from_params(params: RaydiumPoolParams) -> Self {
        Self {
            params,
            supplemental_tick_arrays: Vec::new(),
            include_bitmap_extension: false,
        }
    }
}

/// Builds a Raydium CLMM `swap_v2` instruction for an exact-in trade.
pub fn build_swap_v2_instruction(
    accounts: &RaydiumSwapAccounts,
    pool_state: Pubkey,
    input_mint: Pubkey,
    output_mint: Pubkey,
    user_input_account: Pubkey,
    user_output_account: Pubkey,
    payer: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
) -> Option<Instruction> {
    if amount_in == 0 {
        return None;
    }

    let params = &accounts.params;
    let (input_vault, output_vault, input_vault_mint, output_vault_mint, zero_for_one) =
        direction_accounts(params, input_mint, output_mint)?;

    let tick_spacing = i32::from(params.tick_spacing);
    if tick_spacing <= 0 {
        return None;
    }
    let ticks_per_array = TICK_ARRAY_SIZE.checked_mul(tick_spacing)?;
    let primary_start = align_tick_start(params.tick_current, ticks_per_array);
    let tick_arrays = derive_tick_arrays(pool_state, primary_start, ticks_per_array, zero_for_one)?;

    let extra_accounts = accounts.supplemental_tick_arrays.len()
        + if accounts.include_bitmap_extension {
            1
        } else {
            0
        };
    let mut metas = Vec::with_capacity(13 + tick_arrays.len() + extra_accounts);
    metas.push(AccountMeta::new(payer, true));
    metas.push(AccountMeta::new_readonly(params.amm_config, false));
    metas.push(AccountMeta::new(pool_state, false));
    metas.push(AccountMeta::new(user_input_account, false));
    metas.push(AccountMeta::new(user_output_account, false));
    metas.push(AccountMeta::new(input_vault, false));
    metas.push(AccountMeta::new(output_vault, false));
    metas.push(AccountMeta::new(params.observation_key, false));
    metas.push(AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false));
    metas.push(AccountMeta::new_readonly(TOKEN_PROGRAM_2022_ID, false));
    metas.push(AccountMeta::new_readonly(MEMO_PROGRAM_ID, false));
    metas.push(AccountMeta::new_readonly(input_vault_mint, false));
    metas.push(AccountMeta::new_readonly(output_vault_mint, false));
    for tick_array in tick_arrays {
        metas.push(AccountMeta::new(tick_array, false));
    }
    if accounts.include_bitmap_extension {
        metas.push(AccountMeta::new_readonly(
            derive_bitmap_address(pool_state),
            false,
        ));
    }
    for tick_array in &accounts.supplemental_tick_arrays {
        metas.push(AccountMeta::new(*tick_array, false));
    }

    let sqrt_price_limit = if zero_for_one {
        MIN_SQRT_PRICE_X64
    } else {
        MAX_SQRT_PRICE_X64
    };

    let mut data = Vec::with_capacity(8 + 8 + 8 + 16 + 1);
    data.extend_from_slice(&SWAP_V2_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
    data.push(1); // is_base_input = true (exact-in)

    Some(Instruction {
        program_id: RAYDIUM_CLMM_PROGRAM_ID,
        accounts: metas,
        data,
    })
}

/// Returns `true` when the input leg is token0 -> token1.
pub fn swap_direction(
    params: &RaydiumPoolParams,
    input_mint: Pubkey,
    output_mint: Pubkey,
) -> Option<bool> {
    direction_accounts(params, input_mint, output_mint)
        .map(|(_, _, _, _, zero_for_one)| zero_for_one)
}

/// Derives up to `desired` supplemental tick arrays continuing in the trade direction.
pub fn derive_supplemental_tick_arrays(
    pool_state: Pubkey,
    params: &RaydiumPoolParams,
    zero_for_one: bool,
    desired: usize,
) -> Vec<Pubkey> {
    if desired == 0 {
        return Vec::new();
    }

    let tick_spacing = i32::from(params.tick_spacing);
    if tick_spacing <= 0 {
        return Vec::new();
    }

    let ticks_per_array = match TICK_ARRAY_SIZE.checked_mul(tick_spacing) {
        Some(val) => val,
        None => return Vec::new(),
    };
    let primary_start = align_tick_start(params.tick_current, ticks_per_array);

    let mut extras = Vec::with_capacity(desired);
    for idx in 3..3 + desired {
        let offset = match ticks_per_array.checked_mul(idx as i32) {
            Some(v) => v,
            None => break,
        };
        let target = if zero_for_one {
            match primary_start.checked_sub(offset) {
                Some(v) => v,
                None => break,
            }
        } else {
            match primary_start.checked_add(offset) {
                Some(v) => v,
                None => break,
            }
        };
        extras.push(derive_tick_array_address(pool_state, target));
    }

    extras
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaydiumSwapSimulation {
    pub amount_in: u128,
    pub amount_out: u128,
    pub sqrt_price_after: u128,
    pub tick_after: i32,
    pub liquidity_after: u128,
}

/// Walks Raydium tick arrays to deterministically quote an exact-in swap.
pub fn simulate_swap_exact_in(
    params: &RaydiumPoolParams,
    tick_arrays: &[RaydiumTickArray],
    zero_for_one: bool,
    amount_in: u64,
    fee_rate_millionths: u32,
) -> Option<RaydiumSwapSimulation> {
    if amount_in == 0 || params.liquidity == 0 || params.sqrt_price_x64 == 0 {
        return None;
    }
    if tick_arrays.is_empty() {
        return None;
    }

    let mut initialized = BTreeMap::new();
    for array in tick_arrays {
        for tick in array.initialized_ticks() {
            initialized.insert(tick.tick, tick.liquidity_net);
        }
    }
    if initialized.is_empty() {
        return None;
    }

    let mut liquidity = params.liquidity;
    let mut sqrt_price = params.sqrt_price_x64;
    let mut current_tick = params.tick_current;
    let mut amount_remaining = amount_in as u128;
    let mut total_in = 0u128;
    let mut total_out = 0u128;
    let fee_rate = u128::from(fee_rate_millionths.min(math::FEE_DENOMINATOR as u32));
    let fee_numerator = math::FEE_DENOMINATOR.checked_sub(fee_rate)?;
    if fee_numerator == 0 {
        return None;
    }

    while amount_remaining > 0 && liquidity > 0 {
        let next_tick = if zero_for_one {
            match initialized.range(..current_tick).next_back() {
                Some((tick, _)) => *tick,
                None => break,
            }
        } else {
            match initialized.range((current_tick.saturating_add(1))..).next() {
                Some((tick, _)) => *tick,
                None => break,
            }
        };

        let sqrt_price_target = math::sqrt_price_from_tick(next_tick)?;
        let amount_remaining_less_fee =
            math::mul_div_u256(amount_remaining, fee_numerator, math::FEE_DENOMINATOR)?;
        if amount_remaining_less_fee == 0 {
            break;
        }

        let step = if zero_for_one {
            compute_zero_for_one_step(
                liquidity,
                sqrt_price,
                sqrt_price_target,
                amount_remaining,
                amount_remaining_less_fee,
                fee_numerator,
            )?
        } else {
            compute_one_for_zero_step(
                liquidity,
                sqrt_price,
                sqrt_price_target,
                amount_remaining,
                amount_remaining_less_fee,
                fee_numerator,
            )?
        };

        amount_remaining = amount_remaining.checked_sub(step.amount_in_with_fee)?;
        total_in = total_in.checked_add(step.amount_in_with_fee)?;
        total_out = total_out.checked_add(step.amount_out)?;
        sqrt_price = step.sqrt_price_next;

        if !step.reached_target {
            break;
        }

        let liquidity_net = *initialized.get(&next_tick)?;
        liquidity = apply_liquidity_delta(liquidity, liquidity_net, zero_for_one)?;
        current_tick = if zero_for_one {
            next_tick.saturating_sub(1)
        } else {
            next_tick
        };
    }

    if total_in == 0 || total_out == 0 {
        return None;
    }

    Some(RaydiumSwapSimulation {
        amount_in: total_in,
        amount_out: total_out,
        sqrt_price_after: sqrt_price,
        tick_after: current_tick,
        liquidity_after: liquidity,
    })
}

/// PDA for the optional Raydium tick-array bitmap extension account.
pub fn bitmap_extension_address(pool_state: Pubkey) -> Pubkey {
    derive_bitmap_address(pool_state)
}

fn direction_accounts(
    params: &RaydiumPoolParams,
    input_mint: Pubkey,
    output_mint: Pubkey,
) -> Option<(Pubkey, Pubkey, Pubkey, Pubkey, bool)> {
    if input_mint == params.token_mint_0 && output_mint == params.token_mint_1 {
        Some((
            params.token_vault_0,
            params.token_vault_1,
            params.token_mint_0,
            params.token_mint_1,
            true,
        ))
    } else if input_mint == params.token_mint_1 && output_mint == params.token_mint_0 {
        Some((
            params.token_vault_1,
            params.token_vault_0,
            params.token_mint_1,
            params.token_mint_0,
            false,
        ))
    } else {
        None
    }
}

fn derive_tick_arrays(
    pool_state: Pubkey,
    primary_start: i32,
    ticks_per_array: i32,
    zero_for_one: bool,
) -> Option<[Pubkey; 3]> {
    let mut arrays = [Pubkey::default(); 3];
    for (idx, slot) in arrays.iter_mut().enumerate() {
        let offset = ticks_per_array.checked_mul(idx as i32)?;
        let start_tick = if zero_for_one {
            primary_start.checked_sub(offset)?
        } else {
            primary_start.checked_add(offset)?
        };
        *slot = derive_tick_array_address(pool_state, start_tick);
    }
    Some(arrays)
}

fn align_tick_start(tick_index: i32, ticks_per_array: i32) -> i32 {
    tick_index - tick_index.rem_euclid(ticks_per_array)
}

fn derive_tick_array_address(pool_state: Pubkey, start_tick_index: i32) -> Pubkey {
    let start_bytes = start_tick_index.to_le_bytes();
    Pubkey::find_program_address(
        &[TICK_ARRAY_SEED, pool_state.as_ref(), &start_bytes],
        &RAYDIUM_CLMM_PROGRAM_ID,
    )
    .0
}

fn derive_bitmap_address(pool_state: Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[TICK_ARRAY_BITMAP_SEED, pool_state.as_ref()],
        &RAYDIUM_CLMM_PROGRAM_ID,
    )
    .0
}

#[derive(Debug, Clone, Copy)]
struct StepComputation {
    sqrt_price_next: u128,
    amount_in_with_fee: u128,
    amount_out: u128,
    reached_target: bool,
}

fn compute_zero_for_one_step(
    liquidity: u128,
    sqrt_price_current: u128,
    sqrt_price_target: u128,
    amount_remaining: u128,
    amount_remaining_less_fee: u128,
    fee_numerator: u128,
) -> Option<StepComputation> {
    let fee_den = math::FEE_DENOMINATOR;
    let amount_to_target =
        math::amount0_delta(liquidity, sqrt_price_current, sqrt_price_target, true)?;
    if amount_remaining_less_fee >= amount_to_target {
        let amount_out =
            math::amount1_delta(liquidity, sqrt_price_current, sqrt_price_target, false)?;
        let amount_in_with_fee =
            math::mul_div_u256_rounding_up(amount_to_target, fee_den, fee_numerator)?;
        Some(StepComputation {
            sqrt_price_next: sqrt_price_target,
            amount_in_with_fee,
            amount_out,
            reached_target: true,
        })
    } else {
        let sqrt_price_next = math::next_sqrt_price_from_amount0_in(
            sqrt_price_current,
            liquidity,
            amount_remaining_less_fee,
        )?;
        let amount_out =
            math::amount1_delta(liquidity, sqrt_price_current, sqrt_price_next, false)?;
        Some(StepComputation {
            sqrt_price_next,
            amount_in_with_fee: amount_remaining,
            amount_out,
            reached_target: false,
        })
    }
}

fn compute_one_for_zero_step(
    liquidity: u128,
    sqrt_price_current: u128,
    sqrt_price_target: u128,
    amount_remaining: u128,
    amount_remaining_less_fee: u128,
    fee_numerator: u128,
) -> Option<StepComputation> {
    let fee_den = math::FEE_DENOMINATOR;
    let amount_to_target =
        math::amount1_delta(liquidity, sqrt_price_target, sqrt_price_current, true)?;
    if amount_remaining_less_fee >= amount_to_target {
        let amount_out =
            math::amount0_delta(liquidity, sqrt_price_target, sqrt_price_current, false)?;
        let amount_in_with_fee =
            math::mul_div_u256_rounding_up(amount_to_target, fee_den, fee_numerator)?;
        Some(StepComputation {
            sqrt_price_next: sqrt_price_target,
            amount_in_with_fee,
            amount_out,
            reached_target: true,
        })
    } else {
        let sqrt_price_next = math::next_sqrt_price_from_amount1_in(
            sqrt_price_current,
            liquidity,
            amount_remaining_less_fee,
        )?;
        let amount_out =
            math::amount0_delta(liquidity, sqrt_price_next, sqrt_price_current, false)?;
        Some(StepComputation {
            sqrt_price_next,
            amount_in_with_fee: amount_remaining,
            amount_out,
            reached_target: false,
        })
    }
}

fn apply_liquidity_delta(liquidity: u128, liquidity_net: i128, zero_for_one: bool) -> Option<u128> {
    if liquidity_net >= 0 {
        let delta = liquidity_net as u128;
        if zero_for_one {
            liquidity.checked_sub(delta)
        } else {
            liquidity.checked_add(delta)
        }
    } else {
        let delta = liquidity_net.unsigned_abs();
        if zero_for_one {
            liquidity.checked_add(delta)
        } else {
            liquidity.checked_sub(delta)
        }
    }
}

/// Attempts to synthesize virtual CLMM reserves (token0/token1) using liquidity + sqrt price.
pub fn infer_virtual_reserves(
    params: &RaydiumPoolParams,
    min_shift: u32,
) -> Option<((Pubkey, u128), (Pubkey, u128))> {
    if params.liquidity == 0 || params.sqrt_price_x64 == 0 {
        return None;
    }

    let sqrt = params.sqrt_price_x64;
    let liquidity = params.liquidity;

    let base_scale = liquidity.checked_div(sqrt)?;
    let reserve_0 = base_scale.checked_shl(min_shift)?;
    let quote_scale = liquidity.checked_shr(min_shift)?;
    let reserve_1 = quote_scale.checked_mul(sqrt)?;

    if reserve_0 == 0 || reserve_1 == 0 {
        return None;
    }

    Some((
        (params.token_mint_0, reserve_0),
        (params.token_mint_1, reserve_1),
    ))
}

fn read_pubkey(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    const LEN: usize = 32;
    if *cursor + LEN > data.len() {
        return None;
    }
    let bytes: [u8; LEN] = data[*cursor..*cursor + LEN].try_into().ok()?;
    *cursor += LEN;
    Some(Pubkey::new_from_array(bytes))
}

fn skip_pubkey(data: &[u8], cursor: &mut usize) -> Option<()> {
    skip_bytes(data, cursor, 32)
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    if *cursor + 1 > data.len() {
        return None;
    }
    let value = data[*cursor];
    *cursor += 1;
    Some(value)
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Option<u16> {
    if *cursor + 2 > data.len() {
        return None;
    }
    let bytes: [u8; 2] = data[*cursor..*cursor + 2].try_into().ok()?;
    *cursor += 2;
    Some(u16::from_le_bytes(bytes))
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Option<i32> {
    if *cursor + 4 > data.len() {
        return None;
    }
    let bytes: [u8; 4] = data[*cursor..*cursor + 4].try_into().ok()?;
    *cursor += 4;
    Some(i32::from_le_bytes(bytes))
}

fn read_u128(data: &[u8], cursor: &mut usize) -> Option<u128> {
    if *cursor + 16 > data.len() {
        return None;
    }
    let bytes: [u8; 16] = data[*cursor..*cursor + 16].try_into().ok()?;
    *cursor += 16;
    Some(u128::from_le_bytes(bytes))
}

fn read_i128(data: &[u8], cursor: &mut usize) -> Option<i128> {
    if *cursor + 16 > data.len() {
        return None;
    }
    let bytes: [u8; 16] = data[*cursor..*cursor + 16].try_into().ok()?;
    *cursor += 16;
    Some(i128::from_le_bytes(bytes))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Option<u64> {
    if *cursor + 8 > data.len() {
        return None;
    }
    let bytes: [u8; 8] = data[*cursor..*cursor + 8].try_into().ok()?;
    *cursor += 8;
    Some(u64::from_le_bytes(bytes))
}

fn skip_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<()> {
    if *cursor + len > data.len() {
        return None;
    }
    *cursor += len;
    Some(())
}
