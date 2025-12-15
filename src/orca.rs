use crate::math;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};

pub const ORCA_WHIRLPOOL_PROGRAM_ID: Pubkey =
    pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");

const TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const MEMO_PROGRAM_ID: Pubkey = pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SWAP_V2_DISCRIMINATOR: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];
const WHIRLPOOL_ACCOUNT_DISCRIMINATOR: [u8; 8] = [63, 149, 209, 12, 225, 128, 99, 9];
const TICK_ARRAY_SIZE: i32 = 88;
const TICK_ARRAY_SEED: &[u8] = b"tick_array";
const ORACLE_SEED: &[u8] = b"oracle";
const MIN_SQRT_PRICE_X64: u128 = 1;
const MAX_SQRT_PRICE_X64: u128 = u128::MAX - 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrcaRemainingAccountsType {
    TransferHookA = 0,
    TransferHookB = 1,
    TransferHookReward = 2,
    TransferHookInput = 3,
    TransferHookIntermediate = 4,
    TransferHookOutput = 5,
    SupplementalTickArrays = 6,
    SupplementalTickArraysOne = 7,
    SupplementalTickArraysTwo = 8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrcaRemainingAccountsSlice {
    pub accounts_type: OrcaRemainingAccountsType,
    pub length: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct OrcaSwapV2Extras<'a> {
    pub token_program_a: Pubkey,
    pub token_program_b: Pubkey,
    pub memo_program: Pubkey,
    pub remaining_accounts: &'a [AccountMeta],
    pub remaining_account_slices: &'a [OrcaRemainingAccountsSlice],
}

impl<'a> Default for OrcaSwapV2Extras<'a> {
    fn default() -> Self {
        Self {
            token_program_a: TOKEN_PROGRAM_ID,
            token_program_b: TOKEN_PROGRAM_ID,
            memo_program: MEMO_PROGRAM_ID,
            remaining_accounts: &[],
            remaining_account_slices: &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrcaSwapV2ExtrasOwned {
    pub token_program_a: Pubkey,
    pub token_program_b: Pubkey,
    pub memo_program: Pubkey,
    pub remaining_accounts: Vec<AccountMeta>,
    pub remaining_account_slices: Vec<OrcaRemainingAccountsSlice>,
}

impl OrcaSwapV2ExtrasOwned {
    pub fn as_borrowed(&self) -> OrcaSwapV2Extras<'_> {
        OrcaSwapV2Extras {
            token_program_a: if self.token_program_a == Pubkey::default() {
                TOKEN_PROGRAM_ID
            } else {
                self.token_program_a
            },
            token_program_b: if self.token_program_b == Pubkey::default() {
                TOKEN_PROGRAM_ID
            } else {
                self.token_program_b
            },
            memo_program: if self.memo_program == Pubkey::default() {
                MEMO_PROGRAM_ID
            } else {
                self.memo_program
            },
            remaining_accounts: &self.remaining_accounts,
            remaining_account_slices: &self.remaining_account_slices,
        }
    }
}

impl Default for OrcaSwapV2ExtrasOwned {
    fn default() -> Self {
        Self {
            token_program_a: TOKEN_PROGRAM_ID,
            token_program_b: TOKEN_PROGRAM_ID,
            memo_program: MEMO_PROGRAM_ID,
            remaining_accounts: Vec::new(),
            remaining_account_slices: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrcaSwapAccounts {
    pub params: OrcaPoolParams,
    pub extras: OrcaSwapV2ExtrasOwned,
}

impl OrcaSwapAccounts {
    pub fn from_params(params: OrcaPoolParams) -> Self {
        Self {
            params,
            extras: OrcaSwapV2ExtrasOwned::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrcaTickState {
    pub tick_index: i32,
    pub initialized: bool,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrcaTickArray {
    pub whirlpool: Pubkey,
    pub start_tick_index: i32,
    pub ticks: Vec<OrcaTickState>,
}

impl OrcaTickArray {
    pub fn initialized_ticks(&self) -> impl Iterator<Item = &OrcaTickState> {
        self.ticks
            .iter()
            .filter(|tick| tick.initialized && tick.liquidity_gross > 0)
    }
}

/// Minimal subset of Orca Whirlpool pool state for deterministic planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrcaPoolParams {
    pub whirlpools_config: Pubkey,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub tick_spacing: u16,
    pub fee_rate: u16,
    pub protocol_fee_rate: u16,
    pub liquidity: u128,
    pub sqrt_price_x64: u128,
    pub tick_current_index: i32,
    pub reward_last_updated_timestamp: u64,
}

impl OrcaPoolParams {
    /// Attempts to decode the on-chain `Whirlpool` account layout straight from account data.
    ///
    /// Field order derived from `idls/whirlpool.json` (Anchor zero-copy struct).
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut cursor = 0usize;
        if data.len() < WHIRLPOOL_ACCOUNT_DISCRIMINATOR.len() {
            return None;
        }
        if data[cursor..cursor + WHIRLPOOL_ACCOUNT_DISCRIMINATOR.len()]
            != WHIRLPOOL_ACCOUNT_DISCRIMINATOR
        {
            return None;
        }
        cursor += WHIRLPOOL_ACCOUNT_DISCRIMINATOR.len();
        let whirlpools_config = read_pubkey(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 1)?; // whirlpool_bump
        let tick_spacing = read_u16(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 2)?; // fee_tier_index_seed
        let fee_rate = read_u16(data, &mut cursor)?;
        let protocol_fee_rate = read_u16(data, &mut cursor)?;
        let liquidity = read_u128(data, &mut cursor)?;
        let sqrt_price_x64 = read_u128(data, &mut cursor)?;
        let tick_current_index = read_i32(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 8)?; // protocol_fee_owed_a
        skip_bytes(data, &mut cursor, 8)?; // protocol_fee_owed_b
        let token_mint_a = read_pubkey(data, &mut cursor)?;
        let token_vault_a = read_pubkey(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 16)?; // fee_growth_global_a
        let token_mint_b = read_pubkey(data, &mut cursor)?;
        let token_vault_b = read_pubkey(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 16)?; // fee_growth_global_b
        let reward_last_updated_timestamp = read_u64(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 128 * 3)?; // reward_infos array

        Some(Self {
            whirlpools_config,
            token_mint_a,
            token_mint_b,
            token_vault_a,
            token_vault_b,
            tick_spacing,
            fee_rate,
            protocol_fee_rate,
            liquidity,
            sqrt_price_x64,
            tick_current_index,
            reward_last_updated_timestamp,
        })
    }
}

/// Decodes a Whirlpool tick-array account into a planner-friendly struct.
pub fn decode_tick_array(data: &[u8], tick_spacing: u16) -> Option<OrcaTickArray> {
    let spacing = i32::from(tick_spacing);
    if spacing <= 0 {
        return None;
    }

    let mut cursor = 0usize;
    let start_tick_index = read_i32(data, &mut cursor)?;
    let mut ticks = Vec::with_capacity(TICK_ARRAY_SIZE as usize);
    for idx in 0..TICK_ARRAY_SIZE {
        let initialized = read_bool(data, &mut cursor)?;
        let liquidity_net = read_i128(data, &mut cursor)?;
        let liquidity_gross = read_u128(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 16)?; // fee_growth_outside_a
        skip_bytes(data, &mut cursor, 16)?; // fee_growth_outside_b
        skip_bytes(data, &mut cursor, 16 * 3)?; // reward_growths_outside

        let offset = spacing.checked_mul(idx)?;
        let tick_index = start_tick_index.checked_add(offset)?;
        ticks.push(OrcaTickState {
            tick_index,
            initialized,
            liquidity_net,
            liquidity_gross,
        });
    }

    let whirlpool = read_pubkey(data, &mut cursor)?;
    Some(OrcaTickArray {
        whirlpool,
        start_tick_index,
        ticks,
    })
}

/// Approximates the Whirlpool virtual reserves leveraging liquidity + sqrt price.
pub fn infer_virtual_reserves(
    params: &OrcaPoolParams,
    min_shift: u32,
) -> Option<((Pubkey, u128), (Pubkey, u128))> {
    if params.liquidity == 0 || params.sqrt_price_x64 == 0 {
        return None;
    }

    let sqrt = params.sqrt_price_x64;
    let liquidity = params.liquidity;

    let base_scale = liquidity.checked_div(sqrt)?;
    let reserve_a = base_scale.checked_shl(min_shift)?;
    let quote_scale = liquidity.checked_shr(min_shift)?;
    let reserve_b = quote_scale.checked_mul(sqrt)?;

    if reserve_a == 0 || reserve_b == 0 {
        return None;
    }

    Some((
        (params.token_mint_a, reserve_a),
        (params.token_mint_b, reserve_b),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrcaSwapSimulation {
    pub amount_in: u128,
    pub amount_out: u128,
    pub sqrt_price_after: u128,
    pub tick_after: i32,
    pub liquidity_after: u128,
}

/// Deterministically simulates an exact-in Whirlpool swap using decoded tick arrays.
pub fn simulate_swap_exact_in(
    params: &OrcaPoolParams,
    tick_arrays: &[OrcaTickArray],
    a_to_b: bool,
    amount_in: u64,
    fee_rate_millionths: u32,
) -> Option<OrcaSwapSimulation> {
    if amount_in == 0 || params.liquidity == 0 || params.sqrt_price_x64 == 0 {
        return None;
    }
    if tick_arrays.is_empty() {
        return None;
    }

    let initialized = build_initialized_tick_map(tick_arrays);
    if initialized.is_empty() {
        return None;
    }

    let mut liquidity = params.liquidity;
    let mut sqrt_price = params.sqrt_price_x64;
    let mut current_tick = params.tick_current_index;
    let mut amount_remaining = amount_in as u128;
    let mut total_in = 0u128;
    let mut total_out = 0u128;
    let fee_rate = u128::from(fee_rate_millionths.min(math::FEE_DENOMINATOR as u32));
    let fee_numerator = math::FEE_DENOMINATOR.checked_sub(fee_rate)?;
    if fee_numerator == 0 {
        return None;
    }

    while amount_remaining > 0 && liquidity > 0 {
        let next_tick = if a_to_b {
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

        let step = if a_to_b {
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
        liquidity = apply_liquidity_delta(liquidity, liquidity_net, a_to_b)?;
        current_tick = if a_to_b {
            next_tick.saturating_sub(1)
        } else {
            next_tick
        };
    }

    if total_in == 0 || total_out == 0 {
        return None;
    }

    Some(OrcaSwapSimulation {
        amount_in: total_in,
        amount_out: total_out,
        sqrt_price_after: sqrt_price,
        tick_after: current_tick,
        liquidity_after: liquidity,
    })
}

pub fn build_swap_instruction(
    params: &OrcaPoolParams,
    whirlpool: Pubkey,
    input_mint: Pubkey,
    output_mint: Pubkey,
    user_input_account: Pubkey,
    user_output_account: Pubkey,
    authority: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
) -> Option<Instruction> {
    if amount_in == 0 {
        return None;
    }

    let ctx = compute_swap_context(
        params,
        whirlpool,
        input_mint,
        output_mint,
        user_input_account,
        user_output_account,
    )?;

    let data = encode_swap_legacy_payload(
        amount_in,
        min_amount_out,
        ctx.sqrt_price_limit,
        ctx.direction.a_to_b,
    );

    let mut accounts = Vec::with_capacity(11);
    accounts.push(AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false));
    accounts.push(AccountMeta::new(authority, true));
    accounts.push(AccountMeta::new(whirlpool, false));
    accounts.push(AccountMeta::new(ctx.direction.owner_account_a, false));
    accounts.push(AccountMeta::new(params.token_vault_a, false));
    accounts.push(AccountMeta::new(ctx.direction.owner_account_b, false));
    accounts.push(AccountMeta::new(params.token_vault_b, false));
    for array in ctx.tick_arrays {
        accounts.push(AccountMeta::new(array, false));
    }
    accounts.push(AccountMeta::new_readonly(ctx.oracle, false));

    Some(Instruction {
        program_id: ORCA_WHIRLPOOL_PROGRAM_ID,
        accounts,
        data,
    })
}

pub fn build_swap_v2_instruction(
    accounts: &OrcaSwapAccounts,
    whirlpool: Pubkey,
    input_mint: Pubkey,
    output_mint: Pubkey,
    user_input_account: Pubkey,
    user_output_account: Pubkey,
    authority: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
) -> Option<Instruction> {
    if amount_in == 0 {
        return None;
    }

    let owned = &accounts.extras;
    let extras = owned.as_borrowed();
    validate_remaining_accounts(&extras)?;

    let ctx = compute_swap_context(
        &accounts.params,
        whirlpool,
        input_mint,
        output_mint,
        user_input_account,
        user_output_account,
    )?;

    let data = encode_swap_v2_payload(
        amount_in,
        min_amount_out,
        ctx.sqrt_price_limit,
        ctx.direction.a_to_b,
        extras.remaining_account_slices,
    )?;

    let mut metas = Vec::with_capacity(15 + extras.remaining_accounts.len());
    metas.push(AccountMeta::new_readonly(extras.token_program_a, false));
    metas.push(AccountMeta::new_readonly(extras.token_program_b, false));
    metas.push(AccountMeta::new_readonly(extras.memo_program, false));
    metas.push(AccountMeta::new(authority, true));
    metas.push(AccountMeta::new(whirlpool, false));
    metas.push(AccountMeta::new_readonly(
        accounts.params.token_mint_a,
        false,
    ));
    metas.push(AccountMeta::new_readonly(
        accounts.params.token_mint_b,
        false,
    ));
    metas.push(AccountMeta::new(ctx.direction.owner_account_a, false));
    metas.push(AccountMeta::new(accounts.params.token_vault_a, false));
    metas.push(AccountMeta::new(ctx.direction.owner_account_b, false));
    metas.push(AccountMeta::new(accounts.params.token_vault_b, false));
    for array in ctx.tick_arrays {
        metas.push(AccountMeta::new(array, false));
    }
    metas.push(AccountMeta::new(ctx.oracle, false));
    for account in extras.remaining_accounts {
        metas.push(account.clone());
    }

    Some(Instruction {
        program_id: ORCA_WHIRLPOOL_PROGRAM_ID,
        accounts: metas,
        data,
    })
}

/// Returns `true` when the swap moves from token A toward token B.
pub fn swap_direction(
    params: &OrcaPoolParams,
    input_mint: Pubkey,
    output_mint: Pubkey,
) -> Option<bool> {
    if input_mint == params.token_mint_a && output_mint == params.token_mint_b {
        Some(true)
    } else if input_mint == params.token_mint_b && output_mint == params.token_mint_a {
        Some(false)
    } else {
        None
    }
}

/// Derives supplemental tick arrays that extend the traversal past the primary 3 PDAs.
pub fn derive_supplemental_tick_arrays(
    whirlpool: Pubkey,
    params: &OrcaPoolParams,
    a_to_b: bool,
    desired: usize,
) -> Vec<Pubkey> {
    if desired == 0 {
        return Vec::new();
    }

    let ticks_per_array = match i32::from(params.tick_spacing).checked_mul(TICK_ARRAY_SIZE) {
        Some(val) if val != 0 => val,
        _ => return Vec::new(),
    };

    let primary_start = align_tick_start(params.tick_current_index, ticks_per_array);
    let mut extras = Vec::with_capacity(desired);

    for idx in 3..3 + desired {
        let offset = match ticks_per_array.checked_mul(idx as i32) {
            Some(v) => v,
            None => break,
        };
        let target = if a_to_b {
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
        extras.push(derive_tick_array_address(whirlpool, target));
    }

    extras
}

struct SwapContext {
    direction: DirectionContext,
    tick_arrays: [Pubkey; 3],
    sqrt_price_limit: u128,
    oracle: Pubkey,
}

#[derive(Debug, Clone, Copy)]
struct DirectionContext {
    a_to_b: bool,
    owner_account_a: Pubkey,
    owner_account_b: Pubkey,
}

fn compute_swap_context(
    params: &OrcaPoolParams,
    whirlpool: Pubkey,
    input_mint: Pubkey,
    output_mint: Pubkey,
    user_input_account: Pubkey,
    user_output_account: Pubkey,
) -> Option<SwapContext> {
    let direction = map_owner_accounts(
        params,
        input_mint,
        output_mint,
        user_input_account,
        user_output_account,
    )?;

    let ticks_per_array = i32::from(params.tick_spacing).checked_mul(TICK_ARRAY_SIZE)?;
    if ticks_per_array == 0 {
        return None;
    }

    let primary_start = align_tick_start(params.tick_current_index, ticks_per_array);
    let tick_arrays =
        derive_tick_arrays(whirlpool, primary_start, ticks_per_array, direction.a_to_b)?;
    let oracle = derive_oracle_address(whirlpool);
    let sqrt_price_limit = if direction.a_to_b {
        MIN_SQRT_PRICE_X64
    } else {
        MAX_SQRT_PRICE_X64
    };

    Some(SwapContext {
        direction,
        tick_arrays,
        sqrt_price_limit,
        oracle,
    })
}

fn derive_tick_arrays(
    whirlpool: Pubkey,
    primary_start: i32,
    ticks_per_array: i32,
    a_to_b: bool,
) -> Option<[Pubkey; 3]> {
    let current = derive_tick_array_address(whirlpool, primary_start);
    let second_start = if a_to_b {
        primary_start.checked_sub(ticks_per_array)?
    } else {
        primary_start.checked_add(ticks_per_array)?
    };
    let third_start = if a_to_b {
        second_start.checked_sub(ticks_per_array)?
    } else {
        second_start.checked_add(ticks_per_array)?
    };

    let second = derive_tick_array_address(whirlpool, second_start);
    let third = derive_tick_array_address(whirlpool, third_start);
    Some([current, second, third])
}

fn align_tick_start(tick_index: i32, ticks_per_array: i32) -> i32 {
    tick_index - tick_index.rem_euclid(ticks_per_array)
}

fn derive_tick_array_address(whirlpool: Pubkey, start_index: i32) -> Pubkey {
    let index_bytes = start_index.to_le_bytes();
    Pubkey::find_program_address(
        &[TICK_ARRAY_SEED, whirlpool.as_ref(), &index_bytes],
        &ORCA_WHIRLPOOL_PROGRAM_ID,
    )
    .0
}

fn derive_oracle_address(whirlpool: Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[ORACLE_SEED, whirlpool.as_ref()],
        &ORCA_WHIRLPOOL_PROGRAM_ID,
    )
    .0
}

fn map_owner_accounts(
    params: &OrcaPoolParams,
    input_mint: Pubkey,
    output_mint: Pubkey,
    user_input_account: Pubkey,
    user_output_account: Pubkey,
) -> Option<DirectionContext> {
    if input_mint == params.token_mint_a && output_mint == params.token_mint_b {
        return Some(DirectionContext {
            a_to_b: true,
            owner_account_a: user_input_account,
            owner_account_b: user_output_account,
        });
    }

    if input_mint == params.token_mint_b && output_mint == params.token_mint_a {
        return Some(DirectionContext {
            a_to_b: false,
            owner_account_a: user_output_account,
            owner_account_b: user_input_account,
        });
    }

    None
}

fn build_initialized_tick_map(arrays: &[OrcaTickArray]) -> BTreeMap<i32, i128> {
    let mut map = BTreeMap::new();
    for array in arrays {
        for tick in array.initialized_ticks() {
            map.insert(tick.tick_index, tick.liquidity_net);
        }
    }
    map
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

fn apply_liquidity_delta(liquidity: u128, liquidity_net: i128, a_to_b: bool) -> Option<u128> {
    if liquidity_net >= 0 {
        let delta = liquidity_net as u128;
        if a_to_b {
            liquidity.checked_sub(delta)
        } else {
            liquidity.checked_add(delta)
        }
    } else {
        let delta = liquidity_net.unsigned_abs();
        if a_to_b {
            liquidity.checked_add(delta)
        } else {
            liquidity.checked_sub(delta)
        }
    }
}

fn encode_swap_legacy_payload(
    amount_in: u64,
    min_amount_out: u64,
    sqrt_price_limit: u128,
    a_to_b: bool,
) -> Vec<u8> {
    let mut data = Vec::with_capacity(8 + 8 + 8 + 16 + 1 + 1);
    data.extend_from_slice(&SWAP_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
    data.push(1); // amount_specified_is_input
    data.push(u8::from(a_to_b));
    data
}

fn encode_swap_v2_payload(
    amount_in: u64,
    min_amount_out: u64,
    sqrt_price_limit: u128,
    a_to_b: bool,
    slices: &[OrcaRemainingAccountsSlice],
) -> Option<Vec<u8>> {
    let mut data = Vec::with_capacity(8 + 8 + 8 + 16 + 1 + 1 + 5 + slices.len() * 2);
    data.extend_from_slice(&SWAP_V2_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
    data.push(1); // amount_specified_is_input
    data.push(u8::from(a_to_b));

    if slices.is_empty() {
        data.push(0);
        return Some(data);
    }

    data.push(1); // Option tag for RemainingAccountsInfo
    let slice_len = u32::try_from(slices.len()).ok()?;
    data.extend_from_slice(&slice_len.to_le_bytes());
    for slice in slices {
        data.push(slice.accounts_type as u8);
        data.push(slice.length);
    }

    Some(data)
}

fn validate_remaining_accounts(extras: &OrcaSwapV2Extras<'_>) -> Option<()> {
    if extras.remaining_account_slices.is_empty() {
        return if extras.remaining_accounts.is_empty() {
            Some(())
        } else {
            None
        };
    }

    let mut expected = 0usize;
    for slice in extras.remaining_account_slices {
        expected = expected.checked_add(slice.length as usize)?;
    }

    if expected != extras.remaining_accounts.len() {
        return None;
    }

    Some(())
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

fn read_bool(data: &[u8], cursor: &mut usize) -> Option<bool> {
    if *cursor >= data.len() {
        return None;
    }
    let value = data[*cursor];
    *cursor += 1;
    Some(value != 0)
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

fn read_u128(data: &[u8], cursor: &mut usize) -> Option<u128> {
    if *cursor + 16 > data.len() {
        return None;
    }
    let bytes: [u8; 16] = data[*cursor..*cursor + 16].try_into().ok()?;
    *cursor += 16;
    Some(u128::from_le_bytes(bytes))
}

fn skip_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<()> {
    if *cursor + len > data.len() {
        return None;
    }
    *cursor += len;
    Some(())
}
