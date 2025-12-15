use uint::construct_uint;

construct_uint! {
    /// 256-bit unsigned integer used for deterministic fixed-point math.
    pub struct U256(4);
}

/// Fixed-point scaling factor used by Solana CLMM pools (Q64.64 format).
pub const Q64: u128 = 1u128 << 64;
pub const FEE_DENOMINATOR: u128 = 1_000_000;
const ONE_SHIFT_32: u128 = 1u128 << 32;

/// Computes `(a * b) / denom` using 256-bit intermediates and flooring the result.
/// Returns `None` when `denom` is zero or the final value does not fit into `u128`.
pub fn mul_div_u256(a: u128, b: u128, denom: u128) -> Option<u128> {
    if denom == 0 {
        return None;
    }
    let product = U256::from(a) * U256::from(b);
    let result = product / U256::from(denom);
    if result > U256::from(u128::MAX) {
        None
    } else {
        Some(result.as_u128())
    }
}

/// Same as [`mul_div_u256`] but rounds up when there is a non-zero remainder.
pub fn mul_div_u256_rounding_up(a: u128, b: u128, denom: u128) -> Option<u128> {
    if denom == 0 {
        return None;
    }
    let numerator = U256::from(a) * U256::from(b);
    div_u256_to_u128(numerator, U256::from(denom), true)
}

/// Determines the amount of token0 required to move between two sqrt prices.
/// When `round_up` is true the calculation rounds up the final amount.
pub fn amount0_delta(
    liquidity: u128,
    sqrt_price_upper_x64: u128,
    sqrt_price_lower_x64: u128,
    round_up: bool,
) -> Option<u128> {
    if sqrt_price_lower_x64 == 0 || sqrt_price_upper_x64 < sqrt_price_lower_x64 {
        return None;
    }
    let delta = sqrt_price_upper_x64.checked_sub(sqrt_price_lower_x64)?;
    let mut numerator = U256::from(liquidity);
    numerator = checked_mul_u256(numerator, U256::from(delta))?;
    numerator = checked_mul_u256(numerator, U256::from(Q64))?;
    let mut denominator = U256::from(sqrt_price_upper_x64);
    denominator = checked_mul_u256(denominator, U256::from(sqrt_price_lower_x64))?;
    div_u256_to_u128(numerator, denominator, round_up)
}

/// Determines the amount of token1 obtained when moving between two sqrt prices.
/// When `round_up` is true the calculation rounds up the final amount.
pub fn amount1_delta(
    liquidity: u128,
    sqrt_price_upper_x64: u128,
    sqrt_price_lower_x64: u128,
    round_up: bool,
) -> Option<u128> {
    if sqrt_price_upper_x64 < sqrt_price_lower_x64 {
        return None;
    }
    let delta = sqrt_price_upper_x64.checked_sub(sqrt_price_lower_x64)?;
    let numerator = checked_mul_u256(U256::from(liquidity), U256::from(delta))?;
    div_u256_to_u128(numerator, U256::from(Q64), round_up)
}

/// Computes the next sqrt price after spending `amount_in` of token0.
pub fn next_sqrt_price_from_amount0_in(
    sqrt_price_current_x64: u128,
    liquidity: u128,
    amount_in: u128,
) -> Option<u128> {
    if sqrt_price_current_x64 == 0 || liquidity == 0 {
        return None;
    }
    let numerator = {
        let mut tmp = U256::from(liquidity);
        tmp = checked_mul_u256(tmp, U256::from(sqrt_price_current_x64))?;
        checked_mul_u256(tmp, U256::from(Q64))?
    };
    let base = checked_mul_u256(U256::from(liquidity), U256::from(Q64))?;
    let term = checked_mul_u256(U256::from(amount_in), U256::from(sqrt_price_current_x64))?;
    let denominator = checked_add_u256(base, term)?;
    div_u256_to_u128(numerator, denominator, false)
}

/// Computes the next sqrt price after spending `amount_in` of token1.
pub fn next_sqrt_price_from_amount1_in(
    sqrt_price_current_x64: u128,
    liquidity: u128,
    amount_in: u128,
) -> Option<u128> {
    if liquidity == 0 {
        return None;
    }
    let delta = mul_div_u256(amount_in, Q64, liquidity)?;
    sqrt_price_current_x64.checked_add(delta)
}

const MIN_TICK: i32 = -443_636;
const MAX_TICK: i32 = 443_636;
const RATIO_INITIAL_BIT0: U256 = U256([0xaa2d_162d_1a59_4001, 0xfffc_b933_bd6f_ad37, 0x0, 0x0]);
const RATIO_BASE: U256 = U256([0x0, 0x0, 0x1, 0x0]);
const TICK_RATIO_CONSTANTS: [U256; 19] = [
    U256([0x59a4_6990_580e_213a, 0xfff9_7272_373d_4132, 0x0, 0x0]),
    U256([0xef12_357c_f3c7_fdcc, 0xfff2_e50f_5f65_6932, 0x0, 0x0]),
    U256([0x1c36_24ea_a094_1cd0, 0xffe5_caca_7e10_e4e6, 0x0, 0x0]),
    U256([0xc9db_5883_5c92_6644, 0xffcb_9843_d60f_6159, 0x0, 0x0]),
    U256([0x472e_6896_dfb2_54c0, 0xff97_3b41_fa98_c081, 0x0, 0x0]),
    U256([0x43ec_78b3_26b5_2861, 0xff2e_a164_66c9_6a38, 0x0, 0x0]),
    U256([0x11c4_61f1_969c_3053, 0xfe5d_ee04_6a99_a2a8, 0x0, 0x0]),
    U256([0xdcff_c83b_479a_a3a4, 0xfcbe_86c7_900a_88ae, 0x0, 0x0]),
    U256([0x6f2b_074c_f781_5e54, 0xf987_a725_3ac4_1317, 0x0, 0x0]),
    U256([0x940c_7a39_8e4b_70f3, 0xf339_2b08_22b7_0005, 0x0, 0x0]),
    U256([0x43b2_9c7f_a6e8_89d9, 0xe715_9475_a2c2_9b74, 0x0, 0x0]),
    U256([0x845a_d8f7_92aa_5825, 0xd097_f3bd_fd20_22b8, 0x0, 0x0]),
    U256([0x8a65_dc1f_90e0_61e5, 0xa9f7_4646_2d87_0fdf, 0x0, 0x0]),
    U256([0x90bb_3df6_2baf_32f7, 0x70d8_69a1_56d2_a1b8, 0x0, 0x0]),
    U256([0x8123_1505_542f_cfa6, 0x31be_135f_97d0_8fd9, 0x0, 0x0]),
    U256([0xc677_de54_f3e9_9bc9, 0x09aa_508b_5b7a_84e1, 0x0, 0x0]),
    U256([0x6699_c329_225e_e604, 0x005d_6af8_dedb_8119, 0x0, 0x0]),
    U256([0x1ea9_2604_1bed_fe98, 0x0000_2216_e584_f5fa, 0x0, 0x0]),
    U256([0x91f7_dc42_444e_8fa2, 0x0000_0000_048a_1703, 0x0, 0x0]),
];

/// Converts a Raydium/Orca tick index into its sqrt price (Q64.64 fixed-point).
pub fn sqrt_price_from_tick(tick: i32) -> Option<u128> {
    if tick < MIN_TICK || tick > MAX_TICK {
        return None;
    }
    let abs_tick = tick.unsigned_abs();
    let mut ratio = if abs_tick & 0x1 != 0 {
        RATIO_INITIAL_BIT0
    } else {
        RATIO_BASE
    };

    for (idx, constant) in TICK_RATIO_CONSTANTS.iter().enumerate() {
        if abs_tick & (1 << (idx + 1)) != 0 {
            ratio = checked_mul_u256(ratio, *constant)?;
            ratio >>= 128;
        }
    }

    if tick > 0 {
        let max_val = U256::MAX;
        ratio = max_val / ratio;
    }

    let mut sqrt_price_x96 = ratio >> 32;
    if ratio & (U256::from(ONE_SHIFT_32 - 1)) != U256::zero() {
        sqrt_price_x96 += U256::one();
    }

    if sqrt_price_x96 > U256::from(u128::MAX) {
        return None;
    }
    let sqrt_price_x96_u128 = sqrt_price_x96.as_u128();
    let sqrt_price_x64 = (sqrt_price_x96_u128 + ONE_SHIFT_32 - 1) >> 32;
    Some(sqrt_price_x64)
}

fn checked_mul_u256(a: U256, b: U256) -> Option<U256> {
    let (res, overflow) = a.overflowing_mul(b);
    if overflow {
        None
    } else {
        Some(res)
    }
}

fn checked_add_u256(a: U256, b: U256) -> Option<U256> {
    let (res, overflow) = a.overflowing_add(b);
    if overflow {
        None
    } else {
        Some(res)
    }
}

fn div_u256_to_u128(numerator: U256, denominator: U256, round_up: bool) -> Option<u128> {
    if denominator.is_zero() {
        return None;
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    if quotient > U256::from(u128::MAX) {
        return None;
    }
    let mut value = quotient.as_u128();
    if round_up && !remainder.is_zero() {
        value = value.checked_add(1)?;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::{
        amount0_delta, amount1_delta, mul_div_u256, mul_div_u256_rounding_up,
        next_sqrt_price_from_amount0_in, next_sqrt_price_from_amount1_in, sqrt_price_from_tick,
        Q64,
    };

    #[test]
    fn mul_div_matches_small_values() {
        assert_eq!(mul_div_u256(10, 20, 5), Some(40));
        assert_eq!(mul_div_u256(1234, 5678, 1), Some(7_006_652));
    }

    #[test]
    fn mul_div_handles_zero_denom() {
        assert_eq!(mul_div_u256(10, 20, 0), None);
    }

    #[test]
    fn mul_div_detects_u128_overflow() {
        let almost = u128::MAX;
        assert_eq!(mul_div_u256(almost, 2, 1), None);
    }

    #[test]
    fn mul_div_rounds_up() {
        assert_eq!(mul_div_u256_rounding_up(10, 10, 6), Some(17));
    }

    #[test]
    fn tick_zero_matches_unity() {
        let sqrt_price = sqrt_price_from_tick(0).unwrap();
        assert_eq!(sqrt_price, Q64);
    }

    #[test]
    fn amount_deltas_behave() {
        let upper = sqrt_price_from_tick(10).unwrap();
        let lower = sqrt_price_from_tick(0).unwrap();
        let liq = 100_000_000_000u128;
        let amt0 = amount0_delta(liq, upper, lower, true).unwrap();
        let amt1 = amount1_delta(liq, upper, lower, false).unwrap();
        assert!(amt0 > 0);
        assert!(amt1 > 0);
    }

    #[test]
    fn next_price_from_inputs_changes_directionally() {
        let sqrt_price = sqrt_price_from_tick(0).unwrap();
        let liquidity = 50_000_000_000_000u128;
        let next_zero = next_sqrt_price_from_amount0_in(sqrt_price, liquidity, 1_000).unwrap();
        assert!(next_zero < sqrt_price);
        let next_one = next_sqrt_price_from_amount1_in(sqrt_price, liquidity, 1_000).unwrap();
        assert!(next_one > sqrt_price);
    }
}
