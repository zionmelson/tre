use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solend::SPL_TOKEN_PROGRAM_ID;
use std::convert::{TryFrom, TryInto};
use tracing::warn;

const POOL_FEES_LEN: usize = 160;
const PROTOCOL_AND_PARTNER_FEES_LEN: usize = 8 * 4;
const PRICE_BOUNDS_LEN: usize = 16 * 2;
const STATIC_PARAMS_LEN: usize = 32;
const VARIABLE_PARAMS_LEN: usize = 32;
const PROTOCOL_FEE_LEN: usize = 16;
const REWARD_INFO_LEN: usize = 144;
const METEORA_DAMM_POOL_DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
const METEORA_DLMM_PAIR_DISCRIMINATOR: [u8; 8] = [33, 11, 49, 98, 181, 101, 177, 13];

/// Program id for Meteora DAMM (cp_amm) mainnet deployment.
pub const METEORA_DAMM_PROGRAM_ID: Pubkey = pubkey!("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG");

/// Pool authority PDA used across all Meteora DAMM swaps.
pub const METEORA_DAMM_POOL_AUTHORITY: Pubkey =
    pubkey!("HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC");

/// Program id for Meteora DLMM (lb_clmm) mainnet deployment.
pub const METEORA_DLMM_PROGRAM_ID: Pubkey = pubkey!("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo");

const METEORA_DAMM_EVENT_AUTHORITY_SEED: &[u8] = b"__event_authority";
const METEORA_DAMM_SWAP2_DISCRIMINATOR: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];
const METEORA_SWAP_MODE_EXACT_IN: u8 = 0;
const METEORA_DLMM_SWAP2_DISCRIMINATOR: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];
const METEORA_DLMM_EVENT_AUTHORITY_SEED: &[u8] = b"__event_authority";

/// Minimal subset of Meteora DAMM pool state required for deterministic quoting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeteoraDammParams {
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    pub token_a_vault: Pubkey,
    pub token_b_vault: Pubkey,
    pub whitelisted_vault: Pubkey,
    pub partner: Pubkey,
    pub liquidity: u128,
    pub sqrt_price_x64: u128,
    pub activation_point: u64,
    pub activation_type: u8,
    pub pool_status: u8,
    pub token_a_flag: u8,
    pub token_b_flag: u8,
    pub collect_fee_mode: u8,
    pub pool_type: u8,
    pub version: u8,
}

fn damm_event_authority() -> Pubkey {
    Pubkey::find_program_address(
        &[METEORA_DAMM_EVENT_AUTHORITY_SEED],
        &METEORA_DAMM_PROGRAM_ID,
    )
    .0
}

fn dlmm_event_authority() -> Pubkey {
    Pubkey::find_program_address(
        &[METEORA_DLMM_EVENT_AUTHORITY_SEED],
        &METEORA_DLMM_PROGRAM_ID,
    )
    .0
}

impl MeteoraDammParams {
    /// Attempts to decode the Anchor `Pool` struct from raw account data (`idls/cp_amm.json`).
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut cursor = 0usize;
        skip_anchor_discriminator(data, &mut cursor, &METEORA_DAMM_POOL_DISCRIMINATOR)?;
        skip_bytes(data, &mut cursor, POOL_FEES_LEN)?;
        let token_a_mint = read_pubkey(data, &mut cursor)?;
        let token_b_mint = read_pubkey(data, &mut cursor)?;
        let token_a_vault = read_pubkey(data, &mut cursor)?;
        let token_b_vault = read_pubkey(data, &mut cursor)?;
        let whitelisted_vault = read_pubkey(data, &mut cursor)?;
        let partner = read_pubkey(data, &mut cursor)?;
        let liquidity = read_u128(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 16)?; // _padding
        skip_bytes(data, &mut cursor, PROTOCOL_AND_PARTNER_FEES_LEN)?;
        skip_bytes(data, &mut cursor, PRICE_BOUNDS_LEN)?;
        let sqrt_price_x64 = read_u128(data, &mut cursor)?;
        let activation_point = read_u64(data, &mut cursor)?;
        let activation_type = read_u8(data, &mut cursor)?;
        let pool_status = read_u8(data, &mut cursor)?;
        let token_a_flag = read_u8(data, &mut cursor)?;
        let token_b_flag = read_u8(data, &mut cursor)?;
        let collect_fee_mode = read_u8(data, &mut cursor)?;
        let pool_type = read_u8(data, &mut cursor)?;
        let version = read_u8(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 1)?; // _padding_0

        Some(Self {
            token_a_mint,
            token_b_mint,
            token_a_vault,
            token_b_vault,
            whitelisted_vault,
            partner,
            liquidity,
            sqrt_price_x64,
            activation_point,
            activation_type,
            pool_status,
            token_a_flag,
            token_b_flag,
            collect_fee_mode,
            pool_type,
            version,
        })
    }
}

/// Reconstructs approximate virtual reserves for Meteora DAMM pools.
pub fn infer_virtual_reserves(
    params: &MeteoraDammParams,
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
        (params.token_a_mint, reserve_a),
        (params.token_b_mint, reserve_b),
    ))
}

/// Minimal subset of Meteora DLMM (lb_clmm) state needed to perform planning math.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeteoraDlmmParams {
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub reserve_x: Pubkey,
    pub reserve_y: Pubkey,
    pub oracle: Pubkey,
    pub bin_array_bitmap: [u64; 16],
    pub pair_type: u8,
    pub active_id: i32,
    pub bin_step: u16,
    pub status: u8,
    pub activation_point: u64,
    pub pre_activation_duration: u64,
}

impl MeteoraDlmmParams {
    /// Decodes the Anchor `LbPair` struct directly from account data (`idls/lb_clmm.json`).
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut cursor = 0usize;
        skip_anchor_discriminator(data, &mut cursor, &METEORA_DLMM_PAIR_DISCRIMINATOR)?;
        skip_bytes(data, &mut cursor, STATIC_PARAMS_LEN)?; // parameters
        skip_bytes(data, &mut cursor, VARIABLE_PARAMS_LEN)?; // v_parameters
        skip_bytes(data, &mut cursor, 1)?; // bump_seed
        skip_bytes(data, &mut cursor, 2)?; // bin_step_seed
        let pair_type = read_u8(data, &mut cursor)?;
        let active_id = read_i32(data, &mut cursor)?;
        let bin_step = read_u16(data, &mut cursor)?;
        let status = read_u8(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 1)?; // require_base_factor_seed
        skip_bytes(data, &mut cursor, 2)?; // base_factor_seed
        skip_bytes(data, &mut cursor, 1)?; // activation_type
        skip_bytes(data, &mut cursor, 1)?; // creator_pool_on_off_control
        let token_x_mint = read_pubkey(data, &mut cursor)?;
        let token_y_mint = read_pubkey(data, &mut cursor)?;
        let reserve_x = read_pubkey(data, &mut cursor)?;
        let reserve_y = read_pubkey(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, PROTOCOL_FEE_LEN)?;
        skip_bytes(data, &mut cursor, 32)?; // _padding_1
        skip_bytes(data, &mut cursor, REWARD_INFO_LEN * 2)?; // reward_infos
        let oracle = read_pubkey(data, &mut cursor)?;
        let bin_array_bitmap = read_u64_array::<16>(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 8)?; // last_updated_at
        skip_bytes(data, &mut cursor, 32)?; // _padding_2
        skip_bytes(data, &mut cursor, 32)?; // pre_activation_swap_address
        skip_bytes(data, &mut cursor, 32)?; // base_key
        let activation_point = read_u64(data, &mut cursor)?;
        let pre_activation_duration = read_u64(data, &mut cursor)?;
        skip_bytes(data, &mut cursor, 8)?; // _padding_3
        skip_bytes(data, &mut cursor, 8)?; // _padding_4
        skip_bytes(data, &mut cursor, 32)?; // creator
        skip_bytes(data, &mut cursor, 1)?; // token_mint_x_program_flag
        skip_bytes(data, &mut cursor, 1)?; // token_mint_y_program_flag
        skip_bytes(data, &mut cursor, 22)?; // _reserved

        Some(Self {
            token_x_mint,
            token_y_mint,
            reserve_x,
            reserve_y,
            oracle,
            bin_array_bitmap,
            pair_type,
            active_id,
            bin_step,
            status,
            activation_point,
            pre_activation_duration,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeteoraDlmmRemainingAccountsType {
    TransferHookX,
    TransferHookY,
    TransferHookReward,
    TransferHookMultiReward(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeteoraDlmmRemainingAccountsSlice {
    pub accounts_type: MeteoraDlmmRemainingAccountsType,
    pub length: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct MeteoraDlmmSwapExtras<'a> {
    pub bin_array_bitmap_extension: Option<Pubkey>,
    pub host_fee_in: Option<Pubkey>,
    pub token_x_program: Pubkey,
    pub token_y_program: Pubkey,
    pub memo_program: Pubkey,
    pub remaining_accounts: &'a [AccountMeta],
    pub remaining_account_slices: &'a [MeteoraDlmmRemainingAccountsSlice],
}

impl<'a> Default for MeteoraDlmmSwapExtras<'a> {
    fn default() -> Self {
        Self {
            bin_array_bitmap_extension: None,
            host_fee_in: None,
            token_x_program: SPL_TOKEN_PROGRAM_ID,
            token_y_program: SPL_TOKEN_PROGRAM_ID,
            memo_program: pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"),
            remaining_accounts: &[],
            remaining_account_slices: &[],
        }
    }
}

/// Builds a Meteora DLMM `swap2` instruction capable of handling token-2022 hooks.
pub fn build_dlmm_swap2_instruction(
    params: &MeteoraDlmmParams,
    lb_pair: Pubkey,
    input_mint: Pubkey,
    output_mint: Pubkey,
    user_input_account: Pubkey,
    user_output_account: Pubkey,
    authority: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
    extras: Option<MeteoraDlmmSwapExtras<'_>>,
) -> Option<Instruction> {
    if amount_in == 0 {
        return None;
    }

    let Some(direction) = map_dlmm_owner_accounts(params, input_mint, output_mint) else {
        warn!(
            pool = %lb_pair,
            %input_mint,
            %output_mint,
            token_x = %params.token_x_mint,
            token_y = %params.token_y_mint,
            "meteora::build_dlmm_swap2_instruction direction mismatch"
        );
        return None;
    };
    let extras = extras.unwrap_or_default();
    validate_dlmm_remaining_accounts(&extras)?;

    let mut accounts = Vec::with_capacity(16 + extras.remaining_accounts.len());
    accounts.push(AccountMeta::new(lb_pair, false));
    if let Some(extension) = extras.bin_array_bitmap_extension {
        accounts.push(AccountMeta::new_readonly(extension, false));
    }
    accounts.push(AccountMeta::new(direction.reserve_in, false));
    accounts.push(AccountMeta::new(direction.reserve_out, false));
    accounts.push(AccountMeta::new(user_input_account, false));
    accounts.push(AccountMeta::new(user_output_account, false));
    accounts.push(AccountMeta::new_readonly(params.token_x_mint, false));
    accounts.push(AccountMeta::new_readonly(params.token_y_mint, false));
    accounts.push(AccountMeta::new(params.oracle, false));
    if let Some(host_fee) = extras.host_fee_in {
        accounts.push(AccountMeta::new(host_fee, false));
    }
    accounts.push(AccountMeta::new_readonly(authority, true));
    accounts.push(AccountMeta::new_readonly(extras.token_x_program, false));
    accounts.push(AccountMeta::new_readonly(extras.token_y_program, false));
    accounts.push(AccountMeta::new_readonly(extras.memo_program, false));
    accounts.push(AccountMeta::new_readonly(dlmm_event_authority(), false));
    accounts.extend(extras.remaining_accounts.iter().cloned());

    let mut data = Vec::with_capacity(8 + 8 + 8 + 8 + extras.remaining_account_slices.len() * 3);
    data.extend_from_slice(&METEORA_DLMM_SWAP2_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    encode_remaining_accounts_info(&mut data, extras.remaining_account_slices)?;

    Some(Instruction {
        program_id: METEORA_DLMM_PROGRAM_ID,
        accounts,
        data,
    })
}

struct MeteoraDlmmDirection {
    reserve_in: Pubkey,
    reserve_out: Pubkey,
}

fn map_dlmm_owner_accounts(
    params: &MeteoraDlmmParams,
    input_mint: Pubkey,
    output_mint: Pubkey,
) -> Option<MeteoraDlmmDirection> {
    if input_mint == params.token_x_mint && output_mint == params.token_y_mint {
        return Some(MeteoraDlmmDirection {
            reserve_in: params.reserve_x,
            reserve_out: params.reserve_y,
        });
    }

    if input_mint == params.token_y_mint && output_mint == params.token_x_mint {
        return Some(MeteoraDlmmDirection {
            reserve_in: params.reserve_y,
            reserve_out: params.reserve_x,
        });
    }

    None
}

fn encode_remaining_accounts_info(
    data: &mut Vec<u8>,
    slices: &[MeteoraDlmmRemainingAccountsSlice],
) -> Option<()> {
    let len = u32::try_from(slices.len()).ok()?;
    data.extend_from_slice(&len.to_le_bytes());
    for slice in slices {
        encode_accounts_type(data, &slice.accounts_type);
        data.push(slice.length);
    }
    Some(())
}

fn encode_accounts_type(data: &mut Vec<u8>, ty: &MeteoraDlmmRemainingAccountsType) {
    match ty {
        MeteoraDlmmRemainingAccountsType::TransferHookX => data.push(0),
        MeteoraDlmmRemainingAccountsType::TransferHookY => data.push(1),
        MeteoraDlmmRemainingAccountsType::TransferHookReward => data.push(2),
        MeteoraDlmmRemainingAccountsType::TransferHookMultiReward(index) => {
            data.push(3);
            data.push(*index);
        }
    }
}

fn validate_dlmm_remaining_accounts(extras: &MeteoraDlmmSwapExtras<'_>) -> Option<()> {
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

fn skip_anchor_discriminator(data: &[u8], cursor: &mut usize, expected: &[u8; 8]) -> Option<()> {
    if *cursor + expected.len() > data.len() {
        return None;
    }
    if &data[*cursor..*cursor + expected.len()] != expected {
        return None;
    }
    *cursor += expected.len();
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

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    if *cursor + 1 > data.len() {
        return None;
    }
    let value = data[*cursor];
    *cursor += 1;
    Some(value)
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Option<u64> {
    if *cursor + 8 > data.len() {
        return None;
    }
    let bytes: [u8; 8] = data[*cursor..*cursor + 8].try_into().ok()?;
    *cursor += 8;
    Some(u64::from_le_bytes(bytes))
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

fn read_u64_array<const N: usize>(data: &[u8], cursor: &mut usize) -> Option<[u64; N]> {
    let mut arr = [0u64; N];
    for item in arr.iter_mut() {
        *item = read_u64(data, cursor)?;
    }
    Some(arr)
}

fn skip_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<()> {
    if *cursor + len > data.len() {
        return None;
    }
    *cursor += len;
    Some(())
}

/// Builds a Meteora DAMM `swap2` instruction for an exact-in trade.
pub fn build_damm_swap2_instruction(
    params: &MeteoraDammParams,
    pool_account: Pubkey,
    input_mint: Pubkey,
    output_mint: Pubkey,
    user_input_account: Pubkey,
    user_output_account: Pubkey,
    payer: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
    referral_token_account: Option<Pubkey>,
) -> Option<Instruction> {
    let valid_direction = (input_mint == params.token_a_mint && output_mint == params.token_b_mint)
        || (input_mint == params.token_b_mint && output_mint == params.token_a_mint);
    if !valid_direction {
        warn!(
            pool = %pool_account,
            %input_mint,
            %output_mint,
            token_a = %params.token_a_mint,
            token_b = %params.token_b_mint,
            "meteora::build_damm_swap2_instruction direction mismatch"
        );
        return None;
    }

    let mut accounts = Vec::with_capacity(14);
    accounts.push(AccountMeta::new_readonly(
        METEORA_DAMM_POOL_AUTHORITY,
        false,
    ));
    accounts.push(AccountMeta::new(pool_account, true));
    accounts.push(AccountMeta::new(user_input_account, false));
    accounts.push(AccountMeta::new(user_output_account, false));
    accounts.push(AccountMeta::new(params.token_a_vault, false));
    accounts.push(AccountMeta::new(params.token_b_vault, false));
    accounts.push(AccountMeta::new_readonly(params.token_a_mint, false));
    accounts.push(AccountMeta::new_readonly(params.token_b_mint, false));
    accounts.push(AccountMeta::new_readonly(payer, true));
    accounts.push(AccountMeta::new_readonly(SPL_TOKEN_PROGRAM_ID, false));
    accounts.push(AccountMeta::new_readonly(SPL_TOKEN_PROGRAM_ID, false));
    if let Some(referral) = referral_token_account {
        accounts.push(AccountMeta::new(referral, false));
    }
    accounts.push(AccountMeta::new_readonly(damm_event_authority(), false));

    let mut data = Vec::with_capacity(8 + 16 + 1);
    data.extend_from_slice(&METEORA_DAMM_SWAP2_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());
    data.push(METEORA_SWAP_MODE_EXACT_IN);

    Some(Instruction {
        program_id: METEORA_DAMM_PROGRAM_ID,
        accounts,
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_params() -> MeteoraDammParams {
        MeteoraDammParams {
            token_a_mint: Pubkey::new_unique(),
            token_b_mint: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            whitelisted_vault: Pubkey::default(),
            partner: Pubkey::default(),
            liquidity: 1,
            sqrt_price_x64: 1,
            activation_point: 0,
            activation_type: 0,
            pool_status: 0,
            token_a_flag: 0,
            token_b_flag: 0,
            collect_fee_mode: 0,
            pool_type: 0,
            version: 0,
        }
    }

    #[test]
    fn swap2_builds_instruction() {
        let params = dummy_params();
        let payer = Pubkey::new_unique();
        let input = Pubkey::new_unique();
        let output = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let ix = build_damm_swap2_instruction(
            &params,
            pool,
            params.token_a_mint,
            params.token_b_mint,
            input,
            output,
            payer,
            42,
            40,
            None,
        )
        .expect("instruction");
        assert_eq!(ix.program_id, METEORA_DAMM_PROGRAM_ID);
        assert_eq!(ix.accounts[0].pubkey, METEORA_DAMM_POOL_AUTHORITY);
        assert_eq!(ix.accounts[2].pubkey, input);
        assert_eq!(ix.accounts[3].pubkey, output);
        assert_eq!(ix.data.len(), 8 + 8 + 8 + 1);
    }

    #[test]
    fn swap2_rejects_mismatched_mints() {
        let mut params = dummy_params();
        let payer = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let res = build_damm_swap2_instruction(
            &params,
            pool,
            Pubkey::new_unique(),
            params.token_b_mint,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            payer,
            1,
            1,
            None,
        );
        assert!(res.is_none());

        let res = build_damm_swap2_instruction(
            &params,
            pool,
            params.token_a_mint,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            payer,
            1,
            1,
            None,
        );
        assert!(res.is_none());
    }
}
