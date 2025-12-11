use std::time::Instant;

use solana_sdk::pubkey::Pubkey;

use crate::watch::types::CpAmmPoolFeesExtras;

use super::raydium::MintAccountMeta;

#[derive(Debug, Clone)]
pub struct CpAmmStaticSnapshot {
    pub pool_id: Pubkey,
    pub program_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub meta: HydratedPoolMeta,
    pub base_mint_meta: Option<MintAccountMeta>,
    pub quote_mint_meta: Option<MintAccountMeta>,
    pub recorded_at: Instant,
}

#[derive(Debug, Clone)]
pub struct HydratedPoolMeta {
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    pub token_a_vault: Pubkey,
    pub token_b_vault: Pubkey,
    pub whitelisted_vault: Pubkey,
    pub partner: Pubkey,
    pub sqrt_price_x64: u128,
    pub liquidity: u128,
    pub sqrt_min_price_x64: u128,
    pub sqrt_max_price_x64: u128,
    pub permanent_lock_liquidity: u128,
    pub collect_fee_mode: u8,
    pub activation_point: u64,
    pub activation_type: u8,
    pub pool_status: u8,
    pub pool_type: u8,
    pub pool_version: u8,
    pub token_a_flag: u8,
    pub token_b_flag: u8,
    pub pool_fees: CpAmmPoolFeesExtras,
    pub hydrated_at: Instant,
    pub hydrated_slot: u64,
}
