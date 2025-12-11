use std::time::Instant;

use solana_sdk::pubkey::Pubkey;

use crate::watch::types::WhirlpoolAdaptiveFeeInfo;

use super::raydium::MintAccountMeta;

#[derive(Debug, Clone)]
pub struct WhirlpoolStaticSnapshot {
    pub whirlpool: Pubkey,
    pub program_id: Pubkey,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub meta: HydratedPoolMeta,
    pub tick_arrays: Vec<HydratedTickArray>,
    pub token_a_mint_meta: Option<MintAccountMeta>,
    pub token_b_mint_meta: Option<MintAccountMeta>,
    pub token_a_transfer_hook_program: Option<Pubkey>,
    pub token_b_transfer_hook_program: Option<Pubkey>,
    pub recorded_at: Instant,
}

#[derive(Debug, Clone)]
pub struct HydratedPoolMeta {
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub token_program_a: Pubkey,
    pub token_program_b: Pubkey,
    pub tick_spacing: i32,
    pub liquidity: u128,
    pub sqrt_price_x64: u128,
    pub tick_current_index: i32,
    pub fee_rate: u16,
    pub protocol_fee_rate: u16,
    pub oracle: Pubkey,
    pub trade_enable_timestamp: Option<u64>,
    pub adaptive_fee: Option<WhirlpoolAdaptiveFeeInfo>,
    pub hydrated_slot: u64,
    pub hydrated_at: Instant,
}

#[derive(Debug, Clone)]
pub struct HydratedTickArray {
    pub address: Pubkey,
    pub start_tick_index: i32,
    pub ticks: Vec<HydratedTick>,
}

#[derive(Debug, Clone)]
pub struct HydratedTick {
    pub initialized: bool,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
}
