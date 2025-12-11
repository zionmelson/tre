use std::time::Instant;

use solana_sdk::pubkey::Pubkey;

use crate::watch::types::{LbClmmBinArrayExtras, LbClmmBitmapExtras};

use super::raydium::MintAccountMeta;

#[derive(Debug, Clone)]
pub struct LbClmmStaticSnapshot {
    pub lb_pair: Pubkey,
    pub program_id: Pubkey,
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub meta: HydratedPoolMeta,
    pub token_x_mint_meta: Option<MintAccountMeta>,
    pub token_y_mint_meta: Option<MintAccountMeta>,
    pub recorded_at: Instant,
}

#[derive(Debug, Clone)]
pub struct HydratedPoolMeta {
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub reserve_x: Pubkey,
    pub reserve_y: Pubkey,
    pub oracle: Pubkey,
    pub token_x_program: Option<Pubkey>,
    pub token_y_program: Option<Pubkey>,
    pub bin_step: u16,
    pub active_bin_id: i32,
    pub bin_arrays: Vec<LbClmmBinArrayExtras>,
    pub active_bitmap: LbClmmBitmapExtras,
    pub rewards: Vec<HydratedRewardInfo>,
    pub hydrated_slot: u64,
    pub hydrated_at: Instant,
}

#[derive(Debug, Clone)]
pub struct HydratedRewardInfo {
    pub index: u8,
    pub mint: Pubkey,
    pub vault: Pubkey,
    pub funder: Pubkey,
}
