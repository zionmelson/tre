use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};

/// Carries runtime hints from protocol adapters to Stage2/builder so swap templates
/// can be hydrated with protocol-specific requirements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateContext {
    pub payer: Option<Pubkey>,
    pub input_token_account: Option<Pubkey>,
    pub output_token_account: Option<Pubkey>,
    pub input_token_program: Option<Pubkey>,
    pub output_token_program: Option<Pubkey>,
    pub input_transfer_fee_config: Option<TransferFeeConfigHint>,
    pub output_transfer_fee_config: Option<TransferFeeConfigHint>,
    pub requires_memo: bool,
    pub requires_dont_front: bool,
    pub compute_unit_limit: Option<u32>,
    pub compute_unit_price: Option<u64>,
    pub host_fee_account: Option<Pubkey>,
    pub remaining_accounts: Vec<AccountMeta>,
    pub lb_clmm_remaining_accounts: Vec<LbClmmRemainingAccountsSlice>,
    pub lb_clmm_reward_accounts: Vec<LbClmmRewardAccountContext>,
    pub whirlpool_tick_arrays: Vec<Pubkey>,
    pub whirlpool_remaining_accounts: Vec<WhirlpoolRemainingAccountsSlice>,
    pub raydium_tick_array_bitmap_extension: Option<Pubkey>,
    pub raydium_pool_status: Option<u8>,
    pub raydium_protocol_fee_bps: Option<u16>,
    pub raydium_fund_fee_bps: Option<u16>,
    pub slippage_tolerance_bps: Option<u16>,
    pub expected_fee_amount: Option<u64>,
    pub partial_fill_remaining: Option<u64>,
}

impl Default for TemplateContext {
    fn default() -> Self {
        Self {
            payer: None,
            input_token_account: None,
            output_token_account: None,
            input_token_program: None,
            output_token_program: None,
            input_transfer_fee_config: None,
            output_transfer_fee_config: None,
            requires_memo: false,
            requires_dont_front: true,
            compute_unit_limit: None,
            compute_unit_price: None,
            host_fee_account: None,
            remaining_accounts: Vec::new(),
            lb_clmm_remaining_accounts: Vec::new(),
            lb_clmm_reward_accounts: Vec::new(),
            whirlpool_tick_arrays: Vec::new(),
            whirlpool_remaining_accounts: Vec::new(),
            raydium_tick_array_bitmap_extension: None,
            raydium_pool_status: None,
            raydium_protocol_fee_bps: None,
            raydium_fund_fee_bps: None,
            slippage_tolerance_bps: None,
            expected_fee_amount: None,
            partial_fill_remaining: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferFeeConfigHint {
    pub epoch: u64,
    pub maximum_fee: u64,
    pub basis_points: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhirlpoolRemainingAccountsType {
    TransferHookA,
    TransferHookB,
    TransferHookReward,
    TransferHookInput,
    TransferHookIntermediate,
    TransferHookOutput,
    SupplementalTickArrays,
    SupplementalTickArraysOne,
    SupplementalTickArraysTwo,
}

impl WhirlpoolRemainingAccountsType {
    pub fn discriminant(self) -> u8 {
        match self {
            Self::TransferHookA => 0,
            Self::TransferHookB => 1,
            Self::TransferHookReward => 2,
            Self::TransferHookInput => 3,
            Self::TransferHookIntermediate => 4,
            Self::TransferHookOutput => 5,
            Self::SupplementalTickArrays => 6,
            Self::SupplementalTickArraysOne => 7,
            Self::SupplementalTickArraysTwo => 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WhirlpoolRemainingAccountsSlice {
    pub accounts_type: WhirlpoolRemainingAccountsType,
    pub length: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LbClmmAccountsType {
    TransferHookX,
    TransferHookY,
    TransferHookReward,
    TransferHookMultiReward(u8),
}

impl LbClmmAccountsType {
    pub fn discriminant(self) -> u8 {
        match self {
            Self::TransferHookX => 0,
            Self::TransferHookY => 1,
            Self::TransferHookReward => 2,
            Self::TransferHookMultiReward(_) => 3,
        }
    }

    pub fn reward_index(self) -> Option<u8> {
        match self {
            Self::TransferHookMultiReward(index) => Some(index),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LbClmmRemainingAccountsSlice {
    pub pool: Pubkey,
    pub accounts_type: LbClmmAccountsType,
    pub length: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LbClmmRewardAccountContext {
    pub pool: Pubkey,
    pub index: u8,
    pub mint: Pubkey,
    pub vault: Pubkey,
    pub funder: Pubkey,
    pub token_program: Pubkey,
    pub user_token_account: Option<Pubkey>,
    pub transfer_hook_accounts: Vec<AccountMeta>,
}
