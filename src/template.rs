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
    pub requires_memo: bool,
    pub requires_dont_front: bool,
    pub compute_unit_limit: Option<u32>,
    pub compute_unit_price: Option<u64>,
    pub host_fee_account: Option<Pubkey>,
    pub remaining_accounts: Vec<AccountMeta>,
    pub whirlpool_tick_arrays: Vec<Pubkey>,
    pub whirlpool_remaining_accounts: Vec<WhirlpoolRemainingAccountsSlice>,
    pub slippage_tolerance_bps: Option<u16>,
    pub expected_fee_amount: Option<u64>,
    pub partial_fill_remaining: Option<u64>,
    pub template_ready: bool,
    pub alt_ready: bool,
}

impl Default for TemplateContext {
    fn default() -> Self {
        Self {
            payer: None,
            input_token_account: None,
            output_token_account: None,
            input_token_program: None,
            output_token_program: None,
            requires_memo: false,
            requires_dont_front: true,
            compute_unit_limit: None,
            compute_unit_price: None,
            host_fee_account: None,
            remaining_accounts: Vec::new(),
            whirlpool_tick_arrays: Vec::new(),
            whirlpool_remaining_accounts: Vec::new(),
            slippage_tolerance_bps: None,
            expected_fee_amount: None,
            partial_fill_remaining: None,
            template_ready: false,
            alt_ready: false,
        }
    }
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
