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
            slippage_tolerance_bps: None,
            expected_fee_amount: None,
            partial_fill_remaining: None,
            template_ready: false,
            alt_ready: false,
        }
    }
}
