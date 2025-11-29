use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};

/// Carries runtime hints from protocol adapters to Stage2/builder so swap templates
/// can be hydrated with protocol-specific requirements.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TemplateContext {
    pub payer: Option<Pubkey>,
    pub input_token_account: Option<Pubkey>,
    pub output_token_account: Option<Pubkey>,
    pub input_token_program: Option<Pubkey>,
    pub output_token_program: Option<Pubkey>,
    pub requires_memo: bool,
    pub compute_unit_limit: Option<u32>,
    pub host_fee_account: Option<Pubkey>,
    pub remaining_accounts: Vec<AccountMeta>,
    pub whirlpool_tick_arrays: Vec<Pubkey>,
    pub slippage_tolerance_bps: Option<u16>,
    pub template_ready: bool,
    pub alt_ready: bool,
}
