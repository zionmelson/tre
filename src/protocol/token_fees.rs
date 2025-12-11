use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022::extension::transfer_fee::TransferFee;

use crate::bootstrap::raydium::TransferFeeConfigSummary;

#[derive(Clone, Copy, Debug)]
pub struct TokenTransferFee {
    fee: TransferFee,
}

impl TokenTransferFee {
    pub fn new(fee: TransferFee) -> Self {
        Self { fee }
    }

    pub fn post_fee_amount(&self, pre_fee_amount: u64) -> Option<(u64, u64)> {
        let fee_amount = self.fee.calculate_fee(pre_fee_amount)?;
        let net = pre_fee_amount.checked_sub(fee_amount)?;
        Some((net, fee_amount))
    }

    pub fn pre_fee_amount(&self, post_fee_amount: u64) -> Option<(u64, u64)> {
        let gross = self.fee.calculate_pre_fee_amount(post_fee_amount)?;
        let fee_amount = gross.checked_sub(post_fee_amount)?;
        Some((gross, fee_amount))
    }

    pub fn config(&self) -> TransferFee {
        self.fee
    }
}

pub fn apply_post_fee_amount(amount: u64, fee: Option<TokenTransferFee>) -> Option<(u64, u64)> {
    match fee {
        Some(fee) => fee.post_fee_amount(amount),
        None => Some((amount, 0)),
    }
}

pub fn apply_pre_fee_amount(amount: u64, fee: Option<TokenTransferFee>) -> Option<(u64, u64)> {
    match fee {
        Some(fee) => fee.pre_fee_amount(amount),
        None => Some((amount, 0)),
    }
}

impl From<TransferFeeConfigSummary> for TokenTransferFee {
    fn from(value: TransferFeeConfigSummary) -> Self {
        let fee = TransferFee {
            epoch: value.epoch.into(),
            maximum_fee: value.maximum_fee.into(),
            transfer_fee_basis_points: value.basis_points.into(),
        };
        TokenTransferFee::new(fee)
    }
}

#[derive(Clone, Copy, Debug)]
struct TransferFeeCacheEntry {
    program_id: Pubkey,
    fee: Option<TokenTransferFee>,
    transfer_hook_program: Option<Pubkey>,
}

impl TransferFeeCacheEntry {
    fn new(
        program_id: Pubkey,
        fee: Option<TokenTransferFee>,
        transfer_hook_program: Option<Pubkey>,
    ) -> Self {
        Self {
            program_id,
            fee,
            transfer_hook_program,
        }
    }
}

#[derive(Default)]
pub struct TokenTransferFeeHydrator {
    cache: DashMap<Pubkey, TransferFeeCacheEntry>,
}

impl TokenTransferFeeHydrator {
    pub fn new(endpoints: &[String], _timeout_ms: u64) -> Option<Self> {
        if endpoints.is_empty() {
            None
        } else {
            Some(Self::default())
        }
    }

    pub fn with_static(
        entries: Vec<(Pubkey, Option<TokenTransferFee>, Option<Pubkey>, Pubkey)>,
    ) -> Self {
        let hydrator = Self::default();
        for (mint, fee, hook_program, program_id) in entries {
            hydrator
                .cache
                .insert(mint, TransferFeeCacheEntry::new(program_id, fee, hook_program));
        }
        hydrator
    }

    pub fn hydrate(&self, mint: Pubkey) -> Option<TokenTransferFee> {
        self.cache.get(&mint).and_then(|entry| entry.fee)
    }

    pub fn mint_program(&self, mint: Pubkey) -> Option<Pubkey> {
        self.cache.get(&mint).map(|entry| entry.program_id)
    }

    pub fn transfer_hook_program(&self, mint: Pubkey) -> Option<Pubkey> {
        self.cache
            .get(&mint)
            .and_then(|entry| entry.transfer_hook_program)
    }

    #[allow(dead_code)]
    pub fn record_static(
        &self,
        mint: Pubkey,
        program_id: Pubkey,
        fee: Option<TokenTransferFee>,
        transfer_hook_program: Option<Pubkey>,
    ) {
        self.cache
            .insert(mint, TransferFeeCacheEntry::new(program_id, fee, transfer_hook_program));
    }
}
