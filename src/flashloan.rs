use crate::config::SolendFlashLoanMarket;
use solana_sdk::{instruction::Instruction, pubkey, pubkey::Pubkey};
use solend::{flash_borrow_instruction, flash_repay_instruction};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Canonical Solend main market authority derived on-chain for `4UpD…`.
const SOLEND_MAIN_MARKET_AUTHORITY: Pubkey =
    pubkey!("DdZR6zRFiUt4S5mg7AV1uKB2z1f1WzcNYCaTEEWPAuby");

/// Snapshot of a Solend flash-loan market derived from config for fast runtime access.
#[derive(Debug, Clone)]
pub struct FlashLoanSpec {
    label: Arc<String>,
    program_id: Pubkey,
    lending_market: Pubkey,
    lending_market_authority: Pubkey,
    reserve: Pubkey,
    reserve_liquidity_mint: Pubkey,
    borrow_reserve_liquidity_supply: Pubkey,
    repay_reserve_liquidity_supply: Pubkey,
    reserve_liquidity_fee_receiver: Pubkey,
    host_fee_receiver: Pubkey,
    allowed_programs: Arc<HashSet<Pubkey>>,
}

/// Registry mapping reserve mints to their Solend flash-loan markets.
#[derive(Debug, Clone, Default)]
pub struct FlashLoanBook {
    markets: HashMap<Pubkey, Arc<FlashLoanSpec>>,
}

impl FlashLoanBook {
    /// Builds the registry from configuration.
    pub fn from_config(entries: &BTreeMap<String, SolendFlashLoanMarket>) -> Self {
        let mut markets = HashMap::new();
        for (label, cfg) in entries {
            let spec = Arc::new(FlashLoanSpec::from_config(label, cfg));
            if let Some(prev) = markets.insert(cfg.reserve_liquidity_mint, spec.clone()) {
                tracing::warn!(
                    reserve_mint = %cfg.reserve_liquidity_mint,
                    old = %prev.label(),
                    new = %label,
                    "duplicate solend flash-loan market for mint; overriding"
                );
            }
        }
        Self { markets }
    }

    /// Looks up the market spec for a given reserve mint.
    pub fn get(&self, mint: &Pubkey) -> Option<Arc<FlashLoanSpec>> {
        self.markets.get(mint).cloned()
    }

    /// Returns the number of configured markets.
    pub fn len(&self) -> usize {
        self.markets.len()
    }

    /// Returns true if no markets are configured.
    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }
}

/// Borrow + repay Solend instructions bundled for a plan.
#[derive(Debug, Clone)]
pub struct FlashLoanInstructions {
    pub borrow: Instruction,
    pub repay: Instruction,
}

impl FlashLoanSpec {
    fn from_config(label: &str, cfg: &SolendFlashLoanMarket) -> Self {
        let authority = SOLEND_MAIN_MARKET_AUTHORITY;
        let allowed_programs: HashSet<Pubkey> =
            cfg.receiver_program_allowlist.iter().copied().collect();
        Self {
            label: Arc::new(label.to_string()),
            program_id: cfg.program_id,
            lending_market: cfg.lending_market,
            lending_market_authority: authority,
            reserve: cfg.reserve,
            reserve_liquidity_mint: cfg.reserve_liquidity_mint,
            borrow_reserve_liquidity_supply: cfg.borrow.reserve_liquidity_supply,
            repay_reserve_liquidity_supply: cfg.repay.reserve_liquidity_supply,
            reserve_liquidity_fee_receiver: cfg.repay.reserve_liquidity_fee_receiver,
            host_fee_receiver: cfg.repay.host_fee_receiver,
            allowed_programs: Arc::new(allowed_programs),
        }
    }

    /// Human-readable label pulled from config for diagnostics.
    pub fn label(&self) -> &str {
        self.label.as_str()
    }

    /// Reserve mint for this market.
    pub fn reserve_liquidity_mint(&self) -> Pubkey {
        self.reserve_liquidity_mint
    }

    /// Returns true if the provided program id is allow-listed for flash-loan receivers.
    pub fn allows_program(&self, program: &Pubkey) -> bool {
        if self.allowed_programs.is_empty() {
            return true;
        }
        self.allowed_programs.contains(program)
    }

    /// Ensures every provided program id is allow-listed.
    ///
    /// Returns `Err(program)` containing the first violating program when blocked.
    pub fn ensure_programs_allowed<I>(&self, programs: I) -> Result<(), Pubkey>
    where
        I: IntoIterator<Item = Pubkey>,
    {
        if self.allowed_programs.is_empty() {
            return Ok(());
        }
        for program in programs {
            if !self.allowed_programs.contains(&program) {
                return Err(program);
            }
        }
        Ok(())
    }

    /// Synthesizes borrow + repay instructions for the requested amount and token accounts.
    pub fn build_instructions(
        &self,
        amount: u64,
        destination_liquidity: Pubkey,
        source_liquidity: Pubkey,
        user_authority: Pubkey,
        borrow_instruction_index: u8,
    ) -> FlashLoanInstructions {
        let borrow = flash_borrow_instruction(
            self.program_id,
            amount,
            self.borrow_reserve_liquidity_supply,
            destination_liquidity,
            self.reserve,
            self.lending_market,
            self.lending_market_authority,
        );

        let repay = flash_repay_instruction(
            self.program_id,
            amount,
            source_liquidity,
            self.repay_reserve_liquidity_supply,
            self.reserve_liquidity_fee_receiver,
            self.host_fee_receiver,
            self.reserve,
            self.lending_market,
            user_authority,
            borrow_instruction_index,
        );

        FlashLoanInstructions { borrow, repay }
    }
}
