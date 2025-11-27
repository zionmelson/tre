use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::{anyhow, ensure, Context, Result};
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;

use crate::config::{AppConfig, FlashLoanConfig};

const SOLEND_FLASH_LOAN_FEE_BPS: u64 = 5; // 0.05% per Solend fee schedule
const BASIS_POINTS_DENOMINATOR: u64 = 10_000;
const LENDING_FLASH_LOAN_TAG: u8 = 13;
const SPL_TOKEN_TRANSFER_TAG: u8 = 3;
const SPL_TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

#[derive(Clone)]
pub struct FlashLoanPlanner {
    state: Arc<PlannerState>,
}

struct PlannerState {
    accounts: HashMap<String, FlashLoanAccounts>,
    accounts_by_mint: HashMap<Pubkey, FlashLoanAccounts>,
    route_keys: Vec<String>,
    prepared_routes: AtomicU64,
    outstanding_borrow_lamports: AtomicU64,
    issued_plan_seq: AtomicU64,
}

impl FlashLoanPlanner {
    pub fn new(config: Arc<AppConfig>) -> Result<Self> {
        ensure!(
            !config.flash_loans.is_empty(),
            "flash_loans config must contain at least one entry"
        );

        let mut accounts: HashMap<String, FlashLoanAccounts> = HashMap::new();
        let mut accounts_by_mint: HashMap<Pubkey, FlashLoanAccounts> = HashMap::new();
        for (token, entry) in &config.flash_loans {
            let parsed = FlashLoanAccounts::try_from(entry)
                .with_context(|| format!("invalid flash loan entry for token {token}"))?;
            accounts.insert(token.clone(), parsed.clone());
            accounts_by_mint.insert(parsed.reserve_liquidity_mint, parsed);
        }

        let mut route_keys: Vec<String> = accounts.keys().cloned().collect();
        route_keys.sort();

        Ok(Self {
            state: Arc::new(PlannerState {
                accounts,
                accounts_by_mint,
                route_keys,
                prepared_routes: AtomicU64::new(0),
                outstanding_borrow_lamports: AtomicU64::new(0),
                issued_plan_seq: AtomicU64::new(0),
            }),
        })
    }

    pub fn prepare_default_routes(&self) {
        let routes = self.state.route_keys.len() as u64;
        if routes == 0 {
            return;
        }
        self.state
            .prepared_routes
            .fetch_add(routes, Ordering::Relaxed);
    }

    pub fn prepared_route_count(&self) -> u64 {
        self.state.prepared_routes.load(Ordering::Relaxed)
    }

    pub fn outstanding_borrow_lamports(&self) -> u64 {
        self.state
            .outstanding_borrow_lamports
            .load(Ordering::Relaxed)
    }

    pub fn plan_route(&self, token: &str, borrow_amount: u64) -> Result<FlashLoanRoutePlan> {
        let accounts = self
            .state
            .accounts
            .get(token)
            .ok_or_else(|| anyhow!("unknown flash-loan token: {token}"))?
            .clone();
        self.plan_with_accounts(token.to_string(), accounts, borrow_amount)
    }

    pub fn plan_route_by_mint(
        &self,
        mint: &Pubkey,
        borrow_amount: u64,
    ) -> Result<FlashLoanRoutePlan> {
        let accounts = self
            .state
            .accounts_by_mint
            .get(mint)
            .ok_or_else(|| anyhow!("unknown flash-loan mint: {mint}"))?
            .clone();
        self.plan_with_accounts(mint.to_string(), accounts, borrow_amount)
    }

    pub fn supports_mint(&self, mint: &Pubkey) -> bool {
        self.state.accounts_by_mint.contains_key(mint)
    }

    pub fn release_route(&self, plan: &FlashLoanRoutePlan) {
        self.state
            .outstanding_borrow_lamports
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(plan.borrow_amount)
            })
            .ok();
    }

    fn plan_with_accounts(
        &self,
        token_key: String,
        accounts: FlashLoanAccounts,
        borrow_amount: u64,
    ) -> Result<FlashLoanRoutePlan> {
        ensure!(
            borrow_amount > 0,
            "flash-loan borrow amount must be greater than zero"
        );
        let fee_amount = compute_fee(borrow_amount);
        let repay_amount = borrow_amount
            .checked_add(fee_amount)
            .ok_or_else(|| anyhow!("flash-loan repayment overflowed u64"))?;

        let plan_id = self
            .state
            .issued_plan_seq
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        self.state
            .outstanding_borrow_lamports
            .fetch_add(borrow_amount, Ordering::Relaxed);

        Ok(FlashLoanRoutePlan {
            id: plan_id,
            borrow_amount,
            fee_amount,
            repay_amount,
            token: token_key,
            legs: [
                FlashLoanLegPlan {
                    kind: FlashLoanLegKind::Borrow,
                    amount: borrow_amount,
                    liquidity_mint: accounts.reserve_liquidity_mint,
                },
                FlashLoanLegPlan {
                    kind: FlashLoanLegKind::Repay,
                    amount: repay_amount,
                    liquidity_mint: accounts.reserve_liquidity_mint,
                },
            ],
            accounts,
        })
    }
}

fn compute_fee(amount: u64) -> u64 {
    if amount == 0 {
        return 0;
    }
    let numerator = amount.saturating_mul(SOLEND_FLASH_LOAN_FEE_BPS);
    (numerator + (BASIS_POINTS_DENOMINATOR - 1)) / BASIS_POINTS_DENOMINATOR
}

#[derive(Clone, Debug)]
pub struct FlashLoanAccounts {
    pub program_id: Pubkey,
    pub lending_market: Pubkey,
    pub lending_market_authority: Pubkey,
    pub reserve: Pubkey,
    pub reserve_liquidity_mint: Pubkey,
    pub reserve_liquidity_supply: Pubkey,
    pub reserve_liquidity_fee_receiver: Pubkey,
    pub host_fee_receiver: Pubkey,
    pub allowed_receiver_programs: Vec<Pubkey>,
}

impl TryFrom<&FlashLoanConfig> for FlashLoanAccounts {
    type Error = anyhow::Error;

    fn try_from(config: &FlashLoanConfig) -> Result<Self> {
        let program_id = parse_pubkey("flash_loan.program_id", &config.program_id)?;
        let lending_market = parse_pubkey("flash_loan.lending_market", &config.lending_market)?;
        let (authority, _) = Pubkey::find_program_address(&[lending_market.as_ref()], &program_id);
        Ok(Self {
            program_id,
            lending_market,
            lending_market_authority: authority,
            reserve: parse_pubkey("flash_loan.reserve", &config.reserve)?,
            reserve_liquidity_mint: parse_pubkey(
                "flash_loan.reserve_liquidity_mint",
                &config.reserve_liquidity_mint,
            )?,
            reserve_liquidity_supply: parse_pubkey(
                "flash_loan.reserve_liquidity_supply",
                &config.reserve_liquidity_supply,
            )?,
            reserve_liquidity_fee_receiver: parse_pubkey(
                "flash_loan.reserve_liquidity_fee_receiver",
                &config.reserve_liquidity_fee_receiver,
            )?,
            host_fee_receiver: parse_pubkey(
                "flash_loan.host_fee_receiver",
                &config.host_fee_receiver,
            )?,
            allowed_receiver_programs: parse_allowlist(&config.receiver_program_allowlist)?,
        })
    }
}

fn parse_pubkey(label: &str, value: &str) -> Result<Pubkey> {
    Pubkey::from_str(value).with_context(|| format!("invalid pubkey for {label}: {value}"))
}

fn parse_allowlist(entries: &[String]) -> Result<Vec<Pubkey>> {
    let mut programs = Vec::with_capacity(entries.len());
    for value in entries {
        programs.push(parse_pubkey(
            "flash_loan.receiver_program_allowlist",
            value,
        )?);
    }
    Ok(programs)
}

#[derive(Clone, Debug)]
pub struct FlashLoanRoutePlan {
    pub id: u64,
    pub token: String,
    pub borrow_amount: u64,
    pub fee_amount: u64,
    pub repay_amount: u64,
    pub legs: [FlashLoanLegPlan; 2],
    pub accounts: FlashLoanAccounts,
}

impl FlashLoanRoutePlan {
    pub fn total_repayment(&self) -> u64 {
        self.repay_amount
    }

    pub fn borrow_instruction(
        &self,
        destination_liquidity: Pubkey,
        receiver_program_id: Pubkey,
        receiver_accounts: &[AccountMeta],
    ) -> Instruction {
        self.accounts.borrow_instruction(
            self.borrow_amount,
            destination_liquidity,
            receiver_program_id,
            receiver_accounts,
        )
    }

    pub fn repay_instruction(
        &self,
        source_liquidity: Pubkey,
        authority: Pubkey,
    ) -> Result<Instruction> {
        self.accounts
            .repay_instruction(self.repay_amount, source_liquidity, authority)
    }

    pub fn allows_receiver_program(&self, program: &Pubkey) -> bool {
        self.accounts.allows_receiver_program(program)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlashLoanLegKind {
    Borrow,
    Repay,
}

#[derive(Clone, Copy, Debug)]
pub struct FlashLoanLegPlan {
    pub kind: FlashLoanLegKind,
    pub amount: u64,
    pub liquidity_mint: Pubkey,
}

impl FlashLoanAccounts {
    pub fn borrow_instruction(
        &self,
        amount: u64,
        destination_liquidity: Pubkey,
        receiver_program_id: Pubkey,
        receiver_accounts: &[AccountMeta],
    ) -> Instruction {
        // Solend validates the receiver program via this readonly account slot, so ensure callers
        // only attempt programs that are explicitly allowed (or allow all when list is empty).
        let mut accounts = vec![
            AccountMeta::new(self.reserve_liquidity_supply, false),
            AccountMeta::new(destination_liquidity, false),
            AccountMeta::new(self.reserve, false),
            AccountMeta::new(self.reserve_liquidity_fee_receiver, false),
            AccountMeta::new(self.host_fee_receiver, false),
            AccountMeta::new_readonly(self.lending_market, false),
            AccountMeta::new_readonly(self.lending_market_authority, false),
            AccountMeta::new_readonly(SPL_TOKEN_PROGRAM_ID, false),
            AccountMeta::new_readonly(receiver_program_id, false),
        ];
        accounts.extend(receiver_accounts.iter().cloned());

        let mut data = Vec::with_capacity(1 + std::mem::size_of::<u64>());
        data.push(LENDING_FLASH_LOAN_TAG);
        data.extend_from_slice(&amount.to_le_bytes());

        Instruction {
            program_id: self.program_id,
            accounts,
            data,
        }
    }

    pub fn repay_instruction(
        &self,
        amount: u64,
        source_liquidity: Pubkey,
        authority: Pubkey,
    ) -> Result<Instruction> {
        let mut data = Vec::with_capacity(1 + std::mem::size_of::<u64>());
        data.push(SPL_TOKEN_TRANSFER_TAG);
        data.extend_from_slice(&amount.to_le_bytes());

        Ok(Instruction {
            program_id: SPL_TOKEN_PROGRAM_ID,
            accounts: vec![
                AccountMeta::new(source_liquidity, false),
                AccountMeta::new(self.reserve_liquidity_supply, false),
                AccountMeta::new_readonly(authority, true),
            ],
            data,
        })
    }

    pub fn allows_receiver_program(&self, program: &Pubkey) -> bool {
        if self.allowed_receiver_programs.is_empty() {
            return true;
        }
        self.allowed_receiver_programs
            .iter()
            .any(|allowed| allowed == program)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use solana_sdk::instruction::AccountMeta;

    fn test_config() -> Arc<AppConfig> {
        test_config_with_allowlist(Vec::new())
    }

    fn test_config_with_allowlist(programs: Vec<&str>) -> Arc<AppConfig> {
        let mut raw = json!({
            "wallets": {
                "auth": {"path": "/tmp/auth.json"},
                "payer": {"path": "/tmp/payer.json"}
            },
            "jito": {
                "endpoints": ["https://example"],
                "tip_floor_url": "https://example/floor",
                "tip_stream_url": "https://example/stream"
            },
            "corecast": {
                "address": "example",
                "authorization": "token",
                "cache": {"warm_capacity": 1, "channel_capacity": 1}
            },
            "tokens": {},
            "token_pairs": [ [
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
            ]],
            "program_addresses": [],
            "flash_loans": {
                "SOL": {
                    "program_id": "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo",
                    "lending_market": "4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY",
                    "reserve": "8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36",
                    "reserve_liquidity_mint": "So11111111111111111111111111111111111111112",
                    "reserve_liquidity_supply": "8UviNr47S8eL6J3WfDxMRa3hvLta1VDJwNWqsDgtN3Cv",
                    "reserve_liquidity_fee_receiver": "5wo1tFpi4HaVKnemqaXeQnBEpezrJXcXvuztYaPhvgC7",
                    "host_fee_receiver": "FrGB9HC1vEjUhWbUqfQkdKCuuU58bL843AZPQ21LWudu"
                }
            },
            "quicknode_rpcs": ["https://example"],
            "protocols": {},
            "perf": {
                "ingest_core": 0,
                "eval_core": 1,
                "submit_core": 2,
                "ring_capacity": 8,
                "opportunity_slab": 8,
                "bundle_slab": 8
            }
        });
        if !programs.is_empty() {
            raw["flash_loans"]["SOL"]["receiver_program_allowlist"] =
                serde_json::Value::Array(programs.into_iter().map(|p| json!(p)).collect());
        }
        Arc::new(serde_json::from_value(raw).expect("app config"))
    }

    #[test]
    fn plans_parameterized_route_and_tracks_outstanding() {
        let planner = FlashLoanPlanner::new(test_config()).expect("planner");
        assert_eq!(planner.outstanding_borrow_lamports(), 0);
        let plan = planner.plan_route("SOL", 1_000_000).expect("route");
        assert_eq!(plan.borrow_amount, 1_000_000);
        assert_eq!(plan.legs[0].kind, FlashLoanLegKind::Borrow);
        assert_eq!(planner.outstanding_borrow_lamports(), 1_000_000);
        planner.release_route(&plan);
        assert_eq!(planner.outstanding_borrow_lamports(), 0);
    }

    #[test]
    fn rejects_zero_borrow_amount() {
        let planner = FlashLoanPlanner::new(test_config()).expect("planner");
        assert!(planner.plan_route("SOL", 0).is_err());
    }

    #[test]
    fn builds_flash_loan_borrow_and_repay_instructions() {
        let planner = FlashLoanPlanner::new(test_config()).expect("planner");
        let plan = planner.plan_route("SOL", 1_000_000).expect("route");
        let destination = Pubkey::new_unique();
        let receiver_program = Pubkey::new_unique();
        let extra_account = Pubkey::new_unique();
        let receiver_accounts = vec![AccountMeta::new(extra_account, false)];
        let borrow_ix = plan.borrow_instruction(destination, receiver_program, &receiver_accounts);
        assert_eq!(borrow_ix.program_id, plan.accounts.program_id);
        assert_eq!(borrow_ix.data[0], LENDING_FLASH_LOAN_TAG);
        let amount = u64::from_le_bytes(
            borrow_ix.data[1..1 + std::mem::size_of::<u64>()]
                .try_into()
                .expect("amount bytes"),
        );
        assert_eq!(amount, plan.borrow_amount);
        assert_eq!(borrow_ix.accounts.len(), 9 + receiver_accounts.len());
        assert_eq!(borrow_ix.accounts.last().unwrap().pubkey, extra_account);

        let authority = Pubkey::new_unique();
        let repay_ix = plan
            .repay_instruction(destination, authority)
            .expect("repay instruction");
        assert_eq!(repay_ix.program_id, SPL_TOKEN_PROGRAM_ID);
        assert_eq!(repay_ix.data[0], SPL_TOKEN_TRANSFER_TAG);
        let repay_amount = u64::from_le_bytes(
            repay_ix.data[1..1 + std::mem::size_of::<u64>()]
                .try_into()
                .expect("repay amount bytes"),
        );
        assert_eq!(repay_amount, plan.repay_amount);
    }

    #[test]
    fn plans_route_by_mint_lookup() {
        let planner = FlashLoanPlanner::new(test_config()).expect("planner");
        let mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        assert!(planner.supports_mint(&mint));
        let plan = planner
            .plan_route_by_mint(&mint, 2_500_000)
            .expect("plan by mint");
        assert_eq!(plan.borrow_amount, 2_500_000);
        assert_eq!(plan.token, mint.to_string());
    }

    #[test]
    fn enforces_receiver_program_allowlists() {
        let allowed_program = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
        let planner = FlashLoanPlanner::new(test_config_with_allowlist(vec![allowed_program]))
            .expect("planner");
        let plan = planner.plan_route("SOL", 500_000).expect("route");
        let allowed = Pubkey::from_str(allowed_program).expect("allowed program");
        assert!(plan.allows_receiver_program(&allowed));
        let disallowed = Pubkey::new_unique();
        assert!(!plan.allows_receiver_program(&disallowed));
    }
}
