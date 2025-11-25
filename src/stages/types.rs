use std::str::FromStr;
use std::time::Instant;

use once_cell::sync::Lazy;
use solana_sdk::pubkey::Pubkey;

use crate::proto::solana_corecast::{DexTradeStreamMessage, PoolLiquidityChangeStreamMessage};
use crate::proto::solana_messages::{
    Account as ProtoAccount, Currency, ParsedIdlInstruction, PoolLiquidityChangeEvent, Program,
};

#[derive(Debug, Clone)]
pub struct PoolUpdate {
    pub seq: u64,
    pub slot: u64,
    pub program_id: String,
    pub pool_id: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_post_amount: u128,
    pub quote_post_amount: u128,
    pub base_change: i128,
    pub quote_change: i128,
    pub extensions: PoolUpdateExtensions,
}

impl PoolUpdate {
    pub fn from_corecast(seq: u64, message: &PoolLiquidityChangeStreamMessage) -> Option<Self> {
        let block = message.block.as_ref()?;
        let event = message.pool_event.as_ref()?;
        let dex = event.dex.as_ref()?;
        let market = event.market.as_ref()?;
        let base_currency = market.base_currency.as_ref()?;
        let quote_currency = market.quote_currency.as_ref()?;
        let base_side = event.base_currency.as_ref()?;
        let quote_side = event.quote_currency.as_ref()?;

        let program_id = encode_pubkey(&dex.program_address);
        let pool_id = encode_pubkey(&market.market_address);
        if program_id.is_empty() || pool_id.is_empty() {
            return None;
        }

        Some(Self {
            seq,
            slot: block.slot,
            program_id,
            pool_id,
            base_mint: encode_pubkey(&base_currency.mint_address),
            quote_mint: encode_pubkey(&quote_currency.mint_address),
            base_post_amount: base_side.post_amount as u128,
            quote_post_amount: quote_side.post_amount as u128,
            base_change: base_side.change_amount as i128,
            quote_change: quote_side.change_amount as i128,
            extensions: PoolUpdateExtensions::from_pool_event(event),
        })
    }

    pub fn into_snapshot(self) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: self.pool_id,
            program_id: self.program_id,
            slot: self.slot,
            base_mint: self.base_mint,
            quote_mint: self.quote_mint,
            base_post_amount: self.base_post_amount,
            quote_post_amount: self.quote_post_amount,
            updated_at: Instant::now(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PoolUpdateExtensions {
    pub instruction: Option<ParsedIdlInstruction>,
    pub raydium_v3: Option<RaydiumV3PoolExtras>,
    pub cp_amm: Option<CpAmmPoolExtras>,
    pub lb_clmm: Option<LbClmmPoolExtras>,
    pub lifinity_v2: Option<LifinityPoolExtras>,
    pub whirlpool: Option<WhirlpoolPoolExtras>,
}

impl PoolUpdateExtensions {
    fn from_pool_event(event: &PoolLiquidityChangeEvent) -> Self {
        let raydium_v3 = parse_raydium_v3_extras(event);
        let cp_amm = parse_cp_amm_extras(event);
        Self {
            instruction: event.instruction.clone(),
            raydium_v3,
            cp_amm,
            lb_clmm: None,
            lifinity_v2: None,
            whirlpool: None,
        }
    }
}

const CP_AMM_PROGRAM_ID: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
static CP_AMM_PROGRAM_PUBKEY: Lazy<Pubkey> =
    Lazy::new(|| Pubkey::from_str(CP_AMM_PROGRAM_ID).expect("static cp_amm program id"));

const RAYDIUM_V3_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
static RAYDIUM_V3_PROGRAM_PUBKEY: Lazy<Pubkey> =
    Lazy::new(|| Pubkey::from_str(RAYDIUM_V3_PROGRAM_ID).expect("static Raydium v3 program id"));
const POOL_SEED: &[u8] = b"pool";
const OBSERVATION_SEED: &[u8] = b"observation";
const VAULT_SEED: &[u8] = b"pool_vault";

fn parse_raydium_v3_extras(event: &PoolLiquidityChangeEvent) -> Option<RaydiumV3PoolExtras> {
    let dex = event.dex.as_ref()?;
    let program_key = pubkey_from_bytes(&dex.program_address)?;
    if program_key != *RAYDIUM_V3_PROGRAM_PUBKEY {
        return None;
    }

    let instruction = event.instruction.as_ref()?;
    let program = instruction.program.as_ref()?;
    let account_lookup = InstructionAccountLookup::new(program, &instruction.accounts);

    let mut extras = RaydiumV3PoolExtras::default();
    extras.amm_config = account_lookup.pubkey_with_aliases(&["ammConfig", "amm_config"]);
    extras.observation_state =
        account_lookup.pubkey_with_aliases(&["observationState", "observation_state"]);

    let market = match event.market.as_ref() {
        Some(market) => market,
        None => return extras_if_any(extras),
    };
    let pool_pubkey = pubkey_from_bytes(&market.market_address);
    let base_mint = currency_mint(market.base_currency.as_ref());
    let quote_mint = currency_mint(market.quote_currency.as_ref());

    if let (Some(amm_config), Some(pool_pubkey), Some(base_mint), Some(quote_mint)) = (
        extras.amm_config.as_ref(),
        pool_pubkey,
        base_mint,
        quote_mint,
    ) {
        let (mint0, mint1) = order_mints(&base_mint, &quote_mint);
        let (derived_pool, pool_bump) = Pubkey::find_program_address(
            &[
                POOL_SEED,
                amm_config.as_ref(),
                mint0.as_ref(),
                mint1.as_ref(),
            ],
            &RAYDIUM_V3_PROGRAM_PUBKEY,
        );
        if derived_pool == pool_pubkey {
            extras.pool_bump = Some(pool_bump);
        }

        let (observation_key, observation_bump) = Pubkey::find_program_address(
            &[OBSERVATION_SEED, pool_pubkey.as_ref()],
            &RAYDIUM_V3_PROGRAM_PUBKEY,
        );
        extras.observation_bump = Some(observation_bump);
        extras.observation_state = extras.observation_state.or(Some(observation_key));

        let (_, vault0_bump) = Pubkey::find_program_address(
            &[VAULT_SEED, pool_pubkey.as_ref(), mint0.as_ref()],
            &RAYDIUM_V3_PROGRAM_PUBKEY,
        );
        let (_, vault1_bump) = Pubkey::find_program_address(
            &[VAULT_SEED, pool_pubkey.as_ref(), mint1.as_ref()],
            &RAYDIUM_V3_PROGRAM_PUBKEY,
        );
        extras.vault_bumps = Some([vault0_bump, vault1_bump]);
    }

    extras_if_any(extras)
}

fn extras_if_any(extras: RaydiumV3PoolExtras) -> Option<RaydiumV3PoolExtras> {
    if extras.amm_config.is_none()
        && extras.observation_state.is_none()
        && extras.pool_bump.is_none()
        && extras.observation_bump.is_none()
        && extras.vault_bumps.is_none()
    {
        None
    } else {
        Some(extras)
    }
}

fn parse_cp_amm_extras(event: &PoolLiquidityChangeEvent) -> Option<CpAmmPoolExtras> {
    let dex = event.dex.as_ref()?;
    let program_key = pubkey_from_bytes(&dex.program_address)?;
    if program_key != *CP_AMM_PROGRAM_PUBKEY {
        return None;
    }

    let instruction = event.instruction.as_ref()?;
    let program = instruction.program.as_ref()?;
    let account_lookup = InstructionAccountLookup::new(program, &instruction.accounts);

    let market = event.market.as_ref();
    let base_currency = market.and_then(|m| m.base_currency.as_ref());
    let quote_currency = market.and_then(|m| m.quote_currency.as_ref());

    let mut extras = CpAmmPoolExtras::default();
    extras.pool_authority =
        lookup_account_string(&account_lookup, &["pool_authority", "poolAuthority"]);
    extras.token_a_vault =
        lookup_account_string(&account_lookup, &["token_a_vault", "tokenAVault"]);
    extras.token_b_vault =
        lookup_account_string(&account_lookup, &["token_b_vault", "tokenBVault"]);
    extras.token_a_mint = lookup_account_string(&account_lookup, &["token_a_mint", "tokenAMint"])
        .or_else(|| currency_mint(base_currency).map(|pk| pk.to_string()));
    extras.token_b_mint = lookup_account_string(&account_lookup, &["token_b_mint", "tokenBMint"])
        .or_else(|| currency_mint(quote_currency).map(|pk| pk.to_string()));
    extras.token_a_program =
        lookup_account_string(&account_lookup, &["token_a_program", "tokenAProgram"])
            .or_else(|| currency_program(base_currency).map(|pk| pk.to_string()));
    extras.token_b_program =
        lookup_account_string(&account_lookup, &["token_b_program", "tokenBProgram"])
            .or_else(|| currency_program(quote_currency).map(|pk| pk.to_string()));
    extras.referral_token_account = lookup_account_string(
        &account_lookup,
        &["referral_token_account", "referralTokenAccount"],
    );

    Some(extras)
}

fn lookup_account_string(lookup: &InstructionAccountLookup, aliases: &[&str]) -> Option<String> {
    lookup.pubkey_with_aliases(aliases).map(|pk| pk.to_string())
}

struct InstructionAccountLookup<'a> {
    account_names: &'a [String],
    accounts: &'a [ProtoAccount],
}

impl<'a> InstructionAccountLookup<'a> {
    fn new(program: &'a Program, accounts: &'a [ProtoAccount]) -> Self {
        Self {
            account_names: &program.account_names,
            accounts,
        }
    }

    fn pubkey(&self, name: &str) -> Option<Pubkey> {
        let index = self.account_names.iter().position(|entry| entry == name)?;
        let account = self.accounts.get(index)?;
        pubkey_from_bytes(&account.address)
    }

    fn pubkey_with_aliases(&self, aliases: &[&str]) -> Option<Pubkey> {
        aliases.iter().find_map(|alias| self.pubkey(alias))
    }
}

fn pubkey_from_bytes(bytes: &[u8]) -> Option<Pubkey> {
    if bytes.len() != 32 {
        return None;
    }
    let mut array = [0u8; 32];
    array.copy_from_slice(bytes);
    Some(Pubkey::new_from_array(array))
}

fn currency_mint(currency: Option<&Currency>) -> Option<Pubkey> {
    currency.and_then(|value| pubkey_from_bytes(&value.mint_address))
}

fn currency_program(currency: Option<&Currency>) -> Option<Pubkey> {
    currency.and_then(|value| pubkey_from_bytes(&value.program_address))
}

fn order_mints(a: &Pubkey, b: &Pubkey) -> (Pubkey, Pubkey) {
    if a.to_bytes() <= b.to_bytes() {
        (a.clone(), b.clone())
    } else {
        (b.clone(), a.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::solana_messages::{
        Account as ProtoAccount, Currency, DexInfo, DexMarket, ParsedIdlInstruction,
        PoolLiquidityChangeEvent, PoolSide, Program,
    };

    fn proto_account(address: &Pubkey) -> ProtoAccount {
        ProtoAccount {
            address: address.to_bytes().to_vec(),
            is_signer: false,
            is_writable: false,
            token: None,
        }
    }

    fn proto_currency(mint: &Pubkey) -> Currency {
        Currency {
            name: String::new(),
            decimals: 0,
            uri: String::new(),
            symbol: String::new(),
            native: false,
            wrapped: false,
            fungible: false,
            key: String::new(),
            seller_fee_basis_points: 0,
            edition_nonce: None,
            token_standard: None,
            program_address: Vec::new(),
            mint_address: mint.to_bytes().to_vec(),
            metadata_address: Vec::new(),
            update_authority: Vec::new(),
            collection_address: Vec::new(),
            verified_collection: false,
            primary_sale_happened: false,
            is_mutable: false,
            token_creators: Vec::new(),
            freeze_authority: Vec::new(),
            mint_authority: Vec::new(),
            parsed: false,
        }
    }

    fn pool_side(post_amount: u64) -> PoolSide {
        PoolSide {
            change_amount: 0,
            post_amount,
        }
    }

    fn raydium_dex_info() -> DexInfo {
        DexInfo {
            program_address: RAYDIUM_V3_PROGRAM_PUBKEY.to_bytes().to_vec(),
            protocol_name: String::from("Raydium v3"),
            protocol_family: String::from("Raydium"),
        }
    }

    fn raydium_market(pool_id: &Pubkey, base: &Pubkey, quote: &Pubkey) -> DexMarket {
        DexMarket {
            market_address: pool_id.to_bytes().to_vec(),
            base_currency: Some(proto_currency(base)),
            quote_currency: Some(proto_currency(quote)),
        }
    }

    fn parsed_instruction(
        account_names: Vec<&str>,
        accounts: Vec<ProtoAccount>,
    ) -> ParsedIdlInstruction {
        ParsedIdlInstruction {
            index: 0,
            depth: 0,
            call_path: Vec::new(),
            caller_index: 0,
            ancestor_indexes: Vec::new(),
            external_seq_number: 0,
            internal_seq_number: 0,
            program: Some(Program {
                address: Vec::new(),
                parsed: true,
                json: String::new(),
                signature: Vec::new(),
                name: String::from("initialize"),
                method: String::from("initialize"),
                arguments: Vec::new(),
                account_names: account_names.into_iter().map(|s| s.to_string()).collect(),
            }),
            accounts,
            logs: Vec::new(),
            balance_updates: Vec::new(),
            token_balance_updates: Vec::new(),
            data: Vec::new(),
            token_supply_updates: Vec::new(),
        }
    }

    #[test]
    fn parses_raydium_accounts_and_bumps() {
        let amm_config = Pubkey::new_from_array([1u8; 32]);
        let base_mint = Pubkey::new_from_array([2u8; 32]);
        let quote_mint = Pubkey::new_from_array([3u8; 32]);

        let (pool_id, pool_bump) = Pubkey::find_program_address(
            &[
                POOL_SEED,
                amm_config.as_ref(),
                base_mint.as_ref(),
                quote_mint.as_ref(),
            ],
            &RAYDIUM_V3_PROGRAM_PUBKEY,
        );
        let (observation_state, observation_bump) = Pubkey::find_program_address(
            &[OBSERVATION_SEED, pool_id.as_ref()],
            &RAYDIUM_V3_PROGRAM_PUBKEY,
        );
        let (_, vault0_bump) = Pubkey::find_program_address(
            &[VAULT_SEED, pool_id.as_ref(), base_mint.as_ref()],
            &RAYDIUM_V3_PROGRAM_PUBKEY,
        );
        let (_, vault1_bump) = Pubkey::find_program_address(
            &[VAULT_SEED, pool_id.as_ref(), quote_mint.as_ref()],
            &RAYDIUM_V3_PROGRAM_PUBKEY,
        );

        let event = PoolLiquidityChangeEvent {
            instruction_index: 0,
            dex: Some(raydium_dex_info()),
            market: Some(raydium_market(&pool_id, &base_mint, &quote_mint)),
            base_currency: Some(pool_side(1)),
            quote_currency: Some(pool_side(1)),
            instruction: Some(parsed_instruction(
                vec!["payer", "ammConfig", "observationState"],
                vec![
                    proto_account(&Pubkey::default()),
                    proto_account(&amm_config),
                    proto_account(&observation_state),
                ],
            )),
        };

        let extras = parse_raydium_v3_extras(&event).expect("extras must parse");
        assert_eq!(extras.amm_config, Some(amm_config));
        assert_eq!(extras.observation_state, Some(observation_state));
        assert_eq!(extras.pool_bump, Some(pool_bump));
        assert_eq!(extras.observation_bump, Some(observation_bump));
        assert_eq!(extras.vault_bumps, Some([vault0_bump, vault1_bump]));
    }

    fn cp_amm_dex_info() -> DexInfo {
        DexInfo {
            program_address: CP_AMM_PROGRAM_PUBKEY.to_bytes().to_vec(),
            protocol_name: String::from("cp_amm"),
            protocol_family: String::from("cp_amm"),
        }
    }

    fn cp_amm_market(pool_id: &Pubkey, base: &Pubkey, quote: &Pubkey) -> DexMarket {
        DexMarket {
            market_address: pool_id.to_bytes().to_vec(),
            base_currency: Some(proto_currency(base)),
            quote_currency: Some(proto_currency(quote)),
        }
    }

    #[test]
    fn parses_cp_amm_accounts() {
        let pool = Pubkey::new_unique();
        let pool_authority = Pubkey::new_unique();
        let token_a_vault = Pubkey::new_unique();
        let token_b_vault = Pubkey::new_unique();
        let token_a_mint = Pubkey::new_unique();
        let token_b_mint = Pubkey::new_unique();
        let token_a_program = Pubkey::new_unique();
        let token_b_program = Pubkey::new_unique();
        let referral_token_account = Pubkey::new_unique();

        let accounts = vec![
            proto_account(&pool_authority),
            proto_account(&pool),
            proto_account(&Pubkey::new_unique()),
            proto_account(&Pubkey::new_unique()),
            proto_account(&token_a_vault),
            proto_account(&token_b_vault),
            proto_account(&token_a_mint),
            proto_account(&token_b_mint),
            proto_account(&Pubkey::new_unique()),
            proto_account(&token_a_program),
            proto_account(&token_b_program),
            proto_account(&referral_token_account),
            proto_account(&Pubkey::new_unique()),
            proto_account(&CP_AMM_PROGRAM_PUBKEY),
        ];

        let instruction = parsed_instruction(
            vec![
                "pool_authority",
                "pool",
                "input_token_account",
                "output_token_account",
                "token_a_vault",
                "token_b_vault",
                "token_a_mint",
                "token_b_mint",
                "payer",
                "token_a_program",
                "token_b_program",
                "referral_token_account",
                "event_authority",
                "program",
            ],
            accounts,
        );

        let event = PoolLiquidityChangeEvent {
            instruction_index: 0,
            dex: Some(cp_amm_dex_info()),
            market: Some(cp_amm_market(&pool, &token_a_mint, &token_b_mint)),
            base_currency: Some(pool_side(1)),
            quote_currency: Some(pool_side(1)),
            instruction: Some(instruction),
        };

        let extras = parse_cp_amm_extras(&event).expect("extras must parse");
        assert_eq!(extras.pool_authority, Some(pool_authority.to_string()));
        assert_eq!(extras.token_a_vault, Some(token_a_vault.to_string()));
        assert_eq!(extras.token_b_vault, Some(token_b_vault.to_string()));
        assert_eq!(extras.token_a_mint, Some(token_a_mint.to_string()));
        assert_eq!(extras.token_b_mint, Some(token_b_mint.to_string()));
        assert_eq!(extras.token_a_program, Some(token_a_program.to_string()));
        assert_eq!(extras.token_b_program, Some(token_b_program.to_string()));
        assert_eq!(
            extras.referral_token_account,
            Some(referral_token_account.to_string())
        );
    }
}

#[derive(Debug, Clone, Default)]
pub struct RaydiumV3PoolExtras {
    pub tick_spacing: Option<i32>,
    pub sqrt_price_x64: Option<u128>,
    pub liquidity: Option<u128>,
    pub current_tick: Option<i32>,
    pub amm_config: Option<Pubkey>,
    pub observation_state: Option<Pubkey>,
    pub pool_bump: Option<u8>,
    pub observation_bump: Option<u8>,
    pub vault_bumps: Option<[u8; 2]>,
}

#[derive(Debug, Clone, Default)]
pub struct CpAmmPoolExtras {
    pub pool_authority: Option<String>,
    pub token_a_vault: Option<String>,
    pub token_b_vault: Option<String>,
    pub token_a_mint: Option<String>,
    pub token_b_mint: Option<String>,
    pub token_a_program: Option<String>,
    pub token_b_program: Option<String>,
    pub amp: Option<u64>,
    pub lp_fee_bps: Option<u16>,
    pub protocol_fee_bps: Option<u16>,
    pub referral_bps: Option<u16>,
    pub referral_token_account: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct LbClmmPoolExtras {
    pub reserve_x: Option<String>,
    pub reserve_y: Option<String>,
    pub token_x_mint: Option<String>,
    pub token_y_mint: Option<String>,
    pub token_x_program: Option<String>,
    pub token_y_program: Option<String>,
    pub oracle: Option<String>,
    pub bin_array_bitmap_extension: Option<String>,
    pub host_fee_account: Option<String>,
    pub bin_step: Option<u16>,
    pub active_bin_id: Option<i32>,
    pub dynamic_fee_bps: Option<u16>,
    pub protocol_fee_bps: Option<u16>,
    pub reserve_x_balance: Option<u128>,
    pub reserve_y_balance: Option<u128>,
    pub volatility_bps: Option<u16>,
}

#[derive(Debug, Clone, Default)]
pub struct LifinityPoolExtras {
    pub authority: Option<String>,
    pub token_a_vault: Option<String>,
    pub token_b_vault: Option<String>,
    pub pool_mint: Option<String>,
    pub fee_account: Option<String>,
    pub token_program: Option<String>,
    pub oracle_main: Option<String>,
    pub oracle_sub: Option<String>,
    pub oracle_pc: Option<String>,
    pub fees_bps: Option<u16>,
    pub owner_fee_bps: Option<u16>,
    pub host_fee_bps: Option<u16>,
    pub std_spread_bps: Option<u16>,
    pub spread_buffer_bps: Option<u16>,
    pub vault_a_balance: Option<u128>,
    pub vault_b_balance: Option<u128>,
    pub freeze_trade: Option<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct WhirlpoolPoolExtras {
    pub token_vault_a: Option<String>,
    pub token_vault_b: Option<String>,
    pub token_mint_a: Option<String>,
    pub token_mint_b: Option<String>,
    pub tick_spacing: Option<i32>,
    pub sqrt_price_x64: Option<u128>,
    pub liquidity: Option<u128>,
    pub fee_rate: Option<u16>,
    pub protocol_fee_rate: Option<u16>,
    pub oracle: Option<String>,
    pub tick_array0: Option<String>,
    pub tick_array1: Option<String>,
    pub tick_array2: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub pool_id: String,
    pub program_id: String,
    pub slot: u64,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_post_amount: u128,
    pub quote_post_amount: u128,
    pub updated_at: Instant,
}

impl PoolSnapshot {
    pub fn spot_price(&self) -> f64 {
        if self.base_post_amount == 0 {
            return 0.0;
        }
        self.quote_post_amount as f64 / self.base_post_amount as f64
    }
}

#[derive(Debug, Clone)]
pub struct TradeUpdate {
    pub seq: u64,
    pub slot: u64,
    pub program_id: String,
    pub pool_id: String,
    pub signature: String,
    pub buy_mint: String,
    pub sell_mint: String,
    pub buy_amount: u64,
    pub sell_amount: u64,
}

impl TradeUpdate {
    pub fn from_corecast(seq: u64, message: &DexTradeStreamMessage) -> Option<Self> {
        let block = message.block.as_ref()?;
        let trade = message.trade.as_ref()?;
        let dex = trade.dex.as_ref()?;
        let market = trade.market.as_ref()?;
        let buy_side = trade.buy.as_ref()?;
        let sell_side = trade.sell.as_ref()?;
        let buy_currency = buy_side.currency.as_ref()?;
        let sell_currency = sell_side.currency.as_ref()?;
        let tx = message.transaction.as_ref()?;

        let program_id = encode_pubkey(&dex.program_address);
        let pool_id = encode_pubkey(&market.market_address);
        let signature = encode_pubkey(&tx.signature);
        if program_id.is_empty() || pool_id.is_empty() || signature.is_empty() {
            return None;
        }

        Some(Self {
            seq,
            slot: block.slot,
            program_id,
            pool_id,
            signature,
            buy_mint: encode_pubkey(&buy_currency.mint_address),
            sell_mint: encode_pubkey(&sell_currency.mint_address),
            buy_amount: buy_side.amount,
            sell_amount: sell_side.amount,
        })
    }

    pub fn price(&self) -> f64 {
        if self.sell_amount == 0 {
            return 0.0;
        }
        self.buy_amount as f64 / self.sell_amount as f64
    }
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub seq: u64,
    pub trade: TradeUpdate,
    pub pool_snapshot: Option<PoolSnapshot>,
    pub estimated_edge_bps: f64,
    pub created_at: Instant,
}

fn encode_pubkey(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        String::new()
    } else {
        bs58::encode(bytes).into_string()
    }
}

impl Opportunity {
    pub fn from_trade(
        seq: u64,
        trade: TradeUpdate,
        snapshot: Option<PoolSnapshot>,
        override_edge_bps: Option<f64>,
    ) -> Self {
        let estimated_edge_bps = override_edge_bps
            .or_else(|| {
                snapshot
                    .as_ref()
                    .map(|snap| compute_edge_bps(trade.price(), snap.spot_price()))
            })
            .unwrap_or_default();

        Self {
            seq,
            trade,
            pool_snapshot: snapshot,
            estimated_edge_bps,
            created_at: Instant::now(),
        }
    }
}

pub fn compute_edge_bps(trade_price: f64, pool_price: f64) -> f64 {
    if trade_price == 0.0 || pool_price == 0.0 {
        return 0.0;
    }
    ((pool_price - trade_price) / pool_price) * 10_000.0
}

#[derive(Debug, Clone)]
pub struct BundleCandidate {
    pub seq: u64,
    pub opportunity_seq: u64,
    pub pool_id: String,
    pub trade_signature: String,
    pub estimated_edge_bps: f64,
    pub created_at: Instant,
}

impl BundleCandidate {
    pub fn from_opportunity(opportunity: &Opportunity) -> Self {
        Self {
            seq: opportunity.seq,
            opportunity_seq: opportunity.seq,
            pool_id: opportunity.trade.pool_id.clone(),
            trade_signature: opportunity.trade.signature.clone(),
            estimated_edge_bps: opportunity.estimated_edge_bps,
            created_at: Instant::now(),
        }
    }
}
