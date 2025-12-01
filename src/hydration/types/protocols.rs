use std::str::FromStr;
use std::time::Instant;

use once_cell::sync::Lazy;
use solana_sdk::pubkey::Pubkey;

use crate::proto::solana_messages::{
    Account as ProtoAccount, Currency, PoolLiquidityChangeEvent, Program,
};

pub use self::cp_amm::{CpAmmPoolExtras, CP_AMM_INPUT_UTILIZATION_BPS, CP_AMM_PROGRAM_ID};
pub use self::lb_clmm::{
    LbClmmBinArrayExtras, LbClmmBinLiquidityExtras, LbClmmBitmapExtras, LbClmmPoolExtras,
    LB_CLMM_INPUT_UTILIZATION_BPS, LB_CLMM_PROGRAM_ID,
};
pub use self::lifinity_v2::{
    LifinityPoolExtras, LIFINITY_V2_INPUT_UTILIZATION_BPS, LIFINITY_V2_PROGRAM_ID,
};
pub use self::raydium_v3::{
    RaydiumV3PoolExtras, RAYDIUM_V3_INPUT_UTILIZATION_BPS, RAYDIUM_V3_PROGRAM_ID,
};
pub use self::whirlpool::{
    WhirlpoolPoolExtras, WHIRLPOOL_INPUT_UTILIZATION_BPS, WHIRLPOOL_PROGRAM_ID,
};

pub(crate) use self::cp_amm::parse_extras as parse_cp_amm_extras;
pub(crate) use self::lb_clmm::parse_extras as parse_lb_clmm_extras;
pub(crate) use self::lifinity_v2::parse_extras as parse_lifinity_extras;
pub(crate) use self::raydium_v3::parse_extras as parse_raydium_v3_extras;
pub(crate) use self::whirlpool::parse_extras as parse_whirlpool_extras;

mod shared {
    use super::*;

    pub(super) struct InstructionAccountLookup<'a> {
        account_names: &'a [String],
        accounts: &'a [ProtoAccount],
    }

    impl<'a> InstructionAccountLookup<'a> {
        pub(super) fn new(program: &'a Program, accounts: &'a [ProtoAccount]) -> Self {
            Self {
                account_names: &program.account_names,
                accounts,
            }
        }

        pub(super) fn pubkey(&self, name: &str) -> Option<Pubkey> {
            let index = self.account_names.iter().position(|entry| entry == name)?;
            let account = self.accounts.get(index)?;
            pubkey_from_bytes(&account.address)
        }

        pub(super) fn pubkey_with_aliases(&self, aliases: &[&str]) -> Option<Pubkey> {
            aliases.iter().find_map(|alias| self.pubkey(alias))
        }
    }

    pub(super) fn lookup_account_string(
        lookup: &InstructionAccountLookup,
        aliases: &[&str],
    ) -> Option<String> {
        lookup.pubkey_with_aliases(aliases).map(|pk| pk.to_string())
    }

    pub(super) fn pubkey_from_bytes(bytes: &[u8]) -> Option<Pubkey> {
        if bytes.len() != 32 {
            return None;
        }
        let mut array = [0u8; 32];
        array.copy_from_slice(bytes);
        Some(Pubkey::new_from_array(array))
    }

    pub(super) fn currency_mint(currency: Option<&Currency>) -> Option<Pubkey> {
        currency.and_then(|value| pubkey_from_bytes(&value.mint_address))
    }

    pub(super) fn currency_program(currency: Option<&Currency>) -> Option<Pubkey> {
        currency.and_then(|value| pubkey_from_bytes(&value.program_address))
    }
}

mod raydium_v3 {
    use super::shared::{currency_mint, pubkey_from_bytes, InstructionAccountLookup};
    use super::*;

    pub const RAYDIUM_V3_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
    pub const RAYDIUM_V3_INPUT_UTILIZATION_BPS: u64 = 4_000;
    static RAYDIUM_V3_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
        Pubkey::from_str(RAYDIUM_V3_PROGRAM_ID).expect("static Raydium v3 program id")
    });
    const POOL_SEED: &[u8] = b"pool";
    const OBSERVATION_SEED: &[u8] = b"observation";
    const VAULT_SEED: &[u8] = b"pool_vault";

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
        pub trade_fee_rate: Option<u32>,
        pub protocol_fee_rate: Option<u32>,
        pub base_token_program: Option<Pubkey>,
        pub quote_token_program: Option<Pubkey>,
    }

    pub(crate) fn parse_extras(event: &PoolLiquidityChangeEvent) -> Option<RaydiumV3PoolExtras> {
        let dex = event.dex.as_ref()?;
        let program_key = pubkey_from_bytes(&dex.program_address)?;
        if program_key != *RAYDIUM_V3_PROGRAM_PUBKEY {
            return None;
        }

        let instruction = event.instruction.as_ref()?;
        let program = instruction.program.as_ref()?;
        let account_lookup = InstructionAccountLookup::new(program, &instruction.accounts);

        let mut extras = RaydiumV3PoolExtras {
            amm_config: account_lookup.pubkey_with_aliases(&["ammConfig", "amm_config"]),
            observation_state: account_lookup
                .pubkey_with_aliases(&["observationState", "observation_state"]),
            ..Default::default()
        };

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
            && extras.trade_fee_rate.is_none()
            && extras.protocol_fee_rate.is_none()
            && extras.base_token_program.is_none()
            && extras.quote_token_program.is_none()
        {
            None
        } else {
            Some(extras)
        }
    }

    fn order_mints(a: &Pubkey, b: &Pubkey) -> (Pubkey, Pubkey) {
        if a.to_bytes() <= b.to_bytes() {
            (*a, *b)
        } else {
            (*b, *a)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::test_utils::*;
        use super::*;

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

            let extras = parse_extras(&event).expect("extras must parse");
            assert_eq!(extras.amm_config, Some(amm_config));
            assert_eq!(extras.observation_state, Some(observation_state));
            assert_eq!(extras.pool_bump, Some(pool_bump));
            assert_eq!(extras.observation_bump, Some(observation_bump));
            assert_eq!(extras.vault_bumps, Some([vault0_bump, vault1_bump]));
        }
    }
}

mod cp_amm {
    use super::shared::{
        currency_mint, currency_program, lookup_account_string, pubkey_from_bytes,
        InstructionAccountLookup,
    };
    use super::*;

    pub const CP_AMM_PROGRAM_ID: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
    pub const CP_AMM_INPUT_UTILIZATION_BPS: u64 = 4_000;
    static CP_AMM_PROGRAM_PUBKEY: Lazy<Pubkey> =
        Lazy::new(|| Pubkey::from_str(CP_AMM_PROGRAM_ID).expect("static cp_amm program id"));

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
        pub sqrt_price_x64: Option<u128>,
        pub liquidity: Option<u128>,
        pub sqrt_min_price_x64: Option<u128>,
        pub sqrt_max_price_x64: Option<u128>,
        pub hydrated_at: Option<Instant>,
    }

    pub(crate) fn parse_extras(event: &PoolLiquidityChangeEvent) -> Option<CpAmmPoolExtras> {
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

        let extras = CpAmmPoolExtras {
            pool_authority: lookup_account_string(
                &account_lookup,
                &["pool_authority", "poolAuthority"],
            ),
            token_a_vault: lookup_account_string(
                &account_lookup,
                &["token_a_vault", "tokenAVault"],
            ),
            token_b_vault: lookup_account_string(
                &account_lookup,
                &["token_b_vault", "tokenBVault"],
            ),
            token_a_mint: lookup_account_string(&account_lookup, &["token_a_mint", "tokenAMint"])
                .or_else(|| currency_mint(base_currency).map(|pk| pk.to_string())),
            token_b_mint: lookup_account_string(&account_lookup, &["token_b_mint", "tokenBMint"])
                .or_else(|| currency_mint(quote_currency).map(|pk| pk.to_string())),
            token_a_program: lookup_account_string(
                &account_lookup,
                &["token_a_program", "tokenAProgram"],
            )
            .or_else(|| currency_program(base_currency).map(|pk| pk.to_string())),
            token_b_program: lookup_account_string(
                &account_lookup,
                &["token_b_program", "tokenBProgram"],
            )
            .or_else(|| currency_program(quote_currency).map(|pk| pk.to_string())),
            referral_token_account: lookup_account_string(
                &account_lookup,
                &["referral_token_account", "referralTokenAccount"],
            ),
            ..Default::default()
        };

        Some(extras)
    }

    #[cfg(test)]
    mod tests {
        use super::super::test_utils::*;
        use super::*;

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

            let extras = parse_extras(&event).expect("extras must parse");
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
}

mod lb_clmm {
    use super::shared::{
        currency_mint, currency_program, lookup_account_string, pubkey_from_bytes,
        InstructionAccountLookup,
    };
    use super::*;

    pub const LB_CLMM_INPUT_UTILIZATION_BPS: u64 = 2_000;
    pub const LB_CLMM_PROGRAM_ID: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
    static LB_CLMM_PROGRAM_PUBKEY: Lazy<Pubkey> =
        Lazy::new(|| Pubkey::from_str(LB_CLMM_PROGRAM_ID).expect("static lb_clmm program id"));
    const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
    static MEMO_PROGRAM_PUBKEY: Lazy<Pubkey> =
        Lazy::new(|| Pubkey::from_str(MEMO_PROGRAM_ID).expect("static memo program id"));
    const LB_CLMM_SWAP_METHODS: &[&str] = &[
        "swap",
        "swap2",
        "swap_exact_out",
        "swap_exact_out2",
        "swap_with_price_impact",
        "swap_with_price_impact2",
    ];

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
        pub bin_arrays: Vec<LbClmmBinArrayExtras>,
        pub active_bitmap: Option<LbClmmBitmapExtras>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct LbClmmBinArrayExtras {
        pub start_bin_id: i32,
        pub bins: Vec<LbClmmBinLiquidityExtras>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct LbClmmBinLiquidityExtras {
        pub bin_id: i32,
        pub liquidity_x: u128,
        pub liquidity_y: u128,
    }

    #[derive(Debug, Clone, Default)]
    pub struct LbClmmBitmapExtras {
        pub anchor_bin_id: i32,
        pub bitmap: Vec<u8>,
    }

    pub(crate) fn parse_extras(event: &PoolLiquidityChangeEvent) -> Option<LbClmmPoolExtras> {
        let dex = event.dex.as_ref()?;
        let program_key = pubkey_from_bytes(&dex.program_address)?;
        if program_key != *LB_CLMM_PROGRAM_PUBKEY {
            return None;
        }

        let instruction = event.instruction.as_ref()?;
        let program = instruction.program.as_ref()?;
        let swap_accounts = if is_lb_clmm_swap_method(program.method.as_str()) {
            lb_clmm_swap_accounts(&instruction.accounts)
        } else {
            None
        };
        let market = event.market.as_ref();
        let base_currency = market.and_then(|m| m.base_currency.as_ref());
        let quote_currency = market.and_then(|m| m.quote_currency.as_ref());
        let base_side = event.base_currency.as_ref();
        let quote_side = event.quote_currency.as_ref();

        let mut extras = LbClmmPoolExtras::default();

        if let Some(accounts) = swap_accounts {
            extras.reserve_x = Some(accounts.reserve_x.to_string());
            extras.reserve_y = Some(accounts.reserve_y.to_string());
            extras.token_x_mint = Some(accounts.token_x_mint.to_string());
            extras.token_y_mint = Some(accounts.token_y_mint.to_string());
            extras.token_x_program = Some(accounts.token_x_program.to_string());
            extras.token_y_program = Some(accounts.token_y_program.to_string());
            extras.oracle = Some(accounts.oracle.to_string());
            extras.bin_array_bitmap_extension =
                accounts.bin_array_bitmap_extension.map(|pk| pk.to_string());
            extras.host_fee_account = accounts.host_fee_in.map(|pk| pk.to_string());
        } else {
            let lookup = InstructionAccountLookup::new(program, &instruction.accounts);
            extras.reserve_x = lookup_account_string(&lookup, &["reserve_x", "reserveX"]);
            extras.reserve_y = lookup_account_string(&lookup, &["reserve_y", "reserveY"]);
            extras.token_x_mint = lookup_account_string(&lookup, &["token_x_mint", "tokenXMint"]);
            extras.token_y_mint = lookup_account_string(&lookup, &["token_y_mint", "tokenYMint"]);
            extras.token_x_program =
                lookup_account_string(&lookup, &["token_x_program", "tokenXProgram"]);
            extras.token_y_program =
                lookup_account_string(&lookup, &["token_y_program", "tokenYProgram"]);
            extras.oracle = lookup_account_string(&lookup, &["oracle"]);
            extras.bin_array_bitmap_extension = lookup_account_string(
                &lookup,
                &["bin_array_bitmap_extension", "binArrayBitmapExtension"],
            );
            extras.host_fee_account = lookup_account_string(&lookup, &["host_fee_in", "hostFeeIn"]);
        }

        extras.token_x_mint = extras
            .token_x_mint
            .or_else(|| currency_mint(base_currency).map(|pk| pk.to_string()));
        extras.token_y_mint = extras
            .token_y_mint
            .or_else(|| currency_mint(quote_currency).map(|pk| pk.to_string()));
        extras.token_x_program = extras
            .token_x_program
            .or_else(|| currency_program(base_currency).map(|pk| pk.to_string()));
        extras.token_y_program = extras
            .token_y_program
            .or_else(|| currency_program(quote_currency).map(|pk| pk.to_string()));

        if let Some(side) = base_side {
            extras.reserve_x_balance = Some(side.post_amount as u128);
        }
        if let Some(side) = quote_side {
            extras.reserve_y_balance = Some(side.post_amount as u128);
        }

        extras_if_any(extras)
    }

    fn lb_clmm_swap_accounts(accounts: &[ProtoAccount]) -> Option<LbClmmSwapAccounts> {
        if accounts.len() < 13 {
            return None;
        }

        let mut index = 0usize;
        skip_account(accounts, &mut index)?; // lb_pair

        let bin_array_bitmap_extension = if let Some(account) = accounts.get(index) {
            if !is_token_account(account) {
                let pk = pubkey_from_bytes(&account.address)?;
                index += 1;
                Some(pk)
            } else {
                None
            }
        } else {
            None
        };

        let reserve_x = take_pubkey(accounts, &mut index)?;
        let reserve_y = take_pubkey(accounts, &mut index)?;
        skip_account(accounts, &mut index)?; // user_token_in
        skip_account(accounts, &mut index)?; // user_token_out
        let token_x_mint = take_pubkey(accounts, &mut index)?;
        let token_y_mint = take_pubkey(accounts, &mut index)?;
        let oracle = take_pubkey(accounts, &mut index)?;
        let host_fee_in = if let Some(account) = accounts.get(index) {
            if is_token_account(account) {
                let pk = pubkey_from_bytes(&account.address)?;
                index += 1;
                Some(pk)
            } else {
                None
            }
        } else {
            None
        };
        skip_account(accounts, &mut index)?; // user
        let token_x_program = take_pubkey(accounts, &mut index)?;
        let token_y_program = take_pubkey(accounts, &mut index)?;

        if let Some(account) = accounts.get(index) {
            if is_memo_program(account) {
                index += 1;
            }
        }

        // Ensure the trailing event authority and program accounts exist even though we
        // do not currently consume them.
        accounts.get(index)?;
        accounts.get(index + 1)?;

        Some(LbClmmSwapAccounts {
            reserve_x,
            reserve_y,
            token_x_mint,
            token_y_mint,
            oracle,
            bin_array_bitmap_extension,
            host_fee_in,
            token_x_program,
            token_y_program,
        })
    }

    fn skip_account(accounts: &[ProtoAccount], index: &mut usize) -> Option<()> {
        if *index >= accounts.len() {
            return None;
        }
        *index += 1;
        Some(())
    }

    fn take_pubkey(accounts: &[ProtoAccount], index: &mut usize) -> Option<Pubkey> {
        let account = accounts.get(*index)?;
        let key = pubkey_from_bytes(&account.address)?;
        *index += 1;
        Some(key)
    }

    fn is_token_account(account: &ProtoAccount) -> bool {
        account.token.is_some()
    }

    fn is_memo_program(account: &ProtoAccount) -> bool {
        pubkey_from_bytes(&account.address)
            .map(|pk| pk == *MEMO_PROGRAM_PUBKEY)
            .unwrap_or(false)
    }

    fn is_lb_clmm_swap_method(method: &str) -> bool {
        LB_CLMM_SWAP_METHODS
            .iter()
            .any(|candidate| *candidate == method)
    }

    fn extras_if_any(extras: LbClmmPoolExtras) -> Option<LbClmmPoolExtras> {
        if extras.reserve_x.is_none()
            && extras.reserve_y.is_none()
            && extras.token_x_mint.is_none()
            && extras.token_y_mint.is_none()
            && extras.token_x_program.is_none()
            && extras.token_y_program.is_none()
            && extras.oracle.is_none()
            && extras.bin_array_bitmap_extension.is_none()
            && extras.host_fee_account.is_none()
            && extras.reserve_x_balance.is_none()
            && extras.reserve_y_balance.is_none()
        {
            None
        } else {
            Some(extras)
        }
    }

    struct LbClmmSwapAccounts {
        reserve_x: Pubkey,
        reserve_y: Pubkey,
        token_x_mint: Pubkey,
        token_y_mint: Pubkey,
        oracle: Pubkey,
        bin_array_bitmap_extension: Option<Pubkey>,
        host_fee_in: Option<Pubkey>,
        token_x_program: Pubkey,
        token_y_program: Pubkey,
    }

    #[cfg(test)]
    mod tests {
        use super::super::test_utils::*;
        use super::*;

        #[test]
        fn lb_clmm_swap_accounts_without_optionals() {
            let pool = Pubkey::new_unique();
            let reserve_x = Pubkey::new_unique();
            let reserve_y = Pubkey::new_unique();
            let user = Pubkey::new_unique();
            let user_token_in = Pubkey::new_unique();
            let user_token_out = Pubkey::new_unique();
            let token_x_mint = Pubkey::new_unique();
            let token_y_mint = Pubkey::new_unique();
            let oracle = Pubkey::new_unique();
            let event_authority = Pubkey::new_unique();
            let spl_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
                .expect("spl token program id");

            let accounts = vec![
                proto_account(&pool),
                spl_token_account(&reserve_x, &token_x_mint, &pool, &spl_program),
                spl_token_account(&reserve_y, &token_y_mint, &pool, &spl_program),
                spl_token_account(&user_token_in, &token_x_mint, &user, &spl_program),
                spl_token_account(&user_token_out, &token_y_mint, &user, &spl_program),
                proto_account(&token_x_mint),
                proto_account(&token_y_mint),
                proto_account(&oracle),
                proto_account(&user),
                proto_account(&spl_program),
                proto_account(&spl_program),
                proto_account(&event_authority),
                proto_account(&LB_CLMM_PROGRAM_PUBKEY),
            ];

            let instruction = parsed_instruction_with_method(
                vec![
                    "lb_pair",
                    "reserve_x",
                    "reserve_y",
                    "user_token_in",
                    "user_token_out",
                    "token_x_mint",
                    "token_y_mint",
                    "oracle",
                    "user",
                    "token_x_program",
                    "token_y_program",
                    "event_authority",
                    "program",
                ],
                accounts,
                "swap",
            );

            let event = PoolLiquidityChangeEvent {
                instruction_index: 0,
                dex: Some(lb_clmm_dex_info()),
                market: Some(lb_clmm_market(&pool, &token_x_mint, &token_y_mint)),
                base_currency: Some(pool_side(1)),
                quote_currency: Some(pool_side(1)),
                instruction: Some(instruction),
            };

            let extras = parse_extras(&event).expect("extras must parse");
            assert_eq!(extras.reserve_x, Some(reserve_x.to_string()));
            assert_eq!(extras.reserve_y, Some(reserve_y.to_string()));
            assert_eq!(extras.token_x_mint, Some(token_x_mint.to_string()));
            assert_eq!(extras.token_y_mint, Some(token_y_mint.to_string()));
            assert_eq!(extras.oracle, Some(oracle.to_string()));
            assert_eq!(extras.token_x_program, Some(spl_program.to_string()));
            assert_eq!(extras.token_y_program, Some(spl_program.to_string()));
            assert!(extras.bin_array_bitmap_extension.is_none());
            assert!(extras.host_fee_account.is_none());
        }

        #[test]
        fn lb_clmm_swap_accounts_with_optionals() {
            let pool = Pubkey::new_unique();
            let bitmap_extension = Pubkey::new_unique();
            let reserve_x = Pubkey::new_unique();
            let reserve_y = Pubkey::new_unique();
            let user = Pubkey::new_unique();
            let user_token_in = Pubkey::new_unique();
            let user_token_out = Pubkey::new_unique();
            let host_fee = Pubkey::new_unique();
            let token_x_mint = Pubkey::new_unique();
            let token_y_mint = Pubkey::new_unique();
            let oracle = Pubkey::new_unique();
            let event_authority = Pubkey::new_unique();
            let spl_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
                .expect("spl token program id");

            let accounts = vec![
                proto_account(&pool),
                proto_account(&bitmap_extension),
                spl_token_account(&reserve_x, &token_x_mint, &pool, &spl_program),
                spl_token_account(&reserve_y, &token_y_mint, &pool, &spl_program),
                spl_token_account(&user_token_in, &token_x_mint, &user, &spl_program),
                spl_token_account(&user_token_out, &token_y_mint, &user, &spl_program),
                proto_account(&token_x_mint),
                proto_account(&token_y_mint),
                proto_account(&oracle),
                spl_token_account(&host_fee, &token_x_mint, &user, &spl_program),
                proto_account(&user),
                proto_account(&spl_program),
                proto_account(&spl_program),
                proto_account(&MEMO_PROGRAM_PUBKEY),
                proto_account(&event_authority),
                proto_account(&LB_CLMM_PROGRAM_PUBKEY),
            ];

            let instruction = parsed_instruction_with_method(
                vec![
                    "lb_pair",
                    "bin_array_bitmap_extension",
                    "reserve_x",
                    "reserve_y",
                    "user_token_in",
                    "user_token_out",
                    "token_x_mint",
                    "token_y_mint",
                    "oracle",
                    "host_fee_in",
                    "user",
                    "token_x_program",
                    "token_y_program",
                    "memo_program",
                    "event_authority",
                    "program",
                ],
                accounts,
                "swap2",
            );

            let event = PoolLiquidityChangeEvent {
                instruction_index: 0,
                dex: Some(lb_clmm_dex_info()),
                market: Some(lb_clmm_market(&pool, &token_x_mint, &token_y_mint)),
                base_currency: Some(pool_side(1)),
                quote_currency: Some(pool_side(1)),
                instruction: Some(instruction),
            };

            let extras = parse_extras(&event).expect("extras must parse");
            assert_eq!(
                extras.bin_array_bitmap_extension,
                Some(bitmap_extension.to_string())
            );
            assert_eq!(extras.host_fee_account, Some(host_fee.to_string()));
            assert_eq!(extras.reserve_x, Some(reserve_x.to_string()));
            assert_eq!(extras.reserve_y, Some(reserve_y.to_string()));
            assert_eq!(extras.token_x_program, Some(spl_program.to_string()));
            assert_eq!(extras.token_y_program, Some(spl_program.to_string()));
        }
    }
}

mod lifinity_v2 {
    use super::shared::{lookup_account_string, pubkey_from_bytes, InstructionAccountLookup};
    use super::*;

    pub const LIFINITY_V2_PROGRAM_ID: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";
    pub const LIFINITY_V2_INPUT_UTILIZATION_BPS: u64 = 4_000;
    static LIFINITY_V2_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
        Pubkey::from_str(LIFINITY_V2_PROGRAM_ID).expect("static Lifinity AMM v2 program id")
    });

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
        pub spread_coefficient_bps: Option<u32>,
        pub price_buffer_coin_bps: Option<i32>,
        pub price_buffer_pc_bps: Option<i32>,
        pub fee_trade_bps: Option<u16>,
        pub fee_platform_bps: Option<u16>,
        pub oracle_main_slot_limit: Option<u64>,
        pub oracle_sub_confidence_limit: Option<u64>,
        pub oracle_sub_slot_limit: Option<u64>,
        pub oracle_pc_confidence_limit: Option<u64>,
        pub oracle_pc_slot_limit: Option<u64>,
        pub vault_a_balance: Option<u128>,
        pub vault_b_balance: Option<u128>,
        pub freeze_trade: Option<u8>,
    }

    pub(crate) fn parse_extras(event: &PoolLiquidityChangeEvent) -> Option<LifinityPoolExtras> {
        let dex = event.dex.as_ref()?;
        let program_key = pubkey_from_bytes(&dex.program_address)?;
        if program_key != *LIFINITY_V2_PROGRAM_PUBKEY {
            return None;
        }

        let instruction = event.instruction.as_ref()?;
        let program = instruction.program.as_ref()?;
        let lookup = InstructionAccountLookup::new(program, &instruction.accounts);

        let mut extras = LifinityPoolExtras {
            authority: lookup_account_string(&lookup, &["authority", "Authority"]),
            token_program: lookup_account_string(&lookup, &["tokenProgram", "token_program"]),
            token_a_vault: lookup_account_string(
                &lookup,
                &["swapSource", "tokenA", "token_a_vault", "tokenAAccount"],
            ),
            token_b_vault: lookup_account_string(
                &lookup,
                &[
                    "swapDestination",
                    "tokenB",
                    "token_b_vault",
                    "tokenBAccount",
                ],
            ),
            pool_mint: lookup_account_string(&lookup, &["poolMint", "pool_mint"]),
            fee_account: lookup_account_string(&lookup, &["feeAccount", "fee_account"]),
            oracle_main: lookup_account_string(
                &lookup,
                &["oracleMainAccount", "oracle_main_account", "oracleMain"],
            ),
            oracle_sub: lookup_account_string(
                &lookup,
                &["oracleSubAccount", "oracle_sub_account", "oracleSub"],
            ),
            oracle_pc: lookup_account_string(
                &lookup,
                &["oraclePcAccount", "oracle_pc_account", "oraclePc"],
            ),
            ..Default::default()
        };

        if let Some(base_side) = event.base_currency.as_ref() {
            extras.vault_a_balance = Some(base_side.post_amount as u128);
        }
        if let Some(quote_side) = event.quote_currency.as_ref() {
            extras.vault_b_balance = Some(quote_side.post_amount as u128);
        }

        extras_if_any(extras)
    }

    fn extras_if_any(extras: LifinityPoolExtras) -> Option<LifinityPoolExtras> {
        if extras.authority.is_none()
            && extras.token_a_vault.is_none()
            && extras.token_b_vault.is_none()
            && extras.pool_mint.is_none()
            && extras.fee_account.is_none()
            && extras.token_program.is_none()
            && extras.oracle_main.is_none()
            && extras.oracle_sub.is_none()
            && extras.oracle_pc.is_none()
            && extras.vault_a_balance.is_none()
            && extras.vault_b_balance.is_none()
        {
            None
        } else {
            Some(extras)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::test_utils::*;
        use super::*;

        #[test]
        fn lifinity_extras_parse_accounts() {
            let pool = Pubkey::new_unique();
            let base_mint = Pubkey::new_unique();
            let quote_mint = Pubkey::new_unique();
            let authority = Pubkey::new_unique();
            let token_a_vault = Pubkey::new_unique();
            let token_b_vault = Pubkey::new_unique();
            let pool_mint = Pubkey::new_unique();
            let fee_account = Pubkey::new_unique();
            let token_program = Pubkey::new_unique();
            let oracle_main = Pubkey::new_unique();
            let oracle_sub = Pubkey::new_unique();
            let oracle_pc = Pubkey::new_unique();

            let account_names = vec![
                "authority",
                "amm",
                "userTransferAuthority",
                "sourceInfo",
                "destinationInfo",
                "swapSource",
                "swapDestination",
                "poolMint",
                "feeAccount",
                "tokenProgram",
                "oracleMainAccount",
                "oracleSubAccount",
                "oraclePcAccount",
            ];

            let accounts = vec![
                proto_account(&authority),
                proto_account(&pool),
                proto_account(&Pubkey::new_unique()),
                proto_account(&Pubkey::new_unique()),
                proto_account(&Pubkey::new_unique()),
                proto_account(&token_a_vault),
                proto_account(&token_b_vault),
                proto_account(&pool_mint),
                proto_account(&fee_account),
                proto_account(&token_program),
                proto_account(&oracle_main),
                proto_account(&oracle_sub),
                proto_account(&oracle_pc),
            ];

            let instruction = parsed_instruction(account_names, accounts);

            let event = PoolLiquidityChangeEvent {
                instruction_index: 0,
                dex: Some(lifinity_dex_info()),
                market: Some(raydium_market(&pool, &base_mint, &quote_mint)),
                base_currency: Some(pool_side(42)),
                quote_currency: Some(pool_side(84)),
                instruction: Some(instruction),
            };

            let extras = parse_extras(&event).expect("extras must parse");
            assert_eq!(extras.authority, Some(authority.to_string()));
            assert_eq!(extras.token_a_vault, Some(token_a_vault.to_string()));
            assert_eq!(extras.token_b_vault, Some(token_b_vault.to_string()));
            assert_eq!(extras.pool_mint, Some(pool_mint.to_string()));
            assert_eq!(extras.fee_account, Some(fee_account.to_string()));
            assert_eq!(extras.token_program, Some(token_program.to_string()));
            assert_eq!(extras.oracle_main, Some(oracle_main.to_string()));
            assert_eq!(extras.oracle_sub, Some(oracle_sub.to_string()));
            assert_eq!(extras.oracle_pc, Some(oracle_pc.to_string()));
            assert_eq!(extras.vault_a_balance, Some(42));
            assert_eq!(extras.vault_b_balance, Some(84));
        }
    }
}

mod whirlpool {
    use super::shared::{lookup_account_string, pubkey_from_bytes, InstructionAccountLookup};
    use super::*;

    pub const WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicV1cqQhBYtbJX3Yq9z7Dfc7gXLMGr5vLyN";
    pub const WHIRLPOOL_INPUT_UTILIZATION_BPS: u64 = 4_000;
    static WHIRLPOOL_PROGRAM_PUBKEY: Lazy<Pubkey> =
        Lazy::new(|| Pubkey::from_str(WHIRLPOOL_PROGRAM_ID).expect("static Whirlpool program id"));

    #[derive(Debug, Clone, Default)]
    pub struct WhirlpoolPoolExtras {
        pub token_vault_a: Option<String>,
        pub token_vault_b: Option<String>,
        pub token_mint_a: Option<String>,
        pub token_mint_b: Option<String>,
        pub tick_spacing: Option<i32>,
        pub sqrt_price_x64: Option<u128>,
        pub liquidity: Option<u128>,
        pub tick_current_index: Option<i32>,
        pub fee_rate: Option<u16>,
        pub protocol_fee_rate: Option<u16>,
        pub oracle: Option<String>,
        pub tick_array0: Option<String>,
        pub tick_array1: Option<String>,
        pub tick_array2: Option<String>,
    }

    pub(crate) fn parse_extras(event: &PoolLiquidityChangeEvent) -> Option<WhirlpoolPoolExtras> {
        let dex = event.dex.as_ref()?;
        let program_key = pubkey_from_bytes(&dex.program_address)?;
        if program_key != *WHIRLPOOL_PROGRAM_PUBKEY {
            return None;
        }

        let instruction = event.instruction.as_ref()?;
        let program = instruction.program.as_ref()?;
        let lookup = InstructionAccountLookup::new(program, &instruction.accounts);

        let extras = WhirlpoolPoolExtras {
            token_vault_a: lookup_account_string(&lookup, &["tokenVaultA", "token_vault_a"]),
            token_vault_b: lookup_account_string(&lookup, &["tokenVaultB", "token_vault_b"]),
            oracle: lookup_account_string(&lookup, &["oracle"]),
            tick_array0: lookup_account_string(&lookup, &["tickArray0", "tick_array0"]),
            tick_array1: lookup_account_string(&lookup, &["tickArray1", "tick_array1"]),
            tick_array2: lookup_account_string(&lookup, &["tickArray2", "tick_array2"]),
            ..Default::default()
        };

        extras_if_any(extras)
    }

    fn extras_if_any(extras: WhirlpoolPoolExtras) -> Option<WhirlpoolPoolExtras> {
        if extras.token_vault_a.is_none()
            && extras.token_vault_b.is_none()
            && extras.oracle.is_none()
            && extras.tick_array0.is_none()
            && extras.tick_array1.is_none()
            && extras.tick_array2.is_none()
        {
            None
        } else {
            Some(extras)
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;
    use crate::proto::solana_messages::{
        Currency, DexInfo, DexMarket, ParsedIdlInstruction, PoolLiquidityChangeEvent, PoolSide,
        Program, TokenInfo,
    };

    pub fn proto_account(address: &Pubkey) -> ProtoAccount {
        ProtoAccount {
            address: address.to_bytes().to_vec(),
            is_signer: false,
            is_writable: false,
            token: None,
        }
    }

    pub fn spl_token_account(
        address: &Pubkey,
        mint: &Pubkey,
        owner: &Pubkey,
        program: &Pubkey,
    ) -> ProtoAccount {
        ProtoAccount {
            address: address.to_bytes().to_vec(),
            is_signer: false,
            is_writable: true,
            token: Some(TokenInfo {
                mint: mint.to_bytes().to_vec(),
                owner: owner.to_bytes().to_vec(),
                decimals: 6,
                program_id: program.to_bytes().to_vec(),
            }),
        }
    }

    pub fn proto_currency(mint: &Pubkey) -> Currency {
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

    pub fn pool_side(post_amount: u64) -> PoolSide {
        PoolSide {
            change_amount: 0,
            post_amount,
        }
    }

    pub fn raydium_dex_info() -> DexInfo {
        DexInfo {
            program_address: Pubkey::from_str(super::raydium_v3::RAYDIUM_V3_PROGRAM_ID)
                .expect("raydium program id")
                .to_bytes()
                .to_vec(),
            protocol_name: String::from("Raydium v3"),
            protocol_family: String::from("Raydium"),
        }
    }

    pub fn cp_amm_dex_info() -> DexInfo {
        DexInfo {
            program_address: Pubkey::from_str(super::cp_amm::CP_AMM_PROGRAM_ID)
                .expect("cp_amm program id")
                .to_bytes()
                .to_vec(),
            protocol_name: String::from("cp_amm"),
            protocol_family: String::from("cp_amm"),
        }
    }

    pub fn cp_amm_market(pool_id: &Pubkey, base: &Pubkey, quote: &Pubkey) -> DexMarket {
        DexMarket {
            market_address: pool_id.to_bytes().to_vec(),
            base_currency: Some(proto_currency(base)),
            quote_currency: Some(proto_currency(quote)),
        }
    }

    pub fn lb_clmm_dex_info() -> DexInfo {
        DexInfo {
            program_address: Pubkey::from_str(super::lb_clmm::LB_CLMM_PROGRAM_ID)
                .expect("lb_clmm program id")
                .to_bytes()
                .to_vec(),
            protocol_name: String::from("lb_clmm"),
            protocol_family: String::from("lb_clmm"),
        }
    }

    pub fn lb_clmm_market(pool_id: &Pubkey, base: &Pubkey, quote: &Pubkey) -> DexMarket {
        DexMarket {
            market_address: pool_id.to_bytes().to_vec(),
            base_currency: Some(proto_currency(base)),
            quote_currency: Some(proto_currency(quote)),
        }
    }

    pub fn lifinity_dex_info() -> DexInfo {
        DexInfo {
            program_address: Pubkey::from_str(super::lifinity_v2::LIFINITY_V2_PROGRAM_ID)
                .expect("lifinity program id")
                .to_bytes()
                .to_vec(),
            protocol_name: String::from("Lifinity v2"),
            protocol_family: String::from("Lifinity"),
        }
    }

    pub fn raydium_market(pool_id: &Pubkey, base: &Pubkey, quote: &Pubkey) -> DexMarket {
        DexMarket {
            market_address: pool_id.to_bytes().to_vec(),
            base_currency: Some(proto_currency(base)),
            quote_currency: Some(proto_currency(quote)),
        }
    }

    pub fn parsed_instruction(
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

    pub fn parsed_instruction_with_method(
        account_names: Vec<&str>,
        accounts: Vec<ProtoAccount>,
        method: &str,
    ) -> ParsedIdlInstruction {
        let mut instruction = parsed_instruction(account_names, accounts);
        if let Some(program) = instruction.program.as_mut() {
            program.name = method.to_string();
            program.method = method.to_string();
        }
        instruction
    }
}
