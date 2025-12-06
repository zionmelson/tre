use std::{
    cmp::Ordering,
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use dashmap::{DashMap, DashSet};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, UiAccountEncoding,
};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_client::rpc_response::{UiAccount, UiAccountData};
use solana_sdk::pubkey::Pubkey;
use spl_token_2022::extension::{
    transfer_fee::{TransferFee as SplTokenTransferFee, TransferFeeConfig as SplTransferFeeConfig},
    BaseStateWithExtensions, StateWithExtensionsOwned,
};
use spl_token_2022::state::Mint as Token2022Mint;
use tracing::{debug, info, trace, warn};
use uint::construct_uint;

use crate::builder::{AmmV3PoolMeta, BuilderHandle};
use crate::hydration::token_fees::{apply_post_fee_amount, apply_pre_fee_amount, TokenTransferFee};
use crate::hydration::types::{
    HydrationComponent, PoolBidirectionalQuotes, PoolQuoteSide, PoolSnapshot, PoolUpdate,
    RaydiumTransferFeeConfig, RaydiumV3PoolExtras, RAYDIUM_V3_INPUT_UTILIZATION_BPS,
};
use crate::template::{TemplateContext, TransferFeeConfigHint};

use super::hydration::AsyncHydrator;
use super::{
    build_quote_response, ProtocolAdapter, QuoteRequest, QuoteResponse, QuoteStatus, SwapDirection,
};

const RAYDIUM_V3_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const RAYDIUM_CLMM_PROGRAM: Pubkey =
    solana_sdk::pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK");
const POOL_VAULT_SEED: &[u8] = b"pool_vault";
const POOL_TICK_ARRAY_BITMAP_SEED: &[u8] = b"pool_tick_array_bitmap_extension";
const TICK_ARRAY_STATE_DISCRIMINATOR: [u8; 8] = [192, 155, 85, 205, 49, 249, 129, 42];
const POOL_STATE_DISCRIMINATOR: [u8; 8] = [247, 237, 227, 245, 215, 195, 222, 70];
const AMM_CONFIG_STATE_DISCRIMINATOR: [u8; 8] = [218, 244, 33, 104, 203, 203, 43, 111];
const ANCHOR_DISCRIMINATOR_LEN: usize = 8;
const PUBKEY_BYTES: usize = 32;
const TICK_ARRAY_TICK_COUNT: usize = 60;
const TICK_STATE_PADDING_BYTES: usize = 13 * 4;
const TICK_STATE_SERIALIZED_LEN: usize =
    4 + 16 + 16 + 16 + 16 + (3 * 16) + TICK_STATE_PADDING_BYTES;
const MIN_RAYDIUM_TICK_ARRAY_ACCOUNT_LEN: usize = ANCHOR_DISCRIMINATOR_LEN
    + PUBKEY_BYTES
    + 4
    + (TICK_STATE_SERIALIZED_LEN * TICK_ARRAY_TICK_COUNT)
    + 1
    + 8
    + 107;
const MIN_RAYDIUM_AMM_CONFIG_ACCOUNT_LEN: usize = ANCHOR_DISCRIMINATOR_LEN + 109;
const SPL_TOKEN_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const SPL_TOKEN_2022_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBq2FXk7XJ68");
const MEMO_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

#[allow(clippy::assign_op_pattern, clippy::manual_div_ceil)]
mod u256 {
    use super::construct_uint;

    construct_uint! {
        pub struct U256(4);
    }
}
pub use u256::U256;

#[derive(Debug, Clone, Copy)]
struct ClmmSwapSimulation {
    executed_input_amount: u64,
    net_output_amount: u64,
    sqrt_price_after: u128,
    protocol_fee_amount: u64,
    fund_fee_amount: u64,
}

#[derive(Debug, Clone, Default)]
struct TickArrayCache {
    zero_for_one: Option<TickArrayCacheEntry>,
    one_for_zero: Option<TickArrayCacheEntry>,
}

#[derive(Debug, Clone)]
struct TickArrayCacheEntry {
    hydrated_at: Instant,
    accounts: Vec<Pubkey>,
}

const REQUIRED_TICK_ARRAYS_PER_SIDE: usize = 3;

#[derive(Clone)]
struct SnapshotStatusLogEntry {
    status: QuoteStatus,
    logged_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub struct TransferFeeConfigSummary {
    epoch: u64,
    maximum_fee: u64,
    basis_points: u16,
}

impl From<RaydiumTransferFeeConfig> for TransferFeeConfigSummary {
    fn from(value: RaydiumTransferFeeConfig) -> Self {
        Self {
            epoch: value.epoch,
            maximum_fee: value.maximum_fee,
            basis_points: value.basis_points,
        }
    }
}

impl From<TransferFeeConfigSummary> for TransferFeeConfigHint {
    fn from(value: TransferFeeConfigSummary) -> Self {
        Self {
            epoch: value.epoch,
            maximum_fee: value.maximum_fee,
            basis_points: value.basis_points,
        }
    }
}

impl From<TransferFeeConfigSummary> for RaydiumTransferFeeConfig {
    fn from(value: TransferFeeConfigSummary) -> Self {
        Self {
            epoch: value.epoch,
            maximum_fee: value.maximum_fee,
            basis_points: value.basis_points,
        }
    }
}

impl From<TransferFeeConfigSummary> for TokenTransferFee {
    fn from(value: TransferFeeConfigSummary) -> Self {
        let fee = SplTokenTransferFee {
            epoch: value.epoch.into(),
            maximum_fee: value.maximum_fee.into(),
            transfer_fee_basis_points: value.basis_points.into(),
        };
        TokenTransferFee::new(fee)
    }
}

#[derive(Debug, Clone, Copy)]
struct MintAccountMeta {
    program: Pubkey,
    transfer_fee_config: Option<TransferFeeConfigSummary>,
}

const DEFAULT_RAYDIUM_HYDRATION_RETRY_MS: u64 = 10_000;
const SNAPSHOT_FAILURE_LOG_INTERVAL_MS: u64 = 15_000;
const STATE_MISS_LOG_INTERVAL_MS: u64 = 15_000;

#[derive(Debug, Clone)]
pub struct RaydiumV3AdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
    pub hydration_retry_ms: u64,
    pub tick_array_cache_ttl_ms: u64,
    pub max_swap_utilization_bps: u64,
}

impl Default for RaydiumV3AdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            // tick array hydration can take several seconds; cache once fetched.
            rpc_timeout_ms: 15_000,
            hydration_retry_ms: DEFAULT_RAYDIUM_HYDRATION_RETRY_MS,
            tick_array_cache_ttl_ms: 45_000,
            max_swap_utilization_bps: RAYDIUM_V3_INPUT_UTILIZATION_BPS,
        }
    }
}

pub struct RaydiumV3Adapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, RaydiumV3State>,
    config: RaydiumV3AdapterConfig,
    hydrator: Option<RaydiumPoolHydrator>,
    template_builder: Option<BuilderHandle>,
    registered_templates: DashSet<Pubkey>,
    snapshot_failure_log: DashMap<(Pubkey, bool), SnapshotStatusLogEntry>,
    missing_state_log: DashMap<Pubkey, Instant>,
}

impl RaydiumV3Adapter {
    pub fn new(config: RaydiumV3AdapterConfig, template_builder: Option<BuilderHandle>) -> Self {
        let program_id = Pubkey::from_str(RAYDIUM_V3_PROGRAM_ID)
            .expect("static Raydium AMM v3 program id should be valid");
        let hydrator = RaydiumPoolHydrator::new(&config, program_id);
        if let Some(hydrator_ref) = hydrator.as_ref() {
            info!(
                target = "adapter::raydium_v3",
                endpoints = hydrator_ref.client_count(),
                "raydium pool hydrator online"
            );
        } else {
            debug!(
                target = "adapter::raydium_v3",
                "raydium pool hydrator disabled (no rpc endpoints)"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            hydrator,
            template_builder,
            registered_templates: DashSet::new(),
            snapshot_failure_log: DashMap::new(),
            missing_state_log: DashMap::new(),
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<RaydiumV3State> {
        self.state.get(pool).map(|entry| entry.clone())
    }

    fn evict_stale(&self, current_slot: u64) {
        let ttl = self.config.stale_after_ms;
        self.state.retain(|_, state| {
            let slot_fresh = current_slot.saturating_sub(state.last_update_slot)
                <= self.config.stale_slot_threshold;
            let instant_fresh = state.last_update_at.elapsed().as_millis() as u64 <= ttl;
            if !(slot_fresh && instant_fresh) {
                trace!(
                    target = "adapter::raydium_v3",
                    pool = %state.pool_id,
                    last_slot = state.last_update_slot,
                    "evicting stale pool state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: RaydiumV3PoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::raydium_v3",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let Some(base_mint) = parse_pubkey(&update.base_mint) else {
            warn!(target = "adapter::raydium_v3", pool = %update.pool_id, "invalid base mint");
            return;
        };

        let Some(quote_mint) = parse_pubkey(&update.quote_mint) else {
            warn!(target = "adapter::raydium_v3", pool = %update.pool_id, "invalid quote mint");
            return;
        };

        let extras = self.enrich_extras(pool_pubkey, base_mint, quote_mint, extras);
        let RaydiumV3PoolExtras {
            tick_spacing,
            sqrt_price_x64,
            liquidity,
            current_tick,
            amm_config,
            observation_state,
            pool_bump,
            observation_bump,
            vault_bumps,
            trade_fee_rate,
            protocol_fee_rate,
            fund_fee_rate,
            base_token_program,
            quote_token_program,
            tick_array_bitmap_extension,
            pool_status,
            base_transfer_fee_config,
            quote_transfer_fee_config,
        } = extras;

        let trade_fee_bps = trade_fee_rate.map(fee_bps_from_hundredth_bip);
        let protocol_fee_bps = protocol_fee_rate.and_then(fee_bps_from_hundredth_bip_u16);
        let fund_fee_bps = fund_fee_rate.and_then(fee_bps_from_hundredth_bip_u16);
        let base_transfer_fee = base_transfer_fee_config.map(TransferFeeConfigSummary::from);
        let quote_transfer_fee = quote_transfer_fee_config.map(TransferFeeConfigSummary::from);

        let tick_array_cache = self
            .state
            .get(&pool_pubkey)
            .map(|entry| entry.tick_array_cache.clone())
            .unwrap_or_default();

        let state = RaydiumV3State {
            pool_id: pool_pubkey,
            base_mint,
            quote_mint,
            base_token_program: base_token_program.unwrap_or(SPL_TOKEN_PROGRAM_ID),
            quote_token_program: quote_token_program.unwrap_or(SPL_TOKEN_PROGRAM_ID),
            amm_config,
            observation_state,
            tick_spacing,
            sqrt_price_x64,
            current_tick,
            liquidity,
            vault_base: update.base_post_amount,
            vault_quote: update.quote_post_amount,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
            pool_bump,
            observation_bump,
            vault_bumps,
            trade_fee_bps,
            protocol_fee_bps,
            fund_fee_bps,
            tick_array_bitmap_extension,
            pool_status,
            base_transfer_fee,
            quote_transfer_fee,
            tick_array_cache,
        };

        trace!(
            target = "adapter::raydium_v3",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting Raydium pool state"
        );

        self.register_templates_with_builder(&state);
        self.request_tick_arrays(&state);
        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn meta_ready(state: &RaydiumV3State) -> bool {
        state.tick_spacing.is_some()
            && state.sqrt_price_x64.is_some()
            && state.liquidity.is_some()
            && state.current_tick.is_some()
    }

    fn tick_arrays_ready(&self, state: &RaydiumV3State) -> bool {
        let ttl = Duration::from_millis(self.config.tick_array_cache_ttl_ms.max(1));
        Self::tick_cache_entry_ready(state.tick_array_cache.zero_for_one.as_ref(), ttl)
            && Self::tick_cache_entry_ready(state.tick_array_cache.one_for_zero.as_ref(), ttl)
    }

    fn tick_cache_entry_ready(entry: Option<&TickArrayCacheEntry>, ttl: Duration) -> bool {
        let Some(cache) = entry else {
            return false;
        };
        if cache.accounts.len() < REQUIRED_TICK_ARRAYS_PER_SIDE {
            return false;
        }
        ttl.as_millis() == 0 || cache.hydrated_at.elapsed() <= ttl
    }

    fn request_tick_arrays(&self, state: &RaydiumV3State) {
        if let Some(hydrator) = self.hydrator.as_ref() {
            if let (Some(spacing), Some(current_tick)) = (state.tick_spacing, state.current_tick) {
                hydrator.warm_tick_arrays(state.pool_id, spacing, current_tick);
            }
        }
    }

    fn enrich_extras(
        &self,
        pool: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
        mut extras: RaydiumV3PoolExtras,
    ) -> RaydiumV3PoolExtras {
        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.tick_spacing = extras.tick_spacing.or(Some(meta.tick_spacing));
            extras.sqrt_price_x64 = extras.sqrt_price_x64.or(Some(meta.sqrt_price_x64));
            extras.liquidity = extras.liquidity.or(Some(meta.liquidity));
            extras.current_tick = extras.current_tick.or(Some(meta.current_tick));
            // Always trust the on-chain pool account for amm_config so stale CoreCast
            // metadata cannot point us at a non-existent config account.
            extras.amm_config = Some(meta.amm_config);
            extras.observation_state = extras.observation_state.or(Some(meta.observation_state));
            extras.pool_bump = extras.pool_bump.or(Some(meta.pool_bump));
            extras.trade_fee_rate = extras.trade_fee_rate.or(meta.trade_fee_rate);
            extras.protocol_fee_rate = extras.protocol_fee_rate.or(meta.protocol_fee_rate);
            extras.fund_fee_rate = extras.fund_fee_rate.or(meta.fund_fee_rate);
            extras.tick_array_bitmap_extension = extras
                .tick_array_bitmap_extension
                .or(Some(meta.tick_array_bitmap_extension));
            extras.pool_status = extras.pool_status.or(Some(meta.pool_status));
        }

        extras.base_token_program = extras
            .base_token_program
            .or_else(|| hydrator.lookup_mint_program(base_mint));
        extras.quote_token_program = extras
            .quote_token_program
            .or_else(|| hydrator.lookup_mint_program(quote_mint));

        if extras.base_transfer_fee_config.is_none() {
            extras.base_transfer_fee_config =
                hydrator.mint_transfer_fee_config(base_mint).map(Into::into);
        }
        if extras.quote_transfer_fee_config.is_none() {
            extras.quote_transfer_fee_config = hydrator
                .mint_transfer_fee_config(quote_mint)
                .map(Into::into);
        }

        extras
    }

    fn register_templates_with_builder(&self, state: &RaydiumV3State) {
        let Some(builder) = self.template_builder.as_ref() else {
            return;
        };
        if self.registered_templates.contains(&state.pool_id) {
            let alt_missing = builder
                .alt_manager()
                .handle_for_pool(state.pool_id)
                .is_none();
            if !alt_missing {
                return;
            }
            debug!(
                target = "adapter::raydium_v3",
                pool = %state.pool_id,
                "ALT handle missing; re-registering pool templates"
            );
        }
        let Some(fee_bps) = state.trade_fee_bps else {
            return;
        };
        let (Some(amm_config), Some(observation_state)) =
            (state.amm_config, state.observation_state)
        else {
            warn!(
                target = "adapter::raydium_v3",
                pool = %state.pool_id,
                "skipping template registration due to missing pool metadata"
            );
            return;
        };

        let fee_tier_bps = u16::try_from(fee_bps).unwrap_or(u16::MAX);
        let (mint0, mint1, _program0, _program1) = order_mints_with_programs(
            state.base_mint,
            state.quote_mint,
            state.base_token_program,
            state.quote_token_program,
        );
        let owner_ata0 = match builder.token_account_for_mint(&mint0) {
            Some(pk) => pk,
            None => {
                warn!(
                    target = "adapter::raydium_v3",
                    pool = %state.pool_id,
                    mint = %mint0,
                    "missing configured owner ATA for mint"
                );
                return;
            }
        };
        let owner_ata1 = match builder.token_account_for_mint(&mint1) {
            Some(pk) => pk,
            None => {
                warn!(
                    target = "adapter::raydium_v3",
                    pool = %state.pool_id,
                    mint = %mint1,
                    "missing configured owner ATA for mint"
                );
                return;
            }
        };
        let alt_accounts = build_raydium_alt_accounts(
            state.pool_id,
            state.base_mint,
            state.quote_mint,
            state.base_token_program,
            state.quote_token_program,
            amm_config,
            observation_state,
            &self.program_id,
            owner_ata0,
            owner_ata1,
        );

        builder.register_amm_v3_pool(AmmV3PoolMeta {
            pool_id: state.pool_id,
            fee_tier_bps,
            base_mint: state.base_mint,
            quote_mint: state.quote_mint,
            base_token_program: Some(state.base_token_program),
            quote_token_program: Some(state.quote_token_program),
            amm_config,
            observation_state,
            alt_accounts,
            alt_handle: None,
        });

        self.registered_templates.insert(state.pool_id);
    }

    fn log_state_cache_miss(&self, pool: Pubkey, slot: u64) {
        if let Some(entry) = self.missing_state_log.get(&pool) {
            if (entry.value().elapsed().as_millis() as u64) < STATE_MISS_LOG_INTERVAL_MS {
                return;
            }
        }
        self.missing_state_log.insert(pool, Instant::now());
        warn!(
            target = "adapter::raydium_v3",
            pool = %pool,
            slot,
            hydrator_online = self.hydrator.is_some(),
            "raydium strict price snapshot requested before hydration"
        );
    }

    fn clear_snapshot_issue(&self, pool: Pubkey, base_to_quote: bool) {
        self.snapshot_failure_log.remove(&(pool, base_to_quote));
    }

    fn record_snapshot_issue(
        &self,
        state: &RaydiumV3State,
        slot: u64,
        base_to_quote: bool,
        status: QuoteStatus,
        reason: &'static str,
        probe_amount: Option<u64>,
    ) {
        if status == QuoteStatus::Ok {
            self.clear_snapshot_issue(state.pool_id, base_to_quote);
            return;
        }

        let key = (state.pool_id, base_to_quote);
        let should_log = match self.snapshot_failure_log.get(&key) {
            Some(entry) => {
                let cached = entry.value();
                cached.status != status
                    || cached.logged_at.elapsed().as_millis() as u64
                        >= SNAPSHOT_FAILURE_LOG_INTERVAL_MS
            }
            None => true,
        };

        if !should_log {
            return;
        }

        self.snapshot_failure_log.insert(
            key,
            SnapshotStatusLogEntry {
                status,
                logged_at: Instant::now(),
            },
        );

        let tick_cache_zero = state
            .tick_array_cache
            .zero_for_one
            .as_ref()
            .map(|entry| entry.accounts.len())
            .unwrap_or(0);
        let tick_cache_one = state
            .tick_array_cache
            .one_for_zero
            .as_ref()
            .map(|entry| entry.accounts.len())
            .unwrap_or(0);
        let tick_cache_zero_age = state
            .tick_array_cache
            .zero_for_one
            .as_ref()
            .map(|entry| entry.hydrated_at.elapsed().as_millis() as u64);
        let tick_cache_one_age = state
            .tick_array_cache
            .one_for_zero
            .as_ref()
            .map(|entry| entry.hydrated_at.elapsed().as_millis() as u64);
        let hydrator_clients = self
            .hydrator
            .as_ref()
            .map(|hydrator| hydrator.client_count())
            .unwrap_or(0);

        warn!(
            target = "adapter::raydium_v3",
            pool = %state.pool_id,
            slot,
            direction = if base_to_quote {
                "base_to_quote"
            } else {
                "quote_to_base"
            },
            status = ?status,
            reason,
            probe_amount,
            last_update_slot = state.last_update_slot,
            base_vault = %state.vault_base,
            quote_vault = %state.vault_quote,
            has_tick_spacing = state.tick_spacing.is_some(),
            has_current_tick = state.current_tick.is_some(),
            has_sqrt_price = state.sqrt_price_x64.is_some(),
            has_liquidity = state.liquidity.is_some(),
            cached_zero_tick_arrays = tick_cache_zero,
            cached_zero_age_ms = tick_cache_zero_age,
            cached_one_tick_arrays = tick_cache_one,
            cached_one_age_ms = tick_cache_one_age,
            hydrator_clients,
            "raydium strict price quote unavailable"
        );
    }

    fn decorate_quote_context(
        &self,
        context: &mut TemplateContext,
        state: &RaydiumV3State,
        input_mint: Pubkey,
        output_mint: Pubkey,
    ) {
        context.requires_memo = false;
        context.requires_dont_front = false;
        if context.raydium_tick_array_bitmap_extension.is_none() {
            context.raydium_tick_array_bitmap_extension = state.tick_array_bitmap_extension;
        }
        context.raydium_pool_status = state.pool_status;
        context.raydium_protocol_fee_bps = state.protocol_fee_bps;
        context.raydium_fund_fee_bps = state.fund_fee_bps;
        context.input_transfer_fee_config = Self::transfer_fee_hint_for_mint(state, input_mint);
        context.output_transfer_fee_config = Self::transfer_fee_hint_for_mint(state, output_mint);
        if context.input_token_program.is_none() {
            context.input_token_program = self.mint_program_for(state, input_mint);
        }
        if context.output_token_program.is_none() {
            context.output_token_program = self.mint_program_for(state, output_mint);
        }

        let builder = self.template_builder.as_ref();
        if context.payer.is_none() {
            if let Some(builder_handle) = builder {
                context.payer = Some(builder_handle.payer_pubkey());
            }
        }

        if context.input_token_account.is_none() {
            if let Some(builder_handle) = builder {
                if let Some(ata) = builder_handle.token_account_for_mint(&input_mint) {
                    context.input_token_account = Some(ata);
                } else {
                    warn!(
                        target = "adapter::raydium_v3",
                        pool = %state.pool_id,
                        mint = %input_mint,
                        "missing configured input token ATA"
                    );
                }
            }
        }

        if context.output_token_account.is_none() {
            if let Some(builder_handle) = builder {
                if let Some(ata) = builder_handle.token_account_for_mint(&output_mint) {
                    context.output_token_account = Some(ata);
                } else {
                    warn!(
                        target = "adapter::raydium_v3",
                        pool = %state.pool_id,
                        mint = %output_mint,
                        "missing configured output token ATA"
                    );
                }
            }
        }

        if let Some(zero_for_one) = classify_swap_direction(state, input_mint, output_mint) {
            if let Some(arrays) = self.resolve_tick_arrays(state, zero_for_one) {
                context.whirlpool_tick_arrays = arrays;
            }
        }
    }

    fn mint_program_for(&self, state: &RaydiumV3State, mint: Pubkey) -> Option<Pubkey> {
        if mint == state.base_mint {
            Some(state.base_token_program)
        } else if mint == state.quote_mint {
            Some(state.quote_token_program)
        } else {
            None
        }
    }

    fn transfer_fee_hint_for_mint(
        state: &RaydiumV3State,
        mint: Pubkey,
    ) -> Option<TransferFeeConfigHint> {
        Self::transfer_fee_summary_for_mint(state, mint).map(TransferFeeConfigHint::from)
    }

    fn transfer_fee_model_for_mint(
        state: &RaydiumV3State,
        mint: Pubkey,
    ) -> Option<TokenTransferFee> {
        Self::transfer_fee_summary_for_mint(state, mint).map(TokenTransferFee::from)
    }

    fn transfer_fee_summary_for_mint(
        state: &RaydiumV3State,
        mint: Pubkey,
    ) -> Option<TransferFeeConfigSummary> {
        if mint == state.base_mint {
            state.base_transfer_fee
        } else if mint == state.quote_mint {
            state.quote_transfer_fee
        } else {
            None
        }
    }

    fn clamp_request_amount(&self, state: &RaydiumV3State, req: &QuoteRequest) -> u64 {
        if req.amount == 0 {
            return 0;
        }
        let reserve = if req.input_mint == state.base_mint {
            state.vault_base
        } else if req.input_mint == state.quote_mint {
            state.vault_quote
        } else {
            return req.amount;
        };
        let utilization_bps = self.config.max_swap_utilization_bps.min(10_000);
        let capped = reserve.saturating_mul(utilization_bps as u128) / 10_000;
        if capped == 0 {
            return 0;
        }
        let limit = capped.min(u64::MAX as u128) as u64;
        if limit == 0 {
            return 0;
        }
        req.amount.min(limit)
    }

    fn clamp_request_output_amount(&self, state: &RaydiumV3State, req: &QuoteRequest) -> u64 {
        if req.amount == 0 {
            return 0;
        }
        let reserve = if req.output_mint == state.base_mint {
            state.vault_base
        } else if req.output_mint == state.quote_mint {
            state.vault_quote
        } else {
            return 0;
        };
        let utilization_bps = self.config.max_swap_utilization_bps.min(10_000);
        let capped = reserve.saturating_mul(utilization_bps as u128) / 10_000;
        if capped == 0 {
            return 0;
        }
        let limit = capped.min(u64::MAX as u128) as u64;
        if limit == 0 {
            return 0;
        }
        req.amount.min(limit)
    }

    fn resolve_tick_arrays(
        &self,
        state: &RaydiumV3State,
        zero_for_one: bool,
    ) -> Option<Vec<Pubkey>> {
        let ttl = Duration::from_millis(self.config.tick_array_cache_ttl_ms.max(1));
        if let Some(hydrator) = self.hydrator.as_ref() {
            if let (Some(tick_spacing), Some(current_tick)) =
                (state.tick_spacing, state.current_tick)
            {
                if let Some(arrays) = hydrator.preferred_tick_arrays_for_swap(
                    state.pool_id,
                    tick_spacing,
                    current_tick,
                    zero_for_one,
                ) {
                    self.update_tick_array_cache(state.pool_id, zero_for_one, arrays.clone());
                    return Some(arrays);
                }
            }
        }
        self.cached_tick_arrays(state.pool_id, zero_for_one, ttl)
    }

    fn update_tick_array_cache(&self, pool_id: Pubkey, zero_for_one: bool, accounts: Vec<Pubkey>) {
        if accounts.is_empty() {
            return;
        }
        if let Some(mut entry) = self.state.get_mut(&pool_id) {
            let cache_entry = TickArrayCacheEntry {
                hydrated_at: Instant::now(),
                accounts,
            };
            if zero_for_one {
                entry.tick_array_cache.zero_for_one = Some(cache_entry);
            } else {
                entry.tick_array_cache.one_for_zero = Some(cache_entry);
            }
        }
    }

    fn cached_tick_arrays(
        &self,
        pool_id: Pubkey,
        zero_for_one: bool,
        ttl: Duration,
    ) -> Option<Vec<Pubkey>> {
        let entry = self.state.get(&pool_id)?;
        let cache_opt = if zero_for_one {
            entry.tick_array_cache.zero_for_one.as_ref()
        } else {
            entry.tick_array_cache.one_for_zero.as_ref()
        }?;
        if cache_opt.accounts.is_empty() {
            return None;
        }
        if ttl.as_millis() > 0 && cache_opt.hydrated_at.elapsed() > ttl {
            return None;
        }
        Some(cache_opt.accounts.clone())
    }

    fn simulate_clmm_exact_in(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Result<ClmmSwapSimulation, QuoteStatus> {
        let hydrator = self.hydrator.as_ref().ok_or(QuoteStatus::MissingState)?;
        let tick_spacing = state.tick_spacing.ok_or(QuoteStatus::MissingState)?;
        let mut current_tick = state.current_tick.ok_or(QuoteStatus::MissingState)?;
        let sqrt_price_x64 = state.sqrt_price_x64.ok_or(QuoteStatus::MissingState)?;
        let mut liquidity = state.liquidity.ok_or(QuoteStatus::MissingState)?;
        if liquidity == 0 || req.amount == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }

        let Some(zero_for_one) = classify_swap_direction(state, req.input_mint, req.output_mint)
        else {
            return Err(QuoteStatus::MissingState);
        };

        let input_transfer_fee = Self::transfer_fee_model_for_mint(state, req.input_mint);
        let output_transfer_fee = Self::transfer_fee_model_for_mint(state, req.output_mint);
        let (swap_budget, _) = apply_post_fee_amount(req.amount, input_transfer_fee)
            .ok_or(QuoteStatus::TokenFeeUnsupported)?;
        if swap_budget == 0 {
            return Err(QuoteStatus::TokenFeeUnsupported);
        }

        let mut sqrt_price_x96 = math::to_x96(sqrt_price_x64);
        let sqrt_limit_tick = if zero_for_one {
            math::MIN_TICK
        } else {
            math::MAX_TICK
        };
        let sqrt_price_limit_x96 =
            math::sqrt_price_at_tick_x96(sqrt_limit_tick).ok_or(QuoteStatus::MissingState)?;

        let mut cursor = TickArrayCursor::new(hydrator, state.pool_id, tick_spacing)
            .ok_or(QuoteStatus::MissingTickArrays)?;
        let mut amount_remaining = swap_budget as u128;
        let mut pool_input_consumed = 0u128;
        let mut trade_fee_total = 0u128;
        let mut amount_out_accum = 0u128;
        let mut protocol_fee_total = 0u128;
        let mut fund_fee_total = 0u128;
        let fee_bps = state.trade_fee_bps.unwrap_or(DEFAULT_FEE_BPS);
        let protocol_rate = u128::from(state.protocol_fee_bps.unwrap_or(0));
        let fund_rate = u128::from(state.fund_fee_bps.unwrap_or(0));
        let mut steps = 0usize;

        while amount_remaining > 0 && liquidity > 0 && sqrt_price_x96 != sqrt_price_limit_x96 {
            steps += 1;
            if steps > 512 {
                break;
            }

            let next_tick = cursor.next_initialized_tick(current_tick, zero_for_one);
            let mut target_price = sqrt_price_limit_x96;
            let mut crossing = None;
            if let Some(tick_info) = next_tick {
                let tick_price = math::sqrt_price_at_tick_x96(tick_info.index)
                    .ok_or(QuoteStatus::MissingState)?;
                if zero_for_one {
                    if tick_price > target_price {
                        target_price = tick_price;
                    }
                } else if tick_price < target_price {
                    target_price = tick_price;
                }
                crossing = Some(tick_info);
            }

            let step = math::compute_swap_step(
                sqrt_price_x96,
                target_price,
                liquidity,
                amount_remaining,
                fee_bps,
                zero_for_one,
            )
            .ok_or(QuoteStatus::MissingTickArrays)?;

            if step.amount_in == 0 {
                break;
            }

            sqrt_price_x96 = step.sqrt_price_next;
            let total_spent = step.amount_in.saturating_add(step.fee_amount);
            amount_remaining = amount_remaining.saturating_sub(total_spent);
            pool_input_consumed = pool_input_consumed.saturating_add(step.amount_in);
            trade_fee_total = trade_fee_total.saturating_add(step.fee_amount);
            amount_out_accum = amount_out_accum.saturating_add(step.amount_out);

            if protocol_rate > 0 {
                let delta =
                    step.fee_amount.saturating_mul(protocol_rate) / RAYDIUM_FEE_RATE_DENOMINATOR;
                protocol_fee_total = protocol_fee_total.saturating_add(delta);
            }
            if fund_rate > 0 {
                let delta =
                    step.fee_amount.saturating_mul(fund_rate) / RAYDIUM_FEE_RATE_DENOMINATOR;
                fund_fee_total = fund_fee_total.saturating_add(delta);
            }

            if sqrt_price_x96 == target_price {
                if let Some(init_tick) = crossing {
                    current_tick = if zero_for_one {
                        init_tick.index - 1
                    } else {
                        init_tick.index
                    };
                    liquidity =
                        apply_liquidity_delta(liquidity, init_tick.liquidity_net, zero_for_one)
                            .ok_or(QuoteStatus::InsufficientLiquidity)?;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if amount_out_accum == 0 || pool_input_consumed == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }

        let amount_including_fees = pool_input_consumed.saturating_add(trade_fee_total);
        if amount_including_fees == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }
        let amount_including_fees_u64 = amount_including_fees.min(u64::MAX as u128) as u64;
        let (executed_input_amount, _input_fee_amount) =
            apply_pre_fee_amount(amount_including_fees_u64, input_transfer_fee)
                .ok_or(QuoteStatus::TokenFeeUnsupported)?;

        let raw_output = amount_out_accum.min(u64::MAX as u128) as u64;
        let (net_output_amount, _output_fee_amount) =
            apply_post_fee_amount(raw_output, output_transfer_fee)
                .ok_or(QuoteStatus::TokenFeeUnsupported)?;
        if net_output_amount == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }

        let sqrt_price_after = sqrt_price_x64_from_x96(&sqrt_price_x96);
        Ok(ClmmSwapSimulation {
            executed_input_amount,
            net_output_amount,
            sqrt_price_after,
            protocol_fee_amount: protocol_fee_total.min(u64::MAX as u128) as u64,
            fund_fee_amount: fund_fee_total.min(u64::MAX as u128) as u64,
        })
    }

    fn quote_amount_out(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Result<ClmmSwapSimulation, QuoteStatus> {
        self.simulate_clmm_exact_in(state, req)
    }

    fn quote_amount_out_exact_out(
        &self,
        state: &RaydiumV3State,
        req: &QuoteRequest,
    ) -> Result<ClmmSwapSimulation, QuoteStatus> {
        if req.amount == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }
        let mut trial_req = QuoteRequest {
            pool_id: req.pool_id,
            input_mint: req.input_mint,
            output_mint: req.output_mint,
            amount: u64::MAX,
            slot: req.slot,
            block_time: req.block_time,
            direction: SwapDirection::ExactIn,
        };

        let max_input = self.clamp_request_amount(state, &trial_req);
        if max_input == 0 {
            return Err(QuoteStatus::InsufficientLiquidity);
        }

        trial_req.amount = 0;

        let target = req.amount;
        let mut low = 1u64;
        let mut high = max_input;
        let mut best: Option<ClmmSwapSimulation> = None;
        let mut last_error = QuoteStatus::InsufficientLiquidity;

        while low <= high {
            let mid = low + (high - low) / 2;
            trial_req.amount = mid;
            match self.quote_amount_out(state, &trial_req) {
                Ok(sim) if sim.net_output_amount >= target => {
                    best = Some(sim);
                    if mid == 0 {
                        break;
                    }
                    high = mid.saturating_sub(1);
                }
                Ok(_) => {
                    low = mid.saturating_add(1);
                }
                Err(status) => {
                    last_error = status;
                    if mid == 0 {
                        break;
                    }
                    high = mid.saturating_sub(1);
                }
            }
        }

        let mut simulation = best.ok_or(last_error)?;
        simulation.net_output_amount = target;
        Ok(simulation)
    }

    fn build_snapshot_side(
        &self,
        state: &RaydiumV3State,
        pool_id: Pubkey,
        slot: u64,
        base_to_quote: bool,
        probe_amount: u64,
    ) -> Option<PoolQuoteSide> {
        if probe_amount == 0 {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::InsufficientLiquidity,
                "probe_amount_zero",
                Some(probe_amount),
            );
            return None;
        }

        let fee_bps = state.trade_fee_bps.unwrap_or(DEFAULT_FEE_BPS);
        let template_fee_bps = fee_bps.min(u64::from(u16::MAX)) as u16;
        let (input_mint, output_mint) = if base_to_quote {
            (state.base_mint, state.quote_mint)
        } else {
            (state.quote_mint, state.base_mint)
        };

        if state.is_disabled() {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::TradingDisabled,
                "pool_disabled",
                Some(probe_amount),
            );
            return Some(self.unavailable_snapshot_side(
                input_mint,
                output_mint,
                slot,
                fee_bps,
                template_fee_bps,
                QuoteStatus::TradingDisabled,
            ));
        }
        if state.is_stale(Some(slot), &self.config) {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::StaleState,
                "state_stale",
                Some(probe_amount),
            );
            return Some(self.unavailable_snapshot_side(
                input_mint,
                output_mint,
                slot,
                fee_bps,
                template_fee_bps,
                QuoteStatus::StaleState,
            ));
        }

        let req = QuoteRequest {
            pool_id,
            input_mint,
            output_mint,
            amount: probe_amount,
            slot,
            block_time: None,
            direction: SwapDirection::ExactIn,
        };

        let simulation = match self.quote_amount_out(state, &req) {
            Ok(sim) => sim,
            Err(status) => {
                self.record_snapshot_issue(
                    state,
                    slot,
                    base_to_quote,
                    status,
                    "simulation_error",
                    Some(probe_amount),
                );
                return Some(self.unavailable_snapshot_side(
                    input_mint,
                    output_mint,
                    slot,
                    fee_bps,
                    template_fee_bps,
                    status,
                ));
            }
        };
        if simulation.executed_input_amount == 0 {
            self.record_snapshot_issue(
                state,
                slot,
                base_to_quote,
                QuoteStatus::InsufficientLiquidity,
                "zero_executed_input",
                Some(probe_amount),
            );
            return None;
        }
        let execution_price =
            simulation.net_output_amount as f64 / simulation.executed_input_amount as f64;

        let reference_price = match state.mid_price() {
            Some(mid) if mid > 0.0 => {
                if base_to_quote {
                    Some(mid)
                } else {
                    Some(1.0 / mid)
                }
            }
            _ => None,
        };

        let slippage = reference_price
            .map(|reference| compute_slippage_bps(reference, execution_price))
            .unwrap_or_default();

        self.record_snapshot_issue(
            state,
            slot,
            base_to_quote,
            QuoteStatus::Ok,
            "ok",
            Some(probe_amount),
        );

        Some(PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional: simulation.executed_input_amount,
            expected_output: simulation.net_output_amount,
            fee_bps,
            template_fee_bps,
            effective_price: execution_price,
            estimated_slippage_bps: slippage,
            sqrt_price_limit: Some(simulation.sqrt_price_after),
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            amount_specified_is_input: true,
            context: {
                let mut context = TemplateContext {
                    compute_unit_limit: Some(RAYDIUM_DEFAULT_CU_LIMIT),
                    compute_unit_price: Some(RAYDIUM_DEFAULT_CU_PRICE),
                    ..Default::default()
                };
                self.decorate_quote_context(&mut context, state, input_mint, output_mint);
                context.expected_fee_amount = Some(
                    simulation
                        .protocol_fee_amount
                        .saturating_add(simulation.fund_fee_amount),
                );
                context
            },
            status: QuoteStatus::Ok,
            sqrt_price_before_x64: None,
            sqrt_price_after_x64: None,
            tick_arrays_used: Vec::new(),
            cache_slot: Some(slot),
        })
    }

    fn unavailable_snapshot_side(
        &self,
        input_mint: Pubkey,
        output_mint: Pubkey,
        slot: u64,
        fee_bps: u64,
        template_fee_bps: u16,
        status: QuoteStatus,
    ) -> PoolQuoteSide {
        PoolQuoteSide {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            notional: 0,
            expected_output: 0,
            fee_bps,
            template_fee_bps,
            effective_price: 0.0,
            estimated_slippage_bps: 0.0,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            amount_specified_is_input: true,
            context: TemplateContext::default(),
            status,
            sqrt_price_before_x64: None,
            sqrt_price_after_x64: None,
            tick_arrays_used: Vec::new(),
            cache_slot: Some(slot),
        }
    }
}

impl ProtocolAdapter for RaydiumV3Adapter {
    fn name(&self) -> &'static str {
        "raydium_v3"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::raydium_v3",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        // CoreCast metadata is often stale; rely solely on on-chain hydration.
        self.upsert_state(update, RaydiumV3PoolExtras::default());
    }

    fn enrich_snapshot(&self, snapshot: &mut PoolSnapshot) {
        snapshot.hydration.require(HydrationComponent::Meta);
        snapshot.hydration.require(HydrationComponent::TickArrays);
        let Some(pool_pubkey) = parse_pubkey(&snapshot.pool_id) else {
            return;
        };
        let Some(state) = self.get_state(&pool_pubkey) else {
            return;
        };
        if Self::meta_ready(&state) {
            snapshot
                .hydration
                .mark_ready(HydrationComponent::Meta, state.last_update_slot);
        }
        if self.tick_arrays_ready(&state) {
            snapshot
                .hydration
                .mark_ready(HydrationComponent::TickArrays, state.last_update_slot);
        } else {
            self.request_tick_arrays(&state);
        }
    }

    fn warm_components(&self, snapshot: &PoolSnapshot, components: &[HydrationComponent]) {
        let Some(pool_pubkey) = parse_pubkey(&snapshot.pool_id) else {
            return;
        };
        if components.contains(&HydrationComponent::Meta) {
            if let Some(hydrator) = self.hydrator.as_ref() {
                hydrator.hydrate(pool_pubkey);
            }
        }
        if components.contains(&HydrationComponent::TickArrays) {
            if let Some(state) = self.get_state(&pool_pubkey) {
                self.request_tick_arrays(&state);
            }
        }
    }

    fn quote_exact_in(&self, mut req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if state.is_disabled() {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::TradingDisabled;
            return Some(response);
        }
        if state.is_stale(Some(req.slot), &self.config) {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::StaleState;
            return Some(response);
        }
        req.amount = self.clamp_request_amount(&state, &req);
        if req.amount == 0 {
            return None;
        }
        let simulation = match self.quote_amount_out(&state, &req) {
            Ok(sim) => sim,
            Err(status) => {
                let mut response = build_quote_response(req.clone(), 0);
                response.status = status;
                return Some(response);
            }
        };
        let mut response = build_quote_response(req, simulation.net_output_amount);
        response.amount_in = simulation.executed_input_amount;
        response.sqrt_price_limit = Some(simulation.sqrt_price_after);
        response.min_output_amount = Some(simulation.net_output_amount);
        response.context.compute_unit_limit = Some(RAYDIUM_DEFAULT_CU_LIMIT);
        response.context.compute_unit_price = Some(RAYDIUM_DEFAULT_CU_PRICE);
        response.context.expected_fee_amount = Some(
            simulation
                .protocol_fee_amount
                .saturating_add(simulation.fund_fee_amount),
        );
        self.decorate_quote_context(
            &mut response.context,
            &state,
            response.request.input_mint,
            response.request.output_mint,
        );
        Some(response)
    }

    fn quote_exact_out(&self, mut req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactOut {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        if state.is_disabled() {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::TradingDisabled;
            return Some(response);
        }
        if state.is_stale(Some(req.slot), &self.config) {
            let mut response = build_quote_response(req, 0);
            response.status = QuoteStatus::StaleState;
            return Some(response);
        }

        req.amount = self.clamp_request_output_amount(&state, &req);
        if req.amount == 0 {
            return None;
        }

        let simulation = match self.quote_amount_out_exact_out(&state, &req) {
            Ok(sim) => sim,
            Err(status) => {
                let mut response = build_quote_response(req.clone(), 0);
                response.status = status;
                return Some(response);
            }
        };
        let requested_output = req.amount;
        let mut response = build_quote_response(req, requested_output);
        response.amount_in = simulation.executed_input_amount;
        response.amount_out = requested_output;
        response.sqrt_price_limit = Some(simulation.sqrt_price_after);
        response.min_output_amount = Some(requested_output);
        response.context.compute_unit_limit = Some(RAYDIUM_DEFAULT_CU_LIMIT);
        response.context.compute_unit_price = Some(RAYDIUM_DEFAULT_CU_PRICE);
        response.context.expected_fee_amount = Some(
            simulation
                .protocol_fee_amount
                .saturating_add(simulation.fund_fee_amount),
        );
        self.decorate_quote_context(
            &mut response.context,
            &state,
            response.request.input_mint,
            response.request.output_mint,
        );
        Some(response)
    }

    fn bidirectional_quotes(&self, snapshot: &PoolSnapshot) -> Option<PoolBidirectionalQuotes> {
        let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
        let state = match self.get_state(&pool_pubkey) {
            Some(state) => state,
            None => {
                self.log_state_cache_miss(pool_pubkey, snapshot.slot);
                return None;
            }
        };

        let base_probe =
            match snapshot_probe_amount(state.vault_base, self.config.max_swap_utilization_bps) {
                Some(value) => value,
                None => {
                    self.record_snapshot_issue(
                        &state,
                        snapshot.slot,
                        true,
                        QuoteStatus::InsufficientLiquidity,
                        "base_reserve_empty",
                        None,
                    );
                    return None;
                }
            };
        let quote_probe =
            match snapshot_probe_amount(state.vault_quote, self.config.max_swap_utilization_bps) {
                Some(value) => value,
                None => {
                    self.record_snapshot_issue(
                        &state,
                        snapshot.slot,
                        false,
                        QuoteStatus::InsufficientLiquidity,
                        "quote_reserve_empty",
                        None,
                    );
                    return None;
                }
            };

        let base_to_quote =
            self.build_snapshot_side(&state, pool_pubkey, snapshot.slot, true, base_probe)?;
        let quote_to_base =
            self.build_snapshot_side(&state, pool_pubkey, snapshot.slot, false, quote_probe)?;

        Some(PoolBidirectionalQuotes {
            base_to_quote,
            quote_to_base,
        })
    }
}

const DEFAULT_FEE_BPS: u64 = 25; // assumed Raydium taker fee (0.25%)
const RAYDIUM_DEFAULT_CU_LIMIT: u32 = 350_000;
const RAYDIUM_DEFAULT_CU_PRICE: u64 = 5_000;
const SNAPSHOT_PROBE_DIVISOR: u128 = 5_000;
const SNAPSHOT_MIN_PROBE_INPUT: u128 = 1_000;
const SNAPSHOT_MAX_PROBE_INPUT: u128 = u64::MAX as u128;
const BPS_DENOMINATOR_U128: u128 = 10_000;
const RAYDIUM_FEE_RATE_DENOMINATOR: u128 = 1_000_000;
const RAYDIUM_STATUS_DISABLE_SWAP_BIT: u8 = 1 << 4;

#[derive(Debug, Clone)]
pub struct RaydiumV3State {
    pub pool_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_token_program: Pubkey,
    pub quote_token_program: Pubkey,
    pub amm_config: Option<Pubkey>,
    pub observation_state: Option<Pubkey>,
    pub tick_spacing: Option<i32>,
    pub sqrt_price_x64: Option<u128>,
    pub current_tick: Option<i32>,
    pub liquidity: Option<u128>,
    pub vault_base: u128,
    pub vault_quote: u128,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
    pub pool_bump: Option<u8>,
    pub observation_bump: Option<u8>,
    pub vault_bumps: Option<[u8; 2]>,
    pub trade_fee_bps: Option<u64>,
    pub protocol_fee_bps: Option<u16>,
    pub fund_fee_bps: Option<u16>,
    pub tick_array_bitmap_extension: Option<Pubkey>,
    pub pool_status: Option<u8>,
    pub base_transfer_fee: Option<TransferFeeConfigSummary>,
    pub quote_transfer_fee: Option<TransferFeeConfigSummary>,
    tick_array_cache: TickArrayCache,
}

impl RaydiumV3State {
    pub fn mid_price(&self) -> Option<f64> {
        if self.vault_base == 0 {
            return None;
        }
        Some(self.vault_quote as f64 / self.vault_base as f64)
    }

    pub fn is_disabled(&self) -> bool {
        self.pool_status
            .map(|status| status & RAYDIUM_STATUS_DISABLE_SWAP_BIT != 0)
            .unwrap_or(false)
    }

    pub fn is_stale(&self, current_slot: Option<u64>, config: &RaydiumV3AdapterConfig) -> bool {
        let slot_fresh = current_slot
            .map(|slot| slot.saturating_sub(self.last_update_slot) <= config.stale_slot_threshold)
            .unwrap_or(true);
        let instant_fresh =
            self.last_update_at.elapsed().as_millis() as u64 <= config.stale_after_ms;
        !(slot_fresh && instant_fresh)
    }
}

fn build_raydium_alt_accounts(
    pool_id: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_token_program: Pubkey,
    quote_token_program: Pubkey,
    amm_config: Pubkey,
    observation_state: Pubkey,
    program_id: &Pubkey,
    owner_ata0: Pubkey,
    owner_ata1: Pubkey,
) -> Vec<Pubkey> {
    let (mint0, mint1, _program0, _program1) = order_mints_with_programs(
        base_mint,
        quote_mint,
        base_token_program,
        quote_token_program,
    );
    let vault0 = derive_pool_vault_address(program_id, &pool_id, &mint0);
    let vault1 = derive_pool_vault_address(program_id, &pool_id, &mint1);

    let mut accounts = Vec::with_capacity(12);
    accounts.push(pool_id);
    accounts.push(base_mint);
    accounts.push(quote_mint);
    accounts.push(amm_config);
    accounts.push(observation_state);
    accounts.push(vault0);
    accounts.push(vault1);
    accounts.push(owner_ata0);
    accounts.push(owner_ata1);
    accounts.push(MEMO_PROGRAM_ID);
    accounts.push(SPL_TOKEN_PROGRAM_ID);
    accounts.push(SPL_TOKEN_2022_PROGRAM_ID);
    accounts
}

fn order_mints_with_programs(
    base: Pubkey,
    quote: Pubkey,
    base_program: Pubkey,
    quote_program: Pubkey,
) -> (Pubkey, Pubkey, Pubkey, Pubkey) {
    if base.to_bytes() <= quote.to_bytes() {
        (base, quote, base_program, quote_program)
    } else {
        (quote, base, quote_program, base_program)
    }
}

fn derive_pool_vault_address(program_id: &Pubkey, pool_state: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[POOL_VAULT_SEED, pool_state.as_ref(), mint.as_ref()],
        program_id,
    )
    .0
}

fn derive_tick_array_bitmap_extension(program_id: &Pubkey, pool_state: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[POOL_TICK_ARRAY_BITMAP_SEED, pool_state.as_ref()],
        program_id,
    )
    .0
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

fn snapshot_probe_amount(reserve: u128, utilization_bps: u64) -> Option<u64> {
    if reserve == 0 {
        return None;
    }

    let capped = clamp_reserve_to_utilization(reserve, utilization_bps);
    if capped == 0 {
        return None;
    }

    let mut probe = capped / SNAPSHOT_PROBE_DIVISOR;
    if probe == 0 {
        probe = SNAPSHOT_MIN_PROBE_INPUT.min(capped).max(1u128);
    }

    probe = probe.min(capped);
    probe = probe.clamp(1u128, SNAPSHOT_MAX_PROBE_INPUT);
    Some(probe as u64)
}

fn clamp_reserve_to_utilization(reserve: u128, utilization_bps: u64) -> u128 {
    if utilization_bps >= BPS_DENOMINATOR_U128 as u64 {
        return reserve;
    }
    reserve
        .saturating_mul(utilization_bps as u128)
        .checked_div(BPS_DENOMINATOR_U128)
        .unwrap_or(0)
}

fn sqrt_price_x64_from_x96(value: &U256) -> u128 {
    (value >> 32).low_u128()
}

fn compute_slippage_bps(reference: f64, execution: f64) -> f64 {
    if !reference.is_finite() || reference == 0.0 || !execution.is_finite() {
        return 0.0;
    }
    (((reference - execution) / reference) * 10_000.0).abs()
}

fn tick_array_span(tick_spacing: i32) -> Option<i32> {
    tick_spacing
        .checked_mul(TICK_ARRAY_TICK_COUNT as i32)
        .map(|value| value.abs())
}

fn tick_in_span(start: i32, span: i32, tick: i32) -> bool {
    let start_i64 = start as i64;
    let end_i64 = start_i64 + span as i64;
    let tick_i64 = tick as i64;
    tick_i64 >= start_i64 && tick_i64 < end_i64
}

fn clamp_to_i32(value: i64) -> i32 {
    value.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32
}

fn push_unique_pubkey(list: &mut Vec<Pubkey>, value: Pubkey) {
    if !list.iter().any(|item| *item == value) {
        list.push(value);
    }
}

fn mint_is_less(a: &Pubkey, b: &Pubkey) -> bool {
    a.to_bytes().cmp(&b.to_bytes()) == Ordering::Less
}

fn classify_swap_direction(
    state: &RaydiumV3State,
    input_mint: Pubkey,
    output_mint: Pubkey,
) -> Option<bool> {
    let (token0, token1) = if mint_is_less(&state.base_mint, &state.quote_mint) {
        (state.base_mint, state.quote_mint)
    } else {
        (state.quote_mint, state.base_mint)
    };

    if input_mint == token0 && output_mint == token1 {
        Some(true)
    } else if input_mint == token1 && output_mint == token0 {
        Some(false)
    } else {
        None
    }
}

fn apply_liquidity_delta(liquidity: u128, liquidity_net: i128, zero_for_one: bool) -> Option<u128> {
    let mut current = i128::try_from(liquidity).ok()?;
    current = if zero_for_one {
        current.checked_sub(liquidity_net)?
    } else {
        current.checked_add(liquidity_net)?
    };
    if current < 0 {
        None
    } else {
        Some(current as u128)
    }
}

fn fee_bps_from_hundredth_bip(rate: u32) -> u64 {
    ((rate as u64).saturating_add(50)) / 100
}

fn fee_bps_from_hundredth_bip_u16(rate: u32) -> Option<u16> {
    u16::try_from(fee_bps_from_hundredth_bip(rate)).ok()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    amm_config: Pubkey,
    observation_state: Pubkey,
    tick_spacing: i32,
    sqrt_price_x64: u128,
    liquidity: u128,
    current_tick: i32,
    pool_bump: u8,
    trade_fee_rate: Option<u32>,
    protocol_fee_rate: Option<u32>,
    fund_fee_rate: Option<u32>,
    tick_array_bitmap_extension: Pubkey,
    pool_status: u8,
}

#[derive(Clone, Debug)]
struct HydratedAmmConfig {
    trade_fee_rate: u32,
    protocol_fee_rate: u32,
    fund_fee_rate: u32,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct HydratedTickArray {
    address: Pubkey,
    pool_id: Pubkey,
    start_tick_index: i32,
    ticks: Vec<HydratedTick>,
    initialized_tick_count: u8,
    recent_epoch: u64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct HydratedTick {
    tick: i32,
    liquidity_net: i128,
    liquidity_gross: u128,
    fee_growth_outside_0_x64: u128,
    fee_growth_outside_1_x64: u128,
    reward_growths_outside_x64: [u128; 3],
}

struct TickArrayCursor<'a> {
    hydrator: &'a RaydiumPoolHydrator,
    pool: Pubkey,
    tick_spacing: i32,
    order: Vec<i32>,
    cache: HashMap<i32, HydratedTickArray>,
}

impl<'a> TickArrayCursor<'a> {
    fn new(hydrator: &'a RaydiumPoolHydrator, pool: Pubkey, tick_spacing: i32) -> Option<Self> {
        let mut cursor = Self {
            hydrator,
            pool,
            tick_spacing,
            order: Vec::new(),
            cache: HashMap::new(),
        };
        cursor.reload_inventory();
        if cursor.order.is_empty() {
            None
        } else {
            Some(cursor)
        }
    }

    fn reload_inventory(&mut self) {
        let arrays = self.hydrator.inventory_for_pool(self.pool);
        self.order.clear();
        self.cache.clear();
        for array in arrays {
            self.order.push(array.start_tick_index);
            self.cache.insert(array.start_tick_index, array);
        }
        self.order.sort();
    }

    fn get_array(&self, start: i32) -> Option<HydratedTickArray> {
        self.cache.get(&start).cloned()
    }

    fn find_order_index(&mut self, tick: i32, span: i32) -> Option<usize> {
        if let Some(idx) = self
            .order
            .iter()
            .position(|start| tick_in_span(*start, span, tick))
        {
            return Some(idx);
        }
        self.reload_inventory();
        self.order
            .iter()
            .position(|start| tick_in_span(*start, span, tick))
    }

    fn next_initialized_tick(
        &mut self,
        current_tick: i32,
        zero_for_one: bool,
    ) -> Option<InitializedTick> {
        let span = tick_array_span(self.tick_spacing)?;
        let mut idx = self.find_order_index(current_tick, span)?;
        let spacing = i64::from(self.tick_spacing.abs().max(1));
        let span_i64 = i64::from(span);
        let mut search_tick = i64::from(current_tick);

        const MAX_ARRAYS: usize = 16;
        for _ in 0..MAX_ARRAYS {
            let start = *self.order.get(idx)?;
            let array = self.get_array(start)?;
            if zero_for_one {
                if let Some(tick) = find_prev_initialized(&array, clamp_to_i32(search_tick)) {
                    return Some(tick);
                }
                if idx == 0 {
                    break;
                }
                idx -= 1;
                let prev_start = i64::from(*self.order.get(idx)?);
                search_tick = prev_start + span_i64 - spacing;
            } else {
                if let Some(tick) = find_next_initialized(&array, clamp_to_i32(search_tick)) {
                    return Some(tick);
                }
                idx += 1;
                if idx >= self.order.len() {
                    break;
                }
                let next_start = i64::from(*self.order.get(idx)?);
                search_tick = next_start;
            }
        }

        None
    }
}

#[derive(Clone, Copy, Debug)]
struct InitializedTick {
    index: i32,
    liquidity_net: i128,
}

fn find_prev_initialized(array: &HydratedTickArray, current_tick: i32) -> Option<InitializedTick> {
    for tick in array.ticks.iter().rev() {
        if tick.tick <= current_tick && tick.liquidity_gross > 0 {
            return Some(InitializedTick {
                index: tick.tick,
                liquidity_net: tick.liquidity_net,
            });
        }
    }
    None
}

fn find_next_initialized(array: &HydratedTickArray, current_tick: i32) -> Option<InitializedTick> {
    for tick in &array.ticks {
        if tick.tick > current_tick && tick.liquidity_gross > 0 {
            return Some(InitializedTick {
                index: tick.tick,
                liquidity_net: tick.liquidity_net,
            });
        }
    }
    None
}

struct RaydiumPoolHydrator {
    program_id: Pubkey,
    clients: Arc<Vec<Arc<RpcClient>>>,
    pool_cache: AsyncHydrator<Pubkey, HydratedPoolMeta>,
    tick_array_cache: DashMap<(Pubkey, i32), HydratedTickArray>,
    hydrated_tick_sets: DashSet<Pubkey>,
    mint_metadata_cache: DashMap<Pubkey, MintAccountMeta>,
    missing_mints: DashMap<Pubkey, Instant>,
    retry_backoff: Duration,
}

impl RaydiumPoolHydrator {
    fn new(config: &RaydiumV3AdapterConfig, program_id: Pubkey) -> Option<Self> {
        if config.rpc_endpoints.is_empty() {
            return None;
        }

        let timeout = Duration::from_millis(config.rpc_timeout_ms.max(100));
        let client_list: Vec<Arc<RpcClient>> = config
            .rpc_endpoints
            .iter()
            .map(|url| Arc::new(RpcClient::new_with_timeout(url.clone(), timeout)))
            .collect();

        if client_list.is_empty() {
            return None;
        }

        let clients = Arc::new(client_list);
        let fetch_amm_cache = Arc::new(DashMap::new());
        let fetch_missing = Arc::new(DashMap::new());
        let retry_backoff = Duration::from_millis(config.hydration_retry_ms.max(1));
        let fetch_clients = clients.clone();
        let pool_cache = AsyncHydrator::new("raydium_pool", fetch_clients.len(), move |pool| {
            fetch_raydium_pool_meta(
                fetch_clients.as_ref(),
                fetch_amm_cache.as_ref(),
                fetch_missing.as_ref(),
                retry_backoff,
                pool,
            )
        });

        Some(Self {
            program_id,
            clients,
            pool_cache,
            tick_array_cache: DashMap::new(),
            hydrated_tick_sets: DashSet::new(),
            mint_metadata_cache: DashMap::new(),
            missing_mints: DashMap::new(),
            retry_backoff,
        })
    }

    fn client_count(&self) -> usize {
        self.clients.len()
    }

    fn hydrate(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        self.pool_cache.get_or_schedule(pool)
    }

    fn lookup_mint_program(&self, mint: Pubkey) -> Option<Pubkey> {
        self.mint_metadata(mint).map(|meta| meta.program)
    }

    fn mint_transfer_fee_config(&self, mint: Pubkey) -> Option<TransferFeeConfigSummary> {
        self.mint_metadata(mint)
            .and_then(|meta| meta.transfer_fee_config)
    }

    fn mint_metadata(&self, mint: Pubkey) -> Option<MintAccountMeta> {
        if let Some(entry) = self.mint_metadata_cache.get(&mint) {
            return Some(*entry.value());
        }
        if self.should_skip_missing_mint(&mint) {
            return None;
        }

        for client in self.clients.as_ref() {
            match client.get_account(&mint) {
                Ok(account) => {
                    let program = account.owner;
                    let transfer_fee_config = if program == SPL_TOKEN_2022_PROGRAM_ID {
                        parse_transfer_fee_config(&account.data)
                    } else {
                        None
                    };
                    let meta = MintAccountMeta {
                        program,
                        transfer_fee_config,
                    };
                    self.mint_metadata_cache.insert(mint, meta);
                    self.missing_mints.remove(&mint);
                    return Some(meta);
                }
                Err(error) => {
                    if is_account_not_found(&error) {
                        if self.mark_missing_mint(mint) {
                            warn!(
                                target = "adapter::raydium_v3",
                                mint = %mint,
                                "raydium mint missing on-chain"
                            );
                        }
                        return None;
                    } else {
                        warn!(
                            target = "adapter::raydium_v3",
                            mint = %mint,
                            %error,
                            "failed to fetch raydium mint account"
                        );
                    }
                }
            }
        }

        None
    }

    fn inventory_for_pool(&self, pool: Pubkey) -> Vec<HydratedTickArray> {
        let mut arrays = self.collect_cached_arrays(pool);
        let needs_refresh = arrays.len() < 3 || !self.hydrated_tick_sets.contains(&pool);
        if needs_refresh {
            if let Some(fetched) = self.fetch_tick_arrays_for_pool(pool) {
                for array in &fetched {
                    self.tick_array_cache
                        .insert((pool, array.start_tick_index), array.clone());
                }
                self.hydrated_tick_sets.insert(pool);
                arrays = self.collect_cached_arrays(pool);
            }
        }
        arrays
    }

    fn collect_cached_arrays(&self, pool: Pubkey) -> Vec<HydratedTickArray> {
        let mut arrays = Vec::new();
        for entry in self.tick_array_cache.iter() {
            if entry.key().0 == pool {
                arrays.push(entry.value().clone());
            }
        }
        arrays.sort_by_key(|array| array.start_tick_index);
        arrays
    }

    fn fetch_tick_arrays_for_pool(&self, pool: Pubkey) -> Option<Vec<HydratedTickArray>> {
        let pool_bytes = pool.to_bytes().to_vec();
        for client in self.clients.as_ref() {
            let filters = Some(vec![
                RpcFilterType::DataSize(MIN_RAYDIUM_TICK_ARRAY_ACCOUNT_LEN as u64),
                RpcFilterType::Memcmp(Memcmp::new(
                    ANCHOR_DISCRIMINATOR_LEN,
                    MemcmpEncodedBytes::Bytes(pool_bytes.clone()),
                )),
            ]);
            let mut account_config = RpcAccountInfoConfig::default();
            account_config.encoding = Some(UiAccountEncoding::Base64);
            let config = RpcProgramAccountsConfig {
                filters,
                account_config,
                with_context: None,
                sort_results: None,
            };

            match client.get_program_ui_accounts_with_config(&self.program_id, config) {
                Ok(accounts) => {
                    let mut hydrated = Vec::new();
                    for (address, account) in accounts {
                        let Some(data) = decode_ui_account_bytes(&account) else {
                            continue;
                        };
                        if let Some(array) = parse_tick_array_account(address, &data) {
                            if array.pool_id == pool {
                                hydrated.push(array);
                            }
                        }
                    }
                    if !hydrated.is_empty() {
                        return Some(hydrated);
                    }
                }
                Err(error) => {
                    warn!(
                        target = "adapter::raydium_v3",
                        pool = %pool,
                        %error,
                        "failed to fetch raydium tick arrays via gPA"
                    );
                }
            }
        }

        None
    }

    fn preferred_tick_arrays_for_swap(
        &self,
        pool: Pubkey,
        tick_spacing: i32,
        current_tick: i32,
        zero_for_one: bool,
    ) -> Option<Vec<Pubkey>> {
        let span = tick_array_span(tick_spacing)?;
        if span == 0 {
            return None;
        }

        let arrays = self.inventory_for_pool(pool);
        if arrays.is_empty() {
            return None;
        }

        let anchor_idx = arrays
            .iter()
            .position(|array| tick_in_span(array.start_tick_index, span, current_tick))
            .or_else(|| {
                if zero_for_one {
                    arrays.len().checked_sub(1)
                } else if !arrays.is_empty() {
                    Some(0)
                } else {
                    None
                }
            })?;

        let mut selected = Vec::with_capacity(3);
        push_unique_pubkey(&mut selected, arrays[anchor_idx].address);

        if zero_for_one {
            for step in 1..=2 {
                if let Some(idx) = anchor_idx.checked_sub(step) {
                    push_unique_pubkey(&mut selected, arrays[idx].address);
                }
            }
        } else {
            for step in 1..=2 {
                let idx = anchor_idx + step;
                if let Some(array) = arrays.get(idx) {
                    push_unique_pubkey(&mut selected, array.address);
                }
            }
        }

        if selected.len() < 3 {
            for array in &arrays {
                if selected.len() == 3 {
                    break;
                }
                push_unique_pubkey(&mut selected, array.address);
            }
        }

        if selected.len() < 3 {
            warn!(
                target = "adapter::raydium_v3",
                pool = %pool,
                available = arrays.len(),
                "insufficient tick arrays hydrated for raydium swap"
            );
            return None;
        }

        Some(selected)
    }

    fn warm_tick_arrays(&self, pool: Pubkey, tick_spacing: i32, current_tick: i32) {
        let _ = self.preferred_tick_arrays_for_swap(pool, tick_spacing, current_tick, true);
        let _ = self.preferred_tick_arrays_for_swap(pool, tick_spacing, current_tick, false);
    }

    fn should_skip_missing_mint(&self, key: &Pubkey) -> bool {
        Self::should_skip_missing(&self.missing_mints, key, self.retry_backoff)
    }

    fn mark_missing_mint(&self, key: Pubkey) -> bool {
        Self::record_missing(&self.missing_mints, key)
    }

    fn should_skip_missing(
        map: &DashMap<Pubkey, Instant>,
        key: &Pubkey,
        backoff: Duration,
    ) -> bool {
        if let Some(entry) = map.get(key) {
            if entry.value().elapsed() < backoff {
                return true;
            }
            drop(entry);
            map.remove(key);
        }
        false
    }

    fn record_missing(map: &DashMap<Pubkey, Instant>, key: Pubkey) -> bool {
        map.insert(key, Instant::now()).is_none()
    }
}

const MIN_RAYDIUM_POOL_ACCOUNT_LEN: usize = ANCHOR_DISCRIMINATOR_LEN + 390;

fn parse_amm_config_account(data: &[u8]) -> Option<HydratedAmmConfig> {
    if data.len() < MIN_RAYDIUM_AMM_CONFIG_ACCOUNT_LEN {
        return None;
    }

    let discriminator = data.get(0..ANCHOR_DISCRIMINATOR_LEN)?;
    if discriminator != AMM_CONFIG_STATE_DISCRIMINATOR {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    read_u8(data, &mut cursor)?; // bump
    read_u16(data, &mut cursor)?; // index
    skip_pubkey(data, &mut cursor)?; // owner
    let protocol_fee_rate = read_u32(data, &mut cursor)?;
    let trade_fee_rate = read_u32(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 2)?; // tick_spacing
    let fund_fee_rate = read_u32(data, &mut cursor)?;

    Some(HydratedAmmConfig {
        trade_fee_rate,
        protocol_fee_rate,
        fund_fee_rate,
    })
}

fn parse_pool_account(
    data: &[u8],
    program_id: &Pubkey,
    pool_id: &Pubkey,
) -> Option<HydratedPoolMeta> {
    if data.len() < MIN_RAYDIUM_POOL_ACCOUNT_LEN {
        return None;
    }

    let discriminator = data.get(0..ANCHOR_DISCRIMINATOR_LEN)?;
    if discriminator != POOL_STATE_DISCRIMINATOR {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    let pool_bump = *data.get(cursor)?;
    cursor += 1;

    let amm_config = read_pubkey(data, &mut cursor)?;
    skip_pubkey(data, &mut cursor)?; // owner
    skip_pubkey(data, &mut cursor)?; // token_mint_0
    skip_pubkey(data, &mut cursor)?; // token_mint_1
    skip_pubkey(data, &mut cursor)?; // token_vault_0
    skip_pubkey(data, &mut cursor)?; // token_vault_1
    let observation_state = read_pubkey(data, &mut cursor)?;

    skip_bytes(data, &mut cursor, 2)?; // mint_decimals_0 + mint_decimals_1
    skip_bytes(data, &mut cursor, 8)?; // reserved flags (token program ids, decimals extras)

    let tick_spacing = read_u16(data, &mut cursor)? as i32;
    let liquidity = read_u128(data, &mut cursor)?;
    let sqrt_price_x64 = read_u128(data, &mut cursor)?;
    let current_tick = read_i32(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 2)?; // padding3
    skip_bytes(data, &mut cursor, 2)?; // padding4
    skip_bytes(data, &mut cursor, 16)?; // fee_growth_global_0_x64
    skip_bytes(data, &mut cursor, 16)?; // fee_growth_global_1_x64
    skip_bytes(data, &mut cursor, 8)?; // protocol_fees_token_0
    skip_bytes(data, &mut cursor, 8)?; // protocol_fees_token_1
    skip_bytes(data, &mut cursor, 16)?; // swap_in_amount_token_0
    skip_bytes(data, &mut cursor, 16)?; // swap_out_amount_token_1
    skip_bytes(data, &mut cursor, 16)?; // swap_in_amount_token_1
    skip_bytes(data, &mut cursor, 16)?; // swap_out_amount_token_0
    let pool_status = read_u8(data, &mut cursor)?;

    Some(HydratedPoolMeta {
        amm_config,
        observation_state,
        tick_spacing,
        sqrt_price_x64,
        liquidity,
        current_tick,
        pool_bump,
        trade_fee_rate: None,
        protocol_fee_rate: None,
        fund_fee_rate: None,
        tick_array_bitmap_extension: derive_tick_array_bitmap_extension(program_id, pool_id),
        pool_status,
    })
}

fn fetch_raydium_pool_meta(
    clients: &[Arc<RpcClient>],
    amm_cache: &DashMap<Pubkey, HydratedAmmConfig>,
    missing_configs: &DashMap<Pubkey, Instant>,
    retry_backoff: Duration,
    pool: Pubkey,
) -> Option<HydratedPoolMeta> {
    for client in clients {
        match client.get_account(&pool) {
            Ok(account) => {
                if let Some(mut meta) =
                    parse_pool_account(&account.data, &RAYDIUM_CLMM_PROGRAM, &pool)
                {
                    attach_config_fees_from_cache(
                        &mut meta,
                        clients,
                        amm_cache,
                        missing_configs,
                        retry_backoff,
                    );
                    return Some(meta);
                } else {
                    warn!(
                        target = "adapter::raydium_v3",
                        pool = %pool,
                        "unexpected raydium pool account layout"
                    );
                }
            }
            Err(error) => {
                warn!(
                    target = "adapter::raydium_v3",
                    pool = %pool,
                    %error,
                    "failed to fetch raydium pool account"
                );
            }
        }
    }

    None
}

fn attach_config_fees_from_cache(
    meta: &mut HydratedPoolMeta,
    clients: &[Arc<RpcClient>],
    amm_cache: &DashMap<Pubkey, HydratedAmmConfig>,
    missing_configs: &DashMap<Pubkey, Instant>,
    retry_backoff: Duration,
) {
    let trade_missing = meta.trade_fee_rate.is_none();
    let protocol_missing = meta.protocol_fee_rate.is_none();
    let fund_missing = meta.fund_fee_rate.is_none();
    if !(trade_missing || protocol_missing || fund_missing) {
        return;
    }

    if let Some(config) = hydrate_amm_config_from_clients(
        meta.amm_config,
        clients,
        amm_cache,
        missing_configs,
        retry_backoff,
    ) {
        if trade_missing {
            meta.trade_fee_rate = Some(config.trade_fee_rate);
        }
        if protocol_missing {
            meta.protocol_fee_rate = Some(config.protocol_fee_rate);
        }
        if fund_missing {
            meta.fund_fee_rate = Some(config.fund_fee_rate);
        }
    }
}

fn hydrate_amm_config_from_clients(
    amm_config: Pubkey,
    clients: &[Arc<RpcClient>],
    cache: &DashMap<Pubkey, HydratedAmmConfig>,
    missing_configs: &DashMap<Pubkey, Instant>,
    retry_backoff: Duration,
) -> Option<HydratedAmmConfig> {
    if RaydiumPoolHydrator::should_skip_missing(missing_configs, &amm_config, retry_backoff) {
        return None;
    }
    if let Some(entry) = cache.get(&amm_config) {
        return Some(entry.clone());
    }

    for client in clients {
        match client.get_account(&amm_config) {
            Ok(account) => {
                if let Some(config) = parse_amm_config_account(&account.data) {
                    cache.insert(amm_config, config.clone());
                    return Some(config);
                } else {
                    warn!(
                        target = "adapter::raydium_v3",
                        amm_config = %amm_config,
                        "unexpected raydium amm config layout"
                    );
                }
            }
            Err(error) => {
                if is_account_not_found(&error) {
                    if RaydiumPoolHydrator::record_missing(missing_configs, amm_config) {
                        warn!(
                            target = "adapter::raydium_v3",
                            amm_config = %amm_config,
                            "raydium amm config missing on-chain"
                        );
                    }
                } else {
                    warn!(
                        target = "adapter::raydium_v3",
                        amm_config = %amm_config,
                        %error,
                        "failed to fetch raydium amm config"
                    );
                }
            }
        }
    }

    None
}

fn parse_tick_array_account(address: Pubkey, data: &[u8]) -> Option<HydratedTickArray> {
    if data.len() < MIN_RAYDIUM_TICK_ARRAY_ACCOUNT_LEN {
        return None;
    }

    let discriminator = data.get(0..ANCHOR_DISCRIMINATOR_LEN)?;
    if discriminator != TICK_ARRAY_STATE_DISCRIMINATOR {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    let pool_id = read_pubkey(data, &mut cursor)?;
    let start_tick_index = read_i32(data, &mut cursor)?;

    let mut ticks = Vec::with_capacity(TICK_ARRAY_TICK_COUNT);
    for _ in 0..TICK_ARRAY_TICK_COUNT {
        let tick = read_i32(data, &mut cursor)?;
        let liquidity_net = read_i128(data, &mut cursor)?;
        let liquidity_gross = read_u128(data, &mut cursor)?;
        let fee_growth_outside_0_x64 = read_u128(data, &mut cursor)?;
        let fee_growth_outside_1_x64 = read_u128(data, &mut cursor)?;
        let mut reward_growths_outside_x64 = [0u128; 3];
        for reward in reward_growths_outside_x64.iter_mut() {
            *reward = read_u128(data, &mut cursor)?;
        }
        skip_bytes(data, &mut cursor, TICK_STATE_PADDING_BYTES)?;
        ticks.push(HydratedTick {
            tick,
            liquidity_net,
            liquidity_gross,
            fee_growth_outside_0_x64,
            fee_growth_outside_1_x64,
            reward_growths_outside_x64,
        });
    }

    let initialized_tick_count = read_u8(data, &mut cursor)?;
    let recent_epoch = read_u64(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 107)?;

    Some(HydratedTickArray {
        address,
        pool_id,
        start_tick_index,
        ticks,
        initialized_tick_count,
        recent_epoch,
    })
}

fn decode_ui_account_bytes(account: &UiAccount) -> Option<Vec<u8>> {
    match &account.data {
        UiAccountData::Binary(data, UiAccountEncoding::Base64)
        | UiAccountData::LegacyBinary(data) => BASE64_STANDARD.decode(data).ok(),
        _ => None,
    }
}

fn parse_transfer_fee_config(data: &[u8]) -> Option<TransferFeeConfigSummary> {
    let state = StateWithExtensionsOwned::<Token2022Mint>::unpack(data.to_vec()).ok()?;
    let config = state.get_extension::<SplTransferFeeConfig>().ok()?;
    let fee = config.newer_transfer_fee;
    Some(TransferFeeConfigSummary {
        epoch: fee.epoch.into(),
        maximum_fee: fee.maximum_fee.into(),
        basis_points: fee.transfer_fee_basis_points.into(),
    })
}

fn skip_pubkey(data: &[u8], cursor: &mut usize) -> Option<()> {
    *cursor = cursor.checked_add(PUBKEY_BYTES)?;
    if *cursor <= data.len() {
        Some(())
    } else {
        None
    }
}

fn read_pubkey(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    let end = cursor.checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Option<u16> {
    let end = cursor.checked_add(2)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u16::from_le_bytes(bytes))
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Option<u32> {
    let end = cursor.checked_add(4)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u32::from_le_bytes(bytes))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Option<u64> {
    let end = cursor.checked_add(8)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u64::from_le_bytes(bytes))
}

fn read_u128(data: &[u8], cursor: &mut usize) -> Option<u128> {
    let end = cursor.checked_add(16)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u128::from_le_bytes(bytes))
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    let end = cursor.checked_add(1)?;
    let value = *data.get(*cursor)?;
    *cursor = end;
    Some(value)
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Option<i32> {
    let end = cursor.checked_add(4)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(i32::from_le_bytes(bytes))
}

fn read_i128(data: &[u8], cursor: &mut usize) -> Option<i128> {
    let end = cursor.checked_add(16)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(i128::from_le_bytes(bytes))
}

fn skip_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<()> {
    *cursor = cursor.checked_add(len)?;
    if *cursor <= data.len() {
        Some(())
    } else {
        None
    }
}

fn is_account_not_found(error: &ClientError) -> bool {
    use solana_client::rpc_request::RpcError;

    match error.kind() {
        ClientErrorKind::RpcError(RpcError::RpcResponseError { message, data, .. }) => {
            message.contains("AccountNotFound") || format!("{:?}", data).contains("AccountNotFound")
        }
        ClientErrorKind::Custom(message) => message.contains("AccountNotFound"),
        _ => error.to_string().contains("AccountNotFound"),
    }
}

mod math {
    use super::U256;
    use once_cell::sync::Lazy;

    pub const MIN_TICK: i32 = -443_636;
    pub const MAX_TICK: i32 = 443_636;
    const FEE_DENOMINATOR: u128 = 10_000;

    static TICK_RATIO_FACTORS: Lazy<[U256; 20]> = Lazy::new(|| {
        [
            U256::from_str_radix("fffcb933bd6fad37aa2d162d1a594001", 16).unwrap(),
            U256::from_str_radix("fff97272373d413259a46990580e213a", 16).unwrap(),
            U256::from_str_radix("fff2e50f5f656932ef12357cf3c7fdcc", 16).unwrap(),
            U256::from_str_radix("ffe5caca7e10e4e61c3624eaa0941cd0", 16).unwrap(),
            U256::from_str_radix("ffcb9843d60f6159c9db58835c926644", 16).unwrap(),
            U256::from_str_radix("ff973b41fa98c081472e6896dfb254c0", 16).unwrap(),
            U256::from_str_radix("ff2ea16466c96a3843ec78b326b52861", 16).unwrap(),
            U256::from_str_radix("fe5dee046a99a2a811c461f1969c3053", 16).unwrap(),
            U256::from_str_radix("fcbe86c7900a88aedcffc83b479aa3a4", 16).unwrap(),
            U256::from_str_radix("f987a7253ac413176f2b074cf7815e54", 16).unwrap(),
            U256::from_str_radix("f3392b0822b70005940c7a398e4b70f3", 16).unwrap(),
            U256::from_str_radix("e7159475a2c29b7443b29c7fa6e889d9", 16).unwrap(),
            U256::from_str_radix("d097f3bdfd2022b8845ad8f792aa5825", 16).unwrap(),
            U256::from_str_radix("a9f746462d870fdf8a65dc1f90e061e5", 16).unwrap(),
            U256::from_str_radix("70d869a156d2a1b890bb3df62baf32f7", 16).unwrap(),
            U256::from_str_radix("31be135f97d08fd981231505542fcfa6", 16).unwrap(),
            U256::from_str_radix("9aa508b5b7a84e1c677de54f3e99bc9", 16).unwrap(),
            U256::from_str_radix("5d6af8dedb81196699c329225ee604", 16).unwrap(),
            U256::from_str_radix("2216e584f5fa1ea926041bedfe98", 16).unwrap(),
            U256::from_str_radix("48a170391f7dc42444e8fa2", 16).unwrap(),
        ]
    });

    #[derive(Debug, Clone)]
    pub struct SwapStepResult {
        pub sqrt_price_next: U256,
        pub amount_in: u128,
        pub amount_out: u128,
        pub fee_amount: u128,
    }

    pub fn to_x96(value_x64: u128) -> U256 {
        U256::from(value_x64) << 32
    }

    pub fn sqrt_price_at_tick_x96(tick: i32) -> Option<U256> {
        if !(MIN_TICK..=MAX_TICK).contains(&tick) {
            return None;
        }
        let mut ratio = if (tick & 1) != 0 {
            TICK_RATIO_FACTORS[0]
        } else {
            U256::one() << 128
        };
        let abs_tick = tick.abs();
        for (i, factor) in TICK_RATIO_FACTORS.iter().enumerate().skip(1) {
            if (abs_tick & (1 << i)) != 0 {
                ratio = mul_shift_128(ratio, *factor);
            }
        }

        if tick > 0 {
            ratio = U256::MAX / ratio;
        }

        let remainder_mask = (U256::one() << 32) - U256::one();
        let mut ratio_shifted = ratio >> 32;
        if ratio & remainder_mask != U256::zero() {
            ratio_shifted += U256::one();
        }
        Some(ratio_shifted)
    }

    pub fn compute_swap_step(
        sqrt_price_current: U256,
        sqrt_price_target: U256,
        liquidity: u128,
        amount_remaining: u128,
        fee_bps: u64,
        zero_for_one: bool,
    ) -> Option<SwapStepResult> {
        if liquidity == 0 {
            return None;
        }

        let fee_bps = fee_bps as u128;
        if fee_bps >= FEE_DENOMINATOR {
            return None;
        }

        let amount_less_fee = amount_remaining
            .checked_mul(FEE_DENOMINATOR - fee_bps)?
            .checked_div(FEE_DENOMINATOR)?;

        let (sqrt_price_next, amount_in, amount_out) = if zero_for_one {
            let amount_in_max =
                get_amount0_delta(liquidity, sqrt_price_target, sqrt_price_current, true)?;
            if amount_less_fee >= amount_in_max {
                let out =
                    get_amount1_delta(liquidity, sqrt_price_target, sqrt_price_current, false)?;
                (sqrt_price_target, amount_in_max, out)
            } else {
                let next_price = get_next_sqrt_price_from_input(
                    sqrt_price_current,
                    liquidity,
                    amount_less_fee,
                    true,
                )?;
                let in_amount = get_amount0_delta(liquidity, next_price, sqrt_price_current, true)?;
                let out = get_amount1_delta(liquidity, next_price, sqrt_price_current, false)?;
                (next_price, in_amount, out)
            }
        } else {
            let amount_in_max =
                get_amount1_delta(liquidity, sqrt_price_current, sqrt_price_target, true)?;
            if amount_less_fee >= amount_in_max {
                let out =
                    get_amount0_delta(liquidity, sqrt_price_current, sqrt_price_target, false)?;
                (sqrt_price_target, amount_in_max, out)
            } else {
                let next_price = get_next_sqrt_price_from_input(
                    sqrt_price_current,
                    liquidity,
                    amount_less_fee,
                    false,
                )?;
                let in_amount = get_amount1_delta(liquidity, sqrt_price_current, next_price, true)?;
                let out = get_amount0_delta(liquidity, sqrt_price_current, next_price, false)?;
                (next_price, in_amount, out)
            }
        };

        let fee_amount = mul_div_round_up(amount_in, fee_bps, FEE_DENOMINATOR - fee_bps)?;

        Some(SwapStepResult {
            sqrt_price_next,
            amount_in,
            amount_out,
            fee_amount,
        })
    }

    fn get_amount0_delta(
        liquidity: u128,
        sqrt_a: U256,
        sqrt_b: U256,
        round_up: bool,
    ) -> Option<u128> {
        let (sqrt_low, sqrt_high) = if sqrt_a > sqrt_b {
            (sqrt_b, sqrt_a)
        } else {
            (sqrt_a, sqrt_b)
        };
        if sqrt_low == U256::zero() {
            return None;
        }
        let numerator1 = U256::from(liquidity) << 96;
        let numerator2 = sqrt_high.checked_sub(sqrt_low)?;
        let numerator = numerator1.checked_mul(numerator2)?;
        let denominator = sqrt_high.checked_mul(sqrt_low)?;
        let quotient = if round_up {
            div_round_up(numerator, denominator)
        } else {
            numerator.checked_div(denominator)?
        };
        quotient.try_into().ok()
    }

    fn get_amount1_delta(
        liquidity: u128,
        sqrt_a: U256,
        sqrt_b: U256,
        round_up: bool,
    ) -> Option<u128> {
        let (sqrt_low, sqrt_high) = if sqrt_a > sqrt_b {
            (sqrt_b, sqrt_a)
        } else {
            (sqrt_a, sqrt_b)
        };
        let diff = sqrt_high.checked_sub(sqrt_low)?;
        let numerator = U256::from(liquidity).checked_mul(diff)?;
        let denominator = U256::one() << 96;
        let quotient = if round_up {
            div_round_up(numerator, denominator)
        } else {
            numerator >> 96
        };
        quotient.try_into().ok()
    }

    fn get_next_sqrt_price_from_input(
        sqrt_price_current: U256,
        liquidity: u128,
        amount_in: u128,
        zero_for_one: bool,
    ) -> Option<U256> {
        if amount_in == 0 {
            return Some(sqrt_price_current);
        }
        let liquidity_u256 = U256::from(liquidity);
        if zero_for_one {
            let numerator = sqrt_price_current
                .checked_mul(liquidity_u256)?
                .checked_mul(U256::one() << 96)?;
            let denominator = liquidity_u256
                .checked_mul(U256::one() << 96)?
                .checked_add(U256::from(amount_in).checked_mul(sqrt_price_current)?)?;
            Some(numerator.checked_div(denominator)?)
        } else {
            let increment = (U256::from(amount_in) << 96).checked_div(liquidity_u256)?;
            Some(sqrt_price_current.checked_add(increment)?)
        }
    }

    fn mul_shift_128(value: U256, multiplier: U256) -> U256 {
        value.checked_mul(multiplier).unwrap_or_default() >> 128
    }

    fn div_round_up(numerator: U256, denominator: U256) -> U256 {
        let mut quotient = numerator / denominator;
        if numerator % denominator != U256::zero() {
            quotient += U256::one();
        }
        quotient
    }

    fn mul_div_round_up(value: u128, numerator: u128, denominator: u128) -> Option<u128> {
        if denominator == 0 {
            return None;
        }
        let result = div_round_up(
            U256::from(value).checked_mul(U256::from(numerator))?,
            U256::from(denominator),
        );
        result.try_into().ok()
    }

    #[cfg(test)]
    mod tests {
        use super::{sqrt_price_at_tick_x96, U256};

        #[test]
        fn sqrt_price_at_zero_tick_matches_one() {
            let sqrt = sqrt_price_at_tick_x96(0).expect("tick 0");
            assert_eq!(sqrt, U256::one() << 96);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn parses_pool_account_layout() {
        let mut data = vec![0u8; MIN_RAYDIUM_POOL_ACCOUNT_LEN + 32];
        data[..ANCHOR_DISCRIMINATOR_LEN].copy_from_slice(&POOL_STATE_DISCRIMINATOR);
        let mut cursor = 0;
        cursor += ANCHOR_DISCRIMINATOR_LEN;
        data[cursor] = 5; // bump
        cursor += 1;

        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // amm_config
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // owner
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_mint_0
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_mint_1
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_vault_0
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_vault_1
        let observation = Pubkey::new_unique();
        cursor = write_pubkey(&mut data, cursor, &observation); // observation

        data[cursor] = 6;
        data[cursor + 1] = 9;
        cursor += 2;

        cursor += 8; // reserved flags

        let tick_spacing: u16 = 128;
        data[cursor..cursor + 2].copy_from_slice(&tick_spacing.to_le_bytes());
        cursor += 2;

        let liquidity: u128 = 1_000_000;
        data[cursor..cursor + 16].copy_from_slice(&liquidity.to_le_bytes());
        cursor += 16;

        let sqrt_price_x64: u128 = 42_000;
        data[cursor..cursor + 16].copy_from_slice(&sqrt_price_x64.to_le_bytes());
        cursor += 16;

        let current_tick: i32 = -64;
        data[cursor..cursor + 4].copy_from_slice(&current_tick.to_le_bytes());

        let pool = Pubkey::new_unique();
        let meta =
            parse_pool_account(&data, &RAYDIUM_CLMM_PROGRAM, &pool).expect("meta should parse");
        assert_eq!(meta.pool_bump, 5);
        assert_eq!(meta.tick_spacing, tick_spacing as i32);
        assert_eq!(meta.liquidity, liquidity);
        assert_eq!(meta.sqrt_price_x64, sqrt_price_x64);
        assert_eq!(meta.current_tick, current_tick);
        assert_eq!(meta.observation_state, observation);
        assert_eq!(meta.pool_status, 0);
        assert_eq!(
            meta.tick_array_bitmap_extension,
            derive_tick_array_bitmap_extension(&RAYDIUM_CLMM_PROGRAM, &pool)
        );
    }

    #[test]
    fn parses_tick_array_account_layout() {
        let pool = Pubkey::new_unique();
        let address = Pubkey::new_unique();
        let start_tick_index: i32 = -120;
        let mut data = vec![0u8; MIN_RAYDIUM_TICK_ARRAY_ACCOUNT_LEN];
        data[..ANCHOR_DISCRIMINATOR_LEN].copy_from_slice(&TICK_ARRAY_STATE_DISCRIMINATOR);
        let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
        cursor = write_pubkey(&mut data, cursor, &pool);
        data[cursor..cursor + 4].copy_from_slice(&start_tick_index.to_le_bytes());
        cursor += 4;

        for idx in 0..TICK_ARRAY_TICK_COUNT {
            let tick_value = start_tick_index + (idx as i32 * 2);
            data[cursor..cursor + 4].copy_from_slice(&tick_value.to_le_bytes());
            cursor += 4;

            let liquidity_net: i128 = idx as i128 - 30;
            data[cursor..cursor + 16].copy_from_slice(&liquidity_net.to_le_bytes());
            cursor += 16;

            let liquidity_gross: u128 = (idx as u128) * 1_000;
            data[cursor..cursor + 16].copy_from_slice(&liquidity_gross.to_le_bytes());
            cursor += 16;

            let fee0: u128 = 1_000 + idx as u128;
            data[cursor..cursor + 16].copy_from_slice(&fee0.to_le_bytes());
            cursor += 16;

            let fee1: u128 = 2_000 + idx as u128;
            data[cursor..cursor + 16].copy_from_slice(&fee1.to_le_bytes());
            cursor += 16;

            for reward in 0..3 {
                let reward_value: u128 = (reward as u128) << 32 | idx as u128;
                data[cursor..cursor + 16].copy_from_slice(&reward_value.to_le_bytes());
                cursor += 16;
            }

            cursor += TICK_STATE_PADDING_BYTES;
        }

        let initialized_count = 12u8;
        data[cursor] = initialized_count;
        cursor += 1;

        let recent_epoch: u64 = 99;
        data[cursor..cursor + 8].copy_from_slice(&recent_epoch.to_le_bytes());

        let array = parse_tick_array_account(address, &data).expect("tick array should parse");
        assert_eq!(array.address, address);
        assert_eq!(array.pool_id, pool);
        assert_eq!(array.start_tick_index, start_tick_index);
        assert_eq!(array.initialized_tick_count, initialized_count);
        assert_eq!(array.recent_epoch, recent_epoch);
        assert_eq!(array.ticks.len(), TICK_ARRAY_TICK_COUNT);
        let first = &array.ticks[0];
        assert_eq!(first.tick, start_tick_index);
        assert_eq!(first.liquidity_net, -30);
        assert_eq!(first.liquidity_gross, 0);
        assert_eq!(first.reward_growths_outside_x64[0], 0);
        let last = &array.ticks[TICK_ARRAY_TICK_COUNT - 1];
        assert_eq!(
            last.tick,
            start_tick_index + ((TICK_ARRAY_TICK_COUNT as i32 - 1) * 2)
        );
        assert_eq!(
            last.fee_growth_outside_1_x64,
            2_000 + (TICK_ARRAY_TICK_COUNT as u128 - 1)
        );
    }

    fn write_pubkey(buffer: &mut [u8], cursor: usize, key: &Pubkey) -> usize {
        let end = cursor + PUBKEY_BYTES;
        buffer[cursor..end].copy_from_slice(key.as_ref());
        end
    }

    #[test]
    fn alt_account_builder_includes_pool_derivatives() {
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let quote = Pubkey::new_unique();
        let base_program = Pubkey::new_unique();
        let quote_program = Pubkey::new_unique();
        let amm_config = Pubkey::new_unique();
        let observation = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();

        let (mint0, mint1, _program0, _program1) =
            order_mints_with_programs(base, quote, base_program, quote_program);
        let owner_ata0 = Pubkey::new_unique();
        let owner_ata1 = Pubkey::new_unique();

        let accounts = build_raydium_alt_accounts(
            pool,
            base,
            quote,
            base_program,
            quote_program,
            amm_config,
            observation,
            &program_id,
            owner_ata0,
            owner_ata1,
        );

        assert!(accounts.contains(&pool));
        assert!(accounts.contains(&base));
        assert!(accounts.contains(&quote));
        assert!(accounts.contains(&amm_config));
        assert!(accounts.contains(&observation));
        assert!(accounts.contains(&MEMO_PROGRAM_ID));
        assert!(accounts.contains(&SPL_TOKEN_PROGRAM_ID));
        assert!(accounts.contains(&SPL_TOKEN_2022_PROGRAM_ID));

        let vault0 = derive_pool_vault_address(&program_id, &pool, &mint0);
        let vault1 = derive_pool_vault_address(&program_id, &pool, &mint1);
        assert!(accounts.contains(&vault0));
        assert!(accounts.contains(&vault1));

        assert!(accounts.contains(&owner_ata0));
        assert!(accounts.contains(&owner_ata1));
    }
}
