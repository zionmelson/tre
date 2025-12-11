use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, ensure, Context, Result};
use solana_client::{rpc_client::RpcClient, rpc_config::CommitmentConfig};
use solana_sdk::{account::Account, pubkey::Pubkey};
use spl_token_2022::{
    extension::{transfer_fee::TransferFeeConfig, transfer_hook, BaseStateWithExtensions, StateWithExtensionsOwned},
    state::Mint as SplToken2022Mint,
    ID as SPL_TOKEN_2022_PROGRAM_ID,
};
use tracing::{info, warn};

use crate::config::BootstrapPoolEntry;
use crate::telemetry::{self, PoolBootstrapStatus};
use crate::protocol::mint_metadata::SPL_TOKEN_PROGRAM_ID;

use super::StaticPoolRegistry;

#[derive(Debug, Clone, Copy)]
pub struct TransferFeeConfigSummary {
    pub epoch: u64,
    pub maximum_fee: u64,
    pub basis_points: u16,
}

#[derive(Debug, Clone, Copy)]
pub struct MintAccountMeta {
    pub program: Pubkey,
    pub decimals: Option<u8>,
    pub transfer_fee_config: Option<TransferFeeConfigSummary>,
}

#[derive(Debug, Clone)]
pub struct RaydiumStaticSnapshot {
    pub pool_id: Pubkey,
    pub program_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub meta: HydratedPoolMeta,
    pub tick_arrays: Vec<HydratedTickArray>,
    pub base_mint_meta: Option<MintAccountMeta>,
    pub quote_mint_meta: Option<MintAccountMeta>,
    pub base_transfer_hook_program: Option<Pubkey>,
    pub quote_transfer_hook_program: Option<Pubkey>,
    pub recorded_at: Instant,
}

#[derive(Clone, Debug)]
pub struct HydratedPoolMeta {
    pub amm_config: Pubkey,
    pub observation_state: Pubkey,
    pub tick_spacing: i32,
    pub sqrt_price_x64: u128,
    pub liquidity: u128,
    pub current_tick: i32,
    pub pool_bump: u8,
    pub trade_fee_rate: Option<u32>,
    pub protocol_fee_rate: Option<u32>,
    pub fund_fee_rate: Option<u32>,
    pub tick_array_bitmap_extension: Pubkey,
    pub pool_status: u8,
    pub slot: u64,
}

#[derive(Clone, Debug)]
pub struct HydratedAmmConfig {
    pub trade_fee_rate: u32,
    pub protocol_fee_rate: u32,
    pub fund_fee_rate: u32,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct HydratedTickArray {
    pub address: Pubkey,
    pub pool_id: Pubkey,
    pub start_tick_index: i32,
    pub ticks: Vec<HydratedTick>,
    pub initialized_tick_count: u8,
    pub recent_epoch: u64,
    pub hydrated_slot: u64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct HydratedTickArraySelection {
    pub accounts: Vec<Pubkey>,
    pub slot: u64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct HydratedTick {
    pub tick: i32,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
    pub fee_growth_outside_0_x64: u128,
    pub fee_growth_outside_1_x64: u128,
    pub reward_growths_outside_x64: [u128; 3],
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub struct InitializedTick {
    pub index: i32,
    pub liquidity_net: i128,
}

const RAYDIUM_V3_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const POOL_STATE_DISCRIMINATOR: [u8; 8] = [247, 237, 227, 245, 215, 195, 222, 70];
const AMM_CONFIG_STATE_DISCRIMINATOR: [u8; 8] = [218, 244, 33, 104, 203, 203, 43, 111];
const TICK_ARRAY_STATE_DISCRIMINATOR: [u8; 8] = [192, 155, 85, 205, 49, 249, 129, 42];
const ANCHOR_DISCRIMINATOR_LEN: usize = 8;
const TICK_ARRAY_TICK_COUNT: usize = 60;
const BOOTSTRAP_RPC_TIMEOUT_MS: u64 = 15_000;
const POOL_TICK_ARRAY_BITMAP_SEED: &[u8] = b"pool_tick_array_bitmap_extension";

#[derive(Debug, Clone)]
pub struct RaydiumPoolTarget {
    pub pool: Pubkey,
    pub tick_arrays: Vec<Pubkey>,
}

pub fn resolve_pool_targets(entries: &[BootstrapPoolEntry]) -> Result<Vec<RaydiumPoolTarget>> {
    entries
        .iter()
        .map(|entry| match entry {
            BootstrapPoolEntry::Pool(value) => {
                let pool = parse_pubkey("bootstrap.pool_allowlist", value)?;
                Ok(RaydiumPoolTarget {
                    pool,
                    tick_arrays: Vec::new(),
                })
            }
            BootstrapPoolEntry::Detailed { pool, tick_arrays } => {
                let pool_key = parse_pubkey("bootstrap.pool_allowlist.pool", pool)?;
                let arrays = tick_arrays
                    .iter()
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| {
                        parse_pubkey("bootstrap.pool_allowlist.tick_arrays", value).with_context(
                            || format!("invalid tick array {} configured for pool {}", value, pool),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(RaydiumPoolTarget {
                    pool: pool_key,
                    tick_arrays: arrays,
                })
            }
        })
        .collect()
}

pub struct RaydiumBootstrapFetcher {
    clients: Vec<RpcEndpoint>,
    commitment: CommitmentConfig,
    program_id: Pubkey,
}

struct RpcEndpoint {
    label: String,
    client: RpcClient,
}

impl RaydiumBootstrapFetcher {
    pub fn new(endpoints: Vec<String>) -> Result<Self> {
        ensure!(
            !endpoints.is_empty(),
            "at least one bootstrap RPC endpoint is required"
        );
        let timeout = Duration::from_millis(BOOTSTRAP_RPC_TIMEOUT_MS);
        let mut clients = Vec::with_capacity(endpoints.len());
        for url in endpoints {
            let client = RpcClient::new_with_timeout(url.clone(), timeout);
            clients.push(RpcEndpoint { label: url, client });
        }
        let program_id = Pubkey::from_str(RAYDIUM_V3_PROGRAM_ID)
            .expect("static Raydium program id should be valid");
        Ok(Self {
            clients,
            commitment: CommitmentConfig::confirmed(),
            program_id,
        })
    }

    pub fn bootstrap(
        &self,
        pools: &[RaydiumPoolTarget],
        registry: &StaticPoolRegistry,
    ) -> Result<usize> {
        if pools.is_empty() {
            return Ok(0);
        }
        let epoch = self.fetch_epoch()?;
        let mut recorded = 0usize;
        for target in pools {
            let snapshot = self.fetch_pool_snapshot(target, epoch)?;
            registry.record_raydium_snapshot(snapshot);
            telemetry::record_pool_bootstrap(PoolBootstrapStatus::Success, 1);
            recorded += 1;
        }
        Ok(recorded)
    }

    fn fetch_epoch(&self) -> Result<u64> {
        for endpoint in &self.clients {
            match endpoint.client.get_epoch_info() {
                Ok(info) => return Ok(info.epoch),
                Err(error) => warn!(
                    target = "bootstrap::raydium",
                    rpc = %endpoint.label,
                    %error,
                    "failed to fetch epoch info; trying next endpoint"
                ),
            }
        }
        bail!("failed to load epoch info from all bootstrap RPC endpoints")
    }

    fn fetch_pool_snapshot(
        &self,
        target: &RaydiumPoolTarget,
        epoch: u64,
    ) -> Result<RaydiumStaticSnapshot> {
        let mut last_error: Option<anyhow::Error> = None;
        for endpoint in &self.clients {
            match self.fetch_from_client(endpoint, target, epoch) {
                Ok(snapshot) => {
                    info!(
                        target = "bootstrap::raydium",
                        pool = %target.pool,
                        rpc = %endpoint.label,
                        tick_arrays = target.tick_arrays.len(),
                        "bootstrapped Raydium pool"
                    );
                    return Ok(snapshot);
                }
                Err(error) => {
                    warn!(
                        target = "bootstrap::raydium",
                        pool = %target.pool,
                        rpc = %endpoint.label,
                        %error,
                        "bootstrap attempt failed; trying next endpoint"
                    );
                    last_error = Some(error);
                }
            }
        }
        telemetry::record_pool_bootstrap(PoolBootstrapStatus::Failure, 1);
        Err(last_error.unwrap_or_else(|| {
            anyhow!(
                "bootstrap failed for pool {} using all configured RPC endpoints",
                target.pool
            )
        }))
    }

    fn fetch_from_client(
        &self,
        endpoint: &RpcEndpoint,
        target: &RaydiumPoolTarget,
        epoch: u64,
    ) -> Result<RaydiumStaticSnapshot> {
        let (pool_account, pool_slot) =
            self.fetch_account_with_slot(&endpoint.client, &target.pool, "pool_state")?;
        let pool_state = parse_pool_state(&pool_account.data)?;

        let (amm_account, _) =
            self.fetch_account_with_slot(&endpoint.client, &pool_state.amm_config, "amm_config")?;
        let amm_config = parse_amm_config(&amm_account.data)?;

        let (base_meta, base_hook) = fetch_mint_account_meta(
            &endpoint.client,
            &pool_state.token_mint_0,
            self.commitment,
            epoch,
        )?;
        let (quote_meta, quote_hook) = fetch_mint_account_meta(
            &endpoint.client,
            &pool_state.token_mint_1,
            self.commitment,
            epoch,
        )?;

        let tick_arrays = if target.tick_arrays.is_empty() {
            Vec::new()
        } else {
            let mut arrays = Vec::with_capacity(target.tick_arrays.len());
            for array_key in &target.tick_arrays {
                let (account, slot) =
                    self.fetch_account_with_slot(&endpoint.client, array_key, "tick_array")?;
                let array = parse_tick_array(&account.data, *array_key, target.pool, slot)?;
                arrays.push(array);
            }
            arrays
        };

        let snapshot = RaydiumStaticSnapshot {
            pool_id: target.pool,
            program_id: self.program_id,
            base_mint: pool_state.token_mint_0,
            quote_mint: pool_state.token_mint_1,
            meta: HydratedPoolMeta {
                amm_config: pool_state.amm_config,
                observation_state: pool_state.observation_key,
                tick_spacing: pool_state.tick_spacing,
                sqrt_price_x64: pool_state.sqrt_price_x64,
                liquidity: pool_state.liquidity,
                current_tick: pool_state.tick_current,
                pool_bump: pool_state.bump,
                trade_fee_rate: Some(amm_config.trade_fee_rate),
                protocol_fee_rate: Some(amm_config.protocol_fee_rate),
                fund_fee_rate: Some(amm_config.fund_fee_rate),
                tick_array_bitmap_extension: derive_tick_array_bitmap_extension(
                    &self.program_id,
                    &target.pool,
                ),
                pool_status: pool_state.status,
                slot: pool_slot,
            },
            tick_arrays,
            base_mint_meta: Some(base_meta),
            quote_mint_meta: Some(quote_meta),
            base_transfer_hook_program: base_hook,
            quote_transfer_hook_program: quote_hook,
            recorded_at: Instant::now(),
        };

        Ok(snapshot)
    }

    fn fetch_account_with_slot(
        &self,
        client: &RpcClient,
        key: &Pubkey,
        label: &str,
    ) -> Result<(Account, u64)> {
        let response = client.get_account_with_commitment(key, self.commitment)?;
        let slot = response.context.slot;
        let account = response
            .value
            .ok_or_else(|| anyhow!("{} account {} missing (slot {})", label, key, slot))?;
        Ok((account, slot))
    }
}

#[derive(Debug)]
struct ParsedPoolState {
    bump: u8,
    amm_config: Pubkey,
    observation_key: Pubkey,
    token_mint_0: Pubkey,
    token_mint_1: Pubkey,
    tick_spacing: i32,
    sqrt_price_x64: u128,
    liquidity: u128,
    tick_current: i32,
    status: u8,
}

#[derive(Debug)]
struct ParsedAmmConfig {
    trade_fee_rate: u32,
    protocol_fee_rate: u32,
    fund_fee_rate: u32,
}

fn parse_pool_state(data: &[u8]) -> Result<ParsedPoolState> {
    let content = strip_anchor_discriminator(data, &POOL_STATE_DISCRIMINATOR, "pool_state")?;
    let mut offset = 0;
    let bump = read_u8(content, &mut offset)?;
    let amm_config = read_pubkey(content, &mut offset)?;
    skip_pubkey(content, &mut offset)?; // owner
    let token_mint_0 = read_pubkey(content, &mut offset)?;
    let token_mint_1 = read_pubkey(content, &mut offset)?;
    skip_pubkey(content, &mut offset)?; // token_vault_0
    skip_pubkey(content, &mut offset)?; // token_vault_1
    let observation_key = read_pubkey(content, &mut offset)?;
    offset += 1; // mint_decimals_0
    offset += 1; // mint_decimals_1
    let tick_spacing = i32::from(read_u16(content, &mut offset)?);
    let liquidity = read_u128(content, &mut offset)?;
    let sqrt_price_x64 = read_u128(content, &mut offset)?;
    let tick_current = read_i32(content, &mut offset)?;
    offset += 4; // padding3 + padding4
    offset += 16; // fee_growth_global_0_x64
    offset += 16; // fee_growth_global_1_x64
    offset += 8; // protocol_fees_token_0
    offset += 8; // protocol_fees_token_1
    offset += 16 * 4; // swap stats
    let status = read_u8(content, &mut offset)?;

    Ok(ParsedPoolState {
        bump,
        amm_config,
        observation_key,
        token_mint_0,
        token_mint_1,
        tick_spacing,
        sqrt_price_x64,
        liquidity,
        tick_current,
        status,
    })
}

fn parse_amm_config(data: &[u8]) -> Result<ParsedAmmConfig> {
    let content = strip_anchor_discriminator(data, &AMM_CONFIG_STATE_DISCRIMINATOR, "amm_config")?;
    let mut offset = 0;
    offset += 1; // bump
    offset += 2; // index
    skip_pubkey(content, &mut offset)?; // owner
    let protocol_fee_rate = read_u32(content, &mut offset)?;
    let trade_fee_rate = read_u32(content, &mut offset)?;
    offset += 2; // tick_spacing
    let fund_fee_rate = read_u32(content, &mut offset)?;
    Ok(ParsedAmmConfig {
        trade_fee_rate,
        protocol_fee_rate,
        fund_fee_rate,
    })
}

fn parse_tick_array(
    data: &[u8],
    address: Pubkey,
    expected_pool: Pubkey,
    hydrated_slot: u64,
) -> Result<HydratedTickArray> {
    let content = strip_anchor_discriminator(data, &TICK_ARRAY_STATE_DISCRIMINATOR, "tick_array")?;
    let mut offset = 0;
    let pool_id = read_pubkey(content, &mut offset)?;
    ensure!(
        pool_id == expected_pool,
        "tick array {} belongs to pool {} not {}",
        address,
        pool_id,
        expected_pool
    );
    let start_tick_index = read_i32(content, &mut offset)?;
    let mut ticks = Vec::with_capacity(TICK_ARRAY_TICK_COUNT);
    for _ in 0..TICK_ARRAY_TICK_COUNT {
        ticks.push(read_tick(content, &mut offset)?);
    }
    let initialized_tick_count = read_u8(content, &mut offset)?;
    let recent_epoch = read_u64(content, &mut offset)?;

    Ok(HydratedTickArray {
        address,
        pool_id,
        start_tick_index,
        ticks,
        initialized_tick_count,
        recent_epoch,
        hydrated_slot,
    })
}

fn read_tick(data: &[u8], offset: &mut usize) -> Result<HydratedTick> {
    let tick = read_i32(data, offset)?;
    let liquidity_net = read_i128(data, offset)?;
    let liquidity_gross = read_u128(data, offset)?;
    let fee_growth_outside_0_x64 = read_u128(data, offset)?;
    let fee_growth_outside_1_x64 = read_u128(data, offset)?;
    let mut reward_growths_outside_x64 = [0u128; 3];
    for entry in reward_growths_outside_x64.iter_mut() {
        *entry = read_u128(data, offset)?;
    }
    *offset += 13 * 4; // padding
    Ok(HydratedTick {
        tick,
        liquidity_net,
        liquidity_gross,
        fee_growth_outside_0_x64,
        fee_growth_outside_1_x64,
        reward_growths_outside_x64,
    })
}

fn fetch_mint_account_meta(
    client: &RpcClient,
    mint: &Pubkey,
    commitment: CommitmentConfig,
    epoch: u64,
) -> Result<(MintAccountMeta, Option<Pubkey>)> {
    let response = client.get_account_with_commitment(mint, commitment)?;
    let account = response
        .value
        .ok_or_else(|| anyhow!("mint account {} missing", mint))?;
    parse_mint_account_meta(&account, epoch)
}

fn parse_mint_account_meta(
    account: &Account,
    epoch: u64,
) -> Result<(MintAccountMeta, Option<Pubkey>)> {
    let program = account.owner;
    if program == SPL_TOKEN_PROGRAM_ID {
        let decimals = account.data.get(44).copied();
        return Ok((
            MintAccountMeta {
                program,
                decimals,
                transfer_fee_config: None,
            },
            None,
        ));
    }
    if program == Pubkey::new_from_array(SPL_TOKEN_2022_PROGRAM_ID.to_bytes()) {
        let state = StateWithExtensionsOwned::<SplToken2022Mint>::unpack(account.data.clone())
            .with_context(|| format!("failed to parse Token-2022 mint {}", account.owner))?;
        let decimals = Some(state.base.decimals);
        let transfer_hook_program =
            transfer_hook::get_program_id(&state).map(|pk| Pubkey::new_from_array(pk.to_bytes()));
        let transfer_fee_config = parse_transfer_fee_summary(&state, epoch);
        return Ok((
            MintAccountMeta {
                program,
                decimals,
                transfer_fee_config,
            },
            transfer_hook_program,
        ));
    }
    Ok((
        MintAccountMeta {
            program,
            decimals: None,
            transfer_fee_config: None,
        },
        None,
    ))
}

fn parse_transfer_fee_summary(
    state: &StateWithExtensionsOwned<SplToken2022Mint>,
    epoch: u64,
) -> Option<TransferFeeConfigSummary> {
    let config = state.get_extension::<TransferFeeConfig>().ok()?;
    let fee = *config.get_epoch_fee(epoch);
    Some(TransferFeeConfigSummary {
        epoch,
        maximum_fee: fee.maximum_fee.into(),
        basis_points: fee.transfer_fee_basis_points.into(),
    })
}

fn strip_anchor_discriminator<'a>(
    data: &'a [u8],
    expected: &[u8; 8],
    label: &str,
) -> Result<&'a [u8]> {
    ensure!(
        data.len() >= ANCHOR_DISCRIMINATOR_LEN,
        "{} account smaller than anchor discriminator",
        label
    );
    ensure!(
        &data[..ANCHOR_DISCRIMINATOR_LEN] == expected,
        "{} account has unexpected discriminator",
        label
    );
    Ok(&data[ANCHOR_DISCRIMINATOR_LEN..])
}

fn read_pubkey(data: &[u8], offset: &mut usize) -> Result<Pubkey> {
    let bytes = read_const_bytes::<32>(data, offset)?;
    Ok(Pubkey::new_from_array(bytes))
}

fn skip_pubkey(data: &[u8], offset: &mut usize) -> Result<()> {
    read_const_bytes::<32>(data, offset)?;
    Ok(())
}

fn read_const_bytes<const N: usize>(data: &[u8], offset: &mut usize) -> Result<[u8; N]> {
    ensure!(
        data.len() >= *offset + N,
        "account truncated at offset {}",
        offset
    );
    let mut out = [0u8; N];
    out.copy_from_slice(&data[*offset..*offset + N]);
    *offset += N;
    Ok(out)
}

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8> {
    ensure!(
        data.len() > *offset,
        "account truncated at offset {}",
        offset
    );
    let value = data[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u16(data: &[u8], offset: &mut usize) -> Result<u16> {
    let bytes = read_const_bytes::<2>(data, offset)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32> {
    let bytes = read_const_bytes::<4>(data, offset)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64> {
    let bytes = read_const_bytes::<8>(data, offset)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_i32(data: &[u8], offset: &mut usize) -> Result<i32> {
    let bytes = read_const_bytes::<4>(data, offset)?;
    Ok(i32::from_le_bytes(bytes))
}

fn read_i128(data: &[u8], offset: &mut usize) -> Result<i128> {
    let bytes = read_const_bytes::<16>(data, offset)?;
    Ok(i128::from_le_bytes(bytes))
}

fn read_u128(data: &[u8], offset: &mut usize) -> Result<u128> {
    let bytes = read_const_bytes::<16>(data, offset)?;
    Ok(u128::from_le_bytes(bytes))
}

fn parse_pubkey(field: &str, value: &str) -> Result<Pubkey> {
    Pubkey::from_str(value.trim())
        .with_context(|| format!("invalid pubkey for {}: {}", field, value))
}

fn derive_tick_array_bitmap_extension(program_id: &Pubkey, pool_state: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[POOL_TICK_ARRAY_BITMAP_SEED, pool_state.as_ref()],
        program_id,
    )
    .0
}
