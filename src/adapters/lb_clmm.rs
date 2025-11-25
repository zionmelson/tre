use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, trace, warn};

use crate::stages::types::{LbClmmPoolExtras, PoolUpdate};

use super::{
    build_quote_response, quote_from_reserves, ProtocolAdapter, QuoteRequest, QuoteResponse,
    SwapDirection,
};

const LB_CLMM_PROGRAM_ID: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SPL_TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

static SPL_TOKEN_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str(SPL_TOKEN_PROGRAM_ID).expect("static spl token program id should be valid")
});
static SPL_TOKEN_2022_PROGRAM_PUBKEY: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str(SPL_TOKEN_2022_PROGRAM_ID)
        .expect("static spl token 2022 program id should be valid")
});

#[derive(Debug, Clone)]
pub struct LbClmmAdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
}

impl Default for LbClmmAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 1_000,
        }
    }
}

pub struct LbClmmAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, LbClmmState>,
    config: LbClmmAdapterConfig,
    hydrator: Option<LbClmmPoolHydrator>,
}

impl LbClmmAdapter {
    pub fn new(config: LbClmmAdapterConfig) -> Self {
        let program_id = Pubkey::from_str(LB_CLMM_PROGRAM_ID)
            .expect("static lb_clmm program id should be valid");
        let hydrator = LbClmmPoolHydrator::new(&config);
        if let Some(hydrator_ref) = hydrator.as_ref() {
            info!(
                target = "adapter::lb_clmm",
                endpoints = hydrator_ref.client_count(),
                "lb_clmm pool hydrator online"
            );
        } else {
            debug!(
                target = "adapter::lb_clmm",
                "lb_clmm pool hydrator disabled (no rpc endpoints)"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            hydrator,
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<LbClmmState> {
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
                    target = "adapter::lb_clmm",
                    pool = %state.lb_pair,
                    last_slot = state.last_update_slot,
                    "evicting stale lb_clmm state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: LbClmmPoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::lb_clmm",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let extras = self.enrich_extras(pool_pubkey, extras);
        let Some(token_x_mint) = extras
            .token_x_mint
            .as_ref()
            .and_then(|value| parse_pubkey(value))
        else {
            warn!(target = "adapter::lb_clmm", pool = %update.pool_id, "missing token_x mint");
            return;
        };

        let Some(token_y_mint) = extras
            .token_y_mint
            .as_ref()
            .and_then(|value| parse_pubkey(value))
        else {
            warn!(target = "adapter::lb_clmm", pool = %update.pool_id, "missing token_y mint");
            return;
        };

        let state = LbClmmState {
            lb_pair: pool_pubkey,
            token_x_mint,
            token_y_mint,
            token_x_program: extras
                .token_x_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_y_program: extras
                .token_y_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            reserve_x: extras
                .reserve_x
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            reserve_y: extras
                .reserve_y
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            oracle: extras.oracle.as_ref().and_then(|value| parse_pubkey(value)),
            bin_array_bitmap_extension: extras
                .bin_array_bitmap_extension
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            host_fee_account: extras
                .host_fee_account
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            bin_step: extras.bin_step,
            active_bin_id: extras.active_bin_id,
            dynamic_fee_bps: extras.dynamic_fee_bps,
            protocol_fee_bps: extras.protocol_fee_bps,
            reserve_x_balance: extras.reserve_x_balance,
            reserve_y_balance: extras.reserve_y_balance,
            volatility_bps: extras.volatility_bps,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
        };

        trace!(
            target = "adapter::lb_clmm",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting lb_clmm pool state"
        );

        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: LbClmmPoolExtras) -> LbClmmPoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.token_x_mint = extras
                .token_x_mint
                .or_else(|| Some(meta.token_x_mint.to_string()));
            extras.token_y_mint = extras
                .token_y_mint
                .or_else(|| Some(meta.token_y_mint.to_string()));
            extras.reserve_x = extras
                .reserve_x
                .or_else(|| Some(meta.reserve_x.to_string()));
            extras.reserve_y = extras
                .reserve_y
                .or_else(|| Some(meta.reserve_y.to_string()));
            extras.oracle = extras.oracle.or_else(|| Some(meta.oracle.to_string()));
            extras.token_x_program = extras
                .token_x_program
                .or_else(|| meta.token_x_program.map(|pk| pk.to_string()));
            extras.token_y_program = extras
                .token_y_program
                .or_else(|| meta.token_y_program.map(|pk| pk.to_string()));
            extras.bin_step = extras.bin_step.or(Some(meta.bin_step));
            extras.active_bin_id = extras.active_bin_id.or(Some(meta.active_bin_id));
        }

        extras
    }
}

impl ProtocolAdapter for LbClmmAdapter {
    fn name(&self) -> &'static str {
        "lb_clmm"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::lb_clmm",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        let extras = update.extensions.lb_clmm.clone().unwrap_or_default();
        self.upsert_state(update, extras);
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let (Some(reserve_x), Some(reserve_y)) = (state.reserve_x_balance, state.reserve_y_balance)
        else {
            return None;
        };
        let amount_out = quote_from_reserves(
            &req,
            &state.token_x_mint,
            &state.token_y_mint,
            reserve_x,
            reserve_y,
        )?;
        Some(build_quote_response(req, amount_out))
    }
}

fn needs_hydration(extras: &LbClmmPoolExtras) -> bool {
    extras.token_x_mint.is_none()
        || extras.token_y_mint.is_none()
        || extras.reserve_x.is_none()
        || extras.reserve_y.is_none()
        || extras.oracle.is_none()
        || extras.token_x_program.is_none()
        || extras.token_y_program.is_none()
        || extras.bin_step.is_none()
        || extras.active_bin_id.is_none()
}

#[derive(Debug, Clone)]
pub struct LbClmmState {
    pub lb_pair: Pubkey,
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub token_x_program: Option<Pubkey>,
    pub token_y_program: Option<Pubkey>,
    pub reserve_x: Option<Pubkey>,
    pub reserve_y: Option<Pubkey>,
    pub oracle: Option<Pubkey>,
    pub bin_array_bitmap_extension: Option<Pubkey>,
    pub host_fee_account: Option<Pubkey>,
    pub bin_step: Option<u16>,
    pub active_bin_id: Option<i32>,
    pub dynamic_fee_bps: Option<u16>,
    pub protocol_fee_bps: Option<u16>,
    pub reserve_x_balance: Option<u128>,
    pub reserve_y_balance: Option<u128>,
    pub volatility_bps: Option<u16>,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
}

impl LbClmmState {
    pub fn mid_price(&self) -> Option<f64> {
        match (self.reserve_x_balance, self.reserve_y_balance) {
            (Some(x), Some(y)) if x > 0 => Some(y as f64 / x as f64),
            _ => None,
        }
    }
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    token_x_mint: Pubkey,
    token_y_mint: Pubkey,
    reserve_x: Pubkey,
    reserve_y: Pubkey,
    oracle: Pubkey,
    token_x_program: Option<Pubkey>,
    token_y_program: Option<Pubkey>,
    bin_step: u16,
    active_bin_id: i32,
}

struct LbClmmPoolHydrator {
    clients: Vec<Arc<RpcClient>>,
    cache: DashMap<Pubkey, HydratedPoolMeta>,
}

impl LbClmmPoolHydrator {
    fn new(config: &LbClmmAdapterConfig) -> Option<Self> {
        if config.rpc_endpoints.is_empty() {
            return None;
        }

        let timeout = Duration::from_millis(config.rpc_timeout_ms.max(100));
        let clients: Vec<Arc<RpcClient>> = config
            .rpc_endpoints
            .iter()
            .map(|url| Arc::new(RpcClient::new_with_timeout(url.clone(), timeout)))
            .collect();

        if clients.is_empty() {
            return None;
        }

        Some(Self {
            clients,
            cache: DashMap::new(),
        })
    }

    fn client_count(&self) -> usize {
        self.clients.len()
    }

    fn hydrate(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        if let Some(entry) = self.cache.get(&pool) {
            return Some(entry.clone());
        }
        self.fetch_from_rpc(pool)
    }

    fn fetch_from_rpc(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        for client in &self.clients {
            match client.get_account(&pool) {
                Ok(account) => {
                    if let Some(meta) = parse_lb_pair_account(&account.data) {
                        self.cache.insert(pool, meta.clone());
                        return Some(meta);
                    } else {
                        warn!(
                            target = "adapter::lb_clmm",
                            pool = %pool,
                            "unexpected lb_clmm pool account layout"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        target = "adapter::lb_clmm",
                        pool = %pool,
                        %error,
                        "failed to fetch lb_clmm pool account"
                    );
                }
            }
        }

        None
    }
}

const STATIC_PARAMETERS_LEN: usize = 32;
const VARIABLE_PARAMETERS_LEN: usize = 32;
const PROTOCOL_FEE_LEN: usize = 16;
const REWARD_INFO_LEN: usize = 144;
const REWARD_INFO_COUNT: usize = 2;
const BIN_ARRAY_BITMAP_BYTES: usize = 16 * 8;
const PUBKEY_BYTES: usize = 32;
const LB_PAIR_ACCOUNT_MIN_LEN: usize = STATIC_PARAMETERS_LEN
        + VARIABLE_PARAMETERS_LEN
        + 1 // bump_seed
        + 2 // bin_step_seed
        + 1 // pair_type
        + 4 // active_id
        + 2 // bin_step
        + 1 // status
        + 1 // require_base_factor_seed
        + 2 // base_factor_seed
        + 1 // activation_type
        + 1 // creator_pool_on_off_control
        + PUBKEY_BYTES * 4 // token mints + reserves
        + PROTOCOL_FEE_LEN
        + 32 // _padding_1
        + REWARD_INFO_LEN * REWARD_INFO_COUNT
        + PUBKEY_BYTES // oracle
        + BIN_ARRAY_BITMAP_BYTES
        + 8 // last_updated_at
        + 32 // _padding_2
        + PUBKEY_BYTES // pre_activation_swap_address
        + PUBKEY_BYTES // base_key
        + 8 // activation_point
        + 8 // pre_activation_duration
        + 8 // _padding_3
        + 8 // _padding_4
        + PUBKEY_BYTES // creator
        + 1 // token_mint_x_program_flag
        + 1 // token_mint_y_program_flag
        + 22; // _reserved

fn parse_lb_pair_account(data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < LB_PAIR_ACCOUNT_MIN_LEN {
        return None;
    }

    let mut cursor = STATIC_PARAMETERS_LEN + VARIABLE_PARAMETERS_LEN;
    skip_bytes(data, &mut cursor, 1)?; // bump_seed
    skip_bytes(data, &mut cursor, 2)?; // bin_step_seed
    skip_bytes(data, &mut cursor, 1)?; // pair_type
    let active_bin_id = read_i32(data, &mut cursor)?;
    let bin_step = read_u16(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 1)?; // status
    skip_bytes(data, &mut cursor, 1)?; // require_base_factor_seed
    skip_bytes(data, &mut cursor, 2)?; // base_factor_seed
    skip_bytes(data, &mut cursor, 1)?; // activation_type
    skip_bytes(data, &mut cursor, 1)?; // creator_pool_on_off_control

    let token_x_mint = read_pubkey_bytes(data, &mut cursor)?;
    let token_y_mint = read_pubkey_bytes(data, &mut cursor)?;
    let reserve_x = read_pubkey_bytes(data, &mut cursor)?;
    let reserve_y = read_pubkey_bytes(data, &mut cursor)?;

    skip_bytes(data, &mut cursor, PROTOCOL_FEE_LEN)?;
    skip_bytes(data, &mut cursor, 32)?; // _padding_1
    skip_bytes(data, &mut cursor, REWARD_INFO_LEN * REWARD_INFO_COUNT)?;
    let oracle = read_pubkey_bytes(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, BIN_ARRAY_BITMAP_BYTES)?;
    skip_bytes(data, &mut cursor, 8)?; // last_updated_at
    skip_bytes(data, &mut cursor, 32)?; // _padding_2
    skip_bytes(data, &mut cursor, PUBKEY_BYTES)?; // pre_activation_swap_address
    skip_bytes(data, &mut cursor, PUBKEY_BYTES)?; // base_key
    skip_bytes(data, &mut cursor, 8)?; // activation_point
    skip_bytes(data, &mut cursor, 8)?; // pre_activation_duration
    skip_bytes(data, &mut cursor, 8)?; // _padding_3
    skip_bytes(data, &mut cursor, 8)?; // _padding_4
    skip_bytes(data, &mut cursor, PUBKEY_BYTES)?; // creator
    let token_x_flag = read_u8(data, &mut cursor)?;
    let token_y_flag = read_u8(data, &mut cursor)?;
    skip_bytes(data, &mut cursor, 22)?; // _reserved

    Some(HydratedPoolMeta {
        token_x_mint,
        token_y_mint,
        reserve_x,
        reserve_y,
        oracle,
        token_x_program: map_token_program_flag(token_x_flag),
        token_y_program: map_token_program_flag(token_y_flag),
        bin_step,
        active_bin_id,
    })
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Option<u16> {
    let end = (*cursor).checked_add(2)?;
    let bytes = data.get(*cursor..end)?;
    *cursor = end;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Option<i32> {
    let end = (*cursor).checked_add(4)?;
    let bytes = data.get(*cursor..end)?;
    *cursor = end;
    Some(i32::from_le_bytes(bytes.try_into().ok()?))
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    let value = *data.get(*cursor)?;
    *cursor = (*cursor).checked_add(1)?;
    Some(value)
}

fn skip_bytes(data: &[u8], cursor: &mut usize, len: usize) -> Option<()> {
    let end = (*cursor).checked_add(len)?;
    if end > data.len() {
        return None;
    }
    *cursor = end;
    Some(())
}

fn read_pubkey_bytes(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    let end = (*cursor).checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

fn map_token_program_flag(flag: u8) -> Option<Pubkey> {
    match flag {
        0 => Some(*SPL_TOKEN_PROGRAM_PUBKEY),
        1 => Some(*SPL_TOKEN_2022_PROGRAM_PUBKEY),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lb_pair_account_static_fields() {
        let token_x = Pubkey::new_unique();
        let token_y = Pubkey::new_unique();
        let reserve_x = Pubkey::new_unique();
        let reserve_y = Pubkey::new_unique();
        let oracle = Pubkey::new_unique();
        let active_bin_id = 42;
        let bin_step = 25;

        let mut data = vec![0u8; LB_PAIR_ACCOUNT_MIN_LEN];
        let mut cursor = STATIC_PARAMETERS_LEN + VARIABLE_PARAMETERS_LEN;
        cursor += 1 + 2 + 1; // bump, seed, pair_type
        write_i32(&mut data, &mut cursor, active_bin_id);
        write_u16(&mut data, &mut cursor, bin_step);
        cursor += 1 + 1 + 2 + 1 + 1; // status + require + base_factor_seed + activation + control
        write_pubkey(&mut data, &mut cursor, token_x);
        write_pubkey(&mut data, &mut cursor, token_y);
        write_pubkey(&mut data, &mut cursor, reserve_x);
        write_pubkey(&mut data, &mut cursor, reserve_y);
        cursor += PROTOCOL_FEE_LEN + 32 + REWARD_INFO_LEN * REWARD_INFO_COUNT;
        write_pubkey(&mut data, &mut cursor, oracle);
        cursor += BIN_ARRAY_BITMAP_BYTES + 8 + 32 + PUBKEY_BYTES * 2 + 8 + 8 + 8 + 8 + PUBKEY_BYTES;
        data[cursor] = 0;
        cursor += 1;
        data[cursor] = 1;

        let meta = parse_lb_pair_account(&data).expect("should parse hydrated meta");
        assert_eq!(meta.token_x_mint, token_x);
        assert_eq!(meta.token_y_mint, token_y);
        assert_eq!(meta.reserve_x, reserve_x);
        assert_eq!(meta.reserve_y, reserve_y);
        assert_eq!(meta.oracle, oracle);
        assert_eq!(meta.active_bin_id, active_bin_id);
        assert_eq!(meta.bin_step, bin_step);
        assert_eq!(meta.token_x_program, Some(*SPL_TOKEN_PROGRAM_PUBKEY));
        assert_eq!(meta.token_y_program, Some(*SPL_TOKEN_2022_PROGRAM_PUBKEY));
    }

    fn write_pubkey(data: &mut [u8], cursor: &mut usize, key: Pubkey) {
        let end = *cursor + PUBKEY_BYTES;
        data[*cursor..end].copy_from_slice(key.as_ref());
        *cursor = end;
    }

    fn write_u16(data: &mut [u8], cursor: &mut usize, value: u16) {
        let end = *cursor + 2;
        data[*cursor..end].copy_from_slice(&value.to_le_bytes());
        *cursor = end;
    }

    fn write_i32(data: &mut [u8], cursor: &mut usize, value: i32) {
        let end = *cursor + 4;
        data[*cursor..end].copy_from_slice(&value.to_le_bytes());
        *cursor = end;
    }
}
