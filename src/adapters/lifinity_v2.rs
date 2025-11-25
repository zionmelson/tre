use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, trace, warn};

use crate::stages::types::{LifinityPoolExtras, PoolUpdate};

use super::{
    build_quote_response, quote_from_reserves, ProtocolAdapter, QuoteRequest, QuoteResponse,
    SwapDirection,
};

const LIFINITY_PROGRAM_ID: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";

#[derive(Debug, Clone)]
pub struct LifinityAdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
}

impl Default for LifinityAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 1_000,
        }
    }
}

pub struct LifinityAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, LifinityPoolState>,
    config: LifinityAdapterConfig,
    hydrator: Option<LifinityPoolHydrator>,
}

impl LifinityAdapter {
    pub fn new(config: LifinityAdapterConfig) -> Self {
        let program_id = Pubkey::from_str(LIFINITY_PROGRAM_ID)
            .expect("static Lifinity AMM v2 program id should be valid");
        let hydrator = LifinityPoolHydrator::new(program_id, &config);
        if let Some(hydrator) = hydrator.as_ref() {
            info!(
                target = "adapter::lifinity",
                endpoints = hydrator.client_count(),
                "lifinity hydrator online"
            );
        } else {
            debug!(
                target = "adapter::lifinity",
                "lifinity hydrator disabled (no rpc endpoints)"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            hydrator,
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<LifinityPoolState> {
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
                    target = "adapter::lifinity",
                    pool = %state.amm,
                    last_slot = state.last_update_slot,
                    "evicting stale Lifinity state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: LifinityPoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::lifinity",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let extras = self.enrich_extras(pool_pubkey, extras);

        let Some(base_mint) = parse_pubkey(&update.base_mint) else {
            warn!(target = "adapter::lifinity", pool = %update.pool_id, "invalid base mint");
            return;
        };

        let Some(quote_mint) = parse_pubkey(&update.quote_mint) else {
            warn!(target = "adapter::lifinity", pool = %update.pool_id, "invalid quote mint");
            return;
        };

        let state = LifinityPoolState {
            amm: pool_pubkey,
            base_mint,
            quote_mint,
            authority: extras
                .authority
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_a_vault: extras
                .token_a_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_b_vault: extras
                .token_b_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            pool_mint: extras
                .pool_mint
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            fee_account: extras
                .fee_account
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_program: extras
                .token_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            oracle_main: extras
                .oracle_main
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            oracle_sub: extras
                .oracle_sub
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            oracle_pc: extras
                .oracle_pc
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            fees_bps: extras.fees_bps,
            owner_fee_bps: extras.owner_fee_bps,
            host_fee_bps: extras.host_fee_bps,
            std_spread_bps: extras.std_spread_bps,
            spread_buffer_bps: extras.spread_buffer_bps,
            vault_a_balance: extras.vault_a_balance,
            vault_b_balance: extras.vault_b_balance,
            freeze_trade: extras.freeze_trade,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
        };

        trace!(
            target = "adapter::lifinity",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting Lifinity pool state"
        );

        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: LifinityPoolExtras) -> LifinityPoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.authority = extras
                .authority
                .or_else(|| Some(meta.authority.to_string()));
            extras.token_program = extras
                .token_program
                .or_else(|| Some(meta.token_program.to_string()));
            extras.token_a_vault = extras
                .token_a_vault
                .or_else(|| Some(meta.token_a_vault.to_string()));
            extras.token_b_vault = extras
                .token_b_vault
                .or_else(|| Some(meta.token_b_vault.to_string()));
            extras.pool_mint = extras
                .pool_mint
                .or_else(|| Some(meta.pool_mint.to_string()));
            extras.fee_account = extras
                .fee_account
                .or_else(|| Some(meta.fee_account.to_string()));
            extras.oracle_main = extras
                .oracle_main
                .or_else(|| Some(meta.oracle_main.to_string()));
            extras.oracle_sub = extras
                .oracle_sub
                .or_else(|| Some(meta.oracle_sub.to_string()));
            extras.oracle_pc = extras
                .oracle_pc
                .or_else(|| Some(meta.oracle_pc.to_string()));
            extras.fees_bps = extras.fees_bps.or(meta.fees_bps);
            extras.owner_fee_bps = extras.owner_fee_bps.or(meta.owner_fee_bps);
            extras.host_fee_bps = extras.host_fee_bps.or(meta.host_fee_bps);
            extras.std_spread_bps = extras.std_spread_bps.or(meta.std_spread_bps);
            extras.spread_buffer_bps = extras.spread_buffer_bps.or(meta.spread_buffer_bps);
            extras.freeze_trade = extras.freeze_trade.or(Some(meta.freeze_trade));
        }

        extras
    }
}

impl ProtocolAdapter for LifinityAdapter {
    fn name(&self) -> &'static str {
        "lifinity_amm_v2"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::lifinity",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        let extras = update.extensions.lifinity_v2.clone().unwrap_or_default();
        self.upsert_state(update, extras);
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let (Some(vault_a), Some(vault_b)) = (state.vault_a_balance, state.vault_b_balance) else {
            return None;
        };
        let amount_out =
            quote_from_reserves(&req, &state.base_mint, &state.quote_mint, vault_a, vault_b)?;
        Some(build_quote_response(req, amount_out))
    }
}

#[derive(Debug, Clone)]
pub struct LifinityPoolState {
    pub amm: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub authority: Option<Pubkey>,
    pub token_a_vault: Option<Pubkey>,
    pub token_b_vault: Option<Pubkey>,
    pub pool_mint: Option<Pubkey>,
    pub fee_account: Option<Pubkey>,
    pub token_program: Option<Pubkey>,
    pub oracle_main: Option<Pubkey>,
    pub oracle_sub: Option<Pubkey>,
    pub oracle_pc: Option<Pubkey>,
    pub fees_bps: Option<u16>,
    pub owner_fee_bps: Option<u16>,
    pub host_fee_bps: Option<u16>,
    pub std_spread_bps: Option<u16>,
    pub spread_buffer_bps: Option<u16>,
    pub vault_a_balance: Option<u128>,
    pub vault_b_balance: Option<u128>,
    pub freeze_trade: Option<u8>,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
}

impl LifinityPoolState {
    pub fn mid_price(&self) -> Option<f64> {
        match (self.vault_a_balance, self.vault_b_balance) {
            (Some(a), Some(b)) if a > 0 => Some(b as f64 / a as f64),
            _ => None,
        }
    }
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

fn needs_hydration(extras: &LifinityPoolExtras) -> bool {
    extras.authority.is_none()
        || extras.token_a_vault.is_none()
        || extras.token_b_vault.is_none()
        || extras.pool_mint.is_none()
        || extras.fee_account.is_none()
        || extras.token_program.is_none()
        || extras.oracle_main.is_none()
        || extras.oracle_sub.is_none()
        || extras.oracle_pc.is_none()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    authority: Pubkey,
    token_program: Pubkey,
    token_a_vault: Pubkey,
    token_b_vault: Pubkey,
    pool_mint: Pubkey,
    fee_account: Pubkey,
    oracle_main: Pubkey,
    oracle_sub: Pubkey,
    oracle_pc: Pubkey,
    fees_bps: Option<u16>,
    owner_fee_bps: Option<u16>,
    host_fee_bps: Option<u16>,
    std_spread_bps: Option<u16>,
    spread_buffer_bps: Option<u16>,
    freeze_trade: u8,
}

struct LifinityPoolHydrator {
    clients: Vec<Arc<RpcClient>>,
    cache: DashMap<Pubkey, HydratedPoolMeta>,
    program_id: Pubkey,
}

impl LifinityPoolHydrator {
    fn new(program_id: Pubkey, config: &LifinityAdapterConfig) -> Option<Self> {
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
            program_id,
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
                    if let Some(meta) = parse_amm_account(&self.program_id, pool, &account.data) {
                        self.cache.insert(pool, meta.clone());
                        return Some(meta);
                    } else {
                        warn!(
                            target = "adapter::lifinity",
                            pool = %pool,
                            "unexpected Lifinity Amm account layout"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        target = "adapter::lifinity",
                        pool = %pool,
                        %error,
                        "failed to fetch Lifinity Amm account"
                    );
                }
            }
        }

        None
    }
}

const ANCHOR_DISCRIMINATOR_LEN: usize = 8;
const PUBKEY_BYTES: usize = 32;

fn parse_amm_account(program_id: &Pubkey, pool: Pubkey, data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < ANCHOR_DISCRIMINATOR_LEN + PUBKEY_BYTES * 3 {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;

    // Skip initializer metadata (3 pubkeys + 2 u64)
    for _ in 0..3 {
        read_pubkey(data, &mut cursor)?;
    }
    read_u64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?;

    // Flags + bumps
    let _ = read_u8(data, &mut cursor)?; // is_initialized
    let bump_seed = read_u8(data, &mut cursor)?;
    let freeze_trade = read_u8(data, &mut cursor)?;
    read_u8(data, &mut cursor)?; // freeze_deposit
    read_u8(data, &mut cursor)?; // freeze_withdraw
    read_u8(data, &mut cursor)?; // base_decimals

    let token_program = read_pubkey(data, &mut cursor)?;
    let token_a_vault = read_pubkey(data, &mut cursor)?;
    let token_b_vault = read_pubkey(data, &mut cursor)?;
    let pool_mint = read_pubkey(data, &mut cursor)?;
    read_pubkey(data, &mut cursor)?; // token_a_mint
    read_pubkey(data, &mut cursor)?; // token_b_mint
    let fee_account = read_pubkey(data, &mut cursor)?;
    let oracle_main = read_pubkey(data, &mut cursor)?;
    let oracle_sub = read_pubkey(data, &mut cursor)?;
    let oracle_pc = read_pubkey(data, &mut cursor)?;

    // Fees
    let trade_fee_num = read_u64(data, &mut cursor)?;
    let trade_fee_den = read_u64(data, &mut cursor)?;
    let owner_fee_num = read_u64(data, &mut cursor)?;
    let owner_fee_den = read_u64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?; // owner withdraw numerator
    read_u64(data, &mut cursor)?; // owner withdraw denominator
    let host_fee_num = read_u64(data, &mut cursor)?;
    let host_fee_den = read_u64(data, &mut cursor)?;

    // Curve
    read_u8(data, &mut cursor)?; // curve type
    read_u64(data, &mut cursor)?; // curve parameters

    // Config header
    read_u64(data, &mut cursor)?; // last_price
    read_u64(data, &mut cursor)?; // last_balanced_price
    let config_denominator = read_u64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?; // volume_x
    read_u64(data, &mut cursor)?; // volume_y
    read_u64(data, &mut cursor)?; // volume_x_in_y
    read_u64(data, &mut cursor)?; // deposit_cap
    read_u64(data, &mut cursor)?; // regression_target
    read_u64(data, &mut cursor)?; // oracle_type
    read_u64(data, &mut cursor)?; // oracle_status
    read_u64(data, &mut cursor)?; // oracle_main_slot_limit
    read_u64(data, &mut cursor)?; // oracle_sub_confidence_limit
    read_u64(data, &mut cursor)?; // oracle_sub_slot_limit
    read_u64(data, &mut cursor)?; // oracle_pc_confidence_limit
    read_u64(data, &mut cursor)?; // oracle_pc_slot_limit
    let std_spread = read_u64(data, &mut cursor)?;
    let std_spread_buffer = read_u64(data, &mut cursor)?;
    read_u64(data, &mut cursor)?; // spread coefficient
    read_i64(data, &mut cursor)?; // price buffer coin
    read_i64(data, &mut cursor)?; // price buffer pc
    read_u64(data, &mut cursor)?; // rebalance ratio
    read_u64(data, &mut cursor)?; // fee trade
    read_u64(data, &mut cursor)?; // fee platform
    read_u64(data, &mut cursor)?; // oracle main slot buffer
    for _ in 0..5 {
        read_u64(data, &mut cursor)?; // config temps
    }

    // Temp pubkeys (unused)
    for _ in 0..5 {
        read_pubkey(data, &mut cursor)?;
    }

    let authority =
        Pubkey::create_program_address(&[pool.as_ref(), &[bump_seed]], program_id).ok()?;

    Some(HydratedPoolMeta {
        authority,
        token_program,
        token_a_vault,
        token_b_vault,
        pool_mint,
        fee_account,
        oracle_main,
        oracle_sub,
        oracle_pc,
        fees_bps: ratio_to_bps(trade_fee_num, trade_fee_den),
        owner_fee_bps: ratio_to_bps(owner_fee_num, owner_fee_den),
        host_fee_bps: ratio_to_bps(host_fee_num, host_fee_den),
        std_spread_bps: ratio_to_bps(std_spread, config_denominator),
        spread_buffer_bps: ratio_to_bps(std_spread_buffer, config_denominator),
        freeze_trade,
    })
}

fn read_pubkey(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    let end = cursor.checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Option<u64> {
    let end = cursor.checked_add(8)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u64::from_le_bytes(bytes))
}

fn read_i64(data: &[u8], cursor: &mut usize) -> Option<i64> {
    let end = cursor.checked_add(8)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(i64::from_le_bytes(bytes))
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    let byte = *data.get(*cursor)?;
    *cursor = (*cursor).checked_add(1)?;
    Some(byte)
}

fn ratio_to_bps(num: u64, denom: u64) -> Option<u16> {
    if denom == 0 {
        return None;
    }
    let value = (num as u128).saturating_mul(10_000) / denom as u128;
    Some(value.min(u16::MAX as u128) as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_pubkey(buf: &mut Vec<u8>, key: &Pubkey) {
        buf.extend_from_slice(key.as_ref());
    }

    fn push_u64(buf: &mut Vec<u8>, value: u64) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn push_i64(buf: &mut Vec<u8>, value: i64) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    #[test]
    fn parses_lifinity_amm_account() {
        let program_id = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let bump = 9u8;

        let token_program = Pubkey::new_unique();
        let token_a_vault = Pubkey::new_unique();
        let token_b_vault = Pubkey::new_unique();
        let pool_mint = Pubkey::new_unique();
        let token_a_mint = Pubkey::new_unique();
        let token_b_mint = Pubkey::new_unique();
        let fee_account = Pubkey::new_unique();
        let oracle_main = Pubkey::new_unique();
        let oracle_sub = Pubkey::new_unique();
        let oracle_pc = Pubkey::new_unique();

        let mut data = Vec::new();
        data.resize(ANCHOR_DISCRIMINATOR_LEN, 0);

        for _ in 0..3 {
            push_pubkey(&mut data, &Pubkey::new_unique());
        }
        push_u64(&mut data, 0);
        push_u64(&mut data, 0);
        data.push(1); // is_initialized
        data.push(bump);
        data.push(1); // freeze_trade
        data.push(0);
        data.push(0);
        data.push(6); // base decimals
        push_pubkey(&mut data, &token_program);
        push_pubkey(&mut data, &token_a_vault);
        push_pubkey(&mut data, &token_b_vault);
        push_pubkey(&mut data, &pool_mint);
        push_pubkey(&mut data, &token_a_mint);
        push_pubkey(&mut data, &token_b_mint);
        push_pubkey(&mut data, &fee_account);
        push_pubkey(&mut data, &oracle_main);
        push_pubkey(&mut data, &oracle_sub);
        push_pubkey(&mut data, &oracle_pc);

        // Fees (trade=25 bps, owner=5 bps, host=10 bps)
        push_u64(&mut data, 25);
        push_u64(&mut data, 10_000);
        push_u64(&mut data, 5);
        push_u64(&mut data, 10_000);
        push_u64(&mut data, 0);
        push_u64(&mut data, 1);
        push_u64(&mut data, 10);
        push_u64(&mut data, 1_000);

        data.push(1); // curve type
        push_u64(&mut data, 0);

        // Config values
        push_u64(&mut data, 0); // last_price
        push_u64(&mut data, 0); // last_balanced_price
        push_u64(&mut data, 1_000_000); // config_denominator
        for _ in 0..12 {
            push_u64(&mut data, 0);
        }
        push_u64(&mut data, 25_000); // std_spread raw = 250 bps
        push_u64(&mut data, 10_000); // spread buffer raw = 100 bps
        push_u64(&mut data, 0);
        push_i64(&mut data, 0);
        push_i64(&mut data, 0);
        for _ in 0..4 {
            push_u64(&mut data, 0);
        }
        for _ in 0..5 {
            push_u64(&mut data, 0);
        }

        for _ in 0..5 {
            push_pubkey(&mut data, &Pubkey::new_unique());
        }

        let meta = parse_amm_account(&program_id, pool, &data).expect("should parse");
        let expected_authority =
            Pubkey::create_program_address(&[pool.as_ref(), &[bump]], &program_id).unwrap();

        assert_eq!(meta.authority, expected_authority);
        assert_eq!(meta.token_program, token_program);
        assert_eq!(meta.token_a_vault, token_a_vault);
        assert_eq!(meta.token_b_vault, token_b_vault);
        assert_eq!(meta.pool_mint, pool_mint);
        assert_eq!(meta.fee_account, fee_account);
        assert_eq!(meta.oracle_main, oracle_main);
        assert_eq!(meta.oracle_sub, oracle_sub);
        assert_eq!(meta.oracle_pc, oracle_pc);
        assert_eq!(meta.fees_bps, Some(25));
        assert_eq!(meta.owner_fee_bps, Some(5));
        assert_eq!(meta.host_fee_bps, Some(100));
        assert_eq!(meta.std_spread_bps, Some(250));
        assert_eq!(meta.spread_buffer_bps, Some(100));
        assert_eq!(meta.freeze_trade, 1);
    }
}
