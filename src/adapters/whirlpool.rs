use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, trace, warn};

use crate::stages::types::{PoolUpdate, WhirlpoolPoolExtras};

use super::{
    build_quote_response, quote_from_price, ProtocolAdapter, QuoteRequest, QuoteResponse,
    SwapDirection,
};

const WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicV1cqQhBYtbJX3Yq9z7Dfc7gXLMGr5vLyN";

#[derive(Debug, Clone)]
pub struct WhirlpoolAdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
}

impl Default for WhirlpoolAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 1_000,
        }
    }
}

pub struct WhirlpoolAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, WhirlpoolState>,
    config: WhirlpoolAdapterConfig,
    hydrator: Option<WhirlpoolPoolHydrator>,
}

impl WhirlpoolAdapter {
    pub fn new(config: WhirlpoolAdapterConfig) -> Self {
        let program_id = Pubkey::from_str(WHIRLPOOL_PROGRAM_ID)
            .expect("static Whirlpool program id should be valid");
        let hydrator = WhirlpoolPoolHydrator::new(&config, program_id);
        if let Some(hydrator) = hydrator.as_ref() {
            info!(
                target = "adapter::whirlpool",
                endpoints = hydrator.client_count(),
                "whirlpool pool hydrator online"
            );
        } else {
            debug!(
                target = "adapter::whirlpool",
                "whirlpool pool hydrator disabled (no rpc endpoints)"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            hydrator,
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<WhirlpoolState> {
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
                    target = "adapter::whirlpool",
                    pool = %state.whirlpool,
                    last_slot = state.last_update_slot,
                    "evicting stale Whirlpool state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: WhirlpoolPoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::whirlpool",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let extras = self.enrich_extras(pool_pubkey, extras);

        let Some(token_mint_a) = extras
            .token_mint_a
            .as_ref()
            .and_then(|value| parse_pubkey(value))
        else {
            warn!(target = "adapter::whirlpool", pool = %update.pool_id, "missing mint A");
            return;
        };

        let Some(token_mint_b) = extras
            .token_mint_b
            .as_ref()
            .and_then(|value| parse_pubkey(value))
        else {
            warn!(target = "adapter::whirlpool", pool = %update.pool_id, "missing mint B");
            return;
        };

        let state = WhirlpoolState {
            whirlpool: pool_pubkey,
            token_mint_a,
            token_mint_b,
            token_vault_a: extras
                .token_vault_a
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_vault_b: extras
                .token_vault_b
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            tick_spacing: extras.tick_spacing,
            liquidity: extras.liquidity,
            sqrt_price_x64: extras.sqrt_price_x64,
            fee_rate: extras.fee_rate,
            protocol_fee_rate: extras.protocol_fee_rate,
            oracle: extras.oracle.as_ref().and_then(|value| parse_pubkey(value)),
            tick_array0: extras
                .tick_array0
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            tick_array1: extras
                .tick_array1
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            tick_array2: extras
                .tick_array2
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
        };

        trace!(
            target = "adapter::whirlpool",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting Whirlpool pool state"
        );

        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: WhirlpoolPoolExtras) -> WhirlpoolPoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.token_mint_a = extras
                .token_mint_a
                .or_else(|| Some(meta.token_mint_a.to_string()));
            extras.token_mint_b = extras
                .token_mint_b
                .or_else(|| Some(meta.token_mint_b.to_string()));
            extras.token_vault_a = extras
                .token_vault_a
                .or_else(|| Some(meta.token_vault_a.to_string()));
            extras.token_vault_b = extras
                .token_vault_b
                .or_else(|| Some(meta.token_vault_b.to_string()));
            extras.tick_spacing = extras.tick_spacing.or(Some(meta.tick_spacing));
            extras.liquidity = extras.liquidity.or(Some(meta.liquidity));
            extras.sqrt_price_x64 = extras.sqrt_price_x64.or(Some(meta.sqrt_price_x64));
            extras.fee_rate = extras.fee_rate.or(Some(meta.fee_rate));
            extras.protocol_fee_rate = extras.protocol_fee_rate.or(Some(meta.protocol_fee_rate));
            extras.oracle = extras.oracle.or_else(|| Some(meta.oracle.to_string()));
        }

        extras
    }
}

impl ProtocolAdapter for WhirlpoolAdapter {
    fn name(&self) -> &'static str {
        "whirlpool"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::whirlpool",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        let extras = update.extensions.whirlpool.clone().unwrap_or_default();
        self.upsert_state(update, extras);
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let price = state.mid_price()?;
        let amount_out = quote_from_price(&req, &state.token_mint_a, &state.token_mint_b, price)?;
        Some(build_quote_response(req, amount_out))
    }
}

fn needs_hydration(extras: &WhirlpoolPoolExtras) -> bool {
    extras.token_mint_a.is_none()
        || extras.token_mint_b.is_none()
        || extras.token_vault_a.is_none()
        || extras.token_vault_b.is_none()
        || extras.tick_spacing.is_none()
        || extras.sqrt_price_x64.is_none()
        || extras.liquidity.is_none()
        || extras.fee_rate.is_none()
        || extras.protocol_fee_rate.is_none()
        || extras.oracle.is_none()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
    tick_spacing: i32,
    liquidity: u128,
    sqrt_price_x64: u128,
    fee_rate: u16,
    protocol_fee_rate: u16,
    oracle: Pubkey,
}

struct WhirlpoolPoolHydrator {
    program_id: Pubkey,
    clients: Vec<Arc<RpcClient>>,
    cache: DashMap<Pubkey, HydratedPoolMeta>,
}

impl WhirlpoolPoolHydrator {
    fn new(config: &WhirlpoolAdapterConfig, program_id: Pubkey) -> Option<Self> {
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
            program_id,
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
                    if let Some(meta) =
                        parse_whirlpool_account(pool, &self.program_id, &account.data)
                    {
                        self.cache.insert(pool, meta.clone());
                        return Some(meta);
                    } else {
                        warn!(
                            target = "adapter::whirlpool",
                            pool = %pool,
                            "unexpected whirlpool pool account layout"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        target = "adapter::whirlpool",
                        pool = %pool,
                        %error,
                        "failed to fetch whirlpool pool account"
                    );
                }
            }
        }

        None
    }
}

const ANCHOR_DISCRIMINATOR_LEN: usize = 8;
const PUBKEY_BYTES: usize = 32;
const WHIRLPOOL_REWARD_INFO_BYTES: usize = 32 + 32 + 32 + 16 + 16;
const WHIRLPOOL_ACCOUNT_MIN_LEN: usize = ANCHOR_DISCRIMINATOR_LEN
    + PUBKEY_BYTES // whirlpools config
    + 1 // whirlpool bump
    + 2 // tick spacing
    + 2 // fee tier seed prefix
    + 2 // fee rate
    + 2 // protocol fee rate
    + 16 // liquidity
    + 16 // sqrt price
    + 4 // tick current index
    + 8 // protocol fee owed a
    + 8 // protocol fee owed b
    + PUBKEY_BYTES // token mint a
    + PUBKEY_BYTES // token vault a
    + 16 // fee growth global a
    + PUBKEY_BYTES // token mint b
    + PUBKEY_BYTES // token vault b
    + 16 // fee growth global b
    + 8 // reward last updated timestamp
    + (WHIRLPOOL_REWARD_INFO_BYTES * 3);

fn parse_whirlpool_account(
    pool: Pubkey,
    program_id: &Pubkey,
    data: &[u8],
) -> Option<HydratedPoolMeta> {
    if data.len() < WHIRLPOOL_ACCOUNT_MIN_LEN {
        return None;
    }

    let mut cursor = ANCHOR_DISCRIMINATOR_LEN;
    skip_bytes(&mut cursor, PUBKEY_BYTES, data)?; // whirlpools config
    skip_bytes(&mut cursor, 1, data)?; // bump
    let tick_spacing = read_u16(&mut cursor, data)? as i32;
    skip_bytes(&mut cursor, 2, data)?; // fee tier seed prefix
    let fee_rate = read_u16(&mut cursor, data)?;
    let protocol_fee_rate = read_u16(&mut cursor, data)?;
    let liquidity = read_u128(&mut cursor, data)?;
    let sqrt_price_x64 = read_u128(&mut cursor, data)?;
    skip_bytes(&mut cursor, 4, data)?; // tick current index
    skip_bytes(&mut cursor, 8, data)?; // protocol fee owed A
    skip_bytes(&mut cursor, 8, data)?; // protocol fee owed B
    let token_mint_a = read_pubkey_bytes(&mut cursor, data)?;
    let token_vault_a = read_pubkey_bytes(&mut cursor, data)?;
    skip_bytes(&mut cursor, 16, data)?; // fee growth global A
    let token_mint_b = read_pubkey_bytes(&mut cursor, data)?;
    let token_vault_b = read_pubkey_bytes(&mut cursor, data)?;
    skip_bytes(&mut cursor, 16, data)?; // fee growth global B
    skip_bytes(&mut cursor, 8, data)?; // reward last updated timestamp
    skip_bytes(&mut cursor, WHIRLPOOL_REWARD_INFO_BYTES * 3, data)?; // reward infos

    let (oracle, _) = Pubkey::find_program_address(&[b"oracle", pool.as_ref()], program_id);

    Some(HydratedPoolMeta {
        token_mint_a,
        token_mint_b,
        token_vault_a,
        token_vault_b,
        tick_spacing,
        liquidity,
        sqrt_price_x64,
        fee_rate,
        protocol_fee_rate,
        oracle,
    })
}

fn skip_bytes(cursor: &mut usize, len: usize, data: &[u8]) -> Option<()> {
    let end = cursor.checked_add(len)?;
    if end > data.len() {
        return None;
    }
    *cursor = end;
    Some(())
}

fn read_u16(cursor: &mut usize, data: &[u8]) -> Option<u16> {
    let end = cursor.checked_add(2)?;
    let bytes: [u8; 2] = data.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(u16::from_le_bytes(bytes))
}

fn read_u128(cursor: &mut usize, data: &[u8]) -> Option<u128> {
    let end = cursor.checked_add(16)?;
    let bytes: [u8; 16] = data.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(u128::from_le_bytes(bytes))
}

fn read_pubkey_bytes(cursor: &mut usize, data: &[u8]) -> Option<Pubkey> {
    let end = cursor.checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

#[derive(Debug, Clone)]
pub struct WhirlpoolState {
    pub whirlpool: Pubkey,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Option<Pubkey>,
    pub token_vault_b: Option<Pubkey>,
    pub tick_spacing: Option<i32>,
    pub liquidity: Option<u128>,
    pub sqrt_price_x64: Option<u128>,
    pub fee_rate: Option<u16>,
    pub protocol_fee_rate: Option<u16>,
    pub oracle: Option<Pubkey>,
    pub tick_array0: Option<Pubkey>,
    pub tick_array1: Option<Pubkey>,
    pub tick_array2: Option<Pubkey>,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
}

impl WhirlpoolState {
    pub fn mid_price(&self) -> Option<f64> {
        self.sqrt_price_x64.map(|sqrt_price| {
            let price = (sqrt_price as f64 / (1u128 << 64) as f64).powi(2);
            price
        })
    }
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_whirlpool_account_layout() {
        let pool = Pubkey::new_unique();
        let config = Pubkey::new_unique();
        let token_mint_a = Pubkey::new_unique();
        let token_mint_b = Pubkey::new_unique();
        let token_vault_a = Pubkey::new_unique();
        let token_vault_b = Pubkey::new_unique();
        let liquidity = 42_000_000_000_000_000_000u128;
        let sqrt_price = 79228162514264337593543950336u128; // 2^96
        let mut data = vec![0u8; WHIRLPOOL_ACCOUNT_MIN_LEN];

        let mut cursor = 0;
        cursor += ANCHOR_DISCRIMINATOR_LEN;
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(config.as_ref());
        cursor += PUBKEY_BYTES;
        data[cursor] = 3; // bump
        cursor += 1;
        data[cursor..cursor + 2].copy_from_slice(10u16.to_le_bytes().as_ref());
        cursor += 2;
        cursor += 2; // fee tier seed prefix
        data[cursor..cursor + 2].copy_from_slice(300u16.to_le_bytes().as_ref());
        cursor += 2;
        data[cursor..cursor + 2].copy_from_slice(25u16.to_le_bytes().as_ref());
        cursor += 2;
        data[cursor..cursor + 16].copy_from_slice(liquidity.to_le_bytes().as_ref());
        cursor += 16;
        data[cursor..cursor + 16].copy_from_slice(sqrt_price.to_le_bytes().as_ref());
        cursor += 16;
        cursor += 4; // tick current index
        cursor += 8; // protocol fee owed A
        cursor += 8; // protocol fee owed B
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(token_mint_a.as_ref());
        cursor += PUBKEY_BYTES;
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(token_vault_a.as_ref());
        cursor += PUBKEY_BYTES;
        cursor += 16; // fee growth global A
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(token_mint_b.as_ref());
        cursor += PUBKEY_BYTES;
        data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(token_vault_b.as_ref());
        cursor += PUBKEY_BYTES;
        cursor += 16; // fee growth global B
        cursor += 8; // reward last updated timestamp
        cursor += WHIRLPOOL_REWARD_INFO_BYTES * 3; // reward infos
        assert_eq!(cursor, WHIRLPOOL_ACCOUNT_MIN_LEN);

        let program_id = Pubkey::from_str(WHIRLPOOL_PROGRAM_ID).unwrap();
        let meta = parse_whirlpool_account(pool, &program_id, &data).expect("should parse");

        assert_eq!(meta.token_mint_a, token_mint_a);
        assert_eq!(meta.token_mint_b, token_mint_b);
        assert_eq!(meta.token_vault_a, token_vault_a);
        assert_eq!(meta.token_vault_b, token_vault_b);
        assert_eq!(meta.tick_spacing, 10);
        assert_eq!(meta.liquidity, liquidity);
        assert_eq!(meta.sqrt_price_x64, sqrt_price);
        assert_eq!(meta.fee_rate, 300);
        assert_eq!(meta.protocol_fee_rate, 25);
        let (expected_oracle, _) =
            Pubkey::find_program_address(&[b"oracle", pool.as_ref()], &program_id);
        assert_eq!(meta.oracle, expected_oracle);
    }
}
