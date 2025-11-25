use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, trace, warn};

use crate::stages::types::{PoolUpdate, RaydiumV3PoolExtras};

use super::{
    build_quote_response, quote_from_reserves, ProtocolAdapter, QuoteRequest, QuoteResponse,
    SwapDirection,
};

const RAYDIUM_V3_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";

#[derive(Debug, Clone)]
pub struct RaydiumV3AdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
}

impl Default for RaydiumV3AdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 1_000,
        }
    }
}

pub struct RaydiumV3Adapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, RaydiumV3State>,
    config: RaydiumV3AdapterConfig,
    hydrator: Option<RaydiumPoolHydrator>,
}

impl RaydiumV3Adapter {
    pub fn new(config: RaydiumV3AdapterConfig) -> Self {
        let program_id = Pubkey::from_str(RAYDIUM_V3_PROGRAM_ID)
            .expect("static Raydium AMM v3 program id should be valid");
        let hydrator = RaydiumPoolHydrator::new(&config);
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

        let extras = self.enrich_extras(pool_pubkey, extras);
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
        } = extras;

        let state = RaydiumV3State {
            pool_id: pool_pubkey,
            base_mint,
            quote_mint,
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
        };

        trace!(
            target = "adapter::raydium_v3",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting Raydium pool state"
        );

        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: RaydiumV3PoolExtras) -> RaydiumV3PoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.tick_spacing = extras.tick_spacing.or(Some(meta.tick_spacing));
            extras.sqrt_price_x64 = extras.sqrt_price_x64.or(Some(meta.sqrt_price_x64));
            extras.liquidity = extras.liquidity.or(Some(meta.liquidity));
            extras.current_tick = extras.current_tick.or(Some(meta.current_tick));
            extras.amm_config = extras.amm_config.or(Some(meta.amm_config));
            extras.observation_state = extras.observation_state.or(Some(meta.observation_state));
            extras.pool_bump = extras.pool_bump.or(Some(meta.pool_bump));
        }

        extras
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

        let extras = update.extensions.raydium_v3.clone().unwrap_or_default();

        self.upsert_state(update, extras);
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let amount_out = quote_from_reserves(
            &req,
            &state.base_mint,
            &state.quote_mint,
            state.vault_base,
            state.vault_quote,
        )?;
        Some(build_quote_response(req, amount_out))
    }
}

#[derive(Debug, Clone)]
pub struct RaydiumV3State {
    pub pool_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
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
}

impl RaydiumV3State {
    pub fn mid_price(&self) -> Option<f64> {
        if self.vault_base == 0 {
            return None;
        }
        Some(self.vault_quote as f64 / self.vault_base as f64)
    }
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

fn needs_hydration(extras: &RaydiumV3PoolExtras) -> bool {
    extras.tick_spacing.is_none()
        || extras.sqrt_price_x64.is_none()
        || extras.liquidity.is_none()
        || extras.current_tick.is_none()
        || extras.amm_config.is_none()
        || extras.observation_state.is_none()
        || extras.pool_bump.is_none()
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
}

struct RaydiumPoolHydrator {
    clients: Vec<Arc<RpcClient>>,
    cache: DashMap<Pubkey, HydratedPoolMeta>,
}

impl RaydiumPoolHydrator {
    fn new(config: &RaydiumV3AdapterConfig) -> Option<Self> {
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
        if let Some(meta) = self.cache.get(&pool) {
            return Some(meta.clone());
        }

        self.fetch_from_rpc(pool)
    }

    fn fetch_from_rpc(&self, pool: Pubkey) -> Option<HydratedPoolMeta> {
        for client in &self.clients {
            match client.get_account(&pool) {
                Ok(account) => {
                    if let Some(meta) = parse_pool_account(&account.data) {
                        self.cache.insert(pool, meta.clone());
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
}

const MIN_RAYDIUM_POOL_ACCOUNT_LEN: usize = 269;
const PUBKEY_BYTES: usize = 32;

fn parse_pool_account(data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < MIN_RAYDIUM_POOL_ACCOUNT_LEN {
        return None;
    }

    let mut cursor = 0usize;
    let pool_bump = *data.get(cursor)?;
    cursor += 1;

    let amm_config = read_pubkey(data, &mut cursor)?;
    skip_pubkey(data, &mut cursor)?; // owner
    skip_pubkey(data, &mut cursor)?; // token_mint_0
    skip_pubkey(data, &mut cursor)?; // token_mint_1
    skip_pubkey(data, &mut cursor)?; // token_vault_0
    skip_pubkey(data, &mut cursor)?; // token_vault_1
    let observation_state = read_pubkey(data, &mut cursor)?;

    cursor = cursor.checked_add(2)?; // mint_decimals_0 + mint_decimals_1
    if cursor > data.len() {
        return None;
    }

    let tick_spacing = read_u16(data, &mut cursor)? as i32;
    let liquidity = read_u128(data, &mut cursor)?;
    let sqrt_price_x64 = read_u128(data, &mut cursor)?;
    let current_tick = read_i32(data, &mut cursor)?;

    Some(HydratedPoolMeta {
        amm_config,
        observation_state,
        tick_spacing,
        sqrt_price_x64,
        liquidity,
        current_tick,
        pool_bump,
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

fn read_u128(data: &[u8], cursor: &mut usize) -> Option<u128> {
    let end = cursor.checked_add(16)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(u128::from_le_bytes(bytes))
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Option<i32> {
    let end = cursor.checked_add(4)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(i32::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_pool_account_view() {
        let mut data = vec![0u8; MIN_RAYDIUM_POOL_ACCOUNT_LEN];
        data[0] = 5;

        let mut cursor = 1;
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // amm_config
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // owner
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_mint_0
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_mint_1
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_vault_0
        cursor = write_pubkey(&mut data, cursor, &Pubkey::new_unique()); // token_vault_1
        let observation = Pubkey::new_unique();
        cursor = write_pubkey(&mut data, cursor, &observation); // observation_key

        data[cursor] = 6;
        data[cursor + 1] = 9;
        cursor += 2;

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

        let meta = parse_pool_account(&data).expect("meta should parse");
        assert_eq!(meta.pool_bump, 5);
        assert_eq!(meta.tick_spacing, tick_spacing as i32);
        assert_eq!(meta.liquidity, liquidity);
        assert_eq!(meta.sqrt_price_x64, sqrt_price_x64);
        assert_eq!(meta.current_tick, current_tick);
        assert_eq!(meta.observation_state, observation);
    }

    fn write_pubkey(buffer: &mut [u8], cursor: usize, key: &Pubkey) -> usize {
        let end = cursor + PUBKEY_BYTES;
        buffer[cursor..end].copy_from_slice(key.as_ref());
        end
    }
}
