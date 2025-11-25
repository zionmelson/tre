use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, trace, warn};

use crate::stages::types::{CpAmmPoolExtras, PoolUpdate};

use super::{
    build_quote_response, quote_from_reserves, ProtocolAdapter, QuoteRequest, QuoteResponse,
    SwapDirection,
};

const CP_AMM_PROGRAM_ID: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";

#[derive(Debug, Clone)]
pub struct CpAmmAdapterConfig {
    pub stale_slot_threshold: u64,
    pub stale_after_ms: u64,
    pub rpc_endpoints: Vec<String>,
    pub rpc_timeout_ms: u64,
}

impl Default for CpAmmAdapterConfig {
    fn default() -> Self {
        Self {
            stale_slot_threshold: 400,
            stale_after_ms: 2_000,
            rpc_endpoints: Vec::new(),
            rpc_timeout_ms: 1_000,
        }
    }
}

pub struct CpAmmAdapter {
    program_id: Pubkey,
    state: DashMap<Pubkey, CpAmmState>,
    config: CpAmmAdapterConfig,
    hydrator: Option<CpAmmPoolHydrator>,
}

impl CpAmmAdapter {
    pub fn new(config: CpAmmAdapterConfig) -> Self {
        let program_id =
            Pubkey::from_str(CP_AMM_PROGRAM_ID).expect("static cp_amm program id should be valid");
        let hydrator = CpAmmPoolHydrator::new(&config);
        if let Some(hydrator) = hydrator.as_ref() {
            info!(
                target = "adapter::cp_amm",
                endpoints = hydrator.client_count(),
                "cp_amm pool hydrator online"
            );
        } else {
            debug!(
                target = "adapter::cp_amm",
                "cp_amm pool hydrator disabled (no rpc endpoints)"
            );
        }
        Self {
            program_id,
            state: DashMap::new(),
            config,
            hydrator,
        }
    }

    pub fn get_state(&self, pool: &Pubkey) -> Option<CpAmmState> {
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
                    target = "adapter::cp_amm",
                    pool = %state.pool,
                    last_slot = state.last_update_slot,
                    "evicting stale pool state"
                );
            }
            slot_fresh && instant_fresh
        });
    }

    fn upsert_state(&self, update: &PoolUpdate, extras: CpAmmPoolExtras) {
        let Some(pool_pubkey) = parse_pubkey(&update.pool_id) else {
            warn!(
                target = "adapter::cp_amm",
                pool = %update.pool_id,
                "dropping pool update with invalid pool pubkey"
            );
            return;
        };

        let extras = self.enrich_extras(pool_pubkey, extras);

        let Some(base_mint) = extras
            .token_a_mint
            .as_deref()
            .and_then(parse_pubkey)
            .or_else(|| parse_pubkey(&update.base_mint))
        else {
            warn!(target = "adapter::cp_amm", pool = %update.pool_id, "invalid base mint");
            return;
        };

        let Some(quote_mint) = extras
            .token_b_mint
            .as_deref()
            .and_then(parse_pubkey)
            .or_else(|| parse_pubkey(&update.quote_mint))
        else {
            warn!(target = "adapter::cp_amm", pool = %update.pool_id, "invalid quote mint");
            return;
        };

        let state = CpAmmState {
            pool: pool_pubkey,
            token_a_mint: base_mint,
            token_b_mint: quote_mint,
            token_a_vault: extras
                .token_a_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_b_vault: extras
                .token_b_vault
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            pool_authority: extras
                .pool_authority
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_a_program: extras
                .token_a_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            token_b_program: extras
                .token_b_program
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            amp: extras.amp,
            lp_fee_bps: extras.lp_fee_bps,
            protocol_fee_bps: extras.protocol_fee_bps,
            referral_bps: extras.referral_bps,
            referral_token_account: extras
                .referral_token_account
                .as_ref()
                .and_then(|value| parse_pubkey(value)),
            vault_a_balance: update.base_post_amount,
            vault_b_balance: update.quote_post_amount,
            last_update_slot: update.slot,
            last_update_at: Instant::now(),
        };

        trace!(
            target = "adapter::cp_amm",
            pool = %update.pool_id,
            slot = update.slot,
            base = %update.base_post_amount,
            quote = %update.quote_post_amount,
            "upserting cp_amm pool state"
        );

        self.state.insert(pool_pubkey, state);
        self.evict_stale(update.slot);
    }

    fn enrich_extras(&self, pool: Pubkey, mut extras: CpAmmPoolExtras) -> CpAmmPoolExtras {
        if !needs_hydration(&extras) {
            return extras;
        }

        let Some(hydrator) = self.hydrator.as_ref() else {
            return extras;
        };

        if let Some(meta) = hydrator.hydrate(pool) {
            extras.token_a_mint = extras
                .token_a_mint
                .or_else(|| Some(meta.token_a_mint.to_string()));
            extras.token_b_mint = extras
                .token_b_mint
                .or_else(|| Some(meta.token_b_mint.to_string()));
            extras.token_a_vault = extras
                .token_a_vault
                .or_else(|| Some(meta.token_a_vault.to_string()));
            extras.token_b_vault = extras
                .token_b_vault
                .or_else(|| Some(meta.token_b_vault.to_string()));
        }

        extras
    }
}

impl ProtocolAdapter for CpAmmAdapter {
    fn name(&self) -> &'static str {
        "cp_amm"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    fn ingest_pool_update(&self, update: &PoolUpdate) {
        let Some(program_id) = parse_pubkey(&update.program_id) else {
            debug!(
                target = "adapter::cp_amm",
                "invalid program id on pool update"
            );
            return;
        };

        if program_id != self.program_id {
            return;
        }

        let extras = update.extensions.cp_amm.clone().unwrap_or_default();
        self.upsert_state(update, extras);
    }

    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse> {
        if req.direction != SwapDirection::ExactIn {
            return None;
        }
        let state = self.get_state(&req.pool_id)?;
        let amount_out = quote_from_reserves(
            &req,
            &state.token_a_mint,
            &state.token_b_mint,
            state.vault_a_balance,
            state.vault_b_balance,
        )?;
        Some(build_quote_response(req, amount_out))
    }
}

fn needs_hydration(extras: &CpAmmPoolExtras) -> bool {
    extras.token_a_mint.is_none()
        || extras.token_b_mint.is_none()
        || extras.token_a_vault.is_none()
        || extras.token_b_vault.is_none()
}

#[derive(Clone, Debug)]
struct HydratedPoolMeta {
    token_a_mint: Pubkey,
    token_b_mint: Pubkey,
    token_a_vault: Pubkey,
    token_b_vault: Pubkey,
}

struct CpAmmPoolHydrator {
    clients: Vec<Arc<RpcClient>>,
    cache: DashMap<Pubkey, HydratedPoolMeta>,
}

impl CpAmmPoolHydrator {
    fn new(config: &CpAmmAdapterConfig) -> Option<Self> {
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
                    if let Some(meta) = parse_pool_account(&account.data) {
                        self.cache.insert(pool, meta.clone());
                        return Some(meta);
                    } else {
                        warn!(
                            target = "adapter::cp_amm",
                            pool = %pool,
                            "unexpected cp_amm pool account layout"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        target = "adapter::cp_amm",
                        pool = %pool,
                        %error,
                        "failed to fetch cp_amm pool account"
                    );
                }
            }
        }

        None
    }
}

const POOL_FEES_STRUCT_LEN: usize = 160;
const PUBKEY_BYTES: usize = 32;
const CP_AMM_POOL_ACCOUNT_HEADER_LEN: usize = 8 + POOL_FEES_STRUCT_LEN;
const CP_AMM_POOL_ACCOUNT_MIN_LEN: usize = CP_AMM_POOL_ACCOUNT_HEADER_LEN + PUBKEY_BYTES * 4;

fn parse_pool_account(data: &[u8]) -> Option<HydratedPoolMeta> {
    if data.len() < CP_AMM_POOL_ACCOUNT_MIN_LEN {
        return None;
    }

    let mut cursor = CP_AMM_POOL_ACCOUNT_HEADER_LEN;
    let token_a_mint = read_pubkey_from_slice(data, &mut cursor)?;
    let token_b_mint = read_pubkey_from_slice(data, &mut cursor)?;
    let token_a_vault = read_pubkey_from_slice(data, &mut cursor)?;
    let token_b_vault = read_pubkey_from_slice(data, &mut cursor)?;

    Some(HydratedPoolMeta {
        token_a_mint,
        token_b_mint,
        token_a_vault,
        token_b_vault,
    })
}

fn read_pubkey_from_slice(data: &[u8], cursor: &mut usize) -> Option<Pubkey> {
    let end = cursor.checked_add(PUBKEY_BYTES)?;
    let slice = data.get(*cursor..end)?;
    let mut bytes = [0u8; PUBKEY_BYTES];
    bytes.copy_from_slice(slice);
    *cursor = end;
    Some(Pubkey::new_from_array(bytes))
}

#[derive(Debug, Clone)]
pub struct CpAmmState {
    pub pool: Pubkey,
    pub pool_authority: Option<Pubkey>,
    pub token_a_vault: Option<Pubkey>,
    pub token_b_vault: Option<Pubkey>,
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    pub token_a_program: Option<Pubkey>,
    pub token_b_program: Option<Pubkey>,
    pub amp: Option<u64>,
    pub lp_fee_bps: Option<u16>,
    pub protocol_fee_bps: Option<u16>,
    pub referral_bps: Option<u16>,
    pub referral_token_account: Option<Pubkey>,
    pub vault_a_balance: u128,
    pub vault_b_balance: u128,
    pub last_update_slot: u64,
    pub last_update_at: Instant,
}

impl CpAmmState {
    pub fn mid_price(&self) -> Option<f64> {
        if self.vault_a_balance == 0 {
            return None;
        }
        Some(self.vault_b_balance as f64 / self.vault_a_balance as f64)
    }
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pool_account_prefix() {
        let token_a_mint = Pubkey::new_unique();
        let token_b_mint = Pubkey::new_unique();
        let token_a_vault = Pubkey::new_unique();
        let token_b_vault = Pubkey::new_unique();

        let mut data = vec![0u8; CP_AMM_POOL_ACCOUNT_MIN_LEN];
        let mut cursor = CP_AMM_POOL_ACCOUNT_HEADER_LEN;
        for key in [token_a_mint, token_b_mint, token_a_vault, token_b_vault] {
            data[cursor..cursor + PUBKEY_BYTES].copy_from_slice(key.as_ref());
            cursor += PUBKEY_BYTES;
        }

        let meta = parse_pool_account(&data).expect("pool account should hydrate");
        assert_eq!(meta.token_a_mint, token_a_mint);
        assert_eq!(meta.token_b_mint, token_b_mint);
        assert_eq!(meta.token_a_vault, token_a_vault);
        assert_eq!(meta.token_b_vault, token_b_vault);
    }
}
