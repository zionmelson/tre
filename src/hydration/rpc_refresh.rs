use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use solana_client::rpc_client::RpcClient;
use solana_client::rpc_response::UiTokenAmount;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, warn};

use crate::config::AppConfig;
use crate::hydration::types::{
    CpAmmPoolExtras, LbClmmPoolExtras, PoolSnapshot, SnapshotFreshness, WhirlpoolPoolExtras,
    CP_AMM_PROGRAM_ID, LB_CLMM_PROGRAM_ID, RAYDIUM_V3_PROGRAM_ID, WHIRLPOOL_PROGRAM_ID,
};

const DEFAULT_RPC_TIMEOUT_MS: u64 = 5_000;
const PROGRAM_PROTOCOLS: [(&str, &str); 4] = [
    (RAYDIUM_V3_PROGRAM_ID, "raydium_v3"),
    (CP_AMM_PROGRAM_ID, "cp_amm"),
    (LB_CLMM_PROGRAM_ID, "lb_clmm"),
    (WHIRLPOOL_PROGRAM_ID, "whirlpool"),
];

#[derive(Clone, Copy)]
enum ProtocolKind {
    RaydiumV3,
    CpAmm,
    LbClmm,
    Whirlpool,
}

pub struct RpcRefresher {
    client_pools: HashMap<&'static str, Arc<RpcClientPool>>,
}

impl RpcRefresher {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let mut protocol_clients: HashMap<&'static str, Arc<RpcClientPool>> = HashMap::new();
        for (_, protocol_key) in &PROGRAM_PROTOCOLS {
            if protocol_clients.contains_key(protocol_key) {
                continue;
            }
            let endpoints = config.protocol_rpc_endpoints(protocol_key);
            if let Some(pool) = RpcClientPool::new(&endpoints) {
                protocol_clients.insert(protocol_key, Arc::new(pool));
            }
        }

        let mut client_pools = HashMap::new();
        for (program_id, protocol_key) in &PROGRAM_PROTOCOLS {
            if let Some(pool) = protocol_clients.get(protocol_key) {
                client_pools.insert(*program_id, pool.clone());
            }
        }

        Self { client_pools }
    }

    pub fn refresh(&self, snapshot: &PoolSnapshot) -> Option<PoolSnapshot> {
        let protocol = detect_protocol(snapshot.program_id.as_str())?;
        let pool = self
            .client_pools
            .get(snapshot.program_id.as_str())?
            .as_ref();
        match protocol {
            ProtocolKind::RaydiumV3 => refresh_raydium(snapshot, pool),
            ProtocolKind::CpAmm => refresh_cp_amm(snapshot, pool),
            ProtocolKind::LbClmm => refresh_lb_clmm(snapshot, pool),
            ProtocolKind::Whirlpool => refresh_whirlpool(snapshot, pool),
        }
    }
}

fn detect_protocol(program_id: &str) -> Option<ProtocolKind> {
    match program_id {
        RAYDIUM_V3_PROGRAM_ID => Some(ProtocolKind::RaydiumV3),
        CP_AMM_PROGRAM_ID => Some(ProtocolKind::CpAmm),
        LB_CLMM_PROGRAM_ID => Some(ProtocolKind::LbClmm),
        WHIRLPOOL_PROGRAM_ID => Some(ProtocolKind::Whirlpool),
        _ => None,
    }
}

struct RpcClientPool {
    clients: Vec<Arc<RpcClient>>,
}

impl RpcClientPool {
    fn new(endpoints: &[String]) -> Option<Self> {
        if endpoints.is_empty() {
            return None;
        }
        let timeout = Duration::from_millis(DEFAULT_RPC_TIMEOUT_MS);
        let clients: Vec<Arc<RpcClient>> = endpoints
            .iter()
            .map(|url| Arc::new(RpcClient::new_with_timeout(url.clone(), timeout)))
            .collect();
        if clients.is_empty() {
            None
        } else {
            Some(Self { clients })
        }
    }

    fn clients(&self) -> &[Arc<RpcClient>] {
        &self.clients
    }
}

fn refresh_raydium(snapshot: &PoolSnapshot, pool: &RpcClientPool) -> Option<PoolSnapshot> {
    let pool_pubkey = parse_pubkey(&snapshot.pool_id)?;
    let base_mint = parse_pubkey(&snapshot.base_mint)?;
    let quote_mint = parse_pubkey(&snapshot.quote_mint)?;
    let program_id = Pubkey::from_str(RAYDIUM_V3_PROGRAM_ID).ok()?;

    let (mint0, mint1) = order_mints(&base_mint, &quote_mint);
    let vault0 = derive_raydium_vault(&program_id, &pool_pubkey, &mint0);
    let vault1 = derive_raydium_vault(&program_id, &pool_pubkey, &mint1);
    let amt0 = fetch_token_amount(pool.clients(), &vault0)?;
    let amt1 = fetch_token_amount(pool.clients(), &vault1)?;

    let (base_amount, quote_amount) = if base_mint == mint0 {
        (amt0, amt1)
    } else {
        (amt1, amt0)
    };

    build_snapshot(snapshot, base_amount, quote_amount)
}

fn refresh_cp_amm(snapshot: &PoolSnapshot, pool: &RpcClientPool) -> Option<PoolSnapshot> {
    let extras: &CpAmmPoolExtras = snapshot.extensions.cp_amm.as_ref()?;
    let base_vault = parse_pubkey(extras.token_a_vault.as_deref()?)?;
    let quote_vault = parse_pubkey(extras.token_b_vault.as_deref()?)?;
    let base_amount = fetch_token_amount(pool.clients(), &base_vault)?;
    let quote_amount = fetch_token_amount(pool.clients(), &quote_vault)?;
    build_snapshot(snapshot, base_amount, quote_amount)
}

fn refresh_lb_clmm(snapshot: &PoolSnapshot, pool: &RpcClientPool) -> Option<PoolSnapshot> {
    let extras: &LbClmmPoolExtras = snapshot.extensions.lb_clmm.as_ref()?;
    let reserve_x = parse_pubkey(extras.reserve_x.as_deref()?)?;
    let reserve_y = parse_pubkey(extras.reserve_y.as_deref()?)?;
    let token_x_mint = parse_pubkey(extras.token_x_mint.as_deref()?)?;
    let token_y_mint = parse_pubkey(extras.token_y_mint.as_deref()?)?;
    let amount_x = fetch_token_amount(pool.clients(), &reserve_x)?;
    let amount_y = fetch_token_amount(pool.clients(), &reserve_y)?;

    let base_amount;
    let quote_amount;
    if snapshot.base_mint == token_x_mint.to_string() {
        base_amount = amount_x;
        quote_amount = amount_y;
    } else if snapshot.base_mint == token_y_mint.to_string() {
        base_amount = amount_y;
        quote_amount = amount_x;
    } else {
        debug!(
            target = "hydration",
            pool = snapshot.pool_id,
            "lb_clmm refresh could not align mints"
        );
        return None;
    }

    build_snapshot(snapshot, base_amount, quote_amount)
}

fn refresh_whirlpool(snapshot: &PoolSnapshot, pool: &RpcClientPool) -> Option<PoolSnapshot> {
    let extras: &WhirlpoolPoolExtras = snapshot.extensions.whirlpool.as_ref()?;
    let vault_a = parse_pubkey(extras.token_vault_a.as_deref()?)?;
    let vault_b = parse_pubkey(extras.token_vault_b.as_deref()?)?;
    let mint_a = parse_pubkey(extras.token_mint_a.as_deref()?)?;
    let mint_b = parse_pubkey(extras.token_mint_b.as_deref()?)?;
    let amount_a = fetch_token_amount(pool.clients(), &vault_a)?;
    let amount_b = fetch_token_amount(pool.clients(), &vault_b)?;

    let base_amount;
    let quote_amount;
    if snapshot.base_mint == mint_a.to_string() {
        base_amount = amount_a;
        quote_amount = amount_b;
    } else if snapshot.base_mint == mint_b.to_string() {
        base_amount = amount_b;
        quote_amount = amount_a;
    } else {
        debug!(
            target = "hydration",
            pool = snapshot.pool_id,
            "whirlpool refresh could not align mints"
        );
        return None;
    }

    build_snapshot(snapshot, base_amount, quote_amount)
}

fn build_snapshot(
    snapshot: &PoolSnapshot,
    base_amount: u128,
    quote_amount: u128,
) -> Option<PoolSnapshot> {
    let mut refreshed = snapshot.clone();
    refreshed.base_post_amount = base_amount;
    refreshed.quote_post_amount = quote_amount;
    refreshed.updated_at = Instant::now();
    refreshed.freshness = SnapshotFreshness::RpcRefresh;
    refreshed.quotes = None;
    Some(refreshed)
}

fn fetch_token_amount(clients: &[Arc<RpcClient>], address: &Pubkey) -> Option<u128> {
    for client in clients {
        match client.get_token_account_balance(address) {
            Ok(UiTokenAmount { amount, .. }) => {
                if let Ok(value) = amount.parse::<u128>() {
                    return Some(value);
                }
            }
            Err(error) => {
                warn!(
                    target = "hydration",
                    account = %address,
                    %error,
                    "rpc refresh failed to fetch token balance"
                );
            }
        }
    }
    None
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

fn order_mints(a: &Pubkey, b: &Pubkey) -> (Pubkey, Pubkey) {
    if a.to_bytes() <= b.to_bytes() {
        (*a, *b)
    } else {
        (*b, *a)
    }
}

fn derive_raydium_vault(program: &Pubkey, pool: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"pool_vault", pool.as_ref(), mint.as_ref()], program).0
}
