use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use reqwest::blocking::Client;
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, warn};

use crate::{bundle::BundleRequest, config::AppConfig, corecast::Opportunity, idl::IdlCache};

use super::{ProtocolObservation, ProtocolStrategy};

const DEFAULT_RPC_ENDPOINT: &str = "https://api.mainnet-beta.solana.com";
const CACHE_TTL_SLOTS: u64 = 8;
const MIN_LIQUIDITY_THRESHOLD: f64 = 10_000.0;
const Q64_FACTOR: f64 = 18_446_744_073_709_551_616.0; // 2^64

pub struct AmmV3Strategy {
    rpc: Arc<RpcAccountFetcher>,
    cache: Mutex<HashMap<String, CachedSnapshot>>,
    idl_version: String,
}

impl AmmV3Strategy {
    pub fn new(config: Arc<AppConfig>, idl_cache: Arc<IdlCache>) -> Result<Self> {
        let rpc = Arc::new(RpcAccountFetcher::new(&config.quicknode_rpcs)?);
        let idl = idl_cache
            .get("amm_v3")
            .ok_or_else(|| anyhow!("amm_v3 IDL missing from cache"))?;
        let version = idl
            .raw
            .get("metadata")
            .and_then(|meta| meta.get("version"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        info!(target = "strategy::amm_v3", idl_version = %version, "amm_v3 strategy online");

        Ok(Self {
            rpc,
            cache: Mutex::new(HashMap::new()),
            idl_version: version,
        })
    }

    fn load_snapshot(&self, pool: &str, slot_hint: u64) -> Result<AmmV3PoolSnapshot> {
        if let Some(snapshot) = self.snapshot_from_cache(pool, slot_hint) {
            return Ok(snapshot);
        }

        let account = self.rpc.get_account_data(pool)?;
        let mut snapshot = decode_pool_state(&account.data)?;
        snapshot.slot = account.slot;

        let mut guard = self.cache.lock().expect("amm_v3 cache poisoned");
        guard.insert(
            pool.to_string(),
            CachedSnapshot {
                slot: snapshot.slot,
                snapshot: snapshot.clone(),
            },
        );

        Ok(snapshot)
    }

    fn snapshot_from_cache(&self, pool: &str, slot_hint: u64) -> Option<AmmV3PoolSnapshot> {
        let guard = self.cache.lock().ok()?;
        guard.get(pool).and_then(|entry| {
            if entry.slot + CACHE_TTL_SLOTS >= slot_hint {
                Some(entry.snapshot.clone())
            } else {
                None
            }
        })
    }

    fn observation_from_snapshot(
        &self,
        snapshot: &AmmV3PoolSnapshot,
        opportunity: &Opportunity,
    ) -> Option<ProtocolObservation> {
        let price = snapshot.price_for_pair(&opportunity.base_mint, &opportunity.quote_mint)?;
        Some(ProtocolObservation {
            slot: snapshot.slot,
            price,
            liquidity: snapshot.liquidity as f64,
        })
    }
}

impl ProtocolStrategy for AmmV3Strategy {
    fn slug(&self) -> &'static str {
        "amm_v3"
    }

    fn observe(&self, opportunity: &Opportunity) -> Option<ProtocolObservation> {
        let pool = opportunity.pool_address.as_deref()?;
        match self.load_snapshot(pool, opportunity.slot) {
            Ok(snapshot) => self.observation_from_snapshot(&snapshot, opportunity),
            Err(err) => {
                warn!(
                    target = "strategy::amm_v3",
                    pool = %pool,
                    error = %err,
                    "failed to hydrate amm_v3 pool"
                );
                None
            }
        }
    }

    fn evaluate(
        &self,
        opportunity: &Opportunity,
        seq: u64,
        observation: Option<&ProtocolObservation>,
    ) -> Option<BundleRequest> {
        if opportunity.protocol.slug() != self.slug() {
            return None;
        }

        let flow_delta = opportunity.buy_amount as i128 - opportunity.sell_amount as i128;
        if flow_delta.abs() < 10 {
            return None;
        }

        let mut fallback = None;
        let obs = observation.or_else(|| {
            fallback = self.observe(opportunity);
            fallback.as_ref()
        })?;

        if obs.liquidity < MIN_LIQUIDITY_THRESHOLD {
            return None;
        }

        debug!(
            target = "strategy::amm_v3",
            signature = %opportunity.signature,
            pool = %opportunity.pool_address.as_deref().unwrap_or("unknown"),
            base = %opportunity.base_mint,
            quote = %opportunity.quote_mint,
            price = obs.price,
            liquidity = obs.liquidity,
            flow = flow_delta,
            idl_version = %self.idl_version,
            "amm_v3 opportunity satisfied"
        );

        Some(BundleRequest {
            uuid: format!("amm-v3-{seq:08x}-{}", opportunity.signature),
        })
    }
}

struct RpcAccountFetcher {
    client: Client,
    endpoints: Vec<String>,
    cursor: AtomicUsize,
}

impl RpcAccountFetcher {
    fn new(urls: &[String]) -> Result<Self> {
        let endpoints = if urls.is_empty() {
            vec![DEFAULT_RPC_ENDPOINT.to_string()]
        } else {
            urls.iter().cloned().collect()
        };

        if endpoints.len() == 1 && endpoints[0] == DEFAULT_RPC_ENDPOINT {
            warn!(
                target = "strategy::amm_v3",
                "quicknode_rpcs empty; falling back to public RPC"
            );
        }

        let client = Client::builder()
            .timeout(Duration::from_millis(750))
            .build()
            .context("building reqwest client")?;

        Ok(Self {
            client,
            endpoints,
            cursor: AtomicUsize::new(0),
        })
    }

    fn get_account_data(&self, address: &str) -> Result<RpcAccountData> {
        let total = self.endpoints.len();
        let mut last_err = None;
        for attempt in 0..total {
            let idx = (self.cursor.fetch_add(1, Ordering::Relaxed) + attempt) % total;
            let endpoint = &self.endpoints[idx];
            match self.fetch_once(endpoint, address) {
                Ok(data) => return Ok(data),
                Err(err) => {
                    last_err = Some(err);
                    continue;
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("all RPC endpoints failed")))
    }

    fn fetch_once(&self, endpoint: &str, address: &str) -> Result<RpcAccountData> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                address,
                {
                    "encoding": "base64",
                    "commitment": "confirmed"
                }
            ]
        });
        let response = self
            .client
            .post(endpoint)
            .json(&request)
            .send()
            .with_context(|| format!("posting getAccountInfo to {endpoint}"))?;
        let parsed: RpcResponse<RpcAccountEnvelope> =
            response.json().context("parsing getAccountInfo response")?;

        if let Some(error) = parsed.error {
            return Err(anyhow!("rpc error {}: {}", error.code, error.message));
        }

        let envelope = parsed.result.ok_or_else(|| anyhow!("missing result"))?;
        let value = envelope
            .value
            .ok_or_else(|| anyhow!("account missing value"))?;
        let slot = envelope.context.map(|c| c.slot).unwrap_or(0);
        let (data_b64, _encoding) = value.data;
        let data = BASE64
            .decode(data_b64.as_bytes())
            .context("decoding base64 account data")?;

        Ok(RpcAccountData { slot, data })
    }
}

#[derive(Clone)]
struct RpcAccountData {
    slot: u64,
    data: Vec<u8>,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    #[allow(dead_code)]
    jsonrpc: String,
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct RpcAccountEnvelope {
    context: Option<RpcContext>,
    value: Option<RpcAccountValue>,
}

#[derive(Deserialize)]
struct RpcContext {
    slot: u64,
}

#[derive(Deserialize)]
struct RpcAccountValue {
    data: (String, String),
}

#[derive(Clone)]
struct CachedSnapshot {
    slot: u64,
    snapshot: AmmV3PoolSnapshot,
}

#[derive(Clone, Debug)]
struct AmmV3PoolSnapshot {
    slot: u64,
    token_mint_0: String,
    token_mint_1: String,
    mint_decimals_0: u8,
    mint_decimals_1: u8,
    liquidity: u128,
    sqrt_price_x64: u128,
}

impl AmmV3PoolSnapshot {
    fn price_for_pair(&self, base: &str, quote: &str) -> Option<f64> {
        let p_1_over_0 = self.price_token1_per_token0()?;
        if base == self.token_mint_0 && quote == self.token_mint_1 {
            Some(p_1_over_0)
        } else if base == self.token_mint_1 && quote == self.token_mint_0 {
            Some(1.0 / p_1_over_0)
        } else {
            None
        }
    }

    fn price_token1_per_token0(&self) -> Option<f64> {
        let sqrt_price = self.sqrt_price_x64 as f64 / Q64_FACTOR;
        let price = sqrt_price * sqrt_price;
        if price.is_finite() && price > 0.0 {
            Some(price)
        } else {
            None
        }
    }
}

fn decode_pool_state(data: &[u8]) -> Result<AmmV3PoolSnapshot> {
    let mut cursor = PoolCursor::new(data);
    cursor.skip(1)?; // bump
    cursor.skip_pubkey()?; // amm_config
    cursor.skip_pubkey()?; // owner
    let token_mint_0 = cursor.read_pubkey()?;
    let token_mint_1 = cursor.read_pubkey()?;
    cursor.skip_pubkey()?; // token_vault_0
    cursor.skip_pubkey()?; // token_vault_1
    cursor.skip_pubkey()?; // observation
    let mint_decimals_0 = cursor.read_u8()?;
    let mint_decimals_1 = cursor.read_u8()?;
    cursor.skip(2)?; // tick_spacing
    let liquidity = cursor.read_u128()?;
    let sqrt_price_x64 = cursor.read_u128()?;

    Ok(AmmV3PoolSnapshot {
        slot: 0,
        token_mint_0: token_mint_0.to_string(),
        token_mint_1: token_mint_1.to_string(),
        mint_decimals_0,
        mint_decimals_1,
        liquidity,
        sqrt_price_x64,
    })
}

struct PoolCursor<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> PoolCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        self.ensure_available(len)?;
        self.offset += len;
        Ok(())
    }

    fn skip_pubkey(&mut self) -> Result<()> {
        self.skip(32)
    }

    fn read_u8(&mut self) -> Result<u8> {
        self.ensure_available(1)?;
        let value = self.data[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_u128(&mut self) -> Result<u128> {
        let bytes = self.read_array::<16>()?;
        Ok(u128::from_le_bytes(bytes))
    }

    fn read_pubkey(&mut self) -> Result<Pubkey> {
        let bytes = self.read_array::<32>()?;
        Ok(Pubkey::new_from_array(bytes))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.ensure_available(N)?;
        let slice = &self.data[self.offset..self.offset + N];
        self.offset += N;
        Ok(slice.try_into().expect("slice length matches"))
    }

    fn ensure_available(&self, len: usize) -> Result<()> {
        if self.offset + len <= self.data.len() {
            Ok(())
        } else {
            Err(anyhow!("pool account truncated"))
        }
    }
}
