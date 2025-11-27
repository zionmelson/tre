use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

use crate::token_pair::TokenPair;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub wallets: Wallets,
    pub jito: JitoConfig,
    pub corecast: CoreCastConfig,
    pub tokens: HashMap<String, String>,
    #[serde(default)]
    pub token_pairs: Vec<[String; 2]>,
    #[serde(default)]
    pub program_addresses: Vec<String>,
    pub flash_loans: HashMap<String, FlashLoanConfig>,
    #[serde(default)]
    pub quicknode_rpcs: Vec<String>,
    #[serde(default)]
    pub protocols: HashMap<String, ProtocolConfig>,
    #[serde(default)]
    pub perf: PerfTuning,
    #[serde(default)]
    pub strategy: StrategyOverrides,
    #[serde(default)]
    pub alerts: AlertingConfig,
    #[serde(default)]
    pub submission: SubmissionTrackingConfig,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed reading config file {}", path.display()))?;
        let mut config: AppConfig = serde_json::from_str(&contents)
            .with_context(|| format!("failed parsing config file {}", path.display()))?;
        config.populate_token_pairs_from_tokens();
        config.normalize_token_pairs();
        config.apply_protocol_defaults();
        config.validate()?;
        Ok(config)
    }

    pub fn token_pair_allowlist(&self) -> HashSet<TokenPair> {
        self.token_pairs.iter().map(TokenPair::from_array).collect()
    }

    fn apply_protocol_defaults(&mut self) {
        for protocol in KNOWN_PROTOCOLS {
            self.protocols
                .entry((*protocol).to_string())
                .or_insert_with(ProtocolConfig::default);
        }

        for entry in self.protocols.values_mut() {
            if entry.rpc_endpoints.is_empty() {
                entry.rpc_endpoints = self.quicknode_rpcs.clone();
            }
        }
    }

    pub fn protocol_rpc_endpoints(&self, protocol: &str) -> Vec<String> {
        self.protocols
            .get(protocol)
            .map(|entry| entry.rpc_endpoints.clone())
            .filter(|endpoints| !endpoints.is_empty())
            .unwrap_or_else(|| self.quicknode_rpcs.clone())
    }

    pub fn protocol_hydration_retry_ms(&self, protocol: &str) -> Option<u64> {
        self.protocols
            .get(protocol)
            .and_then(|entry| entry.hydration_retry_ms)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.token_pairs.is_empty(),
            "token_pairs must contain at least one allowed pair"
        );
        ensure!(
            self.jito.endpoints.len() >= 2,
            "jito.endpoints must contain at least two endpoints for failover"
        );
        ensure!(
            self.perf.ring_capacity > 0,
            "perf.ring_capacity must be greater than zero"
        );
        ensure!(
            self.perf.opportunity_slab > 0,
            "perf.opportunity_slab must be greater than zero"
        );
        ensure!(
            self.perf.bundle_slab > 0,
            "perf.bundle_slab must be greater than zero"
        );
        ensure!(
            !self.corecast.address.trim().is_empty(),
            "corecast.address must be set"
        );
        ensure!(
            !self.corecast.authorization.trim().is_empty(),
            "corecast.authorization must be set"
        );
        ensure!(
            self.corecast.cache.channel_capacity > 0,
            "corecast.cache.channel_capacity must be greater than zero"
        );
        ensure!(
            self.corecast.cache.warm_capacity <= self.corecast.cache.channel_capacity,
            "corecast.cache.warm_capacity must be less than or equal to channel_capacity"
        );
        ensure!(
            !self.flash_loans.is_empty(),
            "flash_loans must contain at least one Solend reserve entry"
        );
        Ok(())
    }

    fn populate_token_pairs_from_tokens(&mut self) {
        if !self.token_pairs.is_empty() {
            return;
        }
        let mut mints: Vec<String> = self.tokens.values().cloned().collect();
        mints.sort();
        mints.dedup();
        for i in 0..mints.len() {
            for j in (i + 1)..mints.len() {
                self.token_pairs.push([mints[i].clone(), mints[j].clone()]);
            }
        }
    }

    fn normalize_token_pairs(&mut self) {
        if self.token_pairs.is_empty() {
            return;
        }
        let mut normalized: Vec<[String; 2]> = self
            .token_pairs
            .iter()
            .map(|entry| {
                let pair = TokenPair::from_array(entry);
                [pair.mint_a, pair.mint_b]
            })
            .collect();
        normalized.sort();
        normalized.dedup();
        self.token_pairs = normalized;
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Wallets {
    pub auth: WalletPath,
    pub payer: WalletPath,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletPath {
    #[serde(deserialize_with = "deserialize_path")]
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JitoConfig {
    pub endpoints: Vec<String>,
    pub tip_floor_url: String,
    pub tip_stream_url: String,
    #[serde(default)]
    pub api_token: Option<String>,
    #[serde(default)]
    pub auction_stream_url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_path")]
    pub ca_pem: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlashLoanConfig {
    pub program_id: String,
    pub lending_market: String,
    pub reserve: String,
    pub reserve_liquidity_mint: String,
    pub reserve_liquidity_supply: String,
    pub reserve_liquidity_fee_receiver: String,
    pub host_fee_receiver: String,
    #[serde(default)]
    pub receiver_program_allowlist: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreCastConfig {
    pub address: String,
    pub authorization: String,
    #[serde(default)]
    pub cache: CoreCastCache,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CoreCastCache {
    pub warm_capacity: usize,
    pub channel_capacity: usize,
}

impl Default for CoreCastCache {
    fn default() -> Self {
        Self {
            warm_capacity: 32,
            channel_capacity: 512,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PerfTuning {
    pub ingest_core: usize,
    pub eval_core: usize,
    pub submit_core: usize,
    pub ring_capacity: usize,
    pub opportunity_slab: usize,
    pub bundle_slab: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SubmissionTrackingConfig {
    pub pending_ttl_secs: u64,
    pub sweep_interval_secs: u64,
    pub result_stream_backoff_ms: u64,
    pub auction_stream_backoff_ms: u64,
    pub status_poll_interval_secs: u64,
    pub status_poll_batch_size: usize,
    pub status_poll_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProtocolConfig {
    #[serde(default)]
    pub rpc_endpoints: Vec<String>,
    #[serde(default)]
    pub hydration_retry_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct StrategyOverrides {
    pub min_edge_bps: Option<f64>,
    pub max_opportunity_staleness_ms: Option<u64>,
    pub max_duplicates_per_signature: Option<usize>,
    pub max_route_hops: Option<usize>,
    pub min_profit_lamports: Option<u64>,
    pub min_tip_lamports: Option<u64>,
    pub max_tip_lamports: Option<u64>,
    pub tip_profit_share_bps: Option<u64>,
    pub tip_floor_markup_bps: Option<u64>,
    pub tip_floor_fallback_lamports: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AlertingConfig {
    pub webhook_url: Option<String>,
    pub environment: Option<String>,
}

const KNOWN_PROTOCOLS: &[&str] = &[
    "raydium_v3",
    "cp_amm",
    "lb_clmm",
    "lifinity_v2",
    "whirlpool",
];

impl Default for PerfTuning {
    fn default() -> Self {
        Self {
            ingest_core: 0,
            eval_core: 1,
            submit_core: 2,
            ring_capacity: 1024,
            opportunity_slab: 2048,
            bundle_slab: 512,
        }
    }
}

impl Default for SubmissionTrackingConfig {
    fn default() -> Self {
        Self {
            pending_ttl_secs: 180,
            sweep_interval_secs: 10,
            result_stream_backoff_ms: 500,
            auction_stream_backoff_ms: 1_000,
            status_poll_interval_secs: 5,
            status_poll_batch_size: 32,
            status_poll_timeout_secs: 2,
        }
    }
}

fn deserialize_path<'de, D>(deserializer: D) -> std::result::Result<PathBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    Ok(expand_tilde(&raw))
}

fn deserialize_optional_path<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<PathBuf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.map(|raw| expand_tilde(&raw)))
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home).join(stripped);
        }
    } else if path == "~" {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home);
        }
    }

    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::expand_tilde;

    #[test]
    fn expands_home_prefix() {
        env::set_var("HOME", "/tmp/home");
        let path = expand_tilde("~/keys.json");
        assert_eq!(path, std::path::PathBuf::from("/tmp/home/keys.json"));
    }
}
