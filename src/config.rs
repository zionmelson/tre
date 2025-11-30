use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;

use crate::token_pair::TokenPair;

const ENV_CORECAST_AUTHORIZATION: &str = "TRE_CORECAST_AUTHORIZATION";
const ENV_CORECAST_ADDRESS: &str = "TRE_CORECAST_ADDRESS";
const ENV_BAM_VALIDATOR_CSV: &str = "TRE_BAM_VALIDATOR_CSV";
const ENV_JITO_ENDPOINTS: &str = "TRE_JITO_ENDPOINTS";
const ENV_JITO_API_TOKEN: &str = "TRE_JITO_API_TOKEN";
const ENV_JITO_TIP_FLOOR_URL: &str = "TRE_JITO_TIP_FLOOR_URL";
const ENV_JITO_TIP_STREAM_URL: &str = "TRE_JITO_TIP_STREAM_URL";
const ENV_JITO_AUCTION_STREAM_URL: &str = "TRE_JITO_AUCTION_STREAM_URL";
const ENV_JITO_CA_PEM: &str = "TRE_JITO_CA_PEM";
const ENV_QUICKNODE_RPCS: &str = "TRE_QUICKNODE_RPCS";
const ENV_ENABLE_FAST_LANE: &str = "TRE_ENABLE_FAST_LANE";
const ENV_HYDRATION_ONLY_MODE: &str = "TRE_HYDRATION_ONLY_MODE";

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub wallets: Wallets,
    pub jito: JitoConfig,
    pub corecast: CoreCastConfig,
    pub tokens: HashMap<String, String>,
    #[serde(default)]
    pub atas: HashMap<String, String>,
    #[serde(default)]
    pub host_fee_accounts: HashMap<String, String>,
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
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[serde(default, deserialize_with = "deserialize_optional_path")]
    pub bam_validator_csv: Option<PathBuf>,
    #[serde(skip)]
    token_atas: HashMap<Pubkey, Pubkey>,
    #[serde(skip)]
    host_fee_atas: HashMap<Pubkey, Pubkey>,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed reading config file {}", path.display()))?;
        let mut config: AppConfig = serde_json::from_str(&contents)
            .with_context(|| format!("failed parsing config file {}", path.display()))?;
        config.hydrate_env_references()?;
        config.apply_env_overrides();
        config.populate_token_pairs_from_tokens();
        config.normalize_token_pairs();
        config.populate_token_atas()?;
        config.populate_host_fee_accounts()?;
        config.apply_protocol_defaults();
        config.validate()?;
        Ok(config)
    }

    pub fn token_ata_for_mint(&self, mint: &Pubkey) -> Option<Pubkey> {
        self.token_atas.get(mint).copied()
    }

    pub fn token_ata_map(&self) -> &HashMap<Pubkey, Pubkey> {
        &self.token_atas
    }

    pub fn host_fee_account_for_mint(&self, mint: &Pubkey) -> Option<Pubkey> {
        self.host_fee_atas.get(mint).copied()
    }

    pub fn host_fee_account_map(&self) -> &HashMap<Pubkey, Pubkey> {
        &self.host_fee_atas
    }

    pub fn token_pair_allowlist(&self) -> HashSet<TokenPair> {
        self.token_pairs.iter().map(TokenPair::from_array).collect()
    }

    fn apply_protocol_defaults(&mut self) {
        for protocol in KNOWN_PROTOCOLS {
            self.protocols.entry((*protocol).to_string()).or_default();
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

    pub fn protocol_max_utilization_bps(&self, protocol: &str) -> Option<u64> {
        self.protocols
            .get(protocol)
            .and_then(|entry| entry.max_utilization_bps)
    }

    pub fn bam_validator_csv_path(&self) -> PathBuf {
        self.bam_validator_csv
            .clone()
            .unwrap_or_else(|| PathBuf::from("bam_validators.csv"))
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.token_pairs.is_empty(),
            "token_pairs must contain at least one allowed pair"
        );
        ensure!(
            !self.jito.endpoints.is_empty(),
            "jito.endpoints must contain at least one endpoint"
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

    fn populate_token_atas(&mut self) -> Result<()> {
        self.token_atas.clear();
        if self.atas.is_empty() {
            return Ok(());
        }

        let mut missing = Vec::new();
        for (symbol, mint_str) in &self.tokens {
            let mint = Pubkey::from_str(mint_str).with_context(|| {
                format!("invalid mint {} configured for token {}", mint_str, symbol)
            })?;
            if let Some(ata_str) = self.atas.get(symbol).or_else(|| self.atas.get(mint_str)) {
                let ata = Pubkey::from_str(ata_str).with_context(|| {
                    format!("invalid ata {} configured for token {}", ata_str, symbol)
                })?;
                self.token_atas.insert(mint, ata);
            } else {
                missing.push(symbol.clone());
            }
        }

        for (label, ata_str) in &self.atas {
            if self.tokens.contains_key(label) {
                continue;
            }
            if let Ok(mint) = Pubkey::from_str(label) {
                if self.token_atas.contains_key(&mint) {
                    continue;
                }
                let ata = Pubkey::from_str(ata_str).with_context(|| {
                    format!("invalid ata {} configured for mint {}", ata_str, label)
                })?;
                self.token_atas.insert(mint, ata);
            }
        }

        ensure!(
            missing.is_empty(),
            "atas missing entries for tokens: {}",
            missing.join(", ")
        );

        Ok(())
    }

    fn populate_host_fee_accounts(&mut self) -> Result<()> {
        self.host_fee_atas.clear();
        if self.host_fee_accounts.is_empty() {
            return Ok(());
        }

        for (label, ata_str) in &self.host_fee_accounts {
            let mint_str = self.tokens.get(label);
            let mint = if let Some(symbol_mint) = mint_str {
                Pubkey::from_str(symbol_mint).with_context(|| {
                    format!(
                        "invalid mint {} configured for token {}",
                        symbol_mint, label
                    )
                })?
            } else {
                Pubkey::from_str(label).with_context(|| {
                    format!(
                        "host_fee_accounts key {} must be a known token symbol or mint",
                        label
                    )
                })?
            };

            let ata = Pubkey::from_str(ata_str).with_context(|| {
                format!(
                    "invalid host fee account {} configured for {}",
                    ata_str, label
                )
            })?;

            self.host_fee_atas.insert(mint, ata);
        }

        Ok(())
    }

    fn hydrate_env_references(&mut self) -> Result<()> {
        hydrate_string(&mut self.corecast.authorization, "corecast.authorization")?;
        hydrate_string(&mut self.corecast.address, "corecast.address")?;
        hydrate_vec(&mut self.jito.endpoints, "jito.endpoints")?;
        hydrate_string(&mut self.jito.tip_floor_url, "jito.tip_floor_url")?;
        hydrate_string(&mut self.jito.tip_stream_url, "jito.tip_stream_url")?;
        if let Some(url) = self.jito.auction_stream_url.as_mut() {
            hydrate_string(url, "jito.auction_stream_url")?;
        }
        if let Some(token) = self.jito.api_token.as_mut() {
            hydrate_string(token, "jito.api_token")?;
        }
        hydrate_vec(&mut self.quicknode_rpcs, "quicknode_rpcs")?;
        if let Some(ref mut csv_path) = self.bam_validator_csv {
            hydrate_path(csv_path, "bam_validator_csv")?;
        }
        Ok(())
    }

    fn apply_env_overrides(&mut self) {
        if let Some(value) = env_string(ENV_CORECAST_AUTHORIZATION) {
            self.corecast.authorization = value;
        }
        if let Some(value) = env_string(ENV_CORECAST_ADDRESS) {
            self.corecast.address = value;
        }
        if let Some(path) = env_path(ENV_BAM_VALIDATOR_CSV) {
            self.bam_validator_csv = Some(path);
        }
        if let Some(list) = env_list(ENV_JITO_ENDPOINTS) {
            self.jito.endpoints = list;
        }
        if let Some(value) = env_string(ENV_JITO_API_TOKEN) {
            self.jito.api_token = Some(value);
        }
        if let Some(value) = env_string(ENV_JITO_TIP_FLOOR_URL) {
            self.jito.tip_floor_url = value;
        }
        if let Some(value) = env_string(ENV_JITO_TIP_STREAM_URL) {
            self.jito.tip_stream_url = value;
        }
        if let Some(value) = env_string(ENV_JITO_AUCTION_STREAM_URL) {
            self.jito.auction_stream_url = Some(value);
        }
        if let Some(path) = env_path(ENV_JITO_CA_PEM) {
            self.jito.ca_pem = Some(path);
        }
        if let Some(list) = env_list(ENV_QUICKNODE_RPCS) {
            self.quicknode_rpcs = list;
        }
        if let Some(enabled) = env_bool(ENV_ENABLE_FAST_LANE) {
            self.pipeline.enable_fast_lane = enabled;
        }
        if let Some(enabled) = env_bool(ENV_HYDRATION_ONLY_MODE) {
            self.pipeline.hydration_only_mode = enabled;
        }
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
    pub borrow: FlashLoanBorrowAccounts,
    pub repay: FlashLoanRepayAccounts,
    #[serde(default)]
    pub receiver_program_allowlist: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlashLoanBorrowAccounts {
    pub reserve_liquidity_supply: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlashLoanRepayAccounts {
    pub reserve_liquidity_supply: String,
    pub reserve_liquidity_fee_receiver: String,
    pub host_fee_receiver: String,
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
pub struct PipelineConfig {
    pub enable_fast_lane: bool,
    pub hydration_only_mode: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            enable_fast_lane: true,
            hydration_only_mode: false,
        }
    }
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
    #[serde(default)]
    pub max_utilization_bps: Option<u64>,
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
    pub tip_floor_min_lamports: Option<u64>,
    pub wallet_tip_cap_bps: Option<u64>,
    pub tip_congestion_target_latency_ms: Option<u64>,
    pub tip_congestion_max_multiplier: Option<f64>,
    pub tip_congestion_expired_weight_bps: Option<u64>,
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

fn hydrate_string(value: &mut String, field: &str) -> Result<()> {
    if let Some(var) = parse_env_reference(value) {
        let resolved = env::var(&var).with_context(|| {
            format!(
                "environment variable {} referenced by {} is not set",
                var, field
            )
        })?;
        *value = resolved;
    }
    Ok(())
}

fn hydrate_vec(values: &mut [String], field: &str) -> Result<()> {
    for (idx, entry) in values.iter_mut().enumerate() {
        let label = format!("{}[{}]", field, idx);
        hydrate_string(entry, &label)?;
    }
    Ok(())
}

fn hydrate_path(path: &mut PathBuf, field: &str) -> Result<()> {
    if let Some(raw) = path.to_str() {
        if let Some(var) = parse_env_reference(raw) {
            let resolved = env::var(&var).with_context(|| {
                format!(
                    "environment variable {} referenced by {} is not set",
                    var, field
                )
            })?;
            *path = expand_tilde(&resolved);
        }
    }
    Ok(())
}

fn env_string(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_list(name: &str) -> Option<Vec<String>> {
    let raw = env_string(name)?;
    let entries: Vec<String> = raw
        .split([',', '\n'])
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| entry.to_string())
        .collect();
    if entries.is_empty() {
        None
    } else {
        Some(entries)
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    env_string(name).map(|value| expand_tilde(&value))
}

fn env_bool(name: &str) -> Option<bool> {
    env_string(name).and_then(|value| match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    })
}

fn parse_env_reference(value: &str) -> Option<String> {
    value
        .strip_prefix("$ENV{")
        .and_then(|rest| rest.strip_suffix('}'))
        .map(str::trim)
        .filter(|inner| !inner.is_empty())
        .map(|inner| inner.to_string())
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

    use super::{env_list, expand_tilde, parse_env_reference};

    #[test]
    fn expands_home_prefix() {
        env::set_var("HOME", "/tmp/home");
        let path = expand_tilde("~/keys.json");
        assert_eq!(path, std::path::PathBuf::from("/tmp/home/keys.json"));
    }

    #[test]
    fn parses_env_reference() {
        assert_eq!(parse_env_reference("$ENV{FOO}"), Some("FOO".into()));
        assert_eq!(parse_env_reference("$ENV{ BAR }"), Some("BAR".into()));
        assert_eq!(parse_env_reference("$ENV{}"), None);
        assert_eq!(parse_env_reference("FOO"), None);
    }

    #[test]
    fn splits_env_list() {
        env::set_var("TRE_TEST_LIST", "a,b, c\n d");
        let entries = env_list("TRE_TEST_LIST").expect("list parsed");
        assert_eq!(entries, vec!["a", "b", "c", "d"]);
    }
}
