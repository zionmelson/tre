use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Full application configuration loaded from `config.json`.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub corecast: CorecastConfig,
    pub quicknode_rpcs: Vec<String>,
    pub wallets: WalletsConfig,
    pub jito: JitoConfig,
    #[serde(default)]
    pub executor: ExecutorConfig,
    #[serde(default)]
    pub planner: PlannerConfig,
    #[serde(deserialize_with = "serde_helpers::pubkey_map")]
    pub tokens: BTreeMap<String, Pubkey>,
    #[serde(deserialize_with = "serde_helpers::pubkey_map")]
    pub atas: BTreeMap<String, Pubkey>,
    #[serde(deserialize_with = "serde_helpers::token_pairs")]
    pub token_pairs: Vec<TokenPair>,
    #[serde(deserialize_with = "serde_helpers::pubkey_vec")]
    pub program_addresses: Vec<Pubkey>,
    pub solend_flash_loans: BTreeMap<String, SolendFlashLoanMarket>,
    #[serde(default)]
    pub lookup_tables: Vec<LookupTableConfig>,
}

impl Config {
    /// Loads and validates a config from disk.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let data = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;

        let cfg: Config = serde_json::from_str(&data).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;

        cfg.validate().map_err(|reason| ConfigError::Invalid {
            path: path.to_path_buf(),
            reason,
        })?;

        Ok(cfg)
    }

    fn validate(&self) -> Result<(), String> {
        if self.quicknode_rpcs.is_empty() {
            return Err("quicknode_rpcs cannot be empty".into());
        }

        if self.jito.endpoints.is_empty() {
            return Err("jito.endpoints cannot be empty".into());
        }

        if let Some(auth) = &self.jito.auth {
            if auth.endpoint.trim().is_empty() {
                return Err("jito.auth.endpoint cannot be empty".into());
            }
        }

        if self.tokens.is_empty() {
            return Err("tokens map cannot be empty".into());
        }

        if let Some(path) = &self.corecast.ca_pem {
            if !path.exists() {
                return Err(format!("corecast.ca_pem {path:?} does not exist"));
            }
            if !path.is_file() && !path.is_dir() {
                return Err(format!(
                    "corecast.ca_pem {path:?} must be a regular file or directory"
                ));
            }
        }

        for (symbol, mint) in &self.tokens {
            if !self.atas.contains_key(symbol) {
                return Err(format!(
                    "missing ATA entry for token symbol {symbol} ({mint})"
                ));
            }
        }

        if let Some(cap) = self.corecast.cache.sequence_capacity {
            if cap == 0 {
                return Err("corecast.cache.sequence_capacity must be > 0 when set".into());
            }
        }

        if self.planner.min_profit_lamports < 1 {
            return Err("planner.min_profit_lamports must be >= 1".into());
        }

        if let Some(max_hops) = self.planner.max_hops {
            if max_hops == 0 || max_hops > 4 {
                return Err("planner.max_hops must be between 1 and 4 when set".into());
            }
        }

        if let Some(depth) = self.planner.max_queue_depth {
            if depth == 0 {
                return Err("planner.max_queue_depth must be > 0 when set".into());
            }
        }

        if let Some(prefetch) = self.planner.supplemental_prefetch_per_tick {
            if prefetch == 0 {
                return Err("planner.supplemental_prefetch_per_tick must be > 0 when set".into());
            }
        }

        if let Some(arrays) = self.planner.raydium_max_supplemental_arrays {
            if arrays == 0 {
                return Err("planner.raydium_max_supplemental_arrays must be > 0 when set".into());
            }
        }

        if let Some(arrays) = self.planner.orca_max_supplemental_arrays {
            if arrays == 0 {
                return Err("planner.orca_max_supplemental_arrays must be > 0 when set".into());
            }
        }

        if let Some(ttl) = self.planner.supplemental_cache_ttl_secs {
            if ttl == 0 {
                return Err("planner.supplemental_cache_ttl_secs must be > 0 when set".into());
            }
        }

        if let Some(ttl) = self.corecast.curve_state_ttl_secs {
            if ttl == 0 {
                return Err("corecast.curve_state_ttl_secs must be > 0 when set".into());
            }
        }

        let mut seen_tables = HashSet::new();
        for table in &self.lookup_tables {
            if table.label.trim().is_empty() {
                return Err("lookup table labels cannot be empty".into());
            }
            if !seen_tables.insert(table.address) {
                return Err(format!(
                    "duplicate lookup table address {} configured",
                    table.address
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config {path:?}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config {path:?}: {source}")]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("invalid config {path:?}: {reason}")]
    Invalid { path: PathBuf, reason: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct CorecastConfig {
    pub address: String,
    pub authorization: String,
    pub cache: CorecastCacheConfig,
    #[serde(default)]
    pub ca_pem: Option<PathBuf>,
    #[serde(default)]
    pub curve_state_ttl_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CorecastCacheConfig {
    pub warm_capacity: usize,
    pub channel_capacity: usize,
    #[serde(default)]
    pub sequence_capacity: Option<usize>,
    #[serde(default)]
    pub sequence_eviction_warn: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletsConfig {
    pub auth: WalletConfig,
    pub payer: WalletConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JitoConfig {
    pub endpoints: Vec<String>,
    pub ca_pem: PathBuf,
    pub tip_floor_url: String,
    pub tip_stream_url: String,
    #[serde(default)]
    pub auth: Option<JitoAuthConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JitoAuthConfig {
    pub endpoint: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ExecutorConfig {
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub tip_divisor: u64,
    pub min_tip_lamports: u64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            compute_unit_limit: 1_400_000,
            compute_unit_price_micro_lamports: 5_000,
            tip_divisor: 20,
            min_tip_lamports: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PlannerConfig {
    pub min_profit_lamports: i64,
    pub max_hops: Option<u8>,
    pub emit_plan_memos: bool,
    pub max_queue_depth: Option<usize>,
    pub supplemental_prefetch_per_tick: Option<usize>,
    pub raydium_max_supplemental_arrays: Option<usize>,
    pub orca_max_supplemental_arrays: Option<usize>,
    pub supplemental_cache_ttl_secs: Option<u64>,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            min_profit_lamports: 1,
            max_hops: None,
            emit_plan_memos: false,
            max_queue_depth: None,
            supplemental_prefetch_per_tick: None,
            raydium_max_supplemental_arrays: None,
            orca_max_supplemental_arrays: None,
            supplemental_cache_ttl_secs: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SolendFlashLoanMarket {
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub program_id: Pubkey,
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub lending_market: Pubkey,
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub reserve: Pubkey,
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub reserve_liquidity_mint: Pubkey,
    pub borrow: SolendBorrowAccounts,
    pub repay: SolendRepayAccounts,
    #[serde(deserialize_with = "serde_helpers::pubkey_vec")]
    pub receiver_program_allowlist: Vec<Pubkey>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SolendBorrowAccounts {
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub reserve_liquidity_supply: Pubkey,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SolendRepayAccounts {
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub reserve_liquidity_supply: Pubkey,
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub reserve_liquidity_fee_receiver: Pubkey,
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub host_fee_receiver: Pubkey,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LookupTableConfig {
    pub label: String,
    #[serde(deserialize_with = "serde_helpers::pubkey_from_str")]
    pub address: Pubkey,
}

#[derive(Debug, Clone)]
pub struct TokenPair {
    pub base: Pubkey,
    pub quote: Pubkey,
}

mod serde_helpers {
    use super::TokenPair;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer};
    use solana_sdk::pubkey::Pubkey;
    use std::collections::BTreeMap;
    use std::str::FromStr;

    pub fn pubkey_from_str<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Pubkey::from_str(&value).map_err(D::Error::custom)
    }

    pub fn pubkey_vec<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<String>::deserialize(deserializer)?;
        values
            .into_iter()
            .map(|value| Pubkey::from_str(&value).map_err(D::Error::custom))
            .collect()
    }

    pub fn pubkey_map<'de, D>(deserializer: D) -> Result<BTreeMap<String, Pubkey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = BTreeMap::<String, String>::deserialize(deserializer)?;
        values
            .into_iter()
            .map(|(symbol, value)| {
                Pubkey::from_str(&value)
                    .map(|pk| (symbol, pk))
                    .map_err(D::Error::custom)
            })
            .collect()
    }

    pub fn token_pairs<'de, D>(deserializer: D) -> Result<Vec<TokenPair>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw_pairs = Vec::<[String; 2]>::deserialize(deserializer)?;
        raw_pairs
            .into_iter()
            .map(|pair| {
                let base = Pubkey::from_str(&pair[0]).map_err(D::Error::custom)?;
                let quote = Pubkey::from_str(&pair[1]).map_err(D::Error::custom)?;
                Ok(TokenPair { base, quote })
            })
            .collect()
    }
}
