use std::{collections::HashMap, env, fs, path::Path};

use serde::Deserialize;
use thiserror::Error;

const DEFAULT_CONFIG: &str = "config.json";
const CONFIG_ENV_KEY: &str = "ARBITRAGE_CONFIG";

#[derive(Debug, Clone, Deserialize)]
pub struct WalletConfig {
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletsConfig {
    pub auth: WalletConfig,
    pub payer: WalletConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JitoConfig {
    pub endpoints: Vec<String>,
    pub tip_floor_url: String,
    pub tip_stream_url: String,
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub wallets: WalletsConfig,
    pub jito: JitoConfig,
    pub tokens: HashMap<String, String>,
    pub flash_loan: FlashLoanConfig,
    #[serde(default)]
    pub quicknode_rpcs: Vec<String>,
    #[serde(default)]
    pub perf: PerfConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PerfConfig {
    #[serde(default)]
    pub ingest_core: Option<usize>,
    #[serde(default)]
    pub eval_core: Option<usize>,
    #[serde(default)]
    pub submit_core: Option<usize>,
    #[serde(default = "PerfConfig::default_ring_capacity")]
    pub ring_capacity: usize,
    #[serde(default = "PerfConfig::default_opportunity_slab")]
    pub opportunity_slab: usize,
    #[serde(default = "PerfConfig::default_bundle_slab")]
    pub bundle_slab: usize,
}

impl Default for PerfConfig {
    fn default() -> Self {
        Self {
            ingest_core: None,
            eval_core: None,
            submit_core: None,
            ring_capacity: Self::default_ring_capacity(),
            opportunity_slab: Self::default_opportunity_slab(),
            bundle_slab: Self::default_bundle_slab(),
        }
    }
}

impl PerfConfig {
    const fn default_ring_capacity() -> usize {
        2048
    }

    const fn default_opportunity_slab() -> usize {
        4096
    }

    const fn default_bundle_slab() -> usize {
        1024
    }

    pub fn sanitized_ring_capacity(&self) -> usize {
        let candidate = self.ring_capacity.max(2);
        candidate.next_power_of_two()
    }

    pub fn opportunity_capacity(&self) -> usize {
        self.opportunity_slab.max(64)
    }

    pub fn bundle_capacity(&self) -> usize {
        self.bundle_slab.max(64)
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unable to read config at {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to parse config json: {0}")]
    Parse(#[from] serde_json::Error),
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let path = env::var(CONFIG_ENV_KEY).unwrap_or_else(|_| DEFAULT_CONFIG.to_string());
        Self::from_path(path)
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        let data = fs::read_to_string(path_ref).map_err(|source| ConfigError::Io {
            path: path_ref.display().to_string(),
            source,
        })?;
        let mut cfg: AppConfig = serde_json::from_str(&data)?;
        cfg.expand_tilde_paths();
        Ok(cfg)
    }

    fn expand_tilde_paths(&mut self) {
        self.wallets.auth.path = expand_home(&self.wallets.auth.path);
        self.wallets.payer.path = expand_home(&self.wallets.payer.path);
    }
}

fn expand_home(input: &str) -> String {
    if let Some(stripped) = input.strip_prefix("~/") {
        if let Ok(home) = env::var("HOME") {
            return Path::new(&home).join(stripped).display().to_string();
        }
    }
    input.to_string()
}
