use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub wallets: Wallets,
    pub jito: JitoConfig,
    pub corecast: CoreCastConfig,
    pub tokens: HashMap<String, String>,
    #[serde(default)]
    pub program_addresses: Vec<String>,
    pub flash_loan: FlashLoanConfig,
    #[serde(default)]
    pub quicknode_rpcs: Vec<String>,
    #[serde(default)]
    pub perf: PerfTuning,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed reading config file {}", path.display()))?;
        let config: AppConfig = serde_json::from_str(&contents)
            .with_context(|| format!("failed parsing config file {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.jito.endpoints.is_empty(),
            "at least one Jito endpoint is required"
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
        Ok(())
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

fn deserialize_path<'de, D>(deserializer: D) -> std::result::Result<PathBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    Ok(expand_tilde(&raw))
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
