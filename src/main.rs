use std::{path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use rustls::crypto::{aws_lc_rs, CryptoProvider};
use tracing_subscriber::{fmt, EnvFilter};
use tre_arb_bot::{config::AppConfig, runtime, telemetry};

#[tokio::main]
async fn main() -> Result<()> {
    install_rustls_crypto_provider()?;
    init_tracing();
    telemetry::init_from_env().await?;

    let config_path = resolve_config_path();
    let app_config = AppConfig::load(&config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;

    tracing::info!(config_path = %config_path.display(), "configuration loaded");

    runtime::run(Arc::new(app_config)).await?;

    Ok(())
}

fn install_rustls_crypto_provider() -> Result<()> {
    let provider = aws_lc_rs::default_provider();
    CryptoProvider::install_default(provider)
        .map_err(|err| anyhow!("failed to install rustls crypto provider: {err:?}"))?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,tre-arb-bot=info,stage0=info,corecast=info"));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

fn resolve_config_path() -> PathBuf {
    std::env::var("ARBITRAGE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config.json"))
}
