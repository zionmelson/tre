use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use tracing_subscriber::{fmt, EnvFilter};
use tre_arb_bot::{config::AppConfig, runtime};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_tracing();

    let config_path = resolve_config_path();
    let app_config = AppConfig::load(&config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;

    tracing::info!(config_path = %config_path.display(), "configuration loaded");

    runtime::run(Arc::new(app_config)).await?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,tre-arb-bot=info"));

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
