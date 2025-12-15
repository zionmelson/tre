use anyhow::Result;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

/// Initializes tracing subscriber with compact formatting and configurable filter directives.
pub fn init_tracing(directive: Option<&str>) -> Result<()> {
    let env_directive = directive
        .map(|d| d.to_string())
        .or_else(|| std::env::var("RUST_LOG").ok())
        .unwrap_or_else(|| "info".to_string());

    let filter = EnvFilter::try_new(env_directive).or_else(|_| EnvFilter::try_new("info"))?;

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false).compact())
        .try_init()
        .map_err(Into::into)
}
