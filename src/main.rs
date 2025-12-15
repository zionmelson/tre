use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tre_arb_bot::app::App;
use tre_arb_bot::config;
use tre_arb_bot::metrics::Metrics;
use tre_arb_bot::telemetry;

#[derive(Parser, Debug)]
#[command(author, version, about = "tre ultra-low latency arb executor")]
struct Args {
    /// Path to the configuration file.
    #[arg(long, short = 'c', default_value = "config.json")]
    config: PathBuf,
    /// Optional tracing filter directive (overrides RUST_LOG).
    #[arg(long)]
    log: Option<String>,
    /// Address to bind the Prometheus metrics exporter.
    #[arg(long, default_value = "127.0.0.1:9898")]
    metrics_bind: SocketAddr,
    /// Optional recorded pool stream to feed via replay harness.
    #[arg(long)]
    replay_pools: Option<PathBuf>,
    /// Optional recorded trade stream to feed via replay harness.
    #[arg(long)]
    replay_trades: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    telemetry::init_tracing(args.log.as_deref())?;

    let config = config::Config::load(&args.config)?;
    let config_path = args.config.clone();
    let metrics = Metrics::new();
    let metrics_addr = args.metrics_bind;
    let replay_pools = args.replay_pools.clone();
    let replay_trades = args.replay_trades.clone();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build single-threaded runtime")?;

    runtime.block_on(async move {
        let app = App::new(
            config,
            config_path,
            metrics,
            metrics_addr,
            replay_pools,
            replay_trades,
        )?;
        app.run().await
    })
}
