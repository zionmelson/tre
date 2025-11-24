mod bundle;
mod codegen;
mod config;
mod core;
mod corecast;
mod idl;
mod notifier;
mod proto;
mod runtime;
mod stages;
mod strategy;
mod transport;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let config = config::AppConfig::load()?;
    let executor = runtime::Executor::build(config)?;
    executor.run().await
}

fn init_tracing() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn,tre-arb-bot=info");
    }

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}
