use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bincode::deserialize;
use clap::{Args, Parser, Subcommand};
use rustls::crypto::{aws_lc_rs, CryptoProvider};
use solana_client::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use solana_sdk::message::{v0::Message as V0Message, Message, VersionedMessage};
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use solana_sdk::transaction::VersionedTransaction;
use tracing_subscriber::{fmt, EnvFilter};
use tre_arb_bot::{config::AppConfig, runtime, telemetry};

#[derive(Parser, Debug)]
#[command(
    name = "tre-arb-bot",
    version,
    about = "Ultra-light Jito arbitrage executor"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the arbitrage executor (default)
    Run(RunArgs),
    /// Re-sign a previously logged bundle payload with a fresh blockhash
    Resign(ResignArgs),
}

#[derive(Args, Debug, Default)]
struct RunArgs {
    /// Optional path to a config file; defaults to $ARBITRAGE_CONFIG or config.json
    #[arg(long)]
    config: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct ResignArgs {
    /// Path to the config used for locating wallets and QuickNode RPCs
    #[arg(long, default_value = "config.json")]
    config: PathBuf,
    /// Path to a file containing the base64 payload (defaults to stdin)
    #[arg(long)]
    input: Option<PathBuf>,
    /// Optional path to write the refreshed payload (defaults to stdout)
    #[arg(long)]
    output: Option<PathBuf>,
    /// Override the RPC endpoint for fetching the latest blockhash
    #[arg(long)]
    rpc_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli
        .command
        .unwrap_or_else(|| Command::Run(RunArgs::default()))
    {
        Command::Run(args) => run_executor(args).await,
        Command::Resign(args) => resign_bundle(args),
    }
}

async fn run_executor(args: RunArgs) -> Result<()> {
    install_rustls_crypto_provider()?;
    init_tracing();
    telemetry::init_from_env().await?;

    let config_path = resolve_config_path(args.config);
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

fn resolve_config_path(cli_override: Option<PathBuf>) -> PathBuf {
    if let Some(path) = cli_override {
        return path;
    }
    std::env::var("ARBITRAGE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config.json"))
}

fn resign_bundle(args: ResignArgs) -> Result<()> {
    let config = AppConfig::load(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config.display()))?;
    let rpc_url = args
        .rpc_url
        .or_else(|| config.quicknode_rpcs.first().cloned())
        .unwrap_or_else(|| "https://api.mainnet-beta.solana.com".to_string());

    let raw_base64 = read_payload(args.input.as_deref())?;
    let tx = decode_transaction(&raw_base64)?;

    let rpc = RpcClient::new(rpc_url.clone());
    let latest_hash = rpc
        .get_latest_blockhash()
        .with_context(|| format!("failed to fetch latest blockhash from {rpc_url}"))?;

    let mut message = tx.message.clone();
    update_blockhash(&mut message, latest_hash);

    let payer = load_payer(&config.wallets.payer.path)?;
    let signer: &dyn Signer = &payer;
    let resigned = VersionedTransaction::try_new(message, &[signer])
        .map_err(|err| anyhow!("failed to sign refreshed transaction: {err}"))?;

    let encoded = encode_transaction(&resigned)?;
    write_output(args.output.as_deref(), &encoded)?;

    eprintln!(
        "Re-signed bundle using blockhash {}. New signature: {}",
        latest_hash,
        resigned
            .signatures
            .first()
            .map(|sig| sig.to_string())
            .unwrap_or_default()
    );

    Ok(())
}

fn read_payload(path: Option<&Path>) -> Result<String> {
    if let Some(path) = path {
        return fs::read_to_string(path)
            .with_context(|| format!("failed to read payload from {}", path.display()));
    }
    let mut buf = String::new();
    io::stdin()
        .read_to_string(&mut buf)
        .context("failed to read payload from stdin")?;
    let trimmed = buf.trim();
    if trimmed.is_empty() {
        Err(anyhow!("no payload provided via --input or stdin"))
    } else {
        Ok(trimmed.to_string())
    }
}

fn decode_transaction(encoded: &str) -> Result<VersionedTransaction> {
    let raw = BASE64_STANDARD
        .decode(encoded.trim())
        .context("failed to decode base64 payload")?;
    deserialize(&raw).context("failed to deserialize VersionedTransaction")
}

fn encode_transaction(tx: &VersionedTransaction) -> Result<String> {
    let raw = bincode::serialize(tx).context("failed to serialize transaction")?;
    Ok(BASE64_STANDARD.encode(raw))
}

fn load_payer(path: &Path) -> Result<Keypair> {
    read_keypair_file(path).map_err(|err| anyhow!("failed to load payer keypair: {err}"))
}

fn update_blockhash(message: &mut VersionedMessage, new_hash: Hash) {
    match message {
        VersionedMessage::Legacy(Message {
            recent_blockhash, ..
        }) => *recent_blockhash = new_hash,
        VersionedMessage::V0(V0Message {
            recent_blockhash, ..
        }) => *recent_blockhash = new_hash,
    }
}

fn write_output(path: Option<&Path>, payload: &str) -> Result<()> {
    if let Some(output_path) = path {
        fs::write(output_path, payload).with_context(|| {
            format!(
                "failed to write refreshed payload to {}",
                output_path.display()
            )
        })?
    } else {
        io::stdout()
            .write_all(payload.as_bytes())
            .context("failed to write payload to stdout")?;
    }
    Ok(())
}
