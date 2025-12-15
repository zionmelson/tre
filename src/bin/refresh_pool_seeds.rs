use anyhow::{anyhow, Context, Result};
use clap::Parser;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tre_arb_bot::config::Config;
use tre_arb_bot::pipeline::CurveKind;
use tre_arb_bot::pool_seeds::{PoolSeed, PoolSeedRegistry};

#[derive(Parser, Debug)]
#[command(
    about = "Refresh config/pool_seeds.json from health check logs",
    version
)]
struct Args {
    /// Path to the application config.
    #[arg(long, default_value = "config.json")]
    config: PathBuf,

    /// Explicit health log to parse. When omitted the newest log in --log-dir is used.
    #[arg(long)]
    log: Option<PathBuf>,

    /// Directory containing health logs when --log is unset.
    #[arg(long, default_value = "healthchecks")]
    log_dir: PathBuf,

    /// Output path for the refreshed snapshot.
    #[arg(long, default_value = "config/pool_seeds.json")]
    output: PathBuf,

    /// Optional template file describing which pools to refresh. Defaults to --output when it exists.
    #[arg(long)]
    template: Option<PathBuf>,

    /// Maximum pools to emit when no template is provided.
    #[arg(long, default_value_t = 32)]
    limit: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config).context("load config")?;
    let allowed_pairs = build_pair_allowlist(&config);
    let log_path = match &args.log {
        Some(path) => path.clone(),
        None => discover_latest_log(&args.log_dir)?,
    };
    let updates = read_pool_updates(&log_path)?;
    if updates.is_empty() {
        return Err(anyhow!("no pool updates found in {:?}", log_path));
    }

    let template_path = args
        .template
        .clone()
        .or_else(|| args.output.exists().then(|| args.output.clone()));

    let (seeds, missing) = if let Some(path) = template_path {
        let registry = PoolSeedRegistry::load(&path)
            .with_context(|| format!("load template snapshot from {path:?}"))?;
        refresh_from_template(registry.seeds(), &updates)
    } else {
        (
            build_from_updates(&updates, &allowed_pairs, args.limit),
            Vec::new(),
        )
    };

    PoolSeedRegistry::write_snapshot(&args.output, &seeds)
        .with_context(|| format!("write refreshed seeds to {:?}", args.output))?;

    if missing.is_empty() {
        println!(
            "wrote {} pool seeds from {}",
            seeds.len(),
            log_path.display()
        );
    } else {
        eprintln!(
            "warning: {} pools were not observed in {}; kept existing snapshot entries",
            missing.len(),
            log_path.display()
        );
        println!(
            "wrote {} pool seeds ({} stale) from {}",
            seeds.len(),
            missing.len(),
            log_path.display()
        );
    }

    Ok(())
}

fn build_pair_allowlist(config: &Config) -> HashSet<(Pubkey, Pubkey)> {
    let mut pairs = HashSet::new();
    for pair in &config.token_pairs {
        pairs.insert((pair.base, pair.quote));
        pairs.insert((pair.quote, pair.base));
    }
    pairs
}

fn discover_latest_log(dir: &Path) -> Result<PathBuf> {
    let mut newest: Option<(SystemTime, PathBuf)> = None;
    for entry in fs::read_dir(dir).with_context(|| format!("read log dir {dir:?}"))? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        let name = match entry.file_name().into_string() {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !name.starts_with("health-") || !name.ends_with(".log") {
            continue;
        }
        let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        match &mut newest {
            Some((ts, path)) => {
                if modified > *ts {
                    *ts = modified;
                    *path = entry.path();
                }
            }
            None => newest = Some((modified, entry.path())),
        }
    }
    newest
        .map(|(_, path)| path)
        .ok_or_else(|| anyhow!("no health logs found under {dir:?}"))
}

fn read_pool_updates(path: &Path) -> Result<HashMap<Pubkey, LogPoolUpdate>> {
    let file = File::open(path).with_context(|| format!("open log {path:?}"))?;
    let reader = io::BufReader::new(file);
    let mut updates = HashMap::new();
    for line in reader.lines() {
        let line = line?;
        if let Some(update) = parse_pool_update_line(&line) {
            updates
                .entry(update.pool)
                .and_modify(|existing: &mut LogPoolUpdate| {
                    if update.slot >= existing.slot {
                        *existing = update.clone();
                    }
                })
                .or_insert(update);
        }
    }
    Ok(updates)
}

fn refresh_from_template(
    template: &[PoolSeed],
    updates: &HashMap<Pubkey, LogPoolUpdate>,
) -> (Vec<PoolSeed>, Vec<Pubkey>) {
    let mut refreshed = Vec::with_capacity(template.len());
    let mut missing = Vec::new();
    for seed in template {
        if let Some(update) = updates.get(&seed.pool) {
            refreshed.push(update.as_seed());
        } else {
            refreshed.push(seed.clone());
            missing.push(seed.pool);
        }
    }
    (refreshed, missing)
}

fn build_from_updates(
    updates: &HashMap<Pubkey, LogPoolUpdate>,
    allowed_pairs: &HashSet<(Pubkey, Pubkey)>,
    limit: usize,
) -> Vec<PoolSeed> {
    let mut records: Vec<_> = updates
        .values()
        .filter(|entry| pair_allowed(allowed_pairs, entry.base_mint, entry.quote_mint))
        .cloned()
        .collect();
    records.sort_by(|a, b| b.slot.cmp(&a.slot));
    records
        .into_iter()
        .take(limit)
        .map(|entry| entry.as_seed())
        .collect()
}

fn pair_allowed(pairs: &HashSet<(Pubkey, Pubkey)>, base: Pubkey, quote: Pubkey) -> bool {
    pairs.contains(&(base, quote)) || pairs.contains(&(quote, base))
}

#[derive(Debug, Clone)]
struct LogPoolUpdate {
    pool: Pubkey,
    program: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    curve: CurveKind,
    slot: u64,
}

impl LogPoolUpdate {
    fn as_seed(&self) -> PoolSeed {
        let mut seed = PoolSeed {
            pool: self.pool,
            program: self.program,
            curve: self.curve,
            base_mint: self.base_mint,
            quote_mint: self.quote_mint,
        };
        seed.canonicalize_curve();
        seed
    }
}

fn parse_pool_update_line(line: &str) -> Option<LogPoolUpdate> {
    if !line.contains("received pool update") {
        return None;
    }
    let mut fields: HashMap<&str, &str> = HashMap::new();
    for token in line.split_whitespace() {
        if let Some((key, value)) = token.split_once('=') {
            fields.insert(key.trim(), value.trim_matches(|c| c == '\"' || c == ','));
        }
    }
    let pool = parse_pubkey(fields.get("pool")?)?;
    let program = parse_pubkey(fields.get("program")?)?;
    let base_mint = parse_pubkey(fields.get("base_mint")?)?;
    let quote_mint = parse_pubkey(fields.get("quote_mint")?)?;
    let curve = parse_curve(fields.get("update.curve")?)?;
    let slot = fields.get("slot")?.parse().ok()?;
    Some(LogPoolUpdate {
        pool,
        program,
        base_mint,
        quote_mint,
        curve,
        slot,
    })
}

fn parse_pubkey(raw: &&str) -> Option<Pubkey> {
    Pubkey::from_str(raw).ok()
}

fn parse_curve(raw: &&str) -> Option<CurveKind> {
    match *raw {
        "RaydiumClmm" => Some(CurveKind::RaydiumClmm),
        "OrcaWhirlpool" => Some(CurveKind::OrcaWhirlpool),
        "MeteoraDamm" => Some(CurveKind::MeteoraDamm),
        "MeteoraDlmm" => Some(CurveKind::MeteoraDlmm),
        _ => None,
    }
}
