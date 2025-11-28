use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use once_cell::sync::Lazy;
use reqwest::blocking::Client as BlockingClient;
use serde::Deserialize;
use solana_client::rpc_client::RpcClient;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Signer};
use tracing::{debug, info, warn};

use crate::builder::compute_flash_loan_fee;
use crate::config::AppConfig;
use crate::stages::{ArbitrageOpportunity, BundleCandidate, Opportunity};
use crate::telemetry;
use crate::tip_pressure;

const DEFAULT_MIN_EDGE_BPS: f64 = 2.5;
const DEFAULT_MAX_STALE_MS: u64 = 750;
const DEFAULT_MAX_DUPLICATES_PER_SIGNATURE: usize = 2;
const DEFAULT_MAX_ROUTE_HOPS: usize = 4;
const DEFAULT_MIN_PROFIT_LAMPORTS: u64 = 50_000; // 0.00005 SOL
const DEFAULT_MIN_TIP_LAMPORTS: u64 = 50_000; // 0.00005 SOL
const DEFAULT_MAX_TIP_LAMPORTS: u64 = 20_000_000; // 0.02 SOL
const DEFAULT_TIP_PROFIT_SHARE_BPS: u64 = 1500; // 15%
const DEFAULT_TIP_FLOOR_MARKUP_BPS: u64 = 3_500; // 35%
const DEFAULT_TIP_FLOOR_FALLBACK_LAMPORTS: u64 = 75_000;
const DEFAULT_TIP_FLOOR_MIN_LAMPORTS: u64 = 75_000;
const DEFAULT_TIP_TARGET_STAGE3_LATENCY_MS: u64 = 600;
const DEFAULT_TIP_CONGESTION_MAX_MULTIPLIER: f64 = 10.0;
const DEFAULT_TIP_CONGESTION_EXPIRED_WEIGHT_BPS: u64 = 7_500; // 75%
const TIP_POLL_INTERVAL_SECS: u64 = 1;
const TIP_FETCH_TIMEOUT_SECS: u64 = 3;
const WALLET_BALANCE_POLL_INTERVAL_SECS: u64 = 5;
const WALLET_BALANCE_RPC_TIMEOUT_SECS: u64 = 6;
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

static TIP_RECIPIENTS: Lazy<Vec<Pubkey>> = Lazy::new(|| {
    [
        "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
        "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
        "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
        "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
        "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
        "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
        "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    ]
    .iter()
    .map(|pk| Pubkey::from_str(pk).expect("valid static Jito tip account"))
    .collect()
});

#[derive(Clone, Debug)]
pub struct StrategyConfig {
    pub min_edge_bps: f64,
    pub max_opportunity_staleness: Duration,
    pub max_duplicates_per_signature: usize,
    pub max_route_hops: usize,
    pub min_profit_lamports: u64,
    pub min_tip_lamports: u64,
    pub max_tip_lamports: u64,
    pub tip_profit_share_bps: u64,
    pub tip_floor_markup_bps: u64,
    pub tip_floor_fallback_lamports: u64,
    pub tip_floor_min_lamports: u64,
    pub wallet_tip_cap_bps: Option<u64>,
    pub tip_congestion_target_latency_ms: u64,
    pub tip_congestion_max_multiplier: f64,
    pub tip_congestion_expired_weight_bps: u64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            min_edge_bps: DEFAULT_MIN_EDGE_BPS,
            max_opportunity_staleness: Duration::from_millis(DEFAULT_MAX_STALE_MS),
            max_duplicates_per_signature: DEFAULT_MAX_DUPLICATES_PER_SIGNATURE,
            max_route_hops: DEFAULT_MAX_ROUTE_HOPS,
            min_profit_lamports: DEFAULT_MIN_PROFIT_LAMPORTS,
            min_tip_lamports: DEFAULT_MIN_TIP_LAMPORTS,
            max_tip_lamports: DEFAULT_MAX_TIP_LAMPORTS,
            tip_profit_share_bps: DEFAULT_TIP_PROFIT_SHARE_BPS,
            tip_floor_markup_bps: DEFAULT_TIP_FLOOR_MARKUP_BPS,
            tip_floor_fallback_lamports: DEFAULT_TIP_FLOOR_FALLBACK_LAMPORTS,
            tip_floor_min_lamports: DEFAULT_TIP_FLOOR_MIN_LAMPORTS,
            wallet_tip_cap_bps: None,
            tip_congestion_target_latency_ms: DEFAULT_TIP_TARGET_STAGE3_LATENCY_MS,
            tip_congestion_max_multiplier: DEFAULT_TIP_CONGESTION_MAX_MULTIPLIER,
            tip_congestion_expired_weight_bps: DEFAULT_TIP_CONGESTION_EXPIRED_WEIGHT_BPS,
        }
    }
}

impl StrategyConfig {
    pub fn from_app_config(config: &AppConfig) -> Self {
        let mut cfg = Self::default();
        let overrides = &config.strategy;
        if let Some(value) = overrides.min_edge_bps {
            cfg.min_edge_bps = value;
        }
        if let Some(ms) = overrides.max_opportunity_staleness_ms {
            cfg.max_opportunity_staleness = Duration::from_millis(ms);
        }
        if let Some(value) = overrides.max_duplicates_per_signature {
            cfg.max_duplicates_per_signature = value;
        }
        if let Some(value) = overrides.max_route_hops {
            cfg.max_route_hops = value;
        }
        if let Some(value) = overrides.min_profit_lamports {
            cfg.min_profit_lamports = value;
        }
        if let Some(value) = overrides.min_tip_lamports {
            cfg.min_tip_lamports = value;
        }
        if let Some(value) = overrides.max_tip_lamports {
            cfg.max_tip_lamports = value;
        }
        if let Some(value) = overrides.tip_profit_share_bps {
            cfg.tip_profit_share_bps = value;
        }
        if let Some(value) = overrides.tip_floor_markup_bps {
            cfg.tip_floor_markup_bps = value;
        }
        if let Some(value) = overrides.tip_floor_fallback_lamports {
            cfg.tip_floor_fallback_lamports = value;
        }
        if let Some(value) = overrides.tip_floor_min_lamports {
            cfg.tip_floor_min_lamports = value;
        }
        if let Some(value) = overrides.wallet_tip_cap_bps {
            cfg.wallet_tip_cap_bps = Some(value);
        }
        if let Some(value) = overrides.tip_congestion_target_latency_ms {
            cfg.tip_congestion_target_latency_ms = value;
        }
        if let Some(value) = overrides.tip_congestion_max_multiplier {
            cfg.tip_congestion_max_multiplier = value;
        }
        if let Some(value) = overrides.tip_congestion_expired_weight_bps {
            cfg.tip_congestion_expired_weight_bps = value;
        }
        cfg
    }
}

#[derive(Debug)]
struct DedupState {
    signature_counts: HashMap<String, usize>,
    last_cleanup: Instant,
}

impl Default for DedupState {
    fn default() -> Self {
        Self {
            signature_counts: HashMap::new(),
            last_cleanup: Instant::now(),
        }
    }
}

pub struct StrategyEngine {
    config: StrategyConfig,
    tip_oracle: TipOracle,
    dedup: Mutex<DedupState>,
    accepted_count: AtomicU64,
    rejected_count: AtomicU64,
}

impl StrategyEngine {
    pub fn new(app_config: Arc<AppConfig>) -> Result<Self> {
        let config = StrategyConfig::from_app_config(app_config.as_ref());
        let payer = read_keypair_file(&app_config.wallets.payer.path).map_err(|err| {
            anyhow!(
                "failed to read payer wallet {}: {err}",
                app_config.wallets.payer.path.display()
            )
        })?;
        let payer_pubkey = payer.pubkey();
        drop(payer);

        let wallet_tracker = if let Some(cap_bps) = config.wallet_tip_cap_bps {
            if cap_bps == 0 {
                None
            } else {
                Some(WalletBalanceTracker::spawn(
                    payer_pubkey,
                    app_config.quicknode_rpcs.clone(),
                )?)
            }
        } else {
            None
        };

        let wallet_cap = wallet_tracker.as_ref().and_then(|tracker| {
            config
                .wallet_tip_cap_bps
                .map(|bps| WalletTipCap::new(bps, Arc::clone(tracker)))
        });

        let tip_oracle = TipOracle::from_url(
            &app_config.jito.tip_floor_url,
            config.tip_floor_fallback_lamports,
            wallet_cap,
        )?;
        Ok(Self::with_tip_oracle(config, tip_oracle))
    }

    pub fn with_config(config: StrategyConfig) -> Self {
        let fallback = config.tip_floor_fallback_lamports;
        Self::with_tip_oracle(config, TipOracle::stub_with_fallback(fallback, None))
    }

    fn with_tip_oracle(config: StrategyConfig, tip_oracle: TipOracle) -> Self {
        Self {
            config,
            tip_oracle,
            dedup: Mutex::new(DedupState::default()),
            accepted_count: AtomicU64::new(0),
            rejected_count: AtomicU64::new(0),
        }
    }

    pub fn evaluate(&self, opportunity: &Opportunity) -> Option<BundleCandidate> {
        if opportunity.estimated_edge_bps < self.config.min_edge_bps {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            debug!(
                seq = opportunity.seq,
                edge_bps = opportunity.estimated_edge_bps,
                min_edge = self.config.min_edge_bps,
                "opportunity rejected: insufficient edge"
            );
            return None;
        }

        let age = opportunity.created_at.elapsed();
        if age > self.config.max_opportunity_staleness {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            debug!(
                seq = opportunity.seq,
                ?age,
                max_age = ?self.config.max_opportunity_staleness,
                "opportunity rejected: stale"
            );
            return None;
        }

        if !self.track_signature(&opportunity.trade.signature) {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            debug!(
                seq = opportunity.seq,
                signature = %opportunity.trade.signature,
                limit = self.config.max_duplicates_per_signature,
                "opportunity rejected: duplicate signature"
            );
            return None;
        }

        let route = match opportunity.route_plan.clone() {
            Some(route) => route,
            None => {
                self.rejected_count.fetch_add(1, Ordering::Relaxed);
                debug!(
                    seq = opportunity.seq,
                    "opportunity rejected: missing route plan"
                );
                return None;
            }
        };

        if route.legs.is_empty() {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            debug!(
                seq = opportunity.seq,
                "opportunity rejected: empty route plan"
            );
            return None;
        }

        if route.legs.len() > self.config.max_route_hops {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            debug!(
                seq = opportunity.seq,
                hop_count = route.legs.len(),
                max_hops = self.config.max_route_hops,
                "opportunity rejected: route exceeds hop budget"
            );
            return None;
        }

        if route.start_mint != route.end_mint {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            debug!(
                seq = opportunity.seq,
                start_mint = %route.start_mint,
                end_mint = %route.end_mint,
                "opportunity rejected: non-circular route"
            );
            return None;
        }

        let first_leg = route.legs.first().expect("validated non-empty route");
        let estimated_final = route.estimated_final_output_amount.unwrap_or(0);
        if estimated_final <= first_leg.input_amount {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            debug!(
                seq = opportunity.seq,
                input_amount = first_leg.input_amount,
                estimated_final,
                "opportunity rejected: non-positive profit"
            );
            return None;
        }

        let profit_native = estimated_final - first_leg.input_amount;
        let profit_lamports = lamports_if_wsol(&route.start_mint, profit_native);
        if let Some(lamports) = profit_lamports {
            if lamports < self.config.min_profit_lamports {
                self.rejected_count.fetch_add(1, Ordering::Relaxed);
                debug!(
                    seq = opportunity.seq,
                    profit_lamports = lamports,
                    min_profit_lamports = self.config.min_profit_lamports,
                    "opportunity rejected: profit below threshold"
                );
                return None;
            }
        }

        let mut candidate = BundleCandidate::from_opportunity(opportunity);
        if let Some((tip_lamports, tip_account)) = self
            .tip_oracle
            .recommended_tip(&self.config, profit_lamports)
        {
            if let Some(lamports) = profit_lamports {
                let carry_fee = tip_carry_fee(&route.start_mint, tip_lamports);
                let net_profit = lamports.saturating_sub(tip_lamports.saturating_add(carry_fee));
                if net_profit < self.config.min_profit_lamports {
                    self.rejected_count.fetch_add(1, Ordering::Relaxed);
                    debug!(
                        seq = opportunity.seq,
                        profit_lamports = lamports,
                        net_profit_lamports = net_profit,
                        tip_lamports,
                        carry_fee,
                        min_profit_lamports = self.config.min_profit_lamports,
                        "opportunity rejected: net profit below threshold after tip"
                    );
                    return None;
                }
            }
            candidate.tip_lamports = Some(tip_lamports);
            candidate.tip_account = Some(tip_account);
        }

        self.accepted_count.fetch_add(1, Ordering::Relaxed);
        Some(candidate)
    }

    pub fn on_arbitrage_signal(&self, opportunity: &ArbitrageOpportunity) {
        let route = opportunity
            .path
            .legs
            .iter()
            .map(|leg| leg.pool_id.as_str())
            .collect::<Vec<_>>()
            .join("->");
        debug!(gain = opportunity.gain, %route, "strategy received arbitrage signal");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::types::{PoolQuoteDirection, RouteLegPlan, RoutePlan, TradeUpdate};
    use crate::template::TemplateContext;

    const TEST_INPUT_LAMPORTS: u64 = 1_000_000_000;
    const TEST_PROFIT_LAMPORTS: u64 = 50_000_000;

    fn opportunity_with(edge_bps: f64, signature: &str, age: Duration) -> Opportunity {
        Opportunity {
            seq: 0,
            trade: TradeUpdate {
                seq: 0,
                slot: 0,
                program_id: "program".into(),
                pool_id: "pool".into(),
                signature: signature.into(),
                buy_mint: WSOL_MINT.into(),
                sell_mint: WSOL_MINT.into(),
                buy_amount: TEST_INPUT_LAMPORTS + TEST_PROFIT_LAMPORTS,
                sell_amount: TEST_INPUT_LAMPORTS,
            },
            pool_snapshot: None,
            estimated_edge_bps: edge_bps,
            created_at: Instant::now() - age,
            route_plan: Some(sample_route_plan()),
        }
    }

    fn sample_route_plan() -> RoutePlan {
        RoutePlan {
            start_mint: WSOL_MINT.into(),
            end_mint: WSOL_MINT.into(),
            legs: vec![RouteLegPlan {
                pool_id: "pool".into(),
                program_id: "program".into(),
                input_mint: WSOL_MINT.into(),
                output_mint: WSOL_MINT.into(),
                input_amount: TEST_INPUT_LAMPORTS,
                estimated_output_amount: TEST_INPUT_LAMPORTS + TEST_PROFIT_LAMPORTS,
                effective_price: 1.0,
                fee_bps: 30,
                estimated_slippage_bps: 1.0,
                direction: PoolQuoteDirection::BaseToQuote,
                sqrt_price_limit: None,
                min_output_amount: None,
                limit_price: None,
                price_impact_limit_bps: None,
                context: TemplateContext::default(),
                template_ready: false,
                alt_ready: false,
            }],
            estimated_final_output_amount: Some(TEST_INPUT_LAMPORTS + TEST_PROFIT_LAMPORTS),
        }
    }

    #[test]
    fn rejects_low_edge_opportunities() {
        let engine = StrategyEngine::with_config(StrategyConfig {
            min_edge_bps: 5.0,
            ..StrategyConfig::default()
        });
        let opportunity = opportunity_with(1.0, "sig-low", Duration::from_millis(10));
        assert!(engine.evaluate(&opportunity).is_none());
    }

    #[test]
    fn rejects_stale_opportunities() {
        let engine = StrategyEngine::with_config(StrategyConfig {
            max_opportunity_staleness: Duration::from_millis(100),
            ..StrategyConfig::default()
        });
        let opportunity = opportunity_with(10.0, "sig-stale", Duration::from_millis(500));
        assert!(engine.evaluate(&opportunity).is_none());
    }

    #[test]
    fn enforces_signature_duplicate_limit() {
        let engine = StrategyEngine::with_config(StrategyConfig {
            max_duplicates_per_signature: 2,
            ..StrategyConfig::default()
        });
        let fresh = Duration::from_millis(10);
        assert!(engine
            .evaluate(&opportunity_with(10.0, "dup", fresh))
            .is_some());
        assert!(engine
            .evaluate(&opportunity_with(10.0, "dup", fresh))
            .is_some());
        assert!(engine
            .evaluate(&opportunity_with(10.0, "dup", fresh))
            .is_none());
    }
}

impl StrategyEngine {
    fn track_signature(&self, signature: &str) -> bool {
        let mut dedup = self.dedup.lock().expect("dedup mutex poisoned");
        Self::cleanup_signature_counts(&mut dedup);
        let count = dedup
            .signature_counts
            .entry(signature.to_string())
            .or_default();
        if *count >= self.config.max_duplicates_per_signature {
            return false;
        }
        *count += 1;
        true
    }

    fn cleanup_signature_counts(state: &mut DedupState) {
        const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);
        if Instant::now().duration_since(state.last_cleanup) < CLEANUP_INTERVAL {
            return;
        }
        state.signature_counts.clear();
        state.last_cleanup = Instant::now();
    }
}

fn lamports_if_wsol(mint: &str, amount: u64) -> Option<u64> {
    if mint == WSOL_MINT {
        Some(amount)
    } else {
        None
    }
}

fn tip_carry_fee(start_mint: &str, tip_lamports: u64) -> u64 {
    if start_mint == WSOL_MINT {
        compute_flash_loan_fee(tip_lamports)
    } else {
        0
    }
}

struct TipOracle {
    state: Arc<TipOracleState>,
    recipients: &'static [Pubkey],
    cursor: AtomicUsize,
    wallet_cap: Option<WalletTipCap>,
}

impl TipOracle {
    fn from_url(
        url: &str,
        fallback_lamports: u64,
        wallet_cap: Option<WalletTipCap>,
    ) -> Result<Self> {
        if url.trim().is_empty() {
            return Ok(Self::stub_with_fallback(fallback_lamports, wallet_cap));
        }
        let state = Arc::new(TipOracleState::new(fallback_lamports));
        spawn_tip_fetcher(url.to_string(), Arc::clone(&state))?;
        Ok(Self::with_state(state, wallet_cap))
    }

    fn stub_with_fallback(fallback_lamports: u64, wallet_cap: Option<WalletTipCap>) -> Self {
        Self::with_state(Arc::new(TipOracleState::new(fallback_lamports)), wallet_cap)
    }

    fn with_state(state: Arc<TipOracleState>, wallet_cap: Option<WalletTipCap>) -> Self {
        Self {
            state,
            recipients: TIP_RECIPIENTS.as_slice(),
            cursor: AtomicUsize::new(0),
            wallet_cap,
        }
    }

    fn recommended_tip(
        &self,
        config: &StrategyConfig,
        profit_lamports: Option<u64>,
    ) -> Option<(u64, Pubkey)> {
        if self.recipients.is_empty() {
            return None;
        }

        let mut floor = self.state.floor_lamports.load(Ordering::Relaxed);
        if floor < config.tip_floor_min_lamports {
            floor = config.tip_floor_min_lamports;
        }
        let mut tip = floor.max(config.min_tip_lamports);
        let markup = floor + floor.saturating_mul(config.tip_floor_markup_bps) / 10_000;
        tip = tip.max(markup);

        if let Some(profit) = profit_lamports {
            let share = profit.saturating_mul(config.tip_profit_share_bps) / 10_000;
            tip = tip.max(share);
        }

        let pressure_multiplier = tip_pressure::congestion_multiplier(
            config.tip_congestion_target_latency_ms,
            config.tip_congestion_max_multiplier,
            config.tip_congestion_expired_weight_bps,
        );
        telemetry::record_tip_multiplier(pressure_multiplier);
        if pressure_multiplier.is_finite() && pressure_multiplier > 1.0 {
            let scaled = (tip as f64) * pressure_multiplier;
            if scaled.is_finite() && scaled > 0.0 {
                tip = scaled.min(u64::MAX as f64).round() as u64;
            }
        }

        tip = tip.clamp(config.min_tip_lamports, config.max_tip_lamports);
        if tip == 0 {
            return None;
        }

        if let Some(cap) = self.wallet_cap.as_ref() {
            match cap.apply(tip) {
                Some(clamped) if clamped > 0 => tip = clamped,
                _ => return None,
            }
        }

        let idx = self.cursor.fetch_add(1, Ordering::Relaxed);
        let recipient = self.recipients[idx % self.recipients.len()];
        Some((tip, recipient))
    }
}

#[derive(Clone)]
struct WalletTipCap {
    tracker: Arc<WalletBalanceTracker>,
    max_bps: u64,
}

impl WalletTipCap {
    fn new(max_bps: u64, tracker: Arc<WalletBalanceTracker>) -> Self {
        Self { tracker, max_bps }
    }

    fn apply(&self, tip: u64) -> Option<u64> {
        let balance = self.tracker.current();
        if balance == 0 {
            return None;
        }
        let cap = balance
            .saturating_mul(self.max_bps)
            .checked_div(10_000)
            .unwrap_or(u64::MAX);
        if cap == 0 {
            return None;
        }
        Some(tip.min(cap))
    }
}

struct TipOracleState {
    floor_lamports: AtomicU64,
}

impl TipOracleState {
    fn new(initial: u64) -> Self {
        Self {
            floor_lamports: AtomicU64::new(initial),
        }
    }

    fn update_floor(&self, lamports: u64) -> bool {
        let previous = self.floor_lamports.swap(lamports, Ordering::Relaxed);
        previous != lamports
    }
}

struct WalletBalanceTracker {
    balance: AtomicU64,
}

impl WalletBalanceTracker {
    fn spawn(payer: Pubkey, rpc_urls: Vec<String>) -> Result<Arc<Self>> {
        anyhow::ensure!(
            !rpc_urls.is_empty(),
            "wallet_tip_cap_bps requires at least one RPC endpoint"
        );
        let tracker = Arc::new(Self {
            balance: AtomicU64::new(0),
        });
        let urls = rpc_urls;
        let handle_tracker = Arc::clone(&tracker);
        thread::Builder::new()
            .name("wallet_balance_monitor".into())
            .spawn(move || {
                let timeout = Duration::from_secs(WALLET_BALANCE_RPC_TIMEOUT_SECS);
                let mut idx = 0usize;
                loop {
                    let url = &urls[idx % urls.len()];
                    let client = RpcClient::new_with_timeout(url.clone(), timeout);
                    match client.get_balance(&payer) {
                        Ok(balance) => {
                            handle_tracker.balance.store(balance, Ordering::Relaxed);
                        }
                        Err(err) => {
                            warn!(rpc = %url, error = %err, "wallet balance refresh failed");
                        }
                    }
                    idx = idx.wrapping_add(1);
                    thread::sleep(Duration::from_secs(WALLET_BALANCE_POLL_INTERVAL_SECS));
                }
            })
            .context("failed to spawn wallet balance monitor")?;
        Ok(tracker)
    }

    fn current(&self) -> u64 {
        self.balance.load(Ordering::Relaxed)
    }
}

fn spawn_tip_fetcher(url: String, state: Arc<TipOracleState>) -> Result<()> {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    thread::Builder::new()
        .name("tip_oracle".into())
        .spawn(move || {
            let client = match BlockingClient::builder()
                .timeout(Duration::from_secs(TIP_FETCH_TIMEOUT_SECS))
                .build()
            {
                Ok(client) => {
                    let _ = ready_tx.send(Ok(()));
                    client
                }
                Err(err) => {
                    let _ = ready_tx.send(Err(err));
                    return;
                }
            };
            poll_tip_floor(client, url, state);
        })
        .context("failed to spawn tip oracle thread")?;

    match ready_rx.recv() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err).context("failed to build tip oracle HTTP client"),
        Err(_) => Err(anyhow!("tip oracle thread failed to initialize")),
    }
}

fn poll_tip_floor(client: BlockingClient, url: String, state: Arc<TipOracleState>) {
    loop {
        let response = client
            .get(&url)
            .send()
            .and_then(|resp| resp.error_for_status())
            .and_then(|resp| resp.json::<Vec<TipFloorEntry>>());

        match response {
            Ok(entries) => {
                if let Some(sol) = extract_floor_sol(&entries) {
                    let lamports = sol_to_lamports(sol);
                    if lamports > 0 && state.update_floor(lamports) {
                        info!(
                            tip_floor_sol = sol,
                            tip_floor_lamports = lamports,
                            "updated tip floor from Jito"
                        );
                    }
                }
            }
            Err(err) => {
                warn!(error = %err, "failed to refresh Jito tip floor");
            }
        }

        thread::sleep(Duration::from_secs(TIP_POLL_INTERVAL_SECS));
    }
}

#[derive(Debug, Deserialize)]
struct TipFloorEntry {
    landed_tips_25th_percentile: Option<f64>,
    landed_tips_50th_percentile: Option<f64>,
    landed_tips_75th_percentile: Option<f64>,
    ema_landed_tips_50th_percentile: Option<f64>,
}

fn extract_floor_sol(entries: &[TipFloorEntry]) -> Option<f64> {
    let entry = entries.first()?;
    entry
        .ema_landed_tips_50th_percentile
        .or(entry.landed_tips_75th_percentile)
        .or(entry.landed_tips_50th_percentile)
        .or(entry.landed_tips_25th_percentile)
}

fn sol_to_lamports(sol: f64) -> u64 {
    if sol <= 0.0 || !sol.is_finite() {
        return 0;
    }
    let value = (sol * LAMPORTS_PER_SOL as f64).round();
    value.clamp(0.0, u64::MAX as f64) as u64
}
