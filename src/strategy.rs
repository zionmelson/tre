use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::stages::{ArbitrageOpportunity, BundleCandidate, Opportunity};

const DEFAULT_MIN_EDGE_BPS: f64 = 2.5;
const DEFAULT_MAX_STALE_MS: u64 = 750;
const DEFAULT_MAX_DUPLICATES_PER_SIGNATURE: usize = 2;

#[derive(Clone, Debug)]
pub struct StrategyConfig {
    pub min_edge_bps: f64,
    pub max_opportunity_staleness: Duration,
    pub max_duplicates_per_signature: usize,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            min_edge_bps: DEFAULT_MIN_EDGE_BPS,
            max_opportunity_staleness: Duration::from_millis(DEFAULT_MAX_STALE_MS),
            max_duplicates_per_signature: DEFAULT_MAX_DUPLICATES_PER_SIGNATURE,
        }
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
    dedup: Mutex<DedupState>,
    accepted_count: AtomicU64,
    rejected_count: AtomicU64,
}

impl StrategyEngine {
    pub fn new() -> Self {
        Self::with_config(StrategyConfig::default())
    }

    pub fn with_config(config: StrategyConfig) -> Self {
        Self {
            config,
            dedup: Mutex::new(DedupState::default()),
            accepted_count: AtomicU64::new(0),
            rejected_count: AtomicU64::new(0),
        }
    }

    pub fn evaluate(&self, opportunity: &Opportunity) -> Option<BundleCandidate> {
        if opportunity.estimated_edge_bps < self.config.min_edge_bps {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
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
            tracing::debug!(
                seq = opportunity.seq,
                ?age,
                max_age = ?self.config.max_opportunity_staleness,
                "opportunity rejected: stale"
            );
            return None;
        }

        if !self.track_signature(&opportunity.trade.signature) {
            self.rejected_count.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                seq = opportunity.seq,
                signature = %opportunity.trade.signature,
                limit = self.config.max_duplicates_per_signature,
                "opportunity rejected: duplicate signature"
            );
            return None;
        }

        self.accepted_count.fetch_add(1, Ordering::Relaxed);
        Some(BundleCandidate::from_opportunity(opportunity))
    }

    pub fn on_arbitrage_signal(&self, opportunity: &ArbitrageOpportunity) {
        let route = opportunity
            .path
            .legs
            .iter()
            .map(|leg| leg.pool_id.as_str())
            .collect::<Vec<_>>()
            .join("->");
        tracing::debug!(gain = opportunity.gain, %route, "strategy received arbitrage signal");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::types::TradeUpdate;

    fn opportunity_with(edge_bps: f64, signature: &str, age: Duration) -> Opportunity {
        Opportunity {
            seq: 0,
            trade: TradeUpdate {
                seq: 0,
                slot: 0,
                program_id: "program".into(),
                pool_id: "pool".into(),
                signature: signature.into(),
                buy_mint: "A".into(),
                sell_mint: "B".into(),
                buy_amount: 100,
                sell_amount: 50,
            },
            pool_snapshot: None,
            estimated_edge_bps: edge_bps,
            created_at: Instant::now() - age,
            route_plan: None,
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
