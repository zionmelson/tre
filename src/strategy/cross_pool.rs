use std::collections::HashMap;
use std::sync::Mutex;

use tracing::debug;

use crate::{bundle::BundleRequest, corecast::Opportunity};

use super::ProtocolObservation;

const MIN_SPREAD_BPS: f64 = 25.0;
const MAX_SLOT_SKEW: u64 = 40;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PairKey {
    base: String,
    quote: String,
}

impl PairKey {
    fn new(base: &str, quote: &str) -> Self {
        Self {
            base: base.to_string(),
            quote: quote.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
struct PriceSample {
    price: f64,
    slot: u64,
    signature: String,
    liquidity: Option<f64>,
    canonical: bool,
}

#[derive(Default)]
struct PairState {
    quotes: HashMap<String, PriceSample>,
}

impl PairState {
    fn prune(&mut self, min_slot: u64) {
        self.quotes.retain(|_, sample| sample.slot >= min_slot);
    }

    fn record(
        &mut self,
        pair: &PairKey,
        slug: &str,
        sample: PriceSample,
        seq: u64,
    ) -> Option<BundleRequest> {
        let price = sample.price;
        let slot = sample.slot;
        let mut signal = None;

        for (other_slug, other) in self.quotes.iter() {
            if other_slug == slug {
                continue;
            }
            if slot.abs_diff(other.slot) > MAX_SLOT_SKEW {
                continue;
            }
            if other.price <= f64::EPSILON {
                continue;
            }
            let spread_bps = ((price - other.price) / other.price).abs() * 10_000.0;
            if spread_bps < MIN_SPREAD_BPS {
                continue;
            }

            debug!(
                target = "strategy::xpool",
                base = %pair.base,
                quote = %pair.quote,
                left = slug,
                right = other_slug,
                spread_bps,
                seq,
                left_liq = ?sample.liquidity,
                right_liq = ?other.liquidity,
                left_canonical = sample.canonical,
                right_canonical = other.canonical,
                "detected cross-pool spread"
            );

            signal = Some(BundleRequest {
                uuid: format!(
                    "xpool-{base}-{quote}-{left}-{right}-{seq:08x}",
                    base = pair.base,
                    quote = pair.quote,
                    left = slug,
                    right = other_slug,
                ),
            });
            break;
        }

        self.quotes.insert(slug.to_string(), sample);
        signal
    }
}

#[derive(Default)]
pub struct CrossPoolBook {
    pairs: Mutex<HashMap<PairKey, PairState>>,
}

impl CrossPoolBook {
    pub fn new() -> Self {
        Self {
            pairs: Mutex::new(HashMap::new()),
        }
    }

    pub fn observe(
        &self,
        opportunity: &Opportunity,
        seq: u64,
        canonical: Option<&ProtocolObservation>,
    ) -> Option<BundleRequest> {
        let (price, slot, liquidity, canonical_flag) = if let Some(obs) = canonical {
            (obs.price, obs.slot, Some(obs.liquidity), true)
        } else {
            let price = Self::implied_price(opportunity)?;
            (price, opportunity.slot, None, false)
        };

        if !price.is_finite() || price <= 0.0 {
            return None;
        }

        let slug = opportunity.protocol.slug().to_string();
        let pair_key = PairKey::new(&opportunity.base_mint, &opportunity.quote_mint);
        let sample = PriceSample {
            price,
            slot,
            signature: opportunity.signature.clone(),
            liquidity,
            canonical: canonical_flag,
        };

        let mut guard = self.pairs.lock().expect("cross-pool price book poisoned");
        let state = guard
            .entry(pair_key.clone())
            .or_insert_with(PairState::default);
        let min_slot = slot.saturating_sub(MAX_SLOT_SKEW * 2);
        state.prune(min_slot);
        state.record(&pair_key, &slug, sample, seq)
    }

    fn implied_price(opportunity: &Opportunity) -> Option<f64> {
        if opportunity.buy_amount == 0 || opportunity.sell_amount == 0 {
            return None;
        }
        let buy = opportunity.buy_amount as f64;
        let sell = opportunity.sell_amount as f64;
        Some(sell / buy)
    }
}
