use std::collections::VecDeque;
use std::time::Duration;

use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;

use super::{math, sqrt_price_x64_from_x96};

#[derive(Clone, Debug)]
pub struct WhirlpoolTwapObservation {
    pub timestamp: u64,
    pub tick: i32,
    pub liquidity: u128,
    pub cumulative_tick: i128,
    pub cumulative_seconds_per_liquidity_x64: u128,
}

#[derive(Clone, Debug)]
pub struct WhirlpoolTwapEstimate {
    pub end_timestamp: u64,
    pub window_seconds: u64,
    pub average_tick_rounded: i32,
    pub average_price: Option<f64>,
    pub seconds_per_liquidity_x64: Option<u128>,
}

#[derive(Debug)]
pub struct WhirlpoolTwapCache {
    horizon: Duration,
    series: DashMap<Pubkey, WhirlpoolTwapSeries>,
}

impl Default for WhirlpoolTwapCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(
            super::DEFAULT_WHIRLPOOL_TWAP_HISTORY_SECS,
        ))
    }
}

impl WhirlpoolTwapCache {
    pub fn new(horizon: Duration) -> Self {
        let sanitized = if horizon.is_zero() {
            Duration::from_secs(1)
        } else {
            horizon
        };
        Self {
            horizon: sanitized,
            series: DashMap::new(),
        }
    }

    pub fn observe(
        &self,
        pool: Pubkey,
        timestamp: u64,
        tick: i32,
        liquidity: u128,
    ) {
        if liquidity == 0 {
            return;
        }
        let mut entry = self
            .series
            .entry(pool)
            .or_insert_with(WhirlpoolTwapSeries::default);
        entry.record(
            self.horizon,
            timestamp,
            tick,
            liquidity,
        );
    }

    pub fn twap(&self, pool: &Pubkey, lookback: Duration) -> Option<WhirlpoolTwapEstimate> {
        let series = self.series.get(pool)?;
        series.twap(if lookback.is_zero() {
            Duration::from_secs(1)
        } else {
            lookback
        })
    }
}

#[derive(Debug, Default)]
struct WhirlpoolTwapSeries {
    observations: VecDeque<WhirlpoolTwapObservation>,
}

impl WhirlpoolTwapSeries {
    fn record(
        &mut self,
        horizon: Duration,
        timestamp: u64,
        tick: i32,
        liquidity: u128,
    ) {
        let mut effective_timestamp = timestamp;
        let (cumulative_tick, cumulative_seconds_per_liquidity_x64) = if let Some(prev) =
            self.observations.back()
        {
            if effective_timestamp <= prev.timestamp {
                effective_timestamp = prev.timestamp.saturating_add(1);
            }
            let delta_seconds = effective_timestamp.saturating_sub(prev.timestamp);
            let tick_delta = tick_duration(prev.tick, delta_seconds);
            let seconds_per_liquidity = seconds_per_liquidity_x64(delta_seconds, prev.liquidity);
            (
                prev.cumulative_tick.saturating_add(tick_delta),
                prev.cumulative_seconds_per_liquidity_x64
                    .saturating_add(seconds_per_liquidity),
            )
        } else {
            (0, 0)
        };

        self.observations.push_back(WhirlpoolTwapObservation {
            timestamp: effective_timestamp,
            tick,
            liquidity,
            cumulative_tick,
            cumulative_seconds_per_liquidity_x64,
        });
        self.prune(horizon, effective_timestamp);
    }

    fn prune(&mut self, horizon: Duration, latest_timestamp: u64) {
        if self.observations.len() <= 1 {
            return;
        }
        let cutoff = latest_timestamp.saturating_sub(horizon.as_secs());
        while self.observations.len() > 1 {
            let remove = match self.observations.front() {
                Some(front) => front.timestamp < cutoff,
                None => false,
            };
            if remove {
                self.observations.pop_front();
            } else {
                break;
            }
        }
    }

    fn twap(&self, lookback: Duration) -> Option<WhirlpoolTwapEstimate> {
        let end = self.observations.back()?.clone();
        if self.observations.len() == 1 {
            return None;
        }
        let lookback_secs = lookback.as_secs();
        let target_timestamp = end.timestamp.saturating_sub(lookback_secs);
        let mut start = self.observations.front()?.clone();
        for obs in self.observations.iter() {
            if obs.timestamp <= target_timestamp {
                start = obs.clone();
            } else {
                break;
            }
        }
        let elapsed = end.timestamp.saturating_sub(start.timestamp);
        if elapsed == 0 {
            return None;
        }
        let delta_tick = end.cumulative_tick.saturating_sub(start.cumulative_tick);
        let average_tick = delta_tick as f64 / elapsed as f64;
        let average_tick_rounded = round_tick(average_tick);
        let seconds_per_liquidity_x64 = end
            .cumulative_seconds_per_liquidity_x64
            .saturating_sub(start.cumulative_seconds_per_liquidity_x64);
        let average_price = tick_to_price(average_tick_rounded);
        Some(WhirlpoolTwapEstimate {
            end_timestamp: end.timestamp,
            window_seconds: elapsed,
            average_tick_rounded,
            average_price,
            seconds_per_liquidity_x64: Some(seconds_per_liquidity_x64),
        })
    }
}

fn tick_duration(tick: i32, seconds: u64) -> i128 {
    let seconds_i128 = i128::try_from(seconds).unwrap_or(i128::MAX);
    i128::from(tick).saturating_mul(seconds_i128)
}

fn seconds_per_liquidity_x64(delta_seconds: u64, liquidity: u128) -> u128 {
    if delta_seconds == 0 {
        return 0;
    }
    let numerator = (delta_seconds as u128) << 64;
    numerator / liquidity.max(1)
}

fn round_tick(value: f64) -> i32 {
    if !value.is_finite() {
        return 0;
    }
    let rounded = value.round();
    rounded.clamp(i32::MIN as f64, i32::MAX as f64).trunc() as i32
}

fn tick_to_price(tick: i32) -> Option<f64> {
    let sqrt_price_x96 = math::sqrt_price_at_tick_x96(tick)?;
    let sqrt_price_x64 = sqrt_price_x64_from_x96(&sqrt_price_x96);
    let scale = (1u128 << 64) as f64;
    let ratio = (sqrt_price_x64 as f64) / scale;
    let price = ratio * ratio;
    if price.is_finite() && price > 0.0 {
        Some(price)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_twap_with_multiple_observations() {
        let cache = WhirlpoolTwapCache::new(Duration::from_secs(300));
        let pool = Pubkey::new_unique();
        cache.observe(pool, 100, 100, 1_000_000);
        cache.observe(pool, 160, 120, 1_000_000);
        let estimate = cache
            .twap(&pool, Duration::from_secs(30))
            .expect("missing twap");
        assert_eq!(estimate.window_seconds, 60);
        assert!(estimate.average_price.is_some());
        assert!(estimate.seconds_per_liquidity_x64.unwrap() > 0);
    }
}
