use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const ONE_BASIS_POINT: u64 = 10_000;
const EMA_ALPHA_BPS: u64 = 2_000; // 0.2 weight on the latest sample

struct TipPressureState {
    stage3_latency_ms_ema: AtomicU64,
    bundle_results_total: AtomicU64,
    bundle_expired_total: AtomicU64,
}

impl TipPressureState {
    const fn new() -> Self {
        Self {
            stage3_latency_ms_ema: AtomicU64::new(0),
            bundle_results_total: AtomicU64::new(0),
            bundle_expired_total: AtomicU64::new(0),
        }
    }

    fn update_latency(&self, observed_ms: u64) {
        if observed_ms == 0 {
            return;
        }
        let prev = self.stage3_latency_ms_ema.load(Ordering::Relaxed);
        if prev == 0 {
            self.stage3_latency_ms_ema
                .store(observed_ms.max(1), Ordering::Relaxed);
            return;
        }
        let alpha = EMA_ALPHA_BPS.min(ONE_BASIS_POINT);
        let weighted = ((prev as u128) * ((ONE_BASIS_POINT - alpha) as u128)
            + (observed_ms as u128) * (alpha as u128))
            / (ONE_BASIS_POINT as u128);
        self.stage3_latency_ms_ema
            .store((weighted as u64).max(1), Ordering::Relaxed);
    }

    fn expire_ratio(&self) -> f64 {
        let total = self.bundle_results_total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let expired = self.bundle_expired_total.load(Ordering::Relaxed);
        (expired as f64) / (total as f64)
    }

    fn ema_latency_ms(&self) -> u64 {
        self.stage3_latency_ms_ema.load(Ordering::Relaxed).max(1)
    }
}

static TIP_PRESSURE: OnceCell<TipPressureState> = OnceCell::new();

fn state() -> &'static TipPressureState {
    TIP_PRESSURE.get_or_init(TipPressureState::new)
}

pub fn record_stage3_latency(duration: Duration) {
    let ms = (duration.as_secs_f64() * 1_000.0) as u64;
    state().update_latency(ms);
}

pub fn record_bundle_result_expiry(is_expired: bool) {
    let st = state();
    st.bundle_results_total.fetch_add(1, Ordering::Relaxed);
    if is_expired {
        st.bundle_expired_total.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn congestion_multiplier(
    target_latency_ms: u64,
    max_multiplier: f64,
    expired_weight_bps: u64,
) -> f64 {
    if target_latency_ms == 0 {
        return 1.0;
    }
    let st = state();
    let ema_ms = st.ema_latency_ms();
    let mut multiplier = (ema_ms as f64) / (target_latency_ms as f64);
    if !multiplier.is_finite() || multiplier < 1.0 {
        multiplier = 1.0;
    }
    if expired_weight_bps > 0 {
        let ratio = st.expire_ratio();
        let penalty = ratio * (expired_weight_bps as f64 / ONE_BASIS_POINT as f64);
        multiplier *= 1.0 + penalty;
    }
    if max_multiplier.is_finite() && max_multiplier >= 1.0 {
        multiplier = multiplier.min(max_multiplier);
    }
    multiplier
}
