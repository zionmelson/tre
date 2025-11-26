use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::AppConfig;

#[derive(Clone)]
pub struct AltManager {
    state: Arc<AltManagerState>,
}

struct AltManagerState {
    refresh_count: AtomicU64,
    last_refresh_unix_ms: AtomicU64,
}

impl AltManager {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let _ = config; // config hooks for ALT selection arrive later
        Self {
            state: Arc::new(AltManagerState {
                refresh_count: AtomicU64::new(0),
                last_refresh_unix_ms: AtomicU64::new(now_millis()),
            }),
        }
    }

    pub fn refresh_warm_cache(&self) {
        self.state.refresh_count.fetch_add(1, Ordering::Relaxed);
        self.state
            .last_refresh_unix_ms
            .store(now_millis(), Ordering::Relaxed);
    }

    pub fn stats(&self) -> AltManagerStats {
        AltManagerStats {
            refresh_count: self.state.refresh_count.load(Ordering::Relaxed),
            last_refresh_unix_ms: self.state.last_refresh_unix_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AltManagerStats {
    pub refresh_count: u64,
    pub last_refresh_unix_ms: u64,
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}
