use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct TxGraph {
    eval_heartbeat_unix_ms: Arc<AtomicU64>,
    submit_heartbeat_unix_ms: Arc<AtomicU64>,
}

impl TxGraph {
    pub fn new() -> Self {
        let now = now_millis();
        Self {
            eval_heartbeat_unix_ms: Arc::new(AtomicU64::new(now)),
            submit_heartbeat_unix_ms: Arc::new(AtomicU64::new(now)),
        }
    }

    pub fn mark_eval_heartbeat(&self) {
        self.eval_heartbeat_unix_ms
            .store(now_millis(), Ordering::Relaxed);
    }

    pub fn mark_submit_heartbeat(&self) {
        self.submit_heartbeat_unix_ms
            .store(now_millis(), Ordering::Relaxed);
    }

    pub fn last_eval_heartbeat(&self) -> u64 {
        self.eval_heartbeat_unix_ms.load(Ordering::Relaxed)
    }

    pub fn last_submit_heartbeat(&self) -> u64 {
        self.submit_heartbeat_unix_ms.load(Ordering::Relaxed)
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}
