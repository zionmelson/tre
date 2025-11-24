use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tonic::transport::Channel;

use crate::{config::AppConfig, proto::solana_corecast::core_cast_client::CoreCastClient};

#[derive(Clone)]
pub struct CorecastSubscriber {
    config: Arc<AppConfig>,
    sequence: Arc<AtomicU64>,
}

impl CorecastSubscriber {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self {
            config,
            sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn next_opportunity(&self) -> Option<Opportunity> {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        if seq == 0 {
            tracing::info!(
                target = "corecast",
                pools = self.config.tokens.len(),
                "CoreCast subscriber placeholder active"
            );
        }

        if seq & 0x7f == 0 {
            thread::sleep(Duration::from_micros(50));
            return None;
        }

        Some(Opportunity {
            signature: format!("synthetic-{seq:016x}"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub signature: String,
}

pub type CorecastClient = CoreCastClient<Channel>;
