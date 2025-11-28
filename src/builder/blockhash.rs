use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::hash::Hash;

const DEFAULT_TTL: Duration = Duration::from_millis(250);

pub struct BlockhashCache {
    client: RpcClient,
    ttl: Duration,
    state: Mutex<Option<CachedHash>>,
}

struct CachedHash {
    hash: Hash,
    slot: u64,
    fetched_at: Instant,
}

impl BlockhashCache {
    pub fn new(endpoint: String) -> Self {
        Self {
            client: RpcClient::new(endpoint),
            ttl: DEFAULT_TTL,
            state: Mutex::new(None),
        }
    }

    pub fn latest(&self) -> Result<Hash> {
        let current_slot = self.client.get_slot()?;
        if let Some(hash) = self.cached_hash(current_slot) {
            return Ok(hash);
        }
        self.refresh(current_slot)
    }

    fn cached_hash(&self, slot: u64) -> Option<Hash> {
        let guard = self.state.lock().expect("blockhash cache lock poisoned");
        guard
            .as_ref()
            .filter(|entry| entry.slot == slot && entry.fetched_at.elapsed() <= self.ttl)
            .map(|entry| entry.hash)
    }

    fn refresh(&self, slot: u64) -> Result<Hash> {
        let hash = self.client.get_latest_blockhash()?;
        let mut guard = self.state.lock().expect("blockhash cache lock poisoned");
        *guard = Some(CachedHash {
            hash,
            slot,
            fetched_at: Instant::now(),
        });
        Ok(hash)
    }
}
