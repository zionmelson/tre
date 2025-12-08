use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey, pubkey::Pubkey};
use spl_token_2022::state::Mint as SplToken2022Mint;
use spl_token_2022::{extension::StateWithExtensionsOwned, ID as SPL_TOKEN_2022_PROGRAM_ID};

use crate::hydration::adapters::hydration::{AsyncHydrator, HydratedValue};

pub const SPL_TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

#[derive(Clone, Debug)]
pub struct MintMetadata {
    pub decimals: Option<u8>,
    pub program_id: Pubkey,
    pub hydrated_at: Instant,
}

pub struct MintMetadataHydrator {
    hydrator: AsyncHydrator<Pubkey, MintMetadata>,
    retry_backoff: Duration,
    last_attempts: DashMap<Pubkey, Instant>,
}

impl MintMetadataHydrator {
    pub fn new(
        rpc_endpoints: &[String],
        rpc_timeout_ms: u64,
        hydration_retry_ms: u64,
    ) -> Option<Self> {
        if rpc_endpoints.is_empty() {
            return None;
        }

        let timeout = Duration::from_millis(rpc_timeout_ms.max(100));
        let client_list: Vec<Arc<RpcClient>> = rpc_endpoints
            .iter()
            .map(|url| Arc::new(RpcClient::new_with_timeout(url.clone(), timeout)))
            .collect();

        if client_list.is_empty() {
            return None;
        }

        let endpoint_count = client_list.len();
        let clients = Arc::new(client_list);
        let fetch_clients = clients.clone();
        let hydrator = AsyncHydrator::new("mint_meta", endpoint_count, move |mint| {
            fetch_mint_metadata(fetch_clients.as_ref(), mint)
                .map(|meta| HydratedValue::new(meta, None))
        });

        Some(Self {
            hydrator,
            retry_backoff: Duration::from_millis(hydration_retry_ms.max(1)),
            last_attempts: DashMap::new(),
        })
    }

    pub fn hydrate(&self, mint: Pubkey) -> Option<MintMetadata> {
        if let Some(meta) = self.hydrator.cached(&mint) {
            if meta.hydrated_at.elapsed() >= self.retry_backoff && self.should_schedule(mint) {
                self.hydrator.schedule(mint);
            }
            return Some(meta);
        }
        if self.should_schedule(mint) {
            self.hydrator.schedule(mint);
        }
        None
    }

    pub fn mint_program(&self, mint: Pubkey) -> Option<Pubkey> {
        self.hydrate(mint).map(|meta| meta.program_id)
    }

    fn should_schedule(&self, mint: Pubkey) -> bool {
        let now = Instant::now();
        if let Some(last) = self.last_attempts.get(&mint) {
            if now.duration_since(*last.value()) < self.retry_backoff {
                return false;
            }
        }
        self.last_attempts.insert(mint, now);
        true
    }
}

fn fetch_mint_metadata(clients: &[Arc<RpcClient>], mint: Pubkey) -> Option<MintMetadata> {
    for client in clients {
        match client.get_account(&mint) {
            Ok(account) => {
                let owner = account.owner;
                let decimals = parse_mint_decimals(&owner, &account.data);
                return Some(MintMetadata {
                    decimals,
                    program_id: owner,
                    hydrated_at: Instant::now(),
                });
            }
            Err(error) => {
                tracing::warn!(
                    target = "hydration::mint_metadata",
                    mint = %mint,
                    %error,
                    "failed to fetch mint account for metadata"
                );
            }
        }
    }

    None
}

fn parse_mint_decimals(owner: &Pubkey, data: &[u8]) -> Option<u8> {
    if owner == &SPL_TOKEN_PROGRAM_ID {
        return data.get(44).copied();
    }
    if owner == &SPL_TOKEN_2022_PROGRAM_ID {
        return StateWithExtensionsOwned::<SplToken2022Mint>::unpack(data.to_vec())
            .ok()
            .map(|mint| mint.base.decimals);
    }
    None
}
