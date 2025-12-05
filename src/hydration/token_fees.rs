use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022::extension::{
    transfer_fee::{TransferFee, TransferFeeConfig},
    transfer_hook, BaseStateWithExtensions, StateWithExtensionsOwned,
};
use spl_token_2022::state::Mint as SplToken2022Mint;
use spl_token_2022::ID as SPL_TOKEN_2022_PROGRAM_ID;
use tracing::warn;

use crate::hydration::adapters::AsyncHydrator;

#[derive(Clone, Copy, Debug)]
pub struct TokenTransferFee {
    fee: TransferFee,
}

impl TokenTransferFee {
    pub fn new(fee: TransferFee) -> Self {
        Self { fee }
    }

    pub fn post_fee_amount(&self, pre_fee_amount: u64) -> Option<(u64, u64)> {
        let fee_amount = self.fee.calculate_fee(pre_fee_amount)?;
        let net = pre_fee_amount.checked_sub(fee_amount)?;
        Some((net, fee_amount))
    }

    pub fn pre_fee_amount(&self, post_fee_amount: u64) -> Option<(u64, u64)> {
        let gross = self.fee.calculate_pre_fee_amount(post_fee_amount)?;
        let fee_amount = gross.checked_sub(post_fee_amount)?;
        Some((gross, fee_amount))
    }
}

pub fn apply_post_fee_amount(amount: u64, fee: Option<TokenTransferFee>) -> Option<(u64, u64)> {
    match fee {
        Some(fee) => fee.post_fee_amount(amount),
        None => Some((amount, 0)),
    }
}

pub fn apply_pre_fee_amount(amount: u64, fee: Option<TokenTransferFee>) -> Option<(u64, u64)> {
    match fee {
        Some(fee) => fee.pre_fee_amount(amount),
        None => Some((amount, 0)),
    }
}

#[derive(Clone, Debug)]
struct TransferFeeMeta {
    fee: Option<TokenTransferFee>,
    transfer_hook_program_id: Option<Pubkey>,
    hydrated_at: Instant,
}

pub struct TokenTransferFeeHydrator {
    hydrator: AsyncHydrator<Pubkey, TransferFeeMeta>,
    retry_backoff: Duration,
    last_attempts: DashMap<Pubkey, Instant>,
}

impl TokenTransferFeeHydrator {
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
        let hydrator = AsyncHydrator::new("token_fee", endpoint_count, move |mint| {
            fetch_token_transfer_fee_meta(fetch_clients.as_ref(), mint)
        });

        Some(Self {
            hydrator,
            retry_backoff: Duration::from_millis(hydration_retry_ms.max(1)),
            last_attempts: DashMap::new(),
        })
    }

    pub fn hydrate(&self, mint: Pubkey) -> Option<TokenTransferFee> {
        if let Some(meta) = self.hydrator.cached(&mint) {
            if meta.hydrated_at.elapsed() >= self.retry_backoff && self.should_schedule(mint) {
                self.hydrator.schedule(mint);
            }
            return meta.fee;
        }
        if self.should_schedule(mint) {
            self.hydrator.schedule(mint);
        }
        None
    }

    pub fn transfer_hook_program(&self, mint: Pubkey) -> Option<Pubkey> {
        self.hydrator
            .cached(&mint)
            .and_then(|meta| meta.transfer_hook_program_id)
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

fn fetch_token_transfer_fee_meta(
    clients: &[Arc<RpcClient>],
    mint: Pubkey,
) -> Option<TransferFeeMeta> {
    for client in clients {
        match client.get_account(&mint) {
            Ok(account) => {
                let owner = Pubkey::new_from_array(account.owner.to_bytes());
                let token_program = Pubkey::new_from_array(SPL_TOKEN_2022_PROGRAM_ID.to_bytes());
                if owner != token_program {
                    return Some(TransferFeeMeta {
                        fee: None,
                        transfer_hook_program_id: None,
                        hydrated_at: Instant::now(),
                    });
                }
                let state =
                    StateWithExtensionsOwned::<SplToken2022Mint>::unpack(account.data.clone())
                        .ok()?;
                let transfer_hook_program_id = transfer_hook::get_program_id(&state)
                    .map(|program| Pubkey::new_from_array(program.to_bytes()));
                match client.get_epoch_info() {
                    Ok(epoch_info) => {
                        let fee = parse_transfer_fee_extension(&state, epoch_info.epoch);
                        return Some(TransferFeeMeta {
                            fee,
                            transfer_hook_program_id,
                            hydrated_at: Instant::now(),
                        });
                    }
                    Err(error) => {
                        warn!(
                            target = "hydration",
                            mint = %mint,
                            %error,
                            "failed to fetch epoch info for token fee metadata"
                        );
                    }
                }
            }
            Err(error) => {
                warn!(
                    target = "hydration",
                    mint = %mint,
                    %error,
                    "failed to fetch mint account for token fee metadata"
                );
            }
        }
    }

    None
}

fn parse_transfer_fee_extension(
    state: &StateWithExtensionsOwned<SplToken2022Mint>,
    epoch: u64,
) -> Option<TokenTransferFee> {
    let config = state.get_extension::<TransferFeeConfig>().ok()?;
    let fee = *config.get_epoch_fee(epoch);
    Some(TokenTransferFee::new(fee))
}
