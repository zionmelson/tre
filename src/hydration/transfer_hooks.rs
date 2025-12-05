use std::{collections::HashSet, sync::Arc, time::Duration};

use dashmap::DashMap;
use futures::{executor::block_on, future};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};
use spl_tlv_account_resolution::state::{AccountDataResult, ExtraAccountMetaList};
use spl_transfer_hook_interface::{
    get_extra_account_metas_address,
    instruction::{self, ExecuteInstruction},
    solana_instruction::AccountMeta as HookAccountMeta,
    solana_pubkey::Pubkey as HookPubkey,
};
use tracing::{debug, warn};

/// Resolves the on-chain accounts required to satisfy TransferHook CPI checks
/// for a specific mint/program pair.
pub struct TransferHookAccountsResolver {
    clients: Arc<Vec<Arc<RpcClient>>>,
    validate_cache: DashMap<(Pubkey, Pubkey), Vec<u8>>,
}

#[derive(Clone, Copy)]
pub struct TransferHookAccountContext {
    pub source: Pubkey,
    pub destination: Pubkey,
    pub authority: Pubkey,
}

impl TransferHookAccountsResolver {
    pub fn new(rpc_endpoints: &[String], rpc_timeout_ms: u64) -> Option<Self> {
        if rpc_endpoints.is_empty() {
            return None;
        }
        let timeout = Duration::from_millis(rpc_timeout_ms.max(100));
        let clients: Vec<Arc<RpcClient>> = rpc_endpoints
            .iter()
            .map(|url| Arc::new(RpcClient::new_with_timeout(url.clone(), timeout)))
            .collect();
        if clients.is_empty() {
            return None;
        }
        Some(Self {
            clients: Arc::new(clients),
            validate_cache: DashMap::new(),
        })
    }

    pub fn resolve_accounts(
        &self,
        program_id: Pubkey,
        mint: Pubkey,
        contexts: &[TransferHookAccountContext],
    ) -> Option<Vec<AccountMeta>> {
        let mut merged = Vec::new();
        let mut seen = HashSet::new();
        for ctx in contexts {
            if let Some(accounts) = self.resolve_for_context(program_id, mint, ctx) {
                for meta in accounts {
                    if seen.insert((meta.pubkey, meta.is_signer, meta.is_writable)) {
                        merged.push(meta);
                    }
                }
            }
        }
        if merged.is_empty() {
            None
        } else {
            Some(merged)
        }
    }

    fn resolve_for_context(
        &self,
        program_id: Pubkey,
        mint: Pubkey,
        ctx: &TransferHookAccountContext,
    ) -> Option<Vec<AccountMeta>> {
        let hook_program_id = sdk_to_hook_pubkey(&program_id);
        let hook_mint = sdk_to_hook_pubkey(&mint);
        let validate_hook_pubkey = get_extra_account_metas_address(&hook_mint, &hook_program_id);
        let validate_pubkey = hook_to_sdk_pubkey(&validate_hook_pubkey);
        let validate_data = self.validation_data(program_id, mint, validate_pubkey)?;

        let mut instruction = instruction::execute(
            &hook_program_id,
            &sdk_to_hook_pubkey(&ctx.source),
            &hook_mint,
            &sdk_to_hook_pubkey(&ctx.destination),
            &sdk_to_hook_pubkey(&ctx.authority),
            0,
        );
        instruction.accounts.push(HookAccountMeta::new_readonly(
            validate_hook_pubkey.clone(),
            false,
        ));

        let fetch_account_data = |hook_key: HookPubkey| {
            let sdk_key = hook_to_sdk_pubkey(&hook_key);
            future::ready::<AccountDataResult>(Ok(self.fetch_account_data(&sdk_key)))
        };

        if let Err(error) = block_on(ExtraAccountMetaList::add_to_instruction::<
            ExecuteInstruction,
            _,
            _,
        >(
            &mut instruction, fetch_account_data, &validate_data
        )) {
            warn!(
                target = "transfer_hook",
                program_id = %program_id,
                mint = %mint,
                %error,
                "failed to resolve transfer-hook extra accounts"
            );
            return None;
        }

        let mut extras: Vec<AccountMeta> = instruction.accounts[5..]
            .iter()
            .cloned()
            .map(hook_meta_to_sdk)
            .collect();
        extras.push(AccountMeta::new_readonly(program_id, false));
        Some(extras)
    }

    fn validation_data(
        &self,
        program_id: Pubkey,
        mint: Pubkey,
        validate_pubkey: Pubkey,
    ) -> Option<Vec<u8>> {
        if let Some(entry) = self.validate_cache.get(&(program_id, mint)) {
            return Some(entry.clone());
        }
        let data = self.fetch_account_data(&validate_pubkey)?;
        self.validate_cache.insert((program_id, mint), data.clone());
        Some(data)
    }

    fn fetch_account_data(&self, key: &Pubkey) -> Option<Vec<u8>> {
        for client in self.clients.iter() {
            match client.get_account_data(key) {
                Ok(data) => return Some(data),
                Err(error) => {
                    debug!(target = "transfer_hook", %key, %error, "rpc fetch failed for account");
                }
            }
        }
        warn!(
            target = "transfer_hook",
            %key,
            "unable to fetch account data for transfer hook resolver"
        );
        None
    }
}

fn sdk_to_hook_pubkey(key: &Pubkey) -> HookPubkey {
    HookPubkey::new_from_array(key.to_bytes())
}

fn hook_to_sdk_pubkey(key: &HookPubkey) -> Pubkey {
    Pubkey::new_from_array(key.to_bytes())
}

fn hook_meta_to_sdk(meta: HookAccountMeta) -> AccountMeta {
    AccountMeta {
        pubkey: hook_to_sdk_pubkey(&meta.pubkey),
        is_signer: meta.is_signer,
        is_writable: meta.is_writable,
    }
}
