use crate::config::WalletsConfig;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use std::path::Path;
use std::sync::Arc;

/// Loaded Solana keypairs referenced throughout the application.
#[derive(Clone, Debug)]
pub struct LoadedWallets {
    auth: Arc<Keypair>,
    payer: Arc<Keypair>,
}

impl LoadedWallets {
    pub fn load(cfg: &WalletsConfig) -> Result<Self> {
        let auth = load_keypair(&cfg.auth.path)?;
        let payer = load_keypair(&cfg.payer.path)?;
        Ok(Self { auth, payer })
    }

    pub fn auth(&self) -> Arc<Keypair> {
        self.auth.clone()
    }

    pub fn payer(&self) -> Arc<Keypair> {
        self.payer.clone()
    }

    pub fn payer_pubkey(&self) -> Pubkey {
        self.payer.pubkey()
    }
}

fn load_keypair(path: &Path) -> Result<Arc<Keypair>> {
    let kp =
        read_keypair_file(path).map_err(|err| anyhow!("failed to read keypair {path:?}: {err}"))?;
    Ok(Arc::new(kp))
}
