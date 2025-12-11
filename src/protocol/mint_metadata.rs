use dashmap::DashMap;
use solana_sdk::{pubkey, pubkey::Pubkey};

pub const SPL_TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

#[derive(Clone, Copy, Debug, Default)]
pub struct MintMetadata {
	pub program_id: Pubkey,
	pub decimals: Option<u8>,
}

#[derive(Default)]
pub struct MintMetadataHydrator {
	cache: DashMap<Pubkey, MintMetadata>,
}

impl MintMetadataHydrator {
	pub fn new(endpoints: &[String], _timeout_ms: u64) -> Option<Self> {
		if endpoints.is_empty() {
			None
		} else {
			Some(Self::default())
		}
	}

	pub fn hydrate(&self, mint: Pubkey) -> Option<MintMetadata> {
		self.cache.get(&mint).map(|entry| *entry)
	}

	#[allow(dead_code)]
	pub fn record_static(&self, mint: Pubkey, metadata: MintMetadata) {
		self.cache.insert(mint, metadata);
	}
}
