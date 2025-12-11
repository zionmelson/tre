use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};

use crate::builder::templates::{AccountSpec, SwapMintRole, SwapTemplateDirection};
use crate::builder::{CpAmmPoolAccounts, LbClmmPoolAccounts, WhirlpoolPoolAccounts};
use crate::config::AppConfig;
use crate::telemetry;

#[derive(Clone)]
pub struct TemplateCache {
    inner: Arc<TemplateCacheState>,
}

struct TemplateCacheState {
    templates: DashMap<TemplateCacheKey, Arc<InstructionTemplate>>,
    latest: DashMap<(Pubkey, SwapTemplateDirection), TemplateCacheKey>,
    template_builds: AtomicU64,
}

impl TemplateCache {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let _ = config;
        Self {
            inner: Arc::new(TemplateCacheState {
                templates: DashMap::new(),
                latest: DashMap::new(),
                template_builds: AtomicU64::new(0),
            }),
        }
    }

    /// Inserts a template if it has not been seen before.
    pub fn insert(&self, template: InstructionTemplate) -> Arc<InstructionTemplate> {
        let key = template.key.clone();
        match self.inner.templates.entry(key.clone()) {
            DashEntry::Vacant(slot) => {
                self.inner.template_builds.fetch_add(1, Ordering::Relaxed);
                let arc = Arc::new(template);
                slot.insert(Arc::clone(&arc));
                self.inner
                    .latest
                    .insert((key.pool_id, key.direction), key);
                self.record_template_metrics();
                arc
            }
            DashEntry::Occupied(mut entry) => {
                Arc::clone(entry.get())
            }
        }
    }

    pub fn extend<I>(&self, templates: I) -> usize
    where
        I: IntoIterator<Item = InstructionTemplate>,
    {
        let mut count = 0usize;
        for template in templates {
            self.insert(template);
            count += 1;
        }
        count
    }

    pub fn get(&self, key: &TemplateCacheKey) -> Option<Arc<InstructionTemplate>> {
        self.inner
            .templates
            .get(key)
            .map(|entry| Arc::clone(entry.value()))
    }

    pub fn latest_key(
        &self,
        pool_id: &Pubkey,
        direction: SwapTemplateDirection,
    ) -> Option<TemplateCacheKey> {
        self.inner
            .latest
            .get(&(*pool_id, direction))
            .map(|entry| entry.value().clone())
    }

    pub fn latest_template(
        &self,
        pool_id: &Pubkey,
        direction: SwapTemplateDirection,
    ) -> Option<Arc<InstructionTemplate>> {
        let key = self.latest_key(pool_id, direction)?;
        self.get(&key)
    }

    pub fn template_count(&self) -> usize {
        self.inner.templates.len()
    }

    fn record_template_metrics(&self) {
        telemetry::record_template_build();
        telemetry::record_template_cache_entries(self.template_count());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TemplateCacheKey {
    pub pool_id: Pubkey,
    pub direction: SwapTemplateDirection,
    pub template_hash: [u8; 32],
}

impl TemplateCacheKey {
    pub fn new(
        pool_id: Pubkey,
        direction: SwapTemplateDirection,
        template_hash: [u8; 32],
    ) -> Self {
        Self {
            pool_id,
            direction,
            template_hash,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstructionTemplate {
    pub key: TemplateCacheKey,
    pub program_id: Pubkey,
    pub discriminator: [u8; 8],
    pub accounts: Arc<[AccountMeta]>,
    pub data: Arc<Vec<u8>>,
    pub patches: Arc<[TemplatePatch]>,
    pub cu_budget: u32,
    pub tlv_bytes: u16,
    pub template_hash: [u8; 32],
    pub alt_handle: Option<AltHandleId>,
    pub alt_accounts: Arc<[Pubkey]>,
    pub metadata: TemplateMetadata,
}

impl InstructionTemplate {
    pub fn from_amm_v3(params: AmmV3TemplateParams) -> Self {
        let AmmV3TemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps: _fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
            alt_accounts,
            account_specs,
            pool_accounts,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, template_hash),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
            alt_accounts: alt_accounts.into(),
            metadata: TemplateMetadata::AmmV3 {
                account_specs,
                pool_accounts,
            },
        }
    }

    pub fn from_cp_amm(params: CpAmmTemplateParams) -> Self {
        let CpAmmTemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps: _fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
            alt_accounts,
            account_specs,
            pool_accounts,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, template_hash),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
            alt_accounts: alt_accounts.into(),
            metadata: TemplateMetadata::CpAmm {
                account_specs,
                pool_accounts,
            },
        }
    }

    pub fn from_lb_clmm(params: LbClmmTemplateParams) -> Self {
        let LbClmmTemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps: _fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
            alt_accounts,
            account_specs,
            pool_accounts,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, template_hash),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
            alt_accounts: alt_accounts.into(),
            metadata: TemplateMetadata::LbClmm {
                account_specs,
                pool_accounts,
            },
        }
    }

    pub fn from_whirlpool(params: WhirlpoolTemplateParams) -> Self {
        let WhirlpoolTemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps: _fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
            alt_accounts,
            account_specs,
            pool_accounts,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, template_hash),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
            alt_accounts: alt_accounts.into(),
            metadata: TemplateMetadata::Whirlpool {
                account_specs,
                pool_accounts,
            },
        }
    }

}

#[derive(Debug, Clone)]
pub enum TemplateMetadata {
    None,
    AmmV3 {
        account_specs: &'static [AccountSpec],
        pool_accounts: Option<AmmV3PoolAccounts>,
    },
    CpAmm {
        account_specs: &'static [AccountSpec],
        pool_accounts: Option<CpAmmPoolAccounts>,
    },
    LbClmm {
        account_specs: &'static [AccountSpec],
        pool_accounts: Option<LbClmmPoolAccounts>,
    },
    Whirlpool {
        account_specs: &'static [AccountSpec],
        pool_accounts: Option<WhirlpoolPoolAccounts>,
    },
}

#[derive(Debug, Clone)]
pub struct AmmV3PoolAccounts {
    pub amm_config: Pubkey,
    pub observation_state: Pubkey,
    pub mint0: Pubkey,
    pub mint1: Pubkey,
    pub mint0_program: Pubkey,
    pub mint1_program: Pubkey,
    pub vault0: Pubkey,
    pub vault1: Pubkey,
}

impl AmmV3PoolAccounts {
    pub fn mint_for_role(&self, role: SwapMintRole) -> Pubkey {
        match role {
            SwapMintRole::Mint0 => self.mint0,
            SwapMintRole::Mint1 => self.mint1,
        }
    }

    pub fn token_program_for_role(&self, role: SwapMintRole) -> Pubkey {
        match role {
            SwapMintRole::Mint0 => self.mint0_program,
            SwapMintRole::Mint1 => self.mint1_program,
        }
    }

    pub fn vault_for_role(&self, role: SwapMintRole) -> Pubkey {
        match role {
            SwapMintRole::Mint0 => self.vault0,
            SwapMintRole::Mint1 => self.vault1,
        }
    }

    pub fn is_populated(&self) -> bool {
        const ZERO: Pubkey = Pubkey::new_from_array([0u8; 32]);
        self.amm_config != ZERO
            && self.observation_state != ZERO
            && self.mint0 != ZERO
            && self.mint1 != ZERO
            && self.mint0_program != ZERO
            && self.mint1_program != ZERO
            && self.vault0 != ZERO
            && self.vault1 != ZERO
    }
}

impl LbClmmPoolAccounts {
    pub fn mint_for_role(&self, role: SwapMintRole) -> Pubkey {
        match role {
            SwapMintRole::Mint0 => self.token_x_mint,
            SwapMintRole::Mint1 => self.token_y_mint,
        }
    }

    pub fn is_populated(&self) -> bool {
        const ZERO: Pubkey = Pubkey::new_from_array([0u8; 32]);
        self.token_x_mint != ZERO
            && self.token_y_mint != ZERO
            && self.token_x_program != ZERO
            && self.token_y_program != ZERO
            && self.reserve_x.is_some()
            && self.reserve_y.is_some()
            && self.oracle.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct TemplatePatch {
    pub offset: u16,
    pub length: u8,
    pub kind: TemplateFieldKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemplateFieldKind {
    AmmV3(AmmV3Field),
    CpAmm(CpAmmField),
    LbClmm(LbClmmField),
    Whirlpool(WhirlpoolField),
    Custom(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AmmV3Field {
    AmountSpecified,
    OtherAmountThreshold,
    SqrtPriceLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CpAmmField {
    AmountIn,
    MinimumOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LbClmmField {
    AmountIn,
    AmountOut,
    LimitPrice,
    PriceImpactLimit,
    RemainingAccountsInfo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WhirlpoolField {
    AmountIn,
    AmountOut,
    AmountSpecifiedIsInput,
    SqrtPriceLimit,
    RemainingAccountsInfo,
}

#[derive(Debug, Clone, Copy)]
pub struct TemplateSlice {
    pub offset: u16,
    pub length: u8,
}

impl TemplateSlice {
    pub const fn new(offset: u16, length: u8) -> Self {
        Self { offset, length }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AmmV3FieldLayout {
    pub amount_specified: TemplateSlice,
    pub other_amount_threshold: TemplateSlice,
    pub sqrt_price_limit: Option<TemplateSlice>,
}

impl AmmV3FieldLayout {
    fn into_patches(self) -> Vec<TemplatePatch> {
        let mut patches = Vec::with_capacity(3);
        patches.push(TemplatePatch {
            offset: self.amount_specified.offset,
            length: self.amount_specified.length,
            kind: TemplateFieldKind::AmmV3(AmmV3Field::AmountSpecified),
        });
        patches.push(TemplatePatch {
            offset: self.other_amount_threshold.offset,
            length: self.other_amount_threshold.length,
            kind: TemplateFieldKind::AmmV3(AmmV3Field::OtherAmountThreshold),
        });
        if let Some(slice) = self.sqrt_price_limit {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::AmmV3(AmmV3Field::SqrtPriceLimit),
            });
        }
        patches
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CpAmmFieldLayout {
    pub amount_in: TemplateSlice,
    pub minimum_output: TemplateSlice,
}

impl CpAmmFieldLayout {
    fn into_patches(self) -> Vec<TemplatePatch> {
        vec![
            TemplatePatch {
                offset: self.amount_in.offset,
                length: self.amount_in.length,
                kind: TemplateFieldKind::CpAmm(CpAmmField::AmountIn),
            },
            TemplatePatch {
                offset: self.minimum_output.offset,
                length: self.minimum_output.length,
                kind: TemplateFieldKind::CpAmm(CpAmmField::MinimumOutput),
            },
        ]
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LbClmmFieldLayout {
    pub amount_in: TemplateSlice,
    pub amount_out: Option<TemplateSlice>,
    pub limit_price: Option<TemplateSlice>,
    pub price_impact_bps: Option<TemplateSlice>,
    pub remaining_accounts_info: Option<TemplateSlice>,
}

impl LbClmmFieldLayout {
    fn into_patches(self) -> Vec<TemplatePatch> {
        let mut patches = Vec::with_capacity(5);
        patches.push(TemplatePatch {
            offset: self.amount_in.offset,
            length: self.amount_in.length,
            kind: TemplateFieldKind::LbClmm(LbClmmField::AmountIn),
        });
        if let Some(slice) = self.amount_out {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::LbClmm(LbClmmField::AmountOut),
            });
        }
        if let Some(slice) = self.limit_price {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::LbClmm(LbClmmField::LimitPrice),
            });
        }
        if let Some(slice) = self.price_impact_bps {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::LbClmm(LbClmmField::PriceImpactLimit),
            });
        }
        if let Some(slice) = self.remaining_accounts_info {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::LbClmm(LbClmmField::RemainingAccountsInfo),
            });
        }
        patches
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WhirlpoolFieldLayout {
    pub amount_in: TemplateSlice,
    pub amount_out: TemplateSlice,
    pub amount_specified_is_input: TemplateSlice,
    pub sqrt_price_limit: Option<TemplateSlice>,
    pub remaining_accounts_info: Option<TemplateSlice>,
}

impl WhirlpoolFieldLayout {
    fn into_patches(self) -> Vec<TemplatePatch> {
        let mut patches = Vec::with_capacity(5);
        patches.push(TemplatePatch {
            offset: self.amount_in.offset,
            length: self.amount_in.length,
            kind: TemplateFieldKind::Whirlpool(WhirlpoolField::AmountIn),
        });
        patches.push(TemplatePatch {
            offset: self.amount_out.offset,
            length: self.amount_out.length,
            kind: TemplateFieldKind::Whirlpool(WhirlpoolField::AmountOut),
        });
        patches.push(TemplatePatch {
            offset: self.amount_specified_is_input.offset,
            length: self.amount_specified_is_input.length,
            kind: TemplateFieldKind::Whirlpool(WhirlpoolField::AmountSpecifiedIsInput),
        });
        if let Some(slice) = self.sqrt_price_limit {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::Whirlpool(WhirlpoolField::SqrtPriceLimit),
            });
        }
        if let Some(slice) = self.remaining_accounts_info {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::Whirlpool(WhirlpoolField::RemainingAccountsInfo),
            });
        }
        patches
    }
}

#[derive(Debug, Clone)]
pub struct AmmV3TemplateParams {
    pub pool_id: Pubkey,
    pub program_id: Pubkey,
    pub direction: SwapTemplateDirection,
    pub fee_tier_bps: u16,
    pub discriminator: [u8; 8],
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
    pub field_layout: AmmV3FieldLayout,
    pub cu_budget: u32,
    pub tlv_bytes: u16,
    pub template_hash: [u8; 32],
    pub alt_handle: Option<AltHandleId>,
    pub alt_accounts: Vec<Pubkey>,
    pub account_specs: &'static [AccountSpec],
    pub pool_accounts: Option<AmmV3PoolAccounts>,
}

#[derive(Debug, Clone)]
pub struct CpAmmTemplateParams {
    pub pool_id: Pubkey,
    pub program_id: Pubkey,
    pub direction: SwapTemplateDirection,
    pub fee_tier_bps: u16,
    pub discriminator: [u8; 8],
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
    pub field_layout: CpAmmFieldLayout,
    pub cu_budget: u32,
    pub tlv_bytes: u16,
    pub template_hash: [u8; 32],
    pub alt_handle: Option<AltHandleId>,
    pub alt_accounts: Vec<Pubkey>,
    pub account_specs: &'static [AccountSpec],
    pub pool_accounts: Option<CpAmmPoolAccounts>,
}

#[derive(Debug, Clone)]
pub struct LbClmmTemplateParams {
    pub pool_id: Pubkey,
    pub program_id: Pubkey,
    pub direction: SwapTemplateDirection,
    pub fee_tier_bps: u16,
    pub discriminator: [u8; 8],
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
    pub field_layout: LbClmmFieldLayout,
    pub cu_budget: u32,
    pub tlv_bytes: u16,
    pub template_hash: [u8; 32],
    pub alt_handle: Option<AltHandleId>,
    pub alt_accounts: Vec<Pubkey>,
    pub account_specs: &'static [AccountSpec],
    pub pool_accounts: Option<LbClmmPoolAccounts>,
}

#[derive(Debug, Clone)]
pub struct WhirlpoolTemplateParams {
    pub pool_id: Pubkey,
    pub program_id: Pubkey,
    pub direction: SwapTemplateDirection,
    pub fee_tier_bps: u16,
    pub discriminator: [u8; 8],
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
    pub field_layout: WhirlpoolFieldLayout,
    pub cu_budget: u32,
    pub tlv_bytes: u16,
    pub template_hash: [u8; 32],
    pub alt_handle: Option<AltHandleId>,
    pub alt_accounts: Vec<Pubkey>,
    pub account_specs: &'static [AccountSpec],
    pub pool_accounts: Option<WhirlpoolPoolAccounts>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct AltHandleId(pub u32);
