use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};

use super::templates::SwapTemplateDirection;
use crate::config::AppConfig;

#[derive(Clone)]
pub struct TemplateCache {
    state: Arc<TemplateCacheState>,
}

struct TemplateCacheState {
    warm_calls: AtomicU64,
    last_refresh_unix_ms: AtomicU64,
    templates: DashMap<TemplateCacheKey, Arc<InstructionTemplate>>,
}

impl TemplateCache {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let _ = config; // config plumbing will drive template selection later
        Self {
            state: Arc::new(TemplateCacheState {
                warm_calls: AtomicU64::new(0),
                last_refresh_unix_ms: AtomicU64::new(now_millis()),
                templates: DashMap::new(),
            }),
        }
    }

    /// Pre-warms instruction templates so Stage 1 can build swap graphs without allocations.
    pub fn prewarm(&self) {
        self.state.warm_calls.fetch_add(1, Ordering::Relaxed);
        self.state
            .last_refresh_unix_ms
            .store(now_millis(), Ordering::Relaxed);
    }

    /// Inserts or replaces a template and returns the stored Arc handle.
    pub fn insert(&self, template: InstructionTemplate) -> Arc<InstructionTemplate> {
        let key = template.key.clone();
        let handle = Arc::new(template);
        self.state.templates.insert(key, handle.clone());
        handle
    }

    /// Loads a batch of templates, returning how many entries were inserted.
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

    /// Retrieves an instruction template for the provided key.
    pub fn get(&self, key: &TemplateCacheKey) -> Option<Arc<InstructionTemplate>> {
        self.state
            .templates
            .get(key)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Returns simple cache stats for observability.
    pub fn stats(&self) -> TemplateCacheStats {
        TemplateCacheStats {
            warm_calls: self.state.warm_calls.load(Ordering::Relaxed),
            last_refresh_unix_ms: self.state.last_refresh_unix_ms.load(Ordering::Relaxed),
            template_count: self.state.templates.len() as u64,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TemplateCacheStats {
    pub warm_calls: u64,
    pub last_refresh_unix_ms: u64,
    pub template_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TemplateCacheKey {
    pub pool_id: Pubkey,
    pub direction: SwapTemplateDirection,
    pub fee_tier_bps: u16,
}

impl TemplateCacheKey {
    pub fn new(pool_id: Pubkey, direction: SwapTemplateDirection, fee_tier_bps: u16) -> Self {
        Self {
            pool_id,
            direction,
            fee_tier_bps,
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
}

impl InstructionTemplate {
    pub fn from_amm_v3(params: AmmV3TemplateParams) -> Self {
        let AmmV3TemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, fee_tier_bps),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        }
    }

    pub fn from_cp_amm(params: CpAmmTemplateParams) -> Self {
        let CpAmmTemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, fee_tier_bps),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        }
    }

    pub fn from_lb_clmm(params: LbClmmTemplateParams) -> Self {
        let LbClmmTemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, fee_tier_bps),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        }
    }

    pub fn from_lifinity(params: LifinityTemplateParams) -> Self {
        let LifinityTemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, fee_tier_bps),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        }
    }

    pub fn from_whirlpool(params: WhirlpoolTemplateParams) -> Self {
        let WhirlpoolTemplateParams {
            pool_id,
            program_id,
            direction,
            fee_tier_bps,
            discriminator,
            accounts,
            data,
            field_layout,
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        } = params;

        let patches = field_layout.into_patches();

        Self {
            key: TemplateCacheKey::new(pool_id, direction, fee_tier_bps),
            program_id,
            discriminator,
            accounts: accounts.into(),
            data: Arc::new(data),
            patches: patches.into(),
            cu_budget,
            tlv_bytes,
            template_hash,
            alt_handle,
        }
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
    Lifinity(LifinityField),
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifinityField {
    AmountIn,
    AmountOut,
    SlotPadding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WhirlpoolField {
    AmountIn,
    AmountOut,
    SqrtPriceLimit,
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
}

impl LbClmmFieldLayout {
    fn into_patches(self) -> Vec<TemplatePatch> {
        let mut patches = Vec::with_capacity(4);
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
        patches
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LifinityFieldLayout {
    pub amount_in: TemplateSlice,
    pub amount_out: TemplateSlice,
    pub slot_padding: Option<TemplateSlice>,
}

impl LifinityFieldLayout {
    fn into_patches(self) -> Vec<TemplatePatch> {
        let mut patches = Vec::with_capacity(3);
        patches.push(TemplatePatch {
            offset: self.amount_in.offset,
            length: self.amount_in.length,
            kind: TemplateFieldKind::Lifinity(LifinityField::AmountIn),
        });
        patches.push(TemplatePatch {
            offset: self.amount_out.offset,
            length: self.amount_out.length,
            kind: TemplateFieldKind::Lifinity(LifinityField::AmountOut),
        });
        if let Some(slice) = self.slot_padding {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::Lifinity(LifinityField::SlotPadding),
            });
        }
        patches
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WhirlpoolFieldLayout {
    pub amount_in: TemplateSlice,
    pub amount_out: TemplateSlice,
    pub sqrt_price_limit: Option<TemplateSlice>,
}

impl WhirlpoolFieldLayout {
    fn into_patches(self) -> Vec<TemplatePatch> {
        let mut patches = Vec::with_capacity(3);
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
        if let Some(slice) = self.sqrt_price_limit {
            patches.push(TemplatePatch {
                offset: slice.offset,
                length: slice.length,
                kind: TemplateFieldKind::Whirlpool(WhirlpoolField::SqrtPriceLimit),
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
}

#[derive(Debug, Clone)]
pub struct LifinityTemplateParams {
    pub pool_id: Pubkey,
    pub program_id: Pubkey,
    pub direction: SwapTemplateDirection,
    pub fee_tier_bps: u16,
    pub discriminator: [u8; 8],
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
    pub field_layout: LifinityFieldLayout,
    pub cu_budget: u32,
    pub tlv_bytes: u16,
    pub template_hash: [u8; 32],
    pub alt_handle: Option<AltHandleId>,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct AltHandleId(pub u32);

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}
