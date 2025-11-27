mod alt_manager;
mod blockhash;
mod bundle_builder;
mod flash_loan;
mod mev_guard;
mod template_cache;
mod template_loader;
pub mod templates;
mod tx_graph;

use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::config::AppConfig;
use blockhash::BlockhashCache;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use solana_sdk::transaction::VersionedTransaction;
use tracing::warn;

pub use alt_manager::{AltManager, AltManagerStats};
pub use bundle_builder::{BundleBuildContext, BundleInstructionPlan, BundleLookupTablePlan};
pub use flash_loan::{FlashLoanPlanner, FlashLoanRoutePlan};
pub use mev_guard::MevGuard;
pub use template_cache::{
    AltHandleId, AmmV3Field, AmmV3FieldLayout, AmmV3TemplateParams, CpAmmField, CpAmmFieldLayout,
    CpAmmTemplateParams, InstructionTemplate, LbClmmField, LbClmmFieldLayout, LbClmmTemplateParams,
    LifinityField, LifinityFieldLayout, LifinityTemplateParams, TemplateCache, TemplateCacheKey,
    TemplateCacheStats, TemplateFieldKind, TemplatePatch, TemplateSlice, WhirlpoolField,
    WhirlpoolFieldLayout, WhirlpoolTemplateParams,
};
pub use tx_graph::TxGraph;

#[derive(Clone)]
pub struct BuilderHandle(Arc<BuilderContext>);

#[derive(Clone, Copy, Debug, Default)]
pub struct TemplateReadiness {
    pub template_ready: bool,
    pub alt_ready: bool,
}

impl TemplateReadiness {
    pub const fn missing() -> Self {
        Self {
            template_ready: false,
            alt_ready: false,
        }
    }
}

impl BuilderHandle {
    pub fn new(config: Arc<AppConfig>) -> Result<Self> {
        Ok(Self(Arc::new(BuilderContext::new(config)?)))
    }

    pub fn template_cache(&self) -> &TemplateCache {
        &self.0.template_cache
    }

    pub fn alt_manager(&self) -> &AltManager {
        &self.0.alt_manager
    }

    pub fn tx_graph(&self) -> &TxGraph {
        &self.0.tx_graph
    }

    pub fn flash_loan_planner(&self) -> &FlashLoanPlanner {
        &self.0.flash_loan
    }

    pub fn payer_pubkey(&self) -> Pubkey {
        self.0.payer_pubkey
    }

    pub fn mev_guard(&self) -> &MevGuard {
        &self.0.mev_guard
    }

    pub fn recent_blockhash(&self) -> Result<Hash> {
        self.0.blockhash_cache.latest()
    }

    pub fn auth_pubkey(&self) -> Pubkey {
        self.0.auth_pubkey
    }

    pub fn auth_signer(&self) -> Arc<Keypair> {
        self.0.auth.clone()
    }

    pub fn prepare_bundle(
        &self,
        seq: u64,
        prefix_instructions: &[Instruction],
        legs: &[bundle_builder::BundleInstructionPlan],
        suffix_instructions: &[Instruction],
    ) -> Result<bundle_builder::BundleBuildContext> {
        let blockhash = self.recent_blockhash()?;
        bundle_builder::build_bundle(
            seq,
            self.0.payer_pubkey,
            &self.0.template_cache,
            blockhash,
            prefix_instructions,
            legs,
            suffix_instructions,
        )
    }

    pub fn instruction_accounts_for_plan(
        &self,
        plan: &bundle_builder::BundleInstructionPlan,
    ) -> Result<Vec<AccountMeta>> {
        bundle_builder::hydrate_accounts_for_plan(&self.0.template_cache, plan)
    }

    pub fn sign_bundle(
        &self,
        ctx: &bundle_builder::BundleBuildContext,
    ) -> Result<VersionedTransaction> {
        let signers = [&*self.0.payer];
        VersionedTransaction::try_new(ctx.message.clone(), &signers)
            .map_err(|err| anyhow!("failed to sign bundle {}: {err}", ctx.seq))
    }

    pub fn heartbeat(&self, stage: BuilderStage) {
        match stage {
            BuilderStage::EvalHotPath => self.0.tx_graph.mark_eval_heartbeat(),
            BuilderStage::SubmitHotPath => self.0.tx_graph.mark_submit_heartbeat(),
        }
    }

    pub fn template_readiness(
        &self,
        pool: &Pubkey,
        direction: templates::SwapTemplateDirection,
        fee_tier_bps: u16,
    ) -> TemplateReadiness {
        let key = TemplateCacheKey::new(*pool, direction, fee_tier_bps);
        if let Some(template) = self.0.template_cache.get(&key) {
            let alt_ready = template
                .alt_handle
                .map(|handle| self.0.alt_manager.table_info(handle).is_some())
                .unwrap_or(true);
            TemplateReadiness {
                template_ready: true,
                alt_ready,
            }
        } else {
            TemplateReadiness::missing()
        }
    }

    pub fn register_amm_v3_pool(&self, mut meta: AmmV3PoolMeta) {
        if meta.alt_handle.is_none() {
            meta.alt_handle = self
                .0
                .alt_manager
                .ensure_handle(meta.pool_id, &meta.alt_accounts);
        }
        if let Err(err) = template_loader::register_amm_v3_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register amm_v3 templates");
        }
    }

    pub fn register_cp_amm_pool(&self, mut meta: CpAmmPoolMeta) {
        if meta.alt_handle.is_none() {
            meta.alt_handle = self
                .0
                .alt_manager
                .ensure_handle(meta.pool_id, &meta.alt_accounts);
        }
        if let Err(err) = template_loader::register_cp_amm_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register cp_amm templates");
        }
    }

    pub fn register_lb_clmm_pool(&self, mut meta: LbClmmPoolMeta) {
        if meta.alt_handle.is_none() {
            meta.alt_handle = self
                .0
                .alt_manager
                .ensure_handle(meta.pool_id, &meta.alt_accounts);
        }
        if let Err(err) = template_loader::register_lb_clmm_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register lb_clmm templates");
        }
    }

    pub fn register_lifinity_pool(&self, mut meta: LifinityPoolMeta) {
        if meta.alt_handle.is_none() {
            meta.alt_handle = self
                .0
                .alt_manager
                .ensure_handle(meta.pool_id, &meta.alt_accounts);
        }
        if let Err(err) = template_loader::register_lifinity_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register lifinity templates");
        }
    }

    pub fn register_whirlpool_pool(&self, mut meta: WhirlpoolPoolMeta) {
        if meta.alt_handle.is_none() {
            meta.alt_handle = self
                .0
                .alt_manager
                .ensure_handle(meta.pool_id, &meta.alt_accounts);
        }
        if let Err(err) = template_loader::register_whirlpool_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register whirlpool templates");
        }
    }
}

struct BuilderContext {
    template_cache: TemplateCache,
    alt_manager: AltManager,
    tx_graph: TxGraph,
    flash_loan: FlashLoanPlanner,
    mev_guard: MevGuard,
    blockhash_cache: BlockhashCache,
    payer: Arc<Keypair>,
    payer_pubkey: Pubkey,
    auth: Arc<Keypair>,
    auth_pubkey: Pubkey,
}

impl BuilderContext {
    fn new(config: Arc<AppConfig>) -> Result<Self> {
        let template_cache = TemplateCache::new(config.clone());
        template_loader::populate_static_templates(&template_cache, config.as_ref());

        let payer_path = config.wallets.payer.path.clone();
        let payer = read_keypair_file(&payer_path).map_err(|err| {
            anyhow!(
                "failed to load payer wallet from {}: {err}",
                payer_path.display()
            )
        })?;

        let auth_path = config.wallets.auth.path.clone();
        let auth = read_keypair_file(&auth_path).map_err(|err| {
            anyhow!(
                "failed to load auth wallet from {}: {err}",
                auth_path.display()
            )
        })?;

        let blockhash_endpoint = config
            .quicknode_rpcs
            .first()
            .cloned()
            .unwrap_or_else(|| "https://api.mainnet-beta.solana.com".to_string());
        let blockhash_cache = BlockhashCache::new(blockhash_endpoint);

        let payer = Arc::new(payer);
        let auth = Arc::new(auth);

        Ok(Self {
            template_cache,
            alt_manager: AltManager::new(config.clone())?,
            tx_graph: TxGraph::new(),
            flash_loan: FlashLoanPlanner::new(config.clone())?,
            mev_guard: MevGuard::new(config),
            blockhash_cache,
            payer_pubkey: payer.pubkey(),
            payer,
            auth_pubkey: auth.pubkey(),
            auth,
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BuilderStage {
    EvalHotPath,
    SubmitHotPath,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AmmV3PoolMeta {
    pub pool_id: Pubkey,
    pub fee_tier_bps: u16,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpAmmPoolMeta {
    pub pool_id: Pubkey,
    pub fee_tier_bps: u16,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LbClmmPoolMeta {
    pub pool_id: Pubkey,
    pub fee_tier_bps: u16,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifinityPoolMeta {
    pub pool_id: Pubkey,
    pub fee_tier_bps: u16,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WhirlpoolPoolMeta {
    pub pool_id: Pubkey,
    pub fee_tier_bps: u16,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
}
