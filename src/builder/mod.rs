mod alt_manager;
mod blockhash;
mod bundle_builder;
mod flash_loan;
mod mev_guard;
mod template_cache;
mod template_loader;
pub mod templates;
mod tx_graph;

use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::{anyhow, Context, Result};

use crate::config::AppConfig;
use blockhash::BlockhashCache;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use solana_sdk::transaction::VersionedTransaction;
use tracing::warn;

pub use alt_manager::AltManager;
pub use flash_loan::{
    compute_flash_loan_fee, FlashLoanPlanner, FlashLoanRoutePlan, FLASH_LOAN_BPS_DENOMINATOR,
    SOLEND_FLASH_LOAN_FEE_BPS,
};
pub use template_cache::{
    AltHandleId, AmmV3Field, AmmV3FieldLayout, AmmV3PoolAccounts, AmmV3TemplateParams, CpAmmField,
    CpAmmFieldLayout, CpAmmTemplateParams, InstructionTemplate, LbClmmField, LbClmmFieldLayout,
    LbClmmTemplateParams, TemplateCache, TemplateCacheKey, TemplateFieldKind, TemplateMetadata,
    TemplatePatch, TemplateSlice, WhirlpoolField, WhirlpoolFieldLayout, WhirlpoolTemplateParams,
};
pub use bundle_builder::{BundleBuildContext, BundleInstructionPlan, BundleLookupTablePlan};
pub use mev_guard::MevGuard;
pub use tx_graph::TxGraph;

#[derive(Clone)]
pub struct BuilderHandle(Arc<BuilderContext>);

const LEGACY_COMPUTE_UNIT_LIMIT: u32 = 200_000;

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

    pub fn token_account_for_mint(&self, mint: &Pubkey) -> Option<Pubkey> {
        self.0.token_atas.get(mint).copied()
    }

    pub fn host_fee_account_for_mint(&self, mint: &Pubkey) -> Option<Pubkey> {
        self.0.host_fee_accounts.get(mint).copied()
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
        let mut plans = legs.to_vec();
        for plan in plans.iter_mut() {
            if plan.lookup_table.is_none() {
                if let Some(lookup) = self.ensure_lookup_table(plan)? {
                    plan.lookup_table = Some(lookup);
                }
            }
        }
        bundle_builder::build_bundle(
            seq,
            self.0.payer_pubkey,
            &self.0.template_cache,
            &self.0.token_atas,
            blockhash,
            prefix_instructions,
            &plans,
            suffix_instructions,
        )
    }

    fn ensure_lookup_table(
        &self,
        plan: &bundle_builder::BundleInstructionPlan,
    ) -> Result<Option<BundleLookupTablePlan>> {
        let pool = Pubkey::from_str(&plan.pool_id)
            .with_context(|| format!("invalid pool id {}", plan.pool_id))?;
        let key = TemplateCacheKey::new(pool, plan.direction, plan.template_hash);
        let template = self.0.template_cache.get(&key).with_context(|| {
            format!(
                "missing template for pool {} direction {:?}",
                plan.pool_id, plan.direction
            )
        })?;
        if !lookup_table_required(plan, template.as_ref()) {
            return Ok(None);
        }

        let handle = match self
            .0
            .alt_manager
            .ensure_handle(pool, template.alt_accounts.as_ref())
        {
            Some(handle) => handle,
            None => return Ok(None),
        };
        let info = self.0.alt_manager.table_info(handle).ok_or_else(|| {
            anyhow!(
                "ALT handle {} for pool {} not ready",
                handle.0,
                plan.pool_id
            )
        })?;

        Ok(Some(BundleLookupTablePlan {
            handle_id: handle.0,
            table_address: info.address.to_string(),
            authority: info.authority.to_string(),
            creation_slot: info.creation_slot,
            last_extended_slot: info.last_extended_slot,
            accounts: info.addresses.iter().map(|pk| pk.to_string()).collect(),
        }))
    }

    pub fn instruction_accounts_for_plan(
        &self,
        plan: &bundle_builder::BundleInstructionPlan,
    ) -> Result<Vec<AccountMeta>> {
        bundle_builder::hydrate_accounts_for_plan(
            &self.0.template_cache,
            plan,
            self.0.payer_pubkey,
            &self.0.token_atas,
        )
    }

    pub fn sign_bundle(
        &self,
        ctx: &bundle_builder::BundleBuildContext,
    ) -> Result<VersionedTransaction> {
        let latest_hash = self.recent_blockhash()?;
        let mut message = ctx.message.clone();
        update_message_blockhash(&mut message, latest_hash);
        let signers = [&*self.0.payer];
        VersionedTransaction::try_new(message, &signers)
            .map_err(|err| anyhow!("failed to sign bundle {}: {err}", ctx.seq))
    }

    pub fn heartbeat(&self, stage: BuilderStage) {
        match stage {
            BuilderStage::EvalHotPath => self.0.tx_graph.mark_eval_heartbeat(),
            BuilderStage::SubmitHotPath => self.0.tx_graph.mark_submit_heartbeat(),
        }
    }

    pub fn register_amm_v3_pool(&self, meta: AmmV3PoolMeta) {
        if let Err(err) = template_loader::register_amm_v3_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register amm_v3 templates");
        }
    }

    pub fn register_cp_amm_pool(&self, meta: CpAmmPoolMeta) {
        if let Err(err) = template_loader::register_cp_amm_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register cp_amm templates");
        }
    }

    pub fn register_lb_clmm_pool(&self, meta: LbClmmPoolMeta) {
        if let Err(err) = template_loader::register_lb_clmm_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register lb_clmm templates");
        }
    }

    pub fn register_whirlpool_pool(&self, meta: WhirlpoolPoolMeta) {
        if let Err(err) = template_loader::register_whirlpool_pool(&self.0.template_cache, &meta) {
            warn!(error = ?err, pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, "failed to register whirlpool templates");
        }
    }

    }

fn lookup_table_required(
    plan: &bundle_builder::BundleInstructionPlan,
    template: &InstructionTemplate,
) -> bool {
    if template.alt_accounts.is_empty() {
        return false;
    }

    match plan.context.compute_unit_limit {
        Some(limit) => limit > LEGACY_COMPUTE_UNIT_LIMIT,
        None => false,
    }
}

fn update_message_blockhash(message: &mut VersionedMessage, new_hash: Hash) {
    match message {
        VersionedMessage::Legacy(legacy) => legacy.recent_blockhash = new_hash,
        VersionedMessage::V0(v0) => v0.recent_blockhash = new_hash,
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
    token_atas: HashMap<Pubkey, Pubkey>,
    host_fee_accounts: HashMap<Pubkey, Pubkey>,
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

        let token_atas = config.token_ata_map().clone();
        let host_fee_accounts = config.host_fee_account_map().clone();
        let mev_guard = MevGuard::new(config.clone());

        let alt_manager = AltManager::new(config.clone())?;
        let flash_loan = FlashLoanPlanner::new(config.clone())?;
        flash_loan.prepare_default_routes();

        Ok(Self {
            template_cache,
            alt_manager,
            tx_graph: TxGraph::new(),
            flash_loan,
            mev_guard,
            blockhash_cache,
            payer_pubkey: payer.pubkey(),
            payer,
            auth_pubkey: auth.pubkey(),
            auth,
            token_atas,
            host_fee_accounts,
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
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_token_program: Option<Pubkey>,
    pub quote_token_program: Option<Pubkey>,
    pub amm_config: Pubkey,
    pub observation_state: Pubkey,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpAmmPoolMeta {
    pub pool_id: Pubkey,
    pub fee_tier_bps: u16,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
    pub accounts: CpAmmPoolAccounts,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CpAmmPoolAccounts {
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    pub token_a_vault: Option<Pubkey>,
    pub token_b_vault: Option<Pubkey>,
    pub token_a_program: Option<Pubkey>,
    pub token_b_program: Option<Pubkey>,
    pub referral_token_account: Option<Pubkey>,
}

impl CpAmmPoolAccounts {
    pub fn for_direction(&self, direction: templates::SwapTemplateDirection) -> Self {
        match direction {
            templates::SwapTemplateDirection::BaseIn => self.clone(),
            templates::SwapTemplateDirection::BaseOut => Self {
                token_a_mint: self.token_b_mint,
                token_b_mint: self.token_a_mint,
                token_a_vault: self.token_b_vault,
                token_b_vault: self.token_a_vault,
                token_a_program: self.token_b_program,
                token_b_program: self.token_a_program,
                referral_token_account: self.referral_token_account,
            },
        }
    }

    pub fn is_populated(&self) -> bool {
        const ZERO: Pubkey = Pubkey::new_from_array([0u8; 32]);
        self.token_a_mint != ZERO
            && self.token_b_mint != ZERO
            && self.token_a_vault.is_some()
            && self.token_b_vault.is_some()
            && self.token_a_program.is_some()
            && self.token_b_program.is_some()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LbClmmPoolAccounts {
    pub reserve_x: Option<Pubkey>,
    pub reserve_y: Option<Pubkey>,
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub oracle: Option<Pubkey>,
    pub bin_array_bitmap_extension: Option<Pubkey>,
    pub token_x_program: Pubkey,
    pub token_y_program: Pubkey,
    pub host_fee_in: Option<Pubkey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LbClmmPoolMeta {
    pub pool_id: Pubkey,
    pub fee_tier_bps: u16,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
    pub accounts: LbClmmPoolAccounts,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WhirlpoolPoolAccounts {
    pub whirlpool: Pubkey,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Option<Pubkey>,
    pub token_vault_b: Option<Pubkey>,
    pub token_program_a: Option<Pubkey>,
    pub token_program_b: Option<Pubkey>,
    pub oracle: Option<Pubkey>,
}

impl WhirlpoolPoolAccounts {
    pub fn is_populated(&self) -> bool {
        const ZERO: Pubkey = Pubkey::new_from_array([0u8; 32]);
        self.whirlpool != ZERO
            && self.token_mint_a != ZERO
            && self.token_mint_b != ZERO
            && self.token_vault_a.is_some()
            && self.token_vault_b.is_some()
            && self.token_program_a.is_some()
            && self.token_program_b.is_some()
            && self.oracle.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WhirlpoolPoolMeta {
    pub pool_id: Pubkey,
    pub fee_tier_bps: u16,
    pub alt_accounts: Vec<Pubkey>,
    pub alt_handle: Option<AltHandleId>,
    pub accounts: WhirlpoolPoolAccounts,
}
