mod alt_manager;
mod flash_loan;
mod mev_guard;
mod template_cache;
pub mod templates;
mod tx_graph;

use std::sync::Arc;

use crate::config::AppConfig;

pub use alt_manager::{AltManager, AltManagerStats};
pub use flash_loan::FlashLoanPlanner;
pub use mev_guard::MevGuard;
pub use template_cache::{
    AmmV3Field, AmmV3FieldLayout, AmmV3TemplateParams, CpAmmField, CpAmmFieldLayout,
    CpAmmTemplateParams, InstructionTemplate, LbClmmField, LbClmmFieldLayout, LbClmmTemplateParams,
    LifinityField, LifinityFieldLayout, LifinityTemplateParams, TemplateCache, TemplateCacheKey,
    TemplateCacheStats, TemplateFieldKind, TemplatePatch, WhirlpoolField, WhirlpoolFieldLayout,
    WhirlpoolTemplateParams,
};
pub use tx_graph::TxGraph;

#[derive(Clone)]
pub struct BuilderHandle(Arc<BuilderContext>);

impl BuilderHandle {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self(Arc::new(BuilderContext::new(config)))
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

    pub fn mev_guard(&self) -> &MevGuard {
        &self.0.mev_guard
    }

    pub fn heartbeat(&self, stage: BuilderStage) {
        match stage {
            BuilderStage::EvalHotPath => self.0.tx_graph.mark_eval_heartbeat(),
            BuilderStage::SubmitHotPath => self.0.tx_graph.mark_submit_heartbeat(),
        }
    }
}

struct BuilderContext {
    template_cache: TemplateCache,
    alt_manager: AltManager,
    tx_graph: TxGraph,
    flash_loan: FlashLoanPlanner,
    mev_guard: MevGuard,
}

impl BuilderContext {
    fn new(config: Arc<AppConfig>) -> Self {
        Self {
            template_cache: TemplateCache::new(config.clone()),
            alt_manager: AltManager::new(config.clone()),
            tx_graph: TxGraph::new(),
            flash_loan: FlashLoanPlanner::new(config.clone()),
            mev_guard: MevGuard::new(config),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BuilderStage {
    EvalHotPath,
    SubmitHotPath,
}
