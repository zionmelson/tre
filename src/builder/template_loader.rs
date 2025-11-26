use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};
use tracing::{info, warn};

use crate::builder::templates::{
    AccountSpec, AmmV3SwapTemplate, ArgSpec, SwapTemplateDirection, AMM_V3_SWAP_TEMPLATES,
};
use crate::builder::{
    AmmV3Field, AmmV3FieldLayout, AmmV3TemplateParams, InstructionTemplate, TemplateCache,
    TemplateSlice,
};
use crate::config::AppConfig;

const DEFAULT_AMM_V3_CU_BUDGET: u32 = 350_000;

pub fn populate_static_templates(cache: &TemplateCache, config: &AppConfig) {
    if let Err(err) = load_amm_v3_templates(cache, config) {
        warn!(error = ?err, "failed to load amm_v3 templates into cache");
    }
}

fn load_amm_v3_templates(cache: &TemplateCache, _config: &AppConfig) -> Result<()> {
    let mut inserted = 0usize;
    for template in AMM_V3_SWAP_TEMPLATES {
        let params = build_amm_v3_params(template).with_context(|| {
            format!("failed to build amm_v3 template for direction {:?}