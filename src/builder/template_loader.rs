use std::{convert::TryFrom, str::FromStr};

use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use solana_sdk::{instruction::AccountMeta, pubkey, pubkey::Pubkey};
use tracing::{info, warn};

use crate::builder::templates::{
    AccountSpec, AmmV3SwapTemplate, ArgSpec, CpAmmSwapTemplate, LbClmmSwapTemplate,
    SwapTemplateDirection, WhirlpoolSwapTemplate, AMM_V3_SWAP_TEMPLATES, CP_AMM_SWAP_TEMPLATES,
    LB_CLMM_SWAP_TEMPLATES, WHIRLPOOL_SWAP_TEMPLATES,
};
use crate::builder::{
    AmmV3FieldLayout, AmmV3PoolAccounts, AmmV3PoolMeta, AmmV3TemplateParams, CpAmmFieldLayout,
    CpAmmPoolMeta, CpAmmTemplateParams, InstructionTemplate, LbClmmFieldLayout, LbClmmPoolMeta,
    LbClmmTemplateParams, TemplateCache, TemplateSlice, WhirlpoolFieldLayout, WhirlpoolPoolMeta,
    WhirlpoolTemplateParams,
};
use crate::config::AppConfig;

const DEFAULT_AMM_V3_CU_BUDGET: u32 = 350_000;
const DEFAULT_CP_AMM_CU_BUDGET: u32 = 400_000;
const DEFAULT_LB_CLMM_CU_BUDGET: u32 = 420_000;
const DEFAULT_WHIRLPOOL_CU_BUDGET: u32 = 380_000;
const AMM_V3_POOL_VAULT_SEED: &[u8] = b"pool_vault";
const SPL_TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

pub fn populate_static_templates(cache: &TemplateCache, config: &AppConfig) {
    if let Err(err) = load_amm_v3_templates(cache, config) {
        warn!(error = ?err, "failed to load amm_v3 templates into cache");
    }
    if let Err(err) = load_cp_amm_templates(cache, config) {
        warn!(error = ?err, "failed to load cp_amm templates into cache");
    }
    if let Err(err) = load_lb_clmm_templates(cache, config) {
        warn!(error = ?err, "failed to load lb_clmm templates into cache");
    }
    if let Err(err) = load_whirlpool_templates(cache, config) {
        warn!(error = ?err, "failed to load whirlpool templates into cache");
    }
}

fn load_amm_v3_templates(cache: &TemplateCache, _config: &AppConfig) -> Result<()> {
    let mut inserted = 0usize;
    for template in AMM_V3_SWAP_TEMPLATES {
        let params = build_amm_v3_params(template, None).with_context(|| {
            format!(
                "failed to build amm_v3 template for direction {:?}",
                template.direction
            )
        })?;
        cache.insert(InstructionTemplate::from_amm_v3(params));
        inserted += 1;
    }

    info!(count = inserted, "loaded amm_v3 instruction templates");
    Ok(())
}

pub(super) fn register_amm_v3_pool(cache: &TemplateCache, meta: &AmmV3PoolMeta) -> Result<()> {
    let mut inserted = 0usize;
    for template in AMM_V3_SWAP_TEMPLATES {
        let params = build_amm_v3_params(template, Some(meta)).with_context(|| {
            format!(
                "failed to build amm_v3 template for pool {} direction {:?}",
                meta.pool_id, template.direction
            )
        })?;
        cache.insert(InstructionTemplate::from_amm_v3(params));
        inserted += 1;
    }

    info!(pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, count = inserted, "registered amm_v3 instruction templates");
    Ok(())
}

fn load_cp_amm_templates(cache: &TemplateCache, _config: &AppConfig) -> Result<()> {
    let mut inserted = 0usize;
    for template in CP_AMM_SWAP_TEMPLATES {
        let params = build_cp_amm_params(template, None).with_context(|| {
            format!(
                "failed to build cp_amm template for name {} direction {:?}",
                template.name, template.direction
            )
        })?;
        cache.insert(InstructionTemplate::from_cp_amm(params));
        inserted += 1;
    }

    info!(count = inserted, "loaded cp_amm instruction templates");
    Ok(())
}

pub(super) fn register_cp_amm_pool(cache: &TemplateCache, meta: &CpAmmPoolMeta) -> Result<()> {
    let mut inserted = 0usize;
    for template in CP_AMM_SWAP_TEMPLATES {
        let params = build_cp_amm_params(template, Some(meta)).with_context(|| {
            format!(
                "failed to build cp_amm template for pool {} direction {:?}",
                meta.pool_id, template.direction
            )
        })?;
        cache.insert(InstructionTemplate::from_cp_amm(params));
        inserted += 1;
    }

    info!(pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, count = inserted, "registered cp_amm instruction templates");
    Ok(())
}

fn load_lb_clmm_templates(cache: &TemplateCache, _config: &AppConfig) -> Result<()> {
    let mut inserted = 0usize;
    for template in LB_CLMM_SWAP_TEMPLATES {
        let params = build_lb_clmm_params(template, None).with_context(|| {
            format!(
                "failed to build lb_clmm template for name {} direction {:?}",
                template.name, template.direction
            )
        })?;
        cache.insert(InstructionTemplate::from_lb_clmm(params));
        inserted += 1;
    }

    info!(count = inserted, "loaded lb_clmm instruction templates");
    Ok(())
}

pub(super) fn register_lb_clmm_pool(cache: &TemplateCache, meta: &LbClmmPoolMeta) -> Result<()> {
    let mut inserted = 0usize;
    for template in LB_CLMM_SWAP_TEMPLATES {
        let params = build_lb_clmm_params(template, Some(meta)).with_context(|| {
            format!(
                "failed to build lb_clmm template for pool {} direction {:?}",
                meta.pool_id, template.direction
            )
        })?;
        cache.insert(InstructionTemplate::from_lb_clmm(params));
        inserted += 1;
    }

    info!(pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, count = inserted, "registered lb_clmm instruction templates");
    Ok(())
}

fn load_whirlpool_templates(cache: &TemplateCache, _config: &AppConfig) -> Result<()> {
    let mut inserted = 0usize;
    for template in WHIRLPOOL_SWAP_TEMPLATES {
        let params = build_whirlpool_params(template, None).with_context(|| {
            format!(
                "failed to build whirlpool template for direction {:?}",
                template.direction
            )
        })?;
        cache.insert(InstructionTemplate::from_whirlpool(params));
        inserted += 1;
    }

    info!(count = inserted, "loaded whirlpool instruction templates");
    Ok(())
}

pub(super) fn register_whirlpool_pool(
    cache: &TemplateCache,
    meta: &WhirlpoolPoolMeta,
) -> Result<()> {
    let mut inserted = 0usize;
    for template in WHIRLPOOL_SWAP_TEMPLATES {
        let params = build_whirlpool_params(template, Some(meta)).with_context(|| {
            format!(
                "failed to build whirlpool template for pool {} direction {:?}",
                meta.pool_id, template.direction
            )
        })?;
        cache.insert(InstructionTemplate::from_whirlpool(params));
        inserted += 1;
    }

    info!(pool = %meta.pool_id, fee_bps = meta.fee_tier_bps, count = inserted, "registered whirlpool instruction templates");
    Ok(())
}

fn build_amm_v3_params(
    template: &AmmV3SwapTemplate,
    pool_meta: Option<&AmmV3PoolMeta>,
) -> Result<AmmV3TemplateParams> {
    let program_id = Pubkey::from_str(template.program_id)
        .with_context(|| format!("invalid program id {}", template.program_id))?;
    let accounts = convert_accounts(template.accounts)?;

    let mut data = Vec::with_capacity(64);
    data.extend_from_slice(&template.discriminator);

    let mut amount_slice = None;
    let mut threshold_slice = None;
    let mut sqrt_slice = None;

    for arg in template.args {
        let slice = encode_arg(arg, &mut data)?;
        match arg.name {
            "amount" => amount_slice = Some(slice),
            "other_amount_threshold" => threshold_slice = Some(slice),
            "sqrt_price_limit_x64" => sqrt_slice = Some(slice),
            _ => {}
        }
    }

    let field_layout = AmmV3FieldLayout {
        amount_specified: amount_slice
            .ok_or_else(|| anyhow!("amm_v3 template missing amount arg definition"))?,
        other_amount_threshold: threshold_slice.ok_or_else(|| {
            anyhow!("amm_v3 template missing other_amount_threshold arg definition")
        })?,
        sqrt_price_limit: sqrt_slice,
    };

    let template_hash = compute_template_hash(&program_id, &accounts, &data);

    let (pool_id, fee_tier_bps, alt_handle, alt_accounts) = pool_meta
        .map(|meta| {
            (
                meta.pool_id,
                meta.fee_tier_bps,
                meta.alt_handle,
                meta.alt_accounts.clone(),
            )
        })
        .unwrap_or_else(|| {
            (
                synth_pool_id(&program_id, template.direction),
                0,
                None,
                Vec::new(),
            )
        });

    let pool_accounts = pool_meta
        .map(|meta| derive_amm_v3_pool_accounts(meta, &program_id))
        .transpose()?;

    Ok(AmmV3TemplateParams {
        pool_id,
        program_id,
        direction: template.direction,
        fee_tier_bps,
        discriminator: template.discriminator,
        accounts,
        data,
        field_layout,
        cu_budget: DEFAULT_AMM_V3_CU_BUDGET,
        tlv_bytes: 0,
        template_hash,
        alt_handle,
        alt_accounts,
        account_specs: template.accounts,
        pool_accounts,
    })
}

fn derive_amm_v3_pool_accounts(
    meta: &AmmV3PoolMeta,
    program_id: &Pubkey,
) -> Result<AmmV3PoolAccounts> {
    let base_bytes = meta.base_mint.to_bytes();
    let quote_bytes = meta.quote_mint.to_bytes();
    let base_program = meta.base_token_program.unwrap_or(SPL_TOKEN_PROGRAM_ID);
    let quote_program = meta.quote_token_program.unwrap_or(SPL_TOKEN_PROGRAM_ID);
    let (mint0, mint1, mint0_program, mint1_program) = if base_bytes <= quote_bytes {
        (meta.base_mint, meta.quote_mint, base_program, quote_program)
    } else {
        (meta.quote_mint, meta.base_mint, quote_program, base_program)
    };

    let vault0 = Pubkey::find_program_address(
        &[
            AMM_V3_POOL_VAULT_SEED,
            meta.pool_id.as_ref(),
            mint0.as_ref(),
        ],
        program_id,
    )
    .0;
    let vault1 = Pubkey::find_program_address(
        &[
            AMM_V3_POOL_VAULT_SEED,
            meta.pool_id.as_ref(),
            mint1.as_ref(),
        ],
        program_id,
    )
    .0;

    Ok(AmmV3PoolAccounts {
        amm_config: meta.amm_config,
        observation_state: meta.observation_state,
        mint0,
        mint1,
        mint0_program,
        mint1_program,
        vault0,
        vault1,
    })
}

fn build_cp_amm_params(
    template: &CpAmmSwapTemplate,
    pool_meta: Option<&CpAmmPoolMeta>,
) -> Result<CpAmmTemplateParams> {
    let program_id = Pubkey::from_str(template.program_id)
        .with_context(|| format!("invalid program id {}", template.program_id))?;
    let mut accounts = convert_accounts(template.accounts)?;

    let mut data = Vec::with_capacity(64);
    data.extend_from_slice(&template.discriminator);
    let field_layout = encode_cp_amm_args(template, &mut data)?;

    let (pool_id, fee_tier_bps, alt_handle, alt_accounts) = pool_meta
        .map(|meta| {
            (
                meta.pool_id,
                meta.fee_tier_bps,
                meta.alt_handle,
                meta.alt_accounts.clone(),
            )
        })
        .unwrap_or_else(|| {
            (
                synth_pool_id(&program_id, template.direction),
                0,
                None,
                Vec::new(),
            )
        });

    let pool_accounts = if let Some(meta) = pool_meta {
        apply_cp_amm_pool_accounts(template.accounts, &mut accounts, meta, template.direction)?;
        Some(meta.accounts.for_direction(template.direction))
    } else {
        None
    };

    let template_hash = compute_template_hash(&program_id, &accounts, &data);

    Ok(CpAmmTemplateParams {
        pool_id,
        program_id,
        direction: template.direction,
        fee_tier_bps,
        discriminator: template.discriminator,
        accounts,
        data,
        field_layout,
        cu_budget: DEFAULT_CP_AMM_CU_BUDGET,
        tlv_bytes: 0,
        template_hash,
        alt_handle,
        alt_accounts,
        account_specs: template.accounts,
        pool_accounts,
    })
}

fn apply_cp_amm_pool_accounts(
    specs: &[AccountSpec],
    accounts: &mut [AccountMeta],
    pool_meta: &CpAmmPoolMeta,
    direction: SwapTemplateDirection,
) -> Result<()> {
    if specs.len() != accounts.len() {
        anyhow::bail!(
            "template account/spec length mismatch ({} != {})",
            accounts.len(),
            specs.len()
        );
    }

    let directional = pool_meta.accounts.for_direction(direction);

    for (spec, meta) in specs.iter().zip(accounts.iter_mut()) {
        match spec.name {
            "pool" => {
                meta.pubkey = pool_meta.pool_id;
            }
            "token_a_vault" => {
                meta.pubkey = resolve_cp_amm_account(
                    pool_meta.pool_id,
                    "token_a_vault",
                    directional.token_a_vault,
                )?;
            }
            "token_b_vault" => {
                meta.pubkey = resolve_cp_amm_account(
                    pool_meta.pool_id,
                    "token_b_vault",
                    directional.token_b_vault,
                )?;
            }
            "token_a_mint" => {
                meta.pubkey = directional.token_a_mint;
            }
            "token_b_mint" => {
                meta.pubkey = directional.token_b_mint;
            }
            "token_a_program" => {
                meta.pubkey = resolve_cp_amm_account(
                    pool_meta.pool_id,
                    "token_a_program",
                    directional.token_a_program,
                )?;
            }
            "token_b_program" => {
                meta.pubkey = resolve_cp_amm_account(
                    pool_meta.pool_id,
                    "token_b_program",
                    directional.token_b_program,
                )?;
            }
            "referral_token_account" => {
                if let Some(pk) = directional.referral_token_account {
                    meta.pubkey = pk;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn apply_whirlpool_pool_accounts(
    specs: &[AccountSpec],
    accounts: &mut [AccountMeta],
    pool_meta: &WhirlpoolPoolMeta,
) -> Result<()> {
    if specs.len() != accounts.len() {
        anyhow::bail!(
            "template account/spec length mismatch ({} != {})",
            accounts.len(),
            specs.len()
        );
    }

    let vault_a = pool_meta
        .accounts
        .token_vault_a
        .ok_or_else(|| anyhow!("whirlpool pool {} missing token_vault_a", pool_meta.pool_id))?;
    let vault_b = pool_meta
        .accounts
        .token_vault_b
        .ok_or_else(|| anyhow!("whirlpool pool {} missing token_vault_b", pool_meta.pool_id))?;
    let oracle = pool_meta
        .accounts
        .oracle
        .ok_or_else(|| anyhow!("whirlpool pool {} missing oracle", pool_meta.pool_id))?;
    let token_program_a = pool_meta.accounts.token_program_a.ok_or_else(|| {
        anyhow!(
            "whirlpool pool {} missing token_program_a",
            pool_meta.pool_id
        )
    })?;
    let token_program_b = pool_meta.accounts.token_program_b.ok_or_else(|| {
        anyhow!(
            "whirlpool pool {} missing token_program_b",
            pool_meta.pool_id
        )
    })?;
    let token_program_any = token_program_a;

    for (spec, meta) in specs.iter().zip(accounts.iter_mut()) {
        match spec.name {
            name if whirlpool_account_name_is(name, &["whirlpool"]) => {
                meta.pubkey = pool_meta.pool_id;
            }
            name if whirlpool_account_name_is(name, &["tokenVaultA", "token_vault_a"]) => {
                meta.pubkey = vault_a;
            }
            name if whirlpool_account_name_is(name, &["tokenVaultB", "token_vault_b"]) => {
                meta.pubkey = vault_b;
            }
            name if whirlpool_account_name_is(name, &["tokenMintA", "token_mint_a"]) => {
                meta.pubkey = pool_meta.accounts.token_mint_a;
            }
            name if whirlpool_account_name_is(name, &["tokenMintB", "token_mint_b"]) => {
                meta.pubkey = pool_meta.accounts.token_mint_b;
            }
            name if whirlpool_account_name_is(name, &["tokenProgramA", "token_program_a"]) => {
                meta.pubkey = token_program_a;
            }
            name if whirlpool_account_name_is(name, &["tokenProgramB", "token_program_b"]) => {
                meta.pubkey = token_program_b;
            }
            name if whirlpool_account_name_is(name, &["tokenProgram", "token_program"]) => {
                meta.pubkey = token_program_any;
            }
            name if whirlpool_account_name_is(name, &["oracle"]) => {
                meta.pubkey = oracle;
            }
            _ => {}
        }
    }

    Ok(())
}

fn whirlpool_account_name_is(name: &str, variants: &[&str]) -> bool {
    variants.iter().any(|candidate| *candidate == name)
}

fn build_lb_clmm_params(
    template: &LbClmmSwapTemplate,
    pool_meta: Option<&LbClmmPoolMeta>,
) -> Result<LbClmmTemplateParams> {
    let program_id = Pubkey::from_str(template.program_id)
        .with_context(|| format!("invalid program id {}", template.program_id))?;
    let mut accounts = convert_accounts(template.accounts)?;
    let pool_accounts = if let Some(meta) = pool_meta {
        apply_lb_clmm_pool_accounts(template.accounts, &mut accounts, meta)?;
        Some(meta.accounts.clone())
    } else {
        None
    };

    let mut data = Vec::with_capacity(128);
    data.extend_from_slice(&template.discriminator);
    let field_layout = encode_lb_clmm_args(template, &mut data)?;
    let template_hash = compute_template_hash(&program_id, &accounts, &data);

    let (pool_id, fee_tier_bps, alt_handle, alt_accounts) = pool_meta
        .map(|meta| {
            (
                meta.pool_id,
                meta.fee_tier_bps,
                meta.alt_handle,
                meta.alt_accounts.clone(),
            )
        })
        .unwrap_or_else(|| {
            (
                synth_pool_id(&program_id, template.direction),
                0,
                None,
                Vec::new(),
            )
        });

    Ok(LbClmmTemplateParams {
        pool_id,
        program_id,
        direction: template.direction,
        fee_tier_bps,
        discriminator: template.discriminator,
        accounts,
        data,
        field_layout,
        cu_budget: DEFAULT_LB_CLMM_CU_BUDGET,
        tlv_bytes: 0,
        template_hash,
        alt_handle,
        alt_accounts,
        account_specs: template.accounts,
        pool_accounts,
    })
}

fn apply_lb_clmm_pool_accounts(
    specs: &[AccountSpec],
    accounts: &mut [AccountMeta],
    pool_meta: &LbClmmPoolMeta,
) -> Result<()> {
    for (spec, meta) in specs.iter().zip(accounts.iter_mut()) {
        match spec.name {
            "lb_pair" => {
                meta.pubkey = pool_meta.pool_id;
            }
            "bin_array_bitmap_extension" => {
                if let Some(pk) = pool_meta.accounts.bin_array_bitmap_extension {
                    meta.pubkey = pk;
                }
            }
            "reserve_x" => {
                meta.pubkey = resolve_required_account(
                    pool_meta.pool_id,
                    "reserve_x",
                    pool_meta.accounts.reserve_x,
                )?;
            }
            "reserve_y" => {
                meta.pubkey = resolve_required_account(
                    pool_meta.pool_id,
                    "reserve_y",
                    pool_meta.accounts.reserve_y,
                )?;
            }
            "token_x_mint" => {
                meta.pubkey = pool_meta.accounts.token_x_mint;
            }
            "token_y_mint" => {
                meta.pubkey = pool_meta.accounts.token_y_mint;
            }
            "oracle" => {
                meta.pubkey = resolve_required_account(
                    pool_meta.pool_id,
                    "oracle",
                    pool_meta.accounts.oracle,
                )?;
            }
            "host_fee_in" => {
                if let Some(pk) = pool_meta.accounts.host_fee_in {
                    meta.pubkey = pk;
                }
            }
            "token_x_program" => {
                meta.pubkey = pool_meta.accounts.token_x_program;
            }
            "token_y_program" => {
                meta.pubkey = pool_meta.accounts.token_y_program;
            }
            _ => {}
        }
    }

    Ok(())
}

fn resolve_required_account(pool_id: Pubkey, name: &str, value: Option<Pubkey>) -> Result<Pubkey> {
    value.ok_or_else(|| anyhow!("pool {} missing {name} account", pool_id))
}

fn resolve_cp_amm_account(pool_id: Pubkey, name: &str, value: Option<Pubkey>) -> Result<Pubkey> {
    value.ok_or_else(|| anyhow!("cp_amm pool {} missing {name} account", pool_id))
}

fn build_whirlpool_params(
    template: &WhirlpoolSwapTemplate,
    pool_meta: Option<&WhirlpoolPoolMeta>,
) -> Result<WhirlpoolTemplateParams> {
    let program_id = Pubkey::from_str(template.program_id)
        .with_context(|| format!("invalid program id {}", template.program_id))?;
    let mut accounts = convert_accounts(template.accounts)?;

    let mut data = Vec::with_capacity(64);
    data.extend_from_slice(&template.discriminator);
    let field_layout = encode_whirlpool_args(template, &mut data)?;
    let template_hash = compute_template_hash(&program_id, &accounts, &data);

    let (pool_id, fee_tier_bps, alt_handle, alt_accounts) = pool_meta
        .map(|meta| {
            (
                meta.pool_id,
                meta.fee_tier_bps,
                meta.alt_handle,
                meta.alt_accounts.clone(),
            )
        })
        .unwrap_or_else(|| {
            (
                synth_pool_id(&program_id, template.direction),
                0,
                None,
                Vec::new(),
            )
        });

    let pool_accounts = if let Some(meta) = pool_meta {
        apply_whirlpool_pool_accounts(template.accounts, &mut accounts, meta)?;
        Some(meta.accounts.clone())
    } else {
        None
    };

    Ok(WhirlpoolTemplateParams {
        pool_id,
        program_id,
        direction: template.direction,
        fee_tier_bps,
        discriminator: template.discriminator,
        accounts,
        data,
        field_layout,
        cu_budget: DEFAULT_WHIRLPOOL_CU_BUDGET,
        tlv_bytes: 0,
        template_hash,
        alt_handle,
        alt_accounts,
        account_specs: template.accounts,
        pool_accounts,
    })
}

fn convert_accounts(specs: &[AccountSpec]) -> Result<Vec<AccountMeta>> {
    specs
        .iter()
        .map(|spec| {
            let pubkey = match spec.address {
                Some(address) => Pubkey::from_str(address).with_context(|| {
                    format!(
                        "invalid pubkey literal {} for account {}",
                        address, spec.name
                    )
                })?,
                None => Pubkey::default(),
            };

            Ok(if spec.is_writable {
                AccountMeta::new(pubkey, spec.is_signer)
            } else {
                AccountMeta::new_readonly(pubkey, spec.is_signer)
            })
        })
        .collect()
}

fn encode_cp_amm_args(
    template: &CpAmmSwapTemplate,
    buffer: &mut Vec<u8>,
) -> Result<CpAmmFieldLayout> {
    let Some(arg) = template.args.first() else {
        return Err(anyhow!(
            "cp_amm template {} missing params arg",
            template.name
        ));
    };

    match arg.ty {
        "SwapParameters" => encode_swap_parameters(buffer),
        "SwapParameters2" => encode_swap_parameters2(buffer),
        other => Err(anyhow!("unsupported cp_amm arg type {}", other)),
    }
}

fn encode_swap_parameters(buffer: &mut Vec<u8>) -> Result<CpAmmFieldLayout> {
    let amount_in = write_u64(buffer, 0)?;
    let min_out = write_u64(buffer, 0)?;
    Ok(CpAmmFieldLayout {
        amount_in,
        minimum_output: min_out,
    })
}

const CP_AMM_SWAP_MODE_EXACT_IN: u8 = 0;

fn encode_swap_parameters2(buffer: &mut Vec<u8>) -> Result<CpAmmFieldLayout> {
    let amount_in = write_u64(buffer, 0)?;
    let min_out = write_u64(buffer, 0)?;
    // Stage1 and the cp_amm adapter exclusively use exact-in semantics today,
    // so always emit the matching swap mode.
    buffer.push(CP_AMM_SWAP_MODE_EXACT_IN);
    Ok(CpAmmFieldLayout {
        amount_in,
        minimum_output: min_out,
    })
}

fn write_u64(buffer: &mut Vec<u8>, value: u64) -> Result<TemplateSlice> {
    let offset = buffer.len();
    buffer.extend_from_slice(&value.to_le_bytes());
    make_slice(offset, 8)
}

fn encode_lb_clmm_args(
    template: &LbClmmSwapTemplate,
    buffer: &mut Vec<u8>,
) -> Result<LbClmmFieldLayout> {
    let mut amount_in = None;
    let mut amount_out = None;
    let mut limit_price = None;
    let mut price_impact_limit = None;
    let mut remaining_accounts_info = None;

    let requires_min_out = !template.name.contains("price_impact");

    for arg in template.args {
        match arg.name {
            "amount_in" | "max_in_amount" => {
                let slice = write_u64(buffer, 0)?;
                amount_in.get_or_insert(slice);
            }
            "min_amount_out" | "out_amount" => {
                let slice = write_u64(buffer, 0)?;
                amount_out = Some(slice);
            }
            "max_price_impact_bps" => {
                let slice = write_u16(buffer, 0)?;
                price_impact_limit = Some(slice);
            }
            "active_id" => {
                let slice = write_option_i32_none(buffer)?;
                limit_price = Some(slice);
            }
            "remaining_accounts_info" => {
                let slice = write_remaining_accounts_info(buffer)?;
                remaining_accounts_info = Some(slice);
            }
            _ if arg.ty == "u64" => {
                let _ = write_u64(buffer, 0)?;
            }
            _ if arg.ty == "u16" => {
                let _ = write_u16(buffer, 0)?;
            }
            _ if arg.ty.starts_with("Option") => {
                return Err(anyhow!(
                    "unsupported optional arg {} on lb_clmm template {}",
                    arg.name,
                    template.name
                ));
            }
            _ if arg.ty == "RemainingAccountsInfo" => {
                let slice = write_remaining_accounts_info(buffer)?;
                remaining_accounts_info = Some(slice);
            }
            other => {
                return Err(anyhow!("unsupported lb_clmm arg {}", other));
            }
        }
    }

    let amount_in = amount_in.ok_or_else(|| {
        anyhow!(
            "lb_clmm template {} missing primary amount field",
            template.name
        )
    })?;
    if requires_min_out && amount_out.is_none() {
        return Err(anyhow!(
            "lb_clmm template {} missing secondary amount field",
            template.name
        ));
    }

    Ok(LbClmmFieldLayout {
        amount_in,
        amount_out,
        limit_price,
        price_impact_bps: price_impact_limit,
        remaining_accounts_info,
    })
}

fn encode_whirlpool_args(
    template: &WhirlpoolSwapTemplate,
    buffer: &mut Vec<u8>,
) -> Result<WhirlpoolFieldLayout> {
    let mut amount_in = None;
    let mut amount_out = None;
    let mut amount_specified_is_input = None;
    let mut sqrt_price_limit = None;
    let mut remaining_accounts_info = None;

    for arg in template.args {
        match arg.name {
            "amount" => {
                let value = parse_u64(arg)?;
                amount_in = Some(write_u64(buffer, value)?);
            }
            "otherAmountThreshold" | "other_amount_threshold" => {
                let value = parse_u64(arg)?;
                amount_out = Some(write_u64(buffer, value)?);
            }
            "sqrtPriceLimit" | "sqrt_price_limit" => {
                let value = parse_u128(arg)?;
                sqrt_price_limit = Some(write_u128(buffer, value)?);
            }
            "amountSpecifiedIsInput" | "amount_specified_is_input" => {
                let literal = parse_bool(arg)? != 0;
                let slice = write_bool(buffer, literal)?;
                amount_specified_is_input = Some(slice);
            }
            "aToB" | "a_to_b" => {
                let literal = parse_bool(arg)?;
                buffer.push(literal);
            }
            "remainingAccountsInfo" | "remaining_accounts_info" => {
                let slice = write_option_remaining_accounts_info(buffer)?;
                remaining_accounts_info = Some(slice);
            }
            other => {
                return Err(anyhow!("unsupported whirlpool arg {}", other));
            }
        }
    }

    let amount_in = amount_in.ok_or_else(|| anyhow!("whirlpool template missing amount arg"))?;
    let amount_out =
        amount_out.ok_or_else(|| anyhow!("whirlpool template missing otherAmountThreshold arg"))?;
    let amount_specified_is_input = amount_specified_is_input
        .ok_or_else(|| anyhow!("whirlpool template missing amountSpecifiedIsInput arg"))?;

    Ok(WhirlpoolFieldLayout {
        amount_in,
        amount_out,
        amount_specified_is_input,
        sqrt_price_limit,
        remaining_accounts_info,
    })
}

fn write_u16(buffer: &mut Vec<u8>, value: u16) -> Result<TemplateSlice> {
    let offset = buffer.len();
    buffer.extend_from_slice(&value.to_le_bytes());
    make_slice(offset, 2)
}

fn write_option_i32_none(buffer: &mut Vec<u8>) -> Result<TemplateSlice> {
    let offset = buffer.len();
    buffer.push(0); // None discriminant
    buffer.extend_from_slice(&0i32.to_le_bytes());
    make_slice(offset, 5)
}

fn write_remaining_accounts_info(buffer: &mut Vec<u8>) -> Result<TemplateSlice> {
    let offset = buffer.len();
    buffer.extend_from_slice(&0u32.to_le_bytes());
    make_slice(offset, 4)
}

fn write_option_remaining_accounts_info(buffer: &mut Vec<u8>) -> Result<TemplateSlice> {
    let offset = buffer.len();
    buffer.push(0); // None discriminant
    make_slice(offset, 1)
}

fn write_u128(buffer: &mut Vec<u8>, value: u128) -> Result<TemplateSlice> {
    let offset = buffer.len();
    buffer.extend_from_slice(&value.to_le_bytes());
    make_slice(offset, 16)
}

fn write_bool(buffer: &mut Vec<u8>, value: bool) -> Result<TemplateSlice> {
    let offset = buffer.len();
    buffer.push(value as u8);
    make_slice(offset, 1)
}

fn encode_arg(arg: &ArgSpec, buffer: &mut Vec<u8>) -> Result<TemplateSlice> {
    let offset = buffer.len();
    match arg.ty {
        "u64" => {
            let value = parse_u64(arg)?;
            buffer.extend_from_slice(&value.to_le_bytes());
            make_slice(offset, 8)
        }
        "u128" => {
            let value = parse_u128(arg)?;
            buffer.extend_from_slice(&value.to_le_bytes());
            make_slice(offset, 16)
        }
        "bool" => {
            let value = parse_bool(arg)?;
            buffer.push(value);
            make_slice(offset, 1)
        }
        other => Err(anyhow!("unsupported arg type {}", other)),
    }
}

fn parse_u64(arg: &ArgSpec) -> Result<u64> {
    match arg.fixed_value {
        Some(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("invalid u64 literal {} for arg {}", raw, arg.name)),
        None => Ok(0),
    }
}

fn parse_u128(arg: &ArgSpec) -> Result<u128> {
    match arg.fixed_value {
        Some(raw) => raw
            .parse::<u128>()
            .with_context(|| format!("invalid u128 literal {} for arg {}", raw, arg.name)),
        None => Ok(0),
    }
}

fn parse_bool(arg: &ArgSpec) -> Result<u8> {
    match arg.fixed_value {
        Some(raw) if raw.eq_ignore_ascii_case("true") => Ok(1),
        Some(raw) if raw.eq_ignore_ascii_case("false") => Ok(0),
        Some(raw) => Err(anyhow!("invalid bool literal {} for arg {}", raw, arg.name)),
        None => Ok(0),
    }
}

fn make_slice(offset: usize, length: usize) -> Result<TemplateSlice> {
    let offset = u16::try_from(offset)
        .map_err(|_| anyhow!("template data exceeds supported offset space"))?;
    let length = u8::try_from(length)
        .map_err(|_| anyhow!("template field larger than supported slice length"))?;
    Ok(TemplateSlice::new(offset, length))
}

fn compute_template_hash(program_id: &Pubkey, accounts: &[AccountMeta], data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(program_id.as_ref());
    for meta in accounts {
        hasher.update(meta.pubkey.as_ref());
        hasher.update([meta.is_signer as u8, meta.is_writable as u8]);
    }
    hasher.update(data);
    let digest = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&digest);
    hash
}

fn synth_pool_id(program_id: &Pubkey, direction: SwapTemplateDirection) -> Pubkey {
    let mut hasher = Sha256::new();
    hasher.update(program_id.as_ref());
    hasher.update([direction_tag(direction)]);
    let digest = hasher.finalize();
    Pubkey::new_from_array(digest.into())
}

fn direction_tag(direction: SwapTemplateDirection) -> u8 {
    match direction {
        SwapTemplateDirection::BaseIn => 0,
        SwapTemplateDirection::BaseOut => 1,
    }
}
