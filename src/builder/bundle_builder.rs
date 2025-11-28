use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::{v0::Message, AddressLookupTableAccount, VersionedMessage};
use solana_sdk::pubkey::Pubkey;

use crate::builder::templates::SwapTemplateDirection;
use crate::template::TemplateContext;

use super::{
    AmmV3Field, CpAmmField, InstructionTemplate, LbClmmField, LifinityField, TemplateCache,
    TemplateCacheKey, TemplateFieldKind, TemplatePatch, WhirlpoolField,
};

static CP_AMM_PROGRAM_ID: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG")
        .expect("static cp_amm program id")
});
static LB_CLMM_PROGRAM_ID: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo")
        .expect("static lb_clmm program id")
});
static MEMO_PROGRAM_ID: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").expect("static memo program id")
});
static JITO_DONT_FRONT_ACCOUNT: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str("jitodontfront111111111111111111111111111111")
        .expect("static jitodontfront pubkey")
});

const CP_AMM_HOST_FEE_INDEX: usize = 11;
const LB_CLMM_HOST_FEE_INDEX: usize = 9;
const DEFAULT_SLIPPAGE_TOLERANCE_BPS: u16 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleInstructionPlan {
    pub pool_id: String,
    pub program_id: String,
    pub input_mint: String,
    pub output_mint: String,
    pub amount_in: u64,
    pub expected_output_amount: u64,
    pub fee_bps: u16,
    pub direction: SwapTemplateDirection,
    pub sqrt_price_limit: Option<u128>,
    pub min_output_amount: Option<u64>,
    pub limit_price: Option<i64>,
    pub price_impact_limit_bps: Option<u16>,
    pub slot_padding: Option<u64>,
    pub context: TemplateContext,
    pub lookup_table: Option<BundleLookupTablePlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleLookupTablePlan {
    pub handle_id: u32,
    pub table_address: String,
    pub authority: String,
    pub creation_slot: u64,
    pub last_extended_slot: u64,
    pub accounts: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BundleBuildContext {
    pub seq: u64,
    pub instructions: Vec<Instruction>,
    pub lookup_tables: Vec<AddressLookupTableAccount>,
    pub message: VersionedMessage,
}

pub fn build_bundle(
    seq: u64,
    payer: Pubkey,
    cache: &TemplateCache,
    blockhash: Hash,
    prefix_instructions: &[Instruction],
    plans: &[BundleInstructionPlan],
    suffix_instructions: &[Instruction],
) -> Result<BundleBuildContext> {
    if plans.is_empty() {
        bail!("bundle {} did not include any swap legs", seq);
    }

    let mut instructions = Vec::with_capacity(
        prefix_instructions.len() + suffix_instructions.len() + plans.len().saturating_mul(2) + 1,
    );
    if let Some(limit) = required_compute_unit_limit(plans) {
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(limit));
    }
    instructions.extend_from_slice(prefix_instructions);
    for plan in plans {
        let template = resolve_template(cache, plan)?;
        let instruction = hydrate_instruction(plan, template.as_ref(), payer)
            .with_context(|| format!("failed to hydrate plan for pool {}", plan.pool_id))?;
        instructions.push(instruction);
    }
    instructions.extend_from_slice(suffix_instructions);

    let lookup_tables = materialize_lookup_tables(plans)?;
    let message = Message::try_compile(&payer, &instructions, &lookup_tables, blockhash)
        .context("failed to assemble MessageV0 from planned instructions")?;

    Ok(BundleBuildContext {
        seq,
        instructions,
        lookup_tables,
        message: VersionedMessage::V0(message),
    })
}

fn resolve_template(
    cache: &TemplateCache,
    plan: &BundleInstructionPlan,
) -> Result<Arc<InstructionTemplate>> {
    let pool = Pubkey::from_str(&plan.pool_id)
        .with_context(|| format!("invalid pool id {}", plan.pool_id))?;
    let key = TemplateCacheKey::new(pool, plan.direction, plan.fee_bps);
    cache.get(&key).with_context(|| {
        format!(
            "missing template for pool {} direction {:?}",
            plan.pool_id, plan.direction
        )
    })
}

fn hydrate_instruction(
    plan: &BundleInstructionPlan,
    template: &InstructionTemplate,
    payer: Pubkey,
) -> Result<Instruction> {
    let mut data = (*template.data).clone();
    for patch in template.patches.iter() {
        apply_patch(plan, patch, &mut data)?;
    }

    let mut accounts = template.accounts.as_ref().to_vec();
    apply_template_context(plan, template, &mut accounts)?;
    rewrite_placeholder_signers(plan, &mut accounts, payer);

    Ok(Instruction {
        program_id: template.program_id,
        accounts,
        data,
    })
}

fn apply_patch(plan: &BundleInstructionPlan, patch: &TemplatePatch, data: &mut [u8]) -> Result<()> {
    let offset = patch.offset as usize;
    let length = patch.length as usize;
    let slice = data
        .get_mut(offset..offset + length)
        .ok_or_else(|| anyhow!("template patch out of bounds"))?;

    match patch.kind {
        TemplateFieldKind::AmmV3(field) => match field {
            AmmV3Field::AmountSpecified => write_i128(plan.amount_in as i128, slice),
            AmmV3Field::OtherAmountThreshold => {
                let threshold = guarded_expected_output(plan);
                write_u128(threshold as u128, slice)
            }
            AmmV3Field::SqrtPriceLimit => {
                if let Some(limit) = plan.sqrt_price_limit {
                    write_u128(limit, slice);
                }
            }
        },
        TemplateFieldKind::CpAmm(field) => match field {
            CpAmmField::AmountIn => write_u128(plan.amount_in as u128, slice),
            CpAmmField::MinimumOutput => {
                let min_out = plan
                    .min_output_amount
                    .unwrap_or_else(|| guarded_expected_output(plan));
                write_u128(min_out as u128, slice)
            }
        },
        TemplateFieldKind::LbClmm(field) => match field {
            LbClmmField::AmountIn => write_u128(plan.amount_in as u128, slice),
            LbClmmField::AmountOut => {
                let threshold = plan
                    .min_output_amount
                    .unwrap_or_else(|| guarded_expected_output(plan));
                write_u128(threshold as u128, slice)
            }
            LbClmmField::LimitPrice => {
                if let Some(limit) = plan.limit_price {
                    write_option_i32(limit, slice)?;
                }
            }
            LbClmmField::PriceImpactLimit => {
                if let Some(limit) = plan.price_impact_limit_bps {
                    write_u128(limit as u128, slice);
                }
            }
        },
        TemplateFieldKind::Lifinity(field) => match field {
            LifinityField::AmountIn => write_u128(plan.amount_in as u128, slice),
            LifinityField::AmountOut => {
                let min_out = plan
                    .min_output_amount
                    .unwrap_or_else(|| guarded_expected_output(plan));
                write_u128(min_out as u128, slice)
            }
            LifinityField::SlotPadding => {
                if let Some(slot) = plan.slot_padding {
                    write_u128(slot as u128, slice);
                }
            }
        },
        TemplateFieldKind::Whirlpool(field) => match field {
            WhirlpoolField::AmountIn => write_u128(plan.amount_in as u128, slice),
            WhirlpoolField::AmountOut => {
                let threshold = plan
                    .min_output_amount
                    .unwrap_or_else(|| guarded_expected_output(plan));
                write_u128(threshold as u128, slice)
            }
            WhirlpoolField::SqrtPriceLimit => {
                if let Some(limit) = plan.sqrt_price_limit {
                    write_u128(limit, slice);
                }
            }
        },
        TemplateFieldKind::Custom(_) => {}
    }

    Ok(())
}

fn write_u128(value: u128, dst: &mut [u8]) {
    let bytes = value.to_le_bytes();
    let len = dst.len().min(bytes.len());
    dst[..len].copy_from_slice(&bytes[..len]);
}

fn write_i128(value: i128, dst: &mut [u8]) {
    let bytes = value.to_le_bytes();
    let len = dst.len().min(bytes.len());
    dst[..len].copy_from_slice(&bytes[..len]);
}

fn write_option_i32(value: i64, dst: &mut [u8]) -> Result<()> {
    if dst.len() != 5 {
        bail!(
            "unexpected option encoding length {}, expected 5",
            dst.len()
        );
    }
    let clamped =
        i32::try_from(value).map_err(|_| anyhow!("limit price {} exceeds i32 bounds", value))?;
    dst[0] = 1; // Some discriminant
    dst[1..].copy_from_slice(&clamped.to_le_bytes());
    Ok(())
}

fn guarded_expected_output(plan: &BundleInstructionPlan) -> u64 {
    let tolerance = u64::from(
        plan.context
            .slippage_tolerance_bps
            .unwrap_or(DEFAULT_SLIPPAGE_TOLERANCE_BPS),
    );
    apply_slippage_guard(plan.expected_output_amount, tolerance)
}

fn apply_slippage_guard(amount: u64, tolerance_bps: u64) -> u64 {
    if tolerance_bps == 0 || amount == 0 {
        return amount;
    }
    let slip = amount.saturating_mul(tolerance_bps).saturating_div(10_000);
    amount.saturating_sub(slip)
}

fn apply_template_context(
    plan: &BundleInstructionPlan,
    template: &InstructionTemplate,
    accounts: &mut Vec<AccountMeta>,
) -> Result<()> {
    if let Some(host_fee) = plan.context.host_fee_account {
        apply_host_fee_account(template.program_id, accounts.as_mut_slice(), host_fee)?;
    }

    if plan.context.requires_memo {
        ensure_memo_account(accounts);
    }

    if !plan.context.remaining_accounts.is_empty() {
        accounts.extend(plan.context.remaining_accounts.iter().cloned());
    }

    ensure_dont_front_account(accounts);

    Ok(())
}

pub(crate) fn hydrate_accounts_for_plan(
    cache: &TemplateCache,
    plan: &BundleInstructionPlan,
    payer: Pubkey,
) -> Result<Vec<AccountMeta>> {
    let template = resolve_template(cache, plan)?;
    let mut accounts = template.accounts.as_ref().to_vec();
    apply_template_context(plan, template.as_ref(), &mut accounts)?;
    rewrite_placeholder_signers(plan, &mut accounts, payer);
    Ok(accounts)
}

fn rewrite_placeholder_signers(
    plan: &BundleInstructionPlan,
    accounts: &mut [AccountMeta],
    default_payer: Pubkey,
) {
    let authority = plan.context.payer.unwrap_or(default_payer);
    for meta in accounts.iter_mut().filter(|meta| meta.is_signer) {
        if meta.pubkey == Pubkey::default() {
            meta.pubkey = authority;
        }
    }
}

fn ensure_memo_account(accounts: &mut Vec<AccountMeta>) {
    if accounts.iter().any(|meta| meta.pubkey == *MEMO_PROGRAM_ID) {
        return;
    }
    accounts.push(AccountMeta::new_readonly(*MEMO_PROGRAM_ID, false));
}

fn ensure_dont_front_account(accounts: &mut Vec<AccountMeta>) {
    if accounts
        .iter()
        .any(|meta| meta.pubkey == *JITO_DONT_FRONT_ACCOUNT)
    {
        return;
    }
    accounts.push(AccountMeta::new_readonly(*JITO_DONT_FRONT_ACCOUNT, false));
}

fn apply_host_fee_account(
    program_id: Pubkey,
    accounts: &mut [AccountMeta],
    host_fee: Pubkey,
) -> Result<()> {
    let index = if program_id == *CP_AMM_PROGRAM_ID {
        CP_AMM_HOST_FEE_INDEX
    } else if program_id == *LB_CLMM_PROGRAM_ID {
        LB_CLMM_HOST_FEE_INDEX
    } else {
        bail!("program {} does not support host fee overrides", program_id);
    };

    let meta = accounts
        .get_mut(index)
        .ok_or_else(|| anyhow!("template missing host fee slot at index {}", index))?;
    meta.pubkey = host_fee;
    Ok(())
}

fn required_compute_unit_limit(plans: &[BundleInstructionPlan]) -> Option<u32> {
    plans
        .iter()
        .filter_map(|plan| plan.context.compute_unit_limit)
        .max()
}

fn materialize_lookup_tables(
    plans: &[BundleInstructionPlan],
) -> Result<Vec<AddressLookupTableAccount>> {
    let mut dedup = BTreeMap::<String, BundleLookupTablePlan>::new();
    for plan in plans {
        if let Some(info) = &plan.lookup_table {
            dedup
                .entry(info.table_address.clone())
                .or_insert_with(|| info.clone());
        }
    }

    let mut accounts = Vec::with_capacity(dedup.len());
    for info in dedup.values() {
        let key = Pubkey::from_str(&info.table_address)
            .with_context(|| format!("invalid ALT address {}", info.table_address))?;
        let resolved = info
            .accounts
            .iter()
            .map(|addr| {
                Pubkey::from_str(addr).with_context(|| format!("invalid ALT member {}", addr))
            })
            .collect::<Result<Vec<_>>>()?;
        accounts.push(AddressLookupTableAccount {
            key,
            addresses: resolved,
        });
    }

    Ok(accounts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use serde_json::json;
    use solana_sdk::instruction::AccountMeta;
    use std::convert::TryInto;
    use std::sync::Arc as StdArc;

    #[test]
    fn builds_message_from_plan() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        insert_template(&cache, pool, SwapTemplateDirection::BaseIn);

        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: Pubkey::new_unique().to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 42,
            expected_output_amount: 84,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext::default(),
            lookup_table: None,
        };

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(1, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");
        assert_eq!(ctx.instructions.len(), 1);
        assert_eq!(ctx.lookup_tables.len(), 0);
        assert_eq!(ctx.instructions[0].data[..8], 42u64.to_le_bytes());
        match &ctx.message {
            VersionedMessage::V0(message) => {
                assert!(message.address_table_lookups.is_empty());
            }
            _ => panic!("expected v0 message"),
        }
    }

    #[test]
    fn deduplicates_lookup_tables() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        insert_template(&cache, pool, SwapTemplateDirection::BaseIn);

        let table_key = Pubkey::new_unique();
        let table_accounts = vec![
            Pubkey::new_unique().to_string(),
            Pubkey::new_unique().to_string(),
        ];
        let lookup = BundleLookupTablePlan {
            handle_id: 1,
            table_address: table_key.to_string(),
            authority: Pubkey::new_unique().to_string(),
            creation_slot: 0,
            last_extended_slot: 0,
            accounts: table_accounts,
        };

        let plan_a = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: Pubkey::new_unique().to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 2,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext::default(),
            lookup_table: Some(lookup.clone()),
        };
        let mut plan_b = plan_a.clone();
        plan_b.amount_in = 3;

        let payer = Pubkey::new_unique();
        let ctx = build_bundle(
            9,
            payer,
            &cache,
            Hash::default(),
            &[],
            &[plan_a, plan_b],
            &[],
        )
        .expect("bundle");
        assert_eq!(ctx.lookup_tables.len(), 1);
    }

    #[test]
    fn adds_compute_budget_instruction_when_requested() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        insert_template(&cache, pool, SwapTemplateDirection::BaseIn);

        let mut plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: Pubkey::new_unique().to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 2,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext::default(),
            lookup_table: None,
        };
        plan.context.compute_unit_limit = Some(400_000);

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(5, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");

        assert_eq!(ctx.instructions.len(), 2);
        assert_eq!(
            ctx.instructions[0].program_id,
            solana_compute_budget_interface::ID
        );
    }

    #[test]
    fn required_compute_budget_limit_prefers_maximum() {
        let mut plan_a = BundleInstructionPlan {
            pool_id: "a".into(),
            program_id: "prog".into(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 1,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext::default(),
            lookup_table: None,
        };
        let mut plan_b = plan_a.clone();
        plan_a.context.compute_unit_limit = Some(300_000);
        plan_b.context.compute_unit_limit = Some(450_000);

        let limit = required_compute_unit_limit(&[plan_a, plan_b]);
        assert_eq!(limit, Some(450_000));
    }

    #[test]
    fn overrides_cp_amm_host_fee_account() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        let mut accounts = Vec::new();
        for _ in 0..=CP_AMM_HOST_FEE_INDEX + 2 {
            accounts.push(AccountMeta::new(Pubkey::new_unique(), false));
        }
        insert_template_with_program(
            &cache,
            pool,
            SwapTemplateDirection::BaseIn,
            *CP_AMM_PROGRAM_ID,
            accounts.clone(),
        );

        let host_fee = Pubkey::new_unique();
        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: CP_AMM_PROGRAM_ID.to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 2,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext {
                host_fee_account: Some(host_fee),
                ..TemplateContext::default()
            },
            lookup_table: None,
        };

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(7, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");
        let ix = &ctx.instructions[0];
        assert_eq!(ix.accounts[CP_AMM_HOST_FEE_INDEX].pubkey, host_fee);
        assert_eq!(ix.accounts.len(), accounts.len() + 1);
        assert_eq!(
            ix.accounts
                .last()
                .map(|meta| meta.pubkey)
                .expect("dont front account"),
            *JITO_DONT_FRONT_ACCOUNT
        );
    }

    #[test]
    fn appends_memo_and_remaining_accounts() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        let account_count = CP_AMM_HOST_FEE_INDEX + 2;
        let accounts = (0..account_count)
            .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
            .collect();
        insert_template_with_program(
            &cache,
            pool,
            SwapTemplateDirection::BaseIn,
            *CP_AMM_PROGRAM_ID,
            accounts,
        );

        let extra = Pubkey::new_unique();
        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: CP_AMM_PROGRAM_ID.to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 2,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext {
                requires_memo: true,
                remaining_accounts: vec![AccountMeta::new(extra, false)],
                ..TemplateContext::default()
            },
            lookup_table: None,
        };

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(11, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");
        let ix = &ctx.instructions[0];
        let memo_pos = ix
            .accounts
            .iter()
            .position(|meta| meta.pubkey == *MEMO_PROGRAM_ID)
            .expect("memo account present");
        let extra_pos = ix
            .accounts
            .iter()
            .position(|meta| meta.pubkey == extra)
            .expect("extra account present");
        assert!(memo_pos < extra_pos, "memo should precede extra accounts");
        assert_eq!(ix.accounts[ix.accounts.len() - 2].pubkey, extra);
        assert_eq!(ix.accounts.last().unwrap().pubkey, *JITO_DONT_FRONT_ACCOUNT);
    }

    #[test]
    fn does_not_duplicate_existing_memo_account() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        let mut accounts = vec![AccountMeta::new(Pubkey::new_unique(), false)];
        accounts.push(AccountMeta::new_readonly(*MEMO_PROGRAM_ID, false));
        insert_template_with_program(
            &cache,
            pool,
            SwapTemplateDirection::BaseIn,
            Pubkey::new_unique(),
            accounts,
        );

        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: "prog".into(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 2,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext {
                requires_memo: true,
                ..TemplateContext::default()
            },
            lookup_table: None,
        };

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(13, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");
        let memo_count = ctx.instructions[0]
            .accounts
            .iter()
            .filter(|meta| meta.pubkey == *MEMO_PROGRAM_ID)
            .count();
        assert_eq!(memo_count, 1);
    }

    #[test]
    fn applies_default_slippage_guard_when_no_min_output_provided() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();
        let accounts = vec![AccountMeta::new(Pubkey::new_unique(), false)];
        insert_template_with_custom_patches(
            &cache,
            pool,
            SwapTemplateDirection::BaseIn,
            program_id,
            accounts,
            vec![TemplatePatch {
                offset: 0,
                length: 8,
                kind: TemplateFieldKind::CpAmm(CpAmmField::MinimumOutput),
            }],
        );

        let expected_output_amount = 1_000_000u64;
        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: program_id.to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 10,
            expected_output_amount,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext::default(),
            lookup_table: None,
        };

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(21, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");
        let encoded = u64::from_le_bytes(ctx.instructions[0].data[..8].try_into().unwrap());
        let tolerance = u64::from(DEFAULT_SLIPPAGE_TOLERANCE_BPS);
        let slip = expected_output_amount
            .saturating_mul(tolerance)
            .saturating_div(10_000);
        assert_eq!(encoded, expected_output_amount.saturating_sub(slip));
    }

    #[test]
    fn honors_explicit_min_output_amount() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();
        let accounts = vec![AccountMeta::new(Pubkey::new_unique(), false)];
        insert_template_with_custom_patches(
            &cache,
            pool,
            SwapTemplateDirection::BaseIn,
            program_id,
            accounts,
            vec![TemplatePatch {
                offset: 0,
                length: 8,
                kind: TemplateFieldKind::CpAmm(CpAmmField::MinimumOutput),
            }],
        );

        let explicit_min = 900_000u64;
        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: program_id.to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 10,
            expected_output_amount: 1_000_000,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: Some(explicit_min),
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext::default(),
            lookup_table: None,
        };

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(23, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");
        let encoded = u64::from_le_bytes(ctx.instructions[0].data[..8].try_into().unwrap());
        assert_eq!(encoded, explicit_min);
    }

    #[test]
    fn replaces_placeholder_signers_with_payer() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        let placeholder = AccountMeta::new(Pubkey::default(), true);
        let accounts = vec![placeholder];
        let program_id = insert_template_with_program(
            &cache,
            pool,
            SwapTemplateDirection::BaseIn,
            Pubkey::new_unique(),
            accounts,
        );

        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: program_id.to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 1,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext::default(),
            lookup_table: None,
        };

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(17, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");
        assert_eq!(ctx.instructions[0].accounts[0].pubkey, payer);
    }

    #[test]
    fn honors_template_context_payer_override() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        let placeholder = AccountMeta::new(Pubkey::default(), true);
        let accounts = vec![placeholder];
        let program_id = insert_template_with_program(
            &cache,
            pool,
            SwapTemplateDirection::BaseIn,
            Pubkey::new_unique(),
            accounts,
        );

        let override_authority = Pubkey::new_unique();
        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: program_id.to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 1,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext {
                payer: Some(override_authority),
                ..TemplateContext::default()
            },
            lookup_table: None,
        };

        let payer = Pubkey::new_unique();
        let ctx =
            build_bundle(19, payer, &cache, Hash::default(), &[], &[plan], &[]).expect("bundle");
        assert_eq!(ctx.instructions[0].accounts[0].pubkey, override_authority);
    }

    #[test]
    fn hydrates_accounts_for_plan_respects_context() {
        let cache = TemplateCache::new(test_config());
        let pool = Pubkey::new_unique();
        let account_count = CP_AMM_HOST_FEE_INDEX + 2;
        let accounts = (0..account_count)
            .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
            .collect();
        insert_template_with_program(
            &cache,
            pool,
            SwapTemplateDirection::BaseIn,
            *CP_AMM_PROGRAM_ID,
            accounts,
        );

        let host_fee = Pubkey::new_unique();
        let extra = Pubkey::new_unique();
        let plan = BundleInstructionPlan {
            pool_id: pool.to_string(),
            program_id: CP_AMM_PROGRAM_ID.to_string(),
            input_mint: "mintA".into(),
            output_mint: "mintB".into(),
            amount_in: 1,
            expected_output_amount: 2,
            fee_bps: 5,
            direction: SwapTemplateDirection::BaseIn,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            slot_padding: None,
            context: TemplateContext {
                host_fee_account: Some(host_fee),
                requires_memo: true,
                remaining_accounts: vec![AccountMeta::new(extra, false)],
                ..TemplateContext::default()
            },
            lookup_table: None,
        };

        let hydrated =
            hydrate_accounts_for_plan(&cache, &plan, Pubkey::new_unique()).expect("accounts");
        assert_eq!(hydrated[CP_AMM_HOST_FEE_INDEX].pubkey, host_fee);
        let memo_pos = hydrated
            .iter()
            .position(|meta| meta.pubkey == *MEMO_PROGRAM_ID)
            .expect("memo account present");
        let extra_pos = hydrated
            .iter()
            .position(|meta| meta.pubkey == extra)
            .expect("extra account present");
        assert!(memo_pos < extra_pos, "memo should precede extra accounts");
        assert_eq!(hydrated[hydrated.len() - 2].pubkey, extra);
        assert_eq!(hydrated.last().unwrap().pubkey, *JITO_DONT_FRONT_ACCOUNT);
    }

    fn insert_template(
        cache: &TemplateCache,
        pool: Pubkey,
        direction: SwapTemplateDirection,
    ) -> Pubkey {
        let program_id = Pubkey::new_unique();
        let accounts = vec![AccountMeta::new(pool, false)];
        insert_template_with_program(cache, pool, direction, program_id, accounts)
    }

    fn insert_template_with_program(
        cache: &TemplateCache,
        pool: Pubkey,
        direction: SwapTemplateDirection,
        program_id: Pubkey,
        accounts: Vec<AccountMeta>,
    ) -> Pubkey {
        let patches = vec![TemplatePatch {
            offset: 0,
            length: 8,
            kind: TemplateFieldKind::CpAmm(CpAmmField::AmountIn),
        }];
        insert_template_with_custom_patches(cache, pool, direction, program_id, accounts, patches)
    }

    fn insert_template_with_custom_patches(
        cache: &TemplateCache,
        pool: Pubkey,
        direction: SwapTemplateDirection,
        program_id: Pubkey,
        accounts: Vec<AccountMeta>,
        patches: Vec<TemplatePatch>,
    ) -> Pubkey {
        let key = TemplateCacheKey::new(pool, direction, 5);
        let template = InstructionTemplate {
            key,
            program_id,
            discriminator: [0u8; 8],
            accounts: StdArc::from(accounts),
            data: StdArc::new(vec![0u8; 8]),
            patches: StdArc::from(patches),
            cu_budget: 0,
            tlv_bytes: 0,
            template_hash: [0u8; 32],
            alt_handle: None,
        };
        cache.insert(template);
        program_id
    }

    fn test_config() -> StdArc<AppConfig> {
        let raw = json!({
            "wallets": {
                "auth": {"path": "/tmp/auth.json"},
                "payer": {"path": "/tmp/payer.json"}
            },
            "jito": {
                "endpoints": ["https://example"],
                "tip_floor_url": "https://example/floor",
                "tip_stream_url": "https://example/stream"
            },
            "corecast": {
                "address": "https://example",
                "authorization": "token",
                "cache": {"warm_capacity": 1, "channel_capacity": 1}
            },
            "tokens": {},
            "program_addresses": [],
            "flash_loans": {
                "FLASH": {
                    "program_id": "Flash1111111111111111111111111111111111",
                    "lending_market": "Flash1111111111111111111111111111111111",
                    "reserve": "Flash1111111111111111111111111111111111",
                    "reserve_liquidity_mint": "Flash1111111111111111111111111111111111",
                    "borrow": {
                        "reserve_liquidity_supply": "Flash1111111111111111111111111111111111"
                    },
                    "repay": {
                        "reserve_liquidity_supply": "Flash1111111111111111111111111111111111",
                        "reserve_liquidity_fee_receiver": "Flash1111111111111111111111111111111111",
                        "host_fee_receiver": "Flash1111111111111111111111111111111111"
                    }
                }
            },
            "quicknode_rpcs": ["https://example"],
            "protocols": {},
            "perf": {
                "ingest_core": 0,
                "eval_core": 0,
                "submit_core": 0,
                "ring_capacity": 1,
                "opportunity_slab": 1,
                "bundle_slab": 1
            }
        });
        let config: AppConfig = serde_json::from_value(raw).expect("test config");
        StdArc::new(config)
    }
}
