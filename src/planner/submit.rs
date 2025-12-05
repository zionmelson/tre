use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, ensure, Context, Result};
use bincode::serialize;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::rent::Rent;
use solana_sdk::sysvar;
use solana_system_interface::{instruction as system_instruction, program as system_program};
use tracing::{debug, info, warn};

use crate::builder::templates::SwapTemplateDirection;
use crate::builder::{
    BuilderHandle, BuilderStage, BundleInstructionPlan, BundleLookupTablePlan, FlashLoanRoutePlan,
};
use crate::core::{pin_current_thread, Lease, RingConsumer, RingProducer};
use crate::telemetry::{self, Stage2PlanEvent};

use crate::hydration::types::{PoolQuoteDirection, LB_CLMM_PROGRAM_ID};
use crate::planner::types::{
    BundleCandidate, FlashLoanSummary, PlannedInstruction, PlannedLookupTable, RouteLegPlan,
    RoutePlan,
};

pub fn spawn_submit_stage(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    producer: RingProducer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("stage2_submit".into())
        .spawn(move || run(stop, core_id, consumer, producer, builder))
        .map_err(Into::into)
}

fn run(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    producer: RingProducer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    let planner = InstructionPlanner::new(Some(&builder));
    let mut last_alt_stats = Instant::now() - Duration::from_secs(30);
    let mev_revision = builder.mev_guard().current_revision();
    info!(mev_revision, "stage2 submit online");
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::SubmitHotPath);
        if last_alt_stats.elapsed() >= Duration::from_secs(30) {
            log_alt_manager_status(&builder);
            last_alt_stats = Instant::now();
        }
        if let Some(bundle) = consumer.pop() {
            process_bundle(bundle, &planner, &builder, &producer);
        } else {
            thread::yield_now();
            thread::sleep(Duration::from_millis(10));
        }
    }
    info!("stage2 submit shutting down");
    Ok(())
}

fn process_bundle(
    mut bundle: Lease<BundleCandidate>,
    planner: &InstructionPlanner,
    builder: &BuilderHandle,
    producer: &RingProducer<Lease<BundleCandidate>>,
) {
    match prepare_bundle(&mut bundle, planner, builder) {
        Ok(()) => {
            if bundle.bundle_context.is_some() {
                if producer.push(bundle).is_err() {
                    warn!("submission queue rejected bundle candidate");
                }
            } else {
                warn!(
                    seq = bundle.seq,
                    "bundle missing context after planning; dropping"
                );
            }
        }
        Err(Stage2Failure::Plan(err)) => {
            handle_stage2_failure(bundle, err, FailureKind::Plan, builder);
        }
        Err(Stage2Failure::Prepare(err)) => {
            handle_stage2_failure(bundle, err, FailureKind::Prepare, builder);
        }
    }
}

fn handle_stage2_failure(
    mut bundle: Lease<BundleCandidate>,
    err: anyhow::Error,
    kind: FailureKind,
    builder: &BuilderHandle,
) {
    let attempt = bundle.stage2_attempts.saturating_add(1);
    bundle.stage2_attempts = attempt;
    warn!(seq = bundle.seq, attempt, error = %err, ?kind, "stage2 planning failure");
    match kind {
        FailureKind::Plan => telemetry::record_stage2_plan_event(Stage2PlanEvent::PlanFailed),
        FailureKind::Prepare => telemetry::record_stage2_plan_event(Stage2PlanEvent::PrepareFailed),
    }
    if let Some(plan) = bundle.flash_loan_reservation.take() {
        builder.flash_loan_planner().release_route(&plan);
    }
}

#[derive(Debug)]
enum FailureKind {
    Plan,
    Prepare,
}

fn bundle_instruction_plan_from(ix: &PlannedInstruction) -> BundleInstructionPlan {
    BundleInstructionPlan {
        pool_id: ix.pool_id.clone(),
        program_id: ix.program_id.clone(),
        input_mint: ix.input_mint.clone(),
        output_mint: ix.output_mint.clone(),
        amount_in: ix.amount_specified,
        input_amount: ix.input_amount,
        expected_output_amount: ix.estimated_output_amount,
        amount_specified_is_input: ix.amount_specified_is_input,
        fee_bps: u16::try_from(ix.fee_bps).unwrap_or(u16::MAX),
        template_fee_bps: ix.template_fee_bps,
        direction: match ix.direction {
            PoolQuoteDirection::BaseToQuote => SwapTemplateDirection::BaseIn,
            PoolQuoteDirection::QuoteToBase => SwapTemplateDirection::BaseOut,
        },
        sqrt_price_limit: ix.sqrt_price_limit,
        min_output_amount: ix.min_output_amount,
        limit_price: ix.limit_price,
        price_impact_limit_bps: ix.price_impact_limit_bps,
        slot_padding: None,
        context: ix.context.clone(),
        lookup_table: ix.lookup_table.as_ref().map(|lt| BundleLookupTablePlan {
            handle_id: lt.handle_id,
            table_address: lt.table_address.clone(),
            authority: lt.authority.clone(),
            creation_slot: lt.creation_slot,
            last_extended_slot: lt.last_extended_slot,
            accounts: lt.accounts.clone(),
        }),
    }
}

fn build_tip_instruction(bundle: &BundleCandidate, payer: Pubkey) -> Option<Instruction> {
    let lamports = bundle.tip_lamports?;
    let recipient = bundle.tip_account?;
    if lamports == 0 {
        return None;
    }
    let data = serialize(&system_instruction::SystemInstruction::Transfer { lamports })
        .expect("serialize system transfer");
    Some(Instruction {
        program_id: system_program::ID,
        accounts: vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(recipient, false),
        ],
        data,
    })
}

fn compute_budget_instruction_count(swaps: &[PlannedInstruction]) -> usize {
    let needs_limit = swaps
        .iter()
        .any(|ix| ix.context.compute_unit_limit.is_some());
    let needs_price = swaps
        .iter()
        .any(|ix| ix.context.compute_unit_price.is_some());

    needs_limit as usize + needs_price as usize
}

#[derive(Debug)]
enum Stage2Failure {
    Plan(anyhow::Error),
    Prepare(anyhow::Error),
}

#[derive(Default)]
struct PlannedRouteInstructions {
    swap_instructions: Vec<PlannedInstruction>,
    prefix_instructions: Vec<Instruction>,
    suffix_instructions: Vec<Instruction>,
    flash_loan_plan: Option<FlashLoanInstructionPlan>,
}

fn prepare_bundle(
    bundle: &mut Lease<BundleCandidate>,
    planner: &InstructionPlanner,
    builder: &BuilderHandle,
) -> Result<(), Stage2Failure> {
    if bundle.stage2_attempts == 0 {
        telemetry::record_detection_latency(bundle.created_at.elapsed());
    }

    if let Some(route_plan) = bundle.route_plan.as_ref() {
        let planned = planner
            .plan(route_plan, bundle.seq, bundle.tip_lamports)
            .map_err(Stage2Failure::Plan)?;
        build_multi_leg_bundle(bundle, planned, builder)
    } else {
        let detection_latency_ms = duration_ms(bundle.created_at.elapsed());
        info!(
            seq = bundle.seq,
            pool = %bundle.pool_id,
            edge_bps = bundle.estimated_edge_bps,
            detection_latency_ms,
            "submitting single-leg bundle"
        );
        Ok(())
    }
}

fn build_multi_leg_bundle(
    bundle: &mut Lease<BundleCandidate>,
    planned: PlannedRouteInstructions,
    builder: &BuilderHandle,
) -> Result<(), Stage2Failure> {
    let PlannedRouteInstructions {
        swap_instructions,
        mut prefix_instructions,
        mut suffix_instructions,
        flash_loan_plan,
    } = planned;

    if let Some(tip_ix) = build_tip_instruction(bundle, builder.payer_pubkey()) {
        prefix_instructions.push(tip_ix);
    }

    let mut flash_loan_plan = flash_loan_plan;
    if let Some(plan) = flash_loan_plan.as_mut() {
        let borrow_prefix_index = match plan.borrow_prefix_index {
            Some(idx) => idx,
            None => {
                builder.flash_loan_planner().release_route(&plan.route_plan);
                return Err(Stage2Failure::Plan(anyhow!(
                    "missing flash-loan borrow prefix index"
                )));
            }
        };
        let compute_budget_ixs = compute_budget_instruction_count(&swap_instructions);
        let borrow_instruction_index = match compute_budget_ixs.checked_add(borrow_prefix_index) {
            Some(idx) => idx,
            None => {
                builder.flash_loan_planner().release_route(&plan.route_plan);
                return Err(Stage2Failure::Plan(anyhow!(
                    "flash-loan borrow instruction index overflow"
                )));
            }
        };
        if borrow_instruction_index > u8::MAX as usize {
            builder.flash_loan_planner().release_route(&plan.route_plan);
            return Err(Stage2Failure::Plan(anyhow!(
                "flash-loan borrow instruction index exceeded u8"
            )));
        }
        let repay_instruction = plan.route_plan.repay_instruction(
            plan.repay_source_liquidity,
            plan.repay_authority,
            borrow_instruction_index as u8,
        );
        suffix_instructions.push(repay_instruction);
    }

    builder.mev_guard().record_protected_bundle();
    bundle.instructions = swap_instructions.clone();
    let bundle_plan = swap_instructions
        .iter()
        .map(bundle_instruction_plan_from)
        .collect::<Vec<_>>();
    let flash_summary = flash_loan_plan
        .as_ref()
        .map(|plan| flash_loan_summary_from(&plan.route_plan));

    match builder.prepare_bundle(
        bundle.seq,
        &prefix_instructions,
        &bundle_plan,
        &suffix_instructions,
    ) {
        Ok(context) => {
            bundle.bundle_context = Some(context);
            bundle.flash_loan = flash_summary;
        }
        Err(err) => {
            if let Some(plan) = flash_loan_plan.as_ref() {
                builder.flash_loan_planner().release_route(&plan.route_plan);
            }
            return Err(Stage2Failure::Prepare(err));
        }
    }

    if let Some(plan) = flash_loan_plan {
        builder.flash_loan_planner().release_route(&plan.route_plan);
    }

    let detection_latency_ms = duration_ms(bundle.created_at.elapsed());
    info!(
        seq = bundle.seq,
        edge_bps = bundle.estimated_edge_bps,
        hop_count = swap_instructions.len(),
        detection_latency_ms,
        "submitting multi-leg bundle",
    );
    for (idx, ix) in swap_instructions.iter().enumerate() {
        info!(
            seq = bundle.seq,
            step = idx,
            pool = %ix.pool_id,
            input = %ix.input_mint,
            output = %ix.output_mint,
            amount = ix.input_amount,
            lookup_table = ix
                .lookup_table
                .as_ref()
                .map(|lt| lt.table_address.as_str()),
            lookup_table_accounts = ix
                .lookup_table
                .as_ref()
                .map(|lt| lt.accounts.len()),
            "constructed instruction"
        );
    }
    Ok(())
}

impl PlannedRouteInstructions {
    fn with_swaps(swaps: Vec<PlannedInstruction>) -> Self {
        Self {
            swap_instructions: swaps,
            prefix_instructions: Vec::new(),
            suffix_instructions: Vec::new(),
            flash_loan_plan: None,
        }
    }
}

struct FlashLoanInstructionPlan {
    route_plan: FlashLoanRoutePlan,
    borrow_instruction: Instruction,
    tip_funding: Option<TipFundingPlan>,
    repay_source_liquidity: Pubkey,
    repay_authority: Pubkey,
    borrow_prefix_index: Option<usize>,
}

fn flash_loan_summary_from(plan: &FlashLoanRoutePlan) -> FlashLoanSummary {
    FlashLoanSummary {
        plan_id: plan.id,
        token: plan.token.clone(),
        liquidity_mint: plan.accounts.reserve_liquidity_mint.to_string(),
        borrow_amount: plan.borrow_amount,
        repay_amount: plan.repay_amount,
        fee_amount: plan.fee_amount,
        program_id: plan.accounts.program_id.to_string(),
        lending_market: plan.accounts.lending_market.to_string(),
        reserve: plan.accounts.reserve.to_string(),
    }
}

struct TipFundingPlan {
    create_account: Instruction,
    init_account: Instruction,
    siphon_instruction: Instruction,
    close_account: Instruction,
}

const SPL_TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const WSOL_MINT_PUBKEY: Pubkey = pubkey!("So11111111111111111111111111111111111111112");
const TIP_BUFFER_SEED_PREFIX: &str = "tipbuf";
const SPL_TOKEN_ACCOUNT_LEN: usize = 165;

struct InstructionPlanner<'a> {
    builder: Option<&'a BuilderHandle>,
    payer: Option<Pubkey>,
}

impl<'a> InstructionPlanner<'a> {
    fn new(builder: Option<&'a BuilderHandle>) -> Self {
        let payer = builder.map(|b| b.payer_pubkey());
        Self { builder, payer }
    }

    fn plan(
        &self,
        route: &RoutePlan,
        bundle_seq: u64,
        tip_lamports: Option<u64>,
    ) -> anyhow::Result<PlannedRouteInstructions> {
        let mut instructions = Vec::with_capacity(route.legs.len());
        for leg in &route.legs {
            self.ensure_leg_ready(leg)?;
            let mut context = leg.context.clone();
            if context.slippage_tolerance_bps.is_none() {
                context.slippage_tolerance_bps = derive_slippage_tolerance_bps(leg);
            }
            let amount_specified = if leg.amount_specified_is_input {
                leg.input_amount
            } else {
                leg.estimated_output_amount
            };
            instructions.push(PlannedInstruction {
                pool_id: leg.pool_id.clone(),
                program_id: leg.program_id.clone(),
                input_mint: leg.input_mint.clone(),
                output_mint: leg.output_mint.clone(),
                input_amount: leg.input_amount,
                amount_specified,
                amount_specified_is_input: leg.amount_specified_is_input,
                estimated_output_amount: leg.estimated_output_amount,
                fee_bps: leg.fee_bps,
                template_fee_bps: leg.template_fee_bps,
                direction: leg.direction,
                lookup_table: self.lookup_table_for_leg(leg),
                sqrt_price_limit: leg.sqrt_price_limit,
                min_output_amount: leg.min_output_amount,
                limit_price: leg.limit_price,
                price_impact_limit_bps: leg.price_impact_limit_bps,
                context,
            });
        }

        self.ensure_no_duplicate_heavy_legs(&instructions)?;
        self.warn_missing_lookup_tables(&instructions);

        let mut planned = PlannedRouteInstructions::with_swaps(instructions);
        if route.flash_loan_permitted {
            if let Some(first_swap) = planned.swap_instructions.first() {
                if let Some(mut flash_plan) =
                    self.plan_flash_loan(route, first_swap, bundle_seq, tip_lamports)?
                {
                    if let Some(tip_plan) = flash_plan.tip_funding.as_ref() {
                        planned
                            .prefix_instructions
                            .push(tip_plan.create_account.clone());
                        planned
                            .prefix_instructions
                            .push(tip_plan.init_account.clone());
                    }
                    let borrow_index = planned.prefix_instructions.len();
                    planned
                        .prefix_instructions
                        .push(flash_plan.borrow_instruction.clone());
                    flash_plan.borrow_prefix_index = Some(borrow_index);
                    if let Some(tip_plan) = flash_plan.tip_funding.as_ref() {
                        planned
                            .prefix_instructions
                            .push(tip_plan.siphon_instruction.clone());
                        planned
                            .prefix_instructions
                            .push(tip_plan.close_account.clone());
                    }
                    planned.flash_loan_plan = Some(flash_plan);
                }
            }
        }

        Ok(planned)
    }

    fn ensure_no_duplicate_heavy_legs(&self, instructions: &[PlannedInstruction]) -> Result<()> {
        let mut lb_pools = HashSet::new();
        for ix in instructions {
            if ix.program_id == LB_CLMM_PROGRAM_ID && !lb_pools.insert(ix.pool_id.clone()) {
                bail!(
                    "route contains duplicate lb_clmm leg for pool {}",
                    ix.pool_id
                );
            }
        }
        Ok(())
    }

    fn ensure_leg_ready(&self, leg: &RouteLegPlan) -> Result<()> {
        if !leg.template_ready {
            telemetry::record_stage2_pool_block(&leg.pool_id);
            bail!("template not ready for pool {}", leg.pool_id);
        }
        if !leg.alt_ready {
            telemetry::record_stage2_pool_block(&leg.pool_id);
            bail!("ALT not ready for pool {}", leg.pool_id);
        }
        Ok(())
    }

    fn lookup_table_for_leg(&self, leg: &RouteLegPlan) -> Option<PlannedLookupTable> {
        let builder = self.builder?;
        let pool_key = match Pubkey::from_str(&leg.pool_id) {
            Ok(pk) => pk,
            Err(err) => {
                warn!(pool = %leg.pool_id, error = %err, "invalid pool id for ALT lookup");
                return None;
            }
        };

        let handle = match builder.alt_manager().handle_for_pool(pool_key) {
            Some(handle) => handle,
            None => {
                debug!(pool = %leg.pool_id, "no ALT handle registered for pool");
                return None;
            }
        };

        builder
            .alt_manager()
            .table_info(handle)
            .map(|info| PlannedLookupTable {
                handle_id: handle.0,
                table_address: info.address.to_string(),
                authority: info.authority.to_string(),
                creation_slot: info.creation_slot,
                last_extended_slot: info.last_extended_slot,
                accounts: info.addresses.iter().map(|pk| pk.to_string()).collect(),
            })
    }

    fn plan_flash_loan(
        &self,
        route: &RoutePlan,
        first_swap: &PlannedInstruction,
        bundle_seq: u64,
        tip_lamports: Option<u64>,
    ) -> Result<Option<FlashLoanInstructionPlan>> {
        let builder = match self.builder {
            Some(handle) => handle,
            None => return Ok(None),
        };
        let payer = match self.payer {
            Some(pk) => pk,
            None => return Ok(None),
        };

        let start_mint = Pubkey::from_str(&route.start_mint)
            .with_context(|| format!("invalid start mint {}", route.start_mint))?;
        let end_mint = Pubkey::from_str(&route.end_mint)
            .with_context(|| format!("invalid end mint {}", route.end_mint))?;
        if start_mint != end_mint {
            return Ok(None);
        }

        let mut borrow_amount = first_swap.input_amount;
        if borrow_amount == 0 {
            return Ok(None);
        }

        let flash_planner = builder.flash_loan_planner();
        if !flash_planner.supports_mint(&start_mint) {
            return Ok(None);
        }

        let tip_lamports = tip_lamports.unwrap_or(0);
        let enable_tip_funding = start_mint == WSOL_MINT_PUBKEY && tip_lamports > 0;
        if enable_tip_funding {
            borrow_amount = borrow_amount.saturating_add(tip_lamports);
        }

        let receiver_program = Pubkey::from_str(&first_swap.program_id).with_context(|| {
            format!(
                "invalid flash-loan receiver program id {}",
                first_swap.program_id
            )
        })?;
        let destination = builder.token_account_for_mint(&start_mint).ok_or_else(|| {
            anyhow!(
                "flash-loan mint {} missing configured token ATA",
                route.start_mint
            )
        })?;
        let route_plan = flash_planner.plan_route_by_mint(&start_mint, borrow_amount)?;
        ensure!(
            route_plan.allows_receiver_program(&receiver_program),
            "flash-loan mint {} does not allow receiver program {}",
            route.start_mint,
            first_swap.program_id,
        );
        let borrow_instruction = route_plan.borrow_instruction(destination);

        let tip_funding = if enable_tip_funding {
            Some(build_tip_funding_plan(
                payer,
                start_mint,
                destination,
                tip_lamports,
                bundle_seq,
            )?)
        } else {
            None
        };

        Ok(Some(FlashLoanInstructionPlan {
            route_plan,
            borrow_instruction,
            tip_funding,
            repay_source_liquidity: destination,
            repay_authority: payer,
            borrow_prefix_index: None,
        }))
    }

    fn warn_missing_lookup_tables(&self, instructions: &[PlannedInstruction]) {
        let builder = match self.builder {
            Some(handle) => handle,
            None => return,
        };
        let missing = instructions
            .iter()
            .filter(|ix| ix.lookup_table.is_none())
            .count();
        if missing > 0 {
            let stats = builder.alt_manager().stats();
            warn!(
                missing_lookup_tables = missing,
                total_legs = instructions.len(),
                ready_tables = stats.ready_tables,
                pending_jobs = stats.pending_jobs,
                registered_handles = stats.registered_handles,
                "missing ALT coverage for planned route"
            );
        }
    }
}

fn derive_slippage_tolerance_bps(leg: &RouteLegPlan) -> Option<u16> {
    if let Some(min_output) = leg.min_output_amount {
        let estimated = leg.estimated_output_amount;
        if estimated > 0 {
            let slip = estimated.saturating_sub(min_output);
            let numerator = u128::from(slip).saturating_mul(10_000);
            let denom = u128::from(estimated);
            if denom > 0 {
                let mut bps = numerator / denom;
                if numerator % denom != 0 {
                    bps = bps.saturating_add(1);
                }
                let capped = bps.min(u128::from(u16::MAX));
                return Some(capped as u16);
            }
        }
    }

    let estimate = leg.estimated_slippage_bps;
    if estimate.is_finite() && estimate >= 0.0 {
        let ceil = estimate.ceil().min(f64::from(u16::MAX)).max(0.0) as u16;
        return Some(ceil);
    }

    None
}

fn build_tip_funding_plan(
    payer: Pubkey,
    mint: Pubkey,
    source_ata: Pubkey,
    amount: u64,
    bundle_seq: u64,
) -> Result<TipFundingPlan> {
    ensure!(amount > 0, "tip funding plan requires non-zero amount");
    let seed = format!("{TIP_BUFFER_SEED_PREFIX}{bundle_seq:016x}");
    let tip_account = Pubkey::create_with_seed(&payer, &seed, &SPL_TOKEN_PROGRAM_ID)
        .context("failed to derive tip buffer address")?;
    let rent = Rent::default().minimum_balance(SPL_TOKEN_ACCOUNT_LEN);
    let create_account = create_account_with_seed_instruction(
        &payer,
        &tip_account,
        &seed,
        rent,
        SPL_TOKEN_ACCOUNT_LEN as u64,
        &SPL_TOKEN_PROGRAM_ID,
    );
    let init_account = build_initialize_token_account_instruction(
        &SPL_TOKEN_PROGRAM_ID,
        &tip_account,
        &mint,
        &payer,
    );
    let siphon_instruction = build_token_transfer_instruction(
        &SPL_TOKEN_PROGRAM_ID,
        &source_ata,
        &tip_account,
        &payer,
        amount,
    );
    let close_account =
        build_close_token_account_instruction(&SPL_TOKEN_PROGRAM_ID, &tip_account, &payer, &payer);

    Ok(TipFundingPlan {
        create_account,
        init_account,
        siphon_instruction,
        close_account,
    })
}

fn create_account_with_seed_instruction(
    payer: &Pubkey,
    new_account: &Pubkey,
    seed: &str,
    lamports: u64,
    space: u64,
    owner: &Pubkey,
) -> Instruction {
    let data = serialize(
        &system_instruction::SystemInstruction::CreateAccountWithSeed {
            base: *payer,
            seed: seed.to_string(),
            lamports,
            space,
            owner: *owner,
        },
    )
    .expect("serialize create-account-with-seed instruction");
    Instruction {
        program_id: system_program::ID,
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new(*new_account, false),
            AccountMeta::new_readonly(*payer, true),
        ],
        data,
    }
}

fn build_initialize_token_account_instruction(
    token_program: &Pubkey,
    account: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
) -> Instruction {
    Instruction {
        program_id: *token_program,
        accounts: vec![
            AccountMeta::new(*account, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(*owner, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: vec![1],
    }
}

fn build_token_transfer_instruction(
    token_program: &Pubkey,
    source: &Pubkey,
    destination: &Pubkey,
    authority: &Pubkey,
    amount: u64,
) -> Instruction {
    let mut data = Vec::with_capacity(1 + std::mem::size_of::<u64>());
    data.push(3);
    data.extend_from_slice(&amount.to_le_bytes());
    Instruction {
        program_id: *token_program,
        accounts: vec![
            AccountMeta::new(*source, false),
            AccountMeta::new(*destination, false),
            AccountMeta::new_readonly(*authority, true),
        ],
        data,
    }
}

fn build_close_token_account_instruction(
    token_program: &Pubkey,
    account: &Pubkey,
    destination: &Pubkey,
    authority: &Pubkey,
) -> Instruction {
    Instruction {
        program_id: *token_program,
        accounts: vec![
            AccountMeta::new(*account, false),
            AccountMeta::new(*destination, false),
            AccountMeta::new_readonly(*authority, true),
        ],
        data: vec![9],
    }
}
fn log_alt_manager_status(builder: &BuilderHandle) {
    let stats = builder.alt_manager().stats();
    info!(
        ready_tables = stats.ready_tables,
        registered_handles = stats.registered_handles,
        pending_jobs = stats.pending_jobs,
        refresh_count = stats.refresh_count,
        last_refresh_unix_ms = stats.last_refresh_unix_ms,
        "alt manager status"
    );
}

fn duration_ms(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::templates::SwapTemplateDirection;
    use crate::builder::{InstructionTemplate, TemplateCacheKey, TemplateMetadata, TemplatePatch};
    use crate::config::AppConfig;
    use crate::core::SlabPool;
    use crate::hydration::types::{PoolQuoteDirection, TradeUpdate};
    use crate::planner::types::{BundleCandidate, Opportunity, RouteLegPlan, RoutePlan};
    use crate::template::TemplateContext;
    use anyhow::anyhow;
    use serde_json::json;
    use solana_sdk::signature::{write_keypair_file, Keypair};
    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};

    struct BuilderTestFixture {
        builder: BuilderHandle,
        _temp_dir: TempDir,
    }

    impl BuilderTestFixture {
        fn with_flash_loan_receiver(receiver_program: Pubkey) -> Self {
            let temp_dir = tempdir().expect("tempdir");
            let payer = Keypair::new();
            let payer_path = temp_dir.path().join("payer.json");
            write_keypair_file(&payer, &payer_path).expect("write payer");
            let auth = Keypair::new();
            let auth_path = temp_dir.path().join("auth.json");
            write_keypair_file(&auth, &auth_path).expect("write auth");

            let quote_mint = Pubkey::new_unique();
            let wsol_ata = Pubkey::new_unique();
            let config_path = temp_dir.path().join("config.json");
            let config_json = json!({
                "wallets": {
                    "auth": {"path": auth_path.to_str().unwrap()},
                    "payer": {"path": payer_path.to_str().unwrap()}
                },
                "jito": {
                    "endpoints": ["https://example-rpc-1.sol", "https://example-rpc-2.sol"],
                    "tip_floor_url": "https://example/tipfloor",
                    "tip_stream_url": "wss://example/stream"
                },
                "corecast": {
                    "address": "example",
                    "authorization": "token",
                    "cache": {"warm_capacity": 1, "channel_capacity": 1}
                },
                "tokens": {},
                "atas": {
                    (WSOL_MINT_PUBKEY.to_string()): wsol_ata.to_string()
                },
                "token_pairs": [[WSOL_MINT_PUBKEY.to_string(), quote_mint.to_string()]],
                "program_addresses": [],
                "flash_loans": {
                    "SOL": {
                        "program_id": "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo",
                        "lending_market": "4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY",
                        "reserve": "8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36",
                        "reserve_liquidity_mint": WSOL_MINT_PUBKEY.to_string(),
                        "borrow": {
                            "reserve_liquidity_supply": "8UviNr47S8eL6J3WfDxMRa3hvLta1VDJwNWqsDgtN3Cv"
                        },
                        "repay": {
                            "reserve_liquidity_supply": "8UviNr47S8eL6J3WfDxMRa3hvLta1VDJwNWqsDgtN3Cv",
                            "reserve_liquidity_fee_receiver": "5wo1tFpi4HaVKnemqaXeQnBEpezrJXcXvuztYaPhvgC7",
                            "host_fee_receiver": "FrGB9HC1vEjUhWbUqfQkdKCuuU58bL843AZPQ21LWudu"
                        },
                        "receiver_program_allowlist": [receiver_program.to_string()]
                    }
                },
                "quicknode_rpcs": ["https://example-rpc.sol"],
                "protocols": {},
                "perf": {
                    "ingest_core": 0,
                    "eval_core": 1,
                    "submit_core": 2,
                    "ring_capacity": 8,
                    "opportunity_slab": 8,
                    "bundle_slab": 8
                }
            });
            std::fs::write(&config_path, serde_json::to_vec(&config_json).unwrap()).unwrap();
            let config = AppConfig::load(&config_path).expect("config");
            let builder = BuilderHandle::new(Arc::new(config)).expect("builder");
            Self {
                builder,
                _temp_dir: temp_dir,
            }
        }

        fn builder(&self) -> BuilderHandle {
            self.builder.clone()
        }
    }

    #[test]
    fn planner_builds_instructions_for_route() {
        let route = RoutePlan {
            start_mint: "A".into(),
            end_mint: "C".into(),
            legs: vec![
                RouteLegPlan {
                    pool_id: "pool-ab".into(),
                    program_id: "prog".into(),
                    input_mint: "A".into(),
                    output_mint: "B".into(),
                    input_amount: 100,
                    available_input_liquidity: 100,
                    estimated_output_amount: 200,
                    amount_specified_is_input: true,
                    effective_price: 2.0,
                    fee_bps: 30,
                    template_fee_bps: 30,
                    estimated_slippage_bps: 5.0,
                    direction: PoolQuoteDirection::BaseToQuote,
                    sqrt_price_limit: None,
                    min_output_amount: None,
                    limit_price: None,
                    price_impact_limit_bps: None,
                    context: TemplateContext::default(),
                    template_ready: true,
                    alt_ready: true,
                },
                RouteLegPlan {
                    pool_id: "pool-bc".into(),
                    program_id: "prog".into(),
                    input_mint: "B".into(),
                    output_mint: "C".into(),
                    input_amount: 200,
                    available_input_liquidity: 200,
                    estimated_output_amount: 190,
                    amount_specified_is_input: true,
                    effective_price: 0.95,
                    fee_bps: 30,
                    template_fee_bps: 30,
                    estimated_slippage_bps: 6.0,
                    direction: PoolQuoteDirection::BaseToQuote,
                    sqrt_price_limit: None,
                    min_output_amount: None,
                    limit_price: None,
                    price_impact_limit_bps: None,
                    context: TemplateContext::default(),
                    template_ready: true,
                    alt_ready: true,
                },
            ],
            estimated_final_output_amount: Some(190),
            flash_loan_permitted: false,
        };

        let planner = InstructionPlanner::new(None);
        let planned = planner.plan(&route, 0, None).expect("instructions");
        assert_eq!(planned.swap_instructions.len(), 2);
        assert!(planned
            .swap_instructions
            .iter()
            .all(|ix| ix.amount_specified_is_input));
        assert_eq!(planned.swap_instructions[1].output_mint, "C");
        assert!(planned
            .swap_instructions
            .iter()
            .all(|ix| ix.lookup_table.is_none()));
        assert!(planned.prefix_instructions.is_empty());
        assert!(planned.suffix_instructions.is_empty());
        assert!(planned.flash_loan_plan.is_none());
        assert_eq!(
            planned.swap_instructions[0].context.slippage_tolerance_bps,
            Some(5)
        );
        assert_eq!(
            planned.swap_instructions[1].context.slippage_tolerance_bps,
            Some(6)
        );
    }

    #[test]
    fn planner_handles_exact_out_leg() {
        let leg = RouteLegPlan {
            pool_id: "pool-exact-out".into(),
            program_id: "prog".into(),
            input_mint: "A".into(),
            output_mint: "B".into(),
            input_amount: 200,
            available_input_liquidity: 1_000,
            estimated_output_amount: 150,
            amount_specified_is_input: false,
            effective_price: 0.75,
            fee_bps: 25,
            template_fee_bps: 20,
            estimated_slippage_bps: 3.0,
            direction: PoolQuoteDirection::BaseToQuote,
            sqrt_price_limit: None,
            min_output_amount: Some(140),
            limit_price: None,
            price_impact_limit_bps: None,
            context: TemplateContext::default(),
            template_ready: true,
            alt_ready: true,
        };

        let route = RoutePlan {
            start_mint: "A".into(),
            end_mint: "B".into(),
            legs: vec![leg.clone()],
            estimated_final_output_amount: Some(leg.estimated_output_amount),
            flash_loan_permitted: false,
        };

        let planner = InstructionPlanner::new(None);
        let planned = planner.plan(&route, 0, None).expect("instructions");
        assert_eq!(planned.swap_instructions.len(), 1);
        let ix = &planned.swap_instructions[0];
        assert!(!ix.amount_specified_is_input);
        assert_eq!(ix.amount_specified, leg.estimated_output_amount);
        assert_eq!(ix.input_amount, leg.input_amount);

        let bundle_plan = super::bundle_instruction_plan_from(ix);
        assert!(!bundle_plan.amount_specified_is_input);
        assert_eq!(bundle_plan.amount_in, leg.estimated_output_amount);
    }

    #[test]
    fn planner_derives_slippage_from_min_output() {
        let route = RoutePlan {
            start_mint: "A".into(),
            end_mint: "A".into(),
            legs: vec![RouteLegPlan {
                pool_id: "pool-ab".into(),
                program_id: "prog".into(),
                input_mint: "A".into(),
                output_mint: "B".into(),
                input_amount: 100,
                available_input_liquidity: 100,
                estimated_output_amount: 1_000_000,
                amount_specified_is_input: true,
                effective_price: 1.0,
                fee_bps: 30,
                template_fee_bps: 30,
                estimated_slippage_bps: 1.0,
                direction: PoolQuoteDirection::BaseToQuote,
                sqrt_price_limit: None,
                min_output_amount: Some(950_000),
                limit_price: None,
                price_impact_limit_bps: None,
                context: TemplateContext::default(),
                template_ready: true,
                alt_ready: true,
            }],
            estimated_final_output_amount: Some(950_000),
            flash_loan_permitted: false,
        };

        let planner = InstructionPlanner::new(None);
        let planned = planner.plan(&route, 0, None).expect("instructions");
        assert_eq!(planned.swap_instructions.len(), 1);
        assert_eq!(planned.swap_instructions[0].input_amount, 100);
        assert_eq!(
            planned.swap_instructions[0].context.slippage_tolerance_bps,
            Some(500)
        );
    }

    #[test]
    fn planner_includes_tip_funding_in_flash_loan_route() {
        let pool_id = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();
        let fixture = BuilderTestFixture::with_flash_loan_receiver(program_id);
        let builder = fixture.builder();

        let template = InstructionTemplate {
            key: TemplateCacheKey::new(pool_id, SwapTemplateDirection::BaseIn),
            program_id,
            discriminator: [0; 8],
            accounts: Arc::from(Vec::<AccountMeta>::new()),
            data: Arc::new(Vec::new()),
            patches: Arc::from(Vec::<TemplatePatch>::new()),
            cu_budget: 0,
            tlv_bytes: 0,
            template_hash: [0; 32],
            alt_handle: None,
            metadata: TemplateMetadata::None,
            hydrated_at: Instant::now(),
        };
        builder.template_cache().insert(template);

        let planner = InstructionPlanner::new(Some(&builder));
        let leg = RouteLegPlan {
            pool_id: pool_id.to_string(),
            program_id: program_id.to_string(),
            input_mint: WSOL_MINT_PUBKEY.to_string(),
            output_mint: WSOL_MINT_PUBKEY.to_string(),
            input_amount: 1_000_000,
            available_input_liquidity: 1_000_000,
            estimated_output_amount: 999_000,
            amount_specified_is_input: true,
            effective_price: 1.0,
            fee_bps: 30,
            template_fee_bps: 30,
            estimated_slippage_bps: 0.0,
            direction: PoolQuoteDirection::BaseToQuote,
            sqrt_price_limit: None,
            min_output_amount: None,
            limit_price: None,
            price_impact_limit_bps: None,
            context: TemplateContext::default(),
            template_ready: true,
            alt_ready: true,
        };
        let route = RoutePlan {
            start_mint: WSOL_MINT_PUBKEY.to_string(),
            end_mint: WSOL_MINT_PUBKEY.to_string(),
            legs: vec![leg],
            estimated_final_output_amount: Some(999_000),
            flash_loan_permitted: true,
        };

        let tip_lamports = 250_000;
        let planned = planner
            .plan(&route, 42, Some(tip_lamports))
            .expect("planned route");

        assert_eq!(planned.prefix_instructions.len(), 5);
        assert!(planned.suffix_instructions.is_empty());
        let flash_plan = planned.flash_loan_plan.expect("flash plan");
        assert_eq!(
            flash_plan.route_plan.borrow_amount,
            1_000_000 + tip_lamports
        );
        assert_eq!(
            planned.prefix_instructions[0].program_id,
            system_program::ID
        );
        assert_eq!(
            planned.prefix_instructions[3].program_id,
            SPL_TOKEN_PROGRAM_ID
        );
        assert_eq!(
            planned.prefix_instructions[4].program_id,
            SPL_TOKEN_PROGRAM_ID
        );
    }

    #[test]
    fn flash_loan_summary_from_copies_route_details() {
        let program_id = Pubkey::new_unique();
        let fixture = BuilderTestFixture::with_flash_loan_receiver(program_id);
        let builder = fixture.builder();
        let planner = builder.flash_loan_planner();
        let route_plan = planner
            .plan_route_by_mint(&WSOL_MINT_PUBKEY, 2_000_000)
            .expect("route plan");

        let summary = flash_loan_summary_from(&route_plan);
        assert_eq!(summary.plan_id, route_plan.id);
        assert_eq!(summary.token, route_plan.token);
        assert_eq!(summary.borrow_amount, route_plan.borrow_amount);
        assert_eq!(summary.repay_amount, route_plan.repay_amount);
        assert_eq!(summary.fee_amount, route_plan.fee_amount);
        assert_eq!(
            summary.program_id,
            route_plan.accounts.program_id.to_string()
        );
        assert_eq!(
            summary.lending_market,
            route_plan.accounts.lending_market.to_string()
        );
        assert_eq!(summary.reserve, route_plan.accounts.reserve.to_string());
        assert_eq!(
            summary.liquidity_mint,
            route_plan.accounts.reserve_liquidity_mint.to_string()
        );
    }

    #[test]
    fn stage2_failure_releases_flash_loan_plan() {
        let fixture = BuilderTestFixture::with_flash_loan_receiver(Pubkey::new_unique());
        let builder = fixture.builder();
        let planner = builder.flash_loan_planner();
        let borrow_amount = 500_000;
        let route_plan = planner
            .plan_route_by_mint(&WSOL_MINT_PUBKEY, borrow_amount)
            .expect("route plan");
        let outstanding_after_plan = planner.outstanding_borrow_lamports();

        let pool = SlabPool::new(1);
        let mut bundle = pool
            .try_acquire(sample_candidate(42))
            .expect("lease available");
        bundle.flash_loan_reservation = Some(route_plan);

        handle_stage2_failure(bundle, anyhow!("boom"), FailureKind::Plan, &builder);

        assert_eq!(
            planner.outstanding_borrow_lamports(),
            outstanding_after_plan.saturating_sub(borrow_amount)
        );
    }

    fn sample_candidate(seq: u64) -> BundleCandidate {
        let trade = TradeUpdate {
            seq,
            slot: 1,
            program_id: "prog".into(),
            pool_id: "pool".into(),
            signature: format!("sig-{seq}"),
            buy_mint: "buy".into(),
            sell_mint: "sell".into(),
            buy_amount: 100,
            sell_amount: 50,
        };
        let opportunity = Opportunity::from_trade(seq, trade, None, None, Some(100.0));
        BundleCandidate::from_opportunity(&opportunity)
    }
}
