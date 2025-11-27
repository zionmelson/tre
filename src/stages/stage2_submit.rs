use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use bincode::serialize;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_system_interface::{instruction as system_instruction, program as system_program};
use tracing::{debug, info, warn};

use crate::builder::templates::SwapTemplateDirection;
use crate::builder::{
    BuilderHandle, BuilderStage, BundleInstructionPlan, BundleLookupTablePlan, FlashLoanRoutePlan,
};
use crate::core::{pin_current_thread, Lease, RingConsumer, RingProducer};
use crate::telemetry::{self, Stage2PlanEvent};

use super::types::{
    BundleCandidate, PlannedInstruction, PlannedLookupTable, PoolQuoteDirection, RouteLegPlan,
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
    let mut retry_queue: VecDeque<Lease<BundleCandidate>> = VecDeque::new();
    info!(mev_revision, "stage2 submit online");
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::SubmitHotPath);
        if last_alt_stats.elapsed() >= Duration::from_secs(30) {
            log_alt_manager_status(&builder);
            last_alt_stats = Instant::now();
        }
        if let Some((bundle, source)) = pop_work_item(&consumer, &mut retry_queue) {
            process_bundle(
                bundle,
                source,
                &planner,
                &builder,
                &producer,
                &mut retry_queue,
            );
        } else {
            thread::yield_now();
            thread::sleep(Duration::from_millis(10));
        }
    }
    info!("stage2 submit shutting down");
    Ok(())
}

const MAX_STAGE2_ATTEMPTS: u8 = 3;
const STAGE2_RETRY_QUEUE_CAPACITY: usize = 128;

#[derive(Copy, Clone, Eq, PartialEq)]
enum BundleSource {
    Fresh,
    Retry,
}

fn pop_work_item(
    consumer: &RingConsumer<Lease<BundleCandidate>>,
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
) -> Option<(Lease<BundleCandidate>, BundleSource)> {
    if let Some(bundle) = retry_queue.pop_front() {
        Some((bundle, BundleSource::Retry))
    } else {
        consumer.pop().map(|bundle| (bundle, BundleSource::Fresh))
    }
}

fn process_bundle(
    mut bundle: Lease<BundleCandidate>,
    source: BundleSource,
    planner: &InstructionPlanner,
    builder: &BuilderHandle,
    producer: &RingProducer<Lease<BundleCandidate>>,
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
) {
    match prepare_bundle(&mut bundle, planner, builder) {
        Ok(()) => {
            if matches!(source, BundleSource::Retry) {
                telemetry::record_stage2_plan_event(Stage2PlanEvent::RetryFlushed);
            }
            if bundle.bundle_context.is_some() {
                if producer.push(bundle).is_err() {
                    warn!("stage3 queue rejected bundle candidate");
                }
            } else {
                warn!(
                    seq = bundle.seq,
                    "bundle missing context after planning; dropping"
                );
                drop(bundle);
            }
        }
        Err(Stage2Failure::Plan(err)) => {
            handle_stage2_failure(bundle, err, FailureKind::Plan, retry_queue);
        }
        Err(Stage2Failure::Prepare(err)) => {
            handle_stage2_failure(bundle, err, FailureKind::Prepare, retry_queue);
        }
    }
}

fn handle_stage2_failure(
    mut bundle: Lease<BundleCandidate>,
    err: anyhow::Error,
    kind: FailureKind,
    retry_queue: &mut VecDeque<Lease<BundleCandidate>>,
) {
    let attempt = bundle.stage2_attempts.saturating_add(1);
    bundle.stage2_attempts = attempt;
    warn!(seq = bundle.seq, attempt, error = %err, ?kind, "stage2 planning failure");
    let (failure_event, retry_event, exhausted_event) = match kind {
        FailureKind::Plan => (
            Stage2PlanEvent::PlanFailed,
            Stage2PlanEvent::RetryQueuedPlan,
            Stage2PlanEvent::RetriesExhaustedPlan,
        ),
        FailureKind::Prepare => (
            Stage2PlanEvent::PrepareFailed,
            Stage2PlanEvent::RetryQueuedPrepare,
            Stage2PlanEvent::RetriesExhaustedPrepare,
        ),
    };
    telemetry::record_stage2_plan_event(failure_event);
    if attempt < MAX_STAGE2_ATTEMPTS && retry_queue.len() < STAGE2_RETRY_QUEUE_CAPACITY {
        telemetry::record_stage2_plan_event(retry_event);
        retry_queue.push_back(bundle);
    } else {
        telemetry::record_stage2_plan_event(exhausted_event);
        drop(bundle);
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
        amount_in: ix.amount,
        expected_output_amount: ix.estimated_output_amount,
        fee_bps: u16::try_from(ix.fee_bps).unwrap_or(u16::MAX),
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

enum Stage2Failure {
    Plan(anyhow::Error),
    Prepare(anyhow::Error),
}

#[derive(Default)]
struct PlannedRouteInstructions {
    swap_instructions: Vec<PlannedInstruction>,
    prefix_instructions: Vec<Instruction>,
    suffix_instructions: Vec<Instruction>,
    flash_loan_plan: Option<FlashLoanRoutePlan>,
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
        let planned = planner.plan(route_plan).map_err(Stage2Failure::Plan)?;
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
        suffix_instructions,
        flash_loan_plan,
    } = planned;

    if let Some(tip_ix) = build_tip_instruction(bundle, builder.payer_pubkey()) {
        prefix_instructions.push(tip_ix);
    }

    builder.mev_guard().record_protected_bundle();
    bundle.instructions = swap_instructions.clone();
    let bundle_plan = swap_instructions
        .iter()
        .map(bundle_instruction_plan_from)
        .collect::<Vec<_>>();

    match builder.prepare_bundle(
        bundle.seq,
        &prefix_instructions,
        &bundle_plan,
        &suffix_instructions,
    ) {
        Ok(context) => {
            bundle.bundle_context = Some(context);
        }
        Err(err) => {
            if let Some(plan) = flash_loan_plan {
                builder.flash_loan_planner().release_route(&plan);
            }
            return Err(Stage2Failure::Prepare(err));
        }
    }

    if let Some(plan) = flash_loan_plan {
        builder.flash_loan_planner().release_route(&plan);
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
            amount = ix.amount,
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
    repay_instruction: Instruction,
}

const ASSOCIATED_TOKEN_PROGRAM_ID: Pubkey = pubkey!("ATokenGPvR93dthLvYbAphit9L8ZsfsZp7r4HQJDebg");
const SPL_TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

fn derive_associated_token_address(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), SPL_TOKEN_PROGRAM_ID.as_ref(), mint.as_ref()],
        &ASSOCIATED_TOKEN_PROGRAM_ID,
    )
    .0
}

struct InstructionPlanner<'a> {
    builder: Option<&'a BuilderHandle>,
    payer: Option<Pubkey>,
}

impl<'a> InstructionPlanner<'a> {
    fn new(builder: Option<&'a BuilderHandle>) -> Self {
        let payer = builder.map(|b| b.payer_pubkey());
        Self { builder, payer }
    }

    fn plan(&self, route: &RoutePlan) -> anyhow::Result<PlannedRouteInstructions> {
        let mut instructions = Vec::with_capacity(route.legs.len());
        for leg in &route.legs {
            instructions.push(PlannedInstruction {
                pool_id: leg.pool_id.clone(),
                program_id: leg.program_id.clone(),
                input_mint: leg.input_mint.clone(),
                output_mint: leg.output_mint.clone(),
                amount: leg.input_amount,
                estimated_output_amount: leg.estimated_output_amount,
                fee_bps: leg.fee_bps,
                direction: leg.direction,
                lookup_table: self.lookup_table_for_leg(leg),
                sqrt_price_limit: leg.sqrt_price_limit,
                min_output_amount: leg.min_output_amount,
                limit_price: leg.limit_price,
                price_impact_limit_bps: leg.price_impact_limit_bps,
                context: leg.context.clone(),
            });
        }

        self.warn_missing_lookup_tables(&instructions);

        let mut planned = PlannedRouteInstructions::with_swaps(instructions);
        if let Some(first_swap) = planned.swap_instructions.first() {
            if let Some(flash_plan) = self.plan_flash_loan(route, first_swap)? {
                planned
                    .prefix_instructions
                    .push(flash_plan.borrow_instruction);
                planned
                    .suffix_instructions
                    .push(flash_plan.repay_instruction);
                planned.flash_loan_plan = Some(flash_plan.route_plan);
            }
        }

        Ok(planned)
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
    ) -> anyhow::Result<Option<FlashLoanInstructionPlan>> {
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

        let borrow_amount = first_swap.amount;
        if borrow_amount == 0 {
            return Ok(None);
        }

        let flash_planner = builder.flash_loan_planner();
        if !flash_planner.supports_mint(&start_mint) {
            return Ok(None);
        }

        let receiver_program = Pubkey::from_str(&first_swap.program_id).with_context(|| {
            format!(
                "invalid flash-loan receiver program id {}",
                first_swap.program_id
            )
        })?;
        let destination = derive_associated_token_address(&payer, &start_mint);
        let route_plan = flash_planner.plan_route_by_mint(&start_mint, borrow_amount)?;
        ensure!(
            route_plan.allows_receiver_program(&receiver_program),
            "flash-loan mint {} does not allow receiver program {}",
            route.start_mint,
            first_swap.program_id,
        );
        let first_plan = bundle_instruction_plan_from(first_swap);
        let receiver_accounts = builder
            .instruction_accounts_for_plan(&first_plan)
            .context("failed to resolve receiver accounts for flash-loan borrow")?;
        let borrow_instruction =
            route_plan.borrow_instruction(destination, receiver_program, &receiver_accounts);
        let repay_instruction = route_plan
            .repay_instruction(destination, payer)
            .context("failed to build flash-loan repay instruction")?;

        Ok(Some(FlashLoanInstructionPlan {
            route_plan,
            borrow_instruction,
            repay_instruction,
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
    use crate::core::SlabPool;
    use crate::stages::types::{BundleCandidate, Opportunity, TradeUpdate};
    use crate::stages::types::{PoolQuoteDirection, RouteLegPlan, RoutePlan};
    use crate::template::TemplateContext;
    use anyhow::anyhow;

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
                    estimated_output_amount: 200,
                    effective_price: 2.0,
                    fee_bps: 30,
                    estimated_slippage_bps: 5.0,
                    direction: PoolQuoteDirection::BaseToQuote,
                    sqrt_price_limit: None,
                    min_output_amount: None,
                    limit_price: None,
                    price_impact_limit_bps: None,
                    context: TemplateContext::default(),
                    template_ready: false,
                    alt_ready: false,
                },
                RouteLegPlan {
                    pool_id: "pool-bc".into(),
                    program_id: "prog".into(),
                    input_mint: "B".into(),
                    output_mint: "C".into(),
                    input_amount: 200,
                    estimated_output_amount: 190,
                    effective_price: 0.95,
                    fee_bps: 30,
                    estimated_slippage_bps: 6.0,
                    direction: PoolQuoteDirection::BaseToQuote,
                    sqrt_price_limit: None,
                    min_output_amount: None,
                    limit_price: None,
                    price_impact_limit_bps: None,
                    context: TemplateContext::default(),
                    template_ready: false,
                    alt_ready: false,
                },
            ],
            estimated_final_output_amount: Some(190),
        };

        let planner = InstructionPlanner::new(None);
        let planned = planner.plan(&route).expect("instructions");
        assert_eq!(planned.swap_instructions.len(), 2);
        assert_eq!(planned.swap_instructions[0].amount, 100);
        assert_eq!(planned.swap_instructions[1].amount, 200);
        assert_eq!(planned.swap_instructions[1].output_mint, "C");
        assert!(planned
            .swap_instructions
            .iter()
            .all(|ix| ix.lookup_table.is_none()));
        assert!(planned.prefix_instructions.is_empty());
        assert!(planned.suffix_instructions.is_empty());
        assert!(planned.flash_loan_plan.is_none());
    }

    #[test]
    fn requeues_until_attempt_limit() {
        let pool = SlabPool::new(4);
        let mut retry_queue = VecDeque::new();
        let bundle = pool
            .try_acquire(sample_candidate(1))
            .expect("lease available");

        handle_stage2_failure(bundle, anyhow!("boom"), FailureKind::Plan, &mut retry_queue);
        assert_eq!(retry_queue.len(), 1, "first failure enqueues bundle");

        let bundle = retry_queue.pop_front().expect("bundle queued");
        assert_eq!(bundle.stage2_attempts, 1);
        handle_stage2_failure(bundle, anyhow!("boom"), FailureKind::Plan, &mut retry_queue);
        assert_eq!(retry_queue.len(), 1, "second failure still enqueues");

        let bundle = retry_queue.pop_front().expect("bundle queued twice");
        assert_eq!(bundle.stage2_attempts, 2);
        handle_stage2_failure(bundle, anyhow!("boom"), FailureKind::Plan, &mut retry_queue);
        assert!(retry_queue.is_empty(), "third failure exhausts retries");
    }

    #[test]
    fn drops_when_retry_queue_full() {
        let pool = SlabPool::new(STAGE2_RETRY_QUEUE_CAPACITY + 1);
        let mut retry_queue = VecDeque::with_capacity(STAGE2_RETRY_QUEUE_CAPACITY);
        for seq in 0..STAGE2_RETRY_QUEUE_CAPACITY {
            let lease = pool
                .try_acquire(sample_candidate(seq as u64))
                .expect("lease available");
            retry_queue.push_back(lease);
        }
        assert_eq!(retry_queue.len(), STAGE2_RETRY_QUEUE_CAPACITY);
        let available_before = pool.available();

        let bundle = pool
            .try_acquire(sample_candidate(9_999))
            .expect("remaining slot");
        handle_stage2_failure(bundle, anyhow!("boom"), FailureKind::Plan, &mut retry_queue);

        assert_eq!(retry_queue.len(), STAGE2_RETRY_QUEUE_CAPACITY);
        assert_eq!(pool.available(), available_before, "lease released on drop");
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
        let opportunity = Opportunity::from_trade(seq, trade, None, Some(100.0));
        BundleCandidate::from_opportunity(&opportunity)
    }
}
