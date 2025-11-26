use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tracing::{info, warn};

use crate::builder::{BuilderHandle, BuilderStage};
use crate::core::{pin_current_thread, Lease, RingConsumer};

use super::types::{BundleCandidate, PlannedInstruction, RoutePlan};

pub fn spawn_submit_stage(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
) -> Result<thread::JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("stage2_submit".into())
        .spawn(move || run(stop, core_id, consumer, builder))
        .map_err(Into::into)
}

fn run(
    stop: Arc<AtomicBool>,
    core_id: Option<usize>,
    consumer: RingConsumer<Lease<BundleCandidate>>,
    builder: BuilderHandle,
) -> Result<()> {
    if let Some(core) = core_id {
        let _ = pin_current_thread(core);
    }

    let mev_revision = builder.mev_guard().current_revision();
    info!(mev_revision, "stage2 submit online");
    while !stop.load(Ordering::Relaxed) {
        builder.heartbeat(BuilderStage::SubmitHotPath);
        if let Some(mut bundle) = consumer.pop() {
            let planned = bundle
                .route_plan
                .as_ref()
                .map(|plan| InstructionPlanner::plan(plan));
            if let Some(result) = planned {
                match result {
                    Ok(instructions) => {
                        builder.mev_guard().record_protected_bundle();
                        bundle.instructions = instructions.clone();
                        info!(
                            seq = bundle.seq,
                            edge_bps = bundle.estimated_edge_bps,
                            hop_count = instructions.len(),
                            "submitting multi-leg bundle",
                        );
                        for (idx, ix) in instructions.iter().enumerate() {
                            info!(
                                seq = bundle.seq,
                                step = idx,
                                pool = %ix.pool_id,
                                input = %ix.input_mint,
                                output = %ix.output_mint,
                                amount = ix.amount,
                                "constructed instruction"
                            );
                        }
                    }
                    Err(err) => {
                        warn!(seq = bundle.seq, error = %err, "failed to plan instructions");
                    }
                }
            } else {
                info!(
                    seq = bundle.seq,
                    pool = %bundle.pool_id,
                    edge_bps = bundle.estimated_edge_bps,
                    "submitting single-leg bundle"
                );
            }
            drop(bundle);
        } else {
            thread::yield_now();
            thread::sleep(Duration::from_millis(10));
        }
    }
    info!("stage2 submit shutting down");
    Ok(())
}

struct InstructionPlanner;

impl InstructionPlanner {
    fn plan(route: &RoutePlan) -> anyhow::Result<Vec<PlannedInstruction>> {
        let mut instructions = Vec::with_capacity(route.legs.len());
        for leg in &route.legs {
            instructions.push(PlannedInstruction {
                pool_id: leg.pool_id.clone(),
                program_id: leg.program_id.clone(),
                input_mint: leg.input_mint.clone(),
                output_mint: leg.output_mint.clone(),
                amount: leg.input_amount,
            });
        }
        Ok(instructions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::types::{RouteLegPlan, RoutePlan};

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
                },
            ],
            estimated_final_output_amount: Some(190),
        };

        let instructions = InstructionPlanner::plan(&route).expect("instructions");
        assert_eq!(instructions.len(), 2);
        assert_eq!(instructions[0].amount, 100);
        assert_eq!(instructions[1].amount, 200);
        assert_eq!(instructions[1].output_mint, "C");
    }
}
