use std::str::FromStr;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;

use crate::builder::{templates::SwapTemplateDirection, BuilderHandle};
use crate::hydration::types::{PoolQuoteDirection, PoolSnapshot};
use crate::hydration::PoolStoreView;
use crate::planner::graph_manager::{PathLeg, PoolPath};
use crate::planner::types::{RouteLegPlan, RoutePlan};
use crate::template::TemplateContext;

const DEFAULT_LEG_FEE_BPS: u64 = 30;
const DEFAULT_LEG_SLIPPAGE_BPS: u64 = 50;

#[derive(Clone)]
pub struct RouteBuilder {
    builder: BuilderHandle,
    pool_store: PoolStoreView,
    freshness_budget: Duration,
    require_alt_ready: bool,
}

pub struct RouteBuildResult {
    pub route_plan: RoutePlan,
    pub max_slot: u64,
    pub min_generation: Option<u64>,
}

impl RouteBuilder {
    pub fn new(
        builder: BuilderHandle,
        pool_store: PoolStoreView,
        freshness_budget: Duration,
        require_alt_ready: bool,
    ) -> Self {
        Self {
            builder,
            pool_store,
            freshness_budget,
            require_alt_ready,
        }
    }

    pub fn build_from_path(&self, path: &PoolPath, notional: u64) -> Option<RouteBuildResult> {
        let notional = notional.max(1);
        let mut current_amount = notional;
        let mut max_slot = 0u64;
        let mut min_generation = u64::MAX;
        let mut legs = Vec::with_capacity(path.legs.len());

        for leg in &path.legs {
            let state = self.pool_store.get(&leg.pool_id)?;
            let snapshot = state.snapshot.clone();
            if snapshot.updated_at.elapsed() > self.freshness_budget {
                return None;
            }
            max_slot = max_slot.max(snapshot.slot);
            min_generation = min_generation.min(state.generation);

            let (direction, price, available_input_liquidity) = price_for_leg(&snapshot, leg)?;
            let pool_key = Pubkey::from_str(&leg.pool_id).ok()?;
            let template_direction = match direction {
                PoolQuoteDirection::BaseToQuote => SwapTemplateDirection::BaseIn,
                PoolQuoteDirection::QuoteToBase => SwapTemplateDirection::BaseOut,
            };
            let readiness = self
                .builder
                .template_readiness(&pool_key, template_direction);
            if !readiness.template_ready {
                return None;
            }
            if self.require_alt_ready && !readiness.alt_ready {
                return None;
            }

            let mut estimated_output = ((current_amount as f64) * price).round() as u64;
            if estimated_output == 0 {
                estimated_output = 1;
            }
            let min_output = estimated_output
                .saturating_sub((estimated_output * DEFAULT_LEG_SLIPPAGE_BPS) / 10_000);

            legs.push(RouteLegPlan {
                pool_id: leg.pool_id.clone(),
                program_id: leg.program_id.clone(),
                input_mint: leg.input_mint.clone(),
                output_mint: leg.output_mint.clone(),
                input_amount: current_amount,
                available_input_liquidity,
                estimated_output_amount: estimated_output,
                effective_price: price,
                fee_bps: DEFAULT_LEG_FEE_BPS,
                template_fee_bps: DEFAULT_LEG_FEE_BPS as u16,
                estimated_slippage_bps: DEFAULT_LEG_SLIPPAGE_BPS as f64,
                direction,
                sqrt_price_limit: None,
                min_output_amount: Some(min_output),
                limit_price: None,
                price_impact_limit_bps: None,
                context: TemplateContext::default(),
                template_ready: readiness.template_ready,
                alt_ready: readiness.alt_ready,
            });
            current_amount = estimated_output;
        }

        if legs.is_empty() || current_amount <= notional {
            return None;
        }

        let route_plan = RoutePlan {
            start_mint: path.start_mint.clone(),
            end_mint: path.end_mint.clone(),
            legs,
            estimated_final_output_amount: Some(current_amount),
            flash_loan_permitted: false,
        };

        Some(RouteBuildResult {
            route_plan,
            max_slot,
            min_generation: if min_generation == u64::MAX {
                None
            } else {
                Some(min_generation)
            },
        })
    }
}

fn price_for_leg(snapshot: &PoolSnapshot, leg: &PathLeg) -> Option<(PoolQuoteDirection, f64, u64)> {
    let direction =
        if leg.input_mint == snapshot.base_mint && leg.output_mint == snapshot.quote_mint {
            PoolQuoteDirection::BaseToQuote
        } else if leg.input_mint == snapshot.quote_mint && leg.output_mint == snapshot.base_mint {
            PoolQuoteDirection::QuoteToBase
        } else {
            return None;
        };

    let (price, available_input_liquidity) = if let Some(side) = snapshot.quote(direction) {
        if let Some(price) = side.price() {
            (price, side.notional)
        } else {
            (0.0, 0)
        }
    } else if let Some(price) = snapshot.price_for_mints(&leg.input_mint, &leg.output_mint) {
        (price, 0)
    } else {
        (0.0, 0)
    };

    if price <= 0.0 {
        return None;
    }

    Some((direction, price, available_input_liquidity))
}
