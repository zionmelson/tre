use std::str::FromStr;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;

use crate::builder::{templates::SwapTemplateDirection, BuilderHandle};
use crate::hydration::types::{PoolQuoteDirection, PoolQuoteSide, PoolSnapshot};
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
    ) -> Self {
        Self {
            builder,
            pool_store,
            freshness_budget,
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

            let (direction, price, available_input_liquidity, quote_side) =
                price_for_leg(&snapshot, leg)?;
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

            let mut estimated_output = ((current_amount as f64) * price).round() as u64;
            if estimated_output == 0 {
                estimated_output = 1;
            }
            let mut min_output = estimated_output
                .saturating_sub((estimated_output * DEFAULT_LEG_SLIPPAGE_BPS) / 10_000);

            let amount_specified_is_input = quote_side
                .map(|side| side.amount_specified_is_input)
                .unwrap_or(true);
            if let Some(side) = quote_side {
                if side.notional > 0 && side.expected_output > 0 && current_amount > 0 {
                    let scaled = u128::from(side.expected_output)
                        .saturating_mul(u128::from(current_amount))
                        / u128::from(side.notional);
                    if scaled > 0 {
                        estimated_output = scaled.min(u128::from(u64::MAX)) as u64;
                    }
                }
                if estimated_output == 0 {
                    estimated_output = 1;
                }
                if let (Some(side_min), expected) = (side.min_output_amount, side.expected_output) {
                    if expected > 0 {
                        let scaled = u128::from(side_min)
                            .saturating_mul(u128::from(estimated_output))
                            / u128::from(expected);
                        min_output = scaled.min(u128::from(u64::MAX)) as u64;
                    }
                }
            }

            let fee_bps = quote_side
                .map(|side| side.fee_bps)
                .unwrap_or(DEFAULT_LEG_FEE_BPS);
            let template_fee_bps = quote_side
                .map(|side| side.template_fee_bps)
                .unwrap_or(DEFAULT_LEG_FEE_BPS as u16);
            let estimated_slippage_bps = quote_side
                .map(|side| side.estimated_slippage_bps)
                .unwrap_or(DEFAULT_LEG_SLIPPAGE_BPS as f64);
            let sqrt_price_limit = quote_side.and_then(|side| side.sqrt_price_limit);
            let limit_price = quote_side.and_then(|side| side.limit_price);
            let price_impact_limit_bps = quote_side.and_then(|side| side.price_impact_limit_bps);
            let mut context = quote_side
                .map(|side| side.context.clone())
                .unwrap_or_else(TemplateContext::default);
            context.template_ready = readiness.template_ready;
            if let Some(side) = quote_side {
                if let (Some(fee), notional) = (context.expected_fee_amount, side.notional) {
                    let scaled = scale_partial_fill_remaining(fee, current_amount, notional);
                    context.expected_fee_amount = (scaled > 0).then_some(scaled);
                }
                if let Some(leftover) = context.partial_fill_remaining {
                    if side.notional == 0 {
                        context.partial_fill_remaining = None;
                    } else {
                        let scaled =
                            scale_partial_fill_remaining(leftover, current_amount, side.notional);
                        context.partial_fill_remaining = (scaled > 0).then_some(scaled);
                    }
                }
            }

            legs.push(RouteLegPlan {
                pool_id: leg.pool_id.clone(),
                program_id: leg.program_id.clone(),
                input_mint: leg.input_mint.clone(),
                output_mint: leg.output_mint.clone(),
                input_amount: current_amount,
                available_input_liquidity,
                estimated_output_amount: estimated_output,
                amount_specified_is_input,
                effective_price: price,
                fee_bps,
                template_fee_bps,
                estimated_slippage_bps,
                direction,
                sqrt_price_limit,
                min_output_amount: Some(min_output),
                limit_price,
                price_impact_limit_bps,
                context,
                template_ready: readiness.template_ready,
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

fn price_for_leg<'a>(
    snapshot: &'a PoolSnapshot,
    leg: &PathLeg,
) -> Option<(PoolQuoteDirection, f64, u64, Option<&'a PoolQuoteSide>)> {
    let direction =
        if leg.input_mint == snapshot.base_mint && leg.output_mint == snapshot.quote_mint {
            PoolQuoteDirection::BaseToQuote
        } else if leg.input_mint == snapshot.quote_mint && leg.output_mint == snapshot.base_mint {
            PoolQuoteDirection::QuoteToBase
        } else {
            return None;
        };

    if let Some(side) = snapshot.quote(direction) {
        if let Some(price) = side.price() {
            if price > 0.0 {
                return Some((direction, price, side.notional, Some(side)));
            }
        }
    }

    if let Some(price) = snapshot.price_for_mints(&leg.input_mint, &leg.output_mint) {
        if price > 0.0 {
            return Some((direction, price, 0, None));
        }
    }

    None
}

fn scale_partial_fill_remaining(leftover: u64, target_amount: u64, reference_notional: u64) -> u64 {
    if leftover == 0 || target_amount == 0 || reference_notional == 0 {
        return 0;
    }
    let scaled = u128::from(leftover).saturating_mul(u128::from(target_amount))
        / u128::from(reference_notional);
    scaled
        .min(u128::from(target_amount))
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::scale_partial_fill_remaining;

    #[test]
    fn scales_partial_remainder_down() {
        let scaled = scale_partial_fill_remaining(200, 500, 1_000);
        assert_eq!(scaled, 100);
    }

    #[test]
    fn scales_partial_remainder_up() {
        let scaled = scale_partial_fill_remaining(150, 2_000, 500);
        assert_eq!(scaled, 600);
    }

    #[test]
    fn zero_reference_zeroes_remainder() {
        assert_eq!(scale_partial_fill_remaining(150, 1_000, 0), 0);
    }
}
