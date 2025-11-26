use std::sync::Arc;

use anyhow::Result;

use crate::stages::ArbitrageOpportunity;
use crate::strategy::StrategyEngine;

pub struct Notifier {
    strategy: Arc<StrategyEngine>,
}

impl Notifier {
    pub fn new(strategy: Arc<StrategyEngine>) -> Self {
        Self { strategy }
    }

    pub fn notify_success(&self, message: &str) -> Result<()> {
        tracing::info!(%message, "notifier placeholder");
        Ok(())
    }

    pub fn notify_arbitrage(&self, opportunities: &[ArbitrageOpportunity]) -> Result<()> {
        if opportunities.is_empty() {
            return Ok(());
        }

        for opportunity in opportunities {
            let route = describe_path(&opportunity.path);
            tracing::info!(
                gain = opportunity.gain,
                hops = opportunity.path.len(),
                %route,
                "profitable cycle surfaced"
            );
            self.strategy.on_arbitrage_signal(opportunity);
        }
        Ok(())
    }
}

fn describe_path(path: &crate::stages::PoolPath) -> String {
    path.legs
        .iter()
        .map(|leg| format!("{}->{}", leg.input_mint, leg.output_mint))
        .collect::<Vec<_>>()
        .join(",")
}
