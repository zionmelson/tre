use crate::stages::{BundleCandidate, Opportunity};

pub struct StrategyEngine;

impl StrategyEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn evaluate(&self, opportunity: &Opportunity) -> Option<BundleCandidate> {
        // TODO: implement real heuristics.
        Some(BundleCandidate::from_opportunity(opportunity))
    }
}
