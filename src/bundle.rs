use anyhow::Result;

use crate::stages::BundleCandidate;

pub struct BundleClient;

impl BundleClient {
    pub fn new() -> Self {
        Self
    }

    pub fn submit(&self, candidate: &BundleCandidate) -> Result<()> {
        // TODO: implement Jito submission flow.
        tracing::info!(seq = candidate.seq, "stub bundle submission");
        Ok(())
    }
}
