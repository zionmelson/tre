use std::sync::Arc;

use anyhow::Result;

use crate::config::AppConfig;

#[derive(Clone)]
pub struct Notifier {
    _config: Arc<AppConfig>,
}

impl Notifier {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self { _config: config }
    }

    pub fn bundle_success(&self, bundle_id: &str) -> Result<()> {
        tracing::info!(
            target = "notifier",
            bundle = bundle_id,
            "gmail notification placeholder"
        );
        Ok(())
    }
}
