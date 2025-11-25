use anyhow::Result;

pub struct Notifier;

impl Notifier {
    pub fn new() -> Self {
        Self
    }

    pub fn notify_success(&self, message: &str) -> Result<()> {
        // TODO: integrate Gmail API.
        tracing::info!(%message, "notifier placeholder");
        Ok(())
    }
}
