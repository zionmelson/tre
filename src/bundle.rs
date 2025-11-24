use std::sync::Arc;

use anyhow::Result;
use tonic::transport::{Channel, Endpoint};

use crate::{config::AppConfig, proto::searcher::searcher_service_client::SearcherServiceClient};

#[derive(Clone)]
pub struct BundleClient {
    config: Arc<AppConfig>,
}

impl BundleClient {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self { config }
    }

    pub async fn connect(&self) -> Result<SearcherServiceClient<Channel>> {
        let endpoint = self
            .config
            .jito
            .endpoints
            .first()
            .cloned()
            .unwrap_or_else(|| "https://mainnet.block-engine.jito.wtf".to_string());
        let channel = Endpoint::from_shared(endpoint)?.connect().await?;
        Ok(SearcherServiceClient::new(channel))
    }

    pub fn submit_bundle(&self, bundle: &BundleRequest) -> Result<()> {
        tracing::debug!(target = "bundle", uuid = %bundle.uuid, "would submit bundle to Jito");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BundleRequest {
    pub uuid: String,
}
