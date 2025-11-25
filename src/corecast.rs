use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
    transport::Endpoint,
    Request, Status,
};

use crate::{
    config::AppConfig, proto::solana_corecast::core_cast_client::CoreCastClient as ProtoCoreCast,
};

#[derive(Clone)]
struct AuthInterceptor {
    header: MetadataValue<Ascii>,
}

impl AuthInterceptor {
    fn new(header: MetadataValue<Ascii>) -> Self {
        Self { header }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("authorization", self.header.clone());
        Ok(req)
    }
}

#[derive(Clone)]
pub struct CoreCastClient {
    config: Arc<AppConfig>,
}

impl CoreCastClient {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self { config }
    }

    pub async fn connect(&self) -> Result<()> {
        let endpoint = Endpoint::from_shared(self.endpoint_uri())
            .with_context(|| "invalid CoreCast endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true);

        let channel = endpoint
            .connect()
            .await
            .with_context(|| "failed to connect to CoreCast endpoint")?;

        let auth_header = make_auth_header(&self.config.corecast.authorization)?;
        let _client = ProtoCoreCast::with_interceptor(channel, AuthInterceptor::new(auth_header));
        Ok(())
    }

    fn endpoint_uri(&self) -> String {
        let addr = self.config.corecast.address.trim();
        if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("https://{}", addr)
        }
    }
}

fn make_auth_header(token: &str) -> Result<MetadataValue<Ascii>> {
    let trimmed = token.trim();
    let value = if trimmed.starts_with("Bearer ") {
        trimmed.to_string()
    } else {
        format!("Bearer {}", trimmed)
    };
    value
        .parse()
        .with_context(|| "invalid CoreCast authorization header")
}
