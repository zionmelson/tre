use std::{fs, io::Cursor, sync::Arc, time::Duration};

use anyhow::{anyhow, ensure, Context, Result};
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnectorBuilder;
use reqwest::{Client as HttpClient, ClientBuilder as HttpClientBuilder};
use rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};
use rustls_pemfile as pemfile;
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, Endpoint},
    Request, Status, Streaming,
};
use tracing::info;
use url::Url;
use webpki_roots::TLS_SERVER_ROOTS;

use crate::{
    config::AppConfig,
    proto::solana_corecast::{
        core_cast_client::CoreCastClient as ProtoCoreCast, AddressFilter, DexTradeStreamMessage,
        PoolLiquidityChangeStreamMessage, SubscribePoolsRequest, SubscribeTradesRequest,
    },
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
    endpoint: String,
    http_client: HttpClient,
    tls_config: Arc<ClientConfig>,
}

impl CoreCastClient {
    pub fn new(config: Arc<AppConfig>) -> Result<Self> {
        let endpoint = normalize_endpoint(&config.corecast.address)?;
        let ca_pem = match config.corecast.ca_pem.as_ref() {
            Some(path) => {
                info!(target = "corecast", pem = %path.display(), "loading CoreCast custom CA bundle");
                let bytes = fs::read(path).with_context(|| {
                    format!("failed to read CoreCast ca_pem from {}", path.display())
                })?;
                Some(Arc::new(bytes))
            }
            None => None,
        };

        let mut http_builder = HttpClientBuilder::new().use_rustls_tls();
        if let Some(pem) = ca_pem.as_ref() {
            let cert = reqwest::tls::Certificate::from_pem(pem)
                .with_context(|| "failed to parse CoreCast ca_pem for HTTP client")?;
            http_builder = http_builder.add_root_certificate(cert);
        }

        let http_client = http_builder
            .build()
            .with_context(|| "failed to build CoreCast HTTP client")?;

        let has_custom_ca = ca_pem.is_some();
        let tls_config = build_tls_config(ca_pem.as_ref().map(|buf| buf.as_slice()))?;

        let client = Self {
            config,
            endpoint,
            http_client,
            tls_config,
        };

        info!(
            target = "corecast",
            endpoint = %client.endpoint,
            has_custom_ca,
            "CoreCast client initialized"
        );

        Ok(client)
    }

    pub async fn connect(&self) -> Result<()> {
        let _client = self.build_proto_client().await?;
        Ok(())
    }

    pub fn http_client(&self) -> HttpClient {
        self.http_client.clone()
    }

    pub async fn trade_stream(&self) -> Result<Streaming<DexTradeStreamMessage>> {
        info!(target = "corecast", endpoint = %self.endpoint, "subscribing to trade stream");
        let mut client = self.build_proto_client().await?;
        let request = SubscribeTradesRequest {
            program: Some(self.program_filter()?),
            ..Default::default()
        };
        let response = client
            .dex_trades(Request::new(request))
            .await
            .with_context(|| "failed to subscribe to CoreCast dex trades")?;
        Ok(response.into_inner())
    }

    pub async fn pool_stream(&self) -> Result<Streaming<PoolLiquidityChangeStreamMessage>> {
        info!(target = "corecast", endpoint = %self.endpoint, "subscribing to pool stream");
        let mut client = self.build_proto_client().await?;
        let request = SubscribePoolsRequest {
            program: Some(self.program_filter()?),
            ..Default::default()
        };
        let response = client
            .dex_pools(Request::new(request))
            .await
            .with_context(|| "failed to subscribe to CoreCast dex pools")?;
        Ok(response.into_inner())
    }

    async fn build_proto_client(&self) -> Result<ProtoClient> {
        info!(target = "corecast", endpoint = %self.endpoint, "dialing CoreCast gRPC endpoint");
        let endpoint = Endpoint::from_shared(self.endpoint_uri())
            .with_context(|| "invalid CoreCast endpoint")?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true);

        let connector = self.https_connector();

        let channel = endpoint
            .connect_with_connector(connector)
            .await
            .with_context(|| "failed to connect to CoreCast endpoint")?;
        info!(target = "corecast", "CoreCast gRPC channel established");
        let auth_header = make_auth_header(&self.config.corecast.authorization)?;
        Ok(ProtoCoreCast::with_interceptor(
            channel,
            AuthInterceptor::new(auth_header),
        ))
    }

    fn endpoint_uri(&self) -> String {
        self.endpoint.clone()
    }

    fn https_connector(&self) -> hyper_rustls::HttpsConnector<HttpConnector> {
        HttpsConnectorBuilder::new()
            .with_tls_config((*self.tls_config).clone())
            .https_or_http()
            .enable_http2()
            .build()
    }

    fn program_filter(&self) -> Result<AddressFilter> {
        ensure!(
            !self.config.program_addresses.is_empty(),
            "config.program_addresses must contain at least one CoreCast program id"
        );
        Ok(AddressFilter {
            addresses: self.config.program_addresses.clone(),
        })
    }
}

type ProtoClient = ProtoCoreCast<InterceptedService<Channel, AuthInterceptor>>;

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

fn normalize_endpoint(addr: &str) -> Result<String> {
    let trimmed = addr.trim();
    let endpoint = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("https://{}", trimmed)
    };

    let parsed = Url::parse(&endpoint)
        .with_context(|| format!("invalid CoreCast endpoint URI: {}", endpoint))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("CoreCast endpoint missing host"))?
        .to_string();

    // Preserve validation by returning the canonicalized URI.
    let _ = host;
    Ok(endpoint)
}

fn build_tls_config(ca_pem: Option<&[u8]>) -> Result<Arc<ClientConfig>> {
    let mut root_store = RootCertStore::empty();
    root_store.add_trust_anchors(TLS_SERVER_ROOTS.iter().map(|ta| {
        OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject,
            ta.spki,
            ta.name_constraints,
        )
    }));

    if let Some(pem) = ca_pem {
        let mut cursor = Cursor::new(pem);
        let certs = pemfile::certs(&mut cursor)
            .with_context(|| "failed to parse CoreCast ca_pem contents")?;
        ensure!(
            !certs.is_empty(),
            "CoreCast ca_pem did not contain certificates"
        );
        let (added, _skipped) = root_store.add_parsable_certificates(&certs);
        ensure!(
            added > 0,
            "CoreCast ca_pem certificates could not be added to root store"
        );
        info!(
            target = "corecast",
            added, "loaded CoreCast custom CA certificates"
        );
    }

    let config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Arc::new(config))
}
