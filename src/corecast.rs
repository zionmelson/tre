use std::{
    fs,
    io::Cursor,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, ensure, Context, Result};
use hyper_rustls::HttpsConnectorBuilder;
use reqwest::{Client as HttpClient, ClientBuilder as HttpClientBuilder};
use rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};
use rustls_pemfile as pemfile;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, Endpoint},
    Request, Status, Streaming,
};
use tracing::{debug, info};
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
    channel_pool: Arc<ChannelPool>,
    auth_header: MetadataValue<Ascii>,
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
        let auth_header = make_auth_header(&config.corecast.authorization)?;
        let channel_pool = ChannelPool::new(
            endpoint.clone(),
            tls_config,
            config.corecast.cache.warm_capacity,
            config.corecast.cache.channel_capacity,
        );

        let client = Self {
            config: config.clone(),
            endpoint,
            http_client,
            channel_pool: Arc::new(channel_pool),
            auth_header,
        };

        info!(
            target = "corecast",
            endpoint = %client.endpoint,
            has_custom_ca,
            warm_capacity = config.corecast.cache.warm_capacity,
            channel_capacity = config.corecast.cache.channel_capacity,
            "CoreCast client initialized"
        );

        Ok(client)
    }

    pub async fn connect(&self) -> Result<()> {
        let _lease = self.channel_pool.acquire().await?;
        Ok(())
    }

    pub fn http_client(&self) -> HttpClient {
        self.http_client.clone()
    }

    pub async fn trade_stream(&self) -> Result<CoreCastStream<DexTradeStreamMessage>> {
        info!(target = "corecast", endpoint = %self.endpoint, "subscribing to trade stream");
        let lease = self.channel_pool.acquire().await?;
        self.subscribe_trades(lease).await
    }

    pub async fn pool_stream(&self) -> Result<CoreCastStream<PoolLiquidityChangeStreamMessage>> {
        info!(target = "corecast", endpoint = %self.endpoint, "subscribing to pool stream");
        let lease = self.channel_pool.acquire().await?;
        self.subscribe_pools(lease).await
    }

    async fn subscribe_trades(
        &self,
        lease: ChannelLease,
    ) -> Result<CoreCastStream<DexTradeStreamMessage>> {
        let mut client = self.proto_client_from_lease(&lease);
        let request = SubscribeTradesRequest {
            program: Some(self.program_filter()?),
            ..Default::default()
        };
        let response = client
            .dex_trades(Request::new(request))
            .await
            .with_context(|| "failed to subscribe to CoreCast dex trades")?;
        Ok(CoreCastStream::new(response.into_inner(), lease))
    }

    async fn subscribe_pools(
        &self,
        lease: ChannelLease,
    ) -> Result<CoreCastStream<PoolLiquidityChangeStreamMessage>> {
        let mut client = self.proto_client_from_lease(&lease);
        let request = SubscribePoolsRequest {
            program: Some(self.program_filter()?),
            ..Default::default()
        };
        let response = client
            .dex_pools(Request::new(request))
            .await
            .with_context(|| "failed to subscribe to CoreCast dex pools")?;
        Ok(CoreCastStream::new(response.into_inner(), lease))
    }

    fn proto_client_from_lease(&self, lease: &ChannelLease) -> ProtoClient {
        ProtoCoreCast::with_interceptor(
            lease.channel().clone(),
            AuthInterceptor::new(self.auth_header.clone()),
        )
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

pub struct CoreCastStream<T> {
    inner: Streaming<T>,
    _lease: ChannelLease,
}

impl<T> CoreCastStream<T> {
    fn new(inner: Streaming<T>, lease: ChannelLease) -> Self {
        Self {
            inner,
            _lease: lease,
        }
    }
}

impl<T> Deref for CoreCastStream<T> {
    type Target = Streaming<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for CoreCastStream<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

struct ChannelPool {
    endpoint: String,
    tls_config: Arc<ClientConfig>,
    warm_capacity: usize,
    connect_timeout: Duration,
    idle: Mutex<Vec<Channel>>,
    semaphore: Arc<Semaphore>,
}

impl ChannelPool {
    fn new(
        endpoint: String,
        tls_config: Arc<ClientConfig>,
        warm_capacity: usize,
        channel_capacity: usize,
    ) -> Self {
        let capacity = channel_capacity.max(1);
        let warm_capacity = warm_capacity.min(capacity);
        if warm_capacity == 0 {
            debug!(target = "corecast", "CoreCast channel warm cache disabled");
        }
        Self {
            endpoint,
            tls_config,
            warm_capacity,
            connect_timeout: Duration::from_secs(5),
            idle: Mutex::new(Vec::new()),
            semaphore: Arc::new(Semaphore::new(capacity)),
        }
    }

    async fn acquire(self: &Arc<Self>) -> Result<ChannelLease> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("CoreCast channel pool closed"))?;

        if let Some(channel) = self.try_take_idle() {
            return Ok(ChannelLease::new(self.clone(), channel, permit));
        }

        let channel = self.connect_channel().await?;
        Ok(ChannelLease::new(self.clone(), channel, permit))
    }

    fn try_take_idle(&self) -> Option<Channel> {
        let mut guard = self
            .idle
            .lock()
            .expect("CoreCast channel pool mutex poisoned");
        guard.pop()
    }

    async fn connect_channel(&self) -> Result<Channel> {
        info!(target = "corecast", endpoint = %self.endpoint, "dialing CoreCast gRPC endpoint");
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .with_context(|| "invalid CoreCast endpoint")?
            .connect_timeout(self.connect_timeout)
            .tcp_nodelay(true);

        let connector = HttpsConnectorBuilder::new()
            .with_tls_config((*self.tls_config).clone())
            .https_or_http()
            .enable_http2()
            .build();

        let channel = endpoint
            .connect_with_connector(connector)
            .await
            .with_context(|| "failed to connect to CoreCast endpoint")?;
        info!(target = "corecast", "CoreCast gRPC channel established");
        Ok(channel)
    }

    fn release_channel(&self, channel: Channel) {
        if self.warm_capacity == 0 {
            debug!(
                target = "corecast",
                "dropping CoreCast channel (warm cache disabled)"
            );
            return;
        }

        let mut guard = self
            .idle
            .lock()
            .expect("CoreCast channel pool mutex poisoned");
        if guard.len() < self.warm_capacity {
            guard.push(channel);
        } else {
            debug!(
                target = "corecast",
                "dropping CoreCast channel (warm cache full)"
            );
        }
    }
}

struct ChannelLease {
    pool: Arc<ChannelPool>,
    channel: Option<Channel>,
    permit: Option<OwnedSemaphorePermit>,
}

impl ChannelLease {
    fn new(pool: Arc<ChannelPool>, channel: Channel, permit: OwnedSemaphorePermit) -> Self {
        Self {
            pool,
            channel: Some(channel),
            permit: Some(permit),
        }
    }

    fn channel(&self) -> &Channel {
        self.channel
            .as_ref()
            .expect("CoreCast channel lease missing channel")
    }
}

impl Drop for ChannelLease {
    fn drop(&mut self) {
        if let Some(channel) = self.channel.take() {
            self.pool.release_channel(channel);
        }
        if let Some(permit) = self.permit.take() {
            drop(permit);
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
