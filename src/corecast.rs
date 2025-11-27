use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use reqwest::{Client as HttpClient, ClientBuilder as HttpClientBuilder};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status, Streaming,
};
use tracing::{debug, info};
use url::Url;

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
        let domain = endpoint_host(&endpoint)?;
        let mut http_builder = HttpClientBuilder::new().use_rustls_tls();
        let http_client = http_builder
            .build()
            .with_context(|| "failed to build CoreCast HTTP client")?;

        let tls_config = build_tls_config(&domain)?;
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
        if self.config.program_addresses.is_empty() {
            return Err(anyhow!(
                "config.program_addresses must contain at least one CoreCast program id"
            ));
        }
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
    tls_config: ClientTlsConfig,
    warm_capacity: usize,
    connect_timeout: Duration,
    idle: Mutex<Vec<Channel>>,
    semaphore: Arc<Semaphore>,
}

impl ChannelPool {
    fn new(
        endpoint: String,
        tls_config: ClientTlsConfig,
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

        let endpoint = endpoint
            .tls_config(self.tls_config.clone())
            .with_context(|| "failed to configure CoreCast TLS")?;

        let channel = endpoint
            .connect()
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

fn build_tls_config(domain: &str) -> Result<ClientTlsConfig> {
    Ok(ClientTlsConfig::new()
        .with_enabled_roots()
        .domain_name(domain.to_owned()))
}

fn endpoint_host(endpoint: &str) -> Result<String> {
    let parsed = Url::parse(endpoint)
        .with_context(|| format!("invalid CoreCast endpoint URI: {}", endpoint))?;
    parsed
        .host_str()
        .ok_or_else(|| anyhow!("CoreCast endpoint missing host"))
        .map(|host| host.to_string())
}
