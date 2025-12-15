use crate::config::CorecastConfig;
use crate::proto::solana_corecast::{
    self, core_cast_client::CoreCastClient, AddressFilter, DexTradeStreamMessage,
    SubscribePoolsRequest, SubscribeTradesRequest,
};
use rustls::pki_types::InvalidDnsNameError;
use solana_sdk::pubkey::Pubkey;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status, Streaming};
use url::Url;

/// Concrete stream type alias for Bitquery pool updates.
pub type PoolStream = Streaming<solana_corecast::PoolLiquidityChangeStreamMessage>;
/// Concrete stream type alias for Bitquery trade updates.
pub type TradeStream = Streaming<DexTradeStreamMessage>;

type CoreCastGrpc = CoreCastClient<InterceptedService<Channel, AuthInterceptor>>;
type CorecastResult<T> = std::result::Result<T, CorecastError>;

/// Simple CoreCast connection cache that reuses a single TLS channel per worker.
#[derive(Clone)]
pub struct CorecastClientPool {
    config: CorecastConfig,
    cached: Option<CorecastClient>,
}

impl CorecastClientPool {
    /// Creates a new pool that lazily dials CoreCast.
    pub fn new(config: &CorecastConfig) -> Self {
        Self {
            config: config.clone(),
            cached: None,
        }
    }

    /// Returns a client, dialing CoreCast only if the cached channel is absent/invalid.
    pub async fn client(&mut self) -> CorecastResult<CorecastClient> {
        if self.cached.is_none() {
            let client = CorecastClient::connect(&self.config).await?;
            self.cached = Some(client);
        }
        Ok(self.cached.as_ref().expect("client cached").clone())
    }

    /// Drops the cached channel forcing the next access to re-dial CoreCast.
    pub fn invalidate(&mut self) {
        self.cached = None;
    }

    /// Updates the cached configuration and invalidates if the endpoint/auth changed.
    pub fn reconfigure(&mut self, config: &CorecastConfig) {
        if self.config.address != config.address
            || self.config.authorization != config.authorization
        {
            self.invalidate();
        }
        self.config = config.clone();
    }
}

/// Lightweight Bitquery CoreCast gRPC client with TLS + auth interceptor.
#[derive(Clone)]
pub struct CorecastClient {
    inner: CoreCastGrpc,
}

impl CorecastClient {
    /// Establishes a TLS channel to CoreCast and wires the static authorization bearer.
    pub async fn connect(config: &CorecastConfig) -> CorecastResult<Self> {
        let endpoint_uri = normalize_endpoint(&config.address);
        let url = Url::parse(&endpoint_uri).map_err(|source| CorecastError::InvalidEndpoint {
            uri: endpoint_uri.clone(),
            source,
        })?;
        let host = url
            .host_str()
            .ok_or_else(|| CorecastError::MissingHost(endpoint_uri.clone()))?
            .to_string();

        let mut tls = ClientTlsConfig::new().domain_name(host.clone());
        if let Some(ca_path) = &config.ca_pem {
            let certs = load_ca_certificates(ca_path)?;
            for cert in certs {
                tls = tls.ca_certificate(cert);
            }
        }

        let endpoint = Endpoint::from_shared(endpoint_uri.clone())
            .map_err(|source| CorecastError::InvalidUri {
                uri: endpoint_uri.clone(),
                source,
            })?
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .timeout(Duration::from_secs(30))
            .rate_limit(256, Duration::from_secs(1))
            .tls_config(tls)?;

        let channel = endpoint.connect().await?;
        let interceptor = AuthInterceptor::new(&config.authorization)?;
        let client = CoreCastClient::with_interceptor(channel, interceptor);

        Ok(Self { inner: client })
    }

    /// Opens a pooled liquidity change subscription stream.
    pub async fn subscribe_pools(
        &self,
        request: SubscribePoolsRequest,
    ) -> CorecastResult<PoolStream> {
        let mut client = self.inner.clone();
        let response = client.dex_pools(request).await?;
        Ok(response.into_inner())
    }

    /// Opens a trade execution subscription stream.
    pub async fn subscribe_trades(
        &self,
        request: SubscribeTradesRequest,
    ) -> CorecastResult<TradeStream> {
        let mut client = self.inner.clone();
        let response = client.dex_trades(request).await?;
        Ok(response.into_inner())
    }
}

/// Converts a slice of pubkeys into an AddressFilter understood by CoreCast.
pub fn address_filter(keys: &[Pubkey]) -> Option<AddressFilter> {
    if keys.is_empty() {
        None
    } else {
        Some(AddressFilter {
            addresses: keys.iter().map(|pk| pk.to_string()).collect(),
        })
    }
}

fn normalize_endpoint(address: &str) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_string()
    } else {
        format!("https://{address}")
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CorecastError {
    #[error("invalid endpoint {uri}: {source}")]
    InvalidEndpoint {
        uri: String,
        source: url::ParseError,
    },
    #[error("endpoint missing host: {0}")]
    MissingHost(String),
    #[error("invalid URI {uri}: {source}")]
    InvalidUri {
        uri: String,
        source: tonic::transport::Error,
    },
    #[error("failed to read corecast ca {path:?}: {source}")]
    CaRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to read corecast ca directory {path:?}: {source}")]
    CaDirRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("corecast ca {path:?} contained no certificates")]
    EmptyCa { path: PathBuf },
    #[error(transparent)]
    InvalidDnsName(#[from] InvalidDnsNameError),
    #[error("invalid authorization header: {0}")]
    InvalidAuthorization(tonic::metadata::errors::InvalidMetadataValue),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
}

#[derive(Clone)]
struct AuthInterceptor {
    authorization: MetadataValue<tonic::metadata::Ascii>,
}

impl AuthInterceptor {
    fn new(token: &str) -> CorecastResult<Self> {
        let value = MetadataValue::from_str(token).map_err(CorecastError::InvalidAuthorization)?;
        Ok(Self {
            authorization: value,
        })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        Ok(request)
    }
}

fn load_ca_certificates(path: &Path) -> CorecastResult<Vec<Certificate>> {
    if path.is_file() {
        return Ok(vec![certificate_from_path(path)?]);
    }

    let mut certificates = Vec::new();
    let entries = fs::read_dir(path).map_err(|source| CorecastError::CaDirRead {
        path: path.to_path_buf(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| CorecastError::CaDirRead {
            path: path.to_path_buf(),
            source,
        })?;
        let entry_path = entry.path();
        if !entry_path.is_file() {
            continue;
        }
        let ext = entry_path
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_ascii_lowercase());
        if let Some(ext) = ext {
            if ext != "pem" && ext != "crt" && ext != "cer" {
                continue;
            }
        }
        certificates.push(certificate_from_path(&entry_path)?);
    }

    if certificates.is_empty() {
        return Err(CorecastError::EmptyCa {
            path: path.to_path_buf(),
        });
    }

    Ok(certificates)
}

fn certificate_from_path(path: &Path) -> CorecastResult<Certificate> {
    let bytes = fs::read(path).map_err(|source| CorecastError::CaRead {
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.is_empty() {
        return Err(CorecastError::EmptyCa {
            path: path.to_path_buf(),
        });
    }
    Ok(Certificate::from_pem(bytes))
}
