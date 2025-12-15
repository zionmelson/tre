use crate::config::{JitoAuthConfig, JitoConfig};
use crate::proto::{
    auth::{
        self, auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role,
    },
    bundle,
    searcher::{self, searcher_service_client::SearcherServiceClient},
};
use prost_types::Timestamp;
use solana_sdk::signature::{Keypair, Signer};
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{self, Instant};
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status, Streaming};
use url::Url;

const ACCESS_REFRESH_SAFETY: Duration = Duration::from_secs(30);
const AUTH_RETRY_MIN_BACKOFF: Duration = Duration::from_secs(5);
const AUTH_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(300);

/// Result alias for Jito client operations.
pub type JitoResult<T> = Result<T, JitoError>;

/// Thin wrapper over the Jito Searcher gRPC service using a single channel.
pub struct JitoClient {
    inner: Arc<JitoInner>,
}

struct JitoInner {
    channel: Channel,
    interceptor: AuthInterceptor,
    _auth_state: Option<Arc<AuthStateInner>>,
}

impl Clone for JitoClient {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl JitoClient {
    /// Establishes a single TLS connection to the first configured Jito endpoint.
    pub async fn connect(
        config: &JitoConfig,
        auth_wallet: Option<Arc<Keypair>>,
    ) -> JitoResult<Self> {
        if config.endpoints.is_empty() {
            return Err(JitoError::NoEndpoints);
        }

        let ca_bytes = std::fs::read(&config.ca_pem).map_err(|source| JitoError::ReadCa {
            path: config.ca_pem.clone(),
            source,
        })?;
        let ca_bytes = Arc::new(ca_bytes);

        let endpoint = &config.endpoints[0];
        let channel = connect_endpoint(endpoint, ca_bytes.as_ref()).await?;
        tracing::info!(target: "jito::client", endpoint, "connected to block engine endpoint");
        let header_store = Arc::new(RwLock::new(None));

        let auth_state = if let Some(auth_cfg) = &config.auth {
            let wallet = auth_wallet.ok_or(JitoError::AuthWalletMissing)?;
            Some(
                AuthStateInner::new(
                    auth_cfg.clone(),
                    ca_bytes.clone(),
                    wallet,
                    header_store.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        let interceptor = AuthInterceptor::new(header_store);
        Ok(Self {
            inner: Arc::new(JitoInner {
                channel,
                interceptor,
                _auth_state: auth_state,
            }),
        })
    }

    /// Sends a bundle to the block engine and returns the UUID.
    pub async fn send_bundle(&self, bundle: bundle::Bundle) -> JitoResult<String> {
        let mut client = SearcherServiceClient::with_interceptor(
            self.inner.channel.clone(),
            self.inner.interceptor.clone(),
        );
        let response = client
            .send_bundle(searcher::SendBundleRequest {
                bundle: Some(bundle),
            })
            .await?;
        Ok(response.into_inner().uuid)
    }

    /// Subscribes to bundle results stream.
    pub async fn subscribe_bundle_results(&self) -> JitoResult<Streaming<bundle::BundleResult>> {
        let mut client = SearcherServiceClient::with_interceptor(
            self.inner.channel.clone(),
            self.inner.interceptor.clone(),
        );
        let response = client
            .subscribe_bundle_results(searcher::SubscribeBundleResultsRequest {})
            .await?;
        Ok(response.into_inner())
    }
}

#[derive(Clone)]
struct AuthInterceptor {
    header: Arc<RwLock<Option<AsciiMetadataValue>>>,
}

impl AuthInterceptor {
    fn new(header: Arc<RwLock<Option<AsciiMetadataValue>>>) -> Self {
        Self { header }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Ok(guard) = self.header.read() {
            if let Some(value) = guard.as_ref() {
                request
                    .metadata_mut()
                    .insert("authorization", value.clone());
            }
        }
        Ok(request)
    }
}

async fn connect_endpoint(endpoint: &str, ca_bytes: &[u8]) -> JitoResult<Channel> {
    let uri = normalize_endpoint(endpoint);
    let url = Url::parse(&uri).map_err(|source| JitoError::InvalidEndpoint {
        uri: uri.clone(),
        source,
    })?;
    let host = url
        .host_str()
        .ok_or_else(|| JitoError::MissingHost(uri.clone()))?
        .to_string();

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_bytes.to_vec()))
        .domain_name(host);

    let channel = Endpoint::from_shared(uri.clone())
        .map_err(|source| JitoError::InvalidUri {
            uri: uri.clone(),
            source,
        })?
        .connect_timeout(Duration::from_secs(5))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .timeout(Duration::from_secs(10))
        .tls_config(tls)?
        .connect()
        .await?;

    Ok(channel)
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("https://{endpoint}")
    }
}

struct AuthStateInner {
    cfg: JitoAuthConfig,
    ca_bytes: Arc<Vec<u8>>,
    wallet: Arc<Keypair>,
    header: Arc<RwLock<Option<AsciiMetadataValue>>>,
    session: Mutex<Option<AuthSession>>,
}

#[derive(Clone)]
struct AuthSession {
    refresh: TokenInfo,
    access_expiry: Option<Instant>,
}

#[derive(Clone)]
struct TokenInfo {
    value: String,
    expiry: Option<Instant>,
}

impl AuthStateInner {
    async fn new(
        cfg: JitoAuthConfig,
        ca_bytes: Arc<Vec<u8>>,
        wallet: Arc<Keypair>,
        header: Arc<RwLock<Option<AsciiMetadataValue>>>,
    ) -> JitoResult<Arc<Self>> {
        let state = Arc::new(Self {
            cfg,
            ca_bytes,
            wallet,
            header,
            session: Mutex::new(None),
        });
        state.authenticate().await?;
        state.spawn_refresh_task();
        Ok(state)
    }

    fn spawn_refresh_task(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            auth_refresh_loop(weak).await;
        });
    }

    async fn authenticate(&self) -> JitoResult<()> {
        let mut client = self.auth_client().await?;
        let challenge = client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: Role::Searcher as i32,
                pubkey: self.wallet.pubkey().to_bytes().to_vec(),
            })
            .await?
            .into_inner()
            .challenge;
        tracing::debug!(
            target: "jito::auth",
            pubkey = %self.wallet.pubkey(),
            challenge = %challenge,
            "received auth challenge"
        );
        let signature = self.sign_challenge(&challenge);
        let tokens = client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge,
                client_pubkey: self.wallet.pubkey().to_bytes().to_vec(),
                signed_challenge: signature,
            })
            .await?
            .into_inner();
        let access = tokens
            .access_token
            .ok_or(JitoError::AuthFieldMissing("access_token"))?;
        let refresh = tokens
            .refresh_token
            .ok_or(JitoError::AuthFieldMissing("refresh_token"))?;
        self.update_tokens(access, refresh).await
    }

    async fn refresh_access_token(&self) -> JitoResult<()> {
        let refresh_token = {
            let guard = self.session.lock().await;
            let session = guard
                .as_ref()
                .ok_or(JitoError::AuthStateMissing("refresh_token"))?;
            if let Some(expiry) = session.refresh.expiry {
                if expiry <= Instant::now() {
                    return Err(JitoError::AuthRefreshExpired);
                }
            }
            session.refresh.value.clone()
        };

        let mut client = self.auth_client().await?;
        let response = client
            .refresh_access_token(RefreshAccessTokenRequest { refresh_token })
            .await?
            .into_inner();
        let access = response
            .access_token
            .ok_or(JitoError::AuthFieldMissing("access_token"))?;
        self.update_access_token(access).await
    }

    async fn update_tokens(&self, access: auth::Token, refresh: auth::Token) -> JitoResult<()> {
        let access_expiry = access
            .expires_at_utc
            .as_ref()
            .and_then(timestamp_to_instant);
        self.apply_header(&access.value)?;
        let refresh_expiry = refresh
            .expires_at_utc
            .as_ref()
            .and_then(timestamp_to_instant);
        let refresh_value = refresh.value;
        let mut guard = self.session.lock().await;
        *guard = Some(AuthSession {
            refresh: TokenInfo {
                value: refresh_value,
                expiry: refresh_expiry,
            },
            access_expiry,
        });
        Ok(())
    }

    async fn update_access_token(&self, access: auth::Token) -> JitoResult<()> {
        let access_expiry = access
            .expires_at_utc
            .as_ref()
            .and_then(timestamp_to_instant);
        self.apply_header(&access.value)?;
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_mut() {
            session.access_expiry = access_expiry;
            Ok(())
        } else {
            Err(JitoError::AuthStateMissing("access_token"))
        }
    }

    fn apply_header(&self, token: &str) -> JitoResult<()> {
        let value = AsciiMetadataValue::try_from(format!("Bearer {token}"))
            .map_err(JitoError::InvalidAuthMetadata)?;
        let mut guard = self
            .header
            .write()
            .map_err(|_| JitoError::AuthHeaderPoisoned)?;
        *guard = Some(value);
        Ok(())
    }

    async fn next_refresh_deadline(&self) -> Instant {
        let guard = self.session.lock().await;
        guard
            .as_ref()
            .and_then(|session| session.access_expiry)
            .and_then(|expiry| expiry.checked_sub(ACCESS_REFRESH_SAFETY))
            .unwrap_or_else(|| Instant::now() + AUTH_RETRY_MIN_BACKOFF)
    }

    async fn auth_client(&self) -> JitoResult<AuthServiceClient<Channel>> {
        let channel = connect_endpoint(&self.cfg.endpoint, self.ca_bytes.as_ref()).await?;
        Ok(AuthServiceClient::new(channel))
    }

    fn sign_challenge(&self, challenge: &str) -> Vec<u8> {
        let payload = format!("{}-{}", self.wallet.pubkey(), challenge);
        self.wallet
            .sign_message(payload.as_bytes())
            .as_ref()
            .to_vec()
    }
}

async fn auth_refresh_loop(state: Weak<AuthStateInner>) {
    let mut backoff = AUTH_RETRY_MIN_BACKOFF;
    loop {
        let Some(inner) = state.upgrade() else {
            break;
        };
        let deadline = inner.next_refresh_deadline().await;
        drop(inner);
        time::sleep_until(deadline).await;
        let Some(inner) = state.upgrade() else {
            break;
        };
        let result = match inner.refresh_access_token().await {
            Ok(_) => Ok(()),
            Err(err) => {
                tracing::warn!(
                    target: "jito::auth",
                    ?err,
                    "access token refresh failed; attempting reauth"
                );
                inner.authenticate().await
            }
        };
        match result {
            Ok(_) => {
                backoff = AUTH_RETRY_MIN_BACKOFF;
            }
            Err(err) => {
                tracing::warn!(target: "jito::auth", ?err, "jito auth attempt failed; retrying");
                drop(inner);
                time::sleep(backoff).await;
                backoff = (backoff * 2).min(AUTH_RETRY_MAX_BACKOFF);
            }
        }
    }
}

fn timestamp_to_instant(ts: &Timestamp) -> Option<Instant> {
    if ts.seconds < 0 || ts.nanos < 0 {
        return None;
    }
    let base = UNIX_EPOCH
        .checked_add(Duration::from_secs(ts.seconds as u64))?
        .checked_add(Duration::from_nanos(ts.nanos as u64))?;
    let now = SystemTime::now();
    let duration = base.duration_since(now).unwrap_or_default();
    Some(Instant::now() + duration)
}

#[derive(thiserror::Error, Debug)]
pub enum JitoError {
    #[error("failed to read CA certificate {path:?}: {source}")]
    ReadCa {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("no Jito endpoints configured")]
    NoEndpoints,
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
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
    #[error("authorization wallet required when jito.auth is configured")]
    AuthWalletMissing,
    #[error("auth token missing field: {0}")]
    AuthFieldMissing(&'static str),
    #[error("auth refresh token expired; re-authentication required")]
    AuthRefreshExpired,
    #[error("auth state missing: {0}")]
    AuthStateMissing(&'static str),
    #[error("invalid auth metadata: {0}")]
    InvalidAuthMetadata(tonic::metadata::errors::InvalidMetadataValue),
    #[error("auth header lock poisoned")]
    AuthHeaderPoisoned,
}
