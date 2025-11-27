use std::fs;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Context, Result};
use bincode::serialize;
use tokio::runtime::Runtime;
use tokio::sync::Mutex as TokioMutex;
use tonic::codegen::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::VersionedTransaction;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::proto::auth::auth_service_client::AuthServiceClient;
use crate::proto::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
};
use crate::proto::bundle::Bundle;
use crate::proto::packet::{Meta as PacketMeta, Packet};
use crate::proto::searcher::searcher_service_client::SearcherServiceClient;
use crate::proto::searcher::SendBundleRequest;

pub mod tracker;

pub use tracker::{BundleTracker, PendingSubmission};

pub struct BundleClient {
    runtime: Runtime,
    auth: Arc<Keypair>,
    endpoints: Vec<String>,
    tls_ca: Option<Arc<Vec<u8>>>,
    state: Mutex<ClientState>,
}

struct ClientState {
    preferred_idx: usize,
    clients: Vec<
        Option<
            Arc<TokioMutex<SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>>>,
        >,
    >,
    backoff: Vec<EndpointBackoff>,
}

pub struct BundleSubmitResult {
    pub endpoint: String,
    pub bundle_id: String,
}

const BASE_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const BACKOFF_EXP_CAP: u32 = 5;

#[derive(Clone, Copy)]
struct EndpointBackoff {
    failures: u32,
    next_ready_at: Instant,
}

impl EndpointBackoff {
    fn new() -> Self {
        Self {
            failures: 0,
            next_ready_at: Instant::now(),
        }
    }

    fn ready(&self) -> bool {
        Instant::now() >= self.next_ready_at
    }

    fn record_failure(&mut self) {
        self.failures = self.failures.saturating_add(1);
        let exponent = self.failures.min(BACKOFF_EXP_CAP);
        let multiplier = 1u32 << exponent;
        let backoff = (BASE_BACKOFF * multiplier).min(MAX_BACKOFF);
        self.next_ready_at = Instant::now() + backoff;
    }

    fn reset(&mut self) {
        self.failures = 0;
        self.next_ready_at = Instant::now();
    }
}

impl BundleClient {
    pub fn new(config: Arc<AppConfig>, auth: Arc<Keypair>) -> Result<Self> {
        let endpoints = normalize_endpoints(&config.jito.endpoints);
        if endpoints.is_empty() {
            return Err(anyhow!("jito endpoint list is empty"));
        }
        let tls_ca = load_jito_ca(&config)?;
        let preferred_idx = preferred_endpoint_index(&endpoints);
        let runtime = Runtime::new().context("failed to create bundle client runtime")?;
        let client = runtime.block_on(connect_searcher(
            &endpoints[preferred_idx],
            auth.clone(),
            tls_ca.clone(),
        ))?;
        info!(
            endpoint = %endpoints[preferred_idx],
            auth_pubkey = %auth.pubkey(),
            "initialized gRPC bundle client"
        );
        let mut clients = vec![None; endpoints.len()];
        clients[preferred_idx] = Some(Arc::new(TokioMutex::new(client)));
        let endpoint_count = endpoints.len();
        Ok(Self {
            runtime,
            auth,
            endpoints,
            tls_ca,
            state: Mutex::new(ClientState {
                preferred_idx,
                clients,
                backoff: vec![EndpointBackoff::new(); endpoint_count],
            }),
        })
    }

    pub fn submit(&self, tx: &VersionedTransaction) -> Result<BundleSubmitResult> {
        let endpoint_count = self.endpoints.len();
        if endpoint_count == 0 {
            return Err(anyhow!("no bundle endpoints configured"));
        }

        let start_idx = {
            let state = self.state.lock().expect("bundle client state poisoned");
            state.preferred_idx
        };

        let mut last_err: Option<anyhow::Error> = None;
        let indices: Vec<usize> = (0..endpoint_count)
            .map(|attempt| (start_idx + attempt) % endpoint_count)
            .collect();
        let mut ready: Vec<usize> = indices
            .iter()
            .copied()
            .filter(|&idx| self.endpoint_ready(idx))
            .collect();
        if ready.is_empty() {
            ready = indices.clone();
        }

        for idx in ready {
            let endpoint = self.endpoints[idx].clone();
            match self.send_via(idx, tx) {
                Ok(bundle_id) => {
                    self.mark_success(idx);
                    return Ok(BundleSubmitResult {
                        endpoint,
                        bundle_id,
                    });
                }
                Err(err) => {
                    warn!(endpoint = %endpoint, error = %err, "bundle submission attempt failed");
                    last_err = Some(err);
                    self.mark_failure(idx);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("all bundle endpoints failed")))
    }

    fn send_via(&self, idx: usize, tx: &VersionedTransaction) -> Result<String> {
        let packet = proto_packet_from_versioned_tx(tx)?;
        let client = self.client_for_index(idx)?;
        let response = self
            .runtime
            .block_on(async {
                let mut guard = client.lock().await;
                guard
                    .send_bundle(SendBundleRequest {
                        bundle: Some(Bundle {
                            header: None,
                            packets: vec![packet],
                        }),
                    })
                    .await
            })
            .map_err(|status| anyhow!("gRPC send_bundle failed: {status}"))?;
        Ok(response.into_inner().uuid)
    }

    fn client_for_index(
        &self,
        idx: usize,
    ) -> Result<
        Arc<TokioMutex<SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>>>,
    > {
        if let Some(existing) = self.cached_client(idx) {
            return Ok(existing);
        }

        let endpoint = self.endpoints[idx].clone();
        let client = self.runtime.block_on(connect_searcher(
            &endpoint,
            self.auth.clone(),
            self.tls_ca.clone(),
        ))?;
        info!(endpoint = %endpoint, "connected bundle client endpoint");
        let client = Arc::new(TokioMutex::new(client));
        let mut state = self.state.lock().expect("bundle client state poisoned");
        state.clients[idx] = Some(client.clone());
        Ok(client)
    }

    fn cached_client(
        &self,
        idx: usize,
    ) -> Option<
        Arc<TokioMutex<SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>>>,
    > {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.clients.get(idx).cloned())
            .and_then(|entry| entry)
    }

    fn endpoint_ready(&self, idx: usize) -> bool {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.backoff.get(idx).map(|entry| entry.ready()))
            .unwrap_or(true)
    }

    fn mark_success(&self, idx: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.preferred_idx = idx;
            if let Some(entry) = state.backoff.get_mut(idx) {
                entry.reset();
            }
        }
    }

    fn mark_failure(&self, idx: usize) {
        if let Ok(mut state) = self.state.lock() {
            if idx < state.clients.len() {
                state.clients[idx] = None;
            }
            if let Some(entry) = state.backoff.get_mut(idx) {
                entry.record_failure();
            }
        }
    }
}

pub(super) fn normalize_endpoints(endpoints: &[String]) -> Vec<String> {
    endpoints
        .iter()
        .filter_map(|ep| {
            let trimmed = ep.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(strip_api_suffix(trimmed.trim_end_matches('/')))
            }
        })
        .collect()
}

fn preferred_endpoint_index(endpoints: &[String]) -> usize {
    endpoints
        .iter()
        .position(|entry| entry.contains("ny."))
        .unwrap_or(0)
}

fn strip_api_suffix(endpoint: &str) -> String {
    endpoint
        .strip_suffix("/api/v1")
        .unwrap_or(endpoint)
        .to_string()
}

pub(super) async fn connect_searcher(
    endpoint: &str,
    auth_keypair: Arc<Keypair>,
    ca_pem: Option<Arc<Vec<u8>>>,
) -> Result<SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>> {
    let auth_channel = create_grpc_channel(endpoint, ca_pem.clone()).await?;
    let searcher_channel = create_grpc_channel(endpoint, ca_pem).await?;
    let interceptor =
        ClientInterceptor::new(AuthServiceClient::new(auth_channel), auth_keypair).await?;
    Ok(SearcherServiceClient::with_interceptor(
        searcher_channel,
        interceptor,
    ))
}

async fn create_grpc_channel(url: &str, ca_pem: Option<Arc<Vec<u8>>>) -> Result<Channel> {
    let mut endpoint =
        Endpoint::from_shared(url.to_string()).context("invalid block engine url")?;
    if url.starts_with("https") {
        let mut tls = ClientTlsConfig::new();
        if let Some(pem) = ca_pem.as_deref() {
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        endpoint = endpoint.tls_config(tls)?;
    }
    endpoint
        .connect()
        .await
        .context("failed to connect to block engine")
}

fn proto_packet_from_versioned_tx(tx: &VersionedTransaction) -> Result<Packet> {
    let data = serialize(tx).context("failed to serialize bundle transaction")?;
    let size = data.len() as u64;
    Ok(Packet {
        data,
        meta: Some(PacketMeta {
            size,
            addr: String::new(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    })
}

pub(super) fn load_jito_ca(config: &AppConfig) -> Result<Option<Arc<Vec<u8>>>> {
    match config.jito.ca_pem.as_ref() {
        Some(path) => {
            info!(
                target = "bundle_tls",
                pem = %path.display(),
                "loading custom Jito CA bundle"
            );
            let bytes = fs::read(path)
                .with_context(|| format!("failed to read Jito ca_pem from {}", path.display()))?;
            Ok(Some(Arc::new(bytes)))
        }
        None => Ok(None),
    }
}

#[derive(Clone)]
pub(super) struct ClientInterceptor {
    bearer_token: Arc<RwLock<String>>,
}

impl ClientInterceptor {
    async fn new(
        mut auth_client: AuthServiceClient<Channel>,
        auth_keypair: Arc<Keypair>,
    ) -> Result<Self> {
        let (access_token, refresh_token) =
            Self::authenticate(&mut auth_client, auth_keypair.as_ref()).await?;
        let bearer_token = Arc::new(RwLock::new(access_token.value.clone()));
        Self::spawn_refresh_task(
            auth_client,
            bearer_token.clone(),
            refresh_token,
            access_token,
            auth_keypair,
        );
        Ok(Self { bearer_token })
    }

    async fn authenticate(
        auth_client: &mut AuthServiceClient<Channel>,
        keypair: &Keypair,
    ) -> Result<(Token, Token)> {
        let challenge_resp = auth_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: Role::Searcher as i32,
                pubkey: keypair.pubkey().as_ref().to_vec(),
            })
            .await?
            .into_inner();

        let raw_challenge = challenge_resp.challenge;
        let combined_challenge = format!("{}-{}", keypair.pubkey(), raw_challenge);

        info!(
            target = "bundle_auth",
            pubkey = %keypair.pubkey(),
            raw_challenge = %raw_challenge,
            combined_challenge = %combined_challenge,
            "received jito auth challenge"
        );

        let signed_challenge = keypair
            .sign_message(combined_challenge.as_bytes())
            .as_ref()
            .to_vec();

        let tokens = auth_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge: combined_challenge,
                client_pubkey: keypair.pubkey().as_ref().to_vec(),
                signed_challenge,
            })
            .await?
            .into_inner();

        info!(
            target = "bundle_auth",
            pubkey = %keypair.pubkey(),
            "received jito auth tokens"
        );

        Ok((
            tokens.access_token.context("missing access token")?,
            tokens.refresh_token.context("missing refresh token")?,
        ))
    }

    fn spawn_refresh_task(
        mut auth_client: AuthServiceClient<Channel>,
        bearer_token: Arc<RwLock<String>>,
        mut refresh_token: Token,
        mut access_token: Token,
        keypair: Arc<Keypair>,
    ) {
        tokio::spawn(async move {
            loop {
                let access_ttl = ttl_until(access_token.expires_at_utc.as_ref());
                let refresh_ttl = ttl_until(refresh_token.expires_at_utc.as_ref());

                if refresh_ttl < Duration::from_secs(300) {
                    match Self::authenticate(&mut auth_client, keypair.as_ref()).await {
                        Ok((new_access, new_refresh)) => {
                            *bearer_token.write().unwrap() = new_access.value.clone();
                            access_token = new_access;
                            refresh_token = new_refresh;
                            info!("refreshed auth + access tokens");
                        }
                        Err(err) => warn!(error = %err, "failed to refresh auth token"),
                    }
                } else if access_ttl < Duration::from_secs(300) {
                    match auth_client
                        .refresh_access_token(RefreshAccessTokenRequest {
                            refresh_token: refresh_token.value.clone(),
                        })
                        .await
                    {
                        Ok(resp) => {
                            if let Some(token) = resp.into_inner().access_token {
                                *bearer_token.write().unwrap() = token.value.clone();
                                access_token = token;
                                info!("refreshed access token");
                            }
                        }
                        Err(err) => warn!(error = %err, "failed to refresh access token"),
                    }
                }

                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }
}

impl Interceptor for ClientInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let token = self.bearer_token.read().unwrap().clone();
        if !token.is_empty() {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}")
                    .parse()
                    .map_err(|_| Status::internal("invalid bearer token"))?,
            );
        }
        Ok(request)
    }
}

fn ttl_until(ts: Option<&prost_types::Timestamp>) -> Duration {
    if let Some(ts) = ts {
        let system_time = SystemTime::UNIX_EPOCH
            + Duration::from_secs(ts.seconds as u64)
            + Duration::from_nanos(ts.nanos as u64);
        system_time
            .duration_since(SystemTime::now())
            .unwrap_or_else(|_| Duration::from_secs(0))
    } else {
        Duration::from_secs(0)
    }
}
