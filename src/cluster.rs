use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use csv::ReaderBuilder;
use serde::Deserialize;
use solana_client::rpc_client::RpcClient;
use tracing::{debug, info, warn};

const LEADER_POLL_INTERVAL_MS: u64 = 200;
const LEADER_FAILOVER_BACKOFF_MS: u64 = 500;
const LEADER_RPC_TIMEOUT_SECS: u64 = 2;
const SNAPSHOT_STALE_MS: u64 = 1_500;

#[derive(Clone, Debug)]
pub struct LeaderSnapshot {
    pub slot: Option<u64>,
    pub leader_identity: Option<String>,
    pub updated_at: Instant,
    pub last_error: Option<String>,
}

struct LeaderSnapshotInner {
    slot: Option<u64>,
    leader_identity: Option<String>,
    updated_at: Instant,
}

impl Default for LeaderSnapshotInner {
    fn default() -> Self {
        Self {
            slot: None,
            leader_identity: None,
            updated_at: Instant::now(),
        }
    }
}

pub struct LeaderScheduleState {
    inner: RwLock<LeaderSnapshotInner>,
    last_error: RwLock<Option<String>>,
}

impl LeaderScheduleState {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(LeaderSnapshotInner::default()),
            last_error: RwLock::new(None),
        }
    }

    pub fn snapshot(&self) -> LeaderSnapshot {
        let inner = self.inner.read().expect("leader snapshot poisoned");
        let last_error = self
            .last_error
            .read()
            .expect("leader snapshot error poisoned")
            .clone();
        LeaderSnapshot {
            slot: inner.slot,
            leader_identity: inner.leader_identity.clone(),
            updated_at: inner.updated_at,
            last_error,
        }
    }

    /// Forcefully updates the cached snapshot. Intended for tests so they can
    /// bypass the asynchronous watcher and unblock the Stage3 gate logic.
    pub fn inject_snapshot(&self, slot: u64, leader: impl Into<String>) {
        self.update(slot, leader.into());
    }

    fn update(&self, slot: u64, leader: String) {
        {
            let mut inner = self.inner.write().expect("leader snapshot poisoned");
            inner.slot = Some(slot);
            inner.leader_identity = Some(leader);
            inner.updated_at = Instant::now();
        }
        self.clear_error();
    }

    fn record_error(&self, err: String) {
        let mut guard = self
            .last_error
            .write()
            .expect("leader snapshot error poisoned");
        *guard = Some(err);
    }

    fn clear_error(&self) {
        let mut guard = self
            .last_error
            .write()
            .expect("leader snapshot error poisoned");
        *guard = None;
    }
}

impl Default for LeaderScheduleState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn spawn_leader_schedule_watcher(
    stop: Arc<AtomicBool>,
    rpc_endpoints: Vec<String>,
) -> Result<(Arc<LeaderScheduleState>, thread::JoinHandle<Result<()>>)> {
    if rpc_endpoints.is_empty() {
        return Err(anyhow!(
            "leader schedule watcher requires at least one RPC endpoint"
        ));
    }
    let state = Arc::new(LeaderScheduleState::new());
    let thread_state = state.clone();
    let handle = thread::Builder::new()
        .name("leader_watcher".into())
        .spawn(move || run_leader_watcher(stop, rpc_endpoints, thread_state))
        .map_err(|err| anyhow!("failed to spawn leader watcher: {err}"))?;
    Ok((state, handle))
}

fn run_leader_watcher(
    stop: Arc<AtomicBool>,
    rpc_endpoints: Vec<String>,
    state: Arc<LeaderScheduleState>,
) -> Result<()> {
    let mut current_idx = 0usize;
    let mut client = RpcClient::new_with_timeout(
        rpc_endpoints[current_idx].clone(),
        Duration::from_secs(LEADER_RPC_TIMEOUT_SECS),
    );
    info!(endpoint = %rpc_endpoints[current_idx], "leader watcher connected");
    while !stop.load(Ordering::Relaxed) {
        match fetch_leader_snapshot(&client) {
            Ok((slot, leader)) => {
                state.update(slot, leader.clone());
                debug!(slot, leader = %leader, "updated leader snapshot");
                thread::sleep(Duration::from_millis(LEADER_POLL_INTERVAL_MS));
            }
            Err(err) => {
                warn!(
                    endpoint = %rpc_endpoints[current_idx],
                    error = %err,
                    "leader watcher RPC failure"
                );
                state.record_error(err.to_string());
                current_idx = (current_idx + 1) % rpc_endpoints.len();
                thread::sleep(Duration::from_millis(LEADER_FAILOVER_BACKOFF_MS));
                client = RpcClient::new_with_timeout(
                    rpc_endpoints[current_idx].clone(),
                    Duration::from_secs(LEADER_RPC_TIMEOUT_SECS),
                );
                info!(endpoint = %rpc_endpoints[current_idx], "leader watcher failover");
            }
        }
    }
    info!("leader watcher shutting down");
    Ok(())
}

fn fetch_leader_snapshot(client: &RpcClient) -> Result<(u64, String)> {
    let slot = client.get_slot()?;
    let leaders = client.get_slot_leaders(slot, 1)?;
    let leader = leaders
        .get(0)
        .cloned()
        .ok_or_else(|| anyhow!("slot leader response empty"))?
        .to_string();
    Ok((slot, leader))
}

#[derive(Clone)]
pub struct BamValidatorSet {
    source: PathBuf,
    identities: HashSet<String>,
    votes: HashSet<String>,
}

impl BamValidatorSet {
    pub fn load_from_csv(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut reader = ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_path(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let mut identities = HashSet::new();
        let mut votes = HashSet::new();
        for record in reader.deserialize::<BamRow>() {
            let row = record.with_context(|| "failed to parse bam validator row")?;
            if let Some(identity) = normalize_field(&row.identity_account) {
                identities.insert(identity);
            }
            if let Some(vote) = normalize_field(&row.vote_account) {
                votes.insert(vote);
            }
        }
        if identities.is_empty() {
            return Err(anyhow!(
                "bam validator file {} did not contain any identities",
                path.display()
            ));
        }
        info!(
            count = identities.len(),
            source = %path.display(),
            "loaded BAM validator allowlist"
        );
        Ok(Self {
            source: path.to_path_buf(),
            identities,
            votes,
        })
    }

    pub fn contains_identity(&self, identity: &str) -> bool {
        self.identities.contains(identity)
    }

    pub fn contains_vote(&self, vote: &str) -> bool {
        self.votes.contains(vote)
    }

    pub fn len(&self) -> usize {
        self.identities.len()
    }

    pub fn source(&self) -> &Path {
        &self.source
    }
}

fn normalize_field(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[derive(Debug, Deserialize)]
struct BamRow {
    #[serde(default, rename = "identity_account")]
    identity_account: String,
    #[serde(default, rename = "vote_account")]
    vote_account: String,
}

pub struct LeaderScheduleGate {
    state: Arc<LeaderScheduleState>,
    allowlist: Arc<BamValidatorSet>,
    stale_after: Duration,
}

impl LeaderScheduleGate {
    pub fn new(state: Arc<LeaderScheduleState>, allowlist: Arc<BamValidatorSet>) -> Self {
        Self::with_stale_after(state, allowlist, Duration::from_millis(SNAPSHOT_STALE_MS))
    }

    pub fn with_stale_after(
        state: Arc<LeaderScheduleState>,
        allowlist: Arc<BamValidatorSet>,
        stale_after: Duration,
    ) -> Self {
        Self {
            state,
            allowlist,
            stale_after,
        }
    }

    /// Forcefully injects a leader snapshot. Intended for integration tests
    /// so they can keep the gate open without spinning up the async watcher.
    pub fn inject_snapshot_for_tests(&self, slot: u64, leader: impl Into<String>) {
        self.state.inject_snapshot(slot, leader.into());
    }

    pub fn verdict(&self) -> GateVerdict {
        let LeaderSnapshot {
            slot,
            leader_identity,
            updated_at,
            last_error,
        } = self.state.snapshot();
        let age = updated_at.elapsed().as_millis();
        if age > self.stale_after.as_millis() {
            return GateVerdict::Blocked {
                slot,
                leader: leader_identity,
                reason: GateBlockReason::SnapshotStale,
                snapshot_age_ms: age,
                last_error,
            };
        }
        match (slot, leader_identity) {
            (Some(slot), Some(ref leader)) if self.allowlist.contains_identity(leader) => {
                GateVerdict::Allowed {
                    slot,
                    leader: leader.clone(),
                }
            }
            (Some(slot), leader) => GateVerdict::Blocked {
                slot: Some(slot),
                leader,
                reason: GateBlockReason::NotBamLeader,
                snapshot_age_ms: age,
                last_error,
            },
            (None, leader) => GateVerdict::Blocked {
                slot: None,
                leader,
                reason: GateBlockReason::LeaderUnknown,
                snapshot_age_ms: age,
                last_error,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateVerdict {
    Allowed {
        slot: u64,
        leader: String,
    },
    Blocked {
        slot: Option<u64>,
        leader: Option<String>,
        reason: GateBlockReason,
        snapshot_age_ms: u128,
        last_error: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateBlockReason {
    NotBamLeader,
    LeaderUnknown,
    SnapshotStale,
}
