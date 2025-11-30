use std::collections::{HashSet, VecDeque};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solana_address_lookup_table_interface::instruction::{
    create_lookup_table, extend_lookup_table,
};
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair, Signature, Signer};
use solana_sdk::transaction::Transaction;
use tracing::{debug, warn};

use crate::config::AppConfig;
use crate::hydration::template_cache::AltHandleId;

const DEFAULT_RPC_URL: &str = "https://api.mainnet-beta.solana.com";
const ALT_REGISTRY_PATH_ENV: &str = "TRE_ALT_REGISTRY";
const ALT_REGISTRY_DEFAULT_FILE: &str = "alt_registry.json";
const MAX_ALT_ACCOUNTS: usize = 256;
const ALT_EXTEND_CHUNK: usize = 32;
const ALT_TABLE_HISTORY: usize = 2;
const ALT_WORKER_BACKOFF_MS: u64 = 750;
const ALT_WORKER_THREAD_NAME: &str = "alt_lookup_worker";
const ALT_WORKER_MAX_THREADS: usize = 4;
const ALT_BLOCKHASH_TTL_SECS: u64 = 20;

#[derive(Clone)]
pub struct AltManager {
    state: Arc<AltManagerState>,
}

struct AltManagerState {
    refresh_count: AtomicU64,
    last_refresh_unix_ms: AtomicU64,
    next_handle: AtomicU32,
    registry: DashMap<Pubkey, AltEntry>,
    handle_index: DashMap<AltHandleId, Pubkey>,
    job_tx: Sender<AltJob>,
    pending_jobs: AtomicU64,
    registry_path: PathBuf,
    persist_lock: Mutex<()>,
}

#[derive(Clone, Debug)]
struct AltEntry {
    handle: AltHandleId,
    pool: Pubkey,
    accounts: Vec<Pubkey>,
    version: u64,
    status: AltStatus,
    tables: VecDeque<AltTableBacking>,
    last_error: Option<String>,
    last_updated_unix_ms: u64,
    last_updated: Instant,
}

#[derive(Clone, Debug)]
struct AltTableBacking {
    address: Pubkey,
    authority: Pubkey,
    creation_slot: u64,
    last_extended_slot: u64,
    addresses: Vec<Pubkey>,
    last_signature: Option<Signature>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum AltStatus {
    Pending,
    Creating,
    Ready,
    Error,
}

#[derive(Clone, Debug)]
pub struct AltTableInfo {
    pub address: Pubkey,
    pub authority: Pubkey,
    pub creation_slot: u64,
    pub last_extended_slot: u64,
    pub addresses: Vec<Pubkey>,
    pub last_updated_unix_ms: u64,
    pub last_updated: Instant,
}

#[derive(Debug, Clone, Copy)]
enum AltJob {
    Ensure { pool: Pubkey, handle: AltHandleId },
}

impl AltManager {
    pub fn new(config: Arc<AppConfig>) -> Result<Self> {
        let payer_path = config.wallets.payer.path.clone();
        let payer = read_keypair_file(&payer_path).map_err(|err| {
            anyhow!(
                "failed to load payer wallet from {}: {err}",
                payer_path.display()
            )
        })?;

        let rpc_urls = if config.quicknode_rpcs.is_empty() {
            vec![DEFAULT_RPC_URL.to_string()]
        } else {
            config.quicknode_rpcs.clone()
        };

        let (job_tx, job_rx) = unbounded();
        let registry_path = registry_file_path();
        let state = Arc::new(AltManagerState {
            refresh_count: AtomicU64::new(0),
            last_refresh_unix_ms: AtomicU64::new(now_millis()),
            next_handle: AtomicU32::new(0),
            registry: DashMap::new(),
            handle_index: DashMap::new(),
            job_tx,
            pending_jobs: AtomicU64::new(0),
            registry_path,
            persist_lock: Mutex::new(()),
        });

        let restored_entries = match state.load_registry_from_disk() {
            Ok(entries) => entries,
            Err(err) => {
                warn!(error = ?err, "failed to restore ALT registry snapshot");
                Vec::new()
            }
        };

        for (pool, handle) in restored_entries {
            if let Err(err) = state.enqueue_job(AltJob::Ensure { pool, handle }) {
                warn!(pool = %pool, error = ?err, "failed to requeue ALT ensure job after restore");
            }
        }

        AltWorker::spawn_many(state.clone(), job_rx, rpc_urls, payer);

        Ok(Self { state })
    }

    pub fn refresh_warm_cache(&self) -> Result<()> {
        self.state.refresh_count.fetch_add(1, Ordering::Relaxed);
        self.state
            .last_refresh_unix_ms
            .store(now_millis(), Ordering::Relaxed);
        self.state.persist_snapshot()
    }

    pub fn stats(&self) -> AltManagerStats {
        let ready_tables = self
            .state
            .registry
            .iter()
            .filter(|entry| matches!(entry.status, AltStatus::Ready))
            .count() as u64;

        AltManagerStats {
            refresh_count: self.state.refresh_count.load(Ordering::Relaxed),
            last_refresh_unix_ms: self.state.last_refresh_unix_ms.load(Ordering::Relaxed),
            registered_handles: self.state.registry.len() as u64,
            ready_tables,
            pending_jobs: self.state.pending_jobs.load(Ordering::Relaxed),
        }
    }

    pub fn ensure_handle(&self, pool: Pubkey, accounts: &[Pubkey]) -> Option<AltHandleId> {
        let normalized = normalize_accounts(accounts);
        if normalized.is_empty() {
            return None;
        }

        if let Some(mut entry) = self.state.registry.get_mut(&pool) {
            if entry.accounts != normalized {
                entry.accounts = normalized.clone();
                entry.version = entry.version.wrapping_add(1);
                entry.status = AltStatus::Pending;
                entry.last_error = None;
                entry.last_updated_unix_ms = now_millis();
                entry.last_updated = Instant::now();
                let handle = entry.handle;
                drop(entry);
                self.queue_job(AltJob::Ensure { pool, handle });
                self.state.persist_snapshot_safely();
                return Some(handle);
            }
            return Some(entry.handle);
        }

        let handle = AltHandleId(self.state.next_handle.fetch_add(1, Ordering::Relaxed) + 1);
        let entry = AltEntry {
            handle,
            pool,
            accounts: normalized.clone(),
            version: 0,
            status: AltStatus::Pending,
            tables: VecDeque::new(),
            last_error: None,
            last_updated_unix_ms: now_millis(),
            last_updated: Instant::now(),
        };
        self.state.registry.insert(pool, entry);
        self.state.handle_index.insert(handle, pool);
        self.queue_job(AltJob::Ensure { pool, handle });
        self.state.persist_snapshot_safely();
        Some(handle)
    }

    pub fn accounts_for(&self, pool: Pubkey) -> Option<Vec<Pubkey>> {
        self.state
            .registry
            .get(&pool)
            .map(|entry| entry.accounts.clone())
    }

    pub fn handle_for_pool(&self, pool: Pubkey) -> Option<AltHandleId> {
        self.state.registry.get(&pool).map(|entry| entry.handle)
    }

    pub fn table_info(&self, handle: AltHandleId) -> Option<AltTableInfo> {
        let pool_guard = self.state.handle_index.get(&handle)?;
        let pool = *pool_guard.value();
        drop(pool_guard);
        let entry = self.state.registry.get(&pool)?;
        entry.tables.front().map(|backing| AltTableInfo {
            address: backing.address,
            authority: backing.authority,
            creation_slot: backing.creation_slot,
            last_extended_slot: backing.last_extended_slot,
            addresses: backing.addresses.clone(),
            last_updated_unix_ms: entry.last_updated_unix_ms,
            last_updated: entry.last_updated,
        })
    }

    fn queue_job(&self, job: AltJob) {
        if let Err(err) = self.state.enqueue_job(job) {
            warn!(error = ?err, "failed to enqueue ALT worker job");
        }
    }
}

impl AltTableInfo {
    pub fn fresh_until(&self, ttl: Duration) -> Instant {
        self.last_updated + ttl
    }
}

struct AltEntrySnapshot {
    _pool: Pubkey,
    handle: AltHandleId,
    accounts: Vec<Pubkey>,
    version: u64,
    current_table: Option<AltTableBacking>,
}

impl AltEntrySnapshot {
    fn matches_current(&self) -> bool {
        self.current_table
            .as_ref()
            .map(|table| table.addresses == self.accounts)
            .unwrap_or(false)
    }
}

struct AltWorker {
    state: Arc<AltManagerState>,
    job_rx: Receiver<AltJob>,
    client: RpcClient,
    payer: Arc<Keypair>,
    rpc_label: String,
    blockhash_cache: Mutex<Option<CachedBlockhash>>,
}

struct CachedBlockhash {
    value: Hash,
    fetched_at: Instant,
}

impl CachedBlockhash {
    fn new(value: Hash) -> Self {
        Self {
            value,
            fetched_at: Instant::now(),
        }
    }

    fn is_valid(&self) -> bool {
        self.fetched_at.elapsed() < Duration::from_secs(ALT_BLOCKHASH_TTL_SECS)
    }
}

impl AltWorker {
    fn spawn_many(
        state: Arc<AltManagerState>,
        job_rx: Receiver<AltJob>,
        rpc_urls: Vec<String>,
        payer: Keypair,
    ) {
        let payer = Arc::new(payer);
        let endpoints = if rpc_urls.is_empty() {
            vec![DEFAULT_RPC_URL.to_string()]
        } else {
            rpc_urls
        };
        let worker_count = endpoints.len().min(ALT_WORKER_MAX_THREADS).max(1);

        for idx in 0..worker_count {
            let worker_state = state.clone();
            let worker_rx = job_rx.clone();
            let rpc_url = endpoints[idx % endpoints.len()].clone();
            let worker_payer = payer.clone();
            thread::Builder::new()
                .name(format!("{ALT_WORKER_THREAD_NAME}-{idx}"))
                .spawn(move || {
                    let client = RpcClient::new_with_commitment(
                        rpc_url.clone(),
                        CommitmentConfig::confirmed(),
                    );
                    let worker = AltWorker {
                        state: worker_state,
                        job_rx: worker_rx,
                        client,
                        payer: worker_payer,
                        rpc_label: rpc_url,
                        blockhash_cache: Mutex::new(None),
                    };
                    worker.run();
                })
                .expect("failed to spawn ALT worker thread");
        }
    }

    fn run(self) {
        debug!(rpc = %self.rpc_label, "ALT worker started");
        while let Ok(job) = self.job_rx.recv() {
            if let Err(err) = self.handle_job(job) {
                warn!(error = ?err, "ALT worker job failed");
            }
            self.state.pending_jobs.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn handle_job(&self, job: AltJob) -> Result<()> {
        match job {
            AltJob::Ensure { pool, handle } => self.ensure_lookup_table(pool, handle),
        }
    }

    fn latest_blockhash(&self) -> Result<Hash> {
        let mut cache = self
            .blockhash_cache
            .lock()
            .expect("blockhash cache mutex poisoned");
        if let Some(existing) = cache.as_ref() {
            if existing.is_valid() {
                return Ok(existing.value);
            }
        }
        let value = self.client.get_latest_blockhash()?;
        *cache = Some(CachedBlockhash::new(value));
        Ok(value)
    }

    fn clear_blockhash_cache(&self) {
        if let Ok(mut cache) = self.blockhash_cache.lock() {
            *cache = None;
        }
    }

    fn execute_transaction(&self, tx: &Transaction) -> Result<Signature> {
        self.client.send_and_confirm_transaction(tx).map_err(|err| {
            self.clear_blockhash_cache();
            err.into()
        })
    }

    fn ensure_lookup_table(&self, pool: Pubkey, handle: AltHandleId) -> Result<()> {
        let snapshot = match self.snapshot(pool) {
            Some(snapshot) => snapshot,
            None => return Ok(()),
        };

        if snapshot.accounts.is_empty() {
            self.mark_ready(pool, snapshot.version);
            return Ok(());
        }

        if snapshot.matches_current() {
            self.mark_ready(pool, snapshot.version);
            return Ok(());
        }

        if snapshot.accounts.len() > MAX_ALT_ACCOUNTS {
            self.mark_failure(pool, snapshot.version, "too many ALT accounts");
            warn!(pool = %pool, count = snapshot.accounts.len(),
                "ALT request exceeded max account capacity");
            return Ok(());
        }

        self.mark_status(pool, snapshot.version, AltStatus::Creating);

        match self.build_lookup_table(&snapshot.accounts) {
            Ok(table) => {
                self.commit_table(pool, snapshot.version, table);
            }
            Err(err) => {
                self.mark_failure(pool, snapshot.version, &format!("{err:?}"));
                warn!(pool = %pool, handle = snapshot.handle.0, rpc = %self.rpc_label, error = ?err, "failed to build ALT");
                thread::sleep(Duration::from_millis(ALT_WORKER_BACKOFF_MS));
                if let Err(send_err) = self.state.enqueue_job(AltJob::Ensure { pool, handle }) {
                    warn!(pool = %pool, error = ?send_err, "failed to requeue ALT ensure job");
                }
            }
        }

        Ok(())
    }

    fn build_lookup_table(&self, accounts: &[Pubkey]) -> Result<AltTableBacking> {
        let authority = self.payer.pubkey();
        let slot = self.client.get_slot()?;
        let (create_ix, table_address) = create_lookup_table(authority, authority, slot);

        let blockhash = self.latest_blockhash()?;
        let create_tx = Transaction::new_signed_with_payer(
            &[create_ix],
            Some(&authority),
            &[self.payer.as_ref()],
            blockhash,
        );
        let mut last_signature = self.execute_transaction(&create_tx)?;
        let mut last_extended_slot = slot;

        for chunk in accounts.chunks(ALT_EXTEND_CHUNK) {
            if chunk.is_empty() {
                continue;
            }
            let extend_ix =
                extend_lookup_table(table_address, authority, Some(authority), chunk.to_vec());
            let bh = self.latest_blockhash()?;
            let extend_tx = Transaction::new_signed_with_payer(
                &[extend_ix],
                Some(&authority),
                &[self.payer.as_ref()],
                bh,
            );
            last_signature = self.execute_transaction(&extend_tx)?;
            last_extended_slot = self.client.get_slot()?;
        }

        debug!(
            table = %table_address,
            account_count = accounts.len(),
            signature = %last_signature,
            "ALT populated"
        );

        Ok(AltTableBacking {
            address: table_address,
            authority,
            creation_slot: slot,
            last_extended_slot,
            addresses: accounts.to_vec(),
            last_signature: Some(last_signature),
        })
    }

    fn snapshot(&self, pool: Pubkey) -> Option<AltEntrySnapshot> {
        self.state
            .registry
            .get(&pool)
            .map(|entry| AltEntrySnapshot {
                _pool: pool,
                handle: entry.handle,
                accounts: entry.accounts.clone(),
                version: entry.version,
                current_table: entry.tables.front().cloned(),
            })
    }

    fn mark_status(&self, pool: Pubkey, version: u64, status: AltStatus) {
        let mut changed = false;
        if let Some(mut entry) = self.state.registry.get_mut(&pool) {
            if entry.version == version {
                entry.status = status;
                entry.last_updated_unix_ms = now_millis();
                entry.last_updated = Instant::now();
                changed = true;
            }
        }
        if changed {
            self.state.persist_snapshot_safely();
        }
    }

    fn mark_ready(&self, pool: Pubkey, version: u64) {
        let mut changed = false;
        if let Some(mut entry) = self.state.registry.get_mut(&pool) {
            if entry.version == version {
                entry.status = AltStatus::Ready;
                entry.last_error = None;
                entry.last_updated_unix_ms = now_millis();
                entry.last_updated = Instant::now();
                changed = true;
            }
        }
        if changed {
            self.state.persist_snapshot_safely();
        }
    }

    fn mark_failure(&self, pool: Pubkey, version: u64, message: &str) {
        let mut changed = false;
        if let Some(mut entry) = self.state.registry.get_mut(&pool) {
            if entry.version == version {
                entry.status = AltStatus::Error;
                entry.last_error = Some(message.to_string());
                entry.last_updated_unix_ms = now_millis();
                entry.last_updated = Instant::now();
                changed = true;
            }
        }
        if changed {
            self.state.persist_snapshot_safely();
        }
    }

    fn commit_table(&self, pool: Pubkey, version: u64, table: AltTableBacking) {
        let mut changed = false;
        if let Some(mut entry) = self.state.registry.get_mut(&pool) {
            if entry.version == version {
                entry.tables.push_front(table);
                while entry.tables.len() > ALT_TABLE_HISTORY {
                    entry.tables.pop_back();
                }
                entry.status = AltStatus::Ready;
                entry.last_error = None;
                entry.last_updated_unix_ms = now_millis();
                entry.last_updated = Instant::now();
                changed = true;
            } else {
                let handle = entry.handle;
                drop(entry);
                if let Err(err) = self.state.enqueue_job(AltJob::Ensure { pool, handle }) {
                    warn!(pool = %pool, error = ?err, "failed to requeue ALT ensure job on version mismatch");
                }
                return;
            }
        }
        if changed {
            self.state.persist_snapshot_safely();
        }
    }
}

impl AltManagerState {
    fn enqueue_job(&self, job: AltJob) -> Result<(), crossbeam_channel::SendError<AltJob>> {
        self.pending_jobs.fetch_add(1, Ordering::Relaxed);
        if let Err(err) = self.job_tx.send(job) {
            self.pending_jobs.fetch_sub(1, Ordering::Relaxed);
            return Err(err);
        }
        Ok(())
    }

    fn load_registry_from_disk(&self) -> Result<Vec<(Pubkey, AltHandleId)>> {
        let contents = match fs::read_to_string(&self.registry_path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };

        let snapshot: AltRegistrySnapshot = serde_json::from_str(&contents).with_context(|| {
            format!(
                "failed to parse ALT registry snapshot from {}",
                self.registry_path.display()
            )
        })?;

        let mut max_handle = 0u32;
        let mut requeue = Vec::new();
        for record in snapshot.entries {
            let pool = Pubkey::from_str(&record.pool)
                .with_context(|| format!("invalid pool pubkey {}", record.pool))?;
            let accounts = record
                .accounts
                .iter()
                .map(|pk| {
                    Pubkey::from_str(pk).with_context(|| format!("invalid ALT account {}", pk))
                })
                .collect::<Result<Vec<_>>>()?;
            let mut tables = VecDeque::new();
            for table_record in record.tables {
                match AltTableBacking::try_from(table_record) {
                    Ok(table) => tables.push_back(table),
                    Err(err) => {
                        warn!(pool = %pool, error = ?err, "failed to restore ALT table entry")
                    }
                }
            }
            let handle = AltHandleId(record.handle);
            let entry = AltEntry {
                handle,
                pool,
                accounts,
                version: record.version,
                status: record.status,
                tables,
                last_error: record.last_error,
                last_updated_unix_ms: record.last_updated_unix_ms,
                last_updated: Instant::now(),
            };
            let needs_requeue = entry.status != AltStatus::Ready
                || entry
                    .tables
                    .front()
                    .map(|table| table.addresses != entry.accounts)
                    .unwrap_or(true);
            self.handle_index.insert(handle, pool);
            self.registry.insert(pool, entry);
            max_handle = max_handle.max(record.handle);
            if needs_requeue {
                requeue.push((pool, handle));
            }
        }

        self.next_handle.store(max_handle, Ordering::Relaxed);
        Ok(requeue)
    }

    fn snapshot(&self) -> AltRegistrySnapshot {
        let entries = self
            .registry
            .iter()
            .map(|entry| AltEntryRecord::from(entry.value().clone()))
            .collect();
        AltRegistrySnapshot {
            generated_unix_ms: now_millis(),
            entries,
        }
    }

    fn persist_snapshot(&self) -> Result<()> {
        let _guard = self.persist_lock.lock().expect("persist lock poisoned");
        let snapshot = self.snapshot();
        if let Some(parent) = self.registry_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let tmp_path = self.registry_path.with_extension("tmp");
        let data = serde_json::to_string_pretty(&snapshot)?;
        fs::write(&tmp_path, data)?;
        fs::rename(&tmp_path, &self.registry_path)?;
        Ok(())
    }

    fn persist_snapshot_safely(&self) {
        if let Err(err) = self.persist_snapshot() {
            warn!(
                path = %self.registry_path.display(),
                error = ?err,
                "failed to persist ALT registry snapshot"
            );
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AltManagerStats {
    pub refresh_count: u64,
    pub last_refresh_unix_ms: u64,
    pub registered_handles: u64,
    pub ready_tables: u64,
    pub pending_jobs: u64,
}

#[derive(Serialize, Deserialize)]
struct AltRegistrySnapshot {
    generated_unix_ms: u64,
    entries: Vec<AltEntryRecord>,
}

#[derive(Serialize, Deserialize)]
struct AltEntryRecord {
    pool: String,
    handle: u32,
    version: u64,
    status: AltStatus,
    accounts: Vec<String>,
    tables: Vec<AltTableRecord>,
    last_error: Option<String>,
    last_updated_unix_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct AltTableRecord {
    address: String,
    authority: String,
    creation_slot: u64,
    last_extended_slot: u64,
    addresses: Vec<String>,
    last_signature: Option<String>,
}

impl From<AltEntry> for AltEntryRecord {
    fn from(value: AltEntry) -> Self {
        let AltEntry {
            handle,
            pool,
            accounts,
            version,
            status,
            tables,
            last_error,
            last_updated_unix_ms,
            last_updated: _,
        } = value;

        Self {
            pool: pool.to_string(),
            handle: handle.0,
            version,
            status,
            accounts: accounts.into_iter().map(|pk| pk.to_string()).collect(),
            tables: tables.into_iter().map(AltTableRecord::from).collect(),
            last_error,
            last_updated_unix_ms,
        }
    }
}

impl From<AltTableBacking> for AltTableRecord {
    fn from(value: AltTableBacking) -> Self {
        let AltTableBacking {
            address,
            authority,
            creation_slot,
            last_extended_slot,
            addresses,
            last_signature,
        } = value;

        Self {
            address: address.to_string(),
            authority: authority.to_string(),
            creation_slot,
            last_extended_slot,
            addresses: addresses.into_iter().map(|pk| pk.to_string()).collect(),
            last_signature: last_signature.map(|sig| sig.to_string()),
        }
    }
}

impl TryFrom<AltTableRecord> for AltTableBacking {
    type Error = anyhow::Error;

    fn try_from(value: AltTableRecord) -> Result<Self> {
        let address = Pubkey::from_str(&value.address)
            .with_context(|| format!("invalid ALT table address {}", value.address))?;
        let authority = Pubkey::from_str(&value.authority)
            .with_context(|| format!("invalid ALT authority {}", value.authority))?;
        let addresses = value
            .addresses
            .into_iter()
            .map(|pk| {
                Pubkey::from_str(&pk).with_context(|| format!("invalid ALT table member {}", pk))
            })
            .collect::<Result<Vec<_>>>()?;
        let last_signature = match value.last_signature {
            Some(sig) => Some(
                Signature::from_str(&sig)
                    .with_context(|| format!("invalid ALT signature {}", sig))?,
            ),
            None => None,
        };

        Ok(Self {
            address,
            authority,
            creation_slot: value.creation_slot,
            last_extended_slot: value.last_extended_slot,
            addresses,
            last_signature,
        })
    }
}

fn normalize_accounts(accounts: &[Pubkey]) -> Vec<Pubkey> {
    let mut seen = HashSet::with_capacity(accounts.len());
    let mut normalized = Vec::with_capacity(accounts.len());
    for pk in accounts {
        if seen.insert(*pk) {
            normalized.push(*pk);
        }
    }
    normalized
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

fn registry_file_path() -> PathBuf {
    env::var(ALT_REGISTRY_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(ALT_REGISTRY_DEFAULT_FILE))
}
