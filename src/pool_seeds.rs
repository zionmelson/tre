use crate::meteora::{METEORA_DAMM_PROGRAM_ID, METEORA_DLMM_PROGRAM_ID};
use crate::pipeline::CurveKind;
use anyhow::{Context, Result};
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

pub const ENV_POOL_SEEDS_PATH: &str = "TRE_POOL_SEEDS_PATH";
pub const ENV_INFERRED_POOL_SEEDS_PATH: &str = "TRE_INFERRED_POOL_SEEDS_PATH";
const DEFAULT_POOL_SEED_PATH: &str = "config/pool_seeds.json";
const DEFAULT_INFERRED_POOL_SEED_PATH: &str = "config/pool_seeds_inferred.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolSeed {
    #[serde(
        deserialize_with = "pubkey_from_str",
        serialize_with = "pubkey_to_string"
    )]
    pub pool: Pubkey,
    #[serde(
        deserialize_with = "pubkey_from_str",
        serialize_with = "pubkey_to_string"
    )]
    pub program: Pubkey,
    #[serde(
        deserialize_with = "deserialize_curve_kind",
        serialize_with = "serialize_curve_kind"
    )]
    pub curve: CurveKind,
    #[serde(
        deserialize_with = "pubkey_from_str",
        serialize_with = "pubkey_to_string"
    )]
    pub base_mint: Pubkey,
    #[serde(
        deserialize_with = "pubkey_from_str",
        serialize_with = "pubkey_to_string"
    )]
    pub quote_mint: Pubkey,
}

impl PoolSeed {
    pub fn pair(&self) -> (Pubkey, Pubkey) {
        (self.base_mint, self.quote_mint)
    }

    pub fn canonicalize_curve(&mut self) {
        self.curve = canonical_curve_for_program(self.program, self.curve);
    }
}

#[derive(Debug, Clone, Default)]
pub struct PoolSeedRegistry {
    path: Option<PathBuf>,
    seeds: Vec<PoolSeed>,
    index: HashMap<Pubkey, PoolSeed>,
    snapshot_age: Option<Duration>,
}

impl PoolSeedRegistry {
    pub fn empty() -> Self {
        Self::new_with(None, Vec::new(), None)
    }

    pub fn with_path(path: Option<PathBuf>, seeds: Vec<PoolSeed>) -> Self {
        Self::new_with(path, seeds, None)
    }

    fn new_with(
        path: Option<PathBuf>,
        mut seeds: Vec<PoolSeed>,
        snapshot_age: Option<Duration>,
    ) -> Self {
        for seed in &mut seeds {
            seed.canonicalize_curve();
        }
        let mut index = HashMap::with_capacity(seeds.len());
        for seed in &seeds {
            index.insert(seed.pool, seed.clone());
        }
        Self {
            path,
            seeds,
            index,
            snapshot_age,
        }
    }

    pub fn resolve_default_path() -> PathBuf {
        env::var_os(ENV_POOL_SEEDS_PATH)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_POOL_SEED_PATH))
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let data =
            fs::read_to_string(path).with_context(|| format!("read pool seeds from {path:?}"))?;
        let snapshot: PoolSeedFile = serde_json::from_str(&data)
            .with_context(|| format!("parse pool seeds from {path:?}"))?;
        let age = file_age(path);
        Ok(Self::new_with(
            Some(path.to_path_buf()),
            snapshot.pools,
            age,
        ))
    }

    pub fn load_optional(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let path = path.as_ref();
        let data = match fs::read_to_string(path) {
            Ok(data) => data,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).with_context(|| format!("read pool seeds from {path:?}"));
            }
        };
        let snapshot: PoolSeedFile = serde_json::from_str(&data)
            .with_context(|| format!("parse pool seeds from {path:?}"))?;
        let age = file_age(path);
        Ok(Some(Self::new_with(
            Some(path.to_path_buf()),
            snapshot.pools,
            age,
        )))
    }

    pub fn len(&self) -> usize {
        self.seeds.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seeds.is_empty()
    }

    pub fn seeds(&self) -> &[PoolSeed] {
        &self.seeds
    }

    pub fn get(&self, pool: &Pubkey) -> Option<&PoolSeed> {
        self.index.get(pool)
    }

    pub fn snapshot_age(&self) -> Option<Duration> {
        self.snapshot_age
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn write_snapshot(path: impl AsRef<Path>, seeds: &[PoolSeed]) -> Result<()> {
        let path = path.as_ref();
        let payload = PoolSeedFileRef { pools: seeds };
        let json = serde_json::to_string_pretty(&payload)
            .with_context(|| format!("serialize pool seeds for {path:?}"))?;
        fs::write(path, format!("{json}\n"))
            .with_context(|| format!("write pool seeds to {path:?}"))
    }
}

#[derive(Debug, Clone)]
pub struct PoolSeedOverlay {
    path: PathBuf,
    entries: HashMap<Pubkey, PoolSeed>,
}

impl PoolSeedOverlay {
    pub fn resolve_default_path() -> PathBuf {
        env::var_os(ENV_INFERRED_POOL_SEEDS_PATH)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_INFERRED_POOL_SEED_PATH))
    }

    pub fn load_or_default(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let entries = match fs::read_to_string(&path) {
            Ok(data) => {
                let snapshot: PoolSeedFile = serde_json::from_str(&data)
                    .with_context(|| format!("parse inferred pool seeds from {path:?}"))?;
                snapshot
                    .pools
                    .into_iter()
                    .map(|mut seed| {
                        seed.canonicalize_curve();
                        (seed.pool, seed)
                    })
                    .collect()
            }
            Err(err) if err.kind() == ErrorKind::NotFound => HashMap::new(),
            Err(err) => {
                return Err(err).with_context(|| format!("read inferred pool seeds from {path:?}"));
            }
        };
        Ok(Self { path, entries })
    }

    pub fn seeds(&self) -> Vec<PoolSeed> {
        self.entries.values().cloned().collect()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn get(&self, pool: &Pubkey) -> Option<&PoolSeed> {
        self.entries.get(pool)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn upsert(&mut self, seed: PoolSeed) -> Result<bool> {
        let mut seed = seed;
        seed.canonicalize_curve();
        let needs_write = match self.entries.get(&seed.pool) {
            Some(existing) if existing == &seed => false,
            _ => true,
        };
        if !needs_write {
            return Ok(false);
        }
        self.entries.insert(seed.pool, seed);
        self.persist()?;
        Ok(true)
    }

    fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create inferred seed parent dir for {parent:?}"))?;
        }

        let mut pools: Vec<_> = self.entries.values().cloned().collect();
        pools.sort_by_key(|seed| seed.pool.to_bytes());
        let payload = PoolSeedFile { pools };
        let json = serde_json::to_string_pretty(&payload)
            .with_context(|| format!("serialize inferred pool seeds for {:?}", self.path))?;
        fs::write(&self.path, format!("{json}\n"))
            .with_context(|| format!("write inferred pool seeds to {:?}", self.path))?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PoolSeedFile {
    pools: Vec<PoolSeed>,
}

#[derive(Serialize)]
struct PoolSeedFileRef<'a> {
    pools: &'a [PoolSeed],
}

fn pubkey_from_str<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Pubkey::from_str(&value).map_err(DeError::custom)
}

fn pubkey_to_string<S>(value: &Pubkey, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn deserialize_curve_kind<'de, D>(deserializer: D) -> Result<CurveKind, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "RaydiumClmm" => Ok(CurveKind::RaydiumClmm),
        "OrcaWhirlpool" => Ok(CurveKind::OrcaWhirlpool),
        "MeteoraDamm" => Ok(CurveKind::MeteoraDamm),
        "MeteoraDlmm" => Ok(CurveKind::MeteoraDlmm),
        other => Err(DeError::custom(format!("unsupported curve kind {other}"))),
    }
}

fn serialize_curve_kind<S>(curve: &CurveKind, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let value = match curve {
        CurveKind::RaydiumClmm => "RaydiumClmm",
        CurveKind::OrcaWhirlpool => "OrcaWhirlpool",
        CurveKind::MeteoraDamm => "MeteoraDamm",
        CurveKind::MeteoraDlmm => "MeteoraDlmm",
    };
    serializer.serialize_str(value)
}

fn canonical_curve_for_program(program: Pubkey, curve: CurveKind) -> CurveKind {
    if program == METEORA_DLMM_PROGRAM_ID {
        CurveKind::MeteoraDlmm
    } else if program == METEORA_DAMM_PROGRAM_ID {
        CurveKind::MeteoraDamm
    } else {
        curve
    }
}

fn file_age(path: &Path) -> Option<Duration> {
    let metadata = fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    SystemTime::now().duration_since(modified).ok()
}
