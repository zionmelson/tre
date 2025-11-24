use std::{collections::HashMap, fs, path::Path};

use anyhow::{Context, Result};
use serde_json::Value;

const DEFAULT_POOL_PATH: &str = "idls/pools";

#[derive(Debug, Clone)]
pub struct IdlTemplate {
    pub name: String,
    pub raw: Value,
}

#[derive(Debug, Default, Clone)]
pub struct IdlCache {
    templates: HashMap<String, IdlTemplate>,
}

impl IdlCache {
    pub fn bootstrap() -> Result<Self> {
        Self::load_from(Path::new(DEFAULT_POOL_PATH))
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        let mut cache = HashMap::new();
        for entry in
            fs::read_dir(path).with_context(|| format!("reading IDL dir {}", path.display()))?
        {
            let entry = entry?;
            let file_path = entry.path();
            if file_path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let name = file_path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| file_path.display().to_string());
            let raw = fs::read_to_string(&file_path)
                .with_context(|| format!("reading {}", file_path.display()))?;
            let json: Value = serde_json::from_str(&raw)
                .with_context(|| format!("parsing {}", file_path.display()))?;
            cache.insert(name.clone(), IdlTemplate { name, raw: json });
        }
        Ok(IdlCache { templates: cache })
    }

    pub fn get(&self, name: &str) -> Option<&IdlTemplate> {
        self.templates.get(name)
    }

    pub fn templates(&self) -> impl Iterator<Item = &IdlTemplate> {
        self.templates.values()
    }
}
