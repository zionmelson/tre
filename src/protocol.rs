use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::codegen::pools_static::{PoolTemplate, POOL_TEMPLATES};
use crate::idl::IdlCache;

const DEFAULT_MANIFEST: &str = "config/protocols.json";

#[derive(Debug, Clone)]
pub struct ProtocolDefinition {
    slug: String,
    _program: String,
    template: &'static PoolTemplate,
}

impl ProtocolDefinition {
    pub fn slug(&self) -> &str {
        &self.slug
    }

    pub fn template(&self) -> &'static PoolTemplate {
        self.template
    }
}

#[derive(Debug, Clone)]
pub struct ProtocolRegistry {
    by_program: HashMap<String, Arc<ProtocolDefinition>>,
}

impl ProtocolRegistry {
    pub fn bootstrap(idl_cache: &IdlCache) -> Result<Self> {
        Self::from_path(DEFAULT_MANIFEST, idl_cache)
    }

    pub fn from_path<P: AsRef<Path>>(path: P, idl_cache: &IdlCache) -> Result<Self> {
        let path_ref = path.as_ref();
        let manifest = fs::read_to_string(path_ref)
            .with_context(|| format!("reading protocol manifest {}", path_ref.display()))?;
        let entries: Vec<ManifestEntry> = serde_json::from_str(&manifest)
            .with_context(|| format!("parsing protocol manifest {}", path_ref.display()))?;

        let mut template_lookup = HashMap::new();
        for template in POOL_TEMPLATES {
            template_lookup.insert(template.name, template);
        }

        let mut by_program = HashMap::new();
        for entry in entries {
            if by_program.contains_key(&entry.program) {
                bail!("duplicate program {} in protocol manifest", entry.program);
            }

            let template = template_lookup
                .get(entry.protocol.as_str())
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "protocol '{}' missing template in pools_static.rs",
                        entry.protocol
                    )
                })?;

            if idl_cache.get(&entry.protocol).is_none() {
                bail!("protocol '{}' missing IDL template", entry.protocol);
            }

            let definition = ProtocolDefinition {
                slug: entry.protocol.clone(),
                _program: entry.program.clone(),
                template,
            };

            by_program.insert(entry.program, Arc::new(definition));
        }

        Ok(Self { by_program })
    }

    pub fn resolve(&self, program: &str) -> Option<Arc<ProtocolDefinition>> {
        self.by_program.get(program).cloned()
    }

    pub fn len(&self) -> usize {
        self.by_program.len()
    }
}

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    program: String,
    protocol: String,
}
