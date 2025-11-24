use std::thread::{self, JoinHandle};

use core_affinity::CoreId;
use tracing::warn;

pub mod stage0_ingest;
pub mod stage1_eval;
pub mod stage2_submit;

pub use stage0_ingest::Stage0Ingest;
pub use stage1_eval::Stage1Eval;
pub use stage2_submit::Stage2Submit;

pub fn spawn_stage_thread<F>(name: &str, core_index: Option<usize>, f: F) -> JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
{
    let thread_name = name.to_string();
    let builder = thread::Builder::new().name(thread_name.clone());
    builder
        .spawn(move || {
            if let Some(core) = core_index {
                if let Some(target) = resolve_core(core) {
                    core_affinity::set_for_current(target);
                } else {
                    warn!(
                        target = "stages",
                        stage = %thread_name,
                        requested_core = core,
                        "unable to pin stage to requested core"
                    );
                }
            }
            f();
        })
        .expect("failed to spawn stage thread")
}

fn resolve_core(requested_index: usize) -> Option<CoreId> {
    let cores = core_affinity::get_core_ids()?;
    cores.get(requested_index).cloned()
}
