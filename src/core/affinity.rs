use anyhow::{anyhow, Result};

/// Attempt to pin the current thread to the supplied core id. Returns Ok even if the
/// platform does not expose affinity information so stages can remain portable.
pub fn pin_current_thread(core_id: usize) -> Result<()> {
    let Some(cores) = core_affinity::get_core_ids() else {
        // macOS returns None when affinity is unsupported; treat as soft success.
        return Ok(());
    };

    let Some(target) = cores.into_iter().find(|core| core.id == core_id) else {
        return Err(anyhow!("requested core id {core_id} not available"));
    };

    if core_affinity::set_for_current(target) {
        Ok(())
    } else {
        Err(anyhow!("failed to set core affinity for id {core_id}"))
    }
}

/// Return the list of discoverable core ids, if any.
pub fn available_core_ids() -> Option<Vec<usize>> {
    core_affinity::get_core_ids().map(|cores| cores.into_iter().map(|core| core.id).collect())
}
