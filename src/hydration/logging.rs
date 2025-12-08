use crate::hydration::types::{
    HydrationComponent, PoolSnapshot, CP_AMM_PROGRAM_ID, LB_CLMM_PROGRAM_ID, RAYDIUM_V3_PROGRAM_ID,
    WHIRLPOOL_PROGRAM_ID,
};

#[derive(Debug, Clone)]
pub struct PendingComponentDetail {
    pub component: HydrationComponent,
    pub slot: Option<u64>,
    pub slot_delta: Option<u64>,
    pub age_ms: u64,
}

#[derive(Debug, Clone)]
pub struct PendingComponentSummary {
    pub lag_ms: u64,
    pub lag_slots: u64,
    pub components: Vec<PendingComponentDetail>,
}

impl PendingComponentSummary {
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    pub fn component_names(&self) -> String {
        self.components
            .iter()
            .map(|detail| detail.component.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn detail_string(&self) -> String {
        self.components
            .iter()
            .map(|detail| {
                let slot = detail
                    .slot
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let slot_delta = detail
                    .slot_delta
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                format!(
                    "{}(slot={},slot_delta={},age_ms={})",
                    detail.component.as_str(),
                    slot,
                    slot_delta,
                    detail.age_ms
                )
            })
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

pub fn describe_pending_components(snapshot: &PoolSnapshot) -> PendingComponentSummary {
    let pending = snapshot.hydration.pending_components();
    let lag_ms = snapshot.hydration.pending_age_ms(snapshot.updated_at);
    let lag_slots = snapshot.hydration.slot_lag(snapshot.slot);
    let mut components = Vec::with_capacity(pending.len());
    for component in pending {
        let slot = snapshot.hydration.component_slot(component);
        let slot_delta = slot.map(|component_slot| snapshot.slot.saturating_sub(component_slot));
        let age_ms = snapshot
            .hydration
            .component_pending_age_ms(component, snapshot.updated_at);
        components.push(PendingComponentDetail {
            component,
            slot,
            slot_delta,
            age_ms,
        });
    }
    PendingComponentSummary {
        lag_ms,
        lag_slots,
        components,
    }
}

pub fn adapter_log_target(adapter: &str) -> &'static str {
    match adapter {
        "raydium_v3" => "adapter::raydium_v3",
        "cp_amm" => "adapter::cp_amm",
        "lb_clmm" => "adapter::lb_clmm",
        "whirlpool" => "adapter::whirlpool",
        _ => "hydration",
    }
}

pub fn program_log_target(program_id: &str) -> &'static str {
    match program_id {
        RAYDIUM_V3_PROGRAM_ID => "adapter::raydium_v3",
        CP_AMM_PROGRAM_ID => "adapter::cp_amm",
        LB_CLMM_PROGRAM_ID => "adapter::lb_clmm",
        WHIRLPOOL_PROGRAM_ID => "adapter::whirlpool",
        _ => "hydration",
    }
}
