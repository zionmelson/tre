use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;

use crate::builder::templates::SwapTemplateDirection;
use crate::config::WatcherConfig;
use crate::hydration::types::{HydrationStatus, PoolUpdate};

const DEFAULT_HYDRATION_TTL_MS: u64 = 1_000;

#[derive(Clone, Debug)]
pub struct ReadinessPolicy {
    hydration_max_age: Duration,
}

impl ReadinessPolicy {
    pub fn new(hydration_max_age: Duration) -> Self {
        Self { hydration_max_age }
    }

    pub fn from_watcher_config(config: &WatcherConfig) -> Self {
        let ttl_ms = config
            .pool_stale_ttl_ms
            .filter(|ms| *ms > 0)
            .unwrap_or(DEFAULT_HYDRATION_TTL_MS)
            .max(50);
        Self {
            hydration_max_age: Duration::from_millis(ttl_ms),
        }
    }

    pub fn hydration_max_age(&self) -> Duration {
        self.hydration_max_age
    }
}

#[derive(Clone, Debug)]
pub struct PoolReadinessState {
    pub tracked: bool,
    pub hydrated: bool,
    pub template_ready: bool,
    pub last_update_slot: Option<u64>,
    pub last_transition: Instant,
    pub hydrated_slot: Option<u64>,
    pub hydration_ready_until: Option<Instant>,
    pub template_ready_until: Option<Instant>,
    pub template_hash: Option<[u8; 32]>,
    pub template_epoch: Option<u64>,
}

impl Default for PoolReadinessState {
    fn default() -> Self {
        Self {
            tracked: false,
            hydrated: false,
            template_ready: false,
            last_update_slot: None,
            last_transition: Instant::now(),
            hydrated_slot: None,
            hydration_ready_until: None,
            template_ready_until: None,
            template_hash: None,
            template_epoch: None,
        }
    }
}

impl PoolReadinessState {
    pub fn is_ready(&self) -> bool {
        self.tracked && self.hydrated_fresh() && self.template_fresh()
    }

    fn hydrated_fresh(&self) -> bool {
        if !self.hydrated {
            return false;
        }
        match self.hydration_ready_until {
            Some(deadline) => Instant::now() <= deadline,
            None => true,
        }
    }

    fn template_fresh(&self) -> bool {
        if !self.template_ready {
            return false;
        }
        match self.template_ready_until {
            Some(deadline) => Instant::now() <= deadline,
            None => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TemplateSignal {
    pub pool_id: String,
    pub direction: SwapTemplateDirection,
    pub hydrated_at: Instant,
    pub ttl: Duration,
    pub template_hash: Option<[u8; 32]>,
    pub epoch: u64,
}

#[derive(Clone, Debug)]
pub enum ReadinessEvent {
    PoolDiscovered {
        pool_id: String,
    },
    PoolUpdated {
        pool_id: String,
        slot: u64,
    },
    HydrationTransition {
        pool_id: String,
        ready: bool,
        hydrated_slot: Option<u64>,
        fresh_until: Option<Instant>,
    },
    TemplateTransition {
        pool_id: String,
        ready: bool,
        fresh_until: Option<Instant>,
        template_hash: Option<[u8; 32]>,
        template_epoch: Option<u64>,
    },
    ReadinessChanged {
        pool_id: String,
        state: PoolReadinessState,
    },
}

struct ReadinessInner {
    states: DashMap<String, PoolReadinessState>,
    listeners: Mutex<Vec<Sender<ReadinessEvent>>>,
}

#[derive(Clone)]
pub struct ReadinessRegistry {
    inner: Arc<ReadinessInner>,
    policy: ReadinessPolicy,
}

impl ReadinessRegistry {
    pub fn new(policy: ReadinessPolicy) -> Self {
        Self {
            inner: Arc::new(ReadinessInner {
                states: DashMap::new(),
                listeners: Mutex::new(Vec::new()),
            }),
            policy,
        }
    }

    pub fn subscribe(&self) -> Receiver<ReadinessEvent> {
        let (tx, rx) = unbounded();
        self.inner
            .listeners
            .lock()
            .expect("readiness listeners poisoned")
            .push(tx);
        rx
    }

    pub fn state_for_pool(&self, pool_id: &str) -> Option<PoolReadinessState> {
        self.inner.states.get(pool_id).map(|entry| entry.clone())
    }

    pub fn record_template_signal(&self, signal: TemplateSignal) {
        let pool_id = signal.pool_id.clone();
        let fresh_until = signal.hydrated_at + signal.ttl;
        let snapshot = {
            let mut entry = self
                .inner
                .states
                .entry(pool_id.clone())
                .or_insert_with(PoolReadinessState::default);
            entry.tracked = true;
            entry.template_ready = true;
            entry.template_ready_until = Some(fresh_until);
            entry.template_hash = signal.template_hash;
            entry.template_epoch = Some(signal.epoch);
            entry.last_transition = Instant::now();
            entry.clone()
        };
        self.publish(ReadinessEvent::TemplateTransition {
            pool_id: pool_id.clone(),
            ready: true,
            fresh_until: Some(fresh_until),
            template_hash: signal.template_hash,
            template_epoch: Some(signal.epoch),
        });
        self.publish(ReadinessEvent::ReadinessChanged {
            pool_id,
            state: snapshot,
        });
    }

    pub fn record_pool_update(&self, update: &PoolUpdate) {
        let pool_id = update.pool_id.clone();
        let mut discovered = false;
        let mut state_for_event = None;
        {
            let mut entry = self
                .inner
                .states
                .entry(pool_id.clone())
                .or_insert_with(PoolReadinessState::default);
            if !entry.tracked {
                entry.tracked = true;
                discovered = true;
                state_for_event = Some(entry.clone());
            }
            entry.last_update_slot = Some(update.slot);
            entry.last_transition = Instant::now();
        }

        if discovered {
            self.publish(ReadinessEvent::PoolDiscovered {
                pool_id: pool_id.clone(),
            });
        }
        self.publish(ReadinessEvent::PoolUpdated {
            pool_id: pool_id.clone(),
            slot: update.slot,
        });
        if let Some(state) = state_for_event {
            self.publish(ReadinessEvent::ReadinessChanged { pool_id, state });
        }
    }

    pub fn record_hydration_status(&self, pool_id: &str, slot: u64, hydration: &HydrationStatus) {
        let ready = hydration.ready();
        let hydrated_slot = hydration.latest_ready_slot().unwrap_or(slot);
        let deadline = if ready {
            Some(Instant::now() + self.policy.hydration_max_age())
        } else {
            None
        };
        let mut snapshot = None;
        let mut transition = None;
        {
            let mut entry = self
                .inner
                .states
                .entry(pool_id.to_string())
                .or_insert_with(PoolReadinessState::default);
            let previous = entry.hydrated;
            entry.tracked = true;
            entry.last_update_slot = Some(slot);
            entry.hydrated = ready;
            entry.hydrated_slot = ready.then_some(hydrated_slot);
            entry.hydration_ready_until = deadline;
            if previous != ready {
                entry.last_transition = Instant::now();
                snapshot = Some(entry.clone());
                transition = Some(ReadinessEvent::HydrationTransition {
                    pool_id: pool_id.to_string(),
                    ready,
                    hydrated_slot: ready.then_some(hydrated_slot),
                    fresh_until: deadline,
                });
            }
        }
        if let Some(event) = transition {
            self.publish(event);
        }
        if let Some(state) = snapshot {
            self.publish(ReadinessEvent::ReadinessChanged {
                pool_id: pool_id.to_string(),
                state,
            });
        }
    }

    pub fn expire_stale(&self) {
        let now = Instant::now();
        let mut notifications = Vec::new();
        for mut entry in self.inner.states.iter_mut() {
            let pool_id = entry.key().clone();
            let mut transitions = Vec::new();
            if entry.hydrated
                && entry
                    .hydration_ready_until
                    .map(|deadline| now > deadline)
                    .unwrap_or(false)
            {
                entry.hydrated = false;
                entry.hydrated_slot = None;
                entry.hydration_ready_until = None;
                transitions.push(ReadinessEvent::HydrationTransition {
                    pool_id: pool_id.clone(),
                    ready: false,
                    hydrated_slot: None,
                    fresh_until: None,
                });
            }
            if entry.template_ready
                && entry
                    .template_ready_until
                    .map(|deadline| now > deadline)
                    .unwrap_or(false)
            {
                entry.template_ready = false;
                entry.template_ready_until = None;
                transitions.push(ReadinessEvent::TemplateTransition {
                    pool_id: pool_id.clone(),
                    ready: false,
                    fresh_until: None,
                    template_hash: entry.template_hash,
                    template_epoch: entry.template_epoch,
                });
            }
            if !transitions.is_empty() {
                entry.last_transition = now;
                notifications.push((pool_id, entry.clone(), transitions));
            }
        }

        for (pool_id, state, events) in notifications {
            for event in events {
                self.publish(event);
            }
            self.publish(ReadinessEvent::ReadinessChanged { pool_id, state });
        }
    }

    fn publish(&self, event: ReadinessEvent) {
        let mut stale = Vec::new();
        {
            let listeners = self
                .inner
                .listeners
                .lock()
                .expect("readiness listeners poisoned");
            for (idx, tx) in listeners.iter().enumerate() {
                if tx.send(event.clone()).is_err() {
                    stale.push(idx);
                }
            }
        }
        if !stale.is_empty() {
            let mut listeners = self
                .inner
                .listeners
                .lock()
                .expect("readiness listeners poisoned");
            for idx in stale.into_iter().rev() {
                listeners.swap_remove(idx);
            }
        }
    }
}
