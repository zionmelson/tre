use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::hydration::HydrationSnapshot;

pub struct SnapshotChannel {
    inner: Arc<SnapshotInner>,
}

impl SnapshotChannel {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SnapshotInner::new()),
        }
    }

    pub fn split(self) -> (SnapshotPublisher, SnapshotReader) {
        (
            SnapshotPublisher {
                inner: self.inner.clone(),
            },
            SnapshotReader {
                inner: self.inner,
                last_seq: 0,
            },
        )
    }
}

pub struct SnapshotPublisher {
    inner: Arc<SnapshotInner>,
}

impl SnapshotPublisher {
    pub fn publish(&self, snapshot: HydrationSnapshot) -> bool {
        let mut slot = self.inner.slot.lock().expect("snapshot slot poisoned");
        let overwritten = slot.is_some();
        *slot = Some(snapshot);
        self.inner.seq.fetch_add(1, Ordering::Release);
        overwritten
    }
}

pub struct SnapshotReader {
    inner: Arc<SnapshotInner>,
    last_seq: u64,
}

impl SnapshotReader {
    pub fn take_latest(&mut self) -> Option<HydrationSnapshot> {
        let seq = self.inner.seq.load(Ordering::Acquire);
        if seq == self.last_seq {
            return None;
        }
        let mut slot = self.inner.slot.lock().expect("snapshot slot poisoned");
        let snapshot = slot.take();
        if snapshot.is_some() {
            self.last_seq = seq;
        }
        snapshot
    }
}

struct SnapshotInner {
    slot: Mutex<Option<HydrationSnapshot>>,
    seq: AtomicU64,
}

impl SnapshotInner {
    fn new() -> Self {
        Self {
            slot: Mutex::new(None),
            seq: AtomicU64::new(0),
        }
    }
}
