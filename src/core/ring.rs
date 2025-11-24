use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::{Backoff, CachePadded};

pub fn spsc_ring<T>(capacity: usize) -> (RingSender<T>, RingReceiver<T>) {
    let bounded = capacity.next_power_of_two().max(2);
    let inner = Arc::new(RingInner::new(bounded));
    (
        RingSender {
            inner: inner.clone(),
        },
        RingReceiver { inner },
    )
}

struct RingInner<T> {
    mask: usize,
    capacity: usize,
    buffer: Vec<UnsafeCell<MaybeUninit<T>>>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

impl<T> RingInner<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        let buffer = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        Self {
            mask: capacity - 1,
            capacity,
            buffer,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    #[inline(always)]
    fn slot(&self, index: usize) -> &UnsafeCell<MaybeUninit<T>> {
        &self.buffer[index & self.mask]
    }
}

impl<T> Drop for RingInner<T> {
    fn drop(&mut self) {
        let mut tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        while tail != head {
            let slot = self.slot(tail);
            unsafe {
                (*slot.get()).assume_init_drop();
            }
            tail = tail.wrapping_add(1);
        }
    }
}

unsafe impl<T: Send> Send for RingInner<T> {}
unsafe impl<T: Send> Sync for RingInner<T> {}

pub struct RingSender<T> {
    inner: Arc<RingInner<T>>,
}

pub struct RingReceiver<T> {
    inner: Arc<RingInner<T>>,
}

impl<T: Send> RingSender<T> {
    pub fn try_push(&self, value: T) -> Result<(), T> {
        let head = self.inner.head.load(Ordering::Relaxed);
        let tail = self.inner.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) == self.inner.capacity {
            return Err(value);
        }
        let slot = self.inner.slot(head);
        unsafe {
            (*slot.get()).write(value);
        }
        self.inner
            .head
            .store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    pub fn spin_push(&self, value: T, running: &AtomicBool) -> bool {
        let mut item = value;
        let backoff = Backoff::new();
        loop {
            match self.try_push(item) {
                Ok(()) => return true,
                Err(v) => {
                    if !running.load(Ordering::Acquire) {
                        drop(v);
                        return false;
                    }
                    item = v;
                    backoff.snooze();
                    continue;
                }
            }
        }
    }
}

impl<T: Send> RingReceiver<T> {
    pub fn try_pop(&self) -> Option<T> {
        let tail = self.inner.tail.load(Ordering::Relaxed);
        let head = self.inner.head.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let slot = self.inner.slot(tail);
        let value = unsafe { (*slot.get()).assume_init_read() };
        self.inner
            .tail
            .store(tail.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    pub fn spin_pop(&self, running: &AtomicBool) -> Option<T> {
        let backoff = Backoff::new();
        loop {
            if let Some(value) = self.try_pop() {
                return Some(value);
            }
            if !running.load(Ordering::Acquire) {
                return self.try_pop();
            }
            backoff.snooze();
        }
    }
}
