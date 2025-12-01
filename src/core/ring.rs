use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Lock-free single-producer/single-consumer ring buffer.
pub struct SpscRing<T> {
    inner: Arc<RingInner<T>>,
}

impl<T> SpscRing<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RingInner::new(capacity)),
        }
    }

    pub fn into_split(self) -> (RingProducer<T>, RingConsumer<T>) {
        let producer = RingProducer {
            inner: Arc::clone(&self.inner),
        };
        let consumer = RingConsumer { inner: self.inner };
        (producer, consumer)
    }
}

pub struct RingProducer<T> {
    inner: Arc<RingInner<T>>,
}

impl<T> RingProducer<T> {
    pub fn push(&self, item: T) -> Result<(), T> {
        self.inner.push(item)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

pub struct RingConsumer<T> {
    inner: Arc<RingInner<T>>,
}

impl<T> RingConsumer<T> {
    pub fn pop(&self) -> Option<T> {
        self.inner.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

struct RingInner<T> {
    buffer: Vec<UnsafeCell<MaybeUninit<T>>>,
    mask: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
    capacity: usize,
}

unsafe impl<T: Send> Send for RingInner<T> {}
unsafe impl<T: Send> Sync for RingInner<T> {}

impl<T> RingInner<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ring capacity must be positive");
        let cap = capacity.next_power_of_two();
        let buffer = (0..cap)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        Self {
            buffer,
            mask: cap - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            capacity: cap,
        }
    }

    fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head - tail == self.capacity {
            return Err(item);
        }

        let index = head & self.mask;
        unsafe {
            (*self.buffer[index].get()).write(item);
        }
        self.head.store(head + 1, Ordering::Release);
        Ok(())
    }

    fn pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if tail == head {
            return None;
        }

        let index = tail & self.mask;
        let value = unsafe { (*self.buffer[index].get()).assume_init_read() };
        self.tail.store(tail + 1, Ordering::Release);
        Some(value)
    }

    fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Acquire) == self.head.load(Ordering::Acquire)
    }

    fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.saturating_sub(tail)
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> Drop for RingInner<T> {
    fn drop(&mut self) {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        for idx in tail..head {
            let slot = idx & self.mask;
            unsafe {
                let _ = (*self.buffer[slot].get()).assume_init_read();
            }
        }
    }
}

unsafe impl<T: Send> Send for RingProducer<T> {}
unsafe impl<T: Send> Send for RingConsumer<T> {}
unsafe impl<T: Send> Sync for RingProducer<T> {}
unsafe impl<T: Send> Sync for RingConsumer<T> {}

#[cfg(test)]
mod tests {
    use super::SpscRing;

    #[test]
    fn pushes_and_pops_in_order() {
        let ring = SpscRing::with_capacity(4);
        let (producer, consumer) = ring.into_split();
        for value in 0..4 {
            producer.push(value).unwrap();
        }
        assert!(producer.push(99).is_err());
        for expected in 0..4 {
            assert_eq!(consumer.pop(), Some(expected));
        }
        assert!(consumer.pop().is_none());
    }
}
