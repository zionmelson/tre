use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crossbeam_queue::SegQueue;

pub struct SlabPool<T> {
    inner: Arc<SlabInner<T>>,
}

impl<T> Clone for SlabPool<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> SlabPool<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "slab capacity must be positive");
        Self {
            inner: Arc::new(SlabInner::new(capacity)),
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn available(&self) -> usize {
        self.inner.free_len()
    }

    pub fn try_acquire(&self, value: T) -> Option<Lease<T>> {
        self.try_acquire_with(|| value)
    }

    pub fn try_acquire_with<F>(&self, init: F) -> Option<Lease<T>>
    where
        F: FnOnce() -> T,
    {
        SlabInner::acquire_with(&self.inner, init)
    }
}

pub struct Lease<T> {
    inner: Arc<SlabInner<T>>,
    index: usize,
    released: bool,
}

unsafe impl<T: Send> Send for Lease<T> {}
unsafe impl<T: Sync> Sync for Lease<T> {}

impl<T> Lease<T> {
    pub fn into_inner(mut self) -> T {
        let value = self
            .inner
            .clear(self.index)
            .expect("lease should contain value");
        self.released = true;
        value
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            let _ = self.inner.clear(self.index);
            self.released = true;
        }
    }
}

impl<T> Deref for Lease<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.get(self.index)
    }
}

impl<T> DerefMut for Lease<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.get_mut(self.index)
    }
}

impl<T> Drop for Lease<T> {
    fn drop(&mut self) {
        self.release_inner();
    }
}

struct SlabInner<T> {
    entries: Vec<UnsafeCell<Option<T>>>,
    free: SegQueue<usize>,
    capacity: usize,
}

unsafe impl<T: Send> Send for SlabInner<T> {}
unsafe impl<T: Send> Sync for SlabInner<T> {}

impl<T> SlabInner<T> {
    fn new(capacity: usize) -> Self {
        let entries = (0..capacity)
            .map(|_| UnsafeCell::new(None))
            .collect::<Vec<_>>();
        let free = SegQueue::new();
        for idx in 0..capacity {
            free.push(idx);
        }
        Self {
            entries,
            free,
            capacity,
        }
    }

    fn acquire_with<F>(inner: &Arc<Self>, init: F) -> Option<Lease<T>>
    where
        F: FnOnce() -> T,
    {
        inner.free.pop().map(|index| {
            let value = init();
            inner.write(index, value);
            Lease {
                inner: Arc::clone(inner),
                index,
                released: false,
            }
        })
    }

    fn write(&self, index: usize, value: T) {
        unsafe {
            let slot = self.entries[index].get();
            (*slot) = Some(value);
        }
    }

    fn clear(&self, index: usize) -> Option<T> {
        unsafe {
            let slot = self.entries[index].get();
            let value = (*slot).take();
            self.free.push(index);
            value
        }
    }

    fn get(&self, index: usize) -> &T {
        unsafe {
            let slot = self.entries[index].get();
            (*slot).as_ref().expect("lease should contain value")
        }
    }

    fn get_mut(&self, index: usize) -> &mut T {
        unsafe {
            let slot = self.entries[index].get();
            (*slot).as_mut().expect("lease should contain value")
        }
    }

    fn free_len(&self) -> usize {
        self.free.len()
    }
}

#[cfg(test)]
mod tests {
    use super::SlabPool;

    #[test]
    fn acquires_and_releases_slots() {
        let pool = SlabPool::new(1);
        let lease = pool.try_acquire(42).expect("slot available");
        assert_eq!(*lease, 42);
        let value = lease.into_inner();
        assert_eq!(value, 42);
        assert!(pool.try_acquire(7).is_some());
    }
}
