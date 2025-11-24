use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

pub struct SlabPool<T> {
    inner: Arc<SlabInner<T>>,
}

struct SlabInner<T> {
    slots: Vec<UnsafeCell<MaybeUninit<T>>>,
    free: ArrayQueue<usize>,
}

pub struct SlabRef<T> {
    inner: Arc<SlabInner<T>>,
    index: usize,
    occupied: bool,
}

impl<T> SlabPool<T> {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let slots = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        let free = ArrayQueue::new(capacity);
        for idx in 0..capacity {
            free.push(idx).expect("initial freelist push");
        }
        Self {
            inner: Arc::new(SlabInner { slots, free }),
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.slots.len()
    }

    pub fn try_insert(&self, value: T) -> Option<SlabRef<T>> {
        let idx = self.inner.free.pop()?;
        unsafe {
            (*self.inner.slots[idx].get()).write(value);
        }
        Some(SlabRef {
            inner: self.inner.clone(),
            index: idx,
            occupied: true,
        })
    }
}

impl<T> Clone for SlabPool<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> SlabInner<T> {
    fn release(&self, index: usize) {
        self.free
            .push(index)
            .expect("slab freelist should never overflow");
    }
}

impl<T> Deref for SlabRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { (*self.inner.slots[self.index].get()).assume_init_ref() }
    }
}

impl<T> DerefMut for SlabRef<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { (*self.inner.slots[self.index].get()).assume_init_mut() }
    }
}

impl<T> Drop for SlabRef<T> {
    fn drop(&mut self) {
        if !self.occupied {
            return;
        }
        unsafe {
            (*self.inner.slots[self.index].get()).assume_init_drop();
        }
        self.inner.release(self.index);
        self.occupied = false;
    }
}

unsafe impl<T: Send> Send for SlabRef<T> {}
unsafe impl<T: Send> Send for SlabPool<T> {}
unsafe impl<T: Send> Sync for SlabPool<T> {}
