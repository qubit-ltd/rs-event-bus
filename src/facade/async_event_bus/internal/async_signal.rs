// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Waker registration for asynchronous facade state changes.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Wakes tasks waiting for asynchronous facade state changes.
#[derive(Default)]
pub(in crate::facade) struct AsyncSignal {
    /// Current waker for each registered waiter ID.
    wakers: Mutex<HashMap<u64, std::task::Waker>>,
    /// Generates waiter IDs from a wrapping `u64` sequence.
    next_waiter: AtomicU64,
}

impl AsyncSignal {
    /// Wakes all registered waiters after releasing the registry lock.
    pub(in crate::facade) fn notify(&self) {
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for (_, waker) in wakers {
            waker.wake();
        }
    }

    /// Registers or replaces a waiter's current waker.
    ///
    /// # Parameters
    /// - `id`: waiter ID allocated by this signal.
    /// - `waker`: task waker to notify after a state change.
    pub(in crate::facade) fn register_waiter(&self, id: u64, waker: &std::task::Waker) {
        let owned_waker = waker.clone();
        let replaced = {
            let mut wakers = self.wakers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            wakers.insert(id, owned_waker)
        };
        drop(replaced);
    }

    /// Removes a waiter's registration when its future completes or is dropped.
    ///
    /// # Parameters
    /// - `id`: waiter ID to remove.
    pub(in crate::facade) fn unregister(&self, id: u64) {
        let removed = {
            let mut wakers = self.wakers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            wakers.remove(&id)
        };
        drop(removed);
    }

    /// Allocates a unique identifier for a new wait registration.
    ///
    /// # Returns
    /// The next ID in the `u64` sequence; IDs repeat only after the sequence
    /// wraps.
    #[must_use = "waiter IDs must be retained for registration"]
    pub(in crate::facade) fn next_waiter_id(&self) -> u64 {
        self.next_waiter.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::TryLockError;
    use std::sync::Weak;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Waker;

    use super::AsyncSignal;

    struct CloneDropProbe {
        signal: Weak<AsyncSignal>,
    }

    fn assert_registry_unlocked(probe: &CloneDropProbe) {
        if let Some(signal) = probe.signal.upgrade() {
            match signal.wakers.try_lock() {
                Ok(_) | Err(TryLockError::Poisoned(_)) => {}
                Err(TryLockError::WouldBlock) => {
                    panic!("external waker code called under registry lock")
                }
            }
        }
    }

    unsafe fn clone_probe(pointer: *const ()) -> RawWaker {
        // SAFETY: The vtable pointer always originates from an Arc<CloneDropProbe>.
        let probe = unsafe { &*pointer.cast::<CloneDropProbe>() };
        assert_registry_unlocked(probe);
        // SAFETY: This creates exactly one owned Arc reference for the cloned waker.
        unsafe { Arc::<CloneDropProbe>::increment_strong_count(pointer.cast()) };
        RawWaker::new(pointer, &PROBE_VTABLE)
    }

    unsafe fn drop_probe(pointer: *const ()) {
        // SAFETY: Every owned waker invokes this callback exactly once.
        let probe = unsafe { Arc::<CloneDropProbe>::from_raw(pointer.cast()) };
        assert_registry_unlocked(&probe);
    }

    unsafe fn wake_probe(pointer: *const ()) {
        // SAFETY: wake consumes this waker's owned reference.
        unsafe { drop_probe(pointer) };
    }

    unsafe fn wake_probe_by_ref(pointer: *const ()) {
        // SAFETY: wake_by_ref only borrows the live Arc allocation.
        let probe = unsafe { &*pointer.cast::<CloneDropProbe>() };
        assert_registry_unlocked(probe);
    }

    static PROBE_VTABLE: RawWakerVTable = RawWakerVTable::new(clone_probe, wake_probe, wake_probe_by_ref, drop_probe);

    #[test]
    fn test_waker_clone_and_drop_are_outside_the_registry_lock() {
        let signal = Arc::new(AsyncSignal::default());
        let probe = Arc::new(CloneDropProbe {
            signal: Arc::downgrade(&signal),
        });
        let pointer = Arc::into_raw(probe).cast();
        // SAFETY: The custom vtable maintains Arc ownership for each raw waker.
        let waker = unsafe { Waker::from_raw(RawWaker::new(pointer, &PROBE_VTABLE)) };
        signal.register_waiter(1, &waker);
        signal.unregister(1);
        drop(waker);
    }
}
