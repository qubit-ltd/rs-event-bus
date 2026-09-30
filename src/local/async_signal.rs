// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-safe waker registration used by local asynchronous waits.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

use super::internal::AsyncWaiter;

/// Waker registry used to notify asynchronous local-provider waiters.
#[derive(Default)]
pub(super) struct AsyncSignal {
    /// Generates unique waiter registration IDs.
    next_id: AtomicU64,
    /// Registered wakers removed by notification or waiter drop.
    waiters: Arc<Mutex<HashMap<u64, Waker>>>,
}

impl AsyncSignal {
    /// Registers the current task for notification.
    ///
    /// # Parameters
    /// - `waker`: task to wake when the signal changes.
    ///
    /// # Returns
    /// A guard that unregisters this waiter when dropped.
    pub(super) fn register(&self, waker: &Waker) -> AsyncWaiter {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let owned_waker = waker.clone();
        let replaced = {
            let mut waiters = self.waiters.lock().unwrap_or_else(PoisonError::into_inner);
            waiters.insert(id, owned_waker)
        };
        drop(replaced);
        AsyncWaiter::new(id, Arc::clone(&self.waiters))
    }

    /// Removes all registered waiters and wakes them outside the registry lock.
    pub(super) fn notify_all(&self) {
        let waiters = self
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(_, waker)| waker)
            .collect::<Vec<_>>();
        for waker in waiters {
            waker.wake();
        }
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
            match signal.waiters.try_lock() {
                Ok(_) | Err(TryLockError::Poisoned(_)) => {}
                Err(TryLockError::WouldBlock) => {
                    panic!("external waker code called under registry lock")
                }
            }
        }
    }

    unsafe fn clone_probe(pointer: *const ()) -> RawWaker {
        // SAFETY: Every vtable pointer originates from an Arc<CloneDropProbe>.
        let probe = unsafe { &*pointer.cast::<CloneDropProbe>() };
        assert_registry_unlocked(probe);
        // SAFETY: Cloning the raw waker adds exactly one owned strong reference.
        unsafe { Arc::<CloneDropProbe>::increment_strong_count(pointer.cast()) };
        RawWaker::new(pointer, &PROBE_VTABLE)
    }

    unsafe fn drop_probe(pointer: *const ()) {
        // SAFETY: Each owned raw waker consumes its one Arc reference exactly once.
        let probe = unsafe { Arc::<CloneDropProbe>::from_raw(pointer.cast()) };
        assert_registry_unlocked(&probe);
    }

    unsafe fn wake_probe(pointer: *const ()) {
        // SAFETY: wake consumes the same owned reference as the drop callback.
        unsafe { drop_probe(pointer) };
    }

    unsafe fn wake_probe_by_ref(pointer: *const ()) {
        // SAFETY: wake_by_ref borrows the live reference without consuming it.
        let probe = unsafe { &*pointer.cast::<CloneDropProbe>() };
        assert_registry_unlocked(probe);
    }

    static PROBE_VTABLE: RawWakerVTable = RawWakerVTable::new(clone_probe, wake_probe, wake_probe_by_ref, drop_probe);

    #[test]
    fn test_registered_waker_clone_and_drop_run_outside_registry_lock() {
        let signal = Arc::new(AsyncSignal::default());
        let probe = Arc::new(CloneDropProbe {
            signal: Arc::downgrade(&signal),
        });
        let pointer = Arc::into_raw(probe).cast();
        // SAFETY: The vtable preserves owned Arc references and shared access.
        let waker = unsafe { Waker::from_raw(RawWaker::new(pointer, &PROBE_VTABLE)) };
        let registration = signal.register(&waker);
        drop(registration);
        drop(waker);
    }
}
