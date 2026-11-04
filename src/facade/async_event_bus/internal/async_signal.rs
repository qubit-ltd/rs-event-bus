// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Waker registration for asynchronous facade state changes.

use std::collections::HashMap;
use std::mem::take;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

use crate::internal::panic_payload::dispose_panic_payload;

/// Wakes tasks waiting for asynchronous facade state changes.
#[derive(Default)]
pub(in crate::facade) struct AsyncSignal {
    /// Current waker for each registered waiter ID.
    wakers: Mutex<HashMap<u64, Waker>>,
    /// Generates waiter IDs from a wrapping `u64` sequence.
    next_waiter: AtomicU64,
}

impl AsyncSignal {
    /// Wakes all registered waiters after releasing the registry lock.
    ///
    /// A panic from one waiter's wake is isolated so the remaining waiters
    /// still receive notification.
    pub(in crate::facade) fn notify(&self) {
        let wakers = take(&mut *self.wakers.lock().unwrap_or_else(PoisonError::into_inner));
        for (_, waker) in wakers {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                dispose_panic_payload(payload);
            }
        }
    }

    /// Registers or replaces a waiter's current waker.
    ///
    /// # Parameters
    /// - `id`: waiter ID allocated by this signal.
    /// - `waker`: task waker to notify after a state change.
    pub(in crate::facade) fn register_waiter(&self, id: u64, waker: &Waker) {
        let owned_waker = waker.clone();
        let replaced = {
            let mut wakers = self.wakers.lock().unwrap_or_else(PoisonError::into_inner);
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
            let mut wakers = self.wakers.lock().unwrap_or_else(PoisonError::into_inner);
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
    #[inline]
    pub(in crate::facade) fn next_waiter_id(&self) -> u64 {
        self.next_waiter.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::sync::Arc;
    use std::sync::TryLockError;
    use std::sync::Weak;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;

    use super::AsyncSignal;

    struct PanicControl {
        panic_id: AtomicUsize,
        counts: Vec<AtomicUsize>,
        panic_payload_drop: bool,
    }

    struct ConfigurableWake {
        id: usize,
        control: Arc<PanicControl>,
    }

    struct DropPanicPayload;

    impl Drop for DropPanicPayload {
        fn drop(&mut self) {
            panic!("intentional panic payload drop panic");
        }
    }

    impl Wake for ConfigurableWake {
        fn wake(self: Arc<Self>) {
            self.control.counts[self.id].fetch_add(1, Ordering::Relaxed);
            if self.control.panic_id.load(Ordering::Relaxed) == self.id {
                if self.control.panic_payload_drop {
                    std::panic::panic_any(DropPanicPayload);
                }
                panic!("intentional waker panic");
            }
        }
    }

    fn assert_waker_panic_is_isolated(panic_payload_drop: bool) {
        const WAITER_COUNT: usize = 33;

        let signal = AsyncSignal::default();
        let control = Arc::new(PanicControl {
            panic_id: AtomicUsize::new(usize::MAX),
            counts: (0..WAITER_COUNT).map(|_| AtomicUsize::new(0)).collect(),
            panic_payload_drop,
        });
        for id in 0..WAITER_COUNT {
            let waker = Waker::from(Arc::new(ConfigurableWake {
                id,
                control: Arc::clone(&control),
            }));
            signal.register_waiter(id as u64, &waker);
        }

        let panic_id = *signal.wakers.lock().unwrap().keys().next().unwrap() as usize;
        control.panic_id.store(panic_id, Ordering::Relaxed);

        let result = catch_unwind(AssertUnwindSafe(|| signal.notify()));
        if let Err(payload) = result {
            // Avoid a second panic while demonstrating that notify unwound.
            std::mem::forget(payload);
            panic!("notify propagated a waker panic");
        }
        assert!(control.counts.iter().all(|count| count.load(Ordering::Relaxed) == 1));
        assert!(signal.wakers.lock().unwrap().is_empty());

        signal.notify();
        assert!(control.counts.iter().all(|count| count.load(Ordering::Relaxed) == 1));

        let waker = Waker::from(Arc::new(ConfigurableWake {
            id: panic_id,
            control: Arc::clone(&control),
        }));
        control.panic_id.store(usize::MAX, Ordering::Relaxed);
        signal.register_waiter(WAITER_COUNT as u64, &waker);
        signal.notify();
        assert_eq!(2, control.counts[panic_id].load(Ordering::Relaxed));
        assert!(signal.wakers.lock().unwrap().is_empty());
    }

    #[test]
    fn test_panicking_waker_does_not_strand_other_waiters() {
        assert_waker_panic_is_isolated(false);
    }

    #[test]
    fn test_panicking_payload_drop_does_not_strand_other_waiters() {
        assert_waker_panic_is_isolated(true);
    }

    struct CloneDropProbe {
        signal: Weak<AsyncSignal>,
        wake_count: AtomicUsize,
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
        let probe = unsafe { Arc::<CloneDropProbe>::from_raw(pointer.cast()) };
        assert_registry_unlocked(&probe);
        probe.wake_count.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn wake_probe_by_ref(pointer: *const ()) {
        // SAFETY: wake_by_ref only borrows the live Arc allocation.
        let probe = unsafe { &*pointer.cast::<CloneDropProbe>() };
        assert_registry_unlocked(probe);
    }

    static PROBE_VTABLE: RawWakerVTable = RawWakerVTable::new(clone_probe, wake_probe, wake_probe_by_ref, drop_probe);

    #[test]
    fn test_waker_callbacks_are_outside_the_registry_lock() {
        let signal = Arc::new(AsyncSignal::default());
        let probe = Arc::new(CloneDropProbe {
            signal: Arc::downgrade(&signal),
            wake_count: AtomicUsize::new(0),
        });
        let pointer = Arc::into_raw(Arc::clone(&probe)).cast();
        // SAFETY: The custom vtable maintains Arc ownership for each raw waker.
        let waker = unsafe { Waker::from_raw(RawWaker::new(pointer, &PROBE_VTABLE)) };
        signal.register_waiter(1, &waker);
        signal.notify();
        assert_eq!(probe.wake_count.load(Ordering::Relaxed), 1);
        signal.register_waiter(2, &waker);
        signal.unregister(2);
        drop(waker);
    }
}
