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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

#[derive(Default)]
pub(super) struct AsyncSignal {
    next_id: AtomicU64,
    waiters: Arc<Mutex<HashMap<u64, Waker>>>,
}

impl AsyncSignal {
    pub(super) fn register(&self, waker: &Waker) -> AsyncWaiter {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, waker.clone());
        AsyncWaiter {
            id,
            waiters: Arc::clone(&self.waiters),
        }
    }

    pub(super) fn notify_all(&self) {
        let waiters = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
            .map(|(_, waker)| waker)
            .collect::<Vec<_>>();
        for waker in waiters {
            waker.wake();
        }
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

pub(super) struct AsyncWaiter {
    id: u64,
    waiters: Arc<Mutex<HashMap<u64, Waker>>>,
}

impl Drop for AsyncWaiter {
    fn drop(&mut self) {
        self.waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Wake;
    use std::task::Waker;

    use super::AsyncSignal;

    #[derive(Default)]
    struct CountWake(std::sync::atomic::AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn registered_wakers_are_removed_and_notified_outside_the_registry_lock() {
        let signal = AsyncSignal::default();
        let counter = Arc::new(CountWake::default());
        let waker = Waker::from(counter.clone());
        let registration = signal.register(&waker);
        assert_eq!(1, signal.waiter_count());
        signal.notify_all();
        assert_eq!(1, counter.0.load(std::sync::atomic::Ordering::Relaxed));
        drop(registration);
        assert_eq!(0, signal.waiter_count());
    }

    #[test]
    fn dropping_a_pending_registration_cleans_it_up() {
        let signal = AsyncSignal::default();
        let counter = Arc::new(CountWake::default());
        drop(signal.register(&Waker::from(counter)));
        assert_eq!(0, signal.waiter_count());
    }

    #[test]
    fn registered_waker_supports_wake_by_reference() {
        let signal = AsyncSignal::default();
        let counter = Arc::new(CountWake::default());
        let waker = Waker::from(counter.clone());
        let _registration = signal.register(&waker);
        waker.wake_by_ref();
        assert_eq!(1, counter.0.load(std::sync::atomic::Ordering::Relaxed));
    }
}
