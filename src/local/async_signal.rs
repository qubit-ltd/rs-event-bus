// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-safe waker registration used by local asynchronous waits.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

use super::internal::AsyncWaiter;
use crate::internal::panic_payload::dispose_panic_payload;

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

    /// Registers the current task or updates its existing waiter in place.
    ///
    /// # Parameters
    /// - `waiter`: optional registration to update, populated when absent.
    /// - `waker`: task notification handle to register or replace.
    pub(super) fn register_or_update(&self, waiter: &mut Option<AsyncWaiter>, waker: &Waker) {
        if let Some(waiter) = waiter {
            waiter.update(waker);
        } else {
            *waiter = Some(self.register(waker));
        }
    }

    /// Removes all registered waiters and wakes them outside the registry lock.
    ///
    /// Each waker's unwind panic is isolated so it cannot interrupt
    /// notification of the remaining waiters.
    pub(super) fn notify_all(&self) {
        let waiters = self
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(_, waker)| waker)
            .collect::<Vec<_>>();
        for waker in waiters {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                dispose_panic_payload(payload);
            }
        }
    }

    /// Returns the number of registered waiters for unit tests.
    ///
    /// # Returns
    /// The current number of stored task wakers.
    #[must_use]
    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner).len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::TryLockError;
    use std::sync::Weak;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread;

    use super::AsyncSignal;

    struct CountedRawWaker {
        clones: AtomicUsize,
        wakes: AtomicUsize,
    }

    unsafe fn clone_counted(pointer: *const ()) -> RawWaker {
        // SAFETY: Every pointer is owned by an Arc and retained by the vtable.
        let state = unsafe { &*pointer.cast::<CountedRawWaker>() };
        state.clones.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The returned RawWaker receives exactly one strong reference.
        unsafe { Arc::<CountedRawWaker>::increment_strong_count(pointer.cast()) };
        RawWaker::new(pointer, &COUNTED_VTABLE)
    }

    unsafe fn wake_counted(pointer: *const ()) {
        // SAFETY: wake consumes the owned reference represented by this waker.
        let state = unsafe { Arc::<CountedRawWaker>::from_raw(pointer.cast()) };
        state.wakes.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn wake_counted_by_ref(pointer: *const ()) {
        // SAFETY: wake_by_ref borrows an Arc reference retained by the waker.
        let state = unsafe { &*pointer.cast::<CountedRawWaker>() };
        state.wakes.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn drop_counted(pointer: *const ()) {
        // SAFETY: drop consumes the owned reference represented by this waker.
        drop(unsafe { Arc::<CountedRawWaker>::from_raw(pointer.cast()) });
    }

    static COUNTED_VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_counted, wake_counted, wake_counted_by_ref, drop_counted);

    fn counted_waker() -> (Waker, Arc<CountedRawWaker>) {
        let state = Arc::new(CountedRawWaker {
            clones: AtomicUsize::new(0),
            wakes: AtomicUsize::new(0),
        });
        let pointer = Arc::into_raw(state.clone()).cast();
        // SAFETY: The vtable owns and releases the Arc reference above.
        (
            unsafe { Waker::from_raw(RawWaker::new(pointer, &COUNTED_VTABLE)) },
            state,
        )
    }

    struct CloneBarrierWaker {
        enabled: AtomicBool,
        entered: Barrier,
        continue_clone: Barrier,
        wakes: AtomicUsize,
    }

    unsafe fn clone_with_barrier(pointer: *const ()) -> RawWaker {
        // SAFETY: The raw waker owns an Arc reference managed by this vtable.
        let state = unsafe { &*pointer.cast::<CloneBarrierWaker>() };
        if state.enabled.load(Ordering::SeqCst) {
            state.entered.wait();
            state.continue_clone.wait();
        }
        // SAFETY: The cloned RawWaker owns one new strong reference.
        unsafe { Arc::<CloneBarrierWaker>::increment_strong_count(pointer.cast()) };
        RawWaker::new(pointer, &CLONE_BARRIER_VTABLE)
    }

    unsafe fn wake_with_barrier(pointer: *const ()) {
        // SAFETY: wake consumes the Arc reference owned by the RawWaker.
        let state = unsafe { Arc::<CloneBarrierWaker>::from_raw(pointer.cast()) };
        state.wakes.fetch_add(1, Ordering::SeqCst);
    }

    unsafe fn drop_with_barrier(pointer: *const ()) {
        // SAFETY: drop consumes the Arc reference owned by the RawWaker.
        drop(unsafe { Arc::<CloneBarrierWaker>::from_raw(pointer.cast()) });
    }

    unsafe fn wake_by_ref_with_barrier(pointer: *const ()) {
        // SAFETY: wake_by_ref only borrows the live Arc reference.
        let state = unsafe { &*pointer.cast::<CloneBarrierWaker>() };
        state.wakes.fetch_add(1, Ordering::SeqCst);
    }

    static CLONE_BARRIER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_with_barrier,
        wake_with_barrier,
        wake_by_ref_with_barrier,
        drop_with_barrier,
    );

    fn clone_barrier_waker() -> (Waker, Arc<CloneBarrierWaker>) {
        let state = Arc::new(CloneBarrierWaker {
            enabled: AtomicBool::new(false),
            entered: Barrier::new(2),
            continue_clone: Barrier::new(2),
            wakes: AtomicUsize::new(0),
        });
        let pointer = Arc::into_raw(state.clone()).cast();
        // SAFETY: The vtable owns and releases the Arc reference above.
        (
            unsafe { Waker::from_raw(RawWaker::new(pointer, &CLONE_BARRIER_VTABLE)) },
            state,
        )
    }

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

    #[derive(Default)]
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("intentional waker panic");
        }
    }

    struct DropPanicPayload;

    impl Drop for DropPanicPayload {
        fn drop(&mut self) {
            panic!("payload drop panic");
        }
    }

    struct PayloadPanicWake;

    impl Wake for PayloadPanicWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(DropPanicPayload);
        }
    }

    fn assert_panicking_waker_is_isolated(panicking_waker: Waker) {
        let signal = AsyncSignal::default();
        let counter = Arc::new(CountWake::default());
        let mut guards = Vec::new();
        for _ in 0..32 {
            guards.push(signal.register(&Waker::from(counter.clone())));
        }
        guards.push(signal.register(&panicking_waker));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            signal.notify_all();
        }));
        assert!(result.is_ok());
        assert_eq!(32, counter.0.load(Ordering::Relaxed));
        assert_eq!(0, signal.waiter_count());
        signal.notify_all();
        assert_eq!(32, counter.0.load(Ordering::Relaxed));
        drop(guards);
    }

    #[test]
    fn test_panicking_waker_does_not_strand_other_waiters() {
        assert_panicking_waker_is_isolated(Waker::from(Arc::new(PanicWake)));
    }

    #[test]
    fn test_panicking_payload_drop_does_not_strand_other_waiters() {
        assert_panicking_waker_is_isolated(Waker::from(Arc::new(PayloadPanicWake)));
    }

    #[test]
    fn test_registered_wakers_are_removed_and_notified_outside_the_registry_lock() {
        let signal = AsyncSignal::default();
        let counter = Arc::new(CountWake::default());
        let waker = Waker::from(counter.clone());
        let registration = signal.register(&waker);
        assert_eq!(1, signal.waiter_count());
        signal.notify_all();
        assert_eq!(1, counter.0.load(Ordering::Relaxed));
        drop(registration);
        assert_eq!(0, signal.waiter_count());
    }

    #[test]
    fn test_register_or_update_reuses_same_waker_registration_without_clone() {
        let signal = AsyncSignal::default();
        let (waker, counts) = counted_waker();
        let mut waiter = None;
        signal.register_or_update(&mut waiter, &waker);
        assert_eq!(1, counts.clones.load(Ordering::Relaxed));
        signal.register_or_update(&mut waiter, &waker);
        assert_eq!(1, counts.clones.load(Ordering::Relaxed));
        assert_eq!(1, signal.waiter_count());
        drop(waiter);
        assert_eq!(0, signal.waiter_count());
    }

    #[test]
    fn test_register_or_update_replaces_waker_and_restores_drained_registration() {
        let signal = AsyncSignal::default();
        let (first, first_counts) = counted_waker();
        let (second, second_counts) = counted_waker();
        let mut waiter = None;
        signal.register_or_update(&mut waiter, &first);
        signal.register_or_update(&mut waiter, &second);
        assert_eq!(1, signal.waiter_count());
        signal.notify_all();
        assert_eq!(0, first_counts.wakes.load(Ordering::Relaxed));
        assert_eq!(1, second_counts.wakes.load(Ordering::Relaxed));
        assert_eq!(0, signal.waiter_count());

        signal.register_or_update(&mut waiter, &second);
        assert_eq!(1, signal.waiter_count());
        signal.notify_all();
        assert_eq!(2, second_counts.wakes.load(Ordering::Relaxed));
        drop(waiter);
        assert_eq!(0, signal.waiter_count());
    }

    #[test]
    fn test_register_or_update_keeps_multiple_waiters_isolated() {
        let signal = AsyncSignal::default();
        let (first, first_counts) = counted_waker();
        let (second, second_counts) = counted_waker();
        let mut first_waiter = None;
        let mut second_waiter = None;
        signal.register_or_update(&mut first_waiter, &first);
        signal.register_or_update(&mut second_waiter, &second);
        signal.register_or_update(&mut first_waiter, &second);
        assert_eq!(2, signal.waiter_count());
        signal.notify_all();
        assert_eq!(0, first_counts.wakes.load(Ordering::Relaxed));
        assert_eq!(2, second_counts.wakes.load(Ordering::Relaxed));
        drop((first_waiter, second_waiter));
        assert_eq!(0, signal.waiter_count());
    }

    #[test]
    fn test_register_or_update_clones_and_drops_wakers_outside_registry_lock() {
        let signal = Arc::new(AsyncSignal::default());
        let probe = Arc::new(CloneDropProbe {
            signal: Arc::downgrade(&signal),
        });
        let pointer = Arc::into_raw(probe).cast();
        // SAFETY: The probe vtable maintains one Arc reference per raw waker.
        let waker = unsafe { Waker::from_raw(RawWaker::new(pointer, &PROBE_VTABLE)) };
        let original = Waker::from(Arc::new(CountWake::default()));
        let mut waiter = None;
        signal.register_or_update(&mut waiter, &original);
        signal.register_or_update(&mut waiter, &waker);
        drop(waiter);
        drop(waker);
    }

    #[test]
    fn test_notify_between_waker_clone_and_insert_does_not_lose_registration() {
        let signal = Arc::new(AsyncSignal::default());
        let original = Waker::from(Arc::new(CountWake::default()));
        let (replacement, replacement_state) = clone_barrier_waker();
        let mut waiter = None;
        signal.register_or_update(&mut waiter, &original);
        replacement_state.enabled.store(true, Ordering::SeqCst);
        let signal_for_update = Arc::clone(&signal);
        let (sender, receiver) = std::sync::mpsc::channel();
        let updater = thread::spawn(move || {
            signal_for_update.register_or_update(&mut waiter, &replacement);
            sender.send(waiter).expect("return updated registration");
        });

        replacement_state.entered.wait();
        signal.notify_all();
        replacement_state.continue_clone.wait();
        let waiter = receiver.recv().expect("updated waiter");
        updater.join().expect("update thread");
        assert_eq!(1, signal.waiter_count());
        signal.notify_all();
        assert_eq!(1, replacement_state.wakes.load(Ordering::SeqCst));
        drop(waiter);
        assert_eq!(0, signal.waiter_count());
    }

    #[test]
    fn test_dropping_a_pending_registration_cleans_it_up() {
        let signal = AsyncSignal::default();
        let counter = Arc::new(CountWake::default());
        drop(signal.register(&Waker::from(counter)));
        assert_eq!(0, signal.waiter_count());
    }

    #[test]
    fn test_registered_waker_supports_wake_by_reference() {
        let signal = AsyncSignal::default();
        let counter = Arc::new(CountWake::default());
        let waker = Waker::from(counter.clone());
        let _registration = signal.register(&waker);
        waker.wake_by_ref();
        assert_eq!(1, counter.0.load(Ordering::Relaxed));
    }

    #[test]
    fn test_waker_clone_and_drop_are_outside_the_registry_lock() {
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
