// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! In-flight delivery and worker tracking for synchronous facade lifecycle.

#[path = "tracker/internal/mod.rs"]
mod internal;

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::Barrier;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::Duration;

pub(crate) use internal::DeliveryTrackerGuard;

use self::internal::TrackerState;
use crate::facade::WaitOutcome;
use crate::facade::internal::FiniteWait;

/// Tracks worker and received-delivery lifetimes without invoking user code.
pub(crate) struct LifecycleTracker {
    /// Active workers and in-flight topic delivery counts.
    state: Mutex<TrackerState>,
    /// Wakes callers waiting for worker or delivery counts to change.
    changed: Condvar,
    /// Synchronizes tests immediately before a finite condition-variable wait.
    #[cfg(test)]
    test_wait_barrier: Mutex<Option<Arc<Barrier>>>,
}

impl LifecycleTracker {
    /// Creates an empty tracker for one event-bus facade.
    ///
    /// # Returns
    /// A tracker with no workers or received deliveries in flight.
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(TrackerState::default()),
            changed: Condvar::new(),
            #[cfg(test)]
            test_wait_barrier: Mutex::new(None),
        }
    }

    /// Records one worker before its thread is started.
    pub(crate) fn worker_started(&self) {
        self.lock_state().active_workers += 1;
    }

    /// Records one worker exit and wakes lifecycle waiters.
    pub(crate) fn worker_finished(&self) {
        let mut state = self.lock_state();
        state.active_workers = state.active_workers.saturating_sub(1);
        self.changed.notify_all();
    }

    /// Tracks one delivery until the returned guard is dropped.
    ///
    /// # Parameters
    /// - `topic`: name used to group in-flight work for idle checks.
    ///
    /// # Returns
    /// A must-use guard that decrements this topic's counter on drop.
    #[must_use = "the guard must remain alive while the delivery is in flight"]
    pub(crate) fn track_delivery(&self, topic: &str) -> DeliveryTrackerGuard<'_> {
        *self.lock_state().in_flight_by_topic.entry(topic.into()).or_default() += 1;
        DeliveryTrackerGuard::new(self, topic.into())
    }

    /// Waits until the selected topic has no tracked work, or the deadline
    /// expires.
    ///
    /// # Parameters
    /// - `topic`: topic whose received deliveries are checked.
    /// - `timeout`: optional maximum wait duration; `None` waits indefinitely.
    ///
    /// # Returns
    /// Idle when the topic has no tracked work, or TimedOut when its deadline
    /// expires. Completion takes priority when the timeout is zero.
    pub(crate) fn wait_for_idle(&self, topic: &str, timeout: Option<Duration>) -> WaitOutcome {
        let budget = timeout.map(FiniteWait::new);
        let mut state = self.lock_state();
        loop {
            if state.in_flight_by_topic.get(topic).copied().unwrap_or_default() == 0 {
                return WaitOutcome::Idle;
            }
            if let Some(budget) = &budget {
                let Some(slice) = budget.remaining() else {
                    return WaitOutcome::TimedOut;
                };
                #[cfg(test)]
                self.wait_test_barrier();
                let (next_state, _) = self
                    .changed
                    .wait_timeout(state, slice)
                    .unwrap_or_else(PoisonError::into_inner);
                state = next_state;
            } else {
                state = self.wait(state);
            }
        }
    }

    /// Waits until all subscription workers stop, returning false on timeout.
    ///
    /// # Parameters
    /// - `timeout`: optional maximum wait duration; `None` waits indefinitely.
    ///
    /// # Returns
    /// True when all workers have exited, or false when the deadline expires.
    #[must_use = "check that all subscription workers exited before continuing"]
    pub(crate) fn wait_for_workers(&self, timeout: Option<Duration>) -> bool {
        let budget = timeout.map(FiniteWait::new);
        let mut state = self.lock_state();
        loop {
            if state.active_workers == 0 {
                return true;
            }
            if let Some(budget) = &budget {
                let Some(slice) = budget.remaining() else {
                    return false;
                };
                #[cfg(test)]
                self.wait_test_barrier();
                let (next_state, _) = self
                    .changed
                    .wait_timeout(state, slice)
                    .unwrap_or_else(PoisonError::into_inner);
                state = next_state;
            } else {
                state = self.wait(state);
            }
        }
    }

    /// Returns true when all subscription workers have exited.
    ///
    /// # Returns
    /// True when the active worker count is zero.
    #[must_use]
    pub(crate) fn workers_are_idle(&self) -> bool {
        self.lock_state().active_workers == 0
    }

    /// Decrements one topic's in-flight count and wakes idle/shutdown waiters.
    ///
    /// # Parameters
    /// - `topic`: topic whose tracked delivery has completed.
    pub(in crate::facade) fn finish_delivery(&self, topic: &str) {
        let mut state = self.lock_state();
        if let Some(count) = state.in_flight_by_topic.get_mut(topic) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.in_flight_by_topic.remove(topic);
            }
        }
        self.changed.notify_all();
    }

    /// Locks tracker state while recovering from a poisoned internal mutex.
    ///
    /// # Returns
    /// The exclusive state guard; dropping it releases the mutex.
    fn lock_state(&self) -> MutexGuard<'_, TrackerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits on the condition variable while recovering its state after poison.
    ///
    /// # Parameters
    /// - `state`: tracker state guard to release while waiting.
    ///
    /// # Returns
    /// Tracker state reacquired after a notification or poison recovery.
    fn wait<'a>(&self, state: MutexGuard<'a, TrackerState>) -> MutexGuard<'a, TrackerState> {
        self.changed.wait(state).unwrap_or_else(PoisonError::into_inner)
    }

    /// Blocks at a test barrier while the caller still holds tracker state.
    #[cfg(test)]
    fn wait_test_barrier(&self) {
        let barrier = self
            .test_wait_barrier
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(barrier) = barrier {
            barrier.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::LifecycleTracker;
    use crate::facade::WaitOutcome;

    #[test]
    fn test_wait_for_idle_zero_timeout_checks_completion_first() {
        let tracker = LifecycleTracker::new();
        let delivery = tracker.track_delivery("orders");

        assert_eq!(
            tracker.wait_for_idle("orders", Some(Duration::ZERO)),
            WaitOutcome::TimedOut
        );

        drop(delivery);
        assert_eq!(tracker.wait_for_idle("orders", Some(Duration::ZERO)), WaitOutcome::Idle);
    }

    #[test]
    fn test_wait_for_workers_zero_timeout_checks_completion_first() {
        let tracker = LifecycleTracker::new();
        tracker.worker_started();

        assert!(!tracker.wait_for_workers(Some(Duration::ZERO)));

        tracker.worker_finished();
        assert!(tracker.wait_for_workers(Some(Duration::ZERO)));
    }

    #[test]
    fn test_wait_for_idle_returns_after_delivery_completion_notification() {
        let tracker = Arc::new(LifecycleTracker::new());
        let delivery = tracker.track_delivery("orders");
        let barrier = Arc::new(Barrier::new(2));
        *tracker
            .test_wait_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&barrier));
        let (result_tx, result_rx) = mpsc::channel();
        let waiter_tracker = Arc::clone(&tracker);
        let waiter = thread::spawn(move || {
            let result = waiter_tracker.wait_for_idle("orders", Some(Duration::MAX));
            result_tx.send(result).expect("main thread is receiving");
        });

        barrier.wait();
        thread::scope(|scope| {
            let notifier = scope.spawn(move || drop(delivery));
            assert_eq!(
                result_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("waiter completes after delivery notification"),
                WaitOutcome::Idle
            );
            notifier.join().expect("delivery notifier completes");
        });
        waiter.join().expect("waiter thread completes");
    }

    #[test]
    fn test_wait_for_workers_returns_after_worker_completion_notification() {
        let tracker = Arc::new(LifecycleTracker::new());
        tracker.worker_started();
        let barrier = Arc::new(Barrier::new(2));
        *tracker
            .test_wait_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&barrier));
        let (result_tx, result_rx) = mpsc::channel();
        let waiter_tracker = Arc::clone(&tracker);
        let waiter = thread::spawn(move || {
            let result = waiter_tracker.wait_for_workers(Some(Duration::MAX));
            result_tx.send(result).expect("main thread is receiving");
        });

        barrier.wait();
        let notifier_tracker = Arc::clone(&tracker);
        let notifier = thread::spawn(move || notifier_tracker.worker_finished());

        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("waiter completes after worker notification")
        );
        notifier.join().expect("worker notifier completes");
        waiter.join().expect("waiter thread completes");
    }
}
