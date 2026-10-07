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

use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

pub(crate) use internal::DeliveryTrackerGuard;

use self::internal::TrackerState;
use crate::facade::WaitOutcome;

/// Tracks worker and received-delivery lifetimes without invoking user code.
pub(crate) struct LifecycleTracker {
    /// Active workers and in-flight topic delivery counts.
    state: Mutex<TrackerState>,
    /// Wakes callers waiting for worker or delivery counts to change.
    changed: Condvar,
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
        *self
            .lock_state()
            .in_flight_by_topic
            .entry(topic.into())
            .or_default() += 1;
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
    /// expires.
    pub(crate) fn wait_for_idle(&self, topic: &str, timeout: Option<Duration>) -> WaitOutcome {
        let deadline = timeout.and_then(|value| Instant::now().checked_add(value));
        let mut state = self.lock_state();
        loop {
            if state
                .in_flight_by_topic
                .get(topic)
                .copied()
                .unwrap_or_default()
                == 0
            {
                return WaitOutcome::Idle;
            }
            let Some(deadline) = deadline else {
                state = self.wait(state);
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return WaitOutcome::TimedOut;
            }
            let (next_state, result) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner);
            state = next_state;
            if result.timed_out()
                && state
                    .in_flight_by_topic
                    .get(topic)
                    .copied()
                    .unwrap_or_default()
                    != 0
            {
                return WaitOutcome::TimedOut;
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
        let deadline = timeout.and_then(|value| Instant::now().checked_add(value));
        let mut state = self.lock_state();
        while state.active_workers != 0 {
            let Some(deadline) = deadline else {
                state = self.wait(state);
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next_state, result) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner);
            state = next_state;
            if result.timed_out() && state.active_workers != 0 {
                return false;
            }
        }
        true
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
        self.changed
            .wait(state)
            .unwrap_or_else(PoisonError::into_inner)
    }
}
