// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! In-flight delivery and worker tracking for synchronous facade lifecycle.

use std::collections::HashMap;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::facade::WaitOutcome;

/// Tracks worker and received-delivery lifetimes without invoking user code.
pub(crate) struct LifecycleTracker {
    state: Mutex<TrackerState>,
    changed: Condvar,
}

#[derive(Default)]
struct TrackerState {
    active_workers: usize,
    in_flight_by_topic: HashMap<Box<str>, usize>,
}

impl LifecycleTracker {
    /// Creates an empty tracker for one event-bus facade.
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
    pub(crate) fn track_delivery(&self, topic: &str) -> DeliveryTrackerGuard<'_> {
        *self.lock_state().in_flight_by_topic.entry(topic.into()).or_default() += 1;
        DeliveryTrackerGuard {
            tracker: self,
            topic: topic.into(),
        }
    }

    /// Waits until the selected topic has no tracked work, or the deadline
    /// expires.
    pub(crate) fn wait_for_idle(&self, topic: &str, timeout: Option<Duration>) -> WaitOutcome {
        let deadline = timeout.and_then(|value| Instant::now().checked_add(value));
        let mut state = self.lock_state();
        loop {
            if state.in_flight_by_topic.get(topic).copied().unwrap_or_default() == 0 {
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
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next_state;
            if result.timed_out() && state.in_flight_by_topic.get(topic).copied().unwrap_or_default() != 0 {
                return WaitOutcome::TimedOut;
            }
        }
    }

    /// Waits until all subscription workers stop, returning false on timeout.
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
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next_state;
            if result.timed_out() && state.active_workers != 0 {
                return false;
            }
        }
        true
    }

    /// Returns true when all subscription workers have exited.
    pub(crate) fn workers_are_idle(&self) -> bool {
        self.lock_state().active_workers == 0
    }

    /// Locks tracker state while recovering from a poisoned internal mutex.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, TrackerState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Waits on the condition variable while recovering its state after poison.
    fn wait<'a>(&self, state: std::sync::MutexGuard<'a, TrackerState>) -> std::sync::MutexGuard<'a, TrackerState> {
        self.changed
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Releases one topic's in-flight count on every worker exit path.
pub(crate) struct DeliveryTrackerGuard<'a> {
    tracker: &'a LifecycleTracker,
    topic: Box<str>,
}

impl Drop for DeliveryTrackerGuard<'_> {
    /// Decrements the topic count and wakes idle/shutdown waiters.
    fn drop(&mut self) {
        let mut state = self.tracker.lock_state();
        if let Some(count) = state.in_flight_by_topic.get_mut(self.topic.as_ref()) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.in_flight_by_topic.remove(self.topic.as_ref());
            }
        }
        self.tracker.changed.notify_all();
    }
}
