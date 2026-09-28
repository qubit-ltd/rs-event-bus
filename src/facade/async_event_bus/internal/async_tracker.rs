// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared operation and delivery counters for the asynchronous facade.

use std::sync::Arc;
use std::sync::Mutex;

use super::AsyncCloseGuard;
use super::AsyncDeliveryGuard;
use super::AsyncSignal;
use super::tracker_state::TrackerState;

/// Counts active facade operations and received deliveries by topic.
#[derive(Default)]
pub(in crate::facade) struct AsyncTracker {
    /// Mutable counts protected as one consistent snapshot.
    pub(super) state: Mutex<TrackerState>,
    /// Wakes waiters after tracked activity changes.
    pub(in crate::facade) signal: AsyncSignal,
}

impl AsyncTracker {
    /// Starts tracking a close operation until its guard is dropped.
    pub(in crate::facade) fn close_started(self: &Arc<Self>) -> AsyncCloseGuard {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_closes += 1;
        AsyncCloseGuard::new(self.clone())
    }

    /// Increments the number of active subscription runners.
    pub(in crate::facade) fn runner_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_runners += 1;
    }

    /// Decrements the runner count and wakes quiescence waiters.
    pub(in crate::facade) fn runner_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_runners = state.active_runners.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }

    /// Increments the number of active publishes.
    pub(in crate::facade) fn publish_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_publishes += 1;
    }

    /// Decrements the publish count and wakes quiescence waiters.
    pub(in crate::facade) fn publish_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_publishes = state.active_publishes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }

    /// Increments the number of active subscribes.
    pub(in crate::facade) fn subscribe_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_subscribes += 1;
    }

    /// Decrements the subscribe count and wakes quiescence waiters.
    pub(in crate::facade) fn subscribe_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_subscribes = state.active_subscribes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }

    /// Decrements the close count and wakes quiescence waiters.
    pub(in crate::facade) fn close_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_closes = state.active_closes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }

    /// Tracks one received delivery until its terminal path releases the guard.
    pub(in crate::facade) fn track(self: &Arc<Self>, topic: &str) -> AsyncDeliveryGuard {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
            .entry(topic.into())
            .or_default() += 1;
        AsyncDeliveryGuard::new(self.clone(), topic.into())
    }

    /// Reports whether a topic has no received deliveries in any processing
    /// stage.
    pub(in crate::facade) fn is_idle(&self, topic: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
            .get(topic)
            .copied()
            .unwrap_or(0)
            == 0
    }

    /// Reports whether runners and facade operations have all reached
    /// quiescence.
    pub(in crate::facade) fn runners_stopped(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_runners == 0
            && state.active_publishes == 0
            && state.active_subscribes == 0
            && state.active_closes == 0
    }
}
