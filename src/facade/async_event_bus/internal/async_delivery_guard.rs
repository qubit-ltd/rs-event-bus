// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII tracking for one received delivery across admission and settlement.

use std::sync::Arc;

use super::AsyncTracker;

/// Decrements the topic's in-flight count when a received delivery is terminal.
pub(in crate::facade) struct AsyncDeliveryGuard {
    /// Shared tracker whose topic count this guard owns.
    pub(super) tracker: Arc<AsyncTracker>,
    /// Topic whose in-flight count must be decremented.
    pub(super) topic: Box<str>,
}

impl AsyncDeliveryGuard {
    /// Creates a guard after the caller increments the matching topic count.
    ///
    /// # Parameters
    /// - `tracker`: tracker that owns the topic delivery count.
    /// - `topic`: topic whose in-flight count this guard releases.
    ///
    /// # Returns
    /// A guard that decrements the topic count when dropped.
    pub(super) fn new(tracker: Arc<AsyncTracker>, topic: Box<str>) -> Self {
        Self { tracker, topic }
    }
}

impl Drop for AsyncDeliveryGuard {
    /// Decrements the topic count and wakes lifecycle waiters.
    fn drop(&mut self) {
        let mut state = self
            .tracker
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = state.in_flight.get_mut(self.topic.as_ref()) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.in_flight.remove(self.topic.as_ref());
            }
        }
        drop(state);
        self.tracker.signal.notify();
    }
}
