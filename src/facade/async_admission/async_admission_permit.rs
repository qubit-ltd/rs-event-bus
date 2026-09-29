// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII ownership of one asynchronous delivery admission slot.

use std::sync::Arc;

use super::AsyncAdmission;

/// Holds one bus-wide slot through delivery settlement or explicit abandonment.
#[must_use]
pub(in crate::facade) struct AsyncAdmissionPermit {
    /// Gate whose in-flight count this permit owns.
    admission: Arc<AsyncAdmission>,
}

impl AsyncAdmissionPermit {
    /// Creates a permit after the future increments the in-flight count.
    ///
    /// # Parameters
    /// - `admission`: gate whose in-flight counter already includes this
    ///   permit.
    ///
    /// # Returns
    /// An RAII guard that releases one slot when dropped.
    pub(super) fn new(admission: Arc<AsyncAdmission>) -> Self {
        Self { admission }
    }
}

impl Drop for AsyncAdmissionPermit {
    /// Releases the in-flight slot and wakes the next queued waiter.
    fn drop(&mut self) {
        let wakers = {
            let mut state = self
                .admission
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.in_flight = state.in_flight.saturating_sub(1);
            state
                .waiters
                .front()
                .map(|(_, waker)| waker.clone())
                .into_iter()
                .collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}
