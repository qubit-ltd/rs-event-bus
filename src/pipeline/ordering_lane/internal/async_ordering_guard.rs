// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal FIFO ordering-lane state.

use std::sync::Arc;
use std::sync::PoisonError;

use super::async_lane::AsyncLane;
use super::async_lane_state::front_waker;

/// Holds an async lane without blocking the executor thread.
///
/// # Type Parameters
/// - `T`: ordered value retained for the duration of the lane turn.
#[must_use = "dropping the guard releases the ordering lane"]
pub(crate) struct AsyncOrderingGuard<T> {
    /// Lane held until this guard is dropped.
    pub(super) lane: Arc<AsyncLane<T>>,
    /// Ordered value owned by this guard.
    pub(super) _value: Option<T>,
}
#[cfg(test)]
impl<T> AsyncOrderingGuard<T> {
    /// Borrows the ordered item while the lane is held.
    ///
    /// # Returns
    /// The ordered value retained by this guard.
    ///
    /// # Panics
    /// Panics if the lane guard was created without its queued value.
    #[must_use]
    #[inline]
    pub(crate) fn value(&self) -> &T {
        self._value.as_ref().expect("lane guard always owns its value")
    }
}
impl<T> Drop for AsyncOrderingGuard<T> {
    /// Releases lane ownership and wakes the next ticket.
    fn drop(&mut self) {
        let waker = {
            let mut state = self.lane.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.active = false;
            front_waker(&state)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
