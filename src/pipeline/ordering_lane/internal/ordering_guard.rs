// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Guard that releases a synchronous ordering lane when dropped.

use std::sync::Arc;
use std::sync::PoisonError;

use super::sync_lane::SyncLane;

/// Holds a sync lane across the complete handler/retry/settlement operation.
///
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
#[cfg(test)]
#[must_use = "dropping the guard releases the ordering lane"]
pub(crate) struct OrderingGuard<T> {
    /// Lane held until this guard is dropped.
    pub(super) lane: Arc<SyncLane<T>>,
    /// Ordered value owned by this guard.
    pub(super) value: Option<T>,
}
#[cfg(test)]
impl<T> OrderingGuard<T> {
    /// Borrows the ordered item while retaining lane ownership.
    ///
    /// # Returns
    /// The ordered value retained by this guard.
    ///
    /// # Panics
    /// Panics if the lane guard was created without its queued value.
    #[must_use]
    #[inline]
    pub(crate) fn value(&self) -> &T {
        self.value.as_ref().expect("lane guard always owns its value")
    }
}
#[cfg(test)]
impl<T> Drop for OrderingGuard<T> {
    /// Releases lane ownership and wakes the next ticket.
    fn drop(&mut self) {
        let mut state = self.lane.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.active = false;
        self.lane.changed.notify_all();
    }
}
