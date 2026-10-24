// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation guard for a caller-driven subscription run.

use std::sync::atomic::Ordering;

use super::AsyncSubscriptionControl;
use crate::facade::AsyncSubscriptionRunState;

/// Returns a live runner to `Paused` when its future is dropped.
///
/// # Type Parameters
/// - `'a`: lifetime of the control whose run state this guard protects.
/// - `T`: payload type handled by the subscription.
#[must_use = "the run state guard must stay alive while the runner is active"]
pub(in crate::facade::async_subscription) struct RunStateGuard<'a, T: 'static> {
    /// Shared lifecycle state restored when the caller-driven run is dropped.
    control: &'a AsyncSubscriptionControl<T>,
}

impl<'a, T: 'static> RunStateGuard<'a, T> {
    /// Creates a guard for a control already transitioned to `Running`.
    ///
    /// # Parameters
    /// - `control`: shared lifecycle state protected by this guard.
    ///
    /// # Returns
    /// A guard that restores the control to `Paused` when dropped while still
    /// running.
    #[inline]
    pub(in crate::facade::async_subscription) fn new(control: &'a AsyncSubscriptionControl<T>) -> Self {
        Self { control }
    }
}

impl<T: 'static> Drop for RunStateGuard<'_, T> {
    /// Pauses the run if it is still marked as running.
    fn drop(&mut self) {
        let _ = self.control.run_state.compare_exchange(
            AsyncSubscriptionRunState::Running as u8,
            AsyncSubscriptionRunState::Paused as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}
