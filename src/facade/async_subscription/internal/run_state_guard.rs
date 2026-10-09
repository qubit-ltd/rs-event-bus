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
pub(in crate::facade::async_subscription) struct RunStateGuard<'a, T: 'static> {
    control: &'a AsyncSubscriptionControl<T>,
}

impl<'a, T: 'static> RunStateGuard<'a, T> {
    /// Creates a guard for a control already transitioned to `Running`.
    #[inline]
    pub(in crate::facade::async_subscription) fn new(control: &'a AsyncSubscriptionControl<T>) -> Self {
        Self { control }
    }
}

impl<T: 'static> Drop for RunStateGuard<'_, T> {
    fn drop(&mut self) {
        let _ = self.control.run_state.compare_exchange(
            AsyncSubscriptionRunState::Running as u8,
            AsyncSubscriptionRunState::Paused as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}
