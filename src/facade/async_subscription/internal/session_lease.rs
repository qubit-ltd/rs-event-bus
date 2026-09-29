// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::facade::async_subscription::AsyncSession;
use crate::facade::async_subscription::AsyncSubscriptionControl;

/// Exclusive lease that returns the session to its control when dropped.
pub(in crate::facade::async_subscription) struct SessionLease<'a, T: 'static> {
    /// Coordinator that regains the session when this lease is dropped.
    pub(super) control: &'a AsyncSubscriptionControl<T>,
    /// Session exclusively held by this lease.
    pub(in crate::facade::async_subscription) session: Option<AsyncSession<T>>,
}

impl<T: 'static> Drop for SessionLease<'_, T> {
    /// Returns the session to its control and wakes the next waiter.
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let mut slot = self
                .control
                .slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if slot.disposed {
                drop(slot);
                drop(session);
                self.control.available.notify();
                return;
            }
            slot.session = Some(session);
            slot.active = false;
            drop(slot);
            self.control.available.notify();
        }
    }
}
