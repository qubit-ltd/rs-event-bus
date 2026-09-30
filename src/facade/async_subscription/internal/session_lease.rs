// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal asynchronous subscription state.

use std::sync::PoisonError;

use super::AsyncSession;
use super::AsyncSubscriptionControl;

/// Temporarily owns the session while a runner or shutdown caller uses it.
///
/// # Type Parameters
/// - `'a`: lifetime of the subscription control borrowed by this lease.
/// - `T`: payload type retained by the session.
pub(in crate::facade::async_subscription) struct SessionLease<'a, T: 'static> {
    /// Coordinator that regains the session when this lease is dropped.
    pub(in crate::facade::async_subscription) control: &'a AsyncSubscriptionControl<T>,
    /// Session exclusively held by this lease.
    pub(in crate::facade::async_subscription) session: Option<AsyncSession<T>>,
}

impl<T: 'static> Drop for SessionLease<'_, T> {
    /// Returns the session to its control and wakes the next waiter.
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            session.inner.scheduler.set_dispatch_active(session.id, false);
            session.inner.scheduler.cancel_receive(session.id);
            session.inner.notify_scheduler();
            let mut slot = self.control.slot.lock().unwrap_or_else(PoisonError::into_inner);
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
