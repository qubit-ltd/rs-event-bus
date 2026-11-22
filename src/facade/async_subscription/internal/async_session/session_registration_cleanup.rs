// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Scheduler registration cleanup for an asynchronous subscription session.

use qubit_id::Id;

use crate::facade::async_event_bus::AsyncEventBusInner;

/// Unregisters a session after its locally owned delivery leases are released.
pub(super) struct SessionRegistrationCleanup<'a> {
    /// Bus whose scheduler owns the session registration.
    inner: &'a AsyncEventBusInner,
    /// Registration identity to remove when the guard is dropped.
    id: Id,
}

impl SessionRegistrationCleanup<'_> {
    /// Creates a cleanup guard for one registered asynchronous session.
    ///
    /// # Parameters
    /// - `inner`: bus that owns the scheduler registration.
    /// - `id`: identity of the registered session.
    ///
    /// # Returns
    /// A guard that unregisters the session and wakes scheduler waiters.
    pub(super) fn new(inner: &AsyncEventBusInner, id: Id) -> SessionRegistrationCleanup<'_> {
        SessionRegistrationCleanup { inner, id }
    }
}

impl Drop for SessionRegistrationCleanup<'_> {
    /// Removes the registration and notifies scheduler waiters.
    fn drop(&mut self) {
        let _ = self.inner.scheduler.unregister(self.id);
        self.inner.notify_scheduler();
    }
}
