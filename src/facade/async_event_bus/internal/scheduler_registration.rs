// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Rollback of subscription capacity when provider subscribe fails or is
//! cancelled.

use std::sync::Arc;

use qubit_id::Id;

use super::AsyncEventBusInner;

/// Owns an unpublished scheduler registration until the session takes over.
#[must_use = "dropping this registration rolls back scheduler capacity"]
pub(in crate::facade::async_event_bus) struct SchedulerRegistration {
    /// Bus whose finite subscription capacity is reserved.
    pub(in crate::facade::async_event_bus) inner: Arc<AsyncEventBusInner>,
    /// Registered subscription identity.
    pub(in crate::facade::async_event_bus) id: Id,
    /// True once the session owns eventual unregister responsibility.
    pub(in crate::facade::async_event_bus) committed: bool,
}
impl Drop for SchedulerRegistration {
    /// Rolls back unpublished capacity after subscribe failure or cancellation.
    fn drop(&mut self) {
        if !self.committed {
            self.inner.scheduler.stop_subscription(self.id);
            let _ = self.inner.scheduler.unregister(self.id);
            self.inner.notify_scheduler();
        }
    }
}
