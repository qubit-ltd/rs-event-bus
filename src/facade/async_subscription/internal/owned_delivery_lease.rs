// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-safe ownership of one scheduler credit.

use std::sync::Arc;
use std::sync::Weak;

use crate::facade::async_event_bus::AsyncEventBusInner;
use crate::facade::internal::DeliverySchedulerCore;

/// Returns receive, lane, and handler capacity when the actual delivery is
/// dropped.
pub(in crate::facade) struct OwnedDeliveryLease {
    /// Stable scheduler identity.
    pub(in crate::facade) id: u64,
    /// Metadata remains available during bus teardown.
    scheduler: Arc<DeliverySchedulerCore>,
    /// Weak bus avoids retaining a session ownership cycle.
    bus: Weak<AsyncEventBusInner>,
}
impl OwnedDeliveryLease {
    /// Takes responsibility for completing an already claimed reservation.
    ///
    /// # Parameters
    /// - `id`: Scheduler identity of the claimed receive credit.
    /// - `bus`: Owner supplying the scheduler and timestamp domain.
    ///
    /// # Returns
    /// A guard retaining the credit through payload, handler, and settlement
    /// stages. Creation samples the clock and records the owned age origin.
    pub(in crate::facade) fn new(id: u64, bus: &Arc<AsyncEventBusInner>) -> Self {
        bus.scheduler.record_owned_start(id, bus.timer.clock().now());
        Self {
            id,
            scheduler: bus.scheduler.clone(),
            bus: Arc::downgrade(bus),
        }
    }
}
impl Drop for OwnedDeliveryLease {
    /// Releases this credit and routes newly enabled scheduler notifications.
    fn drop(&mut self) {
        self.scheduler.complete(self.id);
        if let Some(bus) = self.bus.upgrade() {
            bus.notify_scheduler();
        }
    }
}
