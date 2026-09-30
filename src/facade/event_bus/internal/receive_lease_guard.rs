// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exception-safe ownership of a claimed scheduling lease.

use std::sync::Arc;

use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;

/// Transfers directly from the receive stack into the owned delivery map.
pub(in crate::facade) struct ReceiveLeaseGuard {
    /// Scheduler retaining ownership metadata until this guard is released.
    scheduler: Arc<SyncDeliveryScheduler>,
    /// Claimed receive credit to return exactly once through this guard.
    lease: u64,
}

impl ReceiveLeaseGuard {
    /// Establishes ownership immediately after claiming receive admission.
    ///
    /// # Parameters
    /// - `scheduler`: owner of scheduling metadata.
    /// - `lease`: newly claimed receive credit.
    ///
    /// # Returns
    /// A guard transferable into the received delivery's lifetime.
    #[must_use = "the guard must own the claimed lease until delivery ownership ends"]
    pub(in crate::facade) fn new(scheduler: Arc<SyncDeliveryScheduler>, lease: u64) -> Self {
        Self { scheduler, lease }
    }
}

impl Drop for ReceiveLeaseGuard {
    /// Returns the claimed receive credit when ownership leaves this guard.
    fn drop(&mut self) {
        self.scheduler.complete(self.lease);
    }
}
