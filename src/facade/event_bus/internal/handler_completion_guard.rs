// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Returns execution capacity at actual job exit, including unwinding.

use std::sync::Arc;
use std::sync::mpsc::Sender;

use qubit_id::Id;

use crate::facade::event_bus::CoordinatorMessage;
use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;

/// Keeps handler completion independent from progress of the receiver thread.
#[must_use = "Keep the guard alive until the handler exits."]
pub(in crate::facade) struct HandlerCompletionGuard {
    /// Scheduler whose execution grant is held by this callback.
    scheduler: Arc<SyncDeliveryScheduler>,
    /// Receiver to notify after the callback releases its execution grant.
    subscription: Id,
    /// Owned delivery whose lane remains held until receiver settlement.
    lease: u64,
    /// Owner channel carrying payload-lifetime completion.
    sender: Sender<CoordinatorMessage>,
}

impl HandlerCompletionGuard {
    /// Creates the guard before running any application callback.
    ///
    /// # Parameters
    /// - `scheduler`: pool and scheduling metadata owner.
    /// - `subscription`: receiver identity to wake.
    /// - `lease`: granted delivery identity.
    /// - `sender`: nonblocking owner notification channel.
    ///
    /// # Returns
    /// A guard that returns execution credit when the job exits.
    #[inline]
    pub(in crate::facade) fn new(
        scheduler: Arc<SyncDeliveryScheduler>,
        subscription: Id,
        lease: u64,
        sender: Sender<CoordinatorMessage>,
    ) -> Self {
        Self {
            scheduler,
            subscription,
            lease,
            sender,
        }
    }
}

impl Drop for HandlerCompletionGuard {
    /// Returns the scheduler grant and notifies the receiver after the handler
    /// exits.
    fn drop(&mut self) {
        self.scheduler.handler_finished(self.lease);
        let _ = self
            .sender
            .send(CoordinatorMessage::HandlerFinished(self.lease));
        self.scheduler.notify(self.subscription);
    }
}
