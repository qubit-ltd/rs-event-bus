// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Messages sent to the thread that owns a synchronous provider receiver.

use std::sync::mpsc;

use qubit_id::Id;

use crate::model::EventId;
use crate::model::SubscriberId;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementToken;

/// Work routed to the receiver-owning subscription coordinator.
pub(in crate::facade) enum CoordinatorMessage {
    /// Settlement request to be executed by the receiver owner.
    Settlement {
        /// Provider-issued token, if this delivery can be settled.
        token: Option<SettlementToken>,
        /// Terminal action requested by delivery policy.
        disposition: DeliveryDisposition,
        /// Stable event identity included in diagnostics.
        event_id: EventId,
        /// Event destination included in diagnostics.
        topic: Box<str>,
        /// Receiver that issued the token.
        subscription_id: Id,
        /// Logical subscriber used in provider error context.
        subscriber_id: SubscriberId,
        /// Optional acknowledgement to unblock the handler task.
        settled: Option<mpsc::SyncSender<()>>,
    },
    /// Notification that one scheduled handler task has completed.
    TaskFinished(
        /// Scheduler task ID removed from the active worker set.
        usize,
    ),
}
