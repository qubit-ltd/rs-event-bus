// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous requests routed to the provider receiver's owner thread.

use std::sync::Weak;
use std::sync::mpsc;

use qubit_id::Id;

use super::coordinator_message::CoordinatorMessage;
use crate::model::EventId;
use crate::model::SubscriberId;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementToken;

/// Routes handler settlement requests to the receiver-owning worker.
#[derive(Clone)]
pub(in crate::facade) struct OwnerSettlementRouter {
    /// Lease whose immutable disposition is routed to the owner.
    pub(in crate::facade) lease_id: u64,
    /// Bus state used to wake the owner and report disconnection.
    pub(in crate::facade) inner: Weak<super::EventBusInner>,
    /// Message channel to the receiver-owning worker.
    pub(in crate::facade) sender: mpsc::Sender<CoordinatorMessage>,
}

impl OwnerSettlementRouter {
    /// Marks unresolved recovery without submitting a fake settlement
    /// disposition.
    ///
    /// # Parameters
    /// - `subscription_id`: owner to wake for unresolved provider recovery.
    pub(in crate::facade) fn abandon(&self, subscription_id: Id) {
        let _ = self.sender.send(CoordinatorMessage::Abandoned(self.lease_id));
        if let Some(inner) = self.inner.upgrade() {
            inner.scheduler.notify(subscription_id);
        }
    }

    /// Sends a settlement without blocking the handler thread.
    /// A disconnected owner is diagnosed and provider close owns recovery.
    ///
    /// # Parameters
    /// - `token`: provider token to settle, or `None` when unavailable.
    /// - `disposition`: terminal action requested for the event.
    /// - `event_id`: event identity used for tracking.
    /// - `topic`: destination used for diagnostics.
    /// - `subscription_id`: receiver identity bound to the token.
    /// - `subscriber_id`: logical subscriber identity.
    pub(in crate::facade) fn settle(
        &self,
        token: Option<SettlementToken>,
        disposition: DeliveryDisposition,
        event_id: EventId,
        topic: &str,
        subscription_id: Id,
        subscriber_id: &SubscriberId,
    ) {
        let result = self.sender.send(CoordinatorMessage::Settlement {
            lease_id: self.lease_id,
            token,
            disposition,
            event_id,
            topic: topic.into(),
            subscription_id,
            subscriber_id: subscriber_id.clone(),
        });
        if let Some(inner) = self.inner.upgrade() {
            if result.is_err() {
                inner.emit_internal(
                    "settlement_owner_disconnected",
                    "receiver owner no longer accepts settlement".into(),
                );
            }
            inner.scheduler.notify(subscription_id);
        }
    }
}
