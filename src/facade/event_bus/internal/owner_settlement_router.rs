// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous requests routed to the provider receiver's owner thread.

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
    /// Message channel to the receiver-owning worker.
    pub(in crate::facade) sender: mpsc::Sender<CoordinatorMessage>,
}

impl OwnerSettlementRouter {
    /// Sends a settlement to the receiver owner and waits for its response.
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
        let (settled, wait) = mpsc::sync_channel(0);
        if self
            .sender
            .send(CoordinatorMessage::Settlement {
                token,
                disposition,
                event_id,
                topic: topic.into(),
                subscription_id,
                subscriber_id: subscriber_id.clone(),
                settled: Some(settled),
            })
            .is_ok()
        {
            let _ = wait.recv();
        }
    }
}
