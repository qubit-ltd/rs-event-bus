// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal sync facade state owner.

use crate::EventId;
use crate::SubscriberId;
use crate::facade::event_bus::CoordinatorMessage;
use crate::facade::event_bus::Id;
use crate::facade::event_bus::mpsc;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementToken;

#[derive(Clone)]
pub(in crate::facade) struct OwnerSettlementRouter {
    pub(in crate::facade) sender: mpsc::Sender<CoordinatorMessage>,
}

impl OwnerSettlementRouter {
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
