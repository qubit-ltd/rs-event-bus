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
use crate::facade::event_bus::Id;
use crate::facade::event_bus::mpsc;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementToken;

pub(in crate::facade) enum CoordinatorMessage {
    Settlement {
        token: Option<SettlementToken>,
        disposition: DeliveryDisposition,
        event_id: EventId,
        topic: Box<str>,
        subscription_id: Id,
        subscriber_id: SubscriberId,
        settled: Option<mpsc::SyncSender<()>>,
    },
    TaskFinished(usize),
}
