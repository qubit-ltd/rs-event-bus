// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unique persistent identity for one conformance probe.

use crate::error::EventIdGenerationError;
use crate::model::EventId;
use crate::model::SubscriberId;
use crate::spi::TopicAddress;

/// Unique topic, subscriber, and event identities for one conformance probe.
pub(in crate::spi::conformance) struct ProbeScope {
    /// Topic used by this probe.
    topic: TopicAddress,
    /// Logical subscriber identity used by this probe.
    subscriber_id: SubscriberId,
    /// Event identity used by this probe.
    event_id: EventId,
}

impl ProbeScope {
    /// Creates an independently generated topic, subscriber, and event ID.
    ///
    /// # Errors
    /// Returns the operating-system UUID generation error if an event ID
    /// cannot be generated.
    pub(in crate::spi::conformance) fn new() -> Result<Self, EventIdGenerationError> {
        let event_id = EventId::generate()?;
        let uuid = event_id.as_str();
        let topic_value = format!("spi.conformance.probe.{uuid}");
        let topic = TopicAddress::new(&topic_value).expect("generated UUID forms a valid conformance topic");
        let subscriber_id = SubscriberId::new(format!("spi-conformance-{uuid}"))
            .expect("generated UUID forms a valid conformance subscriber ID");
        Ok(Self {
            topic,
            subscriber_id,
            event_id,
        })
    }

    /// Returns this probe's topic.
    pub(in crate::spi::conformance) fn topic(&self) -> &TopicAddress {
        &self.topic
    }

    /// Returns this probe's logical subscriber identity.
    pub(in crate::spi::conformance) fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }

    /// Returns this probe's event identity.
    pub(in crate::spi::conformance) fn event_id(&self) -> &EventId {
        &self.event_id
    }
}
