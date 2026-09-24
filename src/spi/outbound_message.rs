// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Message sent from facade to provider SPI.

use std::time::Duration;
use std::time::SystemTime;

use super::OrderingKey;
use super::TopicAddress;
use super::TransportPayload;
use crate::model::EventId;
use crate::model::Headers;

/// Type-erased publication request delivered to a backend.
pub struct OutboundMessage {
    topic: TopicAddress,
    id: EventId,
    timestamp: SystemTime,
    headers: Headers,
    ordering_key: Option<OrderingKey>,
    delay: Option<Duration>,
    payload: TransportPayload,
}

impl OutboundMessage {
    /// Creates an outbound transport message.
    pub fn new(
        topic: TopicAddress,
        id: EventId,
        timestamp: SystemTime,
        headers: Headers,
        ordering_key: Option<OrderingKey>,
        delay: Option<Duration>,
        payload: TransportPayload,
    ) -> Self {
        Self {
            topic,
            id,
            timestamp,
            headers,
            ordering_key,
            delay,
            payload,
        }
    }
    /// Returns the destination topic.
    pub fn topic(&self) -> &TopicAddress {
        &self.topic
    }
    /// Returns the event identifier.
    pub fn id(&self) -> &EventId {
        &self.id
    }
    /// Returns the event creation timestamp.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
    /// Returns event headers.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    /// Returns the optional ordering key.
    pub fn ordering_key(&self) -> Option<&OrderingKey> {
        self.ordering_key.as_ref()
    }
    /// Returns the optional provider delay request.
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }
    /// Returns the payload.
    pub fn payload(&self) -> &TransportPayload {
        &self.payload
    }
    /// Consumes this message and returns its payload.
    pub fn into_payload(self) -> TransportPayload {
        self.payload
    }
}
