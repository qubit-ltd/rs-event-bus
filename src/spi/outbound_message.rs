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
///
/// # Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use std::time::SystemTime;
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::spi::{OutboundMessage, TopicAddress, TransportPayload};
///
/// let message = OutboundMessage::new(
///     TopicAddress::new("orders.created").unwrap(),
///     EventId::new("order-42").unwrap(),
///     SystemTime::UNIX_EPOCH,
///     BTreeMap::new(),
///     None,
///     None,
///     TransportPayload::Native(std::sync::Arc::new(String::from("order-42"))),
/// );
/// assert_eq!(message.topic().as_str(), "orders.created");
/// ```
#[must_use]
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
    ///
    /// # Parameters
    /// - `topic`: destination address.
    /// - `id`: stable event identifier.
    /// - `timestamp`: event creation time.
    /// - `headers`: portable event headers.
    /// - `ordering_key`: optional per-key ordering value.
    /// - `delay`: optional provider delay request.
    /// - `payload`: native or encoded transport representation.
    ///
    /// # Returns
    /// A type-erased message retaining the supplied event data.
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
    ///
    /// # Returns
    /// The validated destination address.
    #[inline]
    pub fn topic(&self) -> &TopicAddress {
        &self.topic
    }
    /// Returns the event identifier.
    ///
    /// # Returns
    /// The stable event identifier.
    #[inline]
    pub fn id(&self) -> &EventId {
        &self.id
    }
    /// Returns the event creation timestamp.
    ///
    /// # Returns
    /// The event creation time.
    #[must_use]
    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
    /// Returns event headers.
    ///
    /// # Returns
    /// The portable event header map.
    #[must_use]
    #[inline]
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    /// Returns the optional ordering key.
    ///
    /// # Returns
    /// `Some` with the ordering key when configured, otherwise `None`.
    #[must_use]
    #[inline]
    pub fn ordering_key(&self) -> Option<&OrderingKey> {
        self.ordering_key.as_ref()
    }
    /// Returns the optional provider delay request.
    ///
    /// # Returns
    /// `Some` with the requested delay, otherwise `None` for immediate
    /// delivery.
    #[must_use]
    #[inline]
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }
    /// Returns the payload.
    ///
    /// # Returns
    /// The native or encoded payload representation.
    #[inline]
    pub fn payload(&self) -> &TransportPayload {
        &self.payload
    }
    /// Consumes this message and returns its payload.
    ///
    /// # Returns
    /// The owned payload, with all message metadata discarded.
    pub fn into_payload(self) -> TransportPayload {
        self.payload
    }
}
