// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Message sent from facade to provider SPI.

use std::num::NonZeroUsize;
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
///
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::spi::OutboundMessage;
/// use qubit_event_bus::spi::TopicAddress;
/// use qubit_event_bus::spi::TransportPayload;
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
    /// Validated destination address used for provider routing.
    topic: TopicAddress,
    /// Stable event identity retained across publication attempts.
    id: EventId,
    /// Event creation time preserved in delivered messages.
    timestamp: SystemTime,
    /// Portable event headers forwarded to matching subscriptions.
    headers: Headers,
    /// Optional key requesting ordered delivery within its partition.
    ordering_key: Option<OrderingKey>,
    /// Requested delivery delay, or `None` for immediate eligibility.
    delay: Option<Duration>,
    /// Optional application-declared native payload weight in bytes.
    native_payload_weight_bytes: Option<NonZeroUsize>,
    /// Shared native allocation or encoded bytes to publish.
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
    /// A type-erased message retaining the supplied event data with no native
    /// payload weight declaration.
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
            native_payload_weight_bytes: None,
        }
    }
    /// Returns the destination topic.
    ///
    /// # Returns
    /// The validated destination address.
    #[must_use = "Use the returned topic."]
    #[inline]
    pub fn topic(&self) -> &TopicAddress {
        &self.topic
    }
    /// Returns the event identifier.
    ///
    /// # Returns
    /// The stable event identifier.
    #[must_use = "Use the returned id."]
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
    #[inline]
    #[must_use = "Use the returned ordering key."]
    pub fn ordering_key(&self) -> Option<&OrderingKey> {
        self.ordering_key.as_ref()
    }
    /// Returns the optional provider delay request.
    ///
    /// # Returns
    /// `Some` with the requested delay, otherwise `None` for immediate
    /// delivery.
    #[inline]
    #[must_use = "Use the returned delay."]
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }
    /// Returns the declared native payload weight in bytes.
    ///
    /// # Returns
    /// `Some` for an explicit declaration, or `None` when absent. This is
    /// application metadata and does not measure the payload's heap usage.
    #[must_use]
    #[inline]
    pub fn native_payload_weight_bytes(&self) -> Option<NonZeroUsize> {
        self.native_payload_weight_bytes
    }

    /// Attaches a declared native payload weight to this message.
    ///
    /// # Parameters
    /// - `weight`: positive application-declared weight in bytes.
    ///
    /// # Returns
    /// This message with the supplied weight replacing any earlier declaration.
    #[inline]
    pub fn with_native_payload_weight_bytes(mut self, weight: NonZeroUsize) -> Self {
        self.native_payload_weight_bytes = Some(weight);
        self
    }

    /// Returns the payload.
    ///
    /// # Returns
    /// The native or encoded payload representation.
    #[must_use = "Use the returned payload."]
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
