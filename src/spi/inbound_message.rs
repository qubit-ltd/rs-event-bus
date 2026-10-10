// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Message received from provider SPI.

use std::num::NonZeroU32;
use std::time::SystemTime;

use super::OrderingKey;
use super::SettlementToken;
use super::TopicAddress;
use super::TransportPayload;
use crate::model::EventId;
use crate::model::Headers;
use crate::model::ProviderMessageMetadata;

/// Type-erased event delivered by a backend to the facade.
///
/// # Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use std::time::SystemTime;
///
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::ProviderMessageMetadata;
/// use qubit_event_bus::spi::InboundMessage;
/// use qubit_event_bus::spi::TopicAddress;
/// use qubit_event_bus::spi::TransportPayload;
///
/// let message = InboundMessage::new(
///     TopicAddress::new("orders.created").unwrap(),
///     EventId::new("order-42").unwrap(),
///     SystemTime::UNIX_EPOCH,
///     BTreeMap::new(), None,
///     TransportPayload::Native(std::sync::Arc::new(String::from("order-42"))),
///     None,
///     ProviderMessageMetadata::new(),
/// );
/// assert_eq!(message.topic().as_str(), "orders.created");
/// ```
#[must_use]
pub struct InboundMessage {
    /// Validated source topic address supplied by the provider.
    topic: TopicAddress,
    /// Stable event identity retained across redelivery attempts.
    id: EventId,
    /// Original event creation time supplied by the publisher.
    timestamp: SystemTime,
    /// Portable event headers preserved across the transport boundary.
    headers: Headers,
    /// Optional key selecting a provider or facade ordering lane.
    ordering_key: Option<OrderingKey>,
    /// Native allocation or encoded representation delivered to the facade.
    payload: TransportPayload,
    /// Single-owner token, or `None` for transports without settlement support.
    settlement: Option<SettlementToken>,
    /// Non-sensitive transport metadata associated with this delivery.
    provider_metadata: ProviderMessageMetadata,
    /// Provider-reported attempt number, when the transport knows it.
    provider_attempt: Option<NonZeroU32>,
}

impl InboundMessage {
    /// Creates an inbound transport message.
    ///
    /// # Parameters
    /// - `topic`: source destination address.
    /// - `id`: stable event identifier.
    /// - `timestamp`: event creation time.
    /// - `headers`: portable event headers.
    /// - `ordering_key`: optional per-key ordering value.
    /// - `payload`: native or encoded payload representation.
    /// - `settlement`: optional provider token for terminal settlement.
    /// - `provider_metadata`: non-sensitive provider metadata.
    ///
    /// # Returns
    /// An inbound message retaining the supplied transport fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topic: TopicAddress,
        id: EventId,
        timestamp: SystemTime,
        headers: Headers,
        ordering_key: Option<OrderingKey>,
        payload: TransportPayload,
        settlement: Option<SettlementToken>,
        provider_metadata: ProviderMessageMetadata,
    ) -> Self {
        Self {
            topic,
            id,
            timestamp,
            headers,
            ordering_key,
            payload,
            settlement,
            provider_metadata,
            provider_attempt: None,
        }
    }
    /// Returns the source topic.
    ///
    /// # Returns
    /// The validated source address.
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
    /// Returns the event timestamp.
    ///
    /// # Returns
    /// The event creation time.
    #[must_use]
    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
    /// Returns the event headers.
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
    /// `Some` with the key when configured, otherwise `None`.
    #[inline]
    pub fn ordering_key(&self) -> Option<&OrderingKey> {
        self.ordering_key.as_ref()
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
    /// Returns the provider settlement token, if this delivery is settleable.
    ///
    /// # Returns
    /// `Some` when a provider settlement token is available, otherwise `None`.
    #[inline]
    pub fn settlement(&self) -> Option<&SettlementToken> {
        self.settlement.as_ref()
    }
    /// Returns non-sensitive provider metadata.
    ///
    /// # Returns
    /// The provider metadata map without cloning it.
    #[must_use]
    #[inline]
    pub fn provider_metadata(&self) -> &ProviderMessageMetadata {
        &self.provider_metadata
    }

    /// Returns the provider-reported attempt for this delivery.
    ///
    /// # Returns
    /// `Some` with a positive attempt number when supplied by the provider,
    /// otherwise `None`. Local handler retries do not change this value.
    #[inline]
    pub fn provider_attempt(&self) -> Option<NonZeroU32> {
        self.provider_attempt
    }

    /// Sets the positive provider-reported attempt for this delivery.
    ///
    /// # Parameters
    /// - `attempt`: attempt number supplied by the provider.
    ///
    /// # Returns
    /// The message with its provider attempt set; other fields are retained.
    pub fn with_provider_attempt(mut self, attempt: NonZeroU32) -> Self {
        self.provider_attempt = Some(attempt);
        self
    }

    /// Takes the provider settlement token, if present.
    ///
    /// # Returns
    /// The token when present; a later call returns `None` after it is taken.
    pub fn take_settlement(&mut self) -> Option<SettlementToken> {
        self.settlement.take()
    }

    /// Consumes the provider message and transfers its original transport
    /// fields to the facade. Read `provider_attempt` before consuming it.
    ///
    /// This is the ownership-taking receive path: it allows the facade to
    /// downcast a native `Arc<dyn Any>` without imposing `Clone` on payloads
    /// and keeps settlement-token ownership tied to the received message.
    ///
    /// # Returns
    /// Topic, ID, timestamp, headers, ordering key, payload, optional
    /// settlement token, and provider metadata in that order.
    #[must_use = "Use the returned transport fields."]
    pub fn into_parts(
        self,
    ) -> (
        TopicAddress,
        EventId,
        SystemTime,
        Headers,
        Option<OrderingKey>,
        TransportPayload,
        Option<SettlementToken>,
        ProviderMessageMetadata,
    ) {
        (
            self.topic,
            self.id,
            self.timestamp,
            self.headers,
            self.ordering_key,
            self.payload,
            self.settlement,
            self.provider_metadata,
        )
    }
}
