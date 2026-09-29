// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Message received from provider SPI.

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
/// use qubit_event_bus::model::{EventId, ProviderMessageMetadata};
/// use qubit_event_bus::spi::{InboundMessage, TopicAddress, TransportPayload};
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
    topic: TopicAddress,
    id: EventId,
    timestamp: SystemTime,
    headers: Headers,
    ordering_key: Option<OrderingKey>,
    payload: TransportPayload,
    settlement: Option<SettlementToken>,
    provider_metadata: ProviderMessageMetadata,
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
        }
    }
    /// Returns the source topic.
    ///
    /// # Returns
    /// The validated source address.
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
    #[must_use]
    #[inline]
    pub fn ordering_key(&self) -> Option<&OrderingKey> {
        self.ordering_key.as_ref()
    }
    /// Returns the payload.
    ///
    /// # Returns
    /// The native or encoded payload representation.
    #[inline]
    pub fn payload(&self) -> &TransportPayload {
        &self.payload
    }
    /// Returns the provider settlement token, if this delivery is settleable.
    ///
    /// # Returns
    /// `Some` when a provider settlement token is available, otherwise `None`.
    #[must_use]
    #[inline]
    pub fn settlement(&self) -> Option<&SettlementToken> {
        self.settlement.as_ref()
    }
    /// Takes the provider settlement token, if present.
    ///
    /// # Returns
    /// The token when present; a later call returns `None` after it is taken.
    #[must_use]
    pub fn take_settlement(&mut self) -> Option<SettlementToken> {
        self.settlement.take()
    }

    /// Consumes the provider message and transfers every transport field to the
    /// facade.
    ///
    /// This is the ownership-taking receive path: it allows the facade to
    /// downcast a native `Arc<dyn Any>` without imposing `Clone` on payloads
    /// and keeps settlement-token ownership tied to the received message.
    ///
    /// # Returns
    /// Topic, ID, timestamp, headers, ordering key, payload, optional
    /// settlement token, and provider metadata in that order.
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

    /// Returns non-sensitive provider metadata.
    ///
    /// # Returns
    /// The provider metadata map without cloning it.
    #[must_use]
    #[inline]
    pub fn provider_metadata(&self) -> &ProviderMessageMetadata {
        &self.provider_metadata
    }
}
