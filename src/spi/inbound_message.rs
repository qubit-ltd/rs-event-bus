//! Message received from provider SPI.

use std::time::SystemTime;

use crate::model::{EventId, Headers, ProviderMessageMetadata};

use super::OrderingKey;
use super::SettlementToken;
use super::TopicAddress;
use super::TransportPayload;

/// Type-erased event delivered by a backend to the facade.
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
    pub fn topic(&self) -> &TopicAddress {
        &self.topic
    }
    /// Returns the event identifier.
    pub fn id(&self) -> &EventId {
        &self.id
    }
    /// Returns the event timestamp.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
    /// Returns the event headers.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    /// Returns the optional ordering key.
    pub fn ordering_key(&self) -> Option<&OrderingKey> {
        self.ordering_key.as_ref()
    }
    /// Returns the payload.
    pub fn payload(&self) -> &TransportPayload {
        &self.payload
    }
    /// Returns the provider settlement token, if this delivery is settleable.
    pub fn settlement(&self) -> Option<&SettlementToken> {
        self.settlement.as_ref()
    }
    /// Takes the provider settlement token, if present.
    pub fn take_settlement(&mut self) -> Option<SettlementToken> {
        self.settlement.take()
    }
    /// Returns non-sensitive provider metadata.
    pub fn provider_metadata(&self) -> &ProviderMessageMetadata {
        &self.provider_metadata
    }
}
