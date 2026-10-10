// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Queue-ready event data shared by matching local subscriptions.

use std::time::Instant;
use std::time::SystemTime;

use qubit_id::Id;

use super::local_settlement_state::LocalSettlementHandle;
use super::shared_payload::SharedPayload;
use crate::model::EventId;
use crate::model::Headers;
use crate::model::ProviderMessageMetadata;
use crate::spi::InboundMessage;
use crate::spi::OrderingKey;
use crate::spi::OutboundMessage;
use crate::spi::SettlementToken;
use crate::spi::TopicAddress;

/// Queue-ready event data shared by each matching local subscription.
#[derive(Clone)]
pub(in crate::local) struct LocalEvent {
    /// Destination selected during publication.
    pub(in crate::local) topic: TopicAddress,
    /// Stable event identity copied from the outbound message.
    pub(in crate::local) id: EventId,
    /// Original event creation time.
    pub(in crate::local) timestamp: SystemTime,
    /// Application and facade-managed metadata.
    pub(in crate::local) headers: Headers,
    /// Optional key retained for downstream delivery context.
    pub(in crate::local) ordering_key: Option<OrderingKey>,
    /// Declared copy weight in bytes, normalized to zero when budgeting is
    /// disabled.
    pub(in crate::local) weight_bytes: usize,
    /// Shared native payload; local queues do not serialize it.
    pub(in crate::local) payload: SharedPayload,
    /// Earliest monotonic instant at which this event may be received.
    pub(in crate::local) not_before: Option<Instant>,
}

impl LocalEvent {
    /// Copies outbound metadata and shares its native payload into queue form.
    ///
    /// # Parameters
    /// - `topic`: validated local destination.
    /// - `message`: outbound message whose payload and metadata are retained.
    ///
    /// # Returns
    /// `Some` with a queued event, or `None` if a delay deadline overflows.
    pub(in crate::local) fn transport(topic: TopicAddress, message: &OutboundMessage) -> Option<Self> {
        let not_before = match message.delay() {
            Some(delay) => Some(Instant::now().checked_add(delay)?),
            None => None,
        };
        Some(Self {
            topic,
            id: message.id().clone(),
            timestamp: message.timestamp(),
            headers: message.headers().clone(),
            ordering_key: message.ordering_key().cloned(),
            payload: SharedPayload::from_transport(message.payload()),
            weight_bytes: message
                .native_payload_weight_bytes()
                .map_or(0, std::num::NonZeroUsize::get),
            not_before,
        })
    }

    /// Converts queued data into one delivery and binds a fresh settlement
    /// handle.
    ///
    /// # Parameters
    /// - `subscription_id`: queue identity bound to the settlement token.
    /// - `settlement`: shared state retained by the token and queue entry.
    ///
    /// # Returns
    /// The provider message containing this event and its settlement token.
    pub(in crate::local) fn into_inbound(
        self,
        subscription_id: Id,
        settlement: LocalSettlementHandle,
    ) -> InboundMessage {
        InboundMessage::new(
            self.topic,
            self.id.clone(),
            self.timestamp,
            self.headers,
            self.ordering_key,
            self.payload.to_transport(),
            Some(SettlementToken::new(subscription_id, settlement)),
            ProviderMessageMetadata::default(),
        )
    }

    /// Returns the event identity used to construct a per-delivery token key.
    ///
    /// # Returns
    /// The event ID as a string slice.
    #[must_use = "Use the returned event id."]
    #[inline]
    pub(in crate::local) fn event_id(&self) -> &str {
        self.id.as_str()
    }
}
