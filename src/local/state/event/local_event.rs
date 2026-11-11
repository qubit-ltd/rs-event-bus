// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Queue-ready event data shared by matching local subscriptions.

use std::num::NonZeroUsize;
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
    /// Optional key retained for delivery context.
    pub(in crate::local) ordering_key: Option<OrderingKey>,
    /// Declared copy weight in bytes, normalized to zero when budgeting is
    /// disabled.
    pub(in crate::local) weight_bytes: usize,
    /// Shared native payload; local queues do not serialize it.
    pub(in crate::local) payload: SharedPayload,
    /// Earliest monotonic instant at which this event may be received.
    pub(in crate::local) not_before: Option<Instant>,
    /// Number of successful receives for this subscription's event copy.
    /// `None` means the exact count overflowed and is no longer representable.
    delivery_attempts: Option<u32>,
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
            weight_bytes: message.native_payload_weight_bytes().map_or(0, NonZeroUsize::get),
            not_before,
            delivery_attempts: Some(0),
        })
    }

    /// Records one successful receive for this event copy.
    ///
    /// Once the counter overflows, it remains unknown rather than wrapping
    /// and reporting an inaccurate attempt number.
    pub(in crate::local) fn record_delivery_attempt(&mut self) {
        self.delivery_attempts = self.delivery_attempts.and_then(|value| value.checked_add(1));
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
        let attempt = self.delivery_attempts.and_then(std::num::NonZeroU32::new);
        let message = InboundMessage::new(
            self.topic,
            self.id.clone(),
            self.timestamp,
            self.headers,
            self.ordering_key,
            self.payload.to_transport(),
            Some(SettlementToken::new(subscription_id, settlement)),
            ProviderMessageMetadata::default(),
        );
        if let Some(attempt) = attempt {
            message.with_provider_attempt(attempt)
        } else {
            message
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::SystemTime;

    use qubit_id::Id;

    use super::LocalEvent;
    use crate::local::state::event::local_settlement_state::LocalSettlementState;
    use crate::model::EventId;
    use crate::model::Headers;
    use crate::spi::DeliveryDisposition;
    use crate::spi::OutboundMessage;
    use crate::spi::TopicAddress;
    use crate::spi::TransportPayload;

    fn event() -> LocalEvent {
        let outbound = OutboundMessage::new(
            TopicAddress::new("local.counter").expect("valid topic"),
            EventId::new("counter-event").expect("valid event ID"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(1_u32)),
        );
        LocalEvent::transport(TopicAddress::new("local.counter").unwrap(), &outbound).unwrap()
    }

    #[test]
    fn test_delivery_attempt_overflow_remains_unknown_and_converts_to_unknown_metadata() {
        let mut event = event();
        event.delivery_attempts = Some(u32::MAX - 1);
        event.record_delivery_attempt();
        assert_eq!(Some(u32::MAX), event.delivery_attempts);

        let max_message = event.clone().into_inbound(Id::new(1), settlement());
        assert_eq!(Some(u32::MAX), max_message.provider_attempt().map(|value| value.get()));

        event.record_delivery_attempt();
        assert_eq!(None, event.delivery_attempts);
        event.record_delivery_attempt();
        assert_eq!(None, event.delivery_attempts);
        let unknown_message = event.into_inbound(Id::new(1), settlement());
        assert_eq!(None, unknown_message.provider_attempt());
    }

    fn settlement() -> super::super::local_settlement_state::LocalSettlementHandle {
        Arc::new(Mutex::new(LocalSettlementState {
            token_id: "counter-event:1".into(),
            disposition: Option::<DeliveryDisposition>::None,
        }))
    }
}
