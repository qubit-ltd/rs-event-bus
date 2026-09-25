// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Shared queue state for local subscriptions.

use std::any::TypeId;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::time::Instant;
use std::time::SystemTime;

use qubit_id::Id;

use crate::model::EventId;
use crate::model::Headers;
use crate::model::ProviderMessageMetadata;
use crate::spi::InboundMessage;
use crate::spi::OrderingKey;
use crate::spi::SettlementToken;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

/// Queue-ready event data shared by each matching local subscription.
#[derive(Clone)]
pub(super) struct LocalEvent {
    /// Destination selected during publication.
    pub(super) topic: TopicAddress,
    /// Stable event identity copied from the outbound message.
    pub(super) id: EventId,
    /// Original event creation time.
    pub(super) timestamp: SystemTime,
    /// Application and facade-managed metadata.
    pub(super) headers: Headers,
    /// Optional key retained for downstream delivery context.
    pub(super) ordering_key: Option<OrderingKey>,
    /// Shared native payload; local queues do not serialize it.
    pub(super) payload: SharedPayload,
    /// Earliest monotonic instant at which this event may be received.
    pub(super) not_before: Option<Instant>,
}

impl LocalEvent {
    /// Converts queued data into one delivery and binds a fresh settlement
    /// handle.
    pub(super) fn into_inbound(self, subscription_id: Id, settlement: LocalSettlementHandle) -> InboundMessage {
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
    pub(super) fn event_id(&self) -> &str {
        self.id.as_str()
    }

    /// Copies outbound metadata and shares its native payload into queue form.
    pub(super) fn transport(topic: TopicAddress, message: &crate::spi::OutboundMessage) -> Option<Self> {
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
            not_before,
        })
    }
}

/// Shared settlement state stored both by the receiver and its opaque token.
pub(super) type LocalSettlementHandle = Arc<Mutex<LocalSettlementState>>;

/// Remembers the stable token identity and any terminal result for retries.
pub(super) struct LocalSettlementState {
    /// Per-delivery key that selects its in-flight queue entry.
    pub(super) token_id: Box<str>,
    /// First successfully applied disposition, if settlement has completed.
    pub(super) disposition: Option<crate::spi::DeliveryDisposition>,
}

/// One received event retained until its delivery token is settled.
pub(super) struct LocalInFlight {
    /// Event data needed to requeue a retry.
    pub(super) event: LocalEvent,
    /// Identity shared with the opaque token to prevent token substitution.
    pub(super) settlement: LocalSettlementHandle,
}

/// Cloneable transport payload forms used by the local queue.
#[derive(Clone)]
pub(super) enum SharedPayload {
    /// Native value shared without a payload clone or serialization.
    Native(Arc<dyn std::any::Any + Send + Sync>),
}

impl SharedPayload {
    /// Copies a supported provider payload into its shareable local form.
    pub(super) fn from_transport(payload: &TransportPayload) -> Self {
        match payload {
            TransportPayload::Native(value) => Self::Native(value.clone()),
            TransportPayload::Encoded(_) => unreachable!("encoded payloads are rejected by local SPI"),
        }
    }

    /// Reconstructs an SPI payload while retaining the same native allocation.
    pub(super) fn to_transport(&self) -> TransportPayload {
        match self {
            Self::Native(value) => TransportPayload::Native(value.clone()),
        }
    }
}

/// Mutable state owned by one subscription receiver.
#[derive(Default)]
pub(super) struct LocalQueueState {
    /// Pending events, including those whose native delay has not expired.
    pub(super) messages: VecDeque<LocalEvent>,
    /// Received but not yet terminally settled delivery attempts.
    pub(super) in_flight: HashMap<Box<str>, LocalInFlight>,
    /// Whether receive calls should stop and return `Closed`.
    pub(super) closed: bool,
    /// Monotonic token component that distinguishes redelivery attempts.
    pub(super) next_delivery_token: u64,
}

/// One bounded FIFO queue and receiver lifecycle for a logical subscriber.
pub(super) struct LocalQueue {
    /// Bus-local identifier used to bind settlement tokens.
    pub(super) id: Id,
    /// Topic this queue receives.
    pub(super) topic: TopicAddress,
    /// Logical subscriber identity reported by publish admissions.
    pub(super) subscriber_id: crate::model::SubscriberId,
    /// Maximum number of queued and unsettled events for this subscription.
    pub(super) capacity: usize,
    /// Queue state shared by publisher and its single receiver.
    pub(super) state: Mutex<LocalQueueState>,
    /// Wakes synchronous receives after publish, close, or shutdown.
    pub(super) ready: Condvar,
}

impl LocalQueue {
    /// Locks queue state while recovering from a poisoned standard mutex.
    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, LocalQueueState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Live subscriptions sharing one topic and native payload type.
#[derive(Default)]
pub(super) struct TopicSubscriptions {
    /// Live queues indexed in stable subscription identifier order.
    pub(super) queues: BTreeMap<Id, Weak<LocalQueue>>,
    /// Payload type bound while this bucket has live subscriptions.
    pub(super) payload_type_id: Option<TypeId>,
}

impl TopicSubscriptions {
    /// Removes dead subscriptions and returns live queues in identifier order.
    pub(super) fn live_queues(&mut self) -> Vec<Arc<LocalQueue>> {
        self.queues.retain(|_, queue| queue.strong_count() > 0);
        let queues = self
            .queues
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        if queues.is_empty() {
            self.payload_type_id = None;
        }
        queues
    }

    /// Removes an entry only when it still points to the closing queue.
    pub(super) fn remove(&mut self, id: Id, queue: &Arc<LocalQueue>) -> bool {
        let matches = self
            .queues
            .get(&id)
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, queue));
        if matches {
            self.queues.remove(&id);
        }
        if self.queues.is_empty() {
            self.payload_type_id = None;
        }
        matches
    }
}

/// Provider-wide routing table and shutdown state.
#[derive(Default)]
pub(super) struct BusState {
    /// Topic-local queues and payload type bindings.
    pub(super) topics: HashMap<TopicAddress, TopicSubscriptions>,
    /// Live subscription identifiers are unique across the provider.
    pub(super) subscription_ids: HashSet<Id>,
    /// Rejects new provider operations after shutdown begins.
    pub(super) closed: bool,
    /// Stable outcome returned by all later shutdown calls.
    pub(super) shutdown_outcome: Option<crate::spi::ShutdownOutcome>,
    /// Change counter that prevents missed graceful-shutdown notifications.
    pub(super) change_version: u64,
}

impl BusState {
    /// Removes dead identifiers in one topic and returns its ordered live queues.
    pub(super) fn live_queues_for_topic(&mut self, topic: &TopicAddress) -> Vec<Arc<LocalQueue>> {
        let Some(bucket) = self.topics.get_mut(topic) else {
            return Vec::new();
        };
        let dead_ids = bucket
            .queues
            .iter()
            .filter_map(|(id, queue)| (queue.strong_count() == 0).then_some(*id))
            .collect::<Vec<_>>();
        let queues = bucket.live_queues();
        for id in dead_ids {
            self.subscription_ids.remove(&id);
        }
        if bucket.queues.is_empty() {
            self.topics.remove(topic);
        }
        queues
    }

    /// Collects all live queues while pruning stale topic entries and IDs.
    pub(super) fn live_queues(&mut self) -> Vec<Arc<LocalQueue>> {
        let topics = self.topics.keys().cloned().collect::<Vec<_>>();
        let mut queues = Vec::new();
        for topic in topics {
            queues.extend(self.live_queues_for_topic(&topic));
        }
        queues
    }

    /// Removes one stale provider-wide identifier from whichever bucket owns it.
    pub(super) fn remove_stale_id(&mut self, id: Id) {
        for bucket in self.topics.values_mut() {
            if bucket
                .queues
                .get(&id)
                .is_some_and(|queue| queue.strong_count() == 0)
            {
                bucket.queues.remove(&id);
                if bucket.queues.is_empty() {
                    bucket.payload_type_id = None;
                }
                break;
            }
        }
        self.subscription_ids.remove(&id);
        self.topics.retain(|_, bucket| !bucket.queues.is_empty());
    }
}

/// Shared router retained by the SPI and all active subscription receivers.
pub(super) struct LocalSharedState {
    /// Routing metadata, admission gate, and shutdown result.
    pub(super) state: Mutex<BusState>,
    /// Signals provider-level queue or settlement progress.
    pub(super) changed: Condvar,
    /// Serializes concurrent shutdown callers through the final outcome.
    pub(super) shutdown_gate: Mutex<()>,
    /// Pending message bound copied into each new queue.
    pub(super) capacity: usize,
}

impl LocalSharedState {
    /// Creates an empty provider state with a validated positive queue bound.
    pub(super) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(BusState::default()),
            changed: Condvar::new(),
            shutdown_gate: Mutex::new(()),
            capacity,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::SystemTime;

    use qubit_id::Id;

    use super::LocalEvent;
    use super::LocalSettlementState;
    use crate::model::EventId;
    use crate::model::Headers;
    use crate::spi::OrderingKey;
    use crate::spi::OutboundMessage;
    use crate::spi::TopicAddress;
    use crate::spi::TransportPayload;

    #[test]
    fn test_into_inbound_preserves_event_and_settlement_identity() {
        let topic = TopicAddress::new("orders.created").expect("valid topic");
        let outbound = OutboundMessage::new(
            topic.clone(),
            EventId::new("event-1").expect("valid event ID"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            Some(OrderingKey::new("order-1").expect("valid ordering key")),
            None,
            TransportPayload::Native(Arc::new(42_u32)),
        );
        let event = LocalEvent::transport(topic, &outbound).expect("event has no delayed deadline");
        let subscription_id = Id::new(7);
        let settlement = Arc::new(Mutex::new(LocalSettlementState {
            token_id: event.event_id().into(),
            disposition: None,
        }));

        let inbound = event.into_inbound(subscription_id, settlement);

        assert_eq!("orders.created", inbound.topic().as_str());
        assert_eq!("event-1", inbound.id().as_str());
        assert_eq!(Some("order-1"), inbound.ordering_key().map(OrderingKey::as_str));
        assert!(
            matches!(inbound.payload(), TransportPayload::Native(payload) if payload.downcast_ref::<u32>() == Some(&42))
        );
        assert!(
            inbound
                .settlement()
                .is_some_and(|token| token.belongs_to(subscription_id))
        );
    }
}
