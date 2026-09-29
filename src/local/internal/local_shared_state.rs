// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Focused owner for local provider state.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;

use super::LocalBusState;

/// Shared router retained by the SPI and all active subscription receivers.
pub(in crate::local) struct LocalSharedState {
    /// Routing metadata, admission gate, and shutdown result.
    pub(in crate::local) state: Mutex<LocalBusState>,
    /// Signals provider-level queue or settlement progress.
    pub(in crate::local) changed: Condvar,
    /// Wakes asynchronous provider progress waiters.
    pub(in crate::local) async_changed: crate::local::async_signal::AsyncSignal,
    /// Serializes concurrent shutdown callers through the final outcome.
    pub(in crate::local) shutdown_gate: Mutex<()>,
    /// Pending message bound copied into each new queue.
    pub(in crate::local) capacity: usize,
    /// Provider-wide bound shared by all destination queues.
    pub(in crate::local) outstanding: crate::local::outstanding_budget::OutstandingBudget,
}

impl LocalSharedState {
    /// Creates an empty provider state with a validated positive queue bound.
    ///
    /// # Parameters
    /// - `capacity`: maximum queued and unsettled items for each subscription.
    /// - `max_total_outstanding`: provider-wide outstanding-delivery bound.
    ///
    /// # Returns
    /// Shared provider state with no queues or deliveries.
    pub(in crate::local) fn new(capacity: usize, max_total_outstanding: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(LocalBusState::default()),
            changed: Condvar::new(),
            async_changed: crate::local::async_signal::AsyncSignal::default(),
            shutdown_gate: Mutex::new(()),
            capacity,
            outstanding: crate::local::outstanding_budget::OutstandingBudget::new(max_total_outstanding),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;
    use std::cmp::Ordering;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::SystemTime;

    use qubit_id::Id;

    use super::super::DelayedQueueHead;
    use super::super::LocalBusState;
    use super::super::LocalEvent;
    use super::super::LocalQueue;
    use super::super::LocalQueueState;
    use super::super::LocalSettlementState;
    use super::super::TopicSubscriptions;
    use crate::model::EventId;
    use crate::model::Headers;
    use crate::model::SubscriberId;
    use crate::spi::OrderingKey;
    use crate::spi::OutboundMessage;
    use crate::spi::TopicAddress;
    use crate::spi::TransportPayload;

    /// Creates a local queue event with the requested key and native delay.
    fn create_event(id: &str, key: &str, delay: Option<Duration>) -> LocalEvent {
        let topic = TopicAddress::new("orders.created").expect("valid topic");
        let outbound = OutboundMessage::new(
            topic.clone(),
            EventId::new(id).expect("valid event ID"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            Some(OrderingKey::new(key).expect("valid ordering key")),
            delay,
            TransportPayload::Native(Arc::new(42_u32)),
        );
        LocalEvent::transport(topic, &outbound).expect("delay deadline is representable")
    }

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

    #[test]
    fn test_delayed_heap_retries_compact_stale_heads_behind_earlier_deadline() {
        let mut state = LocalQueueState::default();
        state.enqueue_back(create_event("retry", "key-a", None));
        state.enqueue_back(create_event(
            "successor",
            "key-a",
            Some(Duration::from_secs(2 * 60 * 60)),
        ));
        state.enqueue_back(create_event("blocker", "key-b", Some(Duration::from_secs(60 * 60))));

        let now = std::time::Instant::now();
        for _ in 0..128 {
            let retry = state.pop_ready(now).expect("retry head is ready");
            assert_eq!("retry", retry.event_id());
            state.enqueue_front(retry);
            assert_eq!(3, state.pending_count());
            assert_eq!(1, state.delayed_live_count);
            assert!(
                state.delayed_lanes.len() <= state.delayed_live_count + state.delayed_live_count.max(8),
                "delayed heap metadata stays bounded by live lanes and fixed slack"
            );
        }
    }

    #[test]
    fn test_delayed_queue_head_orders_equal_deadlines_by_sequence() {
        let deadline = std::time::Instant::now();
        let first = DelayedQueueHead {
            deadline,
            sequence: 1,
            key: None,
            version: 0,
        };
        let equal = DelayedQueueHead {
            deadline,
            sequence: 1,
            key: None,
            version: 0,
        };
        let later = DelayedQueueHead {
            deadline,
            sequence: 2,
            key: None,
            version: 0,
        };

        assert!(first == equal);
        assert_eq!(Some(Ordering::Equal), first.partial_cmp(&equal));
        assert!(first < later);
    }

    #[test]
    fn test_remove_stale_id_prunes_dead_topic_registration() {
        let id = Id::new(177);
        let topic = TopicAddress::new("stale.topic").expect("valid topic");
        let queue = Arc::new(LocalQueue {
            id,
            topic: topic.clone(),
            subscriber_id: SubscriberId::new("stale-subscriber").expect("valid subscriber ID"),
            capacity: 1,
            state: Mutex::new(LocalQueueState::default()),
            ready: std::sync::Condvar::new(),
            async_ready: crate::local::async_signal::AsyncSignal::default(),
        });
        let mut bucket = TopicSubscriptions::default();
        bucket.queues.insert(id, Arc::downgrade(&queue));
        bucket.payload_type_id = Some(TypeId::of::<u32>());
        let mut state = LocalBusState::default();
        state.topics.insert(topic.clone(), bucket);
        state.subscription_ids.insert(id);
        drop(queue);

        state.remove_stale_id(id);

        assert!(!state.subscription_ids.contains(&id));
        assert!(!state.topics.contains_key(&topic));
    }
}
