// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared queue state for local subscriptions.

mod bus_state;
mod delayed_queue_head;
mod event;
mod local_queue;
mod local_queue_state;
mod local_shared_state;
mod queue_lane;
mod topic_subscriptions;

pub(super) use event::LocalEvent;
pub(super) use event::LocalInFlight;
pub(super) use event::LocalSettlementHandle;
pub(super) use event::LocalSettlementState;
pub(super) use local_queue::LocalQueue;
pub(super) use local_queue_state::LocalQueueState;
pub(super) use local_shared_state::LocalSharedState;

#[cfg(test)]
mod tests {
    use std::any::TypeId;
    use std::cmp::Ordering;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::Instant;
    use std::time::SystemTime;

    use qubit_id::Id;

    use super::LocalEvent;
    use super::LocalQueue;
    use super::LocalQueueState;
    use super::LocalSettlementState;
    use super::bus_state::BusState;
    use super::delayed_queue_head::DelayedQueueHead;
    use super::topic_subscriptions::TopicSubscriptions;
    use crate::local::async_signal::AsyncSignal;
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

        let now = Instant::now();
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
        let deadline = Instant::now();
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
            ready: Condvar::new(),
            async_ready: AsyncSignal::default(),
        });
        let mut bucket = TopicSubscriptions::default();
        bucket.queues.insert(id, Arc::downgrade(&queue));
        bucket.payload_type_id = Some(TypeId::of::<u32>());
        let mut state = BusState::default();
        state.topics.insert(topic.clone(), bucket);
        state.subscription_ids.insert(id);
        drop(queue);

        state.remove_stale_id(id);

        assert!(!state.subscription_ids.contains(&id));
        assert!(!state.topics.contains_key(&topic));
    }
}
