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
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::time::Duration;
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

/// Key that groups events subject to the same per-key ordering constraint.
type QueueKey = Option<OrderingKey>;

/// A delayed lane head ordered by its deadline and insertion sequence.
struct DelayedQueueHead {
    /// Earliest instant at which this lane head may be received.
    deadline: Instant,
    /// Stable tie breaker for equal deadlines.
    sequence: u64,
    /// Lane whose current head is delayed.
    key: QueueKey,
    /// Lane generation used to discard stale heap entries.
    version: u64,
}

impl PartialEq for DelayedQueueHead {
    /// Compares heap identity by deadline and insertion sequence.
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.sequence == other.sequence
    }
}

impl Eq for DelayedQueueHead {}

impl PartialOrd for DelayedQueueHead {
    /// Orders delayed heads by deadline, then by insertion sequence.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedQueueHead {
    /// Orders delayed heads by deadline, then by insertion sequence.
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

/// FIFO queue and generation for one ordering lane.
#[derive(Default)]
struct QueueLane {
    /// Pending events belonging to this ordering key.
    events: VecDeque<LocalEvent>,
    /// Generation of the current head, invalidating older cached entries.
    version: u64,
    /// Generation currently represented by a live delayed heap entry.
    delayed_version: Option<u64>,
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
            TransportPayload::Encoded(_) => {
                unreachable!("encoded payloads are rejected by local SPI")
            }
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
    /// FIFO lanes keyed by ordering key, including the unkeyed lane.
    lanes: HashMap<QueueKey, QueueLane>,
    /// Number of pending events across all lanes.
    pending_count: usize,
    /// Ready lane heads in round-robin order, with their generations.
    ready_lanes: VecDeque<(QueueKey, u64)>,
    /// Delayed lane heads ordered by deadline with lazy stale-entry removal.
    delayed_lanes: BinaryHeap<Reverse<DelayedQueueHead>>,
    /// Stable tie breaker for delayed heads.
    next_delay_sequence: u64,
    /// Number of delayed heap entries matching current lane heads.
    delayed_live_count: usize,
    /// Number of delayed heap entries known to be stale.
    delayed_stale_count: usize,
    /// Received but not yet terminally settled delivery attempts.
    pub(super) in_flight: HashMap<Box<str>, LocalInFlight>,
    /// Whether receive calls should stop and return `Closed`.
    pub(super) closed: bool,
    /// Monotonic token component that distinguishes redelivery attempts.
    pub(super) next_delivery_token: u64,
}

impl LocalQueueState {
    /// Adds an event to its lane tail and schedules a head when the lane was
    /// empty.
    pub(super) fn enqueue_back(&mut self, event: LocalEvent) {
        let key = event.ordering_key.clone();
        let lane = self.lanes.entry(key.clone()).or_default();
        let was_empty = lane.events.is_empty();
        lane.events.push_back(event);
        self.pending_count += 1;
        if was_empty {
            self.schedule_lane_head(key);
        }
    }

    /// Restores a retried event at its lane front and reschedules the lane
    /// head.
    pub(super) fn enqueue_front(&mut self, event: LocalEvent) {
        let key = event.ordering_key.clone();
        self.lanes.entry(key.clone()).or_default().events.push_front(event);
        self.pending_count += 1;
        self.schedule_lane_head(key);
    }

    /// Returns the total number of queued events across all lanes.
    pub(super) fn pending_count(&self) -> usize {
        self.pending_count
    }

    /// Returns whether no queued event remains.
    pub(super) fn is_pending_empty(&self) -> bool {
        self.pending_count == 0
    }

    /// Clears all pending events and cached lane schedules.
    pub(super) fn clear_pending(&mut self) {
        self.lanes.clear();
        self.ready_lanes.clear();
        self.delayed_lanes.clear();
        self.pending_count = 0;
        self.delayed_live_count = 0;
        self.delayed_stale_count = 0;
    }

    /// Pops one currently ready lane head, promoting expired delayed heads
    /// first.
    pub(super) fn pop_ready(&mut self, now: Instant) -> Option<LocalEvent> {
        self.promote_due_heads(now);
        while let Some((key, version)) = self.ready_lanes.pop_front() {
            let Some(lane) = self.lanes.get_mut(&key) else {
                continue;
            };
            if lane.version != version {
                continue;
            }
            if !lane
                .events
                .front()
                .is_some_and(|event| event.not_before.is_none_or(|deadline| deadline <= now))
            {
                continue;
            }
            let event = lane.events.pop_front().expect("ready lane has a head");
            self.pending_count -= 1;
            if lane.events.is_empty() {
                self.lanes.remove(&key);
            } else {
                self.schedule_lane_head(key);
            }
            return Some(event);
        }
        None
    }

    /// Returns the delay until the earliest live delayed lane head.
    pub(super) fn next_ready_delay(&mut self, now: Instant) -> Option<Duration> {
        self.discard_stale_delayed_heads();
        self.delayed_lanes
            .peek()
            .map(|Reverse(head)| head.deadline.saturating_duration_since(now))
    }

    /// Increments a lane generation and schedules its current ready or delayed
    /// head.
    fn schedule_lane_head(&mut self, key: QueueKey) {
        let Some((version, deadline, invalidated_delayed_head)) = (|| {
            let lane = self.lanes.get_mut(&key)?;
            let invalidated_delayed_head = lane.delayed_version.take().is_some();
            lane.version = lane.version.wrapping_add(1);
            Some((
                lane.version,
                lane.events.front().and_then(|head| head.not_before),
                invalidated_delayed_head,
            ))
        })() else {
            return;
        };

        if invalidated_delayed_head {
            self.delayed_live_count -= 1;
            self.delayed_stale_count += 1;
        }
        if deadline.is_none_or(|deadline| deadline <= Instant::now()) {
            self.ready_lanes.push_back((key, version));
        } else if let Some(deadline) = deadline {
            self.next_delay_sequence = self.next_delay_sequence.wrapping_add(1);
            self.delayed_lanes.push(Reverse(DelayedQueueHead {
                deadline,
                sequence: self.next_delay_sequence,
                key: key.clone(),
                version,
            }));
            self.lanes
                .get_mut(&key)
                .expect("scheduled lane remains present")
                .delayed_version = Some(version);
            self.delayed_live_count += 1;
        }
        self.compact_delayed_heap_if_needed();
    }

    /// Promotes all due, still-current delayed lane heads to the ready queue.
    fn promote_due_heads(&mut self, now: Instant) {
        self.discard_stale_delayed_heads();
        while self
            .delayed_lanes
            .peek()
            .is_some_and(|Reverse(head)| head.deadline <= now)
        {
            let Reverse(head) = self.delayed_lanes.pop().expect("peeked delayed head exists");
            let is_live = self.lanes.get_mut(&head.key).is_some_and(|lane| {
                if lane.version == head.version && lane.delayed_version == Some(head.version) {
                    lane.delayed_version = None;
                    true
                } else {
                    false
                }
            });
            if is_live {
                self.delayed_live_count -= 1;
                self.ready_lanes.push_back((head.key, head.version));
            } else {
                self.delayed_stale_count -= 1;
            }
            self.discard_stale_delayed_heads();
        }
    }

    /// Removes delayed entries whose lane no longer has the recorded
    /// generation.
    fn discard_stale_delayed_heads(&mut self) {
        while self.delayed_lanes.peek().is_some_and(|Reverse(head)| {
            self.lanes
                .get(&head.key)
                .is_none_or(|lane| lane.version != head.version || lane.delayed_version != Some(head.version))
        }) {
            self.delayed_lanes.pop();
            self.delayed_stale_count -= 1;
        }
    }

    /// Rebuilds the delayed heap when stale entries exceed live heads or fixed
    /// slack.
    fn compact_delayed_heap_if_needed(&mut self) {
        if self.delayed_stale_count <= self.delayed_live_count.max(8) {
            return;
        }
        let mut delayed_lanes = BinaryHeap::new();
        let mut live_count = 0;
        for (key, lane) in &self.lanes {
            let Some(version) = lane.delayed_version else {
                continue;
            };
            let Some(deadline) = lane.events.front().and_then(|event| event.not_before) else {
                continue;
            };
            self.next_delay_sequence = self.next_delay_sequence.wrapping_add(1);
            delayed_lanes.push(Reverse(DelayedQueueHead {
                deadline,
                sequence: self.next_delay_sequence,
                key: key.clone(),
                version,
            }));
            live_count += 1;
        }
        self.delayed_lanes = delayed_lanes;
        self.delayed_live_count = live_count;
        self.delayed_stale_count = 0;
    }
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
    /// Wakes asynchronous receives after provider state changes.
    pub(super) async_ready: super::async_signal::AsyncSignal,
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
        let queues = self.queues.values().filter_map(Weak::upgrade).collect::<Vec<_>>();
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
    /// Removes dead identifiers in one topic and returns its ordered live
    /// queues.
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

    /// Removes one stale provider-wide identifier from whichever bucket owns
    /// it.
    pub(super) fn remove_stale_id(&mut self, id: Id) {
        for bucket in self.topics.values_mut() {
            if bucket.queues.get(&id).is_some_and(|queue| queue.strong_count() == 0) {
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
    /// Wakes asynchronous provider progress waiters.
    pub(super) async_changed: super::async_signal::AsyncSignal,
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
            async_changed: super::async_signal::AsyncSignal::default(),
            shutdown_gate: Mutex::new(()),
            capacity,
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

    use super::BusState;
    use super::DelayedQueueHead;
    use super::LocalEvent;
    use super::LocalQueue;
    use super::LocalQueueState;
    use super::LocalSettlementState;
    use super::TopicSubscriptions;
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
        let mut state = BusState::default();
        state.topics.insert(topic.clone(), bucket);
        state.subscription_ids.insert(id);
        drop(queue);

        state.remove_stale_id(id);

        assert!(!state.subscription_ids.contains(&id));
        assert!(!state.topics.contains_key(&topic));
    }
}
