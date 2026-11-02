// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded stateful operations against the real public asynchronous local SPI.
//!
//! Messages have no delay: local queue deadlines currently use Instant
//! directly. The injected manual timer and deterministic timestamps do not
//! claim to model delayed delivery. No operation waits, sleeps, or accesses the
//! network.

use std::any::TypeId;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::SystemTime;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::ManualTimer;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

const MAX_INPUT: usize = 4096;
const MAX_OPERATIONS: usize = 64;
const MAX_SUBSCRIPTIONS: usize = 8;
const MAX_MESSAGES: usize = 64;
const MAX_PAYLOAD: usize = 256;
const QUEUE_CAPACITY: usize = 4;
const TOTAL_CAPACITY: usize = 8;
const WEIGHT_LIMIT: usize = 12;

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingEvent {
    id: u64,
    lane: u8,
    weight: usize,
}

type PendingByLane = BTreeMap<u8, VecDeque<PendingEvent>>;

fn consume_lane(pending: &mut PendingByLane, lane: u8, id: u64) -> Result<PendingEvent, &'static str> {
    let queue = pending.get_mut(&lane).ok_or("receive lane has no pending event")?;
    if queue.front().is_none_or(|event| event.id != id) {
        return Err("receive must consume the same-key FIFO head");
    }
    let event = queue.pop_front().expect("validated queue head");
    if queue.is_empty() {
        pending.remove(&lane);
    }
    Ok(event)
}

fn record_disposition(record: &mut TokenRecord, disposition: DeliveryDisposition) -> bool {
    if record.disposition.is_some() {
        return false;
    }
    record.disposition = Some(disposition);
    true
}

struct TokenRecord {
    event: PendingEvent,
    token: SettlementToken,
    disposition: Option<DeliveryDisposition>,
}

struct Subscription {
    id: Id,
    receiver: Option<Box<dyn AsyncEventSubscriptionSpi>>,
    pending: PendingByLane,
    tokens: Vec<TokenRecord>,
    closed: bool,
}

impl Subscription {
    /// Returns queued and unsettled delivery ownership tracked at the public
    /// boundary.
    fn outstanding(&self) -> usize {
        if self.closed {
            0
        } else {
            self.pending.values().map(VecDeque::len).sum::<usize>()
                + self.tokens.iter().filter(|record| record.disposition.is_none()).count()
        }
    }

    fn outstanding_weight(&self) -> usize {
        if self.closed {
            0
        } else {
            self.pending.values().flatten().map(|event| event.weight).sum::<usize>()
                + self
                    .tokens
                    .iter()
                    .filter(|record| record.disposition.is_none())
                    .map(|record| record.event.weight)
                    .sum::<usize>()
        }
    }

    /// Records one real received message and transfers its non-cloneable token
    /// once.
    fn received(&mut self, mut message: InboundMessage) {
        let event = message
            .id()
            .as_str()
            .parse::<u64>()
            .expect("deterministic numeric event ID");
        let lane = message
            .ordering_key()
            .expect("every modeled event has a lane")
            .as_str()
            .parse::<u8>()
            .expect("numeric lane");
        let event = consume_lane(&mut self.pending, lane, event).expect("receive must consume the same-key FIFO head");
        assert!(
            matches!(message.payload(), TransportPayload::Native(payload) if payload.downcast_ref::<Vec<u8>>().is_some_and(|bytes| bytes.len() <= MAX_PAYLOAD))
        );
        let token = message
            .take_settlement()
            .expect("local delivery owns one settlement token");
        assert!(message.take_settlement().is_none(), "token ownership transfers once");
        assert!(token.belongs_to(self.id));
        self.tokens.push(TokenRecord {
            event,
            token,
            disposition: None,
        });
    }
}

/// Polls an immediate public SPI operation once; Pending is a contract failure.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("bounded operation must complete without a runtime or wait"),
    }
}

/// Creates one native-byte subscription with a deterministic bus-local
/// identity.
fn request(id: u64) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new("fuzz.local").expect("valid topic"),
        SubscriberId::new(format!("subscriber-{id}")).expect("valid subscriber"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::default(),
        TypeId::of::<Vec<u8>>(),
    )
}

/// Creates a bounded native message with an input-selected lane and weight.
fn message(event: u64, lane: u8, weight: usize, timestamp: SystemTime, payload: &[u8]) -> OutboundMessage {
    let declared = NonZeroUsize::new(weight).expect("positive bounded test weight");
    let message = OutboundMessage::new(
        TopicAddress::new("fuzz.local").expect("valid topic"),
        EventId::new(event.to_string()).expect("valid event ID"),
        timestamp,
        Default::default(),
        Some(OrderingKey::new(&lane.to_string()).expect("valid ordering key")),
        None,
        TransportPayload::Native(Arc::new(payload[..payload.len().min(MAX_PAYLOAD)].to_vec())),
    )
    .with_native_payload_weight_bytes(declared);
    assert_eq!(message.native_payload_weight_bytes(), Some(declared));
    message
}

/// Runs at most 64 input-selected public SPI operations, with at most eight
/// receivers.
///
/// Input is truncated to 4096 bytes. At most 64 publications and 256 payload
/// bytes per publication are accepted by the driver. Receive uses zero timeout,
/// and the cancellation operation polls an infinite receive at most once.
pub(crate) fn run(input: &[u8]) {
    run_scenario(input, None);
    run_scenario(input, NonZeroUsize::new(WEIGHT_LIMIT));
}

fn run_scenario(input: &[u8], weight_limit: Option<NonZeroUsize>) {
    let clock = ManualMonotonicClock::new();
    let timer = Arc::new(ManualTimer::from_clock(&clock));
    let spi = AsyncLocalEventBusSpi::with_timer(
        &{
            let mut config = LocalEventBusConfig::new()
                .queue_capacity(QUEUE_CAPACITY)
                .max_total_outstanding(TOTAL_CAPACITY);
            if let Some(limit) = weight_limit {
                config = config.max_total_outstanding_weight_bytes(limit);
            }
            config
        },
        timer,
    )
    .expect("positive bounded local capacities");
    let mut subscriptions: Vec<Option<Subscription>> = (0..MAX_SUBSCRIPTIONS).map(|_| None).collect();
    let mut next_id = 1_u64;
    let mut publications = 0;
    let mut elapsed = Duration::ZERO;
    let mut shutdown = false;

    for chunk in input[..input.len().min(MAX_INPUT)].chunks(64).take(MAX_OPERATIONS) {
        let opcode = chunk[0] % 11;
        let index = usize::from(*chunk.get(1).unwrap_or(&0)) % MAX_SUBSCRIPTIONS;
        let argument = usize::from(*chunk.get(2).unwrap_or(&0));
        match opcode {
            0 => {
                if subscriptions[index].is_none() {
                    let result = ready(spi.subscribe(request(next_id)));
                    if shutdown {
                        assert!(result.is_err(), "shutdown rejects new receivers");
                    } else {
                        subscriptions[index] = Some(Subscription {
                            id: Id::new(next_id),
                            receiver: Some(result.expect("valid new subscription")),
                            pending: PendingByLane::new(),
                            tokens: Vec::new(),
                            closed: false,
                        });
                        next_id += 1;
                    }
                }
            }
            1 if publications < MAX_MESSAGES => {
                publications += 1;
                let event = publications as u64;
                let lane = (argument % 4) as u8;
                let weight = 1 + (argument / 4) % 8;
                let acknowledgement = ready(spi.publish(message(
                    event,
                    lane,
                    weight,
                    SystemTime::UNIX_EPOCH + elapsed,
                    chunk.get(3..).unwrap_or_default(),
                )));
                if shutdown {
                    assert!(acknowledgement.is_err(), "shutdown rejects publication");
                } else {
                    let PublishAcknowledgement::DestinationAdmissions(destinations) =
                        acknowledgement.expect("bounded native publish")
                    else {
                        panic!("local provider reports per-destination admission");
                    };
                    let mut outstanding: usize = subscriptions.iter().flatten().map(Subscription::outstanding).sum();
                    let mut outstanding_weight: usize = subscriptions
                        .iter()
                        .flatten()
                        .map(Subscription::outstanding_weight)
                        .sum();
                    let live = subscriptions
                        .iter()
                        .flatten()
                        .filter(|subscription| !subscription.closed)
                        .count();
                    assert_eq!(destinations.len(), live);
                    for destination in destinations {
                        let subscription = subscriptions
                            .iter_mut()
                            .flatten()
                            .find(|subscription| subscription.id == destination.subscription_id())
                            .expect("admission belongs to a live receiver");
                        let available = subscription.outstanding() < QUEUE_CAPACITY
                            && outstanding < TOTAL_CAPACITY
                            && weight_limit.is_none_or(|_| outstanding_weight + weight <= WEIGHT_LIMIT);
                        assert_eq!(
                            destination.status() == &AdmissionStatus::Accepted,
                            available,
                            "capacity admission matches externally tracked ownership"
                        );
                        if available {
                            subscription.pending.entry(lane).or_default().push_back(PendingEvent {
                                id: event,
                                lane,
                                weight,
                            });
                            outstanding += 1;
                            outstanding_weight += weight;
                        }
                    }
                }
            }
            2 | 7 => {
                if let Some(subscription) = subscriptions[index].as_mut() {
                    let receiver = subscription.receiver.as_mut().expect("receiver retained");
                    let timeout = if opcode == 7 { Duration::MAX } else { Duration::ZERO };
                    let result = {
                        let mut future = pin!(receiver.receive(timeout));
                        future.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                    };
                    match result {
                        Poll::Ready(Ok(ReceiveOutcome::Message(message))) => subscription.received(message),
                        Poll::Ready(Ok(ReceiveOutcome::TimedOut)) => {
                            assert!(subscription.pending.values().all(VecDeque::is_empty))
                        }
                        Poll::Ready(Ok(ReceiveOutcome::Closed)) => assert!(subscription.closed),
                        Poll::Pending => {
                            assert_eq!(opcode, 7);
                            assert!(subscription.pending.values().all(VecDeque::is_empty));
                        }
                        _ => panic!("unexpected local receive outcome"),
                    }
                }
            }
            3..=5 => {
                if let Some(subscription) = subscriptions[index].as_mut()
                    && !subscription.tokens.is_empty()
                {
                    let selected = argument % subscription.tokens.len();
                    let disposition = match opcode {
                        3 => DeliveryDisposition::Accept,
                        4 => DeliveryDisposition::Retry,
                        _ => DeliveryDisposition::Reject,
                    };
                    let record = &mut subscription.tokens[selected];
                    let result = ready(
                        subscription
                            .receiver
                            .as_mut()
                            .expect("receiver retained")
                            .settle(&record.token, disposition),
                    );
                    if let Some(previous) = record.disposition {
                        assert_eq!(
                            result.is_ok(),
                            previous == disposition,
                            "repeat is idempotent; conflicts reject"
                        );
                    } else if subscription.closed {
                        assert!(result.is_err(), "close invalidates unsettled tokens");
                    } else {
                        result.expect("owner settles its live token");
                        assert!(record_disposition(record, disposition));
                        if disposition == DeliveryDisposition::Retry {
                            assert!(
                                subscription
                                    .pending
                                    .entry(record.event.lane)
                                    .or_default()
                                    .front()
                                    .is_none_or(|first| first.id != record.event.id),
                                "retry restores pending ownership without releasing capacity"
                            );
                            subscription
                                .pending
                                .entry(record.event.lane)
                                .or_default()
                                .push_front(record.event.clone());
                        }
                    }
                }
            }
            6 => {
                let source = argument % MAX_SUBSCRIPTIONS;
                if source != index {
                    let mut receiver = subscriptions[index]
                        .as_mut()
                        .and_then(|subscription| subscription.receiver.take());
                    if let (Some(receiver), Some(owner)) = (receiver.as_mut(), subscriptions[source].as_ref())
                        && let Some(record) = owner.tokens.first()
                    {
                        assert!(
                            !record
                                .token
                                .belongs_to(subscriptions[index].as_ref().expect("target exists").id)
                        );
                        assert!(
                            ready(receiver.settle(&record.token, DeliveryDisposition::Accept)).is_err(),
                            "foreign settlement must not release another owner's capacity"
                        );
                    }
                    if let Some(subscription) = subscriptions[index].as_mut() {
                        subscription.receiver = receiver;
                    }
                }
            }
            8 => {
                if let Some(subscription) = subscriptions[index].as_mut() {
                    ready(subscription.receiver.as_mut().expect("receiver retained").close())
                        .expect("idempotent close");
                    subscription.closed = true;
                    subscription.pending.clear();
                    // Close discards pending and unsettled deliveries from the
                    // model.
                }
            }
            9 => {
                if let Some(subscription) = subscriptions[index].take() {
                    drop(subscription);
                }
            }
            10 => {
                if argument % 2 == 0 {
                    clock.advance(Duration::from_millis(1)).expect("bounded manual advance");
                    elapsed += Duration::from_millis(1);
                } else {
                    assert!(matches!(
                        ready(spi.shutdown(ShutdownMode::Immediate)).expect("idempotent shutdown"),
                        ShutdownOutcome::Complete
                    ));
                    shutdown = true;
                    for subscription in subscriptions.iter_mut().flatten() {
                        subscription.closed = true;
                        subscription.pending.clear();
                    }
                }
            }
            _ => {}
        }
        let outstanding: usize = subscriptions
            .iter()
            .flatten()
            .filter(|subscription| !subscription.closed)
            .map(Subscription::outstanding)
            .sum();
        assert!(outstanding <= TOTAL_CAPACITY);
        assert!(
            subscriptions
                .iter()
                .flatten()
                .all(|subscription| subscription.closed || subscription.outstanding() <= QUEUE_CAPACITY)
        );
        if weight_limit.is_some() {
            assert!(
                subscriptions
                    .iter()
                    .flatten()
                    .map(Subscription::outstanding_weight)
                    .sum::<usize>()
                    <= WEIGHT_LIMIT
            );
        }
    }
    drop(subscriptions);
}

#[cfg(test)]
mod tests {
    use super::{
        DeliveryDisposition, Id, PendingByLane, PendingEvent, SettlementToken, TokenRecord, consume_lane,
        record_disposition,
    };
    use std::collections::VecDeque;

    fn event(id: u64, lane: u8, weight: usize) -> PendingEvent {
        PendingEvent { id, lane, weight }
    }

    #[test]
    fn test_local_state_machine_oracle_rejects_same_key_reordering() {
        let mut pending = PendingByLane::from([(1, VecDeque::from([event(1, 1, 1), event(2, 1, 1)]))]);
        assert_eq!(
            consume_lane(&mut pending, 1, 2),
            Err("receive must consume the same-key FIFO head")
        );
    }

    #[test]
    fn test_local_state_machine_oracle_duplicate_accept_does_not_release_twice() {
        let mut record = TokenRecord {
            event: event(1, 0, 4),
            token: SettlementToken::new(Id::new(1), 1_u64),
            disposition: None,
        };
        let outstanding_weight = |record: &TokenRecord| {
            if record.disposition.is_none() {
                record.event.weight
            } else {
                0
            }
        };
        assert_eq!(outstanding_weight(&record), 4);
        assert!(record_disposition(&mut record, DeliveryDisposition::Accept));
        assert_eq!(outstanding_weight(&record), 0);
        assert!(!record_disposition(&mut record, DeliveryDisposition::Accept));
        assert_eq!(outstanding_weight(&record), 0);
    }

    #[test]
    fn test_local_state_machine_oracle_counts_fanout_weight_per_destination() {
        let mut subscriptions = [PendingByLane::new(), PendingByLane::new()];
        for pending in &mut subscriptions {
            pending.entry(0).or_default().push_back(event(1, 0, 4));
        }
        let outstanding_weight: usize = subscriptions
            .iter()
            .flat_map(|pending| pending.values().flatten())
            .map(|pending| pending.weight)
            .sum();
        assert_eq!(outstanding_weight, 8);
    }
}
