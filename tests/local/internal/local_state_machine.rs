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
use std::collections::BTreeSet;
use std::future::Future;
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

struct TokenRecord {
    event: u64,
    token: SettlementToken,
    disposition: Option<DeliveryDisposition>,
}

struct Subscription {
    id: Id,
    receiver: Option<Box<dyn AsyncEventSubscriptionSpi>>,
    pending: BTreeSet<u64>,
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
            self.pending.len() + self.tokens.iter().filter(|record| record.disposition.is_none()).count()
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
        assert!(
            self.pending.remove(&event),
            "receive must consume an admitted pending event"
        );
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

/// Creates a bounded native message with one independent ordering lane per
/// event.
fn message(event: u64, timestamp: SystemTime, payload: &[u8]) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("fuzz.local").expect("valid topic"),
        EventId::new(event.to_string()).expect("valid event ID"),
        timestamp,
        Default::default(),
        Some(OrderingKey::new(&event.to_string()).expect("valid ordering key")),
        None,
        TransportPayload::Native(Arc::new(payload[..payload.len().min(MAX_PAYLOAD)].to_vec())),
    )
}

/// Runs at most 64 input-selected public SPI operations, with at most eight
/// receivers.
///
/// Input is truncated to 4096 bytes. At most 64 publications and 256 payload
/// bytes per publication are accepted by the driver. Receive uses zero timeout,
/// and the cancellation operation polls an infinite receive at most once.
pub(crate) fn run(input: &[u8]) {
    let clock = ManualMonotonicClock::new();
    let timer = Arc::new(ManualTimer::from_clock(&clock));
    let spi = AsyncLocalEventBusSpi::with_timer(
        &LocalEventBusConfig::new()
            .queue_capacity(QUEUE_CAPACITY)
            .max_total_outstanding(TOTAL_CAPACITY),
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
                            pending: BTreeSet::new(),
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
                let acknowledgement = ready(spi.publish(message(
                    event,
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
                        let available = subscription.outstanding() < QUEUE_CAPACITY && outstanding < TOTAL_CAPACITY;
                        assert_eq!(
                            destination.status() == &AdmissionStatus::Accepted,
                            available,
                            "capacity admission matches externally tracked ownership"
                        );
                        if available {
                            assert!(subscription.pending.insert(event));
                            outstanding += 1;
                        }
                    }
                }
            }
            2 | 7 => {
                if let Some(subscription) = subscriptions[index].as_mut() {
                    let receiver = subscription.receiver.as_mut().expect("receiver retained");
                    let timeout = if opcode == 7 { Duration::MAX } else { Duration::ZERO };
                    let result = {
                        let mut future = std::pin::pin!(receiver.receive(timeout));
                        future.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                    };
                    match result {
                        Poll::Ready(Ok(ReceiveOutcome::Message(message))) => subscription.received(message),
                        Poll::Ready(Ok(ReceiveOutcome::TimedOut)) => {
                            assert!(subscription.pending.is_empty())
                        }
                        Poll::Ready(Ok(ReceiveOutcome::Closed)) => assert!(subscription.closed),
                        Poll::Pending => {
                            assert_eq!(opcode, 7);
                            assert!(subscription.pending.is_empty());
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
                        record.disposition = Some(disposition);
                        if disposition == DeliveryDisposition::Retry {
                            assert!(
                                subscription.pending.insert(record.event),
                                "retry restores pending ownership without releasing capacity"
                            );
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
                    // Unsettled tokens remain owned but close discards their
                    // deliveries.
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
    }
    drop(subscriptions);
}
