// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared public lifecycle checks for synchronous and asynchronous local SPIs.

use std::any::TypeId;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_spi::ServiceProvider;

/// Two real local implementations exercised by the same externally visible
/// contract.
enum Bus {
    Sync(Arc<dyn EventBusSpi>),
    Async(AsyncLocalEventBusSpi),
}

/// A real receiver whose immediate methods can be invoked without a runtime.
enum Receiver {
    Sync(Box<dyn EventSubscriptionSpi>),
    Async(Box<dyn AsyncEventSubscriptionSpi>),
}

/// Polls operations that have been deliberately arranged to complete
/// immediately.
fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("test operation must complete immediately"),
    }
}

impl Bus {
    /// Creates the selected local SPI with a ten-slot count capacity and
    /// optional weight budget.
    fn new(asynchronous: bool, weight: Option<usize>) -> Self {
        let mut config = LocalEventBusConfig::new()
            .queue_capacity(10)
            .max_total_outstanding(10);
        if let Some(weight) = weight {
            config = config.max_total_outstanding_weight_bytes(
                NonZeroUsize::new(weight).expect("positive budget"),
            );
        }
        if asynchronous {
            Self::Async(AsyncLocalEventBusSpi::new(&config).expect("valid async config"))
        } else {
            Self::Sync(
                LocalEventBusProvider
                    .create_configured(
                        &EventBusConfig::default().with_provider_options(config.provider_options()),
                    )
                    .expect("valid sync config"),
            )
        }
    }

    /// Creates a deterministic receiver for the shared topic.
    fn subscribe(&self, id: u64) -> Receiver {
        let request = SpiSubscriptionRequest::new(
            Id::new(id),
            topic(),
            SubscriberId::new(format!("weight-{id}")).expect("valid subscriber"),
            None,
            SubscriptionDurability::Ephemeral,
            StartPosition::New,
            ProviderOptions::new(),
            TypeId::of::<u32>(),
        );
        match self {
            Self::Sync(bus) => Receiver::Sync(bus.subscribe(request).expect("sync subscription")),
            Self::Async(bus) => {
                Receiver::Async(ready(bus.subscribe(request)).expect("async subscription"))
            }
        }
    }

    /// Publishes through the real SPI and returns its unmodified result.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        match self {
            Self::Sync(bus) => bus.publish(message),
            Self::Async(bus) => ready(bus.publish(message)),
        }
    }

    /// Completes immediate or zero-timeout graceful shutdown.
    fn shutdown(&self, mode: ShutdownMode) -> ShutdownOutcome {
        match self {
            Self::Sync(bus) => bus.shutdown(mode).expect("sync shutdown"),
            Self::Async(bus) => ready(bus.shutdown(mode)).expect("async shutdown"),
        }
    }
}

impl Receiver {
    /// Receives only already eligible messages and never waits.
    fn receive(&mut self) -> ReceiveOutcome {
        match self {
            Self::Sync(receiver) => receiver.receive(Duration::ZERO).expect("sync receive"),
            Self::Async(receiver) => {
                ready(receiver.receive(Duration::ZERO)).expect("async receive")
            }
        }
    }

    /// Applies one provider settlement and checks that it succeeds.
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) {
        match self {
            Self::Sync(receiver) => receiver
                .settle(token, disposition)
                .expect("sync settlement"),
            Self::Async(receiver) => {
                ready(receiver.settle(token, disposition)).expect("async settlement")
            }
        }
    }

    /// Closes the receiver synchronously or by polling its immediate future.
    fn close(&mut self) {
        match self {
            Self::Sync(receiver) => receiver.close().expect("sync close"),
            Self::Async(receiver) => ready(receiver.close()).expect("async close"),
        }
    }
}

/// Returns the validated topic used by all budget scenarios.
fn topic() -> TopicAddress {
    TopicAddress::new("local.weight.contract").expect("valid topic")
}

/// Creates a native message with optional declaration and independent identity.
fn message(id: &str, weight: Option<usize>, delay: Option<Duration>) -> OutboundMessage {
    let message = OutboundMessage::new(
        topic(),
        EventId::new(id).expect("valid event ID"),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
        delay,
        TransportPayload::Native(Arc::new(7_u32)),
    );
    match weight {
        Some(weight) => message
            .with_native_payload_weight_bytes(NonZeroUsize::new(weight).expect("positive weight")),
        None => message,
    }
}

/// Asserts the exact accepted and rejected counts without assuming fanout
/// order.
fn admissions(ack: PublishAcknowledgement, accepted: usize, rejected: usize) {
    let PublishAcknowledgement::DestinationAdmissions(items) = ack else {
        panic!("local admission detail")
    };
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item.status(), AdmissionStatus::Accepted))
            .count(),
        accepted
    );
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item.status(), AdmissionStatus::Rejected(_)))
            .count(),
        rejected
    );
    assert_eq!(items.len(), accepted + rejected);
}

/// Missing declarations fail before any destination admission, even without
/// subscribers.
pub(crate) fn missing_weight(asynchronous: bool) {
    let bus = Bus::new(asynchronous, Some(5));
    for subscribed in [false, true] {
        let mut receivers = if subscribed {
            vec![bus.subscribe(1), bus.subscribe(2)]
        } else {
            vec![]
        };
        let error = bus
            .publish(message("missing", None, None))
            .expect_err("missing declaration fails");
        assert!(matches!(
            error,
            SpiError::Publish {
                kind: "missing_native_payload_weight",
                retryable: Some(false),
                effect: PublishEffect::NotAccepted,
                ..
            }
        ));
        for receiver in &mut receivers {
            assert!(matches!(receiver.receive(), ReceiveOutcome::TimedOut));
        }
    }
}

/// Oversized deliveries are rejected; partial fanout keeps its accepted copy.
pub(crate) fn partial_fanout(asynchronous: bool) {
    let bus = Bus::new(asynchronous, Some(5));
    let mut receivers = [bus.subscribe(1), bus.subscribe(2)];
    admissions(
        bus.publish(message("oversized", Some(6), None))
            .expect("per-target rejection"),
        0,
        2,
    );
    admissions(
        bus.publish(message("partial", Some(3), None))
            .expect("partial admission"),
        1,
        1,
    );
    let mut delivered = 0;
    for receiver in &mut receivers {
        if let ReceiveOutcome::Message(mut inbound) = receiver.receive() {
            delivered += 1;
            assert_eq!(inbound.id().as_str(), "partial");
            let token = inbound.take_settlement().expect("settlement token");
            receiver.settle(&token, DeliveryDisposition::Accept);
        }
    }
    assert_eq!(delivered, 1);
    admissions(
        bus.publish(message("reused", Some(3), None))
            .expect("released budget reused"),
        1,
        1,
    );
}

/// Retry retains both reservations; terminal and duplicate settlements release
/// exactly once.
pub(crate) fn settlement(asynchronous: bool) {
    for disposition in [DeliveryDisposition::Accept, DeliveryDisposition::Reject] {
        let bus = Bus::new(asynchronous, Some(5));
        let mut receiver = bus.subscribe(1);
        admissions(
            bus.publish(message("first", Some(5), None))
                .expect("first admission"),
            1,
            0,
        );
        let ReceiveOutcome::Message(mut inbound) = receiver.receive() else {
            panic!("first delivery")
        };
        let first_token = inbound.take_settlement().expect("first token");
        receiver.settle(&first_token, DeliveryDisposition::Retry);
        receiver.settle(&first_token, DeliveryDisposition::Retry);
        admissions(
            bus.publish(message("blocked", Some(1), None))
                .expect("weight remains reserved"),
            0,
            1,
        );
        let ReceiveOutcome::Message(mut retried) = receiver.receive() else {
            panic!("retried delivery")
        };
        let token = retried.take_settlement().expect("retry token");
        receiver.settle(&token, disposition);
        receiver.settle(&token, disposition);
        admissions(
            bus.publish(message("reused", Some(5), None))
                .expect("terminal release"),
            1,
            0,
        );
        receiver.close();
        receiver.close();
    }
}

/// Close, Drop, and both shutdown modes clear ready, delayed, and in-flight
/// owners.
pub(crate) fn cleanup(asynchronous: bool) {
    for cleanup in 0..4 {
        let bus = Bus::new(asynchronous, Some(10));
        let mut receiver = bus.subscribe(1);
        admissions(
            bus.publish(message("inflight", Some(2), None))
                .expect("first admission"),
            1,
            0,
        );
        let ReceiveOutcome::Message(inbound) = receiver.receive() else {
            panic!("in-flight delivery")
        };
        drop(inbound);
        admissions(
            bus.publish(message("ready", Some(3), None))
                .expect("ready admission"),
            1,
            0,
        );
        admissions(
            bus.publish(message("delayed", Some(5), Some(Duration::from_secs(3600))))
                .expect("delayed admission"),
            1,
            0,
        );
        admissions(
            bus.publish(message("blocked", Some(1), None))
                .expect("budget full"),
            0,
            1,
        );
        if cleanup == 0 {
            receiver.close();
            receiver.close();
        }
        if cleanup < 2 {
            drop(receiver);
            let mut fresh = bus.subscribe(2);
            admissions(
                bus.publish(message("reused", Some(10), None))
                    .expect("all reservations released"),
                1,
                0,
            );
            fresh.close();
        } else {
            let mode = if cleanup == 2 {
                ShutdownMode::Immediate
            } else {
                ShutdownMode::Graceful {
                    timeout: Duration::ZERO,
                }
            };
            let outcome = bus.shutdown(mode);
            assert_eq!(
                outcome,
                if cleanup == 2 {
                    ShutdownOutcome::Complete
                } else {
                    ShutdownOutcome::TimedOut
                }
            );
            assert_eq!(bus.shutdown(ShutdownMode::Immediate), outcome);
            assert!(matches!(receiver.receive(), ReceiveOutcome::Closed));
            receiver.close();
            receiver.close();
        }
    }
}

/// Disabled weight accounting accepts absent and maximally large declarations.
pub(crate) fn disabled(asynchronous: bool) {
    let bus = Bus::new(asynchronous, None);
    let mut receiver = bus.subscribe(1);
    for (id, weight) in [
        ("missing", None),
        ("large-one", Some(usize::MAX)),
        ("large-two", Some(usize::MAX)),
    ] {
        admissions(
            bus.publish(message(id, weight, None))
                .expect("count-only admission"),
            1,
            0,
        );
    }
    receiver.close();
    let mut fresh = bus.subscribe(2);
    admissions(
        bus.publish(message("after-close", Some(usize::MAX), None))
            .expect("count-only reuse"),
        1,
        0,
    );
    fresh.close();
}
