// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Additional public-contract coverage for synchronous scheduling and lifecycle
//! edges.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::LifecycleError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::error::SubscribeError;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::IntoHandlerResult;
use qubit_event_bus::facade::SyncDeliverySchedulerConfig;
use qubit_event_bus::facade::WaitOutcome;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderMessageMetadata;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::pipeline::Diagnostic;

#[test]
fn facade_config_keeps_the_caller_supplied_codec_registry() {
    let codecs = Arc::new(CodecRegistry::new());
    let config = EventBusFacadeConfig::new().with_codec_registry(Arc::clone(&codecs));

    assert!(Arc::ptr_eq(config.codec_registry(), &codecs));
}

#[test]
fn unit_handler_result_is_treated_as_success() {
    assert!(().into_handler_result().is_ok());
}
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
const PROVIDER_ID: &str = "sync-coverage";
const TOPIC_NAME: &str = "sync.coverage";

struct CoverageState {
    receivers: Mutex<Vec<(String, TopicAddress, SyncSender<InboundMessage>)>>,
    close_attempts: mpsc::Sender<String>,
    shutdown_calls: AtomicUsize,
    fail_next_publish: AtomicBool,
    fail_next_subscribe: AtomicBool,
    fail_next_shutdown: AtomicBool,
    close_fails: bool,
}

#[derive(Clone)]
struct CoverageSpi {
    state: Arc<CoverageState>,
}

struct CoverageSubscription {
    subscriber_id: String,
    receiver: Receiver<InboundMessage>,
    closed: bool,
    close_attempts: mpsc::Sender<String>,
    close_fails: bool,
}

impl CoverageSpi {
    fn new(close_fails: bool) -> (Self, Receiver<String>) {
        let (close_attempts, close_rx) = mpsc::channel();
        (
            Self {
                state: Arc::new(CoverageState {
                    receivers: Mutex::new(Vec::new()),
                    close_attempts,
                    shutdown_calls: AtomicUsize::new(0),
                    fail_next_publish: AtomicBool::new(false),
                    fail_next_subscribe: AtomicBool::new(false),
                    fail_next_shutdown: AtomicBool::new(false),
                    close_fails,
                }),
            },
            close_rx,
        )
    }

    fn shutdown_calls(&self) -> usize {
        self.state.shutdown_calls.load(Ordering::Acquire)
    }

    fn fail_next_publish(&self) {
        self.state.fail_next_publish.store(true, Ordering::Release);
    }

    fn fail_next_subscribe(&self) {
        self.state.fail_next_subscribe.store(true, Ordering::Release);
    }

    fn fail_next_shutdown(&self) {
        self.state.fail_next_shutdown.store(true, Ordering::Release);
    }
}

impl EventBusSpi for CoverageSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::PerKey,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        if self.state.fail_next_publish.swap(false, Ordering::AcqRel) {
            return Err(spi_error("publish", "configured_failure"));
        }
        let TransportPayload::Native(payload) = message.payload() else {
            return Err(spi_error("publish", "unsupported_payload"));
        };
        let receivers = self.state.receivers.lock().expect("receiver registry lock");
        for (_, topic, sender) in receivers.iter() {
            if topic.as_str() != message.topic().as_str() {
                continue;
            }
            let inbound = InboundMessage::new(
                message.topic().clone(),
                message.id().clone(),
                message.timestamp(),
                message.headers().clone(),
                message.ordering_key().cloned(),
                TransportPayload::Native(payload.clone()),
                None,
                ProviderMessageMetadata::default(),
            );
            sender
                .send(inbound)
                .map_err(|_| spi_error("publish", "receiver_closed"))?;
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: ProviderMessageMetadata::default(),
        })
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        if self.state.fail_next_subscribe.swap(false, Ordering::AcqRel) {
            return Err(spi_error("subscribe", "configured_failure"));
        }
        let subscriber_id = request.subscriber_id().as_str().to_owned();
        let (sender, receiver) = mpsc::sync_channel(16);
        self.state.receivers.lock().expect("receiver registry lock").push((
            subscriber_id.clone(),
            request.topic().clone(),
            sender,
        ));
        Ok(Box::new(CoverageSubscription {
            subscriber_id,
            receiver,
            closed: false,
            close_attempts: self.state.close_attempts.clone(),
            close_fails: self.state.close_fails,
        }))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.state.shutdown_calls.fetch_add(1, Ordering::AcqRel);
        if self.state.fail_next_shutdown.swap(false, Ordering::AcqRel) {
            return Err(spi_error("shutdown", "configured_failure"));
        }
        Ok(ShutdownOutcome::Complete)
    }
}

impl EventSubscriptionSpi for CoverageSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.closed {
            return Ok(ReceiveOutcome::Closed);
        }
        match self.receiver.recv_timeout(timeout) {
            Ok(message) => Ok(ReceiveOutcome::Message(message)),
            Err(RecvTimeoutError::Timeout) => Ok(ReceiveOutcome::TimedOut),
            Err(RecvTimeoutError::Disconnected) => Ok(ReceiveOutcome::Closed),
        }
    }

    fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        Err(spi_error("settle", "unsupported"))
    }

    fn close(&mut self) -> Result<(), SpiError> {
        self.closed = true;
        if self.close_attempts.send(self.subscriber_id.clone()).is_err() {
            return Ok(());
        }
        if self.close_fails {
            Err(spi_error("close_subscription", "close_failure"))
        } else {
            Ok(())
        }
    }
}

fn spi_error(operation: &'static str, kind: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: PROVIDER_ID.into(),
        operation,
        resource: None,
        kind,
        retryable: Some(false),
        source: Box::new(std::io::Error::other(kind)),
    }
}

fn topic() -> Topic<String> {
    Topic::new(TOPIC_NAME).expect("static topic is valid")
}

fn other_topic() -> Topic<String> {
    Topic::new("sync.coverage.other").expect("static topic is valid")
}

fn keyed_request(payload: &str, key: &str) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(topic())
        .payload(payload.to_owned())
        .ordering_key(key)
        .build()
        .expect("valid event request")
}

fn keyed_request_for(topic: Topic<String>, payload: &str, key: &str) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(topic)
        .payload(payload.to_owned())
        .ordering_key(key)
        .build()
        .expect("valid event request")
}

fn request(payload: &str) -> PublishRequest<String> {
    PublishRequest::new(topic(), payload.to_owned()).expect("OS random source is available")
}

fn create_bus(max_in_flight: usize, handler_queue_capacity: usize) -> (EventBus, CoverageSpi, Receiver<String>) {
    create_bus_with_close_mode(max_in_flight, handler_queue_capacity, false)
}

fn create_bus_with_close_mode(
    max_in_flight: usize,
    handler_queue_capacity: usize,
    close_fails: bool,
) -> (EventBus, CoverageSpi, Receiver<String>) {
    let (spi, close_rx) = CoverageSpi::new(close_fails);
    let scheduler = SyncDeliverySchedulerConfig::new(max_in_flight, handler_queue_capacity)
        .expect("test scheduler limits are valid");
    let config = EventBusFacadeConfig::default().with_sync_delivery_scheduler(scheduler);
    let bus = EventBus::with_config(
        ProviderId::new(PROVIDER_ID).expect("static provider ID is valid"),
        Arc::new(spi.clone()),
        config,
    );
    (bus, spi, close_rx)
}

fn keyed_options() -> SubscribeOptions<String> {
    SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build()
}

#[test]
fn scheduler_config_rejects_zero_global_admission_capacity() {
    let error = SyncDeliverySchedulerConfig::new(0, 0).expect_err("zero in-flight capacity is invalid");
    assert!(matches!(
        error,
        ConfigurationError::InvalidField {
            field: "max_in_flight",
            ..
        }
    ));
}

#[test]
fn publish_all_keeps_later_results_after_a_provider_failure() {
    let (bus, spi, _) = create_bus(1, 1);
    spi.fail_next_publish();

    let result = bus.publish_all([request("fails"), request("continues")]);

    assert_eq!(2, result.total_count());
    assert_eq!(1, result.accepted_count());
    assert_eq!(1, result.failure_count());
    assert!(matches!(
        &result.items()[0],
        Err(PublishError::Spi(SpiError::Operation {
            operation: "publish",
            kind: "configured_failure",
            ..
        }))
    ));
    assert!(result.items()[1].is_ok());
    bus.shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down after batch publication");
}

#[test]
fn provider_subscribe_failure_does_not_poison_later_subscription() {
    let (bus, spi, _) = create_bus(1, 1);
    spi.fail_next_subscribe();
    let failed_request = SubscribeRequest::new(
        SubscriberId::new("provider-subscribe-failure").expect("valid subscriber ID"),
        topic(),
    );
    let Err(error) = bus.subscribe(failed_request, |_| {}) else {
        panic!("configured provider failure is propagated");
    };
    assert!(matches!(
        error,
        SubscribeError::Spi(SpiError::Operation {
            operation: "subscribe",
            kind: "configured_failure",
            ..
        })
    ));

    let subscription = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("provider-subscribe-recovery").expect("valid subscriber ID"),
                topic(),
            ),
            |_| {},
        )
        .expect("a failed admission leaves the facade usable");
    subscription.cancel().expect("successful receiver closes");
    bus.shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down after subscription recovery");
}

#[test]
fn shutdown_provider_error_is_retryable_and_closes_public_admission() {
    let (bus, spi, _) = create_bus(1, 1);
    spi.fail_next_shutdown();

    let error = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("provider shutdown failure is surfaced");
    assert!(matches!(
        error,
        ShutdownError::Spi(SpiError::Operation {
            operation: "shutdown",
            kind: "configured_failure",
            ..
        })
    ));
    assert_eq!(1, spi.shutdown_calls());

    assert_eq!(
        ShutdownOutcome::Complete,
        bus.shutdown(ShutdownMode::Immediate)
            .expect("a later shutdown retries the provider")
    );
    assert_eq!(2, spi.shutdown_calls());
    assert!(matches!(bus.publish(request("after-close")), Err(PublishError::Closed)));
    assert!(matches!(
        bus.subscribe(
            SubscribeRequest::new(SubscriberId::new("after-close").expect("valid subscriber ID"), topic(),),
            |_| {},
        ),
        Err(SubscribeError::Closed)
    ));
}

#[test]
fn shutdown_is_idempotent_and_caches_the_provider_outcome() {
    let (bus, spi, _) = create_bus(1, 1);

    assert_eq!(
        ShutdownOutcome::Complete,
        bus.shutdown(ShutdownMode::Immediate).expect("first shutdown completes")
    );
    assert_eq!(
        ShutdownOutcome::Complete,
        bus.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_millis(10),
        })
        .expect("repeated shutdown returns cached outcome")
    );
    assert_eq!(1, spi.shutdown_calls());
}

#[test]
fn subscription_handle_exposes_identity_and_repeated_cancel_is_safe() {
    let (bus, _, _) = create_bus(1, 1);
    let expected_id = SubscriberId::new("handle-contract").expect("valid subscriber ID");
    let subscription = bus
        .subscribe(SubscribeRequest::new(expected_id.clone(), topic()), |_| {})
        .expect("subscription starts");

    let object_id = subscription.id();
    assert_eq!(&expected_id, subscription.subscriber_id());
    assert!(!subscription.is_cancelled());
    subscription.cancel().expect("first cancel joins worker");
    assert!(subscription.is_cancelled());
    assert_eq!(object_id, subscription.id());
    subscription.cancel().expect("repeated cancel remains idempotent");
    bus.shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down after explicit cancellation");
}

#[test]
fn callback_reentrant_wait_and_shutdown_return_would_deadlock() {
    let (bus, _, _) = create_bus(1, 1);
    let callback_bus = bus.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("reentrant-lifecycle").expect("valid subscriber ID"),
                topic(),
            ),
            move |_| {
                let wait = callback_bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(1)));
                let shutdown = callback_bus.shutdown(ShutdownMode::Immediate);
                result_tx
                    .send((wait, shutdown))
                    .expect("test remains available to receive callback result");
            },
        )
        .expect("subscription starts");

    bus.publish(request("reentrant")).expect("publish reaches callback");
    let (wait, shutdown) = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("callback reports both lifecycle outcomes");
    assert!(matches!(
        wait,
        Err(LifecycleError::WouldDeadlock {
            operation: "wait_for_received_deliveries"
        })
    ));
    assert!(matches!(
        shutdown,
        Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
            operation: "shutdown"
        }))
    ));

    subscription.cancel().expect("external caller cancels worker");
    bus.shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down after callback exits");
}

#[test]
fn dropping_diagnostic_handle_stops_future_close_failure_notifications() {
    let (bus, _, _close_rx) = create_bus_with_close_mode(1, 1, true);
    let observed = Arc::new(AtomicUsize::new(0));
    let observed_by_callback = observed.clone();
    let observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { .. }) {
            observed_by_callback.fetch_add(1, Ordering::AcqRel);
        }
    });
    let first = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("observed-close-error").expect("valid subscriber ID"),
                topic(),
            ),
            |_| {},
        )
        .expect("first subscription starts");
    let error = first.cancel().expect_err("configured provider close error is surfaced");
    assert!(matches!(error, LifecycleError::SubscriptionClose(_)));
    assert_eq!(1, observed.load(Ordering::Acquire));

    drop(observer);
    let second = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("unobserved-close-error").expect("valid subscriber ID"),
                topic(),
            ),
            |_| {},
        )
        .expect("second subscription starts");
    assert!(matches!(second.cancel(), Err(LifecycleError::SubscriptionClose(_))));
    assert_eq!(1, observed.load(Ordering::Acquire));
    assert!(matches!(
        bus.shutdown(ShutdownMode::Immediate),
        Err(ShutdownError::SubscriptionClose(_))
    ));
}

#[test]
fn same_ordering_key_is_independent_between_subscriptions() {
    let (bus, _, _) = create_bus(2, 4);
    let (first_started_tx, first_started_rx) = mpsc::channel();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let first_release = release_rx.clone();
    let first = bus
        .subscribe(
            SubscribeRequest::new(SubscriberId::new("lane-first").expect("valid subscriber ID"), topic())
                .with_options(keyed_options()),
            move |_| {
                first_started_tx.send(()).expect("first observer remains alive");
                first_release
                    .lock()
                    .expect("release gate lock")
                    .recv()
                    .expect("first subscription is released");
            },
        )
        .expect("first subscription starts");
    let second = bus
        .subscribe(
            SubscribeRequest::new(SubscriberId::new("lane-second").expect("valid subscriber ID"), topic())
                .with_options(keyed_options()),
            move |_| second_started_tx.send(()).expect("second observer remains alive"),
        )
        .expect("second subscription starts");

    bus.publish(keyed_request("shared-key-event", "same-key"))
        .expect("publish to both subscriptions");
    first_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first subscription starts its handler");
    second_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("same key in another subscription is a separate lane");
    release_tx.send(()).expect("first handler gate remains alive");

    assert_eq!(
        WaitOutcome::Idle,
        bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
            .unwrap()
    );
    first.cancel().expect("first subscription closes");
    second.cancel().expect("second subscription closes");
    bus.shutdown(ShutdownMode::Immediate).expect("bus shuts down");
}

#[test]
fn same_ordering_key_is_independent_between_topics() {
    let (bus, _, _) = create_bus(2, 4);
    let (first_started_tx, first_started_rx) = mpsc::channel();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let first_release = Arc::new(Mutex::new(release_rx));
    let first_release_gate = first_release.clone();
    let first_topic = topic();
    let second_topic = other_topic();
    let first = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("topic-lane-first").expect("valid subscriber ID"),
                first_topic.clone(),
            )
            .with_options(keyed_options()),
            move |_| {
                first_started_tx.send(()).expect("first observer remains alive");
                first_release_gate
                    .lock()
                    .expect("release gate lock")
                    .recv()
                    .expect("first topic handler is released");
            },
        )
        .expect("first subscription starts");
    let second = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("topic-lane-second").expect("valid subscriber ID"),
                second_topic.clone(),
            )
            .with_options(keyed_options()),
            move |_| {
                second_started_tx.send(()).expect("second observer remains alive");
            },
        )
        .expect("second subscription starts");

    bus.publish(keyed_request_for(first_topic, "first-topic-event", "same-key"))
        .expect("publish to first topic");
    first_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first topic handler starts");
    bus.publish(keyed_request_for(second_topic, "second-topic-event", "same-key"))
        .expect("publish to second topic");
    second_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("same key in another topic has an independent lane");

    release_tx.send(()).expect("first topic handler gate remains alive");
    assert_eq!(
        WaitOutcome::Idle,
        bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
            .expect("first topic becomes idle")
    );
    assert_eq!(
        WaitOutcome::Idle,
        bus.wait_for_received_deliveries(&other_topic(), Some(Duration::from_secs(2)))
            .expect("second topic becomes idle")
    );
    first.cancel().expect("first subscription closes");
    second.cancel().expect("second subscription closes");
    bus.shutdown(ShutdownMode::Immediate).expect("bus shuts down");
}

#[test]
fn max_in_flight_capacity_is_shared_across_subscriptions() {
    let (bus, _, _) = create_bus(1, 4);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let first_release = release_rx.clone();
    let second_release = release_rx.clone();
    let first_started_tx = started_tx.clone();
    let second_started_tx = started_tx.clone();
    let first = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("capacity-first").expect("valid subscriber ID"),
                topic(),
            ),
            move |_| {
                first_started_tx.send("first").expect("handler observer remains alive");
                first_release
                    .lock()
                    .expect("release gate lock")
                    .recv()
                    .expect("first handler is released");
            },
        )
        .expect("first subscription starts");
    let second = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("capacity-second").expect("valid subscriber ID"),
                topic(),
            ),
            move |_| {
                second_started_tx
                    .send("second")
                    .expect("handler observer remains alive");
                second_release
                    .lock()
                    .expect("release gate lock")
                    .recv()
                    .expect("second handler is released");
            },
        )
        .expect("second subscription starts");

    bus.publish(request("capacity-event"))
        .expect("publish to both subscriptions");
    let first_started = started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("one subscription obtains the sole in-flight permit");
    assert!(started_rx.recv_timeout(Duration::from_millis(100)).is_err());
    release_tx.send(()).expect("active handler gate remains alive");
    let second_started = started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("the other subscription proceeds after capacity is released");
    assert_ne!(first_started, second_started);
    release_tx.send(()).expect("second handler gate remains alive");

    assert_eq!(
        WaitOutcome::Idle,
        bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
            .unwrap()
    );
    first.cancel().expect("first subscription closes");
    second.cancel().expect("second subscription closes");
    bus.shutdown(ShutdownMode::Immediate).expect("bus shuts down");
}

#[test]
fn graceful_timeout_can_be_recovered_and_aggregates_later_close_failures() {
    let (bus, spi, close_rx) = create_bus_with_close_mode(2, 4, true);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let first_started_tx = started_tx.clone();
    let second_started_tx = started_tx.clone();
    let first_release = release_rx.clone();
    let second_release = release_rx.clone();
    let _first = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("timeout-close-a").expect("valid subscriber ID"),
                topic(),
            ),
            move |_| {
                first_started_tx.send(()).expect("handler observer remains alive");
                first_release
                    .lock()
                    .expect("release gate lock")
                    .recv()
                    .expect("first handler is released");
            },
        )
        .expect("first subscription starts");
    let _second = bus
        .subscribe(
            SubscribeRequest::new(
                SubscriberId::new("timeout-close-b").expect("valid subscriber ID"),
                topic(),
            ),
            move |_| {
                second_started_tx.send(()).expect("handler observer remains alive");
                second_release
                    .lock()
                    .expect("release gate lock")
                    .recv()
                    .expect("second handler is released");
            },
        )
        .expect("second subscription starts");

    bus.publish(request("blocked-event"))
        .expect("publish to both subscriptions");
    for _ in 0..2 {
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("both handlers start before timeout");
    }

    assert_eq!(
        WaitOutcome::TimedOut,
        bus.wait_for_received_deliveries(&topic(), Some(Duration::ZERO))
            .expect("idle wait itself succeeds with a timeout outcome")
    );

    let grace = Duration::from_millis(20);
    let timeout = bus
        .shutdown(ShutdownMode::Graceful { timeout: grace })
        .expect_err("blocked handlers exceed the graceful deadline");
    assert!(matches!(timeout, ShutdownError::TimedOut { timeout } if timeout == grace));
    assert_eq!(0, spi.shutdown_calls(), "provider shutdown waits until workers close");

    release_tx.send(()).expect("release first blocked handler");
    release_tx.send(()).expect("release second blocked handler");
    let mut closed = BTreeMap::<String, usize>::new();
    for _ in 0..2 {
        let subscriber = close_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("both provider receivers attempt close after handler completion");
        *closed.entry(subscriber).or_default() += 1;
    }
    assert_eq!(1, closed.get("timeout-close-a").copied().unwrap_or_default());
    assert_eq!(1, closed.get("timeout-close-b").copied().unwrap_or_default());

    let error = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("recovery shutdown returns the persistent close ledger");
    let ShutdownError::SubscriptionClose(errors) = error else {
        panic!("expected aggregated subscription close errors");
    };
    assert_eq!(2, errors.len());
    let mut subscriber_ids: Vec<_> = errors.iter().map(|failure| failure.subscriber_id().as_str()).collect();
    subscriber_ids.sort_unstable();
    assert_eq!(["timeout-close-a", "timeout-close-b"], subscriber_ids.as_slice());
    assert!(errors.iter().all(|failure| {
        failure.error().provider_id() == PROVIDER_ID && failure.error().operation() == "close_subscription"
    }));
    assert_eq!(1, spi.shutdown_calls());
}
