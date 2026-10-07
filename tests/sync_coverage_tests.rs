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
use std::num::NonZeroUsize;
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
use qubit_event_bus::error::CapabilityError;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::DeliveryError;
use qubit_event_bus::error::LifecycleError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::error::SubscribeError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::IntoHandlerResult;
use qubit_event_bus::facade::WaitOutcome;
use qubit_event_bus::model::DeadLetterAdmissionPolicy;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderMessageMetadata;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::pipeline::Diagnostic;
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
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;

const PROVIDER_ID: &str = "sync-coverage";
const TOPIC_NAME: &str = "sync.coverage";

/// Holds the state shared by the synchronous coverage SPI and its
/// subscriptions.
struct CoverageState {
    receivers: Mutex<Vec<(String, TopicAddress, SyncSender<InboundMessage>)>>,
    close_attempts: mpsc::Sender<String>,
    shutdown_calls: AtomicUsize,
    fail_next_publish: AtomicBool,
    fail_next_subscribe: AtomicBool,
    fail_next_shutdown: AtomicBool,
    close_fails: bool,
    ordering: OrderingCapability,
}

/// Supplies controllable synchronous behavior for facade contract tests.
#[derive(Clone)]
struct CoverageSpi {
    state: Arc<CoverageState>,
}

/// Receives messages and reports close attempts for one test subscription.
struct CoverageSubscription {
    subscriber_id: String,
    receiver: Receiver<InboundMessage>,
    closed: bool,
    close_attempts: mpsc::Sender<String>,
    close_fails: bool,
}

impl CoverageSpi {
    /// Creates a provider with the default per-key ordering capability.
    fn new(close_fails: bool) -> (Self, Receiver<String>) {
        Self::new_with_ordering(close_fails, OrderingCapability::PerKey)
    }

    /// Creates a provider with selected ordering and close-failure behavior.
    fn new_with_ordering(
        close_fails: bool,
        ordering: OrderingCapability,
    ) -> (Self, Receiver<String>) {
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
                    ordering,
                }),
            },
            close_rx,
        )
    }

    /// Returns how many shutdown calls reached the test provider.
    fn shutdown_calls(&self) -> usize {
        self.state.shutdown_calls.load(Ordering::Acquire)
    }

    /// Makes the next publish call return a configured provider error.
    fn fail_next_publish(&self) {
        self.state.fail_next_publish.store(true, Ordering::Release);
    }

    /// Makes the next subscribe call return a configured provider error.
    fn fail_next_subscribe(&self) {
        self.state
            .fail_next_subscribe
            .store(true, Ordering::Release);
    }

    /// Makes the next shutdown call return a configured provider error.
    fn fail_next_shutdown(&self) {
        self.state.fail_next_shutdown.store(true, Ordering::Release);
    }
}

impl EventBusSpi for CoverageSpi {
    /// Reports the native, ephemeral, opaque-result capabilities used by the
    /// tests.
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            self.state.ordering,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    /// Routes native payloads to matching receivers or returns an injected
    /// error.
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

    /// Registers a bounded receiver, unless the next subscription failure is
    /// armed.
    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        if self.state.fail_next_subscribe.swap(false, Ordering::AcqRel) {
            return Err(spi_error("subscribe", "configured_failure"));
        }
        let subscriber_id = request.subscriber_id().as_str().to_owned();
        let (sender, receiver) = mpsc::sync_channel(16);
        self.state
            .receivers
            .lock()
            .expect("receiver registry lock")
            .push((subscriber_id.clone(), request.topic().clone(), sender));
        Ok(Box::new(CoverageSubscription {
            subscriber_id,
            receiver,
            closed: false,
            close_attempts: self.state.close_attempts.clone(),
            close_fails: self.state.close_fails,
        }))
    }

    /// Counts shutdown calls and optionally fails the next one.
    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.state.shutdown_calls.fetch_add(1, Ordering::AcqRel);
        if self.state.fail_next_shutdown.swap(false, Ordering::AcqRel) {
            return Err(spi_error("shutdown", "configured_failure"));
        }
        Ok(ShutdownOutcome::Complete)
    }
}

impl EventSubscriptionSpi for CoverageSubscription {
    /// Receives one queued message or reports timeout and closed states.
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

    /// Always reports settlement as unsupported by this test provider.
    fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        Err(spi_error("settle", "unsupported"))
    }

    /// Marks the receiver closed, records the attempt, and optionally fails it.
    fn close(&mut self) -> Result<(), SpiError> {
        self.closed = true;
        if self
            .close_attempts
            .send(self.subscriber_id.clone())
            .is_err()
        {
            return Ok(());
        }
        if self.close_fails {
            Err(spi_error("close_subscription", "close_failure"))
        } else {
            Ok(())
        }
    }
}

/// Builds a non-retryable provider error tagged with the requested operation
/// and kind.
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

/// Returns the shared valid topic used by synchronous coverage tests.
fn topic() -> Topic<String> {
    Topic::new(TOPIC_NAME).expect("static topic is valid")
}

/// Returns a second topic for cross-topic scheduling tests.
fn other_topic() -> Topic<String> {
    Topic::new("sync.coverage.other").expect("static topic is valid")
}

/// Builds a publish request for the shared topic and a selected ordering key.
fn keyed_request(payload: &str, key: &str) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(topic())
        .payload(payload.to_owned())
        .ordering_key(key)
        .build()
        .expect("valid event request")
}

/// Builds a publish request for an explicit topic and ordering key.
fn keyed_request_for(topic: Topic<String>, payload: &str, key: &str) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(topic)
        .payload(payload.to_owned())
        .ordering_key(key)
        .build()
        .expect("valid event request")
}

/// Builds a plain publish request for the shared topic.
fn request(payload: &str) -> PublishRequest<String> {
    PublishRequest::new(topic(), payload.to_owned()).expect("OS random source is available")
}

/// Builds a facade with explicit running and owned delivery limits.
fn create_bus(
    max_running_handlers: usize,
    max_owned_deliveries: usize,
) -> (EventBus, CoverageSpi, Receiver<String>) {
    create_bus_with_close_mode(max_running_handlers, max_owned_deliveries, false)
}

/// Builds the facade and optionally injects provider close failures.
fn create_bus_with_close_mode(
    max_running_handlers: usize,
    max_owned_deliveries: usize,
    close_fails: bool,
) -> (EventBus, CoverageSpi, Receiver<String>) {
    let (spi, close_rx) = CoverageSpi::new(close_fails);
    let scheduler = DeliverySchedulingConfig::new(
        NonZeroUsize::new(max_running_handlers)
            .expect("test running-handler limit must be positive"),
        NonZeroUsize::new(max_owned_deliveries)
            .expect("test owned-delivery limit must be positive"),
        NonZeroUsize::new(max_owned_deliveries)
            .expect("test reserved-receive limit must be positive"),
        NonZeroUsize::new(2).expect("test per-subscription limit must be positive"),
    )
    .expect("test scheduler limits are valid");
    let config = EventBusFacadeConfig::default().with_delivery_scheduling(scheduler);
    let bus = EventBus::with_config(
        ProviderId::new(PROVIDER_ID).expect("static provider ID is valid"),
        Arc::new(spi.clone()),
        config,
    )
    .expect("valid provider capabilities");
    (bus, spi, close_rx)
}

/// Returns subscription options that exercise per-key delivery ordering.
fn keyed_options() -> SubscribeOptions<String> {
    SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build()
}

#[test]
fn test_facade_config_keeps_the_caller_supplied_codec_registry() {
    let codecs = Arc::new(CodecRegistry::new());
    let config = EventBusFacadeConfig::new().with_codec_registry(Arc::clone(&codecs));

    assert!(Arc::ptr_eq(config.codec_registry(), &codecs));
}

#[test]
fn test_unit_handler_result_is_treated_as_success() {
    assert!(().into_handler_result().is_ok());
}

#[test]
fn test_scheduler_config_rejects_running_capacity_above_owned_capacity() {
    let error = DeliverySchedulingConfig::new(
        NonZeroUsize::new(2).expect("test running-handler limit must be positive"),
        NonZeroUsize::new(1).expect("test owned-delivery limit must be positive"),
        NonZeroUsize::new(1).expect("test reserved-receive limit must be positive"),
        NonZeroUsize::new(1).expect("test per-subscription limit must be positive"),
    )
    .expect_err("running handlers cannot exceed owned delivery capacity");
    assert!(matches!(
        error,
        ConfigurationError::InvalidField {
            field: "max_running_handlers",
            ..
        }
    ));
}

#[test]
fn test_per_key_ordering_is_rejected_when_provider_declares_no_ordering_support() {
    let (spi, _close_rx) = CoverageSpi::new_with_ordering(false, OrderingCapability::None);
    let bus = EventBus::from_spi(
        ProviderId::new(PROVIDER_ID).expect("static provider ID must be valid"),
        Arc::new(spi),
    )
    .expect("provider capabilities are structurally valid");
    let request = SubscribeRequest::new("ordered", topic())
        .expect("test subscriber ID must be valid")
        .with_options(keyed_options());

    assert!(matches!(
        bus.subscribe(request, |_| Ok::<(), DeliveryError>(())),
        Err(SubscribeError::Capability(CapabilityError::Unsupported {
            capability: "ordering.per_key"
        }))
    ));
}

#[test]
fn test_known_destination_dead_letter_policy_is_rejected_for_opaque_publish_results() {
    let (bus, _, _) = create_bus(1, 1);
    let request = SubscribeRequest::new("known-dead-letter", topic())
        .expect("test subscriber ID must be valid")
        .with_options(
            SubscribeOptions::builder()
                .dead_letter(
                    DeadLetterPolicy::with_admission(
                        "sync.coverage.dead-letter",
                        DeadLetterAdmissionPolicy::KnownDestination,
                    )
                    .expect("known-destination dead-letter policy must be valid"),
                )
                .build(),
        );

    assert!(matches!(
        bus.subscribe(request, |_| Ok::<(), DeliveryError>(())),
        Err(SubscribeError::Capability(CapabilityError::Unsupported {
            capability: "dead_letter.known_destination_admission"
        }))
    ));
}

#[test]
fn test_publish_all_keeps_later_results_after_a_provider_failure() {
    let (bus, spi, _) = create_bus(1, 1);
    spi.fail_next_publish();

    let result = bus.publish_all([request("fails"), request("continues")]);

    assert_eq!(2, result.total_count());
    assert_eq!(1, result.accepted_count());
    assert_eq!(1, result.failure_count());
    assert!(matches!(
        result.items()[0]
            .as_ref()
            .expect_err("first publication fails")
            .cause(),
        PublishError::Spi(SpiError::Operation {
            operation: "publish",
            kind: "configured_failure",
            ..
        })
    ));
    assert!(result.items()[1].is_ok());
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down after batch publication");
}

#[test]
fn test_provider_subscribe_failure_does_not_poison_later_subscription() {
    let (bus, spi, _) = create_bus(1, 1);
    spi.fail_next_subscribe();
    let failed_request =
        SubscribeRequest::new("provider-subscribe-failure", topic()).expect("valid subscriber ID");
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
            SubscribeRequest::new("provider-subscribe-recovery", topic())
                .expect("valid subscriber ID"),
            |_| {},
        )
        .expect("a failed admission leaves the facade usable");
    subscription.cancel().expect("successful receiver closes");
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down after subscription recovery");
}

#[test]
fn test_shutdown_provider_error_is_retryable_and_closes_public_admission() {
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
            .outcome
    );
    assert_eq!(2, spi.shutdown_calls());
    assert!(
        matches!(bus.publish(request("after-close")), Err(failure) if matches!(failure.cause(), PublishError::Closed))
    );
    assert!(matches!(
        bus.subscribe(
            SubscribeRequest::new("after-close", topic()).expect("valid subscriber ID"),
            |_| {},
        ),
        Err(SubscribeError::Closed)
    ));
}

#[test]
fn test_shutdown_is_idempotent_and_caches_the_provider_outcome() {
    let (bus, spi, _) = create_bus(1, 1);

    assert_eq!(
        ShutdownOutcome::Complete,
        bus.shutdown(ShutdownMode::Immediate)
            .expect("first shutdown completes")
            .outcome
    );
    assert_eq!(
        ShutdownOutcome::Complete,
        bus.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_millis(10),
        })
        .expect("repeated shutdown returns cached outcome")
        .outcome
    );
    assert_eq!(1, spi.shutdown_calls());
}

#[test]
fn test_callback_reentrant_wait_and_shutdown_return_would_deadlock() {
    let (bus, _, _) = create_bus(1, 1);
    let callback_bus = bus.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("reentrant-lifecycle", topic()).expect("valid subscriber ID"),
            move |_| {
                let wait = callback_bus
                    .wait_for_received_deliveries(&topic(), Some(Duration::from_secs(1)));
                let shutdown = callback_bus.shutdown(ShutdownMode::Immediate);
                result_tx
                    .send((wait, shutdown))
                    .expect("test remains available to receive callback result");
            },
        )
        .expect("subscription starts");

    let _ = bus
        .publish(request("reentrant"))
        .expect("publish reaches callback");
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

    subscription
        .cancel()
        .expect("external caller cancels worker");
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down after callback exits");
}

#[test]
fn test_dropping_diagnostic_handle_stops_future_close_failure_notifications() {
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
            SubscribeRequest::new("observed-close-error", topic()).expect("valid subscriber ID"),
            |_| {},
        )
        .expect("first subscription starts");
    let error = first
        .cancel()
        .expect_err("configured provider close error is surfaced");
    assert!(matches!(error, LifecycleError::SubscriptionClose(_)));
    assert_eq!(1, observed.load(Ordering::Acquire));

    drop(observer);
    let second = bus
        .subscribe(
            SubscribeRequest::new("unobserved-close-error", topic()).expect("valid subscriber ID"),
            |_| {},
        )
        .expect("second subscription starts");
    assert!(matches!(
        second.cancel(),
        Err(LifecycleError::SubscriptionClose(_))
    ));
    assert_eq!(1, observed.load(Ordering::Acquire));
    assert!(matches!(
        bus.shutdown(ShutdownMode::Immediate),
        Err(ShutdownError::SubscriptionClose(_))
    ));
}

#[test]
fn test_same_ordering_key_is_independent_between_subscriptions() {
    let (bus, _, _) = create_bus(2, 4);
    let (first_started_tx, first_started_rx) = mpsc::channel();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let first_release = release_rx.clone();
    let first = bus
        .subscribe(
            SubscribeRequest::new("lane-first", topic())
                .expect("valid subscriber ID")
                .with_options(keyed_options()),
            move |_| {
                first_started_tx
                    .send(())
                    .expect("first observer remains alive");
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
            SubscribeRequest::new("lane-second", topic())
                .expect("valid subscriber ID")
                .with_options(keyed_options()),
            move |_| {
                second_started_tx
                    .send(())
                    .expect("second observer remains alive")
            },
        )
        .expect("second subscription starts");

    let _ = bus
        .publish(keyed_request("shared-key-event", "same-key"))
        .expect("publish to both subscriptions");
    first_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first subscription starts its handler");
    second_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("same key in another subscription is a separate lane");
    release_tx
        .send(())
        .expect("first handler gate remains alive");

    assert_eq!(
        WaitOutcome::Idle,
        bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
            .expect("all deliveries for the shared topic should settle")
    );
    first.cancel().expect("first subscription closes");
    second.cancel().expect("second subscription closes");
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down");
}

#[test]
fn test_same_ordering_key_is_independent_between_topics() {
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
            SubscribeRequest::new("topic-lane-first", first_topic.clone())
                .expect("valid subscriber ID")
                .with_options(keyed_options()),
            move |_| {
                first_started_tx
                    .send(())
                    .expect("first observer remains alive");
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
            SubscribeRequest::new("topic-lane-second", second_topic.clone())
                .expect("valid subscriber ID")
                .with_options(keyed_options()),
            move |_| {
                second_started_tx
                    .send(())
                    .expect("second observer remains alive");
            },
        )
        .expect("second subscription starts");

    let _ = bus
        .publish(keyed_request_for(
            first_topic,
            "first-topic-event",
            "same-key",
        ))
        .expect("publish to first topic");
    first_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first topic handler starts");
    let _ = bus
        .publish(keyed_request_for(
            second_topic,
            "second-topic-event",
            "same-key",
        ))
        .expect("publish to second topic");
    second_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("same key in another topic has an independent lane");

    release_tx
        .send(())
        .expect("first topic handler gate remains alive");
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
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down");
}

#[test]
fn test_running_and_owned_capacity_are_shared_across_subscriptions() {
    let (bus, _, _) = create_bus(1, 1);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let first_release = release_rx.clone();
    let second_release = release_rx.clone();
    let first_started_tx = started_tx.clone();
    let second_started_tx = started_tx.clone();
    let first = bus
        .subscribe(
            SubscribeRequest::new("capacity-first", topic()).expect("valid subscriber ID"),
            move |_| {
                first_started_tx
                    .send("first")
                    .expect("handler observer remains alive");
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
            SubscribeRequest::new("capacity-second", topic()).expect("valid subscriber ID"),
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

    let _ = bus
        .publish(request("capacity-event"))
        .expect("publish to both subscriptions");
    let first_started = started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("one subscription obtains the sole in-flight permit");
    assert!(started_rx.recv_timeout(Duration::from_millis(100)).is_err());
    let metrics = bus.delivery_metrics();
    assert_eq!(metrics.running_handlers, 1);
    assert_eq!(
        metrics.reserved_receives + metrics.queued + metrics.running_handlers + metrics.settling,
        1
    );
    release_tx
        .send(())
        .expect("active handler gate remains alive");
    let second_started = started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("the other subscription proceeds after capacity is released");
    assert_ne!(first_started, second_started);
    release_tx
        .send(())
        .expect("second handler gate remains alive");

    assert_eq!(
        WaitOutcome::Idle,
        bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
            .expect("all deliveries for the shared topic should settle")
    );
    first.cancel().expect("first subscription closes");
    second.cancel().expect("second subscription closes");
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down");
}

#[test]
fn test_graceful_timeout_can_be_recovered_and_aggregates_later_close_failures() {
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
            SubscribeRequest::new("timeout-close-a", topic()).expect("valid subscriber ID"),
            move |_| {
                first_started_tx
                    .send(())
                    .expect("handler observer remains alive");
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
            SubscribeRequest::new("timeout-close-b", topic()).expect("valid subscriber ID"),
            move |_| {
                second_started_tx
                    .send(())
                    .expect("handler observer remains alive");
                second_release
                    .lock()
                    .expect("release gate lock")
                    .recv()
                    .expect("second handler is released");
            },
        )
        .expect("second subscription starts");

    let _ = bus
        .publish(request("blocked-event"))
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
    assert_eq!(
        0,
        spi.shutdown_calls(),
        "provider shutdown waits until workers close"
    );

    release_tx.send(()).expect("release first blocked handler");
    release_tx.send(()).expect("release second blocked handler");
    let mut closed = BTreeMap::<String, usize>::new();
    for _ in 0..2 {
        let subscriber = close_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("both provider receivers attempt close after handler completion");
        *closed.entry(subscriber).or_default() += 1;
    }
    assert_eq!(
        1,
        closed.get("timeout-close-a").copied().unwrap_or_default()
    );
    assert_eq!(
        1,
        closed.get("timeout-close-b").copied().unwrap_or_default()
    );

    let error = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("recovery shutdown returns the persistent close ledger");
    let ShutdownError::SubscriptionClose(errors) = error else {
        panic!("expected aggregated subscription close errors");
    };
    assert_eq!(2, errors.len());
    let mut subscriber_ids: Vec<_> = errors
        .iter()
        .map(|failure| failure.subscriber_id().as_str())
        .collect();
    subscriber_ids.sort_unstable();
    assert_eq!(
        ["timeout-close-a", "timeout-close-b"],
        subscriber_ids.as_slice()
    );
    assert!(errors.iter().all(|failure| {
        failure.error().provider_id() == PROVIDER_ID
            && failure.error().operation() == "close_subscription"
    }));
    assert_eq!(1, spi.shutdown_calls());
}
