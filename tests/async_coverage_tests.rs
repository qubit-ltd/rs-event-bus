// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Additional asynchronous facade lifecycle coverage.

mod support;

use std::collections::VecDeque;
use std::future::Future;
use std::future::pending as pending_future;
use std::future::poll_fn;
use std::io::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::StdMonotonicClock;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::PublishError;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::SpiError;
use qubit_event_bus::SubscribeError;
use qubit_event_bus::SubscriberId;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::facade::PublishMetricsSnapshot;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DEAD_LETTER_HEADER;
use qubit_event_bus::model::DEAD_LETTER_HEADER_VALUE;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::PublishMetadata;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SettlementTermination;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusCapabilities;
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
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_retry::RetryPolicy;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::fake_spi::full_capabilities;
use crate::support::fake_spi::inbound_message;
use crate::support::manual_async::block_on;
use crate::support::manual_async::poll_once;

/// A timer double that forces timer registration to fail with instant overflow.
struct FailingTimer {
    clock: StdMonotonicClock,
}

impl Timer for FailingTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    fn at(&self, _: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        Err(TimeError::InstantOverflow)
    }
}

/// Builds the shared topic used by asynchronous coverage tests.
fn topic() -> Topic<u32> {
    Topic::new("async.coverage").expect("valid test topic")
}

/// Configurable publisher double for retry, acknowledgement, codec, and metrics
/// tests.
struct PublisherCoverageSpi {
    payload_modes: PayloadModes,
    retryable_failures: usize,
    terminal_failure: bool,
    acknowledgement: Option<PublishAcknowledgement>,
    pending: bool,
    attempts: AtomicUsize,
    payload_was_encoded: Mutex<Vec<bool>>,
}

impl PublisherCoverageSpi {
    /// Creates a publisher double with the requested payload mode and failure
    /// behavior.
    fn new(payload_modes: PayloadModes, retryable_failures: usize, terminal_failure: bool) -> Self {
        Self {
            payload_modes,
            retryable_failures,
            terminal_failure,
            acknowledgement: None,
            pending: false,
            attempts: AtomicUsize::new(0),
            payload_was_encoded: Mutex::new(Vec::new()),
        }
    }

    /// Makes successful publishes return the supplied acknowledgement.
    fn with_acknowledgement(mut self, acknowledgement: PublishAcknowledgement) -> Self {
        self.acknowledgement = Some(acknowledgement);
        self
    }

    /// Leaves publish futures pending to exercise cancellation behavior.
    fn with_pending(mut self) -> Self {
        self.pending = true;
        self
    }
}

impl AsyncEventBusSpi for PublisherCoverageSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            self.payload_modes,
            SettlementCapabilities::None,
            OrderingCapability::PerKey,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        let attempt = self.attempts.fetch_add(1, Ordering::AcqRel);
        self.payload_was_encoded
            .lock()
            .expect("test state mutex must not be poisoned")
            .push(matches!(message.payload(), TransportPayload::Encoded(_)));
        let should_fail = attempt < self.retryable_failures || self.terminal_failure;
        let acknowledgement = self.acknowledgement.clone();
        let pending = self.pending;
        Box::pin(async move {
            if pending {
                pending_future::<Result<PublishAcknowledgement, SpiError>>().await
            } else if should_fail {
                Err(SpiError::Publish {
                    provider_id: "async-publisher-coverage".into(),
                    resource: None,
                    kind: "injected_publish_failure",
                    retryable: Some(attempt < self.retryable_failures),
                    effect: PublishEffect::NotAccepted,
                    source: Box::new(std::io::Error::other("injected async provider failure")),
                })
            } else {
                Ok(acknowledgement.unwrap_or(PublishAcknowledgement::Accepted {
                    provider_message_id: None,
                    metadata: Default::default(),
                }))
            }
        })
    }

    fn subscribe<'a>(
        &'a self,
        _request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async { unreachable!("publisher coverage SPI is not used for subscriptions") })
    }

    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

/// String codec double that can inject an encode failure.
struct StringCodec {
    content_type: ContentType,
    fail_encode: bool,
}

impl EventCodec<String> for StringCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        if self.fail_encode {
            Err(CodecError::Encode {
                source: Box::new(Error::other("injected async codec failure")),
            })
        } else {
            Ok(Arc::from(value.as_bytes()))
        }
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        let bytes = payload.bytes();
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

#[test]
fn test_async_publisher_retries_a_retryable_failure_then_succeeds() {
    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 1, false));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("async-publisher-retry").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let request = PublishRequest::builder()
        .topic(topic())
        .payload(17_u32)
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .build()
                .expect("retry policy must be valid"),
        )
        .build()
        .expect("test publish request must be valid");

    let receipt = block_on(bus.publish(request)).expect("second async attempt should succeed");
    assert_eq!(receipt.input_event_id().as_str().len(), 36);
    assert_eq!(spi.attempts.load(Ordering::Acquire), 2);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
}

#[test]
fn test_async_publisher_metrics_track_shared_attempts_and_batch_items() {
    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("async-publisher-metrics").expect("static test provider ID must be valid"),
        spi,
    )
    .expect("valid provider capabilities");
    assert_eq!(bus.publish_metrics(), PublishMetricsSnapshot::default());
    let clone = bus.clone();
    let requests = [
        PublishRequest::builder()
            .topic(topic())
            .payload(1_u32)
            .build()
            .expect("test publish request must be valid"),
        PublishRequest::builder()
            .topic(topic())
            .payload(2_u32)
            .build()
            .expect("test publish request must be valid"),
    ];
    let batch = block_on(clone.publish_all(requests));
    assert_eq!(batch.total_count(), 2);
    assert!(batch.items().iter().all(Result::is_ok));
    assert_eq!(bus.publish_metrics().attempts, 2);
    assert_eq!(bus.publish_metrics().opaque_accepted, 2);

    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
    let closed = block_on(
        bus.publish(
            PublishRequest::builder()
                .topic(topic())
                .payload(3_u32)
                .build()
                .expect("test publish request must be valid"),
        ),
    );
    assert!(matches!(closed, Err(failure) if matches!(failure.cause(), PublishError::Closed)));
    assert_eq!(clone.publish_metrics().attempts, 3);
    assert_eq!(clone.publish_metrics().errors, 1);

    let dropped_bus = AsyncEventBus::with_config(
        ProviderId::new("async-publisher-metrics-dropped").expect("static test provider ID must be valid"),
        Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false)),
        EventBusFacadeConfig::new().publisher_interceptor(|_| Ok(false)),
    )
    .expect("publisher interceptor configuration must be valid");
    let _ = block_on(
        dropped_bus.publish(
            PublishRequest::builder()
                .topic(topic())
                .payload(4_u32)
                .build()
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publisher interceptor must allow this request");
    let dropped = dropped_bus.publish_metrics();
    assert_eq!(dropped.attempts, 1);
    assert_eq!(dropped.dropped, 1);
    assert_eq!(dropped.errors, 0);

    let mixed_ack = PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(
            Id::new(10),
            SubscriberId::new("accepted-async-metrics").expect("static subscriber ID must be valid"),
            AdmissionStatus::Accepted,
        ),
        DestinationAdmission::new(
            Id::new(11),
            SubscriberId::new("filtered-async-metrics").expect("static subscriber ID must be valid"),
            AdmissionStatus::Filtered,
        ),
        DestinationAdmission::new(
            Id::new(12),
            SubscriberId::new("rejected-async-metrics").expect("static subscriber ID must be valid"),
            AdmissionStatus::Rejected("injected rejection".into()),
        ),
    ]);
    let destination_bus = AsyncEventBus::from_spi(
        ProviderId::new("async-publisher-metrics-destinations").expect("static test provider ID must be valid"),
        Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false).with_acknowledgement(mixed_ack)),
    )
    .expect("valid provider capabilities");
    let _ = block_on(
        destination_bus.publish(
            PublishRequest::builder()
                .topic(topic())
                .payload(5_u32)
                .build()
                .expect("test publish request must be valid"),
        ),
    )
    .expect("destination admission acknowledgement must be returned");
    let destination_metrics = destination_bus.publish_metrics();
    assert_eq!(destination_metrics.accepted_destinations, 1);
    assert_eq!(destination_metrics.filtered_destinations, 1);
    assert_eq!(destination_metrics.rejected_destinations, 1);

    let empty_bus = AsyncEventBus::from_spi(
        ProviderId::new("async-publisher-metrics-empty").expect("static test provider ID must be valid"),
        Arc::new(
            PublisherCoverageSpi::new(PayloadModes::Native, 0, false)
                .with_acknowledgement(PublishAcknowledgement::DestinationAdmissions(Vec::new())),
        ),
    )
    .expect("valid provider capabilities");
    let _ = block_on(
        empty_bus.publish(
            PublishRequest::builder()
                .topic(topic())
                .payload(6_u32)
                .build()
                .expect("test publish request must be valid"),
        ),
    )
    .expect("empty destination acknowledgement must be returned");
    assert_eq!(empty_bus.publish_metrics().zero_destinations, 1);

    let failing_bus = AsyncEventBus::from_spi(
        ProviderId::new("async-publisher-metrics-error").expect("static test provider ID must be valid"),
        Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, true)),
    )
    .expect("valid provider capabilities");
    assert!(
        block_on(
            failing_bus.publish(
                PublishRequest::builder()
                    .topic(topic())
                    .payload(7_u32)
                    .build()
                    .expect("test publish request must be valid")
            )
        )
        .is_err()
    );
    let failure_metrics = failing_bus.publish_metrics();
    assert_eq!(failure_metrics.attempts, 1);
    assert_eq!(failure_metrics.errors, 1);

    let concurrent_bus = AsyncEventBus::from_spi(
        ProviderId::new("async-publisher-metrics-concurrent").expect("static test provider ID must be valid"),
        Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false)),
    )
    .expect("valid provider capabilities");
    let workers = (0..8)
        .map(|index| {
            let worker_bus = concurrent_bus.clone();
            std::thread::spawn(move || {
                let _ = block_on(
                    worker_bus.publish(
                        PublishRequest::builder()
                            .topic(topic())
                            .payload(index)
                            .build()
                            .expect("test publish request must be valid"),
                    ),
                )
                .expect("concurrent publish should be accepted");
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().expect("async publisher thread should finish");
    }
    assert_eq!(concurrent_bus.publish_metrics().attempts, 8);
    assert_eq!(concurrent_bus.publish_metrics().opaque_accepted, 8);
}

#[test]
fn test_async_publisher_metrics_count_polled_attempt_even_if_future_is_cancelled() {
    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false).with_pending());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("async-publisher-metrics-cancelled").expect("static test provider ID must be valid"),
        spi,
    )
    .expect("valid provider capabilities");
    let mut publish = Box::pin(
        bus.publish(
            PublishRequest::builder()
                .topic(topic())
                .payload(8_u32)
                .build()
                .expect("test publish request must be valid"),
        ),
    );
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(publish.as_mut().poll(&mut context), Poll::Pending));
    let pending_metrics = bus.publish_metrics();
    assert_eq!(pending_metrics.attempts, 1);
    assert_eq!(pending_metrics.errors, 0);
    assert_eq!(pending_metrics.opaque_accepted, 0);

    drop(publish);
    assert_eq!(bus.publish_metrics().attempts, 1);
}

#[test]
fn test_async_global_publisher_interceptor_edits_only_validated_headers() {
    let config = EventBusFacadeConfig::new().publisher_interceptor(|metadata: &mut PublishMetadata| {
        assert_eq!(
            metadata.headers().get("origin").map(String::as_str),
            Some("application")
        );
        assert_eq!(metadata.header("trace"), None);
        metadata.set_header("trace", "global-1")?;
        assert!(metadata.set_header("bad key", "value").is_err());
        assert_eq!(metadata.remove_header("origin").as_deref(), Some("application"));
        assert!(metadata.remove_header(DEAD_LETTER_HEADER).is_none());
        Ok(true)
    });
    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false));
    let bus = AsyncEventBus::with_config(
        ProviderId::new("async-global-interceptor").expect("static test provider ID must be valid"),
        spi.clone(),
        config,
    )
    .expect("valid provider capabilities");
    let request = PublishRequest::builder()
        .topic(topic())
        .payload(18_u32)
        .header("origin", "application")
        .build()
        .expect("test publish request must be valid");

    let _ = block_on(bus.publish(request)).expect("global interceptor allows publish");
    assert_eq!(spi.attempts.load(Ordering::Acquire), 1);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
}

#[test]
fn test_async_string_delivery_runs_error_handler_and_terminates_failure() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("async-string-delivery").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let request = SubscribeRequest::new(
        "string-worker",
        Topic::<String>::new("async.string").expect("static test topic must be valid"),
    )
    .expect("valid subscriber ID")
    .with_options(
        SubscribeOptions::builder()
            .error_handler(|_, _| FailureDirective::Discard)
            .build(),
    );
    let mut subscription = block_on(bus.subscribe(request)).expect("async subscription must start");
    spi.enqueue(InboundMessage::new(
        TopicAddress::new("async.string").expect("static SPI topic address must be valid"),
        EventId::new("async-string-event").expect("static event ID must be valid"),
        std::time::SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        TransportPayload::Native(Arc::new(String::from("body"))),
        None,
        Default::default(),
    ));
    let (handled_tx, handled_rx) = mpsc::channel();
    let runner = spawn(move || {
        block_on(subscription.run(move |delivery| {
            let handled = handled_tx.clone();
            async move {
                handled
                    .send(delivery.payload().clone())
                    .expect("test observer receiver must remain connected");
                Err(DeliveryError::Handler {
                    source: Box::new(Error::other("expected handler failure")),
                })
            }
        }))
    });

    assert_eq!(
        handled_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("expected test signal must arrive before timeout"),
        "body"
    );
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
    runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish cleanly");
}

#[test]
fn test_failed_timer_registration_surfaces_after_a_failed_settlement() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let timer = Arc::new(FailingTimer {
        clock: StdMonotonicClock::new(),
    });
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        spi.clone(),
        timer,
    )
    .expect("valid provider capabilities");
    let request = SubscribeRequest::new("timer-failure", topic()).expect("valid subscriber ID");

    let error = block_on(async {
        let mut subscription = bus.subscribe(request).await.expect("async subscription must start");
        spi.fail_next_settle();
        spi.enqueue(inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "timer-failure-token",
        ))));
        let result = subscription.run(|_| async { Ok(()) }).await;
        let _ = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("event bus shutdown must complete");
        result
    })
    .expect_err("settlement retry cannot continue without timer registration");

    let ReceiveError::Stopped(reason) = error else {
        panic!("settlement timer failure should be retained as the first stop cause");
    };
    assert!(matches!(
        reason.as_ref(),
        SubscriptionStopReason::Settlement {
            termination: SettlementTermination::InfrastructureFailure,
            error,
            ..
        } if std::error::Error::source(error.as_ref()).is_some_and(|source| {
            matches!(source.downcast_ref::<TimeError>(), Some(TimeError::InstantOverflow))
        })
    ));
    assert_eq!(
        1,
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "settle")
            .count()
    );
}

#[test]
fn test_graceful_shutdown_timer_registration_failure_leaves_bus_available_for_immediate_shutdown() {
    let spi = Arc::new(crate::support::fake_spi::FakeAsyncEventBusSpi::new());
    let timer = Arc::new(FailingTimer {
        clock: StdMonotonicClock::new(),
    });
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), timer)
        .expect("valid provider capabilities");

    let error = block_on(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(1),
    }))
    .expect_err("failed graceful deadline registration must be returned");
    assert!(matches!(
        error,
        ShutdownError::Lifecycle(LifecycleError::Timer(TimeError::InstantOverflow))
    ));
    assert!(
        spi.operation_log().is_empty(),
        "failed registration must not start provider shutdown"
    );
    assert_eq!(spi.shutdown_transition_count(), 0);

    let report = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
    assert_eq!(spi.operation_log(), ["shutdown"]);
    assert_eq!(spi.shutdown_transition_count(), 1);
}

#[test]
fn test_async_encoded_publisher_sends_encoded_payload_and_skips_spi_on_codec_failure() {
    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Encoded, 0, false));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("async-encoded-publisher").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let encoded_topic = Topic::new("async.encoded")
        .expect("static codec content type must be valid")
        .with_codec(StringCodec {
            content_type: ContentType::new("text/plain").expect("static codec content type must be valid"),
            fail_encode: false,
        });

    let _ = block_on(bus.publish(
        PublishRequest::new(encoded_topic, "wire payload".to_owned()).expect("test publish request must be valid"),
    ))
    .expect("codec should encode successfully");
    assert_eq!(
        *spi.payload_was_encoded
            .lock()
            .expect("test state mutex must not be poisoned"),
        [true]
    );
    assert_eq!(spi.attempts.load(Ordering::Acquire), 1);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");

    let failing_spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Encoded, 0, false));
    let failing_bus = AsyncEventBus::from_spi(
        ProviderId::new("async-codec-failure").expect("static test provider ID must be valid"),
        failing_spi.clone(),
    )
    .expect("valid provider capabilities");
    let failing_topic = Topic::new("async.codec.failure")
        .expect("static codec content type must be valid")
        .with_codec(StringCodec {
            content_type: ContentType::new("text/plain").expect("static codec content type must be valid"),
            fail_encode: true,
        });
    let error = block_on(failing_bus.publish(
        PublishRequest::new(failing_topic, "cannot encode".to_owned()).expect("test publish request must be valid"),
    ))
    .expect_err("codec failure must be returned as a publish error");
    assert!(matches!(error.cause(), PublishError::Codec(CodecError::Encode { .. })));
    assert_eq!(failing_spi.attempts.load(Ordering::Acquire), 0);
    let _ = block_on(failing_bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
}

#[test]
fn test_async_terminal_failure_handler_reads_non_clone_payload_and_ordering_metadata() {
    /// Payload used to prove failure observers do not require cloning the
    /// value.
    struct NonClonePayload {
        value: String,
    }

    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, true));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("async-terminal-observer").expect("static test provider ID must be valid"),
        spi,
    )
    .expect("valid provider capabilities");
    let observed = Arc::new(Mutex::new(None));
    let observed_by_handler = observed.clone();
    let request = PublishRequest::builder()
        .topic(Topic::new("async.terminal.observer").expect("static test topic must be valid"))
        .payload(NonClonePayload {
            value: "preserved payload".to_owned(),
        })
        .header("trace", "trace-async-1")
        .ordering_key("account-async-1")
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(1)
                .build()
                .expect("retry policy must be valid"),
        )
        .error_handler(move |context, _| {
            *observed_by_handler
                .lock()
                .expect("test state mutex must not be poisoned") = Some((
                context.payload().value.clone(),
                context
                    .header("trace")
                    .expect("publish context must retain the trace header")
                    .to_owned(),
                context
                    .ordering_key()
                    .expect("publish context must retain the ordering key")
                    .to_owned(),
            ));
        })
        .build()
        .expect("test publish request must be valid");

    let error = block_on(bus.publish(request)).expect_err("terminal provider failure must be returned");
    assert!(
        matches!(error.cause(), PublishError::Retry(_)),
        "unexpected publish error: {error:?}"
    );
    assert_eq!(
        *observed.lock().expect("test state mutex must not be poisoned"),
        Some((
            "preserved payload".to_owned(),
            "trace-async-1".to_owned(),
            "account-async-1".to_owned(),
        ))
    );
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
}

#[derive(Default)]
/// Provider double whose first receiver close fails and later attempts succeed.
struct CloseFailingSpi {
    receives: Arc<AtomicUsize>,
    close_attempts: Arc<AtomicUsize>,
    receiver_drops: Arc<AtomicUsize>,
    receive_future_drops: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for CloseFailingSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        full_capabilities()
    }

    fn publish<'a>(&'a self, _message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async {
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            })
        })
    }

    fn subscribe<'a>(
        &'a self,
        _request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let receives = self.receives.clone();
        let close_attempts = self.close_attempts.clone();
        let receiver_drops = self.receiver_drops.clone();
        let receive_future_drops = self.receive_future_drops.clone();
        Box::pin(async move {
            Ok(Box::new(CloseFailingReceiver {
                receives,
                close_attempts: AtomicUsize::new(0),
                total_close_attempts: close_attempts,
                receiver_drops,
                receive_future_drops,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

/// Receiver double that tracks cancellation, close retries, and destruction.
struct CloseFailingReceiver {
    receives: Arc<AtomicUsize>,
    close_attempts: AtomicUsize,
    total_close_attempts: Arc<AtomicUsize>,
    receiver_drops: Arc<AtomicUsize>,
    receive_future_drops: Arc<AtomicUsize>,
}

impl AsyncEventSubscriptionSpi for CloseFailingReceiver {
    fn receive<'a>(&'a mut self, _timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.receives.fetch_add(1, Ordering::AcqRel);
        Box::pin(CancellableReceive {
            dropped: self.receive_future_drops.clone(),
        })
    }

    fn settle<'a>(
        &'a mut self,
        _token: &SettlementToken,
        _disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async { Ok(()) })
    }

    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.total_close_attempts.fetch_add(1, Ordering::AcqRel);
        let attempt = self.close_attempts.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            if attempt == 0 {
                Err(SpiError::Operation {
                    provider_id: "close-failing".into(),
                    operation: "close",
                    resource: None,
                    kind: "close_failed",
                    retryable: Some(false),
                    source: Box::new(Error::other("test close failure")),
                })
            } else {
                Ok(())
            }
        })
    }
}

/// Pending receive future that records when shutdown cancels it.
struct CancellableReceive {
    dropped: Arc<AtomicUsize>,
}

impl Future for CancellableReceive {
    type Output = Result<ReceiveOutcome, SpiError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for CancellableReceive {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}

impl Drop for CloseFailingReceiver {
    fn drop(&mut self) {
        self.receiver_drops.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn test_explicit_subscription_close_returns_a_single_close_failure() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("close-failing").expect("static test provider ID must be valid"),
        spi,
    )
    .expect("valid provider capabilities");

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new("single-close-failure", topic()).expect("valid subscriber ID"))
            .await
            .expect("async subscription must be created");
        let error = subscription
            .close()
            .await
            .expect_err("first close attempt must report the injected failure");
        let LifecycleError::SubscriptionClose(errors) = error else {
            panic!("close failure should be reported through the close-error aggregate");
        };
        assert_eq!(errors.len(), 1);
        let failure = errors.iter().next().expect("one close failure");
        assert_eq!(failure.subscriber_id().as_str(), "single-close-failure");
        assert_eq!(failure.error().kind(), "close_failed");
        assert!(matches!(
            bus.shutdown(ShutdownMode::Immediate).await,
            Err(ShutdownError::SubscriptionClose(errors)) if errors.len() == 1
        ));
    });
}

#[test]
fn test_shutdown_aggregates_multiple_async_subscription_close_failures() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("close-failing").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let mut runners = Vec::new();

    block_on(async {
        for name in ["close-failure-a", "close-failure-b"] {
            let mut subscription = bus
                .subscribe(SubscribeRequest::new(name, topic()).expect("valid subscriber ID"))
                .await
                .expect("async subscription must be created");
            runners.push(std::thread::spawn(move || {
                block_on(subscription.run(|_| async { Ok(()) }))
            }));
        }
    });

    for _ in 0..1_000 {
        if spi.receives.load(Ordering::Acquire) == 2 {
            break;
        }
        sleep(Duration::from_millis(10));
    }
    assert_eq!(spi.receives.load(Ordering::Acquire), 2);

    let shutdown = block_on(bus.shutdown(ShutdownMode::Immediate));
    for runner in runners {
        assert!(
            runner
                .join()
                .expect("subscription runner thread must not panic")
                .is_err(),
            "run reports its close error"
        );
    }
    let Err(ShutdownError::SubscriptionClose(errors)) = shutdown else {
        panic!("shutdown must aggregate all subscription close failures");
    };
    assert_eq!(errors.len(), 2);
    let subscriber_ids: Vec<_> = errors.iter().map(|failure| failure.subscriber_id().as_str()).collect();
    assert!(subscriber_ids.contains(&"close-failure-a"));
    assert!(subscriber_ids.contains(&"close-failure-b"));
}

#[test]
fn test_publish_and_subscribe_are_rejected_after_async_shutdown() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        spi,
    )
    .expect("valid provider capabilities");

    block_on(async {
        let _ = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("event bus shutdown must complete");
        let publish_error = bus
            .publish(PublishRequest::new(topic(), 7).expect("test publish request must be valid"))
            .await
            .expect_err("publish after shutdown must be rejected");
        assert!(matches!(publish_error.cause(), PublishError::Closed));
        let subscribe_result = bus
            .subscribe(SubscribeRequest::new("after-shutdown", topic()).expect("valid subscriber ID"))
            .await;
        let subscribe_error = match subscribe_result {
            Ok(_) => panic!("subscribe after shutdown must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(subscribe_error, SubscribeError::Closed));
    });
}

#[test]
fn test_failed_receiver_close_can_be_retried_and_drop_releases_the_receiver() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("close-failing").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new("close-retry", topic()).expect("valid subscriber ID"))
            .await
            .expect("async subscription must be created");
        assert!(matches!(
            subscription.close().await,
            Err(LifecycleError::SubscriptionClose(_))
        ));
        subscription.close().await.expect("second close succeeds");
        assert_eq!(spi.close_attempts.load(Ordering::Acquire), 2);
        drop(subscription);
        assert_eq!(spi.receiver_drops.load(Ordering::Acquire), 1);
        assert!(matches!(
            bus.shutdown(ShutdownMode::Immediate).await,
            Err(ShutdownError::SubscriptionClose(errors)) if errors.len() == 1
        ));
    });
}

#[test]
fn test_dropping_unrun_subscription_releases_receiver_without_async_close() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("close-failing").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("drop-unrun", topic()).expect("valid subscriber ID"))
            .await
            .expect("async subscription must be created");
        drop(subscription);
        assert_eq!(spi.close_attempts.load(Ordering::Acquire), 0);
        assert_eq!(spi.receiver_drops.load(Ordering::Acquire), 1);
        let _ = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("event bus shutdown must complete");
        assert_eq!(spi.close_attempts.load(Ordering::Acquire), 0);
    });
}

#[test]
fn test_shutdown_cancels_pending_receive_before_closing_receiver() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("close-failing").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("cancel-receive", topic()).expect("valid subscriber ID")))
            .expect("async subscription must be created");
    let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
    for _ in 0..100 {
        if spi.receives.load(Ordering::Acquire) != 0 {
            break;
        }
        sleep(Duration::from_millis(2));
    }
    assert_eq!(spi.receives.load(Ordering::Acquire), 1);

    let shutdown = block_on(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(shutdown, Err(ShutdownError::SubscriptionClose(_))));
    assert!(
        runner
            .join()
            .expect("subscription runner thread must not panic")
            .is_err()
    );
    assert_eq!(spi.receive_future_drops.load(Ordering::Acquire), 1);
    assert!(
        (1..=2).contains(&spi.close_attempts.load(Ordering::Acquire)),
        "the runner and shutdown may race to make the first safe close retry"
    );
}

#[test]
fn test_close_during_shutdown_is_a_noop_for_a_nonrunning_subscription() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let handler_started = Arc::new(AtomicBool::new(false));
    let release_handler = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(Mutex::new(None::<Waker>));

    let (mut active_subscription, mut idle_subscription) = block_on(async {
        let active = bus
            .subscribe(SubscribeRequest::new("active-during-close", topic()).expect("valid subscriber ID"))
            .await
            .expect("async subscription must be created");
        let idle = bus
            .subscribe(SubscribeRequest::new("idle-during-close", topic()).expect("valid subscriber ID"))
            .await
            .expect("async subscription must be created");
        (active, idle)
    });
    assert_eq!(idle_subscription.subscriber_id().as_str(), "idle-during-close");
    assert!(idle_subscription.id().value() > 0);
    spi.enqueue(inbound_message(Some(SettlementToken::new(
        active_subscription.id(),
        "shutdown-close-branch",
    ))));

    let started_by_handler = handler_started.clone();
    let release_by_handler = release_handler.clone();
    let waker_by_handler = handler_waker.clone();
    let runner = spawn(move || {
        block_on(active_subscription.run(move |_| {
            let started = started_by_handler.clone();
            let release = release_by_handler.clone();
            let waker = waker_by_handler.clone();
            async move {
                started.store(true, Ordering::Release);
                poll_fn(move |cx| {
                    if release.load(Ordering::Acquire) {
                        Poll::Ready(Ok(()))
                    } else {
                        *waker.lock().expect("test state mutex must not be poisoned") = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await
            }
        }))
    });
    for _ in 0..100 {
        if handler_started.load(Ordering::Acquire) {
            break;
        }
        sleep(Duration::from_millis(2));
    }
    assert!(handler_started.load(Ordering::Acquire));

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(poll_once(shutdown.as_mut()), Poll::Pending));
    let closes_before = spi
        .operation_log()
        .iter()
        .filter(|operation| **operation == "close")
        .count();
    block_on(idle_subscription.close()).expect("close after shutdown starts is a no-op");
    let closes_after = spi
        .operation_log()
        .iter()
        .filter(|operation| **operation == "close")
        .count();
    assert_eq!(closes_after, closes_before);

    release_handler.store(true, Ordering::Release);
    if let Some(waker) = handler_waker
        .lock()
        .expect("test state mutex must not be poisoned")
        .take()
    {
        waker.wake();
    }
    assert_eq!(
        block_on(shutdown).expect("async shutdown must complete").outcome,
        ShutdownOutcome::Complete
    );
    runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish cleanly");
}

#[test]
fn test_graceful_shutdown_timeout_is_reported_and_immediate_shutdown_can_resume() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        spi.clone(),
        clock.new_timer(),
    )
    .expect("valid provider capabilities");
    let handler_started = Arc::new(AtomicBool::new(false));
    let release_handler = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(Mutex::new(None::<Waker>));

    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("graceful-timeout", topic()).expect("valid subscriber ID")))
            .expect("async subscription must be created");
    spi.enqueue(inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "graceful-timeout-event",
    ))));
    let started_by_handler = handler_started.clone();
    let release_by_handler = release_handler.clone();
    let waker_by_handler = handler_waker.clone();
    let runner = spawn(move || {
        block_on(subscription.run(move |_| {
            let started = started_by_handler.clone();
            let release = release_by_handler.clone();
            let waker = waker_by_handler.clone();
            async move {
                started.store(true, Ordering::Release);
                poll_fn(move |cx| {
                    if release.load(Ordering::Acquire) {
                        Poll::Ready(Ok(()))
                    } else {
                        *waker.lock().expect("test state mutex must not be poisoned") = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await
            }
        }))
    });
    for _ in 0..100 {
        if handler_started.load(Ordering::Acquire) {
            break;
        }
        sleep(Duration::from_millis(2));
    }
    assert!(handler_started.load(Ordering::Acquire));

    let timeout = Duration::from_secs(5);
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful { timeout }));
    assert!(matches!(poll_once(graceful.as_mut()), Poll::Pending));
    assert!(clock.wait_for_waiters(1, Duration::from_secs(1)));
    clock
        .advance(timeout)
        .expect("manual clock advance must stay in its clock domain");
    assert!(matches!(
        poll_once(graceful.as_mut()),
        Poll::Ready(Err(ShutdownError::TimedOut { timeout: elapsed })) if elapsed == timeout
    ));

    release_handler.store(true, Ordering::Release);
    if let Some(waker) = handler_waker
        .lock()
        .expect("test state mutex must not be poisoned")
        .take()
    {
        waker.wake();
    }
    assert_eq!(
        block_on(bus.shutdown(ShutdownMode::Immediate))
            .expect("event bus shutdown must complete")
            .outcome,
        ShutdownOutcome::Complete
    );
    runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish cleanly");
}

#[derive(Default)]
/// Provider double that records dead-letter publications and source
/// settlements.
struct DeadLetterCaptureSpi {
    messages: Arc<Mutex<VecDeque<InboundMessage>>>,
    receive_wakers: Arc<Mutex<Vec<Waker>>>,
    published: Arc<Mutex<Vec<(String, bool)>>>,
    settlements: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for DeadLetterCaptureSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::AcceptRetryReject,
            OrderingCapability::PerSubscription,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        let is_dead_letter =
            message.headers().get(DEAD_LETTER_HEADER).map(|value| value.as_ref()) == Some(DEAD_LETTER_HEADER_VALUE);
        self.published
            .lock()
            .expect("test state mutex must not be poisoned")
            .push((message.topic().as_str().to_owned(), is_dead_letter));
        Box::pin(async {
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            })
        })
    }

    fn subscribe<'a>(
        &'a self,
        _request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let messages = self.messages.clone();
        let receive_wakers = self.receive_wakers.clone();
        let settlements = self.settlements.clone();
        Box::pin(async move {
            Ok(Box::new(DeadLetterCaptureReceiver {
                messages,
                receive_wakers,
                settlements,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    fn shutdown<'a>(&'a self, _mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

/// Receiver double backed by queued inbound messages and stored receive wakers.
struct DeadLetterCaptureReceiver {
    messages: Arc<Mutex<VecDeque<InboundMessage>>>,
    receive_wakers: Arc<Mutex<Vec<Waker>>>,
    settlements: Arc<AtomicUsize>,
}

impl AsyncEventSubscriptionSpi for DeadLetterCaptureReceiver {
    fn receive<'a>(&'a mut self, _timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        let messages = self.messages.clone();
        let receive_wakers = self.receive_wakers.clone();
        Box::pin(async move {
            std::future::poll_fn(move |cx| {
                if let Some(message) = messages
                    .lock()
                    .expect("test state mutex must not be poisoned")
                    .pop_front()
                {
                    return Poll::Ready(Ok(ReceiveOutcome::Message(message)));
                }
                let mut wakers = receive_wakers.lock().expect("test state mutex must not be poisoned");
                if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                    wakers.push(cx.waker().clone());
                }
                Poll::Pending
            })
            .await
        })
    }

    fn settle<'a>(
        &'a mut self,
        _token: &SettlementToken,
        _disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.settlements.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Ok(()) })
    }

    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn test_async_dead_letter_publish_uses_configured_destination_and_reserved_marker() {
    let spi = Arc::new(DeadLetterCaptureSpi::default());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("dead-letter-capture").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let options = SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(
            DeadLetterPolicy::with_topic_name("async.dead").expect("configured dead-letter destination must be valid"),
        )
        .build();

    let mut subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("dead-letter-source", topic())
                .expect("valid subscriber ID")
                .with_options(options),
        ),
    )
    .expect("async subscription must be created");
    spi.messages
        .lock()
        .expect("test state mutex must not be poisoned")
        .push_back(InboundMessage::new(
            TopicAddress::new("async.coverage").expect("static SPI topic address must be valid"),
            EventId::new("dead-letter-source-event").expect("static event ID must be valid"),
            std::time::SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(9_u32)),
            Some(SettlementToken::new(subscription.id(), "dead-letter-source-token")),
            Default::default(),
        ));
    for waker in std::mem::take(
        &mut *spi
            .receive_wakers
            .lock()
            .expect("test state mutex must not be poisoned"),
    ) {
        waker.wake();
    }
    let runner = spawn(move || {
        block_on(subscription.run(|_| async {
            Err(DeliveryError::Handler {
                source: Box::new(Error::other("send to dead letter")),
            })
        }))
    });
    for _ in 0..1_000 {
        if !spi
            .published
            .lock()
            .expect("test state mutex must not be poisoned")
            .is_empty()
            && spi.settlements.load(Ordering::Acquire) == 1
        {
            break;
        }
        sleep(Duration::from_millis(10));
    }
    assert_eq!(
        *spi.published.lock().expect("test state mutex must not be poisoned"),
        [("async.dead".to_owned(), true)],
        "dead-letter publication must use the configured topic and reserved marker"
    );
    assert_eq!(spi.settlements.load(Ordering::Acquire), 1);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
    runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish cleanly");

    let string_spi = Arc::new(DeadLetterCaptureSpi::default());
    let string_bus = AsyncEventBus::from_spi(
        ProviderId::new("dead-letter-string").expect("static test provider ID must be valid"),
        string_spi.clone(),
    )
    .expect("valid provider capabilities");
    let string_options = SubscribeOptions::<String>::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(
            DeadLetterPolicy::with_topic_name("async.dead.string")
                .expect("configured dead-letter destination must be valid"),
        )
        .build();
    let mut string_subscription = block_on(
        string_bus.subscribe(
            SubscribeRequest::new(
                "dead-letter-string-source",
                Topic::<String>::new("async.coverage.string").expect("static test topic must be valid"),
            )
            .expect("valid subscriber ID")
            .with_options(string_options),
        ),
    )
    .expect("async subscription must be created");
    string_spi
        .messages
        .lock()
        .expect("test state mutex must not be poisoned")
        .push_back(InboundMessage::new(
            TopicAddress::new("async.coverage.string").expect("static SPI topic address must be valid"),
            EventId::new("dead-letter-string-event").expect("static event ID must be valid"),
            std::time::SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(String::from("string payload"))),
            Some(SettlementToken::new(
                string_subscription.id(),
                "dead-letter-string-token",
            )),
            Default::default(),
        ));
    for waker in std::mem::take(
        &mut *string_spi
            .receive_wakers
            .lock()
            .expect("test state mutex must not be poisoned"),
    ) {
        waker.wake();
    }
    let string_runner = spawn(move || {
        block_on(string_subscription.run(|_| async {
            Err(DeliveryError::Handler {
                source: Box::new(Error::other("send string to dead letter")),
            })
        }))
    });
    for _ in 0..1_000 {
        if !string_spi
            .published
            .lock()
            .expect("test state mutex must not be poisoned")
            .is_empty()
            && string_spi.settlements.load(Ordering::Acquire) == 1
        {
            break;
        }
        sleep(Duration::from_millis(10));
    }
    assert_eq!(
        *string_spi
            .published
            .lock()
            .expect("test state mutex must not be poisoned"),
        [("async.dead.string".to_owned(), true)]
    );
    let _ = block_on(string_bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
    string_runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish cleanly");

    let non_clone_spi = Arc::new(DeadLetterCaptureSpi::default());
    let non_clone_bus = AsyncEventBus::from_spi(
        ProviderId::new("dead-letter-non-clone").expect("static test provider ID must be valid"),
        non_clone_spi.clone(),
    )
    .expect("valid provider capabilities");
    let non_clone_options = SubscribeOptions::<NonCloneDeadLetterPayload>::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(
            DeadLetterPolicy::with_topic_name("async.dead.non-clone")
                .expect("configured dead-letter destination must be valid"),
        )
        .build();
    let mut non_clone_subscription = block_on(
        non_clone_bus.subscribe(
            SubscribeRequest::new(
                "dead-letter-non-clone-source",
                Topic::<NonCloneDeadLetterPayload>::new("async.coverage.non-clone")
                    .expect("static test topic must be valid"),
            )
            .expect("valid subscriber ID")
            .with_options(non_clone_options),
        ),
    )
    .expect("async subscription must be created");
    non_clone_spi
        .messages
        .lock()
        .expect("test state mutex must not be poisoned")
        .push_back(InboundMessage::new(
            TopicAddress::new("async.coverage.non-clone").expect("static SPI topic address must be valid"),
            EventId::new("dead-letter-non-clone-event").expect("static event ID must be valid"),
            std::time::SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(NonCloneDeadLetterPayload)),
            Some(SettlementToken::new(
                non_clone_subscription.id(),
                "dead-letter-non-clone-token",
            )),
            Default::default(),
        ));
    for waker in std::mem::take(
        &mut *non_clone_spi
            .receive_wakers
            .lock()
            .expect("test state mutex must not be poisoned"),
    ) {
        waker.wake();
    }
    let non_clone_runner = spawn(move || {
        block_on(non_clone_subscription.run(|_| async {
            Err(DeliveryError::Handler {
                source: Box::new(Error::other("send non-clone payload to dead letter")),
            })
        }))
    });
    for _ in 0..1_000 {
        if !non_clone_spi
            .published
            .lock()
            .expect("test state mutex must not be poisoned")
            .is_empty()
            && non_clone_spi.settlements.load(Ordering::Acquire) == 1
        {
            break;
        }
        sleep(Duration::from_millis(10));
    }
    assert_eq!(
        *non_clone_spi
            .published
            .lock()
            .expect("test state mutex must not be poisoned"),
        [("async.dead.non-clone".to_owned(), true)]
    );
    let _ = block_on(non_clone_bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
    non_clone_runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish cleanly");
}

/// Payload type used to cover dead-letter handling without a `Clone`
/// implementation.
struct NonCloneDeadLetterPayload;

#[test]
fn test_async_filter_false_bypasses_handler_and_filter_panic_rejects_delivery() {
    /// Runs one filter scenario and returns handler, settlement, and diagnostic
    /// observations.
    fn run_case(panic_filter: bool) -> (usize, Vec<DeliveryDisposition>, bool) {
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        let bus = AsyncEventBus::from_spi(
            ProviderId::new("fake").expect("static test provider ID must be valid"),
            spi.clone(),
        )
        .expect("valid provider capabilities");
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let delivery_failed = Arc::new(AtomicBool::new(false));
        let observed = delivery_failed.clone();
        let _observer = bus.observe_diagnostics(move |diagnostic| {
            if matches!(diagnostic, Diagnostic::DeliveryFailed { .. }) {
                observed.store(true, Ordering::Release);
            }
        });
        let options = SubscribeOptions::<u32>::builder()
            .filter(move |_| {
                if panic_filter {
                    panic!("filter panic");
                }
                false
            })
            .error_handler(|_, _| FailureDirective::Discard)
            .build();

        block_on(async {
            let mut subscription = bus
                .subscribe(
                    SubscribeRequest::new("async-filter", topic())
                        .expect("valid subscriber ID")
                        .with_options(options),
                )
                .await
                .expect("async subscription must be created");
            spi.enqueue(inbound_message(Some(SettlementToken::new(
                subscription.id(),
                "filter-event",
            ))));
            let calls = handler_calls.clone();
            let runner = spawn(move || {
                block_on(subscription.run(move |_| {
                    calls.fetch_add(1, Ordering::AcqRel);
                    async { Ok(()) }
                }))
            });
            for _ in 0..100 {
                if spi.settlement_count() > 0 {
                    break;
                }
                sleep(Duration::from_millis(2));
            }
            let dispositions = spi.settlement_dispositions();
            let _ = bus
                .shutdown(ShutdownMode::Immediate)
                .await
                .expect("event bus shutdown must complete");
            runner
                .join()
                .expect("subscription runner thread must not panic")
                .expect("subscription run must finish cleanly");
            (
                handler_calls.load(Ordering::Acquire),
                dispositions,
                delivery_failed.load(Ordering::Acquire),
            )
        })
    }

    let (calls, dispositions, failed) = run_case(false);
    assert_eq!(calls, 0, "a filtered delivery must not invoke the handler");
    assert_eq!(dispositions, [DeliveryDisposition::Accept]);
    assert!(!failed, "filter rejection is not a delivery failure");

    let (calls, dispositions, failed) = run_case(true);
    assert_eq!(calls, 0, "a panicking filter must not invoke the handler");
    assert_eq!(dispositions, [DeliveryDisposition::Reject]);
    assert!(failed, "filter panics are reported through delivery diagnostics");
}

#[test]
fn test_async_error_handler_panic_is_diagnosed_and_delivery_is_rejected() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let internal_failure = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = internal_failure.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { .. }) {
            observed.store(true, Ordering::Release);
        }
    });
    let options = SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| -> FailureDirective { panic!("error handler panic") })
        .build();

    let mut subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("async-error-handler-panic", topic())
                .expect("valid subscriber ID")
                .with_options(options),
        ),
    )
    .expect("async subscription must be created");
    spi.enqueue(inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "error-handler-panic-event",
    ))));
    let runner = spawn(move || {
        block_on(subscription.run(|_| async {
            Err(DeliveryError::Handler {
                source: Box::new(Error::other("handler failure")),
            })
        }))
    });
    for _ in 0..100 {
        if spi.settlement_count() > 0 && internal_failure.load(Ordering::Acquire) {
            break;
        }
        sleep(Duration::from_millis(2));
    }
    assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Reject]);
    assert!(internal_failure.load(Ordering::Acquire));
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("event bus shutdown must complete");
    runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish cleanly");
}
