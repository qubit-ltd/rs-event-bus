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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::StdMonotonicClock;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::PublishError;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::SpiError;
use qubit_event_bus::SubscribeError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DEAD_LETTER_HEADER;
use qubit_event_bus::model::DEAD_LETTER_HEADER_VALUE;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DurabilityCapability;
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
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_retry::RetryPolicy;

use crate::support::manual_async::block_on;

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

fn topic() -> Topic<u32> {
    Topic::new("async.coverage").expect("valid test topic")
}

struct PublisherCoverageSpi {
    payload_modes: PayloadModes,
    retryable_failures: usize,
    terminal_failure: bool,
    acknowledgement: Option<PublishAcknowledgement>,
    attempts: AtomicUsize,
    payload_was_encoded: Mutex<Vec<bool>>,
}

impl PublisherCoverageSpi {
    fn new(payload_modes: PayloadModes, retryable_failures: usize, terminal_failure: bool) -> Self {
        Self {
            payload_modes,
            retryable_failures,
            terminal_failure,
            acknowledgement: None,
            attempts: AtomicUsize::new(0),
            payload_was_encoded: Mutex::new(Vec::new()),
        }
    }

    fn with_acknowledgement(mut self, acknowledgement: PublishAcknowledgement) -> Self {
        self.acknowledgement = Some(acknowledgement);
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
            .unwrap()
            .push(matches!(message.payload(), TransportPayload::Encoded(_)));
        let should_fail = attempt < self.retryable_failures || self.terminal_failure;
        let acknowledgement = self.acknowledgement.clone();
        Box::pin(async move {
            if should_fail {
                Err(SpiError::Operation {
                    provider_id: "async-publisher-coverage".into(),
                    operation: "publish",
                    resource: None,
                    kind: "injected_publish_failure",
                    retryable: Some(attempt < self.retryable_failures),
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
                source: Box::new(std::io::Error::other("injected async codec failure")),
            })
        } else {
            Ok(Arc::from(value.as_bytes()))
        }
    }

    fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

#[test]
fn async_publisher_retries_a_retryable_failure_then_succeeds() {
    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 1, false));
    let bus = AsyncEventBus::new(ProviderId::new("async-publisher-retry").unwrap(), spi.clone());
    let request = PublishRequest::builder()
        .topic(topic())
        .payload(17_u32)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .build()
        .unwrap();

    let receipt = block_on(bus.publish(request)).expect("second async attempt should succeed");
    assert_eq!(receipt.input_event_id().as_str().len(), 36);
    assert_eq!(spi.attempts.load(Ordering::Acquire), 2);
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_publisher_metrics_track_shared_attempts_and_batch_items() {
    use qubit_event_bus::EventBusFacadeConfig;
    use qubit_event_bus::facade::PublishMetricsSnapshot;

    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false));
    let bus = AsyncEventBus::new(ProviderId::new("async-publisher-metrics").unwrap(), spi);
    assert_eq!(bus.publish_metrics(), PublishMetricsSnapshot::default());
    let clone = bus.clone();
    let requests = [
        PublishRequest::builder().topic(topic()).payload(1_u32).build().unwrap(),
        PublishRequest::builder().topic(topic()).payload(2_u32).build().unwrap(),
    ];
    let batch = block_on(clone.publish_all(requests));
    assert_eq!(batch.total_count(), 2);
    assert!(batch.items().iter().all(Result::is_ok));
    assert_eq!(bus.publish_metrics().attempts, 2);
    assert_eq!(bus.publish_metrics().opaque_accepted, 2);

    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    let closed = block_on(bus.publish(PublishRequest::builder().topic(topic()).payload(3_u32).build().unwrap()));
    assert!(matches!(closed, Err(PublishError::Closed)));
    assert_eq!(clone.publish_metrics().attempts, 3);
    assert_eq!(clone.publish_metrics().errors, 1);

    let dropped_bus = AsyncEventBus::with_config(
        ProviderId::new("async-publisher-metrics-dropped").unwrap(),
        Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false)),
        EventBusFacadeConfig::new().publisher_interceptor(|_| Ok(false)),
    );
    block_on(dropped_bus.publish(PublishRequest::builder().topic(topic()).payload(4_u32).build().unwrap()))
        .unwrap();
    let dropped = dropped_bus.publish_metrics();
    assert_eq!(dropped.attempts, 1);
    assert_eq!(dropped.dropped, 1);
    assert_eq!(dropped.errors, 0);

    use qubit_event_bus::model::AdmissionStatus;
    use qubit_event_bus::model::DestinationAdmission;
    use qubit_id::Id;
    use qubit_event_bus::SubscriberId;
    let mixed_ack = PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(
            Id::new(10),
            SubscriberId::new("accepted-async-metrics").unwrap(),
            AdmissionStatus::Accepted,
        ),
        DestinationAdmission::new(
            Id::new(11),
            SubscriberId::new("filtered-async-metrics").unwrap(),
            AdmissionStatus::Filtered,
        ),
        DestinationAdmission::new(
            Id::new(12),
            SubscriberId::new("rejected-async-metrics").unwrap(),
            AdmissionStatus::Rejected("injected rejection".into()),
        ),
    ]);
    let destination_bus = AsyncEventBus::new(
        ProviderId::new("async-publisher-metrics-destinations").unwrap(),
        Arc::new(
            PublisherCoverageSpi::new(PayloadModes::Native, 0, false).with_acknowledgement(mixed_ack),
        ),
    );
    block_on(destination_bus.publish(PublishRequest::builder().topic(topic()).payload(5_u32).build().unwrap()))
        .unwrap();
    let destination_metrics = destination_bus.publish_metrics();
    assert_eq!(destination_metrics.accepted_destinations, 1);
    assert_eq!(destination_metrics.filtered_destinations, 1);
    assert_eq!(destination_metrics.rejected_destinations, 1);

    let empty_bus = AsyncEventBus::new(
        ProviderId::new("async-publisher-metrics-empty").unwrap(),
        Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false).with_acknowledgement(
            PublishAcknowledgement::DestinationAdmissions(Vec::new()),
        )),
    );
    block_on(empty_bus.publish(PublishRequest::builder().topic(topic()).payload(6_u32).build().unwrap())).unwrap();
    assert_eq!(empty_bus.publish_metrics().zero_destinations, 1);

    let failing_bus = AsyncEventBus::new(
        ProviderId::new("async-publisher-metrics-error").unwrap(),
        Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, true)),
    );
    assert!(block_on(failing_bus.publish(PublishRequest::builder().topic(topic()).payload(7_u32).build().unwrap())).is_err());
    let failure_metrics = failing_bus.publish_metrics();
    assert_eq!(failure_metrics.attempts, 1);
    assert_eq!(failure_metrics.errors, 1);

    let concurrent_bus = AsyncEventBus::new(
        ProviderId::new("async-publisher-metrics-concurrent").unwrap(),
        Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, false)),
    );
    let workers = (0..8)
        .map(|index| {
            let worker_bus = concurrent_bus.clone();
            std::thread::spawn(move || {
                block_on(worker_bus.publish(PublishRequest::builder().topic(topic()).payload(index).build().unwrap()))
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
fn async_global_publisher_interceptor_edits_only_validated_headers() {
    use qubit_event_bus::EventBusFacadeConfig;
    use qubit_event_bus::model::PublishMetadata;

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
        ProviderId::new("async-global-interceptor").unwrap(),
        spi.clone(),
        config,
    );
    let request = PublishRequest::builder()
        .topic(topic())
        .payload(18_u32)
        .header("origin", "application")
        .build()
        .unwrap();

    block_on(bus.publish(request)).expect("global interceptor allows publish");
    assert_eq!(spi.attempts.load(Ordering::Acquire), 1);
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_string_delivery_runs_error_handler_and_terminates_failure() {
    use qubit_event_bus::model::FailureDirective;
    use qubit_event_bus::model::Headers;
    use qubit_event_bus::model::SubscribeOptions;
    use qubit_event_bus::spi::InboundMessage;

    let spi = Arc::new(crate::support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("async-string-delivery").unwrap(), spi.clone());
    let request = SubscribeRequest::new(
        SubscriberId::new("string-worker").unwrap(),
        Topic::<String>::new("async.string").unwrap(),
    )
    .with_options(
        SubscribeOptions::builder()
            .error_handler(|_, _| FailureDirective::Discard)
            .build(),
    );
    let mut subscription = block_on(bus.subscribe(request)).unwrap();
    spi.enqueue(InboundMessage::new(
        TopicAddress::new("async.string").unwrap(),
        EventId::new("async-string-event").unwrap(),
        std::time::SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        TransportPayload::Native(Arc::new(String::from("body"))),
        None,
        Default::default(),
    ));
    let (handled_tx, handled_rx) = std::sync::mpsc::channel();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |delivery| {
            let handled = handled_tx.clone();
            async move {
                handled.send(delivery.payload().clone()).unwrap();
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("expected handler failure")),
                })
            }
        }))
    });

    assert_eq!(handled_rx.recv_timeout(Duration::from_secs(2)).unwrap(), "body");
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
}

#[test]
fn failed_timer_registration_surfaces_after_a_failed_settlement() {
    let spi = Arc::new(crate::support::fake_spi::FakeAsyncEventBusSpi::new());
    let timer = Arc::new(FailingTimer {
        clock: StdMonotonicClock::new(),
    });
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), timer);
    let request = SubscribeRequest::new(SubscriberId::new("timer-failure").unwrap(), topic());

    let error = block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        spi.fail_next_settle();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "timer-failure-token",
        ))));
        let result = subscription.run(|_| async { Ok(()) }).await;
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        result
    })
    .expect_err("settlement retry cannot continue without timer registration");

    assert!(matches!(error, ReceiveError::Timer(TimeError::InstantOverflow)));
    assert_eq!(
        1,
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "settle")
            .count()
    );
}

#[test]
fn async_encoded_publisher_sends_encoded_payload_and_skips_spi_on_codec_failure() {
    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Encoded, 0, false));
    let bus = AsyncEventBus::new(ProviderId::new("async-encoded-publisher").unwrap(), spi.clone());
    let encoded_topic = Topic::with_codec(
        "async.encoded",
        StringCodec {
            content_type: ContentType::new("text/plain").unwrap(),
            fail_encode: false,
        },
    )
    .unwrap();

    block_on(bus.publish(PublishRequest::new(encoded_topic, "wire payload".to_owned()).unwrap()))
        .expect("codec should encode successfully");
    assert_eq!(*spi.payload_was_encoded.lock().unwrap(), [true]);
    assert_eq!(spi.attempts.load(Ordering::Acquire), 1);
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();

    let failing_spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Encoded, 0, false));
    let failing_bus = AsyncEventBus::new(ProviderId::new("async-codec-failure").unwrap(), failing_spi.clone());
    let failing_topic = Topic::with_codec(
        "async.codec.failure",
        StringCodec {
            content_type: ContentType::new("text/plain").unwrap(),
            fail_encode: true,
        },
    )
    .unwrap();
    let error = block_on(failing_bus.publish(PublishRequest::new(failing_topic, "cannot encode".to_owned()).unwrap()))
        .unwrap_err();
    assert!(matches!(error, PublishError::Codec(CodecError::Encode { .. })));
    assert_eq!(failing_spi.attempts.load(Ordering::Acquire), 0);
    block_on(failing_bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_terminal_failure_handler_reads_non_clone_payload_and_ordering_metadata() {
    struct NonClonePayload {
        value: String,
    }

    let spi = Arc::new(PublisherCoverageSpi::new(PayloadModes::Native, 0, true));
    let bus = AsyncEventBus::new(ProviderId::new("async-terminal-observer").unwrap(), spi);
    let observed = Arc::new(Mutex::new(None));
    let observed_by_handler = observed.clone();
    let request = PublishRequest::builder()
        .topic(Topic::new("async.terminal.observer").unwrap())
        .payload(NonClonePayload {
            value: "preserved payload".to_owned(),
        })
        .header("trace", "trace-async-1")
        .ordering_key("account-async-1")
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .error_handler(move |context, _| {
            *observed_by_handler.lock().unwrap() = Some((
                context.payload().value.clone(),
                context.header("trace").unwrap().to_owned(),
                context.ordering_key().unwrap().to_owned(),
            ));
        })
        .build()
        .unwrap();

    let error = block_on(bus.publish(request)).unwrap_err();
    assert!(
        matches!(error, PublishError::Retry(_)),
        "unexpected publish error: {error:?}"
    );
    assert_eq!(
        *observed.lock().unwrap(),
        Some((
            "preserved payload".to_owned(),
            "trace-async-1".to_owned(),
            "account-async-1".to_owned(),
        ))
    );
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[derive(Default)]
struct CloseFailingSpi {
    receives: Arc<AtomicUsize>,
    close_attempts: Arc<AtomicUsize>,
    receiver_drops: Arc<AtomicUsize>,
    receive_future_drops: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for CloseFailingSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        crate::support::fake_spi::full_capabilities()
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
                    source: Box::new(std::io::Error::other("test close failure")),
                })
            } else {
                Ok(())
            }
        })
    }
}

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
fn explicit_subscription_close_returns_a_single_close_failure() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::new(ProviderId::new("close-failing").unwrap(), spi);

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("single-close-failure").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        let error = subscription.close().await.unwrap_err();
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
fn shutdown_aggregates_multiple_async_subscription_close_failures() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::new(ProviderId::new("close-failing").unwrap(), spi.clone());
    let mut runners = Vec::new();

    block_on(async {
        for name in ["close-failure-a", "close-failure-b"] {
            let mut subscription = bus
                .subscribe(SubscribeRequest::new(SubscriberId::new(name).unwrap(), topic()))
                .await
                .unwrap();
            runners.push(std::thread::spawn(move || {
                block_on(subscription.run(|_| async { Ok(()) }))
            }));
        }
    });

    for _ in 0..100 {
        if spi.receives.load(Ordering::Acquire) == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(spi.receives.load(Ordering::Acquire), 2);

    let shutdown = block_on(bus.shutdown(ShutdownMode::Immediate));
    for runner in runners {
        assert!(runner.join().unwrap().is_err(), "run reports its close error");
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
fn publish_and_subscribe_are_rejected_after_async_shutdown() {
    let spi = Arc::new(crate::support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi);

    block_on(async {
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        let publish_error = bus.publish(PublishRequest::new(topic(), 7).unwrap()).await.unwrap_err();
        assert!(matches!(publish_error, PublishError::Closed));
        let subscribe_result = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("after-shutdown").unwrap(),
                topic(),
            ))
            .await;
        let subscribe_error = match subscribe_result {
            Ok(_) => panic!("subscribe after shutdown must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(subscribe_error, SubscribeError::Closed));
    });
}

#[test]
fn failed_receiver_close_can_be_retried_and_drop_releases_the_receiver() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::new(ProviderId::new("close-failing").unwrap(), spi.clone());

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("close-retry").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
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
fn dropping_unrun_subscription_releases_receiver_without_async_close() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::new(ProviderId::new("close-failing").unwrap(), spi.clone());

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new(SubscriberId::new("drop-unrun").unwrap(), topic()))
            .await
            .unwrap();
        drop(subscription);
        assert_eq!(spi.close_attempts.load(Ordering::Acquire), 0);
        assert_eq!(spi.receiver_drops.load(Ordering::Acquire), 1);
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        assert_eq!(spi.close_attempts.load(Ordering::Acquire), 0);
    });
}

#[test]
fn shutdown_cancels_pending_receive_before_closing_receiver() {
    let spi = Arc::new(CloseFailingSpi::default());
    let bus = AsyncEventBus::new(ProviderId::new("close-failing").unwrap(), spi.clone());
    let mut subscription = block_on(bus.subscribe(SubscribeRequest::new(
        SubscriberId::new("cancel-receive").unwrap(),
        topic(),
    )))
    .unwrap();
    let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
    for _ in 0..100 {
        if spi.receives.load(Ordering::Acquire) != 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(spi.receives.load(Ordering::Acquire), 1);

    let shutdown = block_on(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(shutdown, Err(ShutdownError::SubscriptionClose(_))));
    assert!(runner.join().unwrap().is_err());
    assert_eq!(spi.receive_future_drops.load(Ordering::Acquire), 1);
    assert!(
        (1..=2).contains(&spi.close_attempts.load(Ordering::Acquire)),
        "the runner and shutdown may race to make the first safe close retry"
    );
}

#[test]
fn close_during_shutdown_is_a_noop_for_a_nonrunning_subscription() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Poll;

    let spi = Arc::new(crate::support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let handler_started = Arc::new(AtomicBool::new(false));
    let release_handler = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(std::sync::Mutex::new(None::<std::task::Waker>));

    let (mut active_subscription, mut idle_subscription) = block_on(async {
        let active = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("active-during-close").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        let idle = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("idle-during-close").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        (active, idle)
    });
    assert_eq!(idle_subscription.subscriber_id().as_str(), "idle-during-close");
    assert!(idle_subscription.id().value() > 0);
    spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
        active_subscription.id(),
        "shutdown-close-branch",
    ))));

    let started_by_handler = handler_started.clone();
    let release_by_handler = release_handler.clone();
    let waker_by_handler = handler_waker.clone();
    let runner = std::thread::spawn(move || {
        block_on(active_subscription.run(move |_| {
            let started = started_by_handler.clone();
            let release = release_by_handler.clone();
            let waker = waker_by_handler.clone();
            async move {
                started.store(true, Ordering::Release);
                std::future::poll_fn(move |cx| {
                    if release.load(Ordering::Acquire) {
                        Poll::Ready(Ok(()))
                    } else {
                        *waker.lock().unwrap() = Some(cx.waker().clone());
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
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(handler_started.load(Ordering::Acquire));

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(
        crate::support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Pending
    ));
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
    if let Some(waker) = handler_waker.lock().unwrap().take() {
        waker.wake();
    }
    assert_eq!(block_on(shutdown).unwrap(), ShutdownOutcome::Complete);
    runner.join().unwrap().unwrap();
}

#[test]
fn graceful_shutdown_timeout_is_reported_and_immediate_shutdown_can_resume() {
    use std::sync::atomic::AtomicBool;
    use std::task::Poll;

    use qubit_clock::ManualMonotonicClock;
    use qubit_clock::MonotonicClock;

    let spi = Arc::new(crate::support::fake_spi::FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), clock.new_timer());
    let handler_started = Arc::new(AtomicBool::new(false));
    let release_handler = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(Mutex::new(None::<Waker>));

    let mut subscription = block_on(bus.subscribe(SubscribeRequest::new(
        SubscriberId::new("graceful-timeout").unwrap(),
        topic(),
    )))
    .unwrap();
    spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "graceful-timeout-event",
    ))));
    let started_by_handler = handler_started.clone();
    let release_by_handler = release_handler.clone();
    let waker_by_handler = handler_waker.clone();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |_| {
            let started = started_by_handler.clone();
            let release = release_by_handler.clone();
            let waker = waker_by_handler.clone();
            async move {
                started.store(true, Ordering::Release);
                std::future::poll_fn(move |cx| {
                    if release.load(Ordering::Acquire) {
                        Poll::Ready(Ok(()))
                    } else {
                        *waker.lock().unwrap() = Some(cx.waker().clone());
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
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(handler_started.load(Ordering::Acquire));

    let timeout = Duration::from_secs(5);
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful { timeout }));
    assert!(matches!(
        crate::support::manual_async::poll_once(graceful.as_mut()),
        Poll::Pending
    ));
    assert!(clock.wait_for_waiters(1, Duration::from_secs(1)));
    clock.advance(timeout).unwrap();
    assert!(matches!(
        crate::support::manual_async::poll_once(graceful.as_mut()),
        Poll::Ready(Err(ShutdownError::TimedOut { timeout: elapsed })) if elapsed == timeout
    ));

    release_handler.store(true, Ordering::Release);
    if let Some(waker) = handler_waker.lock().unwrap().take() {
        waker.wake();
    }
    assert_eq!(
        block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap(),
        ShutdownOutcome::Complete
    );
    runner.join().unwrap().unwrap();
}

#[derive(Default)]
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
            .unwrap()
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
                if let Some(message) = messages.lock().unwrap().pop_front() {
                    return Poll::Ready(Ok(ReceiveOutcome::Message(message)));
                }
                let mut wakers = receive_wakers.lock().unwrap();
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
fn async_dead_letter_publish_uses_configured_destination_and_reserved_marker() {
    let spi = Arc::new(DeadLetterCaptureSpi::default());
    let bus = AsyncEventBus::new(ProviderId::new("dead-letter-capture").unwrap(), spi.clone());
    let options = SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::topic("async.dead").unwrap())
        .build();

    let mut subscription = block_on(bus.subscribe(
        SubscribeRequest::new(SubscriberId::new("dead-letter-source").unwrap(), topic()).with_options(options),
    ))
    .unwrap();
    spi.messages.lock().unwrap().push_back(InboundMessage::new(
        TopicAddress::new("async.coverage").unwrap(),
        EventId::new("dead-letter-source-event").unwrap(),
        std::time::SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        TransportPayload::Native(Arc::new(9_u32)),
        Some(SettlementToken::new(subscription.id(), "dead-letter-source-token")),
        Default::default(),
    ));
    for waker in std::mem::take(&mut *spi.receive_wakers.lock().unwrap()) {
        waker.wake();
    }
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(|_| async {
            Err(DeliveryError::Handler {
                source: Box::new(std::io::Error::other("send to dead letter")),
            })
        }))
    });
    for _ in 0..100 {
        if !spi.published.lock().unwrap().is_empty() && spi.settlements.load(Ordering::Acquire) == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        *spi.published.lock().unwrap(),
        [("async.dead".to_owned(), true)],
        "dead-letter publication must use the configured topic and reserved marker"
    );
    assert_eq!(spi.settlements.load(Ordering::Acquire), 1);
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();

    let string_spi = Arc::new(DeadLetterCaptureSpi::default());
    let string_bus = AsyncEventBus::new(ProviderId::new("dead-letter-string").unwrap(), string_spi.clone());
    let string_options = SubscribeOptions::<String>::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::topic("async.dead.string").unwrap())
        .build();
    let mut string_subscription = block_on(
        string_bus.subscribe(
            SubscribeRequest::new(
                SubscriberId::new("dead-letter-string-source").unwrap(),
                Topic::<String>::new("async.coverage.string").unwrap(),
            )
            .with_options(string_options),
        ),
    )
    .unwrap();
    string_spi.messages.lock().unwrap().push_back(InboundMessage::new(
        TopicAddress::new("async.coverage.string").unwrap(),
        EventId::new("dead-letter-string-event").unwrap(),
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
    for waker in std::mem::take(&mut *string_spi.receive_wakers.lock().unwrap()) {
        waker.wake();
    }
    let string_runner = std::thread::spawn(move || {
        block_on(string_subscription.run(|_| async {
            Err(DeliveryError::Handler {
                source: Box::new(std::io::Error::other("send string to dead letter")),
            })
        }))
    });
    for _ in 0..100 {
        if !string_spi.published.lock().unwrap().is_empty() && string_spi.settlements.load(Ordering::Acquire) == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        *string_spi.published.lock().unwrap(),
        [("async.dead.string".to_owned(), true)]
    );
    block_on(string_bus.shutdown(ShutdownMode::Immediate)).unwrap();
    string_runner.join().unwrap().unwrap();

    let non_clone_spi = Arc::new(DeadLetterCaptureSpi::default());
    let non_clone_bus = AsyncEventBus::new(ProviderId::new("dead-letter-non-clone").unwrap(), non_clone_spi.clone());
    let non_clone_options = SubscribeOptions::<NonCloneDeadLetterPayload>::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::topic("async.dead.non-clone").unwrap())
        .build();
    let mut non_clone_subscription = block_on(
        non_clone_bus.subscribe(
            SubscribeRequest::new(
                SubscriberId::new("dead-letter-non-clone-source").unwrap(),
                Topic::<NonCloneDeadLetterPayload>::new("async.coverage.non-clone").unwrap(),
            )
            .with_options(non_clone_options),
        ),
    )
    .unwrap();
    non_clone_spi.messages.lock().unwrap().push_back(InboundMessage::new(
        TopicAddress::new("async.coverage.non-clone").unwrap(),
        EventId::new("dead-letter-non-clone-event").unwrap(),
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
    for waker in std::mem::take(&mut *non_clone_spi.receive_wakers.lock().unwrap()) {
        waker.wake();
    }
    let non_clone_runner = std::thread::spawn(move || {
        block_on(non_clone_subscription.run(|_| async {
            Err(DeliveryError::Handler {
                source: Box::new(std::io::Error::other("send non-clone payload to dead letter")),
            })
        }))
    });
    for _ in 0..100 {
        if !non_clone_spi.published.lock().unwrap().is_empty() && non_clone_spi.settlements.load(Ordering::Acquire) == 1
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        *non_clone_spi.published.lock().unwrap(),
        [("async.dead.non-clone".to_owned(), true)]
    );
    block_on(non_clone_bus.shutdown(ShutdownMode::Immediate)).unwrap();
    non_clone_runner.join().unwrap().unwrap();
}

struct NonCloneDeadLetterPayload;

#[test]
fn async_filter_false_bypasses_handler_and_filter_panic_rejects_delivery() {
    fn run_case(panic_filter: bool) -> (usize, Vec<DeliveryDisposition>, bool) {
        let spi = Arc::new(crate::support::fake_spi::FakeAsyncEventBusSpi::new());
        let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let delivery_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
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
                    SubscribeRequest::new(SubscriberId::new("async-filter").unwrap(), topic()).with_options(options),
                )
                .await
                .unwrap();
            spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
                subscription.id(),
                "filter-event",
            ))));
            let calls = handler_calls.clone();
            let runner = std::thread::spawn(move || {
                block_on(subscription.run(move |_| {
                    calls.fetch_add(1, Ordering::AcqRel);
                    async { Ok(()) }
                }))
            });
            for _ in 0..100 {
                if spi.settlement_count() > 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            let dispositions = spi.settlement_dispositions();
            bus.shutdown(ShutdownMode::Immediate).await.unwrap();
            runner.join().unwrap().unwrap();
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
fn async_error_handler_panic_is_diagnosed_and_delivery_is_rejected() {
    let spi = Arc::new(crate::support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
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

    let mut subscription = block_on(bus.subscribe(
        SubscribeRequest::new(SubscriberId::new("async-error-handler-panic").unwrap(), topic()).with_options(options),
    ))
    .unwrap();
    spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "error-handler-panic-event",
    ))));
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(|_| async {
            Err(DeliveryError::Handler {
                source: Box::new(std::io::Error::other("handler failure")),
            })
        }))
    });
    for _ in 0..100 {
        if spi.settlement_count() > 0 && internal_failure.load(Ordering::Acquire) {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Reject]);
    assert!(internal_failure.load(Ordering::Acquire));
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
}
