// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared receive ordering and recovery contracts exercised through both
//! facades.
mod support;

use std::error::Error as StdError;
use std::future::poll_fn;
use std::io::Error as IoError;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Poll;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::CodecError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::EventBus;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::error::SubscriptionCloseErrors;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::PayloadLimits;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::PayloadDirection;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SchemaId;
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
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

use crate::support::manual_async;

struct Probe {
    content: ContentType,
    schema: SchemaId,
    validate: AtomicUsize,
    decode: AtomicUsize,
    mode: usize,
}
impl Probe {
    /// Creates a codec with independent callback counters.
    fn new() -> Arc<Self> {
        Arc::new(Self {
            content: ContentType::new("application/test").expect("content type"),
            schema: SchemaId::new("v1").expect("schema"),
            validate: AtomicUsize::new(0),
            decode: AtomicUsize::new(0),
            mode: 0,
        })
    }
}
impl EventCodec<u32> for Probe {
    fn content_type(&self) -> &ContentType {
        &self.content
    }
    fn schema_id(&self) -> Option<&SchemaId> {
        Some(&self.schema)
    }
    fn encode(&self, _: &u32) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from([1]))
    }
    fn validate_metadata(&self, value: &EncodedPayload) -> Result<(), CodecError> {
        self.validate.fetch_add(1, Ordering::SeqCst);
        if self.mode == 1 {
            panic!("permanent metadata panic");
        }
        if self.mode == 4 && value.content_type() == &self.content && value.schema_id().is_none() {
            return Ok(());
        }
        if value.content_type() == &self.content && value.schema_id() == Some(&self.schema) {
            Ok(())
        } else {
            Err(CodecError::MetadataMismatch {
                expected_content_type: self.content.clone(),
                actual_content_type: value.content_type().clone(),
                expected_schema_id: Some(self.schema.clone()),
                actual_schema_id: value.schema_id().cloned(),
            })
        }
    }
    fn decode(&self, _: &EncodedPayload) -> Result<u32, CodecError> {
        self.decode.fetch_add(1, Ordering::SeqCst);
        if self.mode == 2 {
            panic!("permanent decode panic");
        }
        if self.mode == 3 {
            return Err(CodecError::Decode {
                source: Box::new(IoError::other("malformed message")),
            });
        }
        Ok(1)
    }
}
#[derive(Clone)]
struct Source {
    size: usize,
    schema: Option<SchemaId>,
    settled: Arc<AtomicUsize>,
    dispositions: Arc<Mutex<Vec<DeliveryDisposition>>>,
    close_failed: Arc<AtomicBool>,
    closed: Arc<AtomicUsize>,
    close_paused: Arc<AtomicBool>,
    wait_second: Arc<AtomicBool>,
    first_good: bool,
    native: bool,
    ephemeral: bool,
    content: ContentType,
}
struct Receiver {
    source: Source,
    id: Id,
    delivered: usize,
}
impl Source {
    /// Creates a deterministic provider with one encoded message per receiver.
    fn new(size: usize, schema: Option<SchemaId>) -> Self {
        Self {
            size,
            schema,
            settled: Arc::default(),
            dispositions: Arc::default(),
            close_failed: Arc::default(),
            closed: Arc::default(),
            close_paused: Arc::default(),
            wait_second: Arc::default(),
            first_good: false,
            native: false,
            ephemeral: false,
            content: ContentType::new("application/test").expect("content"),
        }
    }
    /// Advertises encoded messages and durable settlement recovery.
    fn capabilities_value(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Encoded,
            SettlementCapabilities::AcceptRetryReject,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            if self.ephemeral {
                DurabilityCapability::Ephemeral
            } else {
                DurabilityCapability::Durable
            },
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }
    /// Creates an independent receiver retaining the original source record.
    fn receiver(&self, request: SpiSubscriptionRequest) -> Receiver {
        Receiver {
            source: self.clone(),
            id: request.subscription_id(),
            delivered: 0,
        }
    }
}
impl Receiver {
    /// Records one provider close attempt; the injected failure retains its
    /// source.
    fn close_result(&self) -> Result<(), SpiError> {
        self.source.closed.fetch_add(1, Ordering::SeqCst);
        if self.source.close_failed.load(Ordering::SeqCst) {
            Err(SpiError::Operation {
                provider_id: "probe".into(),
                operation: "close",
                resource: Some("boundary".into()),
                kind: "injected_close_failure",
                retryable: Some(false),
                source: Box::new(IoError::other("receiver close failed independently")),
            })
        } else {
            Ok(())
        }
    }

    /// Returns the source once, then ends the deterministic stream.
    fn next(&mut self) -> ReceiveOutcome {
        let good = self.source.first_good && self.delivered == 0;
        if self.delivered > usize::from(self.source.first_good)
            || self.source.settled.load(Ordering::SeqCst) > 0 && !self.source.first_good
        {
            return ReceiveOutcome::Closed;
        }
        self.delivered += 1;
        let payload = if self.source.native {
            TransportPayload::Native(Arc::new("wrong native type"))
        } else {
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(vec![0; if good { 1 } else { self.source.size }]),
                self.source.content.clone(),
                if good {
                    Some(SchemaId::new("v1").expect("schema"))
                } else {
                    self.source.schema.clone()
                },
            ))
        };
        ReceiveOutcome::Message(InboundMessage::new(
            TopicAddress::new("boundary").expect("topic"),
            EventId::new(if good { "healthy-record" } else { "source-record" }).expect("id"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            payload,
            Some(SettlementToken::new(self.id, "source-record")),
            Default::default(),
        ))
    }
}
impl EventBusSpi for Source {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities_value()
    }
    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        panic!("unused publishing")
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(self.receiver(request)))
    }
    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}
impl EventSubscriptionSpi for Receiver {
    fn receive(&mut self, _: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.delivered == 1 && self.source.first_good {
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.source.wait_second.load(Ordering::SeqCst) {
                assert!(Instant::now() < deadline, "first handler starts before second receive");
                thread::yield_now();
            }
        }
        Ok(self.next())
    }
    fn settle(&mut self, _: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        self.source
            .dispositions
            .lock()
            .expect("settlement log")
            .push(disposition);
        self.source.settled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.close_result()
    }
}
impl AsyncEventBusSpi for Source {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities_value()
    }
    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async { panic!("unused publishing") })
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move { Ok(Box::new(self.receiver(request)) as Box<dyn AsyncEventSubscriptionSpi>) })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}
impl AsyncEventSubscriptionSpi for Receiver {
    fn receive<'a>(&'a mut self, _: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move { Ok(self.next()) })
    }
    fn settle<'a>(
        &'a mut self,
        _: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            self.source
                .dispositions
                .lock()
                .expect("settlement log")
                .push(disposition);
            self.source.settled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(poll_fn(move |_| {
            if self.source.close_paused.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            Poll::Ready(self.close_result())
        }))
    }
}
/// Builds a four-byte receive limit while keeping publishing independent.
fn config() -> EventBusFacadeConfig {
    EventBusFacadeConfig::new().with_payload_limits(PayloadLimits::new(
        NonZeroUsize::new(100).expect("publish limit"),
        NonZeroUsize::new(4).expect("receive limit"),
    ))
}
/// Verifies callback ordering without relying on terminal-state API
/// availability.
fn verify_boundary(
    size: usize,
    schema: Option<SchemaId>,
    expected_validate: usize,
    expected_decode: usize,
    asynchronous: bool,
) {
    let probe = Probe::new();
    let source = Arc::new(Source::new(size, schema));
    let topic = Topic::new("boundary").expect("topic").with_shared_codec(probe.clone());
    if asynchronous {
        let bus = AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
            .expect("bus");
        let mut subscription =
            manual_async::block_on(bus.subscribe(SubscribeRequest::new("probe", topic).expect("request")))
                .expect("subscription");
        let result = manual_async::block_on(subscription.run(|_| async { Ok(()) }));
        if expected_decode == 0 {
            let reason = subscription.terminal_failure().expect("cached receive cause");
            let ReceiveError::Stopped(returned) = result.expect_err("boundary stops") else {
                panic!("structured stop")
            };
            assert!(Arc::ptr_eq(&reason, &returned));
            let ReceiveError::Stopped(repeated) = manual_async::block_on(subscription.run(|_| async { Ok(()) }))
                .expect_err("same handle remains stopped")
            else {
                panic!("structured stop")
            };
            assert!(Arc::ptr_eq(&reason, &repeated));
        } else {
            result.expect("healthy run");
        }
        let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
    } else {
        let bus =
            EventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config()).expect("bus");
        let subscription = bus
            .subscribe(SubscribeRequest::new("probe", topic).expect("request"), |_| {})
            .expect("subscription");
        let deadline = Instant::now() + Duration::from_secs(5);
        while source.closed.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "receiver completes");
            thread::yield_now();
        }
        if expected_decode == 0 {
            assert!(subscription.terminal_failure().is_some());
        }
        subscription.cancel().expect("cancel");
        let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    }
    assert_eq!(
        probe.validate.load(Ordering::SeqCst),
        expected_validate,
        "metadata callback count"
    );
    assert_eq!(
        probe.decode.load(Ordering::SeqCst),
        expected_decode,
        "decode callback count"
    );
    if expected_decode == 0 {
        assert_eq!(
            source.settled.load(Ordering::SeqCst),
            0,
            "durable source remains unsettled"
        );
        assert!(
            source.dispositions.lock().expect("settlement log").is_empty(),
            "fail-stop submits no Accept, Retry or Reject"
        );
    }
}
#[test]
fn test_receive_limit_precedes_callbacks() {
    for asynchronous in [false, true] {
        verify_boundary(5, Some(SchemaId::new("v1").expect("schema")), 0, 0, asynchronous);
    }
}
#[test]
fn test_receive_metadata_precedes_decode() {
    for asynchronous in [false, true] {
        for schema in [None, Some(SchemaId::new("v2").expect("schema"))] {
            verify_boundary(4, schema, 1, 0, asynchronous);
        }
    }
}
#[test]
fn test_receive_exact_limit_allows_decode() {
    for asynchronous in [false, true] {
        verify_boundary(4, Some(SchemaId::new("v1").expect("schema")), 1, 1, asynchronous);
    }
}

/// Builds a codec that permanently fails a selected callback.
fn panic_probe(mode: usize) -> Arc<Probe> {
    Arc::new(Probe {
        content: ContentType::new("application/test").expect("content"),
        schema: SchemaId::new("v1").expect("schema"),
        validate: AtomicUsize::new(0),
        decode: AtomicUsize::new(0),
        mode,
    })
}
#[test]
fn test_permanent_panic_is_unsettled_and_new_subscription_recovers() {
    for mode in [1, 2] {
        let source = Arc::new(Source::new(4, Some(SchemaId::new("v1").expect("schema"))));
        let bus = AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
            .expect("bus");
        let probe = panic_probe(mode);
        let mut stopped = manual_async::block_on(
            bus.subscribe(
                SubscribeRequest::new(
                    "panic",
                    Topic::new("boundary").expect("topic").with_shared_codec(probe.clone()),
                )
                .expect("request"),
            ),
        )
        .expect("subscription");
        assert!(matches!(
            manual_async::block_on(stopped.run(|_| async { Ok(()) })),
            Err(ReceiveError::Stopped(_))
        ));
        assert_eq!(probe.validate.load(Ordering::SeqCst), 1);
        assert_eq!(probe.decode.load(Ordering::SeqCst), usize::from(mode == 2));
        assert_eq!(source.settled.load(Ordering::SeqCst), 0);
        assert!(
            source.dispositions.lock().expect("settlement log").is_empty(),
            "fail-stop must never settle with any disposition"
        );
        let mut healthy = manual_async::block_on(
            bus.subscribe(
                SubscribeRequest::new(
                    "healthy",
                    Topic::new("boundary").expect("topic").with_shared_codec(Probe::new()),
                )
                .expect("request"),
            ),
        )
        .expect("subscription");
        let handled = Arc::new(AtomicUsize::new(0));
        let callback_count = handled.clone();
        manual_async::block_on(healthy.run(move |_| {
            callback_count.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }))
        .expect("new subscription recovers durable source");
        assert_eq!(handled.load(Ordering::SeqCst), 1);
        assert_eq!(source.settled.load(Ordering::SeqCst), 1);
        let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
    }
}
#[test]
fn test_cancelled_close_retains_driver_and_same_cause() {
    let source = Arc::new(Source::new(5, Some(SchemaId::new("v1").expect("schema"))));
    source.close_paused.store(true, Ordering::SeqCst);
    let bus =
        AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config()).expect("bus");
    let mut subscription = manual_async::block_on(
        bus.subscribe(
            SubscribeRequest::new(
                "paused-close",
                Topic::new("boundary").expect("topic").with_shared_codec(Probe::new()),
            )
            .expect("request"),
        ),
    )
    .expect("subscription");
    let mut run = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(manual_async::poll_once(run.as_mut()).is_pending());
    drop(run);
    let reason = subscription
        .terminal_failure()
        .expect("cause retained while close is pending");
    let mut close = Box::pin(subscription.close());
    assert!(manual_async::poll_once(close.as_mut()).is_pending());
    drop(close);
    source.close_paused.store(false, Ordering::SeqCst);
    let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("bus driver resumes provider close");
    assert_eq!(source.closed.load(Ordering::SeqCst), 1);
    assert_eq!(source.settled.load(Ordering::SeqCst), 0);
    assert!(
        source.dispositions.lock().expect("settlement log").is_empty(),
        "fail-stop must never settle with any disposition"
    );
    assert!(Arc::ptr_eq(
        &reason,
        &subscription.terminal_failure().expect("cause remains")
    ));
}
#[test]
fn test_receive_stop_finishes_already_started_handler() {
    let mut source = Source::new(5, Some(SchemaId::new("v1").expect("schema")));
    source.first_good = true;
    let source = Arc::new(source);
    let bus =
        AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config()).expect("bus");
    let mut subscription = manual_async::block_on(
        bus.subscribe(
            SubscribeRequest::new(
                "inflight",
                Topic::new("boundary").expect("topic").with_shared_codec(Probe::new()),
            )
            .expect("request"),
        ),
    )
    .expect("subscription");
    let release = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let handler_release = release.clone();
    let handler_started = started.clone();
    let handler_finished = finished.clone();
    let mut run = Box::pin(subscription.run(move |_| {
        handler_started.fetch_add(1, Ordering::SeqCst);
        let release = handler_release.clone();
        let handler_finished = handler_finished.clone();
        std::future::poll_fn(move |_| {
            if release.load(Ordering::SeqCst) {
                handler_finished.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
    }));
    assert!(manual_async::poll_once(run.as_mut()).is_pending());
    assert_eq!(started.load(Ordering::SeqCst), 1);
    release.store(true, Ordering::SeqCst);
    assert!(matches!(manual_async::block_on(run), Err(ReceiveError::Stopped(_))));
    assert_eq!(finished.load(Ordering::SeqCst), 1, "started handler completes");
    assert_eq!(
        source.settled.load(Ordering::SeqCst),
        0,
        "terminal receive stop leaves the healthy started message unsettled"
    );
    assert_eq!(
        source.closed.load(Ordering::SeqCst),
        1,
        "receiver closes after the handler completes"
    );
    let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
}
#[test]
fn test_regular_decode_failure_rejects_without_terminal_stop() {
    for asynchronous in [false, true] {
        let source = Arc::new(Source::new(4, Some(SchemaId::new("v1").expect("schema"))));
        let topic = Topic::new("boundary").expect("topic").with_shared_codec(panic_probe(3));
        if asynchronous {
            let bus = AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
                .expect("bus");
            let mut subscription =
                manual_async::block_on(bus.subscribe(SubscribeRequest::new("malformed", topic).expect("request")))
                    .expect("subscription");
            manual_async::block_on(subscription.run(|_| async { panic!("no decoded delivery") }))
                .expect("ordinary bad message rejected");
            assert!(subscription.terminal_failure().is_none());
            let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
        } else {
            let bus = EventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
                .expect("bus");
            let subscription = bus
                .subscribe(SubscribeRequest::new("malformed", topic).expect("request"), |_| -> () {
                    panic!("no decoded delivery")
                })
                .expect("subscription");
            let deadline = Instant::now() + Duration::from_secs(5);
            while source.closed.load(Ordering::SeqCst) == 0 {
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }
            subscription.cancel().expect("cancel");
            assert!(subscription.terminal_failure().is_none());
            let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
        }
        assert_eq!(source.settled.load(Ordering::SeqCst), 1);
        assert_eq!(
            *source.dispositions.lock().expect("settlement log"),
            [DeliveryDisposition::Reject],
            "ordinary Decode must reject exactly once; neither Accept nor Retry is permitted"
        );
    }
}

#[test]
fn test_content_native_and_ephemeral_stop_contracts() {
    for native in [false, true] {
        for ephemeral in [false, true] {
            let mut source = Source::new(4, Some(SchemaId::new("v1").expect("schema")));
            source.native = native;
            source.ephemeral = ephemeral;
            source.content = ContentType::new("application/incompatible").expect("content");
            let source = Arc::new(source);
            let bus = AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
                .expect("bus");
            let probe = Probe::new();
            let mut subscription = manual_async::block_on(
                bus.subscribe(
                    SubscribeRequest::new(
                        "contract",
                        Topic::new("boundary").expect("topic").with_shared_codec(probe.clone()),
                    )
                    .expect("request"),
                ),
            )
            .expect("subscription");
            assert!(matches!(
                manual_async::block_on(subscription.run(|_| async { panic!("no handler on mismatch") })),
                Err(ReceiveError::Stopped(_))
            ));
            let reason = subscription.terminal_failure().expect("canonical failure");
            let SubscriptionStopReason::Codec { error, .. } = reason.as_ref() else {
                panic!("codec cause")
            };
            if native {
                assert!(matches!(error.as_ref(), CodecError::NativeTypeMismatch));
            } else {
                assert!(matches!(error.as_ref(), CodecError::MetadataMismatch { .. }));
            }
            assert_eq!(probe.validate.load(Ordering::SeqCst), usize::from(!native));
            assert_eq!(probe.decode.load(Ordering::SeqCst), 0);
            manual_async::block_on(subscription.close()).expect("close");
            let report = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
            assert_eq!(report.known_abandoned_deliveries, u64::from(ephemeral));
            assert_eq!(source.settled.load(Ordering::SeqCst), 0);
            assert!(
                source.dispositions.lock().expect("settlement log").is_empty(),
                "fail-stop must never settle with any disposition"
            );
        }
    }
}
#[test]
fn test_sync_permanent_panic_is_once_and_cached() {
    for mode in [1, 2] {
        let source = Arc::new(Source::new(4, Some(SchemaId::new("v1").expect("schema"))));
        let bus =
            EventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config()).expect("bus");
        let probe = panic_probe(mode);
        let subscription = bus
            .subscribe(
                SubscribeRequest::new(
                    "panic",
                    Topic::new("boundary").expect("topic").with_shared_codec(probe.clone()),
                )
                .expect("request"),
                |_| -> () { panic!("no handler after codec panic") },
            )
            .expect("subscription");
        let deadline = Instant::now() + Duration::from_secs(5);
        while source.closed.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        let reason = subscription.terminal_failure().expect("cached cause");
        subscription.cancel().expect("cancel");
        assert!(Arc::ptr_eq(
            &reason,
            &subscription.terminal_failure().expect("cause remains")
        ));
        assert_eq!(probe.validate.load(Ordering::SeqCst), 1);
        assert_eq!(probe.decode.load(Ordering::SeqCst), usize::from(mode == 2));
        assert_eq!(source.settled.load(Ordering::SeqCst), 0);
        assert!(
            source.dispositions.lock().expect("settlement log").is_empty(),
            "fail-stop must never settle with any disposition"
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    }
}

#[test]
fn test_explicit_legacy_schema_override_can_decode() {
    let source = Arc::new(Source::new(4, None));
    let bus =
        AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config()).expect("bus");
    let probe = panic_probe(4);
    let mut subscription = manual_async::block_on(
        bus.subscribe(
            SubscribeRequest::new(
                "legacy",
                Topic::new("boundary").expect("topic").with_shared_codec(probe.clone()),
            )
            .expect("request"),
        ),
    )
    .expect("subscription");
    manual_async::block_on(subscription.run(|_| async { Ok(()) })).expect("explicit metadata compatibility");
    assert_eq!(probe.validate.load(Ordering::SeqCst), 1);
    assert_eq!(probe.decode.load(Ordering::SeqCst), 1);
    assert!(subscription.terminal_failure().is_none());
    let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
}

#[test]
fn test_sync_receive_stop_drains_started_handler() {
    let mut source = Source::new(5, Some(SchemaId::new("v1").expect("schema")));
    source.first_good = true;
    source.wait_second.store(true, Ordering::SeqCst);
    let source = Arc::new(source);
    let bus =
        EventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config()).expect("bus");
    let (release, wait) = mpsc::channel();
    let wait = Arc::new(Mutex::new(wait));
    let handler_source = source.clone();
    let finished = Arc::new(AtomicUsize::new(0));
    let handler_finished = finished.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(
                "inflight",
                Topic::new("boundary").expect("topic").with_shared_codec(Probe::new()),
            )
            .expect("request"),
            move |_| {
                handler_source.wait_second.store(false, Ordering::SeqCst);
                wait.lock()
                    .expect("handler gate")
                    .recv()
                    .expect("release started handler");
                handler_finished.fetch_add(1, Ordering::SeqCst);
            },
        )
        .expect("subscription");
    let deadline = Instant::now() + Duration::from_secs(5);
    while subscription.terminal_failure().is_none() {
        assert!(Instant::now() < deadline, "second receive caches failure");
        thread::yield_now();
    }
    assert_eq!(
        source.closed.load(Ordering::SeqCst),
        0,
        "receiver waits for active handler"
    );
    release.send(()).expect("release handler");
    subscription.cancel().expect("cancel drains handler");
    assert_eq!(finished.load(Ordering::SeqCst), 1, "started handler completes");
    assert_eq!(
        source.settled.load(Ordering::SeqCst),
        0,
        "terminal receive stop leaves the healthy started message unsettled"
    );
    assert_eq!(source.closed.load(Ordering::SeqCst), 1);
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}
#[test]
fn test_schema_stop_recovers_after_explicit_compatibility_change() {
    let source = Arc::new(Source::new(4, None));
    let bus =
        AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config()).expect("bus");
    let mut stopped = manual_async::block_on(
        bus.subscribe(
            SubscribeRequest::new(
                "strict",
                Topic::new("boundary").expect("topic").with_shared_codec(Probe::new()),
            )
            .expect("request"),
        ),
    )
    .expect("subscription");
    assert!(matches!(
        manual_async::block_on(stopped.run(|_| async { Ok(()) })),
        Err(ReceiveError::Stopped(_))
    ));
    assert_eq!(source.settled.load(Ordering::SeqCst), 0);
    assert!(
        source.dispositions.lock().expect("settlement log").is_empty(),
        "fail-stop must never settle with any disposition"
    );
    let mut restored = manual_async::block_on(
        bus.subscribe(
            SubscribeRequest::new(
                "compatible",
                Topic::new("boundary").expect("topic").with_shared_codec(panic_probe(4)),
            )
            .expect("request"),
        ),
    )
    .expect("subscription");
    manual_async::block_on(restored.run(|_| async { Ok(()) })).expect("same durable record recovers");
    assert_eq!(source.settled.load(Ordering::SeqCst), 1);
    assert_eq!(source.closed.load(Ordering::SeqCst), 2);
    let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
}

#[test]
fn test_panicking_diagnostic_observer_preserves_cause_and_reports_stop_once() {
    for asynchronous in [false, true] {
        let mut source = Source::new(5, Some(SchemaId::new("v1").expect("schema")));
        source.ephemeral = true;
        let source = Arc::new(source);
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed = notifications.clone();
        let diagnostic_origins = Arc::new(Mutex::new(Vec::new()));
        let recorded_origins = diagnostic_origins.clone();
        let topic = Topic::new("boundary").expect("topic").with_shared_codec(Probe::new());
        if asynchronous {
            let bus = AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
                .expect("bus");
            let _observer = bus.observe_diagnostics(move |diagnostic| {
                observed.fetch_add(1, Ordering::SeqCst);
                recorded_origins.lock().expect("diagnostic log").push(match diagnostic {
                    Diagnostic::InternalFailure { origin, .. } => origin.to_string(),
                    _ => "other_diagnostic".to_owned(),
                });
                panic!("observer fails while reporting the terminal boundary");
            });
            let mut subscription =
                manual_async::block_on(bus.subscribe(SubscribeRequest::new("observer", topic).expect("request")))
                    .expect("subscription");
            let ReceiveError::Stopped(returned) =
                manual_async::block_on(subscription.run(|_| async { panic!("no handler") }))
                    .expect_err("boundary stops despite observer panic")
            else {
                panic!("stop cause")
            };
            let cause = subscription
                .terminal_failure()
                .expect("observer panic must not prevent cause caching");
            assert!(Arc::ptr_eq(&cause, &returned));
            for _ in 0..2 {
                let ReceiveError::Stopped(repeated) =
                    manual_async::block_on(subscription.run(|_| async { panic!("no handler") }))
                        .expect_err("repeat stopped")
                else {
                    panic!("same stop")
                };
                assert!(Arc::ptr_eq(&cause, &repeated));
                manual_async::block_on(subscription.close()).expect("repeated close");
            }
            let report = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
            assert_eq!(
                report.known_abandoned_deliveries, 1,
                "observer panic cannot prevent loss accounting"
            );
            assert!(Arc::ptr_eq(
                &cause,
                &subscription.terminal_failure().expect("retained cause")
            ));
        } else {
            let bus = EventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
                .expect("bus");
            let _observer = bus.observe_diagnostics(move |diagnostic| {
                observed.fetch_add(1, Ordering::SeqCst);
                recorded_origins.lock().expect("diagnostic log").push(match diagnostic {
                    Diagnostic::InternalFailure { origin, .. } => origin.to_string(),
                    _ => "other_diagnostic".to_owned(),
                });
                panic!("observer fails while reporting the terminal boundary");
            });
            let subscription = bus
                .subscribe(SubscribeRequest::new("observer", topic).expect("request"), |_| -> () {
                    panic!("no handler")
                })
                .expect("subscription");
            let deadline = Instant::now() + Duration::from_secs(5);
            while source.closed.load(Ordering::SeqCst) == 0 {
                assert!(Instant::now() < deadline, "receiver closes despite observer panic");
                thread::yield_now();
            }
            let cause = subscription.terminal_failure().expect("cached cause");
            subscription.cancel().expect("cancel");
            subscription.cancel().expect("repeated cancel");
            let report = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
            assert_eq!(report.known_abandoned_deliveries, 1);
            assert!(Arc::ptr_eq(
                &cause,
                &subscription.terminal_failure().expect("retained cause")
            ));
        }
        assert_eq!(
            notifications.load(Ordering::SeqCst),
            1,
            "repeat run/close/shutdown must not repeat terminal diagnostic"
        );
        assert_eq!(
            *diagnostic_origins.lock().expect("diagnostic log"),
            ["receive_boundary"],
            "caught observer panic cannot swallow the assertion about diagnostic identity"
        );
        assert!(source.dispositions.lock().expect("settlement log").is_empty());
    }
}

/// Checks the close error independently of the canonical codec stop reason.
fn assert_independent_close_errors(errors: &SubscriptionCloseErrors) {
    assert_eq!(errors.len(), 1, "one canonical close failure");
    let failure = errors.iter().next().expect("close failure");
    assert_eq!(failure.error().kind(), "injected_close_failure");
    assert_eq!(failure.error().operation(), "close");
    assert_eq!(failure.error().resource(), Some("boundary"));
    let source = StdError::source(failure.error()).expect("original provider error source");
    assert_eq!(source.to_string(), "receiver close failed independently");
}

#[test]
fn test_codec_stop_and_provider_close_failure_remain_separately_observable() {
    for asynchronous in [false, true] {
        let source = Arc::new(Source::new(5, Some(SchemaId::new("v1").expect("schema"))));
        source.close_failed.store(true, Ordering::SeqCst);
        let topic = Topic::new("boundary").expect("topic").with_shared_codec(Probe::new());
        let cause;
        if asynchronous {
            let bus = AsyncEventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
                .expect("bus");
            let mut subscription =
                manual_async::block_on(bus.subscribe(SubscribeRequest::new("close-error", topic).expect("request")))
                    .expect("subscription");
            let ReceiveError::Stopped(run_cause) =
                manual_async::block_on(subscription.run(|_| async { panic!("no handler") }))
                    .expect_err("run returns the first codec stop cause")
            else {
                panic!("codec stop cause")
            };
            cause = subscription.terminal_failure().expect("codec cause still available");
            assert!(Arc::ptr_eq(&cause, &run_cause));
            let ReceiveError::Stopped(repeated) =
                manual_async::block_on(subscription.run(|_| async { panic!("no handler") }))
                    .expect_err("same terminal cause")
            else {
                panic!("stop cause")
            };
            assert!(Arc::ptr_eq(&cause, &repeated));
            let LifecycleError::SubscriptionClose(close_errors) =
                manual_async::block_on(subscription.close()).expect_err("provider close remains independently failed")
            else {
                panic!("close failure snapshot")
            };
            assert_independent_close_errors(&close_errors);
            source.close_failed.store(false, Ordering::SeqCst);
            let ShutdownError::SubscriptionClose(shutdown_errors) =
                manual_async::block_on(bus.shutdown(ShutdownMode::Immediate))
                    .expect_err("shutdown retains canonical prior close failure")
            else {
                panic!("shutdown failure snapshot")
            };
            assert_independent_close_errors(&shutdown_errors);
            assert!(Arc::ptr_eq(
                &cause,
                &subscription.terminal_failure().expect("codec cause remains")
            ));
        } else {
            let bus = EventBus::with_config(ProviderId::new("probe").expect("provider"), source.clone(), config())
                .expect("bus");
            let subscription = bus
                .subscribe(
                    SubscribeRequest::new("close-error", topic).expect("request"),
                    |_| -> () { panic!("no handler") },
                )
                .expect("subscription");
            let deadline = Instant::now() + Duration::from_secs(5);
            while source.closed.load(Ordering::SeqCst) == 0 {
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }
            let LifecycleError::SubscriptionClose(close_errors) = subscription
                .cancel()
                .expect_err("provider close failure remains observable")
            else {
                panic!("close failure snapshot")
            };
            assert_independent_close_errors(&close_errors);
            cause = subscription
                .terminal_failure()
                .expect("codec cause independently available");
            let ShutdownError::SubscriptionClose(shutdown_errors) = bus
                .shutdown(ShutdownMode::Immediate)
                .expect_err("shutdown must retain close failure")
            else {
                panic!("shutdown failure snapshot")
            };
            assert_independent_close_errors(&shutdown_errors);
            assert!(Arc::ptr_eq(
                &cause,
                &subscription.terminal_failure().expect("codec cause remains")
            ));
        }
        let SubscriptionStopReason::Codec { event_id, error } = cause.as_ref() else {
            panic!("structured codec cause")
        };
        assert_eq!(event_id.as_str(), "source-record");
        assert!(matches!(
            error.as_ref(),
            CodecError::PayloadTooLarge {
                direction: PayloadDirection::Receive,
                actual: 5,
                limit: 4
            }
        ));
        assert!(
            source.dispositions.lock().expect("settlement log").is_empty(),
            "neither stop nor failed close settles the durable record"
        );
    }
}
