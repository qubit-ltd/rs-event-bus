// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Delivery snapshots observed through facade and retained subscription
//! handles.

mod support;

use std::future::Future;
use std::future::poll_fn;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::SpiError;
use qubit_event_bus::WaitOutcome;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::facade::DeliveryMetricsSnapshot;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SettlementRetryConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionDurability;
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
use qubit_event_bus::spi::TransportPayload;
use support::fake_spi::FakeAsyncEventBusSpi;
use support::fake_spi::FakeEventBusSpi;
use support::fake_spi::inbound_message;
use support::manual_async::block_on;
use support::manual_async::poll_once;
use support::settlement_probe::ProbeBus;
use support::settlement_probe::SettlementProbe;

#[test]
fn test_sync_delivery_metrics_empty_and_closed_handle() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("metrics").unwrap(), spi.clone()).unwrap();
    assert_eq!(bus.delivery_metrics(), DeliveryMetricsSnapshot::default());
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("metrics", Topic::<u32>::new("test.topic").unwrap()).unwrap(),
            |_: Delivery<u32>| Ok::<(), DeliveryError>(()),
        )
        .unwrap();
    subscription.cancel().unwrap();
    let calls = spi.operation_log();
    let snapshot = subscription.delivery_metrics();
    assert_eq!(snapshot.subscription_id, subscription.id());
    assert_eq!(snapshot.subscriber_id, *subscription.subscriber_id());
    assert_eq!(snapshot.metrics, DeliveryMetricsSnapshot::default());
    assert_eq!(bus.delivery_metrics(), DeliveryMetricsSnapshot::default());
    assert_eq!(calls, spi.operation_log(), "snapshots never call the provider");
}

#[test]
fn test_async_delivery_metrics_empty_and_closed_handle() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("metrics").unwrap(), spi.clone()).unwrap();
    assert_eq!(bus.delivery_metrics(), DeliveryMetricsSnapshot::default());
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("metrics", Topic::<u32>::new("test.topic").unwrap()).unwrap()))
            .unwrap();
    block_on(subscription.close()).unwrap();
    let calls = spi.operation_log();
    let snapshot = subscription.delivery_metrics();
    assert_eq!(snapshot.subscription_id, subscription.id());
    assert_eq!(snapshot.subscriber_id, *subscription.subscriber_id());
    assert_eq!(snapshot.metrics, DeliveryMetricsSnapshot::default());
    assert_eq!(bus.delivery_metrics(), DeliveryMetricsSnapshot::default());
    assert_eq!(calls, spi.operation_log(), "snapshots never call the provider");
}

/// A provider gate makes a settlement call observable before its result is
/// ready.
struct GatedAsyncProvider {
    fake: FakeAsyncEventBusSpi,
    released: Arc<AtomicBool>,
}

impl AsyncEventBusSpi for GatedAsyncProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        self.fake.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.fake.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let receiver = self.fake.subscribe(request).await?;
            Ok(Box::new(GatedAsyncReceiver {
                receiver,
                released: self.released.clone(),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.fake.shutdown(mode)
    }
}

/// Delays settlement completion until the test releases the provider gate.
struct GatedAsyncReceiver {
    receiver: Box<dyn AsyncEventSubscriptionSpi>,
    released: Arc<AtomicBool>,
}

impl AsyncEventSubscriptionSpi for GatedAsyncReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.receiver.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        let result = self.receiver.settle(token, disposition);
        let released = self.released.clone();
        Box::pin(async move {
            poll_fn(|_| {
                if released.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            result.await
        })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.receiver.close()
    }
}

/// Asserts the exact scheduler phase decomposition on a manually polled
/// executor.
fn assert_phases(snapshot: DeliveryMetricsSnapshot, reserved: u64, queued: u64, running: u64, settling: u64) {
    assert_eq!(
        (
            snapshot.reserved_receives,
            snapshot.queued,
            snapshot.running_handlers,
            snapshot.settling
        ),
        (reserved, queued, running, settling)
    );
}

#[test]
fn test_async_delivery_metrics_gated_phases_and_final_counters() {
    let clock = ManualMonotonicClock::new();
    let settled = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(AtomicBool::new(false));
    let spi = Arc::new(GatedAsyncProvider {
        fake: FakeAsyncEventBusSpi::new(),
        released: settled.clone(),
    });
    let positive = |n| NonZeroUsize::new(n).unwrap();
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(positive(1), positive(2), positive(2), positive(1)).unwrap(),
    );
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("metrics").unwrap(),
        spi.clone(),
        config,
        clock.new_timer(),
    )
    .unwrap();
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("metrics", Topic::<u32>::new("test.topic").unwrap()).unwrap()))
            .unwrap();
    let id = subscription.id();
    let gate = handler.clone();
    let mut run = Box::pin(subscription.run(move |_| {
        let gate = gate.clone();
        poll_fn(move |_| {
            if gate.load(Ordering::Acquire) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
    }));
    assert!(poll_once(run.as_mut()).is_pending());
    assert_phases(bus.delivery_metrics(), 1, 0, 0, 0);
    spi.fake.enqueue(inbound_message(Some(SettlementToken::new(id, "one"))));
    spi.fake.enqueue(inbound_message(Some(SettlementToken::new(id, "two"))));
    for _ in 0..8 {
        assert!(poll_once(run.as_mut()).is_pending());
    }
    assert_phases(bus.delivery_metrics(), 0, 1, 1, 0);
    clock.advance(Duration::from_nanos(7)).unwrap();
    assert_eq!(bus.delivery_metrics().oldest_owned_age, Some(Duration::from_nanos(7)));
    handler.store(true, Ordering::Release);
    assert!(poll_once(run.as_mut()).is_pending());
    let snapshot = bus.delivery_metrics();
    assert_phases(snapshot, 0, 1, 0, 1);
    assert_eq!(snapshot.handler_duration_count, 1);
    assert_eq!(snapshot.handler_duration_total_nanos, 7);
    assert_eq!(snapshot.settlement_attempts, 1);
    assert_eq!(snapshot.settlement_duration_count, 0);
    clock.advance(Duration::from_nanos(11)).unwrap();
    settled.store(true, Ordering::Release);
    for _ in 0..8 {
        assert!(poll_once(run.as_mut()).is_pending());
    }
    assert_eq!(bus.delivery_metrics().completed, 2);
    drop(run);
    block_on(subscription.close()).unwrap();
    let final_snapshot = subscription.delivery_metrics().metrics;
    assert_phases(final_snapshot, 0, 0, 0, 0);
    assert_eq!(final_snapshot.lane_waiting, 0);
    assert_eq!(final_snapshot.oldest_owned_age, None);
    assert_eq!(final_snapshot.completed, 2);
    assert_eq!(final_snapshot.settlement_attempts, 2);
    assert_eq!(final_snapshot.settlement_retries, 0);
    assert_eq!(final_snapshot.handler_duration_count, 2);
    assert_eq!(final_snapshot.settlement_duration_count, 2);
    assert_eq!(final_snapshot.settlement_duration_total_nanos, 11);
    assert_eq!(bus.delivery_metrics(), final_snapshot);
}

struct HandlerDurationProbe {
    clock: Arc<ManualMonotonicClock>,
    drops: Arc<AtomicBool>,
    advanced_nanos: Arc<AtomicUsize>,
    return_error: bool,
}

impl Future for HandlerDurationProbe {
    type Output = Result<(), DeliveryError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.clock.advance(Duration::from_nanos(7)).unwrap();
        self.advanced_nanos.fetch_add(7, Ordering::SeqCst);
        if self.return_error {
            Poll::Ready(Err(DeliveryError::Handler {
                source: Box::new(std::io::Error::other("probe failure")),
            }))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl Drop for HandlerDurationProbe {
    fn drop(&mut self) {
        self.clock.advance(Duration::from_nanos(11)).unwrap();
        self.advanced_nanos.fetch_add(11, Ordering::SeqCst);
        self.drops.store(true, Ordering::Release);
    }
}

#[test]
fn test_async_handler_duration_boundary_ready_ok_excludes_future_drop() {
    assert_ready_handler_duration_boundary(false);
}

#[test]
fn test_async_handler_duration_boundary_ready_error_excludes_future_drop() {
    assert_ready_handler_duration_boundary(true);
}

#[test]
fn test_async_handler_duration_boundary_paused_run_includes_suspension() {
    let clock = ManualMonotonicClock::new();
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("handler-duration-pause").unwrap(),
        spi.clone(),
        EventBusFacadeConfig::new(),
        clock.new_timer(),
    )
    .unwrap();
    let mut subscription = block_on(
        bus.subscribe(SubscribeRequest::new("duration-pause", Topic::<u32>::new("test.topic").unwrap()).unwrap()),
    )
    .unwrap();
    spi.enqueue(inbound_message(None));
    let ready = Arc::new(AtomicBool::new(false));
    let handler_ready = ready.clone();
    let mut paused = Box::pin(subscription.run(move |_| {
        let ready = handler_ready.clone();
        poll_fn(move |_| {
            if ready.load(Ordering::Acquire) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
    }));
    for _ in 0..8 {
        assert!(poll_once(paused.as_mut()).is_pending());
        if bus.delivery_metrics().running_handlers == 1 {
            break;
        }
    }
    assert_eq!(
        bus.delivery_metrics().running_handlers,
        1,
        "handler is suspended before run is dropped"
    );
    assert_eq!(bus.delivery_metrics().handler_duration_count, 0);
    drop(paused);
    clock.advance(Duration::from_nanos(7)).unwrap();
    assert_eq!(bus.delivery_metrics().handler_duration_count, 0);
    clock.advance(Duration::from_nanos(5)).unwrap();
    ready.store(true, Ordering::Release);
    let handler_ready = ready.clone();
    let mut resumed = Box::pin(subscription.run(move |_| {
        let ready = handler_ready.clone();
        poll_fn(move |_| {
            if ready.load(Ordering::Acquire) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
    }));
    for _ in 0..8 {
        assert!(poll_once(resumed.as_mut()).is_pending());
        if bus.delivery_metrics().handler_duration_count == 1 {
            break;
        }
    }
    let metrics = bus.delivery_metrics();
    assert_eq!(metrics.handler_duration_count, 1);
    assert_eq!(metrics.handler_duration_total_nanos, 12);
    assert_eq!(metrics.handler_duration_max_nanos, 12);
    drop(resumed);
    block_on(subscription.close()).unwrap();
}

fn assert_ready_handler_duration_boundary(return_error: bool) {
    let clock = Arc::new(ManualMonotonicClock::new());
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("handler-duration").unwrap(),
        spi.clone(),
        EventBusFacadeConfig::new(),
        clock.new_timer(),
    )
    .unwrap();
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("duration", Topic::<u32>::new("test.topic").unwrap()).unwrap()))
            .unwrap();
    spi.enqueue(inbound_message(None));
    let drops = Arc::new(AtomicBool::new(false));
    let advanced_nanos = Arc::new(AtomicUsize::new(0));
    let handler_drops = drops.clone();
    let handler_clock = clock.clone();
    let handler_advanced_nanos = advanced_nanos.clone();
    let handler_return_error = return_error;
    let mut run = Box::pin(subscription.run(move |_| HandlerDurationProbe {
        clock: handler_clock.clone(),
        drops: handler_drops.clone(),
        advanced_nanos: handler_advanced_nanos.clone(),
        return_error: handler_return_error,
    }));
    for _ in 0..8 {
        assert!(poll_once(run.as_mut()).is_pending());
        if drops.load(Ordering::Acquire) {
            break;
        }
    }
    assert!(drops.load(Ordering::Acquire), "ready handler future is destroyed");
    assert_eq!(advanced_nanos.load(Ordering::SeqCst), 18);
    let metrics = bus.delivery_metrics();
    assert_eq!(metrics.handler_duration_count, 1);
    assert_eq!(metrics.handler_duration_total_nanos, 7);
    assert_eq!(metrics.handler_duration_max_nanos, 7);
    drop(run);
    block_on(subscription.close()).unwrap();
}

#[test]
fn test_async_delivery_metrics_retry_attempts_and_deadline_admission() {
    for expire in [false, true] {
        let clock = ManualMonotonicClock::new();
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        let retry = SettlementRetryConfig::new(
            NonZeroU32::new(3).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let config = EventBusFacadeConfig::new().with_settlement_retry(retry);
        let bus = AsyncEventBus::with_config_and_timer(
            ProviderId::new("metrics").unwrap(),
            spi.clone(),
            config,
            clock.new_timer(),
        )
        .unwrap();
        let mut subscription = block_on(
            bus.subscribe(SubscribeRequest::new("metrics", Topic::<u32>::new("test.topic").unwrap()).unwrap()),
        )
        .unwrap();
        spi.fail_next_settle();
        spi.enqueue(inbound_message(Some(SettlementToken::new(subscription.id(), "retry"))));
        let mut run = Box::pin(subscription.run(|_| async { Ok(()) }));
        for _ in 0..4 {
            assert!(poll_once(run.as_mut()).is_pending());
        }
        assert_eq!(bus.delivery_metrics().settlement_attempts, 1);
        assert_eq!(bus.delivery_metrics().settlement_retries, 0);
        clock.advance(Duration::from_secs(if expire { 2 } else { 1 })).unwrap();
        let result = poll_once(run.as_mut());
        if expire {
            assert!(result.is_ready());
        } else {
            assert!(result.is_pending());
        }
        drop(run);
        block_on(subscription.close()).unwrap();
        let metrics = subscription.delivery_metrics().metrics;
        assert_eq!(metrics.settlement_attempts, if expire { 1 } else { 2 });
        assert_eq!(metrics.settlement_retries, if expire { 0 } else { 1 });
        assert_eq!(metrics.settlement_terminal_failures, u64::from(expire));
        assert_eq!(metrics.completed, u64::from(!expire));
        assert_eq!(metrics.settlement_duration_count, 1);
        assert_phases(metrics, 0, 0, 0, 0);
    }
}

#[test]
fn test_async_delivery_metrics_permanent_error_keeps_shared_source() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("metrics").unwrap(), spi.clone()).unwrap();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let observed = errors.clone();
    let reentrant_bus = bus.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        let _ = reentrant_bus.delivery_metrics();
        match diagnostic {
            Diagnostic::SettlementFailed { attempt, error, .. } => {
                assert_eq!(*attempt, 1);
                observed.lock().unwrap().push(error.clone());
            }
            Diagnostic::SettlementStopped { error, .. } => observed.lock().unwrap().push(error.clone()),
            _ => {}
        }
    });
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("metrics", Topic::<u32>::new("test.topic").unwrap()).unwrap()))
            .unwrap();
    spi.fail_all_settles();
    spi.enqueue(inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "permanent",
    ))));
    assert!(block_on(subscription.run(|_| async { Ok(()) })).is_err());
    block_on(subscription.close()).unwrap();
    let metrics = subscription.delivery_metrics().metrics;
    assert_eq!(metrics.settlement_attempts, 1);
    assert_eq!(metrics.settlement_retries, 0);
    assert_eq!(metrics.settlement_terminal_failures, 1);
    assert_eq!(metrics.completed, 0);
    assert_eq!(metrics.abandoned_ephemeral, 1);
    assert_phases(metrics, 0, 0, 0, 0);
    let errors = errors.lock().unwrap();
    assert_eq!(errors.len(), 2);
    assert!(Arc::ptr_eq(&errors[0], &errors[1]));
    assert_eq!(bus.delivery_metrics(), metrics);
}

#[test]
fn test_async_delivery_metrics_decode_failure_does_not_sample_handler() {
    let settled = Arc::new(AtomicBool::new(false));
    let spi = Arc::new(GatedAsyncProvider {
        fake: FakeAsyncEventBusSpi::new(),
        released: settled.clone(),
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("metrics").unwrap(), spi.clone()).unwrap();
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("metrics", Topic::<String>::new("test.topic").unwrap()).unwrap()))
            .unwrap();
    let (address, event_id, timestamp, headers, ordering_key, _, token, metadata) =
        inbound_message(Some(SettlementToken::new(subscription.id(), "missing-codec"))).into_parts();
    spi.fake.enqueue(InboundMessage::new(
        address,
        event_id,
        timestamp,
        headers,
        ordering_key,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(b"invalid".as_slice()),
            ContentType::new("text/plain").unwrap(),
            None,
        )),
        token,
        metadata,
    ));
    let mut run = Box::pin(subscription.run(|_| async {
        panic!("decode failure must not enter handler");
    }));
    for _ in 0..4 {
        assert!(poll_once(run.as_mut()).is_pending());
    }
    let snapshot = bus.delivery_metrics();
    assert_eq!(snapshot.running_handlers, 0);
    assert_eq!(snapshot.settling, 1);
    assert_eq!(snapshot.handler_duration_count, 0);
    assert_eq!(snapshot.settlement_attempts, 1);
    settled.store(true, Ordering::Release);
    for _ in 0..4 {
        assert!(poll_once(run.as_mut()).is_pending());
    }
    drop(run);
    block_on(subscription.close()).unwrap();
    let metrics = subscription.delivery_metrics().metrics;
    assert_phases(metrics, 0, 0, 0, 0);
    assert_eq!(metrics.completed, 1);
    assert_eq!(metrics.handler_duration_count, 0);
    assert_eq!(metrics.settlement_attempts, 1);
}

#[test]
fn test_sync_delivery_metrics_retry_and_permanent_final_counters() {
    for permanent in [false, true] {
        let spi = ProbeBus::new();
        let probe = SettlementProbe::new(!permanent, 1);
        spi.register("metrics", probe.clone());
        let bus = EventBus::from_spi(ProviderId::new("metrics").unwrap(), spi).unwrap();
        let topic = Topic::<u32>::new("test.topic").unwrap();
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("metrics", topic.clone()).unwrap(),
                |_: Delivery<u32>| Ok::<(), DeliveryError>(()),
            )
            .unwrap();
        let _ = bus.publish(PublishRequest::new(topic.clone(), 42).unwrap()).unwrap();
        if permanent {
            assert!(probe.closed.wait(1), "permanent failure closes the owner");
        } else {
            assert!(probe.entered.wait(2), "retry reaches actual SPI call two");
            assert_eq!(
                bus.wait_for_received_deliveries(&topic, Some(Duration::from_secs(2)))
                    .unwrap(),
                WaitOutcome::Idle
            );
        }
        subscription.cancel().unwrap();
        let metrics = subscription.delivery_metrics().metrics;
        assert_eq!(metrics.settlement_attempts, if permanent { 1 } else { 2 });
        assert_eq!(metrics.settlement_retries, u64::from(!permanent));
        assert_eq!(metrics.settlement_terminal_failures, u64::from(permanent));
        assert_eq!(metrics.completed, u64::from(!permanent));
        assert_eq!(metrics.abandoned_ephemeral, u64::from(permanent));
        assert_eq!(metrics.handler_duration_count, 1);
        assert_eq!(metrics.settlement_duration_count, 1);
        assert_phases(metrics, 0, 0, 0, 0);
        assert_eq!(bus.delivery_metrics(), metrics);
    }
}

struct DurableFailingSettleBus {
    inner: FakeEventBusSpi,
    settlement_started: mpsc::Sender<()>,
}

impl EventBusSpi for DurableFailingSettleBus {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(DurableFailingSettleSubscription {
            inner: self.inner.subscribe(request)?,
            settlement_started: self.settlement_started.clone(),
        }))
    }

    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}

struct DurableFailingSettleSubscription {
    inner: Box<dyn EventSubscriptionSpi>,
    settlement_started: mpsc::Sender<()>,
}

impl EventSubscriptionSpi for DurableFailingSettleSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        self.inner.receive(timeout)
    }

    fn settle(&mut self, _token: &SettlementToken, _disposition: DeliveryDisposition) -> Result<(), SpiError> {
        let _ = self.settlement_started.send(());
        Err(SpiError::Operation {
            provider_id: "durable-metrics".into(),
            operation: "settle",
            resource: None,
            kind: "injected_failure",
            retryable: Some(false),
            source: Box::new(std::io::Error::other("injected durable settlement failure")),
        })
    }

    fn close(&mut self) -> Result<(), SpiError> {
        self.inner.close()
    }
}

#[test]
fn test_durable_settlement_failure_is_not_counted_as_ephemeral_abandonment() {
    let capabilities = EventBusCapabilities::builder()
        .payload_modes(PayloadModes::Native)
        .settlement(SettlementCapabilities::AcceptRetryReject)
        .ordering(OrderingCapability::None)
        .delayed_delivery(DelayedDeliveryCapability::None)
        .durability(DurabilityCapability::Durable)
        .subscription_modes(SubscriptionModes::DURABLE)
        .consumer_groups(false)
        .replay(ReplayCapability::None)
        .publish_guarantee(PublishGuarantee::Accepted)
        .publish_visibility(PublishVisibility::Opaque)
        .build()
        .expect("durable test capabilities should be complete");
    let fake = FakeEventBusSpi::with_capabilities(capabilities);
    let (settlement_started_tx, settlement_started_rx) = mpsc::channel();
    let spi = Arc::new(DurableFailingSettleBus {
        inner: fake.clone(),
        settlement_started: settlement_started_tx,
    });
    let bus = EventBus::from_spi(ProviderId::new("durable-metrics").unwrap(), spi).unwrap();
    let topic = Topic::<u32>::new("test.topic").unwrap();
    let options = SubscribeOptions::<u32>::builder()
        .durability(SubscriptionDurability::Durable)
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("durable-metrics", topic.clone())
                .unwrap()
                .with_options(options),
            |_: Delivery<u32>| Ok::<(), DeliveryError>(()),
        )
        .unwrap();
    let receipt = bus
        .publish(PublishRequest::new(topic, 42).unwrap())
        .expect("durable fake provider should admit the event");
    assert!(matches!(
        receipt.acknowledgement(),
        PublishAcknowledgement::Accepted { .. }
    ));
    settlement_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("worker should attempt durable settlement");
    subscription.cancel().unwrap();

    assert_eq!(subscription.delivery_metrics().metrics.abandoned_ephemeral, 0);
    assert_eq!(bus.delivery_metrics().abandoned_ephemeral, 0);
}
