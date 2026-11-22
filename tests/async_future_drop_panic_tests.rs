// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public async API regression probes for future destruction panics.
mod support;

use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::SpiError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::error::SubscribeError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::fake_spi::inbound_message;
use crate::support::manual_async::block_on;
use crate::support::manual_async::poll_once;
use crate::support::panic_hook::PanicHookGuard;

struct DropPanicSpi {
    inner: FakeAsyncEventBusSpi,
    drops: Arc<AtomicUsize>,
    pending: bool,
    shutdown_drop_panic: bool,
}

impl AsyncEventBusSpi for DropPanicSpi {
    fn capabilities(&self) -> qubit_event_bus::spi::EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        if self.pending {
            Box::pin(PendingThenDropPanic(self.drops.clone()))
        } else {
            Box::pin(AcceptThenDropPanic(self.drops.clone()))
        }
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.inner.subscribe(request)
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        if self.shutdown_drop_panic && self.drops.load(Ordering::SeqCst) == 0 {
            return Box::pin(ReadyShutdownDropPanic {
                drops: self.drops.clone(),
            });
        }
        self.inner.shutdown(mode)
    }
}

struct SubscribeCancelProbe {
    bus: Mutex<Option<AsyncEventBus>>,
    reentry: Mutex<Option<Result<(), ShutdownError>>>,
    drops: Arc<AtomicUsize>,
}

struct PendingSubscribeDropPanic(Arc<SubscribeCancelProbe>);

impl Future for PendingSubscribeDropPanic {
    type Output = Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingSubscribeDropPanic {
    fn drop(&mut self) {
        let bus = self.0.bus.lock().unwrap().as_ref().unwrap().clone();
        let result = match Box::pin(bus.shutdown(ShutdownMode::Immediate))
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
        {
            Poll::Ready(result) => result.map(|_| ()),
            Poll::Pending => panic!("same-bus shutdown should be rejected immediately"),
        };
        *self.0.reentry.lock().unwrap() = Some(result);
        self.0.drops.fetch_add(1, Ordering::SeqCst);
        panic!("subscribe future destructor panic");
    }
}

struct SubscribeDropPanicSpi {
    inner: FakeAsyncEventBusSpi,
    calls: AtomicUsize,
    probe: Arc<SubscribeCancelProbe>,
}

struct CloseCountReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    close_calls: Arc<AtomicUsize>,
}

impl AsyncEventSubscriptionSpi for CloseCountReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.inner.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.inner.settle(token, disposition)
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.close()
    }
}

struct ReadySubscribeDropPanic<'a> {
    inner: SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>>,
    drops: Arc<AtomicUsize>,
}

impl Future for ReadySubscribeDropPanic<'_> {
    type Output = Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

impl Drop for ReadySubscribeDropPanic<'_> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("ready subscribe future destructor panic");
    }
}

struct ReadySubscribeErrorDropPanic {
    drops: Arc<AtomicUsize>,
}

impl Future for ReadySubscribeErrorDropPanic {
    type Output = Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(SpiError::Operation {
            provider_id: "subscribe-ready-error-drop".into(),
            operation: "subscribe",
            resource: Some("broken".into()),
            kind: "original_subscribe_error",
            retryable: Some(false),
            source: Box::new(std::io::Error::other("original subscribe source")),
        }))
    }
}

impl Drop for ReadySubscribeErrorDropPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("ready error subscribe future destructor panic");
    }
}

struct ReadyErrorDropSubscribeSpi {
    inner: FakeAsyncEventBusSpi,
    calls: AtomicUsize,
    drops: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for ReadyErrorDropSubscribeSpi {
    fn capabilities(&self) -> qubit_event_bus::spi::EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(ReadySubscribeErrorDropPanic {
                drops: self.drops.clone(),
            })
        } else {
            self.inner.subscribe(request)
        }
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

struct ReadyDropSubscribeSpi {
    inner: FakeAsyncEventBusSpi,
    drops: Arc<AtomicUsize>,
    close_calls: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for ReadyDropSubscribeSpi {
    fn capabilities(&self) -> qubit_event_bus::spi::EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let inner = self.inner.clone();
        let close_calls = self.close_calls.clone();
        let future = Box::pin(async move {
            let receiver = inner.subscribe(request).await?;
            Ok(Box::new(CloseCountReceiver {
                inner: receiver,
                close_calls,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        });
        Box::pin(ReadySubscribeDropPanic {
            inner: future,
            drops: self.drops.clone(),
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

impl AsyncEventBusSpi for SubscribeDropPanicSpi {
    fn capabilities(&self) -> qubit_event_bus::spi::EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(PendingSubscribeDropPanic(self.probe.clone()))
        } else {
            self.inner.subscribe(request)
        }
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

#[test]
fn test_async_subscribe_drop_panic_preserves_registration_and_result() {
    let _hook = PanicHookGuard::suppress();
    let probe = Arc::new(SubscribeCancelProbe {
        bus: Mutex::new(None),
        reentry: Mutex::new(None),
        drops: Arc::new(AtomicUsize::new(0)),
    });
    let spi = Arc::new(SubscribeDropPanicSpi {
        inner: FakeAsyncEventBusSpi::new(),
        calls: AtomicUsize::new(0),
        probe: probe.clone(),
    });
    let limit = NonZeroUsize::new(1).unwrap();
    let config = EventBusFacadeConfig::new()
        .with_delivery_scheduling(DeliverySchedulingConfig::new(limit, limit, limit, limit).unwrap());
    let bus = AsyncEventBus::with_config(ProviderId::new("subscribe-drop-panic").unwrap(), spi, config).unwrap();
    *probe.bus.lock().unwrap() = Some(bus.clone());

    let request = SubscribeRequest::new("cancelled", Topic::<u32>::new("subscribe-cancelled").unwrap()).unwrap();
    let mut subscribe = Box::pin(bus.subscribe(request));
    assert!(matches!(
        subscribe
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop())),
        Poll::Pending
    ));
    let cancelled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(subscribe)));
    assert!(cancelled.is_ok(), "cancel cleanup panic must not escape drop");
    assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        probe.reentry.lock().unwrap().take().unwrap(),
        Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
            operation: "shutdown"
        }))
    ));

    let replacement =
        block_on(bus.subscribe(
            SubscribeRequest::new("replacement", Topic::<u32>::new("subscribe-replacement").unwrap()).unwrap(),
        ))
        .expect("cancelled registration and in-flight count are released");
    drop(replacement);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("bus remains usable and shuts down");
}

#[test]
fn test_async_subscribe_ready_result_survives_future_drop_panic() {
    let drops = Arc::new(AtomicUsize::new(0));
    let close_calls = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(ReadyDropSubscribeSpi {
        inner: FakeAsyncEventBusSpi::new(),
        drops: drops.clone(),
        close_calls: close_calls.clone(),
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("subscribe-ready-drop-panic").unwrap(), spi).unwrap();
    let subscription =
        block_on(bus.subscribe(SubscribeRequest::new("ready", Topic::<u32>::new("subscribe-ready").unwrap()).unwrap()))
            .expect("ready receiver must be retained across destructor panic");
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown closes admitted receiver");
    assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    drop(subscription);
}

#[test]
fn test_async_subscribe_ready_error_survives_future_drop_panic() {
    let drops = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(ReadyErrorDropSubscribeSpi {
        inner: FakeAsyncEventBusSpi::new(),
        calls: AtomicUsize::new(0),
        drops: drops.clone(),
    });
    let one = NonZeroUsize::new(1).unwrap();
    let config = EventBusFacadeConfig::new()
        .with_delivery_scheduling(DeliverySchedulingConfig::new(one, one, one, one).unwrap());
    let bus = AsyncEventBus::with_config(ProviderId::new("subscribe-ready-error-drop").unwrap(), spi, config).unwrap();
    let error = match block_on(
        bus.subscribe(SubscribeRequest::new("broken", Topic::<u32>::new("subscribe-broken").unwrap()).unwrap()),
    ) {
        Ok(_) => panic!("provider error remains the subscribe outcome"),
        Err(error) => error,
    };
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let SubscribeError::Spi(error) = error else {
        panic!("expected original SPI error");
    };
    assert!(matches!(
        &error,
        SpiError::Operation {
            operation: "subscribe",
            kind: "original_subscribe_error",
            ..
        }
    ));
    assert_eq!(
        std::error::Error::source(&error).unwrap().to_string(),
        "original subscribe source"
    );

    let replacement =
        block_on(bus.subscribe(
            SubscribeRequest::new("replacement", Topic::<u32>::new("subscribe-replacement").unwrap()).unwrap(),
        ))
        .expect("failed registration and subscription capacity are released");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("replacement closes normally");
    drop(replacement);
}

struct AcceptThenDropPanic(Arc<AtomicUsize>);
impl Future for AcceptThenDropPanic {
    type Output = Result<PublishAcknowledgement, SpiError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        }))
    }
}
impl Drop for AcceptThenDropPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("provider future cleanup panic");
    }
}

struct PendingThenDropPanic(Arc<AtomicUsize>);
impl Future for PendingThenDropPanic {
    type Output = Result<PublishAcknowledgement, SpiError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

struct PendingCleanupDropPanic<T>(Arc<AtomicUsize>, PhantomData<fn() -> T>);
impl<T> Future for PendingCleanupDropPanic<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl<T> Drop for PendingCleanupDropPanic<T> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("pending cleanup future destructor panic");
    }
}

struct PendingCleanupDropPanicSpi {
    inner: FakeAsyncEventBusSpi,
    pending_shutdown: bool,
    pending_close: bool,
    shutdown_drops: Arc<AtomicUsize>,
    shutdown_calls: Arc<AtomicUsize>,
    close_drops: Arc<AtomicUsize>,
    close_calls: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for PendingCleanupDropPanicSpi {
    fn capabilities(&self) -> qubit_event_bus::spi::EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let future = self.inner.subscribe(request);
        let close_drops = self.close_drops.clone();
        let close_calls = self.close_calls.clone();
        let pending_close = self.pending_close;
        Box::pin(async move {
            let receiver = future.await?;
            Ok(Box::new(PendingCleanupDropPanicReceiver {
                inner: receiver,
                pending_close,
                close_drops,
                close_calls,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        if self.pending_shutdown && self.shutdown_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(PendingCleanupDropPanic(self.shutdown_drops.clone(), PhantomData))
        } else {
            self.inner.shutdown(mode)
        }
    }
}

struct PendingCleanupDropPanicReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    pending_close: bool,
    close_drops: Arc<AtomicUsize>,
    close_calls: Arc<AtomicUsize>,
}
impl AsyncEventSubscriptionSpi for PendingCleanupDropPanicReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.inner.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.inner.settle(token, disposition)
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        if self.pending_close && self.close_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(PendingCleanupDropPanic(self.close_drops.clone(), PhantomData))
        } else {
            self.inner.close()
        }
    }
}

fn pending_cleanup_drop_panic_bus(
    provider_id: &str,
    clock: &ManualMonotonicClock,
    pending_shutdown: bool,
    pending_close: bool,
) -> (AsyncEventBus, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let shutdown_drops = Arc::new(AtomicUsize::new(0));
    let close_drops = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(PendingCleanupDropPanicSpi {
        inner: FakeAsyncEventBusSpi::new(),
        pending_shutdown,
        pending_close,
        shutdown_drops: shutdown_drops.clone(),
        shutdown_calls: Arc::new(AtomicUsize::new(0)),
        close_drops: close_drops.clone(),
        close_calls: Arc::new(AtomicUsize::new(0)),
    });
    let bus = AsyncEventBus::with_timer(
        ProviderId::new(provider_id).expect("provider ID"),
        spi,
        clock.new_timer(),
    )
    .expect("facade");
    (bus, shutdown_drops, close_drops)
}

#[test]
fn test_pending_async_spi_cleanup_drop_panic_preserves_shutdown_progress() {
    let _hook = PanicHookGuard::suppress();
    let waker = std::task::Waker::noop();

    let clock = ManualMonotonicClock::new();
    let (bus, _, close_drops) = pending_cleanup_drop_panic_bus("close-timeout-drop-panic", &clock, false, true);
    let topic = Topic::<u32>::new("close-timeout-drop-panic").expect("topic");
    let _subscription = block_on(bus.subscribe(SubscribeRequest::new("close-timeout-drop-panic", topic).unwrap()));
    let timeout = Duration::from_nanos(19);
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful { timeout }));
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(waker)).is_pending());
    clock.advance(timeout).expect("advance receiver close timeout");
    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(waker)),
        Poll::Ready(Err(ShutdownError::TimedOut { timeout: elapsed })) if elapsed == timeout
    ));
    assert_eq!(close_drops.load(Ordering::SeqCst), 1);
    assert!(bus.shutdown_report().is_none());
    assert!(block_on(bus.shutdown(ShutdownMode::Immediate)).is_ok());
    assert!(bus.shutdown_report().is_some());

    let clock = ManualMonotonicClock::new();
    let (bus, shutdown_drops, _) = pending_cleanup_drop_panic_bus("shutdown-timeout-drop-panic", &clock, true, false);
    let timeout = Duration::from_nanos(23);
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful { timeout }));
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(waker)).is_pending());
    clock.advance(timeout).expect("advance provider shutdown timeout");
    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(waker)),
        Poll::Ready(Err(ShutdownError::TimedOut { timeout: elapsed })) if elapsed == timeout
    ));
    assert_eq!(shutdown_drops.load(Ordering::SeqCst), 1);
    assert!(bus.shutdown_report().is_none());
    assert!(block_on(bus.shutdown(ShutdownMode::Immediate)).is_ok());
    assert!(bus.shutdown_report().is_some());

    let clock = ManualMonotonicClock::new();
    let (bus, shutdown_drops, _) = pending_cleanup_drop_panic_bus("shutdown-upgrade-drop-panic", &clock, true, false);
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(graceful.as_mut().poll(&mut Context::from_waker(waker)).is_pending());
    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(immediate.as_mut().poll(&mut Context::from_waker(waker)).is_pending());
    let mut graceful_result = None;
    let mut immediate_result = None;
    for _ in 0..8 {
        if graceful_result.is_none()
            && let Poll::Ready(result) = graceful.as_mut().poll(&mut Context::from_waker(waker))
        {
            graceful_result = Some(result);
        }
        if immediate_result.is_none()
            && let Poll::Ready(result) = immediate.as_mut().poll(&mut Context::from_waker(waker))
        {
            immediate_result = Some(result);
        }
    }
    assert!(graceful_result.is_some_and(|result| result.is_ok()));
    assert!(immediate_result.is_some_and(|result| result.is_ok()));
    assert_eq!(shutdown_drops.load(Ordering::SeqCst), 1);
    assert!(bus.shutdown_report().is_some());

    let clock = ManualMonotonicClock::new();
    let (bus, shutdown_drops, _) = pending_cleanup_drop_panic_bus("shutdown-cancel-drop-panic", &clock, true, false);
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(waker)).is_pending());
    let drop_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(shutdown)));
    assert!(drop_result.is_ok(), "cancelling shutdown must not unwind");
    assert_eq!(shutdown_drops.load(Ordering::SeqCst), 1);
    assert!(block_on(bus.shutdown(ShutdownMode::Immediate)).is_ok());
    assert!(bus.shutdown_report().is_some());
}
impl Drop for PendingThenDropPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("pending provider future cleanup panic");
    }
}

#[test]
fn test_future_drop_panic_public_async_publish_cancellation_has_no_result() {
    let _hook = PanicHookGuard::suppress();
    let drops = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(DropPanicSpi {
        inner: FakeAsyncEventBusSpi::new(),
        drops: drops.clone(),
        pending: true,
        shutdown_drop_panic: false,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("cancel-drop-panic").unwrap(), spi).unwrap();
    let request = PublishRequest::new(Topic::<u32>::new("future.cancel.drop.panic").unwrap(), 7).unwrap();
    let mut publish = Box::pin(bus.publish(request));
    assert!(matches!(
        publish
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop())),
        Poll::Pending
    ));
    let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(publish)));
    assert!(cleanup.is_ok(), "cancel cleanup panic must not escape Drop");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "pending provider future is destroyed once"
    );
    assert_eq!(
        bus.publish_metrics().attempts,
        1,
        "cancelled attempted publish has no terminal result"
    );
}

#[test]
fn test_async_cancelled_spi_drop_rejects_same_bus_shutdown_publish() {
    let (bus, _, drops, _, _, reentry) = future_drop_panic_bus("none", FutureDropFault::PendingPublishReentry);
    let request = PublishRequest::new(Topic::<u32>::new("publish-drop-reentry").unwrap(), 7).unwrap();
    let mut publish = Box::pin(bus.publish(request));
    assert!(
        publish
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    drop(publish);

    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "pending publish future is destroyed once"
    );
    assert_eq!(reentry.drops.load(Ordering::SeqCst), 1);
    assert!(
        !reentry.pending.load(Ordering::SeqCst),
        "nested shutdown must not remain pending"
    );
    assert!(matches!(
        reentry.result.lock().unwrap().as_ref(),
        Some(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
            operation: "shutdown"
        })))
    ));
    assert_eq!(bus.publish_metrics().cancelled, 1);
    assert_eq!(bus.publish_metrics().errors, 0);
    assert!(block_on(bus.shutdown(ShutdownMode::Immediate)).is_ok());
    *reentry.bus.lock().unwrap() = None;
}

#[test]
fn test_future_drop_panic_public_async_publish_ready_cleanup_becomes_uncertain_spi_error() {
    let _hook = PanicHookGuard::suppress();
    let drops = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(DropPanicSpi {
        inner: FakeAsyncEventBusSpi::new(),
        drops: drops.clone(),
        pending: false,
        shutdown_drop_panic: false,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("drop-panic").unwrap(), spi).unwrap();
    let request = PublishRequest::new(Topic::<u32>::new("future.drop.panic").unwrap(), 42).unwrap();

    let result = block_on(bus.publish(request));
    let Err(failure) = result else {
        panic!("provider cleanup panic must fail publication");
    };
    let PublishError::Spi(error) = failure.into_cause() else {
        panic!("provider cleanup panic must be classified as a structured SPI error");
    };
    assert!(matches!(
        &error,
        SpiError::Operation {
            kind: "provider_panicked",
            ..
        }
    ));
    assert_eq!(
        error.publish_effect(),
        qubit_event_bus::model::PublishEffect::MayHaveBeenAccepted
    );
    let source = std::error::Error::source(&error).expect("provider panic source is retained");
    assert!(source.to_string().contains("provider future cleanup panicked"));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the provider future must be destroyed once"
    );
    assert_eq!(
        bus.publish_metrics().attempts,
        1,
        "the provider call has occurred, so it cannot be reported as not accepted"
    );
}

struct ReadyShutdownDropPanic {
    drops: Arc<AtomicUsize>,
}

impl Future for ReadyShutdownDropPanic {
    type Output = Result<ShutdownOutcome, SpiError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(ShutdownOutcome::Complete))
    }
}

impl Drop for ReadyShutdownDropPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("provider shutdown future cleanup panic");
    }
}

#[test]
fn test_finished_async_spi_cleanup_drop_panic_is_structured() {
    let _hook = PanicHookGuard::suppress();
    let drops = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(DropPanicSpi {
        inner: FakeAsyncEventBusSpi::new(),
        drops: drops.clone(),
        pending: false,
        shutdown_drop_panic: true,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("shutdown-drop-panic").unwrap(), spi).unwrap();

    let first = block_on(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(
        first,
        Err(ShutdownError::Spi(SpiError::Operation {
            operation: "shutdown",
            kind: "provider_panicked",
            ..
        }))
    ));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "failed cleanup future is dropped exactly once"
    );
    assert!(
        bus.shutdown_report().is_none(),
        "failed cleanup must not publish its report"
    );

    let retry = block_on(bus.shutdown(ShutdownMode::Immediate));
    assert!(retry.is_ok(), "subsequent provider shutdown can complete");
    assert!(
        bus.shutdown_report().is_some(),
        "report is published after retry succeeds"
    );
}

struct DropPanicCloseSpi {
    inner: FakeAsyncEventBusSpi,
    drops: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for DropPanicCloseSpi {
    fn capabilities(&self) -> qubit_event_bus::spi::EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let future = self.inner.subscribe(request);
        let drops = self.drops.clone();
        let closes = self.closes.clone();
        Box::pin(async move {
            let receiver = future.await?;
            Ok(Box::new(DropPanicCloseReceiver {
                inner: receiver,
                drops,
                closes,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

struct DropPanicCloseReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    drops: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

impl AsyncEventSubscriptionSpi for DropPanicCloseReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.inner.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.inner.settle(token, disposition)
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        if self.closes.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(ReadyCloseDropPanic(self.drops.clone()))
        } else {
            self.inner.close()
        }
    }
}

struct ReadyCloseDropPanic(Arc<AtomicUsize>);
impl Future for ReadyCloseDropPanic {
    type Output = Result<(), SpiError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}
impl Drop for ReadyCloseDropPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("provider receiver close future cleanup panic");
    }
}

#[test]
fn test_finished_receiver_close_drop_panic_is_structured_and_retryable() {
    let _hook = PanicHookGuard::suppress();
    let drops = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(DropPanicCloseSpi {
        inner: FakeAsyncEventBusSpi::new(),
        drops: drops.clone(),
        closes: closes.clone(),
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("close-drop-panic").unwrap(), spi).unwrap();
    let topic = Topic::<u32>::new("close.drop.panic").unwrap();
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("close-drop-panic", topic).unwrap())).expect("subscription");

    let first = block_on(subscription.close());
    assert!(first.is_err(), "close destructor panic must become an error: {first:?}");
    assert!(
        format!("{first:?}").contains("provider_panicked"),
        "structured close error retains panic kind"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "failed close future is dropped exactly once"
    );
    assert_eq!(closes.load(Ordering::SeqCst), 1);

    assert!(
        block_on(subscription.close()).is_ok(),
        "receiver remains available for close retry"
    );
    assert_eq!(closes.load(Ordering::SeqCst), 2);
}

struct HandlerDropPanic {
    drops: Arc<AtomicUsize>,
    clock: Option<Arc<ManualMonotonicClock>>,
}
impl Future for HandlerDropPanic {
    type Output = Result<(), qubit_event_bus::DeliveryError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(clock) = &self.clock {
            clock.advance(Duration::from_nanos(7)).unwrap();
        }
        Poll::Ready(Ok(()))
    }
}
impl Drop for HandlerDropPanic {
    fn drop(&mut self) {
        if let Some(clock) = &self.clock {
            clock.advance(Duration::from_nanos(11)).unwrap();
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("handler future cleanup panic");
    }
}

#[test]
fn test_future_drop_panic_public_async_subscription_handler_cleanup_is_classified() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = Arc::new(ManualMonotonicClock::new());
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("handler-drop-panic").unwrap(),
        spi.clone(),
        EventBusFacadeConfig::new(),
        clock.new_timer(),
    )
    .unwrap();
    let topic = Topic::<u32>::new("test.topic").unwrap();
    let options = qubit_event_bus::model::SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::Discard)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("drop-panic", topic)
                .unwrap()
                .with_options(options),
        ),
    )
    .unwrap();
    let event_id = "handler-future-drop-panic-event";
    spi.enqueue(inbound_message(Some(SettlementToken::new(subscription.id(), event_id))));
    let drops = Arc::new(AtomicUsize::new(0));
    let handler_drops = drops.clone();
    let failure = Arc::new(std::sync::Mutex::new(None::<String>));
    let observed_failure = failure.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { error, .. } = diagnostic {
            *observed_failure.lock().unwrap() = Some(error.to_string());
        }
    });
    let handler_clock = clock.clone();
    let runner = spawn(move || {
        block_on(subscription.run(move |_: Delivery<u32>| HandlerDropPanic {
            drops: handler_drops.clone(),
            clock: Some(handler_clock.clone()),
        }))
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while (spi.settlement_count() == 0 || failure.lock().unwrap().is_none()) && Instant::now() < deadline {
        sleep(Duration::from_millis(2));
    }
    let settled = spi.settlement_count() > 0;
    let dispositions = spi.settlement_dispositions();
    let failure_message = failure.lock().unwrap().clone();
    let _shutdown = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    let runner_result = runner.join();
    assert!(settled, "cleanup failure must reach provider settlement");
    assert_eq!(dispositions, [DeliveryDisposition::Reject]);
    assert!(
        failure_message
            .as_deref()
            .is_some_and(|message| message.contains("future cleanup panicked")),
        "terminal delivery diagnostic must identify future cleanup panic: {failure_message:?}"
    );
    assert!(runner_result.is_ok(), "subscription runner thread must not unwind");
    assert!(runner_result.unwrap().is_ok(), "subscription run must finish cleanly");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "handler future cleanup must be attempted once"
    );
    let metrics = bus.delivery_metrics();
    assert_eq!(metrics.handler_duration_count, 1);
    assert_eq!(metrics.handler_duration_total_nanos, 7);
    assert_eq!(metrics.handler_duration_max_nanos, 7);
    assert_eq!(
        bus.delivery_metrics().running_handlers,
        0,
        "handler capacity must be released"
    );
}

struct HandlerPollPanic {
    clock: Arc<ManualMonotonicClock>,
    drops: Arc<AtomicUsize>,
}

struct HandlerPendingProbe {
    clock: Arc<ManualMonotonicClock>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Future for HandlerPendingProbe {
    type Output = Result<(), qubit_event_bus::DeliveryError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.clock.advance(Duration::from_nanos(7)).unwrap();
        }
        Poll::Pending
    }
}
impl Drop for HandlerPendingProbe {
    fn drop(&mut self) {
        self.clock.advance(Duration::from_nanos(11)).unwrap();
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_async_handler_duration_boundary_pending_actual_disposal_includes_drop() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = Arc::new(ManualMonotonicClock::new());
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("handler-pending-disposal").unwrap(),
        spi.clone(),
        EventBusFacadeConfig::new(),
        clock.new_timer(),
    )
    .unwrap();
    let subscription = block_on(
        bus.subscribe(SubscribeRequest::new("pending-disposal", Topic::<u32>::new("test.topic").unwrap()).unwrap()),
    )
    .unwrap();
    let failure = Arc::new(Mutex::new(None::<String>));
    let observed_failure = failure.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { error, .. } = diagnostic {
            *observed_failure.lock().unwrap() = Some(error.to_string());
        }
    });
    spi.enqueue(inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "pending-disposal",
    ))));
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let handler_polls = polls.clone();
    let handler_drops = drops.clone();
    let handler_clock = clock.clone();
    let mut run = Box::pin(subscription.run(move |_: Delivery<u32>| HandlerPendingProbe {
        clock: handler_clock.clone(),
        polls: handler_polls.clone(),
        drops: handler_drops.clone(),
    }));
    for _ in 0..8 {
        assert!(poll_once(run.as_mut()).is_pending());
        if polls.load(Ordering::SeqCst) > 0 {
            break;
        }
    }
    assert!(polls.load(Ordering::SeqCst) > 0, "pending handler was polled");
    assert_eq!(bus.delivery_metrics().handler_duration_count, 0);
    drop(run);
    drop(subscription);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "dropping paused subscription actually destroys handler future"
    );
    let metrics = bus.delivery_metrics();
    assert_eq!(metrics.handler_duration_count, 1);
    assert_eq!(metrics.handler_duration_total_nanos, 18);
    assert_eq!(metrics.handler_duration_max_nanos, 18);
}
impl Future for HandlerPollPanic {
    type Output = Result<(), qubit_event_bus::DeliveryError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.clock.advance(Duration::from_nanos(7)).unwrap();
        panic!("handler poll panic");
    }
}
impl Drop for HandlerPollPanic {
    fn drop(&mut self) {
        self.clock.advance(Duration::from_nanos(11)).unwrap();
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_async_handler_duration_boundary_poll_panic_includes_future_drop() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = Arc::new(ManualMonotonicClock::new());
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("handler-poll-panic").unwrap(),
        spi.clone(),
        EventBusFacadeConfig::new(),
        clock.new_timer(),
    )
    .unwrap();
    let topic = Topic::<u32>::new("test.topic").unwrap();
    let options = qubit_event_bus::model::SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::Discard)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("poll-panic", topic)
                .unwrap()
                .with_options(options),
        ),
    )
    .unwrap();
    let failure = Arc::new(Mutex::new(None::<String>));
    let observed_failure = failure.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { error, .. } = diagnostic {
            *observed_failure.lock().unwrap() = Some(error.to_string());
        }
    });
    spi.enqueue(inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "poll-panic",
    ))));
    let drops = Arc::new(AtomicUsize::new(0));
    let handler_drops = drops.clone();
    let handler_clock = clock.clone();
    let runner = spawn(move || {
        block_on(subscription.run(move |_: Delivery<u32>| HandlerPollPanic {
            clock: handler_clock.clone(),
            drops: handler_drops.clone(),
        }))
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while (spi.settlement_count() == 0 || drops.load(Ordering::SeqCst) == 0 || failure.lock().unwrap().is_none())
        && Instant::now() < deadline
    {
        sleep(Duration::from_millis(2));
    }
    assert!(
        spi.settlement_count() > 0,
        "poll panic reaches existing failure handling"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let failure_message = failure.lock().unwrap().clone();
    assert!(
        failure_message
            .as_deref()
            .is_some_and(|message| message.contains("subscriber middleware or handler panicked"))
    );
    let _shutdown = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert!(runner.join().unwrap().is_ok());
    let metrics = bus.delivery_metrics();
    assert_eq!(metrics.handler_duration_count, 1);
    assert_eq!(metrics.handler_duration_total_nanos, 18);
    assert_eq!(metrics.handler_duration_max_nanos, 18);
}

#[test]
fn test_async_handler_duration_boundary_factory_panic_records_one_sample() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = Arc::new(ManualMonotonicClock::new());
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("handler-factory-panic").unwrap(),
        spi.clone(),
        EventBusFacadeConfig::new(),
        clock.new_timer(),
    )
    .unwrap();
    let options = qubit_event_bus::model::SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::Discard)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("factory-panic", Topic::<u32>::new("test.topic").unwrap())
                .unwrap()
                .with_options(options),
        ),
    )
    .unwrap();
    spi.enqueue(inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "factory-panic",
    ))));
    let runner = spawn(move || {
        block_on(subscription.run(|_: Delivery<u32>| -> SpiFuture<'static, _> {
            panic!("handler factory panic");
        }))
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while spi.settlement_count() == 0 && Instant::now() < deadline {
        sleep(Duration::from_millis(2));
    }
    assert_eq!(spi.settlement_count(), 1, "factory panic follows existing failure path");
    let _shutdown = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert!(runner.join().unwrap().is_ok());
    let metrics = bus.delivery_metrics();
    assert_eq!(metrics.handler_duration_count, 1);
    assert_eq!(metrics.handler_duration_total_nanos, 0);
    assert_eq!(metrics.handler_duration_max_nanos, 0);
}

#[derive(Clone, Copy)]
enum FutureDropFault {
    PendingPublishReentry,
    ReadyReceive,
    PendingReceiveReentryAfterFirst,
    ReadySettleSuccess,
    ReadySettleError,
    PendingSettleReentryOnce,
}

struct ReentryProbe {
    bus: Mutex<Option<AsyncEventBus>>,
    result: Mutex<Option<Result<(), ShutdownError>>>,
    pending: std::sync::atomic::AtomicBool,
    drops: AtomicUsize,
}

impl ReentryProbe {
    fn trigger(&self) {
        let bus = self.bus.lock().unwrap().as_ref().unwrap().clone();
        let result = match Box::pin(bus.shutdown(ShutdownMode::Immediate))
            .as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
        {
            Poll::Ready(result) => Some(result.map(|_| ())),
            Poll::Pending => {
                self.pending.store(true, Ordering::SeqCst);
                None
            }
        };
        *self.result.lock().unwrap() = result;
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingShutdownReentry<T> {
    probe: Arc<ReentryProbe>,
    drops: Arc<AtomicUsize>,
    _output: PhantomData<fn() -> T>,
}

impl<T> Future for PendingShutdownReentry<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<T> {
        Poll::Pending
    }
}

impl<T> Drop for PendingShutdownReentry<T> {
    fn drop(&mut self) {
        self.probe.trigger();
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("pending provider future cleanup panic");
    }
}

struct FutureDropPanicBus {
    inner: FakeAsyncEventBusSpi,
    target_subscriber: &'static str,
    fault: FutureDropFault,
    drops: Arc<AtomicUsize>,
    settles: Arc<AtomicUsize>,
    pending_receive_created: Arc<std::sync::atomic::AtomicBool>,
    reentry: Arc<ReentryProbe>,
}

impl AsyncEventBusSpi for FutureDropPanicBus {
    fn capabilities(&self) -> qubit_event_bus::spi::EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        if matches!(self.fault, FutureDropFault::PendingPublishReentry) {
            return Box::pin(PendingShutdownReentry {
                probe: self.reentry.clone(),
                drops: self.drops.clone(),
                _output: PhantomData,
            });
        }
        self.inner.publish(message)
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let targeted = request.subscriber_id().as_str() == self.target_subscriber;
        let receiver = self.inner.subscribe(request);
        let fault = self.fault;
        let drops = self.drops.clone();
        let receives = Arc::new(AtomicUsize::new(0));
        let settles = self.settles.clone();
        let pending_receive_created = self.pending_receive_created.clone();
        let reentry = self.reentry.clone();
        Box::pin(async move {
            let inner = receiver.await?;
            Ok(Box::new(FutureDropPanicReceiver {
                inner,
                targeted,
                fault,
                drops,
                receives,
                settles,
                pending_receive_created,
                reentry,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

struct FutureDropPanicReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    targeted: bool,
    fault: FutureDropFault,
    drops: Arc<AtomicUsize>,
    receives: Arc<AtomicUsize>,
    settles: Arc<AtomicUsize>,
    pending_receive_created: Arc<std::sync::atomic::AtomicBool>,
    reentry: Arc<ReentryProbe>,
}

impl AsyncEventSubscriptionSpi for FutureDropPanicReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        let call = self.receives.fetch_add(1, Ordering::SeqCst);
        if self.targeted && matches!(self.fault, FutureDropFault::ReadyReceive) && call == 0 {
            return Box::pin(ReadyReceiveDropPanic {
                inner: self.inner.receive(timeout),
                drops: self.drops.clone(),
            });
        }
        if self.targeted && matches!(self.fault, FutureDropFault::PendingReceiveReentryAfterFirst) && call == 1 {
            self.pending_receive_created.store(true, Ordering::Release);
            return Box::pin(PendingShutdownReentry {
                probe: self.reentry.clone(),
                drops: self.drops.clone(),
                _output: PhantomData,
            });
        }
        self.inner.receive(timeout)
    }

    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        let call = self.settles.fetch_add(1, Ordering::SeqCst);
        if self.targeted && matches!(self.fault, FutureDropFault::ReadySettleSuccess) {
            return Box::pin(ReadySettleDropPanic {
                result: Some(Ok(())),
                drops: self.drops.clone(),
            });
        }
        if self.targeted && matches!(self.fault, FutureDropFault::ReadySettleError) {
            return Box::pin(ReadySettleDropPanic {
                result: Some(Err(SpiError::Operation {
                    provider_id: "drop-panic-test".into(),
                    operation: "settle",
                    resource: Some("settle".into()),
                    kind: "original_settle_failure",
                    retryable: Some(false),
                    source: Box::new(std::io::Error::other("original settle failure")),
                })),
                drops: self.drops.clone(),
            });
        }
        if self.targeted && matches!(self.fault, FutureDropFault::PendingSettleReentryOnce) && call == 0 {
            return Box::pin(PendingShutdownReentry {
                probe: self.reentry.clone(),
                drops: self.drops.clone(),
                _output: PhantomData,
            });
        }
        self.inner.settle(token, disposition)
    }

    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.inner.close()
    }
}

struct ReadyReceiveDropPanic<'a> {
    inner: SpiFuture<'a, Result<ReceiveOutcome, SpiError>>,
    drops: Arc<AtomicUsize>,
}

impl Future for ReadyReceiveDropPanic<'_> {
    type Output = Result<ReceiveOutcome, SpiError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(context)
    }
}

impl Drop for ReadyReceiveDropPanic<'_> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("provider receive future cleanup panic");
    }
}

struct ReadySettleDropPanic {
    result: Option<Result<(), SpiError>>,
    drops: Arc<AtomicUsize>,
}

impl Future for ReadySettleDropPanic {
    type Output = Result<(), SpiError>;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.result.take().expect("settle future polled once"))
    }
}

impl Drop for ReadySettleDropPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("provider settle future cleanup panic");
    }
}

fn future_drop_panic_bus(
    target: &'static str,
    fault: FutureDropFault,
) -> (
    AsyncEventBus,
    FakeAsyncEventBusSpi,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<std::sync::atomic::AtomicBool>,
    Arc<ReentryProbe>,
) {
    let inner = FakeAsyncEventBusSpi::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let settles = Arc::new(AtomicUsize::new(0));
    let pending_receive_created = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reentry = Arc::new(ReentryProbe {
        bus: Mutex::new(None),
        result: Mutex::new(None),
        pending: std::sync::atomic::AtomicBool::new(false),
        drops: AtomicUsize::new(0),
    });
    let spi = Arc::new(FutureDropPanicBus {
        inner: inner.clone(),
        target_subscriber: target,
        fault,
        drops: drops.clone(),
        settles: settles.clone(),
        pending_receive_created: pending_receive_created.clone(),
        reentry: reentry.clone(),
    });
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            NonZeroUsize::new(1).expect("limit"),
            NonZeroUsize::new(8).expect("limit"),
            NonZeroUsize::new(4).expect("limit"),
            NonZeroUsize::new(8).expect("limit"),
        )
        .expect("valid scheduler"),
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("drop-panic-test").unwrap(), spi, config).expect("facade");
    *reentry.bus.lock().unwrap() = Some(bus.clone());
    (bus, inner, drops, settles, pending_receive_created, reentry)
}

fn wait_for(condition: impl Fn() -> bool, description: &str) {
    for _ in 0..500 {
        if condition() {
            return;
        }
        sleep(Duration::from_millis(2));
    }
    assert!(condition(), "timed out waiting for {description}");
}

#[test]
fn test_async_cancelled_spi_drop_rejects_same_bus_shutdown_receive() {
    let _hook = PanicHookGuard::suppress();
    let (bus, _, drops, _, pending_receive_created, reentry) =
        future_drop_panic_bus("second", FutureDropFault::PendingReceiveReentryAfterFirst);
    let first_topic = Topic::<u32>::new("receive-grant-first").unwrap();
    let second_topic = Topic::<u32>::new("receive-grant-second").unwrap();
    let first = block_on(bus.subscribe(SubscribeRequest::new("first", first_topic.clone()).unwrap())).unwrap();
    let second = block_on(bus.subscribe(SubscribeRequest::new("second", second_topic.clone()).unwrap())).unwrap();

    let first_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release_first = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let first_wakers = Arc::new(Mutex::new(Vec::<std::task::Waker>::new()));
    let first_runner = {
        let started = first_started.clone();
        let release = release_first.clone();
        let wakers = first_wakers.clone();
        spawn(move || {
            block_on(first.run(move |_| {
                let started = started.clone();
                let release = release.clone();
                let wakers = wakers.clone();
                async move {
                    started.store(true, Ordering::Release);
                    std::future::poll_fn(move |context| {
                        if release.load(Ordering::Acquire) {
                            return Poll::Ready(Ok(()));
                        }
                        wakers.lock().unwrap().push(context.waker().clone());
                        Poll::Pending
                    })
                    .await
                }
            }))
        })
    };
    let _ = block_on(bus.publish(PublishRequest::new(first_topic, 1).unwrap())).unwrap();
    wait_for(
        || first_started.load(Ordering::Acquire),
        "first handler to hold the slot",
    );

    let second_calls = Arc::new(AtomicUsize::new(0));
    let second_runner = {
        let second_calls = second_calls.clone();
        spawn(move || {
            block_on(second.run(move |_| {
                second_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }))
        })
    };
    let _ = block_on(bus.publish(PublishRequest::new(second_topic, 2).unwrap())).unwrap();
    wait_for(
        || pending_receive_created.load(Ordering::Acquire),
        "second subscription to wait in receive after buffering a message",
    );

    release_first.store(true, Ordering::Release);
    for waker in std::mem::take(&mut *first_wakers.lock().unwrap()) {
        waker.wake();
    }
    wait_for(
        || second_calls.load(Ordering::SeqCst) == 1,
        "scheduler grant to reach second handler",
    );
    wait_for(
        || reentry.drops.load(Ordering::SeqCst) == 1,
        "cancelled receive future cleanup",
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "pending receive future is destroyed once"
    );
    assert!(
        !reentry.pending.load(Ordering::SeqCst),
        "nested shutdown must not remain pending"
    );
    assert!(matches!(
        reentry.result.lock().unwrap().as_ref(),
        Some(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
            operation: "shutdown"
        })))
    ));
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    first_runner.join().unwrap().unwrap();
    second_runner.join().unwrap().unwrap();
    *reentry.bus.lock().unwrap() = None;
}

#[test]
fn test_ready_receive_drop_panic_preserves_message_for_handler() {
    let _hook = PanicHookGuard::suppress();
    let (bus, _, drops, _, _, _) = future_drop_panic_bus("ready", FutureDropFault::ReadyReceive);
    let topic = Topic::<u32>::new("receive-ready-drop-panic").unwrap();
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("ready", topic.clone()).unwrap())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = {
        let calls = calls.clone();
        spawn(move || {
            block_on(subscription.run(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }))
        })
    };
    let _ = block_on(bus.publish(PublishRequest::new(topic, 7).unwrap())).unwrap();
    wait_for(
        || calls.load(Ordering::SeqCst) == 1,
        "message handler after receive destructor panic",
    );
    wait_for(|| drops.load(Ordering::SeqCst) == 1, "completed receive future cleanup");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
}

#[test]
fn test_finished_settle_drop_panic_is_not_recorded_as_cancellation() {
    let _hook = PanicHookGuard::suppress();
    for fault in [FutureDropFault::ReadySettleSuccess, FutureDropFault::ReadySettleError] {
        let (bus, spi, drops, settles, _, _) = future_drop_panic_bus("settle", fault);
        let topic = Topic::<u32>::new("settle-ready-drop-panic").unwrap();
        let subscription = block_on(bus.subscribe(SubscribeRequest::new("settle", topic).unwrap())).unwrap();
        spi.enqueue(inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "settle-token",
        ))));
        let calls = Arc::new(AtomicUsize::new(0));
        let result = block_on(subscription.run({
            let calls = calls.clone();
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }
        }));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "terminal settlement must not rerun handler"
        );
        assert_eq!(settles.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        match fault {
            FutureDropFault::ReadySettleSuccess => assert!(
                matches!(
                    &result,
                    Err(ReceiveError::Stopped(reason))
                        if matches!(reason.as_ref(), SubscriptionStopReason::Settlement {
                            error, termination: qubit_event_bus::model::SettlementTermination::ProviderPanicked, ..
                        } if matches!(error.as_ref(), SpiError::Operation { operation: "settle", kind: "provider_panicked", .. }))
                ),
                "unexpected settle result: {result:?}"
            ),
            FutureDropFault::ReadySettleError => assert!(
                matches!(
                    &result,
                    Err(ReceiveError::Stopped(reason))
                        if matches!(reason.as_ref(), SubscriptionStopReason::Settlement {
                            error, ..
                        } if matches!(error.as_ref(), SpiError::Operation { operation: "settle", kind: "original_settle_failure", .. }))
                ),
                "unexpected settle result: {result:?}"
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_async_cancelled_spi_drop_rejects_same_bus_shutdown_settle() {
    let _hook = PanicHookGuard::suppress();
    let (bus, spi, drops, settles, _, reentry) =
        future_drop_panic_bus("settle", FutureDropFault::PendingSettleReentryOnce);
    let topic = Topic::<u32>::new("settle-pending-drop-panic").unwrap();
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("settle", topic).unwrap())).unwrap();
    spi.enqueue(inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "pending-settle-token",
    ))));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let first_calls = handler_calls.clone();
    let mut first_run = Box::pin(subscription.run(move |_| {
        first_calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    }));
    for _ in 0..20 {
        let _ = poll_once(first_run.as_mut());
        if settles.load(Ordering::SeqCst) == 1 {
            break;
        }
    }
    assert_eq!(
        settles.load(Ordering::SeqCst),
        1,
        "first settle attempt reached pending state"
    );
    let cancellation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(first_run)));
    assert!(
        cancellation.is_ok(),
        "pending settle cleanup panic must not escape cancellation"
    );
    assert_eq!(reentry.drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "pending settlement future is destroyed once"
    );
    assert!(
        !reentry.pending.load(Ordering::SeqCst),
        "nested shutdown must not remain pending"
    );
    assert!(matches!(
        reentry.result.lock().unwrap().as_ref(),
        Some(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
            operation: "shutdown"
        })))
    ));

    let next_calls = handler_calls.clone();
    let runner = spawn(move || {
        block_on(subscription.run(move |_| {
            next_calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }))
    });
    wait_for(|| settles.load(Ordering::SeqCst) >= 2, "idempotent settlement retry");
    assert_eq!(
        handler_calls.load(Ordering::SeqCst),
        1,
        "retry must not rerun the completed handler"
    );
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
    assert_eq!(spi.settlement_count(), 1, "original token is settled once");
    assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Accept]);
    *reentry.bus.lock().unwrap() = None;
}
