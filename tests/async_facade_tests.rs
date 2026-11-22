// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous facade contract tests.

mod support;

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::ManualTimer;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_clock::TimerUnavailableError;
use qubit_event_bus::AsyncSubscription;
use qubit_event_bus::CheckedPublishError;
use qubit_event_bus::CodecError;
use qubit_event_bus::ConfigurationError;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::FacadeBuildError;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::SubscribeError;
use qubit_event_bus::WaitOutcome;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CapabilityError;
use qubit_event_bus::error::DeliveryAttemptError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::facade::AsyncSubscriptionRunState;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SettlementRetryConfig;
use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::ConsumerGroup;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishMetadata;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SettlementTermination;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberNext;
use qubit_event_bus::model::SubscriptionDurability;
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
use qubit_retry::AttemptFailure;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryPolicy;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::manual_async::block_on;

struct FailingRegistrationTimer {
    clock: ManualMonotonicClock,
    registrations: AtomicUsize,
}

impl FailingRegistrationTimer {
    fn new() -> Self {
        Self {
            clock: ManualMonotonicClock::new(),
            registrations: AtomicUsize::new(0),
        }
    }
}

impl MonotonicClock for FailingRegistrationTimer {
    fn domain(&self) -> qubit_clock::ClockDomain {
        self.clock.domain()
    }

    fn now(&self) -> MonotonicInstant {
        self.clock.now()
    }

    fn new_timer(&self) -> Arc<dyn Timer> {
        self.clock.new_timer()
    }
}

impl Timer for FailingRegistrationTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        self
    }

    fn at(&self, _deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        self.registrations.fetch_add(1, Ordering::SeqCst);
        Err(TimeError::TimerUnavailable {
            source: TimerUnavailableError::BackendUnavailable {
                backend: "test",
                source: Box::new(std::io::Error::other("registration failed")),
            },
        })
    }
}

/// Creates the common typed topic used by asynchronous facade contracts.
fn topic() -> Topic<u32> {
    Topic::new("test.topic").unwrap()
}

/// Records provider acceptance when a publish future is first polled, then
/// remains pending so the facade call can be cancelled deterministically.
struct PendingPublishSpi {
    inner: FakeAsyncEventBusSpi,
    accepted: Arc<AtomicUsize>,
    observed_event_id: Arc<std::sync::Mutex<Option<EventId>>>,
}

/// Holds a provider publish until the test explicitly releases it.
struct GatedPublishSpi {
    inner: FakeAsyncEventBusSpi,
    released: Arc<AtomicBool>,
    accepted: Arc<AtomicUsize>,
    fail: bool,
}

impl AsyncEventBusSpi for GatedPublishSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            self.accepted.fetch_add(1, Ordering::AcqRel);
            std::future::poll_fn(|_| {
                if self.released.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            if self.fail {
                Err(SpiError::Operation {
                    provider_id: "gated".into(),
                    operation: "publish",
                    resource: None,
                    kind: "test_failure",
                    retryable: None,
                    source: Box::new(std::io::Error::other("gated publish failure")),
                })
            } else {
                self.inner.publish(message).await
            }
        })
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.inner.subscribe(request)
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

struct ShutdownOnDropProbe {
    bus: AsyncEventBus,
    nested_shutdown_was_rejected: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
    metrics_on_drop: Arc<Mutex<Option<qubit_event_bus::facade::PublishMetricsSnapshot>>>,
    panic_on_drop: bool,
}

impl Drop for ShutdownOnDropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
        *self.metrics_on_drop.lock().unwrap() = Some(self.bus.publish_metrics());
        let mut shutdown = Box::pin(self.bus.shutdown(ShutdownMode::Immediate));
        let mut context = Context::from_waker(Waker::noop());
        self.nested_shutdown_was_rejected.store(
            matches!(
                shutdown.as_mut().poll(&mut context),
                Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                    operation: "shutdown"
                })))
            ),
            Ordering::Release,
        );
        assert!(!self.panic_on_drop, "observer drop panic");
    }
}

fn assert_observer_snapshot_drops_in_publish_context(cancel: bool) {
    let released = Arc::new(AtomicBool::new(false));
    let accepted = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(GatedPublishSpi {
        inner: FakeAsyncEventBusSpi::new(),
        released: released.clone(),
        accepted: accepted.clone(),
        fail: false,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("observer-drop-context").unwrap(), spi)
        .expect("valid provider capabilities");
    let nested_shutdown_was_rejected = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let metrics_on_drop = Arc::new(Mutex::new(None));
    let probe = ShutdownOnDropProbe {
        bus: bus.clone(),
        nested_shutdown_was_rejected: nested_shutdown_was_rejected.clone(),
        drops: drops.clone(),
        metrics_on_drop: metrics_on_drop.clone(),
        panic_on_drop: false,
    };
    let observer = bus.observe_diagnostics(move |_| {
        let _probe = &probe;
    });
    let mut publishing = Box::pin(bus.publish(PublishRequest::new(topic(), 9).unwrap()));
    let mut context = Context::from_waker(Waker::noop());
    assert!(publishing.as_mut().poll(&mut context).is_pending());
    assert_eq!(1, accepted.load(Ordering::Acquire));
    drop(observer);
    assert_eq!(0, drops.load(Ordering::Acquire));

    if cancel {
        drop(publishing);
        assert_eq!(1, bus.publish_metrics().cancelled);
    } else {
        released.store(true, Ordering::Release);
        let receipt = match publishing.as_mut().poll(&mut context) {
            Poll::Ready(Ok(receipt)) => receipt,
            other => panic!("released publish should complete, got {other:?}"),
        };
        assert_eq!("observer-drop-context", receipt.provider_id().as_str());
        assert_eq!(1, bus.publish_metrics().opaque_accepted);
        assert_eq!(1, metrics_on_drop.lock().unwrap().unwrap().opaque_accepted);
        assert_eq!(0, metrics_on_drop.lock().unwrap().unwrap().cancelled);
    }

    assert_eq!(1, drops.load(Ordering::Acquire));
    let metrics_at_drop = metrics_on_drop.lock().unwrap().unwrap();
    if cancel {
        assert_eq!(0, metrics_at_drop.cancelled);
        assert_eq!(0, metrics_at_drop.opaque_accepted);
        assert_eq!(0, metrics_at_drop.errors);
    }
    assert!(nested_shutdown_was_rejected.load(Ordering::Acquire));
    let _report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("external shutdown succeeds");
}

#[test]
fn test_async_publish_observer_drop_observes_provider_error_before_release() {
    let released = Arc::new(AtomicBool::new(false));
    let accepted = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(GatedPublishSpi {
        inner: FakeAsyncEventBusSpi::new(),
        released: released.clone(),
        accepted: accepted.clone(),
        fail: true,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("observer-drop-error").unwrap(), spi)
        .expect("valid provider capabilities");
    let metrics_on_drop = Arc::new(Mutex::new(None));
    let probe = ShutdownOnDropProbe {
        bus: bus.clone(),
        nested_shutdown_was_rejected: Arc::new(AtomicBool::new(false)),
        drops: Arc::new(AtomicUsize::new(0)),
        metrics_on_drop: metrics_on_drop.clone(),
        panic_on_drop: false,
    };
    let observer = bus.observe_diagnostics(move |_| {
        let _probe = &probe;
    });
    let mut publishing = Box::pin(bus.publish(PublishRequest::new(topic(), 11).unwrap()));
    let mut context = Context::from_waker(Waker::noop());
    assert!(publishing.as_mut().poll(&mut context).is_pending());
    drop(observer);
    released.store(true, Ordering::Release);
    assert!(matches!(publishing.as_mut().poll(&mut context), Poll::Ready(Err(_))));
    let metrics = metrics_on_drop.lock().unwrap().unwrap();
    assert_eq!(1, metrics.errors);
    assert_eq!(0, metrics.cancelled);
    let _report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("external shutdown succeeds");
}

#[test]
fn test_async_publish_observer_drop_panics_after_receipt_is_recorded() {
    let released = Arc::new(AtomicBool::new(false));
    let accepted = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(GatedPublishSpi {
        inner: FakeAsyncEventBusSpi::new(),
        released: released.clone(),
        accepted: accepted.clone(),
        fail: false,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("observer-drop-panic").unwrap(), spi)
        .expect("valid provider capabilities");
    let metrics_on_drop = Arc::new(Mutex::new(None));
    let probe = ShutdownOnDropProbe {
        bus: bus.clone(),
        nested_shutdown_was_rejected: Arc::new(AtomicBool::new(false)),
        drops: Arc::new(AtomicUsize::new(0)),
        metrics_on_drop: metrics_on_drop.clone(),
        panic_on_drop: true,
    };
    let observer = bus.observe_diagnostics(move |_| {
        let _probe = &probe;
    });
    let mut publishing = Box::pin(bus.publish(PublishRequest::new(topic(), 10).unwrap()));
    let mut context = Context::from_waker(Waker::noop());
    assert!(publishing.as_mut().poll(&mut context).is_pending());
    drop(observer);
    released.store(true, Ordering::Release);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = publishing.as_mut().poll(&mut context);
    }));
    assert!(panic.is_err());
    drop(publishing);
    let metrics = metrics_on_drop.lock().unwrap().unwrap();
    assert_eq!(1, metrics.opaque_accepted);
    assert_eq!(0, metrics.cancelled);
    let _report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("external shutdown succeeds");
}

#[test]
fn test_async_publish_completion_drops_observer_snapshot_in_bus_context() {
    assert_observer_snapshot_drops_in_publish_context(false);
}

#[test]
fn test_async_publish_cancellation_drops_observer_snapshot_in_bus_context() {
    assert_observer_snapshot_drops_in_publish_context(true);
}

impl AsyncEventBusSpi for PendingPublishSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        let observed_event_id = self.observed_event_id.clone();
        Box::pin(async move {
            *observed_event_id.lock().unwrap() = Some(message.id().clone());
            self.accepted.fetch_add(1, Ordering::AcqRel);
            std::future::pending().await
        })
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.inner.subscribe(request)
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

#[test]
fn test_cancelling_polled_publish_future_does_not_retract_provider_acceptance() {
    let accepted = Arc::new(AtomicUsize::new(0));
    let observed_event_id = Arc::new(std::sync::Mutex::new(None));
    let spi = Arc::new(PendingPublishSpi {
        inner: FakeAsyncEventBusSpi::new(),
        accepted: accepted.clone(),
        observed_event_id: observed_event_id.clone(),
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("pending-publish").unwrap(), spi).unwrap();
    let unpolled = bus.publish(PublishRequest::new(topic(), 7).unwrap());
    assert_eq!(0, accepted.load(Ordering::Acquire));
    assert_eq!(None, *observed_event_id.lock().unwrap());
    drop(unpolled);
    assert_eq!(0, bus.publish_metrics().attempts);

    let request = PublishRequest::new(topic(), 7).unwrap();
    let event_id = request.envelope().id().clone();

    let mut publishing = Box::pin(bus.publish(request));
    let mut context = Context::from_waker(Waker::noop());
    assert!(publishing.as_mut().poll(&mut context).is_pending());
    assert_eq!(1, accepted.load(Ordering::Acquire));
    assert_eq!(Some(event_id.clone()), *observed_event_id.lock().unwrap());
    drop(publishing);

    let metrics = bus.publish_metrics();
    assert_eq!(1, metrics.cancelled);
    assert_eq!(0, metrics.errors);
    assert_eq!(1, accepted.load(Ordering::Acquire));
    assert_eq!(Some(event_id), *observed_event_id.lock().unwrap());
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

struct ReentrantSubscribeSpi {
    inner: FakeAsyncEventBusSpi,
    bus: Mutex<Option<AsyncEventBus>>,
    check_during_poll: bool,
    rejected_shutdowns: AtomicUsize,
}

impl ReentrantSubscribeSpi {
    fn check_shutdown(&self, bus: &AsyncEventBus) {
        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        assert!(matches!(
            shutdown.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                operation: "shutdown"
            })))
        ));
        self.rejected_shutdowns.fetch_add(1, Ordering::AcqRel);
    }
}

impl AsyncEventBusSpi for ReentrantSubscribeSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        let bus = self.bus.lock().unwrap().as_ref().unwrap().clone();
        if !self.check_during_poll {
            self.check_shutdown(&bus);
        }
        let inner = self.inner.subscribe(request);
        Box::pin(async move {
            if self.check_during_poll {
                self.check_shutdown(&bus);
            }
            inner.await
        })
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

#[test]
fn test_async_subscribe_reentrant_shutdown_returns_would_deadlock() {
    for check_during_poll in [false, true] {
        let spi = Arc::new(ReentrantSubscribeSpi {
            inner: FakeAsyncEventBusSpi::new(),
            bus: Mutex::new(None),
            check_during_poll,
            rejected_shutdowns: AtomicUsize::new(0),
        });
        let bus =
            AsyncEventBus::from_spi(ProviderId::new("subscribe-reentrant-shutdown").unwrap(), spi.clone()).unwrap();
        *spi.bus.lock().unwrap() = Some(bus.clone());
        let subscription = block_on(bus.subscribe(SubscribeRequest::new("reentrant", topic()).unwrap()))
            .expect("outer subscription should succeed");
        assert_eq!(1, spi.rejected_shutdowns.load(Ordering::Acquire));
        drop(subscription);
        let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown from normal call site succeeds");
    }
}

struct CleanupReentryProbe {
    bus: Mutex<Option<AsyncEventBus>>,
    subscription: Mutex<Option<Arc<AsyncSubscription<u32>>>>,
    different_bus: Mutex<Option<AsyncEventBus>>,
    check_during_poll: bool,
    calls: AtomicUsize,
}

impl CleanupReentryProbe {
    fn check(&self) {
        let bus = self.bus.lock().unwrap().as_ref().unwrap().clone();
        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        assert!(matches!(
            shutdown.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                operation: "shutdown"
            })))
        ));
        if let Some(other_bus) = self.different_bus.lock().unwrap().as_ref() {
            let _ =
                block_on(other_bus.shutdown(ShutdownMode::Immediate)).expect("shutdown of a different bus is allowed");
        }
        if let Some(subscription) = self.subscription.lock().unwrap().clone() {
            let mut run = Box::pin(subscription.run(|_| async { Ok(()) }));
            assert!(matches!(
                run.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(ReceiveError::WouldDeadlock))
            ));
        }
        self.calls.fetch_add(1, Ordering::AcqRel);
    }
}

struct ReentrantCleanupSpi {
    inner: FakeAsyncEventBusSpi,
    probe: Arc<CleanupReentryProbe>,
    check_close: bool,
    check_shutdown: bool,
    close_calls: Arc<AtomicUsize>,
    shutdown_calls: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for ReentrantCleanupSpi {
    fn capabilities(&self) -> EventBusCapabilities {
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
        let probe = self.probe.clone();
        let check_close = self.check_close;
        let check_during_poll = probe.check_during_poll;
        let close_calls = self.close_calls.clone();
        Box::pin(async move {
            let receiver = inner.subscribe(request).await?;
            Ok(Box::new(ReentrantCleanupReceiver {
                inner: receiver,
                probe,
                check_close,
                check_during_poll,
                close_calls,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.shutdown_calls.fetch_add(1, Ordering::AcqRel);
        if self.check_shutdown && !self.probe.check_during_poll {
            self.probe.check();
        }
        let future = self.inner.shutdown(mode);
        if self.check_shutdown && self.probe.check_during_poll {
            let probe = self.probe.clone();
            Box::pin(async move {
                probe.check();
                future.await
            })
        } else {
            future
        }
    }
}

struct ReentrantCleanupReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    probe: Arc<CleanupReentryProbe>,
    check_close: bool,
    check_during_poll: bool,
    close_calls: Arc<AtomicUsize>,
}

impl Drop for ReentrantCleanupReceiver {
    fn drop(&mut self) {
        if self.check_close {
            self.probe.check();
        }
    }
}

impl AsyncEventSubscriptionSpi for ReentrantCleanupReceiver {
    fn receive<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> SpiFuture<'a, Result<qubit_event_bus::spi::ReceiveOutcome, SpiError>> {
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
        self.close_calls.fetch_add(1, Ordering::AcqRel);
        if self.check_close && !self.check_during_poll {
            self.probe.check();
        }
        let future = self.inner.close();
        if self.check_close && self.check_during_poll {
            let probe = self.probe.clone();
            Box::pin(async move {
                probe.check();
                future.await
            })
        } else {
            future
        }
    }
}

#[test]
fn test_async_spi_cleanup_reentrant_shutdown_returns_would_deadlock() {
    for check_during_poll in [false, true] {
        for close_during_shutdown in [false, true] {
            let probe = Arc::new(CleanupReentryProbe {
                bus: Mutex::new(None),
                subscription: Mutex::new(None),
                different_bus: Mutex::new(None),
                check_during_poll,
                calls: AtomicUsize::new(0),
            });
            let close_calls = Arc::new(AtomicUsize::new(0));
            let shutdown_calls = Arc::new(AtomicUsize::new(0));
            let spi = Arc::new(ReentrantCleanupSpi {
                inner: FakeAsyncEventBusSpi::new(),
                probe: probe.clone(),
                check_close: true,
                check_shutdown: false,
                close_calls: close_calls.clone(),
                shutdown_calls: shutdown_calls.clone(),
            });
            let bus = AsyncEventBus::from_spi(ProviderId::new("cleanup-close-reentrant").unwrap(), spi).unwrap();
            *probe.bus.lock().unwrap() = Some(bus.clone());
            let mut subscription =
                Arc::new(block_on(bus.subscribe(SubscribeRequest::new("cleanup-close", topic()).unwrap())).unwrap());
            if close_during_shutdown {
                *probe.subscription.lock().unwrap() = Some(subscription.clone());
                let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
            } else {
                block_on(Arc::get_mut(&mut subscription).unwrap().close()).unwrap();
                let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
            }
            assert_eq!(2, probe.calls.load(Ordering::Acquire));
            assert_eq!(1, close_calls.load(Ordering::Acquire));
            assert_eq!(1, shutdown_calls.load(Ordering::Acquire));
        }

        let probe = Arc::new(CleanupReentryProbe {
            bus: Mutex::new(None),
            subscription: Mutex::new(None),
            different_bus: Mutex::new(None),
            check_during_poll,
            calls: AtomicUsize::new(0),
        });
        let close_calls = Arc::new(AtomicUsize::new(0));
        let shutdown_calls = Arc::new(AtomicUsize::new(0));
        let spi = Arc::new(ReentrantCleanupSpi {
            inner: FakeAsyncEventBusSpi::new(),
            probe: probe.clone(),
            check_close: false,
            check_shutdown: true,
            close_calls: close_calls.clone(),
            shutdown_calls: shutdown_calls.clone(),
        });
        let bus = AsyncEventBus::from_spi(ProviderId::new("cleanup-provider-reentrant").unwrap(), spi).unwrap();
        *probe.bus.lock().unwrap() = Some(bus.clone());
        let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
        assert_eq!(1, probe.calls.load(Ordering::Acquire));
        assert_eq!(1, shutdown_calls.load(Ordering::Acquire));
    }

    let other_bus = AsyncEventBus::from_spi(
        ProviderId::new("cleanup-other-bus").unwrap(),
        Arc::new(FakeAsyncEventBusSpi::new()),
    )
    .unwrap();
    let probe = Arc::new(CleanupReentryProbe {
        bus: Mutex::new(None),
        subscription: Mutex::new(None),
        different_bus: Mutex::new(Some(other_bus)),
        check_during_poll: false,
        calls: AtomicUsize::new(0),
    });
    let spi = Arc::new(ReentrantCleanupSpi {
        inner: FakeAsyncEventBusSpi::new(),
        probe: probe.clone(),
        check_close: false,
        check_shutdown: true,
        close_calls: Arc::new(AtomicUsize::new(0)),
        shutdown_calls: Arc::new(AtomicUsize::new(0)),
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("cleanup-reentrant-other").unwrap(), spi).unwrap();
    *probe.bus.lock().unwrap() = Some(bus.clone());
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(1, probe.calls.load(Ordering::Acquire));
}

struct RecordingShutdownSpi {
    inner: FakeAsyncEventBusSpi,
    calls: Arc<Mutex<Vec<ShutdownMode>>>,
}

impl AsyncEventBusSpi for RecordingShutdownSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.inner.subscribe(request)
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.calls.lock().unwrap().push(mode);
        self.inner.shutdown(mode)
    }
}

#[test]
fn test_async_graceful_shutdown_passes_remaining_budget_to_spi() {
    let inner = FakeAsyncEventBusSpi::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let spi = Arc::new(RecordingShutdownSpi {
        inner: inner.clone(),
        calls: calls.clone(),
    });
    let clock = ManualMonotonicClock::new();
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("remaining-shutdown-budget").unwrap(),
        spi,
        clock.new_timer(),
    )
    .unwrap();

    block_on(async {
        let _subscription = bus
            .subscribe(SubscribeRequest::new("budget", topic()).unwrap())
            .await
            .unwrap();
        inner.pause_close();
        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(10),
        }));
        let mut context = Context::from_waker(Waker::noop());
        assert!(shutdown.as_mut().poll(&mut context).is_pending());
        clock.advance(Duration::from_secs(3)).unwrap();
        inner.release_close();
        assert!(matches!(shutdown.as_mut().poll(&mut context), Poll::Ready(Ok(_))));
    });
    assert_eq!(
        vec![ShutdownMode::Graceful {
            timeout: Duration::from_secs(7),
        }],
        *calls.lock().unwrap(),
    );

    let zero_calls = Arc::new(Mutex::new(Vec::new()));
    let zero_spi = Arc::new(RecordingShutdownSpi {
        inner: FakeAsyncEventBusSpi::new(),
        calls: zero_calls.clone(),
    });
    let zero_clock = ManualMonotonicClock::new();
    let zero_bus = AsyncEventBus::with_timer(
        ProviderId::new("zero-shutdown-budget").unwrap(),
        zero_spi,
        zero_clock.new_timer(),
    )
    .unwrap();
    let _ = block_on(zero_bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::ZERO,
    }))
    .expect("empty close batch permits immediate provider completion");
    assert_eq!(
        vec![ShutdownMode::Graceful {
            timeout: Duration::ZERO
        }],
        *zero_calls.lock().unwrap(),
    );
}

#[test]
fn test_async_publish_reentrant_shutdown() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("publish-reentrant-shutdown").unwrap(), spi)
        .expect("valid provider capabilities");
    let callback_bus = bus.clone();
    let callback_ran = Arc::new(AtomicBool::new(false));
    let callback_ran_in_interceptor = callback_ran.clone();
    let options = PublishOptions::<String>::builder()
        .interceptor(move |envelope| {
            for mode in [
                ShutdownMode::Immediate,
                ShutdownMode::Graceful {
                    timeout: Duration::from_secs(60),
                },
            ] {
                let mut future = Box::pin(callback_bus.shutdown(mode));
                let mut context = Context::from_waker(Waker::noop());
                assert!(matches!(
                    future.as_mut().poll(&mut context),
                    Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                        operation: "shutdown"
                    })))
                ));
            }
            callback_ran_in_interceptor.store(true, Ordering::Release);
            Ok(Some(envelope))
        })
        .build();
    let request = PublishRequest::new(Topic::<String>::new("publish.reentrant").unwrap(), "event".to_owned())
        .unwrap()
        .with_options(options);

    let _receipt = block_on(bus.publish(request)).expect("publish should continue after rejected reentrant shutdown");
    assert!(callback_ran.load(Ordering::Acquire));
    let _report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("external shutdown should succeed");
}

#[test]
fn test_async_publish_context_failure_callback_and_wait() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("publish-context-failure").unwrap(), spi.clone())
        .expect("valid provider capabilities");
    spi.fail_next_publish();
    let callback_bus = bus.clone();
    let callback_topic = Topic::<String>::new("publish.context.failure").unwrap();
    let callback_ran = Arc::new(AtomicBool::new(false));
    let callback_ran_in_handler = callback_ran.clone();
    let request = PublishRequest::new(
        Topic::<String>::new("publish.context.failure").unwrap(),
        "event".to_owned(),
    )
    .unwrap()
    .with_options(
        PublishOptions::<String>::builder()
            .error_handler(move |_, _| {
                let mut shutdown = Box::pin(callback_bus.shutdown(ShutdownMode::Immediate));
                let mut wait = Box::pin(callback_bus.wait_for_received_deliveries(&callback_topic, None));
                let mut context = Context::from_waker(Waker::noop());
                assert!(matches!(
                    shutdown.as_mut().poll(&mut context),
                    Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                        operation: "shutdown"
                    })))
                ));
                assert!(matches!(
                    wait.as_mut().poll(&mut context),
                    Poll::Ready(Err(LifecycleError::WouldDeadlock {
                        operation: "wait_for_received_deliveries"
                    }))
                ));
                callback_ran_in_handler.store(true, Ordering::Release);
            })
            .build(),
    );
    let error = block_on(bus.publish(request)).expect_err("fake provider failure should be retained");
    assert!(callback_ran.load(Ordering::Acquire));
    assert!(matches!(error.cause(), qubit_event_bus::PublishError::Spi(_)));
    let _report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("external shutdown succeeds");
}

#[test]
fn test_async_publish_context_does_not_reject_different_bus() {
    let first = AsyncEventBus::from_spi(
        ProviderId::new("publish-context-first").unwrap(),
        Arc::new(FakeAsyncEventBusSpi::new()),
    )
    .expect("valid first provider");
    let second = AsyncEventBus::from_spi(
        ProviderId::new("publish-context-second").unwrap(),
        Arc::new(FakeAsyncEventBusSpi::new()),
    )
    .expect("valid second provider");
    let second_for_callback = second.clone();
    let request = PublishRequest::new(
        Topic::<String>::new("publish.context.other").unwrap(),
        "event".to_owned(),
    )
    .unwrap()
    .with_options(
        PublishOptions::<String>::builder()
            .interceptor(move |envelope| {
                let _report = block_on(second_for_callback.shutdown(ShutdownMode::Immediate))
                    .expect("different bus shutdown should be allowed");
                Ok(Some(envelope))
            })
            .build(),
    );
    let _receipt = block_on(first.publish(request)).expect("first bus publish succeeds");
    let _report = block_on(first.shutdown(ShutdownMode::Immediate)).expect("first bus shutdown succeeds");
}

#[test]
fn test_async_publish_context_admission_observer_rejects_same_bus_shutdown() {
    use std::any::TypeId;

    use qubit_event_bus::local::AsyncLocalEventBusProvider;
    use qubit_event_bus::local::LocalEventBusConfig;
    use qubit_event_bus::registry::EventBusConfig;
    use qubit_event_bus::spi::OutboundMessage;
    use qubit_event_bus::spi::TransportPayload;
    use qubit_spi::AsyncServiceProvider;

    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().queue_capacity(1).provider_options());
    let spi = block_on(AsyncLocalEventBusProvider.create_configured(&config)).expect("local SPI");
    let topic_address = TopicAddress::new("publish.context.observer").unwrap();
    let receiver = block_on(spi.subscribe(SpiSubscriptionRequest::new(
        Id::new(903),
        topic_address.clone(),
        qubit_event_bus::model::SubscriberId::new("publish-context-observer").unwrap(),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        Default::default(),
        TypeId::of::<String>(),
    )))
    .expect("raw receiver");
    let raw_publish = |event_id: &'static str| {
        block_on(spi.publish(OutboundMessage::new(
            topic_address.clone(),
            EventId::new(event_id).unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(String::from("fill"))),
        )))
        .expect("raw publish")
    };
    let _filled = raw_publish("observer-fill");
    let _rejected = raw_publish("observer-reject");

    let bus = AsyncEventBus::from_spi(ProviderId::new("publish-context-observer").unwrap(), spi)
        .expect("valid provider capabilities");
    let callback_bus = bus.clone();
    let observed = Arc::new(AtomicBool::new(false));
    let observed_in_callback = observed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::AdmissionRejected { .. }) {
            let mut shutdown = Box::pin(callback_bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(60),
            }));
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                shutdown.as_mut().poll(&mut context),
                Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                    operation: "shutdown"
                })))
            ));
            observed_in_callback.store(true, Ordering::Release);
        }
    });
    let _receipt = block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("publish.context.observer").unwrap(),
                "payload".to_owned(),
            )
            .unwrap(),
        ),
    )
    .expect("observer does not change publication result");
    assert!(observed.load(Ordering::Acquire));
    let _report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("external shutdown succeeds");
    drop(receiver);
}

#[test]
fn test_async_publish_context_pending_poll_restores_scope_and_releases_publish() {
    let accepted = Arc::new(AtomicUsize::new(0));
    let observed_event_id = Arc::new(std::sync::Mutex::new(None));
    let spi = Arc::new(PendingPublishSpi {
        inner: FakeAsyncEventBusSpi::new(),
        accepted: accepted.clone(),
        observed_event_id,
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("publish-context-pending").unwrap(), spi)
        .expect("valid provider capabilities");
    let mut publishing = Box::pin(bus.publish(PublishRequest::new(topic(), 9).unwrap()));
    let mut context = Context::from_waker(Waker::noop());
    assert!(publishing.as_mut().poll(&mut context).is_pending());

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(shutdown.as_mut().poll(&mut context), Poll::Pending));
    drop(shutdown);
    drop(publishing);
    assert_eq!(1, accepted.load(Ordering::Acquire));
    let _report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("publish cancellation releases count");
}

/// Signals a channel whenever an idle-wait future is woken.
struct WakeOnSignal(std::sync::mpsc::Sender<()>);

impl std::task::Wake for WakeOnSignal {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

/// Selects provider capabilities and counts subscriptions that reach the SPI.
struct OrderingTestSpi {
    inner: FakeAsyncEventBusSpi,
    capabilities: EventBusCapabilities,
    subscribe_calls: AtomicUsize,
}

impl OrderingTestSpi {
    fn new(ordering: OrderingCapability) -> Self {
        let base = EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::AcceptRetryReject,
            ordering,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        );
        Self::with_capabilities(base)
    }

    fn with_capabilities(capabilities: EventBusCapabilities) -> Self {
        Self {
            inner: FakeAsyncEventBusSpi::with_capabilities(capabilities),
            capabilities,
            subscribe_calls: AtomicUsize::new(0),
        }
    }

    fn enqueue(&self, message: InboundMessage) {
        self.inner.enqueue(message);
    }
}

impl AsyncEventBusSpi for OrderingTestSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.subscribe_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.subscribe(request)
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

/// Reports an empty visible admission snapshot for checked publication tests.
struct EmptyAdmissionSpi(FakeAsyncEventBusSpi);

impl AsyncEventBusSpi for EmptyAdmissionSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::DestinationAdmissions,
        )
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let _ = self.0.publish(message).await?;
            Ok(PublishAcknowledgement::DestinationAdmissions(vec![]))
        })
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.0.subscribe(request)
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.0.shutdown(mode)
    }
}

/// Counts encoding attempts made after checked publish admission preflight.
struct CheckedPublishCodec {
    content_type: ContentType,
    calls: Arc<AtomicUsize>,
}

impl EventCodec<String> for CheckedPublishCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::from(value.as_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

#[test]
fn test_async_checked_publish_opaque_preflight_has_no_side_effects() {
    let capabilities = EventBusCapabilities::new(
        PayloadModes::Encoded,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(capabilities));
    let provider_id = ProviderId::new("opaque-async-test").expect("valid provider");
    let interceptor_calls = Arc::new(AtomicUsize::new(0));
    let global_calls = interceptor_calls.clone();
    let config = EventBusFacadeConfig::new().publisher_interceptor(move |_metadata: &mut PublishMetadata| {
        global_calls.fetch_add(1, Ordering::AcqRel);
        Ok(true)
    });
    let bus = AsyncEventBus::with_config(provider_id.clone(), spi.clone(), config).expect("valid opaque provider");
    let codec_calls = Arc::new(AtomicUsize::new(0));
    let topic = Topic::new("async.checked.opaque")
        .expect("valid topic")
        .with_codec(CheckedPublishCodec {
            content_type: ContentType::TEXT_PLAIN,
            calls: codec_calls.clone(),
        });

    for requirement in [
        AdmissionRequirement::AtLeastOneAccepted,
        AdmissionRequirement::AtLeastOneAcceptedAndNoRejected,
    ] {
        let typed_calls = interceptor_calls.clone();
        let options = PublishOptions::<String>::builder()
            .interceptor(move |envelope| {
                typed_calls.fetch_add(1, Ordering::AcqRel);
                Ok(Some(envelope))
            })
            .build();
        let request = PublishRequest::new(topic.clone(), "order".to_owned())
            .expect("valid request")
            .with_options(options);
        let event_id = request.envelope().id().clone();
        let error = block_on(bus.publish_checked(request, requirement))
            .expect_err("opaque provider cannot report destinations");
        assert!(matches!(
            error,
            CheckedPublishError::UnsupportedVisibility {
                event_id: actual_event_id,
                provider_id: actual_provider_id
            } if actual_event_id == event_id && actual_provider_id == provider_id
        ));
        assert_eq!(interceptor_calls.load(Ordering::Acquire), 0);
        assert_eq!(codec_calls.load(Ordering::Acquire), 0);
        assert!(!spi.operation_log().contains(&"publish"));
        assert_eq!(bus.publish_metrics(), Default::default());
    }

    let receipt = block_on(bus.publish_checked(
        PublishRequest::new(topic, "order".to_owned()).expect("valid request"),
        AdmissionRequirement::ProviderOrDestinationAccepted,
    ))
    .expect("provider acceptance satisfies the new condition");
    assert_eq!(receipt.admission_outcome(), AdmissionOutcome::OpaqueAccepted);
    assert_eq!(
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "publish")
            .count(),
        1
    );

    let visible_spi = Arc::new(EmptyAdmissionSpi(FakeAsyncEventBusSpi::new()));
    let visible_bus = AsyncEventBus::from_spi(provider_id, visible_spi.clone()).expect("valid visible provider");
    let error = block_on(
        visible_bus.publish_checked(
            PublishRequest::new(
                Topic::<String>::new("async.checked.visible").unwrap(),
                "order".to_owned(),
            )
            .unwrap(),
            AdmissionRequirement::ProviderOrDestinationAccepted,
        ),
    )
    .expect_err("no destinations accepted");
    assert!(
        matches!(error, CheckedPublishError::Admission { receipt, reason: AdmissionCheckError::NoAcceptedDestination }
        if receipt.admission_outcome() == AdmissionOutcome::NoDestinations)
    );
    assert_eq!(
        visible_spi
            .0
            .operation_log()
            .iter()
            .filter(|operation| **operation == "publish")
            .count(),
        1
    );
}

#[test]
fn test_async_per_key_capability_is_checked_before_spi_subscribe() {
    for (capability, accepted) in [
        (OrderingCapability::None, false),
        (OrderingCapability::PerPartition, false),
        (OrderingCapability::PerKey, true),
        (OrderingCapability::PerSubscription, true),
    ] {
        let spi = Arc::new(OrderingTestSpi::new(capability));
        let bus = AsyncEventBus::from_spi(ProviderId::new("ordering-test").expect("valid provider"), spi.clone())
            .expect("valid provider capabilities");
        let options = SubscribeOptions::<u32>::builder()
            .ordering_policy(OrderingPolicy::PerKey)
            .build();
        let result = block_on(
            bus.subscribe(
                SubscribeRequest::new("keyed", topic())
                    .expect("valid subscriber")
                    .with_options(options),
            ),
        );
        if accepted {
            let mut subscription = result.expect("provider supports per-key delivery");
            assert_eq!(spi.subscribe_calls.load(Ordering::Acquire), 1);
            block_on(subscription.close()).expect("subscription closes");
        } else {
            assert!(matches!(
                result,
                Err(SubscribeError::Capability(CapabilityError::Unsupported {
                    capability: "ordering.per_key"
                }))
            ));
            assert_eq!(spi.subscribe_calls.load(Ordering::Acquire), 0);
        }
    }

    let spi = Arc::new(OrderingTestSpi::new(OrderingCapability::None));
    let bus = AsyncEventBus::from_spi(ProviderId::new("ordering-test").expect("valid provider"), spi.clone())
        .expect("valid provider capabilities");
    let mut subscription =
        block_on(bus.subscribe(SubscribeRequest::new("unordered", topic()).expect("valid subscriber")))
            .expect("default unordered subscription works without ordering capability");
    assert_eq!(spi.subscribe_calls.load(Ordering::Acquire), 1);
    block_on(subscription.close()).expect("subscription closes");
}

#[test]
fn test_async_subscription_close_is_idempotent_while_bus_runs() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new("repeat-close", topic()).expect("valid subscriber ID"))
            .await
            .expect("subscription");

        subscription.close().await.expect("first close succeeds");
        subscription.close().await.expect("repeated close succeeds");
        assert_eq!(spi.close_call_count(), 1, "provider receiver closes only once");

        let _ = bus.shutdown(ShutdownMode::Immediate).await.expect("bus shutdown");
        assert!(matches!(subscription.close().await, Err(LifecycleError::Closed)));
        assert_eq!(spi.close_call_count(), 1, "closed bus does not close receiver again");
    });
}

#[test]
fn test_async_subscription_capabilities_are_checked_before_spi_subscribe() {
    let requests = [
        (
            SubscribeOptions::<u32>::builder()
                .durability(SubscriptionDurability::Durable)
                .build(),
            "subscription_durability",
        ),
        (
            SubscribeOptions::<u32>::builder()
                .consumer_group(ConsumerGroup::new("workers").expect("valid consumer group"))
                .build(),
            "consumer_groups",
        ),
        (
            SubscribeOptions::<u32>::builder()
                .start_position(StartPosition::Earliest)
                .build(),
            "replay",
        ),
        (
            SubscribeOptions::<u32>::builder()
                .start_position(StartPosition::At("offset-1".into()))
                .build(),
            "replay",
        ),
    ];

    for (options, capability) in requests {
        let spi = Arc::new(OrderingTestSpi::new(OrderingCapability::None));
        let bus = AsyncEventBus::from_spi(ProviderId::new("capability-test").expect("valid provider"), spi.clone())
            .expect("valid provider capabilities");
        let result = block_on(
            bus.subscribe(
                SubscribeRequest::new("capability-check", topic())
                    .expect("valid subscriber")
                    .with_options(options),
            ),
        );
        match result {
            Err(SubscribeError::Capability(CapabilityError::Unsupported { capability: actual })) => {
                assert_eq!(actual, capability);
            }
            Ok(mut subscription) => {
                block_on(subscription.close()).expect("unexpected subscription is cleaned up");
                panic!("unsupported {capability} request reached the subscriber");
            }
            Err(error) => panic!("expected capability error, got {error}"),
        }
        assert_eq!(spi.subscribe_calls.load(Ordering::Acquire), 0);
    }

    let capabilities = EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::DURABLE,
        true,
        ReplayCapability::Position,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let spi = Arc::new(OrderingTestSpi::with_capabilities(capabilities));
    let bus = AsyncEventBus::from_spi(ProviderId::new("capability-test").expect("valid provider"), spi.clone())
        .expect("valid provider capabilities");
    let mut subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("capability-supported", topic())
                .expect("valid subscriber")
                .with_options(
                    SubscribeOptions::<u32>::builder()
                        .durability(SubscriptionDurability::Durable)
                        .consumer_group(ConsumerGroup::new("workers").expect("valid consumer group"))
                        .start_position(StartPosition::At("offset-1".into()))
                        .build(),
                ),
        ),
    )
    .expect("provider capabilities allow requested options");
    assert_eq!(spi.subscribe_calls.load(Ordering::Acquire), 1);
    block_on(subscription.close()).expect("subscription closes");
}

#[test]
fn test_async_subscription_is_created_without_spawning_until_run_is_driven() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let request = SubscribeRequest::new("async-test", topic()).expect("valid subscriber ID");

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        assert_eq!(spi.operation_log(), ["subscribe"]);
        assert_eq!(subscription.subscriber_id().as_str(), "async-test");
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
    assert_eq!(spi.operation_log(), ["subscribe", "close", "shutdown"]);
}

#[test]
fn test_receiver_object_drop_panic_after_close_releases_subscription_slot() {
    struct HandlerCapture(Arc<AtomicUsize>);

    impl Drop for HandlerCapture {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
        )
        .unwrap(),
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config)
        .expect("valid provider capabilities");
    let receiver_drops = Arc::new(AtomicUsize::new(0));
    let receiver_drop_count = receiver_drops.clone();
    spi.on_receiver_drop(move || {
        if receiver_drop_count.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("receiver object drop probe");
        }
    });

    let mut subscription = block_on(bus.subscribe(SubscribeRequest::new("drop-panic", topic()).unwrap())).unwrap();
    let handler_drops = Arc::new(AtomicUsize::new(0));
    let capture = HandlerCapture(handler_drops.clone());
    let mut runner = Box::pin(subscription.run(move |_| {
        let _capture = &capture;
        async { Ok(()) }
    }));
    assert!(crate::support::manual_async::poll_once(runner.as_mut()).is_pending());
    drop(runner);
    assert_eq!(AsyncSubscriptionRunState::Paused, subscription.run_state());
    assert_eq!(0, handler_drops.load(Ordering::SeqCst));

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(subscription.close())))
        .expect_err("receiver object destructor panic is propagated");
    assert_eq!(
        "receiver object drop probe",
        panic.downcast_ref::<&str>().copied().unwrap()
    );
    assert_eq!(1, spi.close_call_count());
    assert_eq!(1, receiver_drops.load(Ordering::SeqCst));

    let mut replacement = block_on(bus.subscribe(SubscribeRequest::new("replacement", topic()).unwrap()))
        .expect("successful close releases the only subscription slot even after receiver drop panic");
    assert_eq!(1, handler_drops.load(Ordering::SeqCst));
    block_on(subscription.close()).expect("repeated close is idempotent");
    assert_eq!(1, spi.close_call_count(), "original receiver close runs once");
    drop(subscription);
    block_on(replacement.close()).expect("replacement closes");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("bus shuts down");
}

#[test]
fn test_run_receiver_drop_panic_keeps_subscription_stopped() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("run-drop-panic", topic()).unwrap())).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_probe = drops.clone();
    spi.on_receiver_drop(move || {
        if drop_probe.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("natural receiver drop probe");
        }
    });

    let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(crate::support::manual_async::poll_once(runner.as_mut()).is_pending());
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Running);
    spi.close_receiver(subscription.id());

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(runner)))
        .expect_err("natural receiver object destructor panic is propagated");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"natural receiver drop probe"));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(spi.close_call_count(), 1);
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);

    assert!(matches!(
        block_on(subscription.run(|_| async { Ok(()) })),
        Err(ReceiveError::Closed)
    ));
    assert_eq!(spi.close_call_count(), 1);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn test_async_facade_bounds_in_flight_deliveries_across_subscriptions() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            std::num::NonZeroUsize::new(1).expect("limit"),
            std::num::NonZeroUsize::new(256).expect("limit"),
            std::num::NonZeroUsize::new(32).expect("limit"),
            std::num::NonZeroUsize::new(256).expect("limit"),
        )
        .expect("scheduling"),
    );
    let bus =
        AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi, config).expect("valid provider capabilities");
    let shared = Arc::new((
        std::sync::Mutex::new((false, false, 0_usize, Vec::<Waker>::new())),
        std::sync::Condvar::new(),
    ));

    let (first, second) = block_on(async {
        let first = bus
            .subscribe(SubscribeRequest::new("first", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        let second = bus
            .subscribe(SubscribeRequest::new("second", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        let _ = bus.publish(PublishRequest::new(topic(), 5).unwrap()).await.unwrap();
        (first, second)
    });

    let first_state = shared.clone();
    let first_runner = std::thread::spawn(move || {
        block_on(first.run(move |_| {
            let state = first_state.clone();
            async move {
                let (lock, changed) = &*state;
                {
                    let mut state = lock.lock().unwrap();
                    state.2 += 1;
                    state.1 = true;
                    changed.notify_all();
                }
                std::future::poll_fn(move |context| {
                    let mut state = lock.lock().unwrap();
                    if state.0 {
                        return std::task::Poll::Ready(());
                    }
                    state.3.push(context.waker().clone());
                    std::task::Poll::Pending
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    });

    let second_state = shared.clone();
    let second_runner = std::thread::spawn(move || {
        block_on(second.run(move |_| {
            let state = second_state.clone();
            async move {
                let (lock, changed) = &*state;
                {
                    let mut state = lock.lock().unwrap();
                    state.2 += 1;
                    changed.notify_all();
                }
                std::future::poll_fn(move |context| {
                    let mut state = lock.lock().unwrap();
                    if state.0 {
                        return std::task::Poll::Ready(());
                    }
                    state.3.push(context.waker().clone());
                    std::task::Poll::Pending
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    });

    let (lock, changed) = &*shared;
    let state = lock.lock().unwrap();
    let (state, _) = changed
        .wait_timeout_while(state, std::time::Duration::from_secs(2), |state| !state.1)
        .unwrap();
    drop(state);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let observed_concurrency = shared.0.lock().unwrap().2;

    {
        let (lock, _) = &*shared;
        let mut state = lock.lock().unwrap();
        state.0 = true;
        let wakers = std::mem::take(&mut state.3);
        drop(state);
        for waker in wakers {
            waker.wake();
        }
    }
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    for result in [first_runner.join().unwrap(), second_runner.join().unwrap()] {
        assert!(
            result.is_ok() || matches!(result, Err(ReceiveError::Closed)),
            "immediate shutdown may close a session before its runner reacquires the lease: {result:?}"
        );
    }
    assert_eq!(observed_concurrency, 1, "bus-wide cap must include all subscriptions");
}

#[test]
fn test_async_admission_waiter_keeps_polling_existing_tasks_until_a_slot_is_released() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            std::num::NonZeroUsize::new(1).expect("limit"),
            std::num::NonZeroUsize::new(256).expect("limit"),
            std::num::NonZeroUsize::new(32).expect("limit"),
            std::num::NonZeroUsize::new(256).expect("limit"),
        )
        .expect("scheduling"),
    );
    let bus =
        AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi, config).expect("valid provider capabilities");
    let subscription =
        block_on(bus.subscribe(SubscribeRequest::new("admission-progress", topic()).expect("valid subscriber ID")))
            .unwrap();
    block_on(async {
        let _ = bus.publish(PublishRequest::new(topic(), 1).unwrap()).await.unwrap();
        let _ = bus.publish(PublishRequest::new(topic(), 2).unwrap()).await.unwrap();
    });
    let state = Arc::new(std::sync::Mutex::new((false, 0_usize, Vec::<Waker>::new())));
    let handler_state = state.clone();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |_| {
            let state = handler_state.clone();
            async move {
                let call = {
                    let mut state = state.lock().unwrap();
                    state.1 += 1;
                    state.1
                };
                if call == 1 {
                    std::future::poll_fn(move |context| {
                        let mut state = state.lock().unwrap();
                        if state.0 {
                            return std::task::Poll::Ready(Ok::<(), DeliveryError>(()));
                        }
                        state.2.push(context.waker().clone());
                        std::task::Poll::Pending
                    })
                    .await?;
                }
                Ok::<(), DeliveryError>(())
            }
        }))
    });
    for _ in 0..100 {
        if state.lock().unwrap().1 == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert_eq!(state.lock().unwrap().1, 1);
    let wakers = {
        let mut state = state.lock().unwrap();
        state.0 = true;
        std::mem::take(&mut state.2)
    };
    for waker in wakers {
        waker.wake();
    }
    for _ in 0..100 {
        if state.lock().unwrap().1 == 2 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    let second_started = state.lock().unwrap().1 == 2;
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
    assert!(
        second_started,
        "the active task must make progress while another delivery awaits admission"
    );
}

#[test]
fn test_idle_async_subscription_does_not_consume_handler_capacity() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            std::num::NonZeroUsize::new(1).expect("limit"),
            std::num::NonZeroUsize::new(256).expect("limit"),
            std::num::NonZeroUsize::new(32).expect("limit"),
            std::num::NonZeroUsize::new(256).expect("limit"),
        )
        .expect("scheduling"),
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config)
        .expect("valid provider capabilities");
    let (active, idle) = block_on(async {
        let active = bus
            .subscribe(SubscribeRequest::new("active", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        let idle = bus
            .subscribe(
                SubscribeRequest::new("idle", Topic::<u32>::new("idle.topic").expect("valid idle topic"))
                    .expect("valid subscriber ID"),
            )
            .await
            .unwrap();
        let _ = bus.publish(PublishRequest::new(topic(), 7).unwrap()).await.unwrap();
        (active, idle)
    });
    let idle = Arc::new(idle);
    let idle_from_runner = idle.clone();
    let idle_runner =
        std::thread::spawn(move || block_on(idle_from_runner.run(|_| async { Ok::<(), DeliveryError>(()) })));
    let deadline = Instant::now() + Duration::from_secs(5);
    while idle.run_state() != AsyncSubscriptionRunState::Running && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let idle_running = idle.run_state() == AsyncSubscriptionRunState::Running;
    if !idle_running {
        let _ = block_on(bus.shutdown(ShutdownMode::Immediate));
        let _ = idle_runner.join();
        panic!("idle runner must start before shutdown");
    }

    let active_started = Arc::new(AtomicBool::new(false));
    let active_started_from_runner = active_started.clone();
    let active_runner = std::thread::spawn(move || {
        block_on(active.run(move |_| {
            active_started_from_runner.store(true, Ordering::Release);
            async { Ok::<(), DeliveryError>(()) }
        }))
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !active_started.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let was_dispatched = active_started.load(Ordering::Acquire);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    active_runner.join().unwrap().unwrap();
    idle_runner.join().unwrap().unwrap();
    assert!(was_dispatched, "an idle receive must not hold a handler execution slot");
}

#[test]
fn test_async_received_wait_includes_deliveries_queued_for_admission() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            std::num::NonZeroUsize::new(1).expect("limit"),
            std::num::NonZeroUsize::new(256).expect("limit"),
            std::num::NonZeroUsize::new(32).expect("limit"),
            std::num::NonZeroUsize::new(256).expect("limit"),
        )
        .expect("scheduling"),
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config).unwrap();
    let topic = Topic::<usize>::new("received-admission.topic").unwrap();
    let sub_a = block_on(async {
        bus.subscribe(SubscribeRequest::new("received-a", topic.clone()).unwrap())
            .await
            .unwrap()
    });
    spi.enqueue(InboundMessage::new(
        TopicAddress::new(topic.name()).unwrap(),
        EventId::new("received-a-event").unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        TransportPayload::Native(Arc::new(1_usize)),
        Some(SettlementToken::new(sub_a.id(), "received-a-token")),
        Default::default(),
    ));
    let block_a = Arc::new(AtomicBool::new(false));
    let started_a = Arc::new(AtomicUsize::new(0));
    let keep_a_pending = block_a.clone();
    let mark_a_started = started_a.clone();
    let mut run_a = Box::pin(sub_a.run(move |_| {
        let keep_pending = keep_a_pending.clone();
        let started = mark_a_started.clone();
        async move {
            started.fetch_add(1, Ordering::AcqRel);
            std::future::poll_fn(move |_| {
                if keep_pending.load(Ordering::Acquire) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await
        }
    }));
    for _ in 0..8 {
        assert!(crate::support::manual_async::poll_once(run_a.as_mut()).is_pending());
        if started_a.load(Ordering::Acquire) == 1 {
            break;
        }
    }
    assert_eq!(started_a.load(Ordering::Acquire), 1);
    let sub_b = block_on(bus.subscribe(SubscribeRequest::new("received-b", topic.clone()).unwrap())).unwrap();
    let _ = block_on(bus.publish(PublishRequest::new(topic.clone(), 2).unwrap())).unwrap();
    let mut run_b = Box::pin(sub_b.run(|_| async { Ok(()) }));
    for _ in 0..8 {
        assert!(crate::support::manual_async::poll_once(run_b.as_mut()).is_pending());
    }
    assert_eq!(
        block_on(bus.wait_for_received_deliveries(&topic, Some(Duration::ZERO))).unwrap(),
        WaitOutcome::TimedOut,
        "received delivery must remain tracked while it waits for admission",
    );
    drop(run_b);
    block_a.store(true, Ordering::Release);
    let _ = crate::support::manual_async::poll_once(run_a.as_mut());
    drop(run_a);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn test_async_subscription_runs_different_ordering_keys_concurrently() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("parallel-keys", topic())
                .expect("valid subscriber ID")
                .with_options(options),
        ),
    )
    .unwrap();
    for (payload, key) in [(1, "left"), (2, "right")] {
        let _ = block_on(
            bus.publish(
                PublishRequest::builder()
                    .topic(topic())
                    .payload(payload)
                    .ordering_key(key)
                    .build()
                    .unwrap(),
            ),
        )
        .unwrap();
    }
    let gate = Arc::new(AtomicBool::new(false));
    let started = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
    let handler_gate = gate.clone();
    let handler_started = started.clone();
    let mut run = Box::pin(subscription.run(move |delivery| {
        let gate = handler_gate.clone();
        let started = handler_started.clone();
        let payload = *delivery.payload();
        async move {
            started.lock().unwrap().push(payload);
            std::future::poll_fn(|_| {
                if gate.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            Ok::<(), DeliveryError>(())
        }
    }));

    let mut observed = Vec::new();
    let mut snapshot = subscription.delivery_metrics().metrics;
    for _ in 0..1024 {
        let _ = crate::support::manual_async::poll_once(run.as_mut());
        observed = started.lock().unwrap().clone();
        snapshot = subscription.delivery_metrics().metrics;
        if observed.len() == 2 && snapshot.running_handlers == 2 {
            break;
        }
    }
    let gate_was_closed = !gate.load(Ordering::Acquire);
    let mut observed_payloads = observed.clone();
    observed_payloads.sort_unstable();
    gate.store(true, Ordering::Release);
    for _ in 0..1024 {
        let _ = crate::support::manual_async::poll_once(run.as_mut());
        observed = started.lock().unwrap().clone();
        if observed.len() == 2 {
            break;
        }
    }
    drop(run);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(
        observed_payloads,
        vec![1, 2],
        "both payloads must start; last snapshot: {snapshot:?}"
    );
    assert!(gate_was_closed, "both handlers must start while the gate is closed");
    assert_eq!(
        snapshot.running_handlers, 2,
        "both handlers must be running concurrently"
    );
}

#[test]
fn test_async_subscription_preserves_order_for_the_same_ordering_key() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi).expect("valid provider capabilities");
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("serial-key", topic())
                .expect("valid subscriber ID")
                .with_options(options),
        ),
    )
    .unwrap();
    for payload in [1, 2] {
        let _ = block_on(
            bus.publish(
                PublishRequest::builder()
                    .topic(topic())
                    .payload(payload)
                    .ordering_key("same")
                    .build()
                    .unwrap(),
            ),
        )
        .unwrap();
    }
    let gate = Arc::new(AtomicBool::new(false));
    let started = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));
    let handler_gate = gate.clone();
    let handler_started = started.clone();
    let mut run = Box::pin(subscription.run(move |delivery| {
        let gate = handler_gate.clone();
        let started = handler_started.clone();
        let payload = *delivery.payload();
        async move {
            started.lock().unwrap().push(payload);
            std::future::poll_fn(|_| {
                if gate.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            Ok::<(), DeliveryError>(())
        }
    }));

    let mut snapshot = subscription.delivery_metrics().metrics;
    for _ in 0..1024 {
        let _ = crate::support::manual_async::poll_once(run.as_mut());
        snapshot = subscription.delivery_metrics().metrics;
        if snapshot.lane_waiting == 1 {
            break;
        }
    }
    let started_before_release = started.lock().unwrap().clone();
    let lane_waiting = snapshot.lane_waiting;
    gate.store(true, Ordering::Release);
    let mut started_after_release = started_before_release.clone();
    for _ in 0..1024 {
        let _ = crate::support::manual_async::poll_once(run.as_mut());
        started_after_release = started.lock().unwrap().clone();
        if started_after_release == [1, 2] {
            break;
        }
    }
    drop(run);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(
        lane_waiting, 1,
        "second delivery must already be owned; last snapshot: {snapshot:?}"
    );
    assert_eq!(
        started_before_release,
        vec![1],
        "the successor must not bypass its predecessor"
    );
    assert_eq!(started_after_release, vec![1, 2], "both payloads must start in order");
}

#[test]
fn test_immediate_shutdown_drops_same_key_lane_waiters_but_finishes_started_handler() {
    const CASE: &str = "immediate-shutdown-lane-waiter";
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(CASE) {
        crate::support::isolated_process::run_case_with_timeout(
            "test_immediate_shutdown_drops_same_key_lane_waiters_but_finishes_started_handler",
            CASE,
            Duration::from_secs(10),
        );
        return;
    }

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("immediate-lane-waiter", topic())
                .expect("valid subscriber ID")
                .with_options(options),
        ),
    )
    .unwrap();
    for payload in [1, 2] {
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            format!("immediate-lane-{payload}"),
        ))));
    }
    let state = Arc::new(std::sync::Mutex::new((false, 0_usize, Vec::<Waker>::new())));
    let handler_state = state.clone();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |_| {
            let state = handler_state.clone();
            async move {
                state.lock().unwrap().1 += 1;
                std::future::poll_fn(move |context| {
                    let mut state = state.lock().unwrap();
                    if state.0 {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    state.2.push(context.waker().clone());
                    std::task::Poll::Pending
                })
                .await
            }
        }))
    });

    for _ in 0..100 {
        if state.lock().unwrap().1 > 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert_eq!(state.lock().unwrap().1, 1);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(
        crate::support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Pending
    ));
    assert_eq!(state.lock().unwrap().1, 1, "Immediate must not start lane-waiting work");
    let wakers = {
        let mut state = state.lock().unwrap();
        state.0 = true;
        std::mem::take(&mut state.2)
    };
    for waker in wakers {
        waker.wake();
    }
    let _ = block_on(shutdown).unwrap();
    runner.join().unwrap().unwrap();
    assert_eq!(
        spi.settlement_count(),
        1,
        "only the started delivery is settled before receiver close"
    );
    let operations = spi.operation_log();
    assert!(
        operations.contains(&"close"),
        "receiver close delegates recovery of unsettled delivery to the SPI"
    );
}

#[test]
fn test_immediate_shutdown_does_not_start_lane_waiter_when_predecessor_finishes() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("immediate-lane-race", topic())
                .expect("valid subscriber ID")
                .with_options(options),
        ),
    )
    .unwrap();
    for payload in [1_u32, 2_u32] {
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            format!("immediate-race-{payload}"),
        ))));
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let release_first = Arc::new(AtomicBool::new(false));
    let waiters = Arc::new(std::sync::Mutex::new(Vec::<Waker>::new()));
    let calls_for_handler = calls.clone();
    let release_for_handler = release_first.clone();
    let waiters_for_handler = waiters.clone();
    let mut run = Box::pin(subscription.run(move |_| {
        let call = calls_for_handler.fetch_add(1, Ordering::AcqRel);
        let release = release_for_handler.clone();
        let waiters = waiters_for_handler.clone();
        async move {
            if call == 0 {
                std::future::poll_fn(move |context| {
                    if release.load(Ordering::Acquire) {
                        Poll::Ready(Ok(()))
                    } else {
                        waiters.lock().unwrap().push(context.waker().clone());
                        Poll::Pending
                    }
                })
                .await
            } else {
                Ok(())
            }
        }
    }));
    for _ in 0..100 {
        let _ = crate::support::manual_async::poll_once(run.as_mut());
        if calls.load(Ordering::Acquire) == 1 && spi.operation_log().iter().filter(|op| **op == "receive").count() >= 2
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(calls.load(Ordering::Acquire), 1);
    drop(run);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(
        crate::support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Pending
    ));
    release_first.store(true, Ordering::Release);
    for waker in std::mem::take(&mut *waiters.lock().unwrap()) {
        waker.wake();
    }
    let mut outcome = None;
    for _ in 0..100 {
        if let Poll::Ready(result) = crate::support::manual_async::poll_once(shutdown.as_mut()) {
            outcome = Some(result);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let _ = outcome
        .expect("shutdown should finish after the started handler exits")
        .unwrap();
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "queued lane work must not start after Immediate"
    );
    assert_eq!(spi.settlement_count(), 1, "only the started delivery should be settled");
}

#[test]
fn test_shutdown_skips_an_unstarted_subscription_dropped_by_its_owner() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("dropped-before-run", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        drop(subscription);
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(spi.operation_log(), ["subscribe", "shutdown"]);
}

#[test]
fn test_cancelling_shutdown_while_unstarted_receiver_close_is_pending_allows_retry() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_close();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");

    block_on(async {
        let _subscription = bus
            .subscribe(SubscribeRequest::new("cancel-pending-close", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();

        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        assert!(matches!(
            crate::support::manual_async::poll_once(shutdown.as_mut()),
            Poll::Pending
        ));
        assert_eq!(spi.operation_log(), ["subscribe", "close"]);
        drop(shutdown);

        spi.release_close();
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(spi.operation_log(), ["subscribe", "close", "close", "shutdown"]);
}

#[test]
fn test_shutdown_polls_each_unstarted_receiver_close_before_waiting() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_close();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");

    block_on(async {
        let _first = bus
            .subscribe(SubscribeRequest::new("parallel-close-first", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        let _second = bus
            .subscribe(SubscribeRequest::new("parallel-close-second", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();

        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        assert!(matches!(
            crate::support::manual_async::poll_once(shutdown.as_mut()),
            Poll::Pending
        ));
        assert_eq!(
            spi.close_call_count(),
            2,
            "each receiver close must be polled before shutdown waits"
        );
        assert_eq!(
            spi.max_concurrent_close_calls(),
            2,
            "both close futures are live in the same poll"
        );

        spi.release_close();
        let _ = block_on(shutdown).expect("shutdown closes both receivers");
    });
}

#[test]
fn test_graceful_shutdown_timeout_bounds_pending_unstarted_receiver_close() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_close();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let timeout = std::time::Duration::from_millis(100);

    block_on(async {
        let _subscription = bus
            .subscribe(SubscribeRequest::new("graceful-pending-close", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();

        assert!(matches!(
            bus.shutdown(ShutdownMode::Graceful { timeout }).await,
            Err(ShutdownError::TimedOut { timeout: elapsed }) if elapsed == timeout
        ));
        assert_eq!(spi.operation_log(), ["subscribe", "close"]);

        spi.release_close();
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(spi.operation_log(), ["subscribe", "close", "close", "shutdown"]);
}

#[test]
fn test_graceful_shutdown_keeps_progress_when_one_receiver_close_times_out() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_first_close_call();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let timeout = std::time::Duration::from_millis(100);

    block_on(async {
        let _first = bus
            .subscribe(SubscribeRequest::new("graceful-close-first", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        let _second = bus
            .subscribe(SubscribeRequest::new("graceful-close-second", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();

        assert!(matches!(
            bus.shutdown(ShutdownMode::Graceful { timeout }).await,
            Err(ShutdownError::TimedOut { timeout: elapsed }) if elapsed == timeout
        ));
        assert_eq!(
            2,
            spi.close_call_count(),
            "both receiver closes are polled under one deadline"
        );
        assert_eq!(
            1,
            spi.completed_close_call_count(),
            "one receiver completes before the shared deadline"
        );

        let report = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        assert_eq!(ShutdownOutcome::Complete, report.outcome);
    });

    assert_eq!(
        3,
        spi.close_call_count(),
        "retry closes only the receiver left pending after timeout"
    );
    assert_eq!(2, spi.completed_close_call_count());
    assert_eq!(
        1,
        spi.operation_log().iter().filter(|&&call| call == "shutdown").count(),
        "provider shutdown begins only after all receivers close"
    );
}

#[test]
fn test_shutdown_waits_for_in_flight_subscribe_to_close_late_receiver_first() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_subscribe();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let receiver_drop_was_scoped = Arc::new(AtomicBool::new(false));
    {
        let bus = bus.clone();
        let observed = receiver_drop_was_scoped.clone();
        spi.on_receiver_drop(move || {
            let mut nested = Box::pin(bus.shutdown(ShutdownMode::Immediate));
            observed.store(
                matches!(
                    crate::support::manual_async::poll_once(nested.as_mut()),
                    Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                        operation: "shutdown"
                    })))
                ),
                Ordering::SeqCst,
            );
        });
    }
    let (subscribe_sender, subscribe_receiver) = std::sync::mpsc::channel();
    let subscribing_bus = bus.clone();
    let subscriber = std::thread::spawn(move || {
        let result = block_on(
            subscribing_bus
                .subscribe(SubscribeRequest::new("subscribe-shutdown-race", topic()).expect("valid subscriber ID")),
        );
        let _ = subscribe_sender.send(result);
    });
    for _ in 0..100 {
        if spi.operation_log().contains(&"subscribe") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert!(spi.operation_log().contains(&"subscribe"));

    let (shutdown_sender, shutdown_receiver) = std::sync::mpsc::channel();
    let shutdown_bus = bus.clone();
    let shutdown_thread = std::thread::spawn(move || {
        let result = block_on(shutdown_bus.shutdown(ShutdownMode::Immediate));
        let _ = shutdown_sender.send(result);
    });
    let early_shutdown = shutdown_receiver
        .recv_timeout(std::time::Duration::from_millis(30))
        .ok();
    let shutdown_was_early = early_shutdown.is_some();
    spi.release_subscribe();

    let subscribe_result = subscribe_receiver
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let shutdown_result = early_shutdown.unwrap_or_else(|| {
        shutdown_receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap()
    });
    subscriber.join().unwrap();
    shutdown_thread.join().unwrap();

    assert!(matches!(subscribe_result, Err(SubscribeError::Closed)));
    assert!(receiver_drop_was_scoped.load(Ordering::SeqCst));
    assert_eq!(shutdown_result.unwrap().outcome, ShutdownOutcome::Complete);
    assert!(
        !shutdown_was_early,
        "provider shutdown must wait for in-flight subscribe cleanup"
    );
    let operations = spi.operation_log();
    let close = operations.iter().position(|operation| *operation == "close").unwrap();
    let shutdown = operations
        .iter()
        .position(|operation| *operation == "shutdown")
        .unwrap();
    assert!(close < shutdown, "late receiver must close before provider shutdown");
}

#[test]
fn test_cancelling_rejected_subscribe_scopes_late_receiver_drop() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_subscribe();
    spi.pause_close();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let receiver_drop_was_scoped = Arc::new(AtomicBool::new(false));
    {
        let bus = bus.clone();
        let observed = receiver_drop_was_scoped.clone();
        spi.on_receiver_drop(move || {
            let mut nested = Box::pin(bus.shutdown(ShutdownMode::Immediate));
            observed.store(
                matches!(
                    crate::support::manual_async::poll_once(nested.as_mut()),
                    Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                        operation: "shutdown"
                    })))
                ),
                Ordering::SeqCst,
            );
        });
    }
    let request = SubscribeRequest::new("cancel-rejected-subscribe", topic()).expect("valid subscriber ID");
    let mut subscribe = Box::pin(bus.subscribe(request));
    assert!(crate::support::manual_async::poll_once(subscribe.as_mut()).is_pending());

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(crate::support::manual_async::poll_once(shutdown.as_mut()).is_pending());
    spi.release_subscribe();
    assert!(crate::support::manual_async::poll_once(subscribe.as_mut()).is_pending());
    drop(subscribe);
    assert!(receiver_drop_was_scoped.load(Ordering::SeqCst));
    assert!(matches!(
        crate::support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn test_failed_late_receiver_close_is_recorded_and_drop_is_scoped() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_subscribe();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let receiver_drop_was_scoped = Arc::new(AtomicBool::new(false));
    {
        let bus = bus.clone();
        let observed = receiver_drop_was_scoped.clone();
        spi.on_receiver_drop(move || {
            let mut nested = Box::pin(bus.shutdown(ShutdownMode::Immediate));
            observed.store(
                matches!(
                    crate::support::manual_async::poll_once(nested.as_mut()),
                    Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                        operation: "shutdown"
                    })))
                ),
                Ordering::SeqCst,
            );
        });
    }
    let mut subscribe =
        Box::pin(bus.subscribe(SubscribeRequest::new("failed-late-receiver", topic()).expect("valid subscriber ID")));
    assert!(crate::support::manual_async::poll_once(subscribe.as_mut()).is_pending());
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(crate::support::manual_async::poll_once(shutdown.as_mut()).is_pending());

    spi.fail_next_close();
    spi.release_subscribe();
    assert!(matches!(
        crate::support::manual_async::poll_once(subscribe.as_mut()),
        Poll::Ready(Err(SubscribeError::Closed))
    ));
    assert!(receiver_drop_was_scoped.load(Ordering::SeqCst));
    match crate::support::manual_async::poll_once(shutdown.as_mut()) {
        Poll::Ready(Err(ShutdownError::SubscriptionClose(errors))) => {
            assert_eq!(errors.total_count(), 1);
            let failure = errors.iter().next().expect("canonical close failure");
            assert_eq!(failure.subscriber_id().as_str(), "failed-late-receiver");
            assert_eq!(failure.error().operation(), "close");
        }
        other => panic!("expected the original late receiver close error, got {other:?}"),
    }
}

#[test]
fn test_cancelling_pending_subscribe_releases_admission_for_shutdown() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_subscribe();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let request = SubscribeRequest::new("cancel-pending-subscribe", topic()).expect("valid subscriber ID");
    let mut subscribe = Box::pin(bus.subscribe(request));
    assert!(matches!(
        crate::support::manual_async::poll_once(subscribe.as_mut()),
        Poll::Pending
    ));
    drop(subscribe);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    let outcome = match crate::support::manual_async::poll_once(shutdown.as_mut()) {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => panic!("cancelled subscribe must release its admission guard"),
    };
    assert_eq!(outcome.outcome, ShutdownOutcome::Complete);
    assert_eq!(spi.shutdown_transition_count(), 1);
    assert!(
        !spi.operation_log().contains(&"close"),
        "no receiver was returned to the facade"
    );
}

#[test]
fn test_asynchronous_facade_rejects_sync_subscriber_interceptors_before_provider_subscription() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let options = SubscribeOptions::<u32>::builder()
        .interceptor(|delivery, next| next(delivery))
        .build();
    let request = SubscribeRequest::new("sync-only-interceptor", topic())
        .expect("valid subscriber ID")
        .with_options(options);

    let error = match block_on(bus.subscribe(request)) {
        Ok(_) => panic!("async facade must reject sync middleware"),
        Err(error) => error,
    };
    match error {
        SubscribeError::Configuration(ConfigurationError::InvalidField { field, .. }) => {
            assert_eq!(field, "sync_subscriber_interceptor")
        }
        other => panic!("expected runtime-model configuration error, got {other}"),
    }
    assert!(spi.operation_log().is_empty());
}

#[test]
fn test_async_handler_cannot_await_either_shutdown_mode_on_its_own_bus() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let observed = Arc::new(AtomicBool::new(false));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("self-shutdown", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(None));
        let observed_by_handler = observed.clone();
        let bus_by_handler = bus.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let bus = bus_by_handler.clone();
                let observed = observed_by_handler.clone();
                async move {
                    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
                        timeout: std::time::Duration::from_secs(60),
                    }));
                    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
                    std::future::poll_fn(move |context| {
                        let would_deadlock = |result| {
                            matches!(
                                result,
                                Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                                    operation: "shutdown"
                                })))
                            )
                        };
                        let graceful_rejected = would_deadlock(graceful.as_mut().poll(context));
                        let immediate_rejected = would_deadlock(immediate.as_mut().poll(context));
                        observed.store(graceful_rejected && immediate_rejected, Ordering::Release);
                        Poll::Ready(())
                    })
                    .await;
                    Ok(())
                }
            }))
        });
        for _ in 0..100 {
            if observed.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert!(observed.load(Ordering::Acquire));
}

#[test]
fn test_async_idle_wait_timeout_wakes_without_other_bus_activity() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let handler_started = Arc::new(AtomicBool::new(false));
    let release_handler = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(std::sync::Mutex::new(None::<Waker>));
    let delivered_topic = topic();

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("idle-timeout", delivered_topic.clone()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(None));
        let started = handler_started.clone();
        let release = release_handler.clone();
        let saved_waker = handler_waker.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let started = started.clone();
                let release = release.clone();
                let saved_waker = saved_waker.clone();
                async move {
                    started.store(true, Ordering::Release);
                    std::future::poll_fn(move |context| {
                        if release.load(Ordering::Acquire) {
                            return Poll::Ready(Ok(()));
                        }
                        *saved_waker.lock().unwrap() = Some(context.waker().clone());
                        Poll::Pending
                    })
                    .await
                }
            }))
        });
        for _ in 0..100 {
            if handler_started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(handler_started.load(Ordering::Acquire));

        let (wake_sender, wake_receiver) = std::sync::mpsc::channel();
        let waker = Waker::from(Arc::new(WakeOnSignal(wake_sender)));
        let mut context = Context::from_waker(&waker);
        let mut waiting =
            Box::pin(bus.wait_for_received_deliveries(&delivered_topic, Some(std::time::Duration::from_millis(15))));
        let first_poll_pending = matches!(waiting.as_mut().poll(&mut context), Poll::Pending);
        let timer_woke = wake_receiver
            .recv_timeout(std::time::Duration::from_millis(200))
            .is_ok();
        let timeout_result = timer_woke.then(|| waiting.as_mut().poll(&mut context));
        drop(waiting);

        release_handler.store(true, Ordering::Release);
        if let Some(waker) = handler_waker.lock().unwrap().take() {
            waker.wake();
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();

        assert!(first_poll_pending);
        assert!(timer_woke, "timeout must wake the waiter without another bus signal");
        assert!(matches!(timeout_result, Some(Poll::Ready(Ok(WaitOutcome::TimedOut)))));
    });
}

#[test]
fn test_injected_manual_timer_wakes_idle_timeout_and_cleans_up_waiter() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), clock.new_timer())
        .expect("valid provider capabilities");
    let delivered_topic = topic();
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(std::sync::Mutex::new(None::<std::task::Waker>));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("manual-timeout", delivered_topic.clone()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(None));
        let started_by_handler = started.clone();
        let release_by_handler = release.clone();
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
                            return std::task::Poll::Ready(Ok(()));
                        }
                        *waker.lock().unwrap() = Some(cx.waker().clone());
                        std::task::Poll::Pending
                    })
                    .await
                }
            }))
        });
        for _ in 0..100 {
            if started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(started.load(Ordering::Acquire));

        let (cancel_sender, _cancel_receiver) = std::sync::mpsc::channel();
        let cancel_waker = Waker::from(Arc::new(WakeOnSignal(cancel_sender)));
        let mut cancel_context = Context::from_waker(&cancel_waker);
        let mut cancelled_wait =
            Box::pin(bus.wait_for_received_deliveries(&delivered_topic, Some(std::time::Duration::from_secs(60))));
        assert!(matches!(
            cancelled_wait.as_mut().poll(&mut cancel_context),
            Poll::Pending
        ));
        assert_eq!(clock.pending_waiters(), 1);
        drop(cancelled_wait);
        assert_eq!(
            clock.pending_waiters(),
            0,
            "dropping a wait must cancel its timer registration"
        );

        let bus_for_wait = bus.clone();
        let topic_for_wait = delivered_topic.clone();
        let waiter = std::thread::spawn(move || {
            block_on(
                bus_for_wait.wait_for_received_deliveries(&topic_for_wait, Some(std::time::Duration::from_secs(30))),
            )
        });
        assert!(clock.wait_for_waiters(1, std::time::Duration::from_secs(1)));
        assert_eq!(clock.pending_waiters(), 1);
        clock.advance(std::time::Duration::from_secs(30)).unwrap();
        assert!(matches!(waiter.join().unwrap(), Ok(WaitOutcome::TimedOut)));
        assert!(clock.wait_for_waiters(0, std::time::Duration::from_secs(1)));
        assert_eq!(clock.pending_waiters(), 0);

        release.store(true, Ordering::Release);
        if let Some(waker) = handler_waker.lock().unwrap().take() {
            waker.wake();
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_wait_for_received_deliveries_only_registers_timer_when_needed() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let timer = Arc::new(FailingRegistrationTimer::new());
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), timer.clone())
        .expect("valid provider capabilities");
    let delivered_topic = topic();

    block_on(async {
        for timeout in [Some(Duration::from_secs(1)), Some(Duration::ZERO)] {
            assert_eq!(
                bus.wait_for_received_deliveries(&delivered_topic, timeout)
                    .await
                    .unwrap(),
                WaitOutcome::Idle,
            );
        }
        assert_eq!(timer.registrations.load(Ordering::SeqCst), 0);

        let subscription = bus
            .subscribe(SubscribeRequest::new("failing-timer", delivered_topic.clone()).unwrap())
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(None));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let started_by_handler = started.clone();
        let release_by_handler = release.clone();
        let mut runner = Box::pin(subscription.run(move |_| {
            let started = started_by_handler.clone();
            let release = release_by_handler.clone();
            async move {
                started.store(true, Ordering::Release);
                std::future::poll_fn(move |_| {
                    if release.load(Ordering::Acquire) {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Pending
                    }
                })
                .await
            }
        }));
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..16 {
            assert!(runner.as_mut().poll(&mut context).is_pending());
            if started.load(Ordering::Acquire) {
                break;
            }
        }
        assert!(started.load(Ordering::Acquire), "handler must hold a received delivery");

        assert_eq!(
            bus.wait_for_received_deliveries(&delivered_topic, Some(Duration::ZERO))
                .await
                .unwrap(),
            WaitOutcome::TimedOut,
        );
        assert_eq!(timer.registrations.load(Ordering::SeqCst), 0);

        assert!(matches!(
            bus.wait_for_received_deliveries(&delivered_topic, Some(Duration::from_secs(1)))
                .await,
            Err(LifecycleError::Timer(TimeError::TimerUnavailable { .. })),
        ));
        assert_eq!(timer.registrations.load(Ordering::SeqCst), 1);

        release.store(true, Ordering::Release);
        for _ in 0..16 {
            if runner.as_mut().poll(&mut context).is_ready() {
                break;
            }
        }
        drop(runner);
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn test_graceful_shutdown_registration_failure_stops_admission() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let timer = Arc::new(FailingRegistrationTimer::new());
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi, timer.clone())
        .expect("valid provider capabilities");

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("registration-failure", topic()).unwrap())
            .await
            .unwrap();
        let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
        let mut context = Context::from_waker(Waker::noop());
        assert!(runner.as_mut().poll(&mut context).is_pending());

        assert!(matches!(
            bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(1)
            })
            .await,
            Err(ShutdownError::Lifecycle(LifecycleError::Timer(
                TimeError::TimerUnavailable { .. }
            )))
        ));
        assert_eq!(1, timer.registrations.load(Ordering::SeqCst));
        assert_eq!(None, bus.shutdown_report());
        assert!(matches!(runner.as_mut().poll(&mut context), Poll::Ready(Ok(()))));
        assert!(matches!(
            bus.publish(PublishRequest::new(topic(), 1).unwrap()).await,
            Err(error) if matches!(error.cause(), qubit_event_bus::PublishError::Closed)
        ));
        assert!(matches!(
            bus.subscribe(SubscribeRequest::new("after-close", topic()).unwrap())
                .await,
            Err(SubscribeError::Closed)
        ));

        let report = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("immediate retry completes shutdown");
        assert_eq!(report, bus.shutdown_report().unwrap());
        assert_eq!(1, timer.registrations.load(Ordering::SeqCst));
        assert_eq!(
            report,
            bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(1)
            })
            .await
            .unwrap()
        );
        assert_eq!(1, timer.registrations.load(Ordering::SeqCst));
    });
}

#[test]
fn test_graceful_shutdown_preserves_deadline_calculation_overflow() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let timer = Arc::new(FailingRegistrationTimer::new());
    timer.clock.advance(Duration::from_secs(1)).unwrap();
    let bus = AsyncEventBus::with_timer(ProviderId::new("deadline-overflow").unwrap(), spi, timer.clone())
        .expect("valid provider capabilities");

    assert!(matches!(
        block_on(bus.shutdown(ShutdownMode::Graceful { timeout: Duration::MAX })),
        Err(ShutdownError::Lifecycle(LifecycleError::Timer(
            TimeError::InstantOverflow
        )))
    ));
    assert_eq!(0, timer.registrations.load(Ordering::SeqCst));
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("immediate retry completes shutdown");
}

struct AdvanceClockOnWake {
    clock: Arc<ManualMonotonicClock>,
    amount: Duration,
    enabled: AtomicBool,
}

impl Wake for AdvanceClockOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.enabled.swap(false, Ordering::AcqRel) {
            self.clock.advance(self.amount).expect("advance notification clock");
        }
    }
}

#[test]
fn test_graceful_shutdown_counts_stop_notification_in_budget() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = Arc::new(ManualMonotonicClock::new());
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("graceful-notification-budget").unwrap(),
        spi.clone(),
        clock.new_timer(),
    )
    .expect("valid provider capabilities");

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("notification-budget", topic()).unwrap())
            .await
            .unwrap();
        let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
        let wake = Arc::new(AdvanceClockOnWake {
            clock: clock.clone(),
            amount: Duration::from_secs(1),
            enabled: AtomicBool::new(false),
        });
        let waker = Waker::from(wake.clone());
        let mut context = Context::from_waker(&waker);
        assert!(runner.as_mut().poll(&mut context).is_pending());

        wake.enabled.store(true, Ordering::Release);
        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        }));
        assert!(matches!(
            shutdown.as_mut().poll(&mut context),
            Poll::Ready(Err(ShutdownError::TimedOut { timeout })) if timeout == Duration::from_secs(1)
        ));
        assert_eq!(0, spi.close_call_count());
        assert_eq!(0, spi.shutdown_transition_count());

        drop(shutdown);
        assert!(matches!(runner.as_mut().poll(&mut context), Poll::Ready(Ok(()))));
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn test_zero_budget_shutdown_does_not_start_receiver_close() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), clock.new_timer())
        .expect("valid provider capabilities");

    block_on(async {
        let _subscription = bus
            .subscribe(SubscribeRequest::new("zero-budget-close", topic()).expect("valid subscription"))
            .await
            .expect("subscription");

        assert!(matches!(
            bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::ZERO
            })
            .await,
            Err(ShutdownError::TimedOut {
                timeout: Duration::ZERO
            })
        ));
        assert_eq!(
            None,
            bus.shutdown_report(),
            "caller timeout must not publish a provider report"
        );
        assert_eq!(
            0,
            spi.close_call_count(),
            "an expired budget must not start a receiver close"
        );

        let _ = bus.shutdown(ShutdownMode::Immediate).await.expect("retry shutdown");
        assert_eq!(
            1,
            spi.close_call_count(),
            "the retry closes the retained receiver exactly once"
        );
    });
}

#[test]
fn test_zero_budget_shutdown_allows_active_runner_to_close() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), clock.new_timer())
        .expect("valid provider capabilities");

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("active-zero-budget-close", topic()).expect("valid subscription"))
            .await
            .expect("subscription");
        let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
        let mut context = Context::from_waker(Waker::noop());

        assert!(runner.as_mut().poll(&mut context).is_pending());
        assert!(
            spi.operation_log().contains(&"receive"),
            "runner must reach its receive wait before shutdown"
        );
        assert_eq!(0, spi.close_call_count());

        assert!(matches!(
            bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::ZERO
            })
            .await,
            Err(ShutdownError::TimedOut {
                timeout: Duration::ZERO
            })
        ));
        assert_eq!(0, spi.close_call_count(), "shutdown did not start receiver close");

        assert!(matches!(runner.as_mut().poll(&mut context), Poll::Ready(Ok(()))));
        assert_eq!(1, spi.close_call_count(), "the active runner closes its receiver");

        let report = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("retry completes bus shutdown");
        assert_eq!(ShutdownOutcome::Complete, report.outcome);
        assert_eq!(1, spi.close_call_count(), "successful runner close is not repeated");
    });
}

#[test]
fn test_receiver_close_batch_stops_after_deadline_expires_during_first_close() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), clock.new_timer())
        .expect("valid provider capabilities");
    let clock_for_close = clock.clone();
    spi.on_next_close_poll(move || {
        clock_for_close
            .advance(Duration::from_secs(1))
            .expect("advance close deadline");
    });

    block_on(async {
        let _first = bus
            .subscribe(SubscribeRequest::new("deadline-close-a", topic()).expect("valid subscription"))
            .await
            .expect("subscription");
        let _second = bus
            .subscribe(SubscribeRequest::new("deadline-close-b", topic()).expect("valid subscription"))
            .await
            .expect("subscription");

        assert!(matches!(
            bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(1)
            })
            .await,
            Err(ShutdownError::TimedOut { timeout }) if timeout == Duration::from_secs(1)
        ));
        assert_eq!(1, spi.close_call_count(), "no receiver close may start after expiry");

        let _ = bus
            .shutdown(ShutdownMode::Immediate)
            .await
            .expect("retry remaining close");
        assert_eq!(
            2,
            spi.close_call_count(),
            "the next shutdown closes the remaining receiver"
        );
    });
}

#[test]
fn test_receiver_close_completion_at_deadline_is_kept_before_timeout() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), clock.new_timer())
        .expect("valid provider capabilities");
    let clock_for_close = clock.clone();
    spi.on_next_close_poll(move || {
        clock_for_close
            .advance(Duration::from_secs(1))
            .expect("advance close deadline");
    });

    block_on(async {
        let _subscription = bus
            .subscribe(SubscribeRequest::new("deadline-last-close", topic()).expect("valid subscription"))
            .await
            .expect("subscription");

        assert!(matches!(
            bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(1)
            })
            .await,
            Err(ShutdownError::TimedOut { timeout }) if timeout == Duration::from_secs(1)
        ));
        assert_eq!(1, spi.close_call_count());
        assert_eq!(
            1,
            spi.completed_close_call_count(),
            "successful close is recorded before timeout"
        );

        let _ = bus.shutdown(ShutdownMode::Immediate).await.expect("finish shutdown");
        assert_eq!(
            1,
            spi.close_call_count(),
            "retry must not close a deregistered receiver again"
        );
    });
}

#[test]
fn test_empty_receiver_batch_proceeds_to_provider_with_zero_budget() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), clock.new_timer())
        .expect("valid provider capabilities");

    let report = block_on(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::ZERO,
    }))
    .expect("empty close batch proceeds to existing shutdown stages");
    assert_eq!(ShutdownOutcome::Complete, report.outcome);
    assert_eq!(0, spi.close_call_count());
}

/// Measures facade polling overhead for a ready receiver batch and sparse close
/// wakes.
///
/// The decision threshold is 10% of a representative two-second application
/// shutdown budget (200 ms for 256 receivers). The fake provider isolates
/// facade traversal from real provider wait time.
#[test]
#[ignore = "release-only receiver close polling measurement"]
fn test_measure_receiver_close_batch() {
    fn percentile<T: Copy>(sorted: &[T], percentile: usize) -> T {
        let index = (sorted.len() - 1) * percentile / 100;
        sorted[index]
    }

    fn run_sample(count: usize, sparse_wakes: bool) -> (Duration, usize) {
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        if sparse_wakes {
            spi.pause_close();
        }
        let bus = AsyncEventBus::from_spi(ProviderId::new("close-measurement").unwrap(), spi.clone())
            .expect("valid provider capabilities");
        let subscriptions = block_on(async {
            let mut subscriptions = Vec::with_capacity(count);
            for index in 0..count {
                let subscriber_id = format!("measure-{index}");
                subscriptions.push(
                    bus.subscribe(SubscribeRequest::new(&subscriber_id, topic()).expect("valid subscription"))
                        .await
                        .expect("subscription starts"),
                );
            }
            subscriptions
        });
        let before_polls = spi.close_poll_count();
        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        let mut context = Context::from_waker(Waker::noop());
        let started = Instant::now();
        if sparse_wakes {
            assert!(shutdown.as_mut().poll(&mut context).is_pending());
            for index in 0..count {
                assert!(spi.release_one_close(), "receiver {index} has a pending close");
                let result = shutdown.as_mut().poll(&mut context);
                if index + 1 == count {
                    assert!(result.is_ready(), "last close completes shutdown");
                } else {
                    assert!(result.is_pending(), "remaining receivers keep shutdown pending");
                }
            }
        } else {
            assert!(shutdown.as_mut().poll(&mut context).is_ready());
        }
        let elapsed = started.elapsed();
        let polls = spi.close_poll_count() - before_polls;
        drop(subscriptions);
        (elapsed, polls)
    }

    println!(
        "receiver_close_measurement os={} arch={} rustc={} samples=25 budget=2s threshold=200ms",
        std::env::consts::OS,
        std::env::consts::ARCH,
        option_env!("CARGO_PKG_RUST_VERSION").unwrap_or("unspecified")
    );
    for count in [1, 64, 256] {
        for (scenario, sparse_wakes) in [("ready_batch", false), ("one_by_one_wakes", true)] {
            let mut nanos = Vec::with_capacity(25);
            let mut polls = Vec::with_capacity(25);
            for _ in 0..25 {
                let (elapsed, poll_count) = run_sample(count, sparse_wakes);
                nanos.push(elapsed.as_nanos());
                polls.push(poll_count);
            }
            nanos.sort_unstable();
            polls.sort_unstable();
            println!(
                "receiver_close_measurement receivers={count} scenario={scenario} polls_p50={} polls_p95={} elapsed_us_p50={} elapsed_us_p95={}",
                percentile(&polls, 50),
                percentile(&polls, 95),
                percentile(&nanos, 50) / 1_000,
                percentile(&nanos, 95) / 1_000
            );
        }
    }
}

#[test]
fn test_async_failure_with_unsupported_reject_reports_unavailable_without_spi_call() {
    let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(
        crate::support::fake_spi::native_no_settlement_capabilities(),
    ));
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let diagnostics = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = diagnostics.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| captured.lock().unwrap().push(diagnostic.clone()));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("no-reject", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").unwrap(),
            EventId::new("unsettled-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(5_u32)),
            Some(SettlementToken::new(subscription.id(), "no-reject-token")),
            Default::default(),
        ));
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("expected")),
                })
            }))
        });
        for _ in 0..100 {
            if diagnostics
                .lock()
                .unwrap()
                .iter()
                .any(|item| matches!(item, Diagnostic::SettlementUnavailable { .. }))
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert_eq!(spi.settlement_count(), 0);
    assert!(diagnostics.lock().unwrap().iter().any(|item| matches!(item,
        Diagnostic::SettlementUnavailable { event_id, requested: DeliveryDisposition::Reject, .. }
            if event_id.as_str() == "unsettled-event"
    )));
}

#[test]
fn test_async_dropping_diagnostic_observer_releases_its_callback_capture() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi).expect("valid provider capabilities");
    let captured = Arc::new(());
    let weak = Arc::downgrade(&captured);
    let handle = bus.observe_diagnostics(move |_| {
        let _keep_capture = &captured;
    });

    drop(handle);

    assert!(weak.upgrade().is_none());
}

#[test]
fn test_async_observer_snapshot_self_unregister_keeps_two_rejections() {
    use std::any::TypeId;

    use qubit_event_bus::local::AsyncLocalEventBusProvider;
    use qubit_event_bus::local::LocalEventBusConfig;
    use qubit_event_bus::registry::EventBusConfig;
    use qubit_event_bus::spi::OutboundMessage;
    use qubit_event_bus::spi::TransportPayload;
    use qubit_spi::AsyncServiceProvider;

    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().queue_capacity(1).provider_options());
    let spi = block_on(AsyncLocalEventBusProvider.create_configured(&config)).expect("async local SPI");
    let topic_address = TopicAddress::new("observer.snapshot.async").expect("topic address");
    let mut receivers = block_on(async {
        let mut receivers = Vec::new();
        for id in [801_u64, 802] {
            receivers.push(
                spi.subscribe(SpiSubscriptionRequest::new(
                    Id::new(id),
                    topic_address.clone(),
                    qubit_event_bus::model::SubscriberId::new(format!("observer-{id}")).unwrap(),
                    None,
                    SubscriptionDurability::Ephemeral,
                    StartPosition::New,
                    Default::default(),
                    TypeId::of::<String>(),
                ))
                .await
                .expect("raw subscription"),
            );
        }
        receivers
    });
    let raw_publish = |event_id: &'static str| {
        block_on(spi.publish(OutboundMessage::new(
            topic_address.clone(),
            EventId::new(event_id).unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(String::from("fill"))),
        )))
        .expect("raw publication")
    };
    let PublishAcknowledgement::DestinationAdmissions(filled) = raw_publish("fill-async") else {
        panic!("local SPI exposes destination outcomes");
    };
    assert_eq!(filled.len(), 2);
    assert!(
        filled
            .iter()
            .all(|item| matches!(item.status(), qubit_event_bus::model::AdmissionStatus::Accepted))
    );
    let PublishAcknowledgement::DestinationAdmissions(full) = raw_publish("full-async") else {
        panic!("local SPI exposes destination outcomes");
    };
    assert_eq!(full.len(), 2);
    assert!(
        full.iter()
            .all(|item| matches!(item.status(), qubit_event_bus::model::AdmissionStatus::Rejected(_)))
    );

    let bus = AsyncEventBus::from_spi(ProviderId::new("observer-snapshot-async").unwrap(), spi.clone()).unwrap();
    let capture_slot = Arc::new(std::sync::Mutex::new(None));
    let capture_probe = Arc::new(());
    let capture_probe_weak = Arc::downgrade(&capture_probe);
    let capture_probe_for_callback = capture_probe.clone();
    drop(capture_probe);
    let capture_calls = Arc::new(AtomicUsize::new(0));
    let capture_calls_in_callback = capture_calls.clone();
    let capture_observer = bus.observe_diagnostics(move |diagnostic| {
        let _capture = &capture_probe_for_callback;
        if matches!(diagnostic, Diagnostic::AdmissionRejected { .. }) {
            capture_calls_in_callback.fetch_add(1, Ordering::AcqRel);
        }
    });
    *capture_slot.lock().unwrap() = Some(capture_observer);
    let slot = Arc::new(std::sync::Mutex::new(None));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let probe = Arc::new(());
    let probe_weak = Arc::downgrade(&probe);
    let probe_for_callback = probe.clone();
    drop(probe);
    let slot_in_callback = slot.clone();
    let seen_in_callback = seen.clone();
    let weak_in_callback = probe_weak.clone();
    let observer = bus.observe_diagnostics(move |diagnostic| {
        let _capture = &probe_for_callback;
        if let Diagnostic::AdmissionRejected {
            event_id,
            subscription_id,
            ..
        } = diagnostic
        {
            seen_in_callback
                .lock()
                .unwrap()
                .push((event_id.clone(), *subscription_id));
            let removed = { slot_in_callback.lock().unwrap().take() };
            drop(removed);
            assert!(
                weak_in_callback.upgrade().is_some(),
                "selected snapshot retains captures"
            );
        }
    });
    *slot.lock().unwrap() = Some(observer);
    let snapshot_request = || {
        PublishRequest::new(
            Topic::new("observer.snapshot.async").expect("typed topic"),
            String::from("payload"),
        )
        .expect("publish request")
    };
    let capture_slot_in_interceptor = capture_slot.clone();
    let weak_in_interceptor = capture_probe_weak.clone();
    let options = PublishOptions::<String>::builder()
        .interceptor(move |envelope| {
            let removed = { capture_slot_in_interceptor.lock().unwrap().take() };
            drop(removed);
            assert!(
                weak_in_interceptor.upgrade().is_some(),
                "selected snapshot retains captures"
            );
            Ok(Some(envelope))
        })
        .build();
    let _ = block_on(bus.publish(snapshot_request().with_options(options))).expect("publish");
    let first = seen.lock().unwrap().clone();
    assert_eq!(first.len(), 2);
    assert_eq!(capture_calls.load(Ordering::Acquire), 2);
    assert_eq!(first[0].0, first[1].0);
    assert_ne!(first[0].1, first[1].1);
    assert!(first.iter().any(|entry| entry.1 == Id::new(801)));
    assert!(first.iter().any(|entry| entry.1 == Id::new(802)));
    assert!(probe_weak.upgrade().is_none(), "completed snapshot releases captures");
    assert!(
        capture_probe_weak.upgrade().is_none(),
        "completed snapshot releases captures"
    );
    let _ = block_on(bus.publish(snapshot_request())).expect("publish");
    assert_eq!(seen.lock().unwrap().len(), 2);

    block_on(async {
        for receiver in &mut receivers {
            receiver.close().await.expect("close raw subscription");
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.expect("bus shutdown");
    });
}

#[test]
fn test_async_wrong_settlement_token_is_diagnosed_before_capability_gate() {
    let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(
        crate::support::fake_spi::native_no_settlement_capabilities(),
    ));
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let diagnostics = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = diagnostics.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| captured.lock().unwrap().push(diagnostic.clone()));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("wrong-token", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").unwrap(),
            EventId::new("wrong-token-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(1_u32)),
            Some(SettlementToken::new(Id::new(9_999), "wrong-owner")),
            Default::default(),
        ));
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("expected")),
                })
            }))
        });
        for _ in 0..100 {
            if diagnostics.lock().unwrap().iter().any(
                |item| matches!(item, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "settlement"),
            ) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        assert!(matches!(
            runner.join().expect("runner"),
            Err(ReceiveError::Stopped(reason))
                if matches!(
                    reason.as_ref(),
                    SubscriptionStopReason::Settlement {
                        termination: SettlementTermination::InvalidToken,
                        ..
                    }
                )
        ));
    });
    assert!(diagnostics.lock().unwrap().iter().any(|item| matches!(item,
        Diagnostic::InternalFailure { origin, message }
            if origin.as_ref() == "settlement" && message.contains("another subscription")
    )));
    assert_eq!(spi.settlement_count(), 0);
}

#[test]
fn test_inbound_dead_letter_marker_prevents_recursive_async_dead_letter_publish() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let mut headers = Headers::new();
    headers.insert("x-qubit-event-bus-dead-letter".into(), "v1".into());
    let options = SubscribeOptions::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::with_topic_name("test.dead").unwrap())
        .build();

    block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("marked", topic())
                    .expect("valid subscriber ID")
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").unwrap(),
            EventId::new("marked-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            headers,
            None,
            TransportPayload::Native(Arc::new(42_u32)),
            Some(SettlementToken::new(subscription.id(), "marked-dead-letter")),
            Default::default(),
        ));
        let observed = Arc::new(AtomicBool::new(false));
        let observed_by_handler = observed.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery: Delivery<u32>| {
                observed_by_handler.store(delivery.context().is_dead_letter(), Ordering::SeqCst);
                async {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("again")),
                    })
                }
            }))
        });
        for _ in 0..100 {
            if observed.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
        assert!(observed.load(Ordering::SeqCst));
        assert_eq!(spi.operation_log().iter().filter(|call| **call == "publish").count(), 0);
        assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Reject]);
    });
}

#[test]
fn test_async_run_processes_deliveries_on_the_callers_executor_and_shutdown_cancels_receive() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let request = SubscribeRequest::new("async-run", topic()).expect("valid subscriber ID");
    let delivered = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        let delivered_by_handler = delivered.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery| {
                let delivered = delivered_by_handler.clone();
                async move {
                    assert_eq!(*delivery.payload(), 42);
                    delivered.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
            }))
        });

        let _ = bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if delivered.load(Ordering::Acquire) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(delivered.load(Ordering::Acquire), 1);
        assert_eq!(
            bus.wait_for_received_deliveries(&topic(), None).await.unwrap(),
            WaitOutcome::Idle
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_async_middleware_wraps_the_handler_in_registration_order() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = order.clone();
    let options = SubscribeOptions::builder()
        .async_interceptor(move |delivery: Delivery<u32>, next: AsyncSubscriberNext<u32>| {
            let seen = seen.clone();
            Box::pin(async move {
                seen.lock().unwrap().push("before");
                next(delivery).await?;
                seen.lock().unwrap().push("after");
                Ok(())
            }) as SpiFuture<'static, Result<(), DeliveryError>>
        })
        .build();
    let request = SubscribeRequest::new("async-middleware", topic())
        .expect("valid subscriber ID")
        .with_options(options);

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        let seen_by_handler = order.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let seen = seen_by_handler.clone();
                async move {
                    seen.lock().unwrap().push("handler");
                    Ok(())
                }
            }))
        });
        let _ = bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if order.lock().unwrap().len() == 3 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert_eq!(*order.lock().unwrap(), ["before", "handler", "after"]);
}

#[test]
fn test_async_facade_global_middleware_wraps_typed_middleware_and_handler() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let global_calls = calls.clone();
    let config = EventBusFacadeConfig::new().async_subscriber_interceptor(
        move |delivery: Delivery<u32>, next: AsyncSubscriberNext<u32>| {
            let calls = global_calls.clone();
            Box::pin(async move {
                calls.lock().unwrap().push("global-before");
                next(delivery).await?;
                calls.lock().unwrap().push("global-after");
                Ok(())
            }) as SpiFuture<'static, Result<(), DeliveryError>>
        },
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config)
        .expect("valid provider capabilities");
    let typed_calls = calls.clone();
    let options = SubscribeOptions::<u32>::builder()
        .async_interceptor(move |delivery, next| {
            let calls = typed_calls.clone();
            Box::pin(async move {
                calls.lock().unwrap().push("typed-before");
                next(delivery).await?;
                calls.lock().unwrap().push("typed-after");
                Ok(())
            }) as SpiFuture<'static, Result<(), DeliveryError>>
        })
        .build();

    block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("async-global-chain", topic())
                    .expect("valid subscriber ID")
                    .with_options(options),
            )
            .await
            .unwrap();
        let handler_calls = calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let calls = handler_calls.clone();
                async move {
                    calls.lock().unwrap().push("handler");
                    Ok(())
                }
            }))
        });
        let _ = bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if calls.lock().unwrap().len() == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(
            *calls.lock().unwrap(),
            [
                "global-before",
                "typed-before",
                "handler",
                "typed-after",
                "global-after"
            ]
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_async_facade_rejects_sync_global_subscriber_middleware() {
    let config = EventBusFacadeConfig::new()
        .subscriber_interceptor(|_delivery: Delivery<u32>, _next: SubscriberNext<u32>| Ok(()));
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let result = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config);
    assert!(matches!(
        result,
        Err(FacadeBuildError::Configuration(ConfigurationError::InvalidField {
            field: "sync_subscriber_interceptor",
            ..
        }))
    ));
    assert_eq!(spi.capabilities_calls(), 0);
}

#[test]
fn test_async_handler_future_panic_is_reported_and_does_not_escape_the_runner() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi).expect("valid provider capabilities");
    let failed = Arc::new(AtomicBool::new(false));
    let observed = failed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::DeliveryFailed { .. }) {
            observed.store(true, Ordering::Release);
        }
    });
    let request = SubscribeRequest::new("async-panic", topic()).expect("valid subscriber ID");

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(|_| async move {
                panic!("handler future panic");
            }))
        });
        let _ = bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if failed.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(failed.load(Ordering::Acquire));
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_async_retry_reinvokes_the_handler_and_uses_the_configured_qubit_retry_policy() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi).expect("valid provider capabilities");
    let attempts = Arc::new(AtomicUsize::new(0));
    let options = SubscribeOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .error_handler(|_, _| FailureDirective::Retry)
        .build();
    let request = SubscribeRequest::new("async-retry", topic())
        .expect("valid subscriber ID")
        .with_options(options);

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        let attempts_in_handler = attempts.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let attempts = attempts_in_handler.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::AcqRel) == 0 {
                        Err(DeliveryError::Handler {
                            source: Box::new(std::io::Error::other("retry me")),
                        })
                    } else {
                        Ok(())
                    }
                }
            }))
        });
        let _ = bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if attempts.load(Ordering::Acquire) == 2 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_async_provider_attempt_is_stable_across_handler_retry() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let options = SubscribeOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .error_handler(|_, _| FailureDirective::Retry)
        .build();
    let request = SubscribeRequest::new("async-provider-attempt", topic())
        .expect("valid subscriber ID")
        .with_options(options);
    let (observed_tx, observed_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        let handler_calls = calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery| {
                let observed_tx = observed_tx.clone();
                let handler_calls = handler_calls.clone();
                async move {
                    observed_tx
                        .send((
                            delivery.context().provider_attempt(),
                            delivery.context().retry_attempt(),
                        ))
                        .expect("observer remains connected");
                    if handler_calls.fetch_add(1, Ordering::AcqRel) == 0 {
                        Err(DeliveryError::Handler {
                            source: Box::new(std::io::Error::other("retry locally")),
                        })
                    } else {
                        Ok(())
                    }
                }
            }))
        });
        spi.enqueue(
            crate::support::fake_spi::inbound_message(None)
                .with_provider_attempt(NonZeroU32::new(1).expect("positive provider attempt")),
        );
        assert_eq!(
            (Some(1), 1),
            observed_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("first handler attempt")
        );
        assert_eq!(
            (Some(1), 2),
            observed_rx.recv_timeout(Duration::from_secs(2)).expect("facade retry")
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_async_provider_redelivery_attempt_reaches_handler() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let options = SubscribeOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .error_handler(|_, _| FailureDirective::Requeue)
        .build();
    let request = SubscribeRequest::new("async-provider-redelivery-attempt", topic())
        .expect("valid subscriber ID")
        .with_options(options);
    let (observed_tx, observed_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        let subscription_id = subscription.id();
        let handler_calls = calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery| {
                let observed_tx = observed_tx.clone();
                let handler_calls = handler_calls.clone();
                async move {
                    observed_tx
                        .send((
                            delivery.context().provider_attempt(),
                            delivery.context().retry_attempt(),
                        ))
                        .expect("observer remains connected");
                    if handler_calls.fetch_add(1, Ordering::AcqRel) == 0 {
                        Err(DeliveryError::Handler {
                            source: Box::new(std::io::Error::other("request provider redelivery")),
                        })
                    } else {
                        Ok(())
                    }
                }
            }))
        });
        spi.enqueue(
            crate::support::fake_spi::inbound_message(Some(SettlementToken::new(subscription_id, "redelivery-1")))
                .with_provider_attempt(NonZeroU32::new(1).expect("positive provider attempt")),
        );
        assert_eq!(
            (Some(1), 1),
            observed_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("first provider delivery")
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while spi.settlement_dispositions() != [DeliveryDisposition::Retry] && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Retry]);
        spi.enqueue(
            crate::support::fake_spi::inbound_message(Some(SettlementToken::new(subscription_id, "redelivery-2")))
                .with_provider_attempt(NonZeroU32::new(2).expect("positive provider attempt")),
        );
        assert_eq!(
            (Some(2), 1),
            observed_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("provider redelivery")
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
        assert_eq!(
            spi.settlement_dispositions(),
            [DeliveryDisposition::Retry, DeliveryDisposition::Accept]
        );
    });
}

/// Verifies asynchronous manual mode accepts only explicit positive
/// acknowledgements.
#[test]
fn test_async_manual_acknowledgement_requires_an_explicit_ack() {
    for (subscriber, action, expected) in [
        ("async-manual-ack", 1, DeliveryDisposition::Accept),
        ("async-manual-nack", 2, DeliveryDisposition::Retry),
        ("async-manual-pending", 0, DeliveryDisposition::Reject),
    ] {
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone())
            .expect("valid provider capabilities");
        let options = SubscribeOptions::<u32>::builder()
            .ack_mode(AckMode::Manual)
            .error_handler(move |_, _| {
                if action == 2 {
                    FailureDirective::Requeue
                } else {
                    FailureDirective::Discard
                }
            })
            .build();
        block_on(async {
            let subscription = bus
                .subscribe(
                    SubscribeRequest::new(subscriber, topic())
                        .expect("valid subscriber ID")
                        .with_options(options),
                )
                .await
                .unwrap();
            spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
                subscription.id(),
                subscriber,
            ))));
            let runner = std::thread::spawn(move || {
                block_on(subscription.run(move |delivery| async move {
                    match action {
                        1 => delivery.acknowledgement().ack().unwrap(),
                        2 => delivery.acknowledgement().nack().unwrap(),
                        _ => {}
                    }
                    Ok(())
                }))
            });
            let settlement_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while spi.settlement_count() == 0 && std::time::Instant::now() < settlement_deadline {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert_eq!(spi.settlement_count(), 1);
            assert_eq!(spi.settlement_dispositions(), [expected]);
            let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
            runner.join().unwrap().unwrap();
        });
    }
}

/// Verifies asynchronous middleware and error callbacks surround every retry
/// attempt.
#[test]
fn test_async_interceptor_and_error_handler_wrap_each_failed_retry_attempt() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let before = calls.clone();
    let after = calls.clone();
    let error = calls.clone();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_by_handler = attempts.clone();
    let handler_log = calls.clone();
    let options = SubscribeOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .async_interceptor(move |delivery, next: AsyncSubscriberNext<u32>| {
            before.lock().unwrap().push("before");
            let after = after.clone();
            Box::pin(async move {
                let result = next(delivery).await;
                after.lock().unwrap().push("after");
                result
            }) as SpiFuture<'static, Result<(), DeliveryError>>
        })
        .error_handler(move |_, _| {
            error.lock().unwrap().push("error");
            FailureDirective::Retry
        })
        .build();
    block_on(async {
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("async-middleware-retry", topic())
                    .expect("valid subscriber ID")
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "middleware-retry",
        ))));
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let attempt = attempts_by_handler.fetch_add(1, Ordering::AcqRel);
                handler_log
                    .lock()
                    .unwrap()
                    .push(if attempt == 0 { "handler-1" } else { "handler-2" });
                async move {
                    if attempt == 0 {
                        Err(DeliveryError::Handler {
                            source: Box::new(std::io::Error::other("retry")),
                        })
                    } else {
                        Ok(())
                    }
                }
            }))
        });
        for _ in 0..100 {
            if spi.settlement_count() > 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Accept]);
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert_eq!(
        *calls.lock().unwrap(),
        ["before", "handler-1", "after", "error", "before", "handler-2", "after"]
    );
}

/// Verifies failed asynchronous deliveries map directives to provider
/// settlement outcomes.
#[test]
fn test_async_failure_directives_settle_requeue_discard_and_dead_letter_outcomes() {
    /// Runs a failing delivery and returns settlement outcomes and provider
    /// calls.
    fn run_case(
        directive: FailureDirective,
        fail_dead_letter_publish: bool,
    ) -> (Vec<DeliveryDisposition>, Vec<&'static str>) {
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone())
            .expect("valid provider capabilities");
        let mut builder = SubscribeOptions::<u32>::builder().error_handler(move |_, _| directive);
        if directive == FailureDirective::DeadLetter {
            builder = builder
                .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
                .dead_letter(DeadLetterPolicy::with_topic_name("test.dead").unwrap());
        }
        block_on(async {
            let subscription = bus
                .subscribe(
                    SubscribeRequest::new("async-directive", topic())
                        .expect("valid subscriber ID")
                        .with_options(builder.build()),
                )
                .await
                .unwrap();
            if fail_dead_letter_publish {
                spi.fail_next_publish();
            }
            spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
                subscription.id(),
                "directive-event",
            ))));
            let runner = std::thread::spawn(move || {
                block_on(subscription.run(|_| async {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("directive failure")),
                    })
                }))
            });
            for _ in 0..100 {
                if spi.settlement_count() > 0 {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            assert!(spi.settlement_count() > 0, "directive reaches provider settlement");
            let dispositions = spi.settlement_dispositions();
            let operations = spi.operation_log();
            let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
            runner.join().unwrap().unwrap();
            (dispositions, operations)
        })
    }

    let (requeued, _) = run_case(FailureDirective::Requeue, false);
    assert_eq!(requeued, [DeliveryDisposition::Retry]);
    let (discarded, _) = run_case(FailureDirective::Discard, false);
    assert_eq!(discarded, [DeliveryDisposition::Reject]);
    let (dead_lettered, success_operations) = run_case(FailureDirective::DeadLetter, false);
    assert!(dead_lettered.contains(&DeliveryDisposition::Reject));
    assert!(
        success_operations.contains(&"publish"),
        "dead-letter success publishes the record"
    );
    let (retried_dead_letter, failure_operations) = run_case(FailureDirective::DeadLetter, true);
    assert!(!retried_dead_letter.is_empty());
    assert!(
        retried_dead_letter
            .iter()
            .all(|disposition| *disposition == DeliveryDisposition::Reject)
    );
    assert!(
        failure_operations.contains(&"publish"),
        "dead-letter forwarding retries within the configured budget"
    );
    assert_eq!(
        failure_operations
            .iter()
            .filter(|operation| **operation == "publish")
            .count(),
        2
    );
}

#[test]
fn test_async_unconfirmed_dead_letter_send_is_not_republished_or_settled() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    block_on(async {
        let options = SubscribeOptions::<u32>::builder()
            .retry_policy(RetryPolicy::builder().max_attempts(3).build().unwrap())
            .error_handler(|_, _| FailureDirective::DeadLetter)
            .dead_letter(DeadLetterPolicy::with_topic_name("test.dead").unwrap())
            .build();
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("unconfirmed-dead-letter", topic())
                    .expect("valid subscriber ID")
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.unconfirm_next_publish();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "unconfirmed-source-token",
        ))));
        let error = subscription
            .run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failed")),
                })
            })
            .await
            .expect_err("unconfirmed forwarding leaves source unsettled");
        assert!(matches!(error, ReceiveError::DeadLetterForwardFailed { .. }));
        assert_eq!(spi.operation_log().iter().filter(|call| **call == "publish").count(), 1);
        assert!(
            spi.settlement_dispositions().is_empty(),
            "source token remains unsettled"
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn test_async_dead_letter_forward_exhaustion_leaves_the_source_token_unsettled() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    block_on(async {
        let options = SubscribeOptions::<u32>::builder()
            .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
            .error_handler(|_, _| FailureDirective::DeadLetter)
            .dead_letter(DeadLetterPolicy::with_topic_name("test.dead").unwrap())
            .build();
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("dead-letter-exhaustion", topic())
                    .expect("valid subscriber ID")
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.fail_next_publish();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "unsettled-source-token",
        ))));
        let error = subscription
            .run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failed")),
                })
            })
            .await
            .expect_err("exhausted forwarding reports a terminal receive error");
        assert!(matches!(error, ReceiveError::DeadLetterForwardFailed { .. }));
        assert!(
            spi.settlement_dispositions().is_empty(),
            "the source token remains unsettled"
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn test_cancelling_the_run_future_preserves_an_already_received_delivery_for_resume() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let request = SubscribeRequest::new("async-resume", topic()).expect("valid subscriber ID");
    let first_started = Arc::new(AtomicBool::new(false));
    let allow_completion = Arc::new(AtomicBool::new(false));
    let handler_calls = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        let _ = bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        let first_started_by_handler = first_started.clone();
        let allow_completion_by_handler = allow_completion.clone();
        let handler_calls_by_handler = handler_calls.clone();
        let mut first_run = Box::pin(subscription.run(move |_| {
            let started = first_started_by_handler.clone();
            let allow_completion = allow_completion_by_handler.clone();
            let handler_calls = handler_calls_by_handler.clone();
            async move {
                started.store(true, Ordering::Release);
                handler_calls.fetch_add(1, Ordering::AcqRel);
                std::future::poll_fn(|context| {
                    if allow_completion.load(Ordering::Acquire) {
                        std::task::Poll::Ready(Ok(()))
                    } else {
                        context.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                })
                .await
            }
        }));
        for _ in 0..100 {
            let _ = crate::support::manual_async::poll_once(first_run.as_mut());
            if first_started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(first_started.load(Ordering::Acquire));
        drop(first_run);

        allow_completion.store(true, Ordering::Release);
        let replacement_calls = Arc::new(AtomicUsize::new(0));
        let replacement_calls_by_handler = replacement_calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                replacement_calls_by_handler.fetch_add(1, Ordering::AcqRel);
                async { Ok(()) }
            }))
        });
        for _ in 0..100 {
            if spi.settlement_count() == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(
            handler_calls.load(Ordering::Acquire),
            1,
            "resume must continue the original handler future"
        );
        assert_eq!(
            replacement_calls.load(Ordering::Acquire),
            0,
            "a resumed task must not use the replacement handler"
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_dropping_a_paused_subscription_drops_receiver_and_recovers_unsettled_delivery() {
    type ShutdownObservation = Arc<Mutex<Option<Poll<Result<(), ShutdownError>>>>>;

    struct ShutdownOnDrop {
        bus: AsyncEventBus,
        observed: ShutdownObservation,
    }

    impl Drop for ShutdownOnDrop {
        fn drop(&mut self) {
            let mut shutdown = Box::pin(self.bus.shutdown(ShutdownMode::Immediate));
            let result = crate::support::manual_async::poll_once(shutdown.as_mut()).map(|result| result.map(|_| ()));
            *self.observed.lock().unwrap() = Some(result);
        }
    }

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let handler_started = Arc::new(AtomicBool::new(false));
    let observed: ShutdownObservation = Arc::new(Mutex::new(None));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("drop-paused", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "drop-recovery",
        ))));
        let started = handler_started.clone();
        let probe_bus = bus.clone();
        let probe_observed = observed.clone();
        let mut run = Box::pin(subscription.run(move |_| {
            let started = started.clone();
            let bus = probe_bus.clone();
            let observed = probe_observed.clone();
            async move {
                let _probe = ShutdownOnDrop { bus, observed };
                started.store(true, Ordering::Release);
                std::future::pending::<Result<(), DeliveryError>>().await
            }
        }));
        for _ in 0..100 {
            let _ = crate::support::manual_async::poll_once(run.as_mut());
            if handler_started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(handler_started.load(Ordering::Acquire));
        drop(run);
        assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Paused);
        assert!(observed.lock().unwrap().is_none());

        let peer = bus
            .subscribe(
                SubscribeRequest::new(
                    "drop-paused-peer",
                    Topic::<u32>::new("drop-paused-peer").expect("valid peer topic"),
                )
                .expect("valid peer request"),
            )
            .await
            .unwrap();
        let mut peer_run = Box::pin(peer.run(|_| std::future::ready(Ok(()))));
        assert!(matches!(
            crate::support::manual_async::poll_once(peer_run.as_mut()),
            Poll::Pending
        ));
        assert_eq!(peer.run_state(), AsyncSubscriptionRunState::Running);

        drop(subscription);
        {
            let observed_result = observed.lock().unwrap();
            assert!(
                matches!(
                    observed_result.as_ref(),
                    Some(Poll::Ready(Err(ShutdownError::Lifecycle(
                        LifecycleError::WouldDeadlock { operation: "shutdown" }
                    ))))
                ),
                "unexpected nested shutdown result: {observed_result:?}"
            );
        }
        drop(peer_run);
        drop(peer);
    });

    assert_eq!(spi.settlement_count(), 0, "drop must not settle the delivery");
    assert_eq!(
        spi.receiver_drop_recoveries(),
        1,
        "dropping the paused session must drop its receiver and recover its in-flight delivery"
    );
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn test_paused_subscription_payload_drop_panic_releases_registration() {
    struct DropProbe {
        panic_on_drop: bool,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            if self.panic_on_drop {
                panic!("queued payload drop probe");
            }
        }
    }

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            std::num::NonZeroUsize::new(1).expect("limit"),
            std::num::NonZeroUsize::new(3).expect("limit"),
            std::num::NonZeroUsize::new(3).expect("limit"),
            std::num::NonZeroUsize::new(1).expect("limit"),
        )
        .expect("scheduling"),
    );
    let bus =
        AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config).expect("valid provider");
    let payload_topic = Topic::<DropProbe>::new("drop-panic.topic").expect("valid topic");
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("drop-panic", payload_topic.clone()).unwrap()))
        .expect("subscription");

    for (event_id, panic_on_drop) in [
        ("ordinary-first", false),
        ("panic-second", true),
        ("ordinary-third", false),
    ] {
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("drop-panic.topic").unwrap(),
            EventId::new(event_id).unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(DropProbe { panic_on_drop })),
            None,
            Default::default(),
        ));
    }

    let mut runner = Box::pin(subscription.run(|_| std::future::pending::<Result<(), DeliveryError>>()));
    let mut metrics = subscription.delivery_metrics().metrics;
    for _ in 0..1024 {
        let _ = crate::support::manual_async::poll_once(runner.as_mut());
        metrics = subscription.delivery_metrics().metrics;
        if metrics.running_handlers == 1 && metrics.queued == 2 {
            break;
        }
    }
    assert_eq!(metrics.running_handlers, 1);
    assert_eq!(metrics.queued, 2);
    drop(runner);
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Paused);
    assert_eq!(subscription.delivery_metrics().metrics.queued, 2);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(subscription)))
        .expect_err("dropping the queued payload should unwind");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"queued payload drop probe"));
    assert_eq!(spi.settlement_count(), 0);

    let replacement =
        block_on(bus.subscribe(
            SubscribeRequest::new("drop-panic-replacement", payload_topic).expect("valid replacement request"),
        ))
        .expect("session destruction must return the sole scheduler subscription slot");
    drop(replacement);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("bus closes");
}

#[test]
fn test_immediate_close_counts_buffered_delivery_when_payload_drop_panics() {
    struct DropProbe(bool);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            if self.0 {
                panic!("buffered payload drop probe");
            }
        }
    }

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(2).unwrap(),
            std::num::NonZeroUsize::new(2).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
        )
        .unwrap(),
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config).unwrap();
    let payload_topic = Topic::<DropProbe>::new("immediate-close-drop.topic").unwrap();
    let mut subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("immediate-close-drop", payload_topic)
                .unwrap()
                .with_options(
                    SubscribeOptions::builder()
                        .durability(SubscriptionDurability::Ephemeral)
                        .build(),
                ),
        ),
    )
    .unwrap();
    for (event_id, panic_on_drop) in [("ordinary", false), ("panic", true)] {
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("immediate-close-drop.topic").unwrap(),
            EventId::new(event_id).unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(DropProbe(panic_on_drop))),
            None,
            Default::default(),
        ));
    }
    let mut runner = Box::pin(subscription.run(|_| std::future::pending::<Result<(), DeliveryError>>()));
    let mut metrics = subscription.delivery_metrics().metrics;
    for _ in 0..1024 {
        let _ = crate::support::manual_async::poll_once(runner.as_mut());
        metrics = subscription.delivery_metrics().metrics;
        if metrics.running_handlers == 1 && metrics.queued == 1 {
            break;
        }
    }
    assert_eq!(metrics.running_handlers, 1);
    assert_eq!(metrics.queued, 1);
    drop(runner);
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Paused);
    assert_eq!(subscription.delivery_metrics().metrics.abandoned_ephemeral, 0);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(subscription.close())))
        .expect_err("queued payload destructor must panic during immediate close");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"buffered payload drop probe"));
    assert_eq!(subscription.delivery_metrics().metrics.queued, 0);
    assert_eq!(subscription.delivery_metrics().metrics.abandoned_ephemeral, 1);
    drop(subscription);
    let report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");
    assert_eq!(report.known_abandoned_deliveries, 2);
}

#[test]
fn test_dropping_subscription_during_shutdown_takeover_releases_the_active_session() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let started = Arc::new(AtomicBool::new(false));
    let finish = Arc::new(AtomicBool::new(false));
    let handler_wakers = Arc::new(std::sync::Mutex::new(Vec::<Waker>::new()));
    let subscription =
        block_on(bus.subscribe(SubscribeRequest::new("drop-during-takeover", topic()).expect("valid subscriber ID")))
            .unwrap();
    spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "drop-takeover",
    ))));

    let started_by_handler = started.clone();
    let finish_by_handler = finish.clone();
    let wakers_by_handler = handler_wakers.clone();
    let mut run = Box::pin(subscription.run(move |_| {
        let started = started_by_handler.clone();
        let finish = finish_by_handler.clone();
        let wakers = wakers_by_handler.clone();
        async move {
            started.store(true, Ordering::Release);
            std::future::poll_fn(move |context| {
                if finish.load(Ordering::Acquire) {
                    Poll::Ready(Ok(()))
                } else {
                    wakers.lock().unwrap().push(context.waker().clone());
                    Poll::Pending
                }
            })
            .await
        }
    }));
    for _ in 0..100 {
        let _ = crate::support::manual_async::poll_once(run.as_mut());
        if started.load(Ordering::Acquire) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(started.load(Ordering::Acquire));
    drop(run);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(
        crate::support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Pending
    ));
    drop(subscription);
    finish.store(true, Ordering::Release);
    for waker in std::mem::take(&mut *handler_wakers.lock().unwrap()) {
        waker.wake();
    }
    let mut outcome = None;
    for _ in 0..100 {
        if let Poll::Ready(result) = crate::support::manual_async::poll_once(shutdown.as_mut()) {
            outcome = Some(result);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let _ = outcome
        .expect("shutdown should finish after the active handler exits")
        .unwrap();
    assert_eq!(spi.settlement_count(), 1);
    assert!(spi.operation_log().contains(&"close"));
}

#[test]
fn test_async_subscription_decodes_encoded_payload_with_the_topic_codec() {
    struct Utf8Codec(ContentType);

    impl EventCodec<String> for Utf8Codec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }

        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }

        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }

        fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
            let bytes = payload.bytes();
            String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
                source: Box::new(source),
            })
        }
    }

    let capabilities = EventBusCapabilities::new(
        PayloadModes::Encoded,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::PerSubscription,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(capabilities));
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let topic = Topic::new("async.encoded-subscription")
        .unwrap()
        .with_codec(Utf8Codec(ContentType::TEXT_PLAIN));
    let received = Arc::new(std::sync::Mutex::new(None));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("encoded-subscription", topic).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("async.encoded-subscription").unwrap(),
            EventId::new("encoded-subscription-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(b"decoded from transport".as_slice()),
                ContentType::TEXT_PLAIN,
                None,
            )),
            Some(SettlementToken::new(subscription.id(), "encoded-subscription-token")),
            Default::default(),
        ));
        let received_by_handler = received.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery| {
                *received_by_handler.lock().unwrap() = Some(delivery.payload().clone());
                async { Ok(()) }
            }))
        });
        for _ in 0..100 {
            if received.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(*received.lock().unwrap(), Some("decoded from transport".to_owned()));
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });

    assert_eq!(spi.settlement_count(), 1);
}

#[test]
fn test_async_decode_display_panic_still_rejects_bad_message() {
    struct PanickingDrop(Arc<AtomicUsize>);
    impl Drop for PanickingDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("injected decode panic payload drop panic");
        }
    }
    struct PanickingDisplay(Arc<AtomicUsize>);
    impl std::fmt::Display for PanickingDisplay {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::panic::panic_any(PanickingDrop(self.0.clone()));
        }
    }
    impl std::fmt::Debug for PanickingDisplay {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("PanickingDisplay")
        }
    }
    impl std::error::Error for PanickingDisplay {}
    struct PanickingCodec(ContentType, Arc<AtomicUsize>);
    impl EventCodec<String> for PanickingCodec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }
        fn decode(&self, _: &EncodedPayload) -> Result<String, CodecError> {
            Err(CodecError::Decode {
                source: Box::new(PanickingDisplay(self.1.clone())),
            })
        }
    }

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let drops = Arc::new(AtomicUsize::new(0));
    let diagnostics = Arc::new(Mutex::new(None));
    let observed = diagnostics.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { event_id, error, .. } = diagnostic {
            *observed.lock().expect("diagnostics") = Some((event_id.as_str().to_owned(), error.to_string()));
        }
    });
    let topic = Topic::<String>::new("async.decode-panic")
        .unwrap()
        .with_codec(PanickingCodec(ContentType::TEXT_PLAIN, drops.clone()));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("decode-panic", topic).expect("valid subscriber ID"))
            .await
            .expect("subscription");
        let id = subscription.id();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("async.decode-panic").unwrap(),
            EventId::new("decode-panic-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(b"bad payload".as_slice()),
                ContentType::TEXT_PLAIN,
                None,
            )),
            Some(SettlementToken::new(id, "decode-panic-token")),
            Default::default(),
        ));
        let handled = Arc::new(AtomicUsize::new(0));
        let called = handled.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                called.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }))
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while diagnostics.lock().expect("diagnostics").is_none() {
            assert!(Instant::now() < deadline, "decode failure was not diagnosed");
            std::thread::sleep(Duration::from_millis(1));
        }
        spi.close_receiver(id);
        runner.join().expect("runner thread").expect("runner completes");
        assert_eq!(handled.load(Ordering::SeqCst), 0);
    });
    assert_eq!(spi.settlement_dispositions(), [DeliveryDisposition::Reject]);
    assert_eq!(
        diagnostics.lock().expect("diagnostics").as_ref(),
        Some(&(
            "decode-panic-event".to_owned(),
            "decode error formatting panicked".to_owned()
        ))
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn test_async_codec_decode_panic_is_contained_and_stops_without_settlement() {
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok("async-codec-fail-stop") {
        crate::support::isolated_process::run_case(
            "test_async_codec_decode_panic_is_contained_and_stops_without_settlement",
            "async-codec-fail-stop",
        );
        return;
    }
    struct PanicOnceCodec {
        content_type: ContentType,
        panicked: Arc<AtomicBool>,
    }
    impl EventCodec<String> for PanicOnceCodec {
        fn content_type(&self) -> &ContentType {
            &self.content_type
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }
        fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
            let bytes = payload.bytes();
            if !self.panicked.swap(true, Ordering::AcqRel) {
                panic!("synthetic decode panic");
            }
            String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
                source: Box::new(source),
            })
        }
    }
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).unwrap();
    let topic = Topic::new("async.panic-codec").unwrap().with_codec(PanicOnceCodec {
        content_type: ContentType::TEXT_PLAIN,
        panicked: Arc::new(AtomicBool::new(false)),
    });
    let handled = Arc::new(AtomicUsize::new(0));
    let diagnostic_seen = Arc::new(AtomicBool::new(false));
    let observed = diagnostic_seen.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "receive_boundary") {
            observed.store(true, Ordering::Release);
        }
    });
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("panic-codec", topic.clone()).unwrap())).unwrap();
    let subscription_id = subscription.id();
    let enqueue = |id: &'static str, token: &'static str| {
        InboundMessage::new(
            TopicAddress::new("async.panic-codec").unwrap(),
            EventId::new(id).unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(b"ok".as_slice()),
                ContentType::TEXT_PLAIN,
                None,
            )),
            Some(SettlementToken::new(subscription_id, token)),
            Default::default(),
        )
    };
    spi.enqueue(enqueue("panic-codec-event", "panic-codec-token"));
    let handled_by_runner = handled.clone();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |_| {
            let handled = handled_by_runner.clone();
            async move {
                handled.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }))
    });
    let result = runner.join().expect("codec panic runner exits");
    let ReceiveError::Stopped(reason) = result.expect_err("codec panic stops receive") else {
        panic!("codec panic must retain a structured stop reason");
    };
    assert!(matches!(
        reason.as_ref(),
        SubscriptionStopReason::Codec { event_id, error }
            if event_id.as_str() == "panic-codec-event"
                && matches!(
                    error.as_ref(),
                    CodecError::Panicked {
                        operation: "decode",
                        ..
                    }
                )
    ));
    assert!(diagnostic_seen.load(Ordering::Acquire));
    assert_eq!(handled.load(Ordering::Acquire), 0);
    assert!(
        spi.settlement_dispositions().is_empty(),
        "codec panic must not accept, reject or retry the source token"
    );
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert!(spi.settlement_dispositions().is_empty());

    // Recovery uses a new subscription after the codec is repaired. This probe
    // panics only once, so its shared topic codec now successfully decodes.
    let recovered_spi = Arc::new(FakeAsyncEventBusSpi::new());
    let recovered_bus = AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), recovered_spi.clone()).unwrap();
    let recovered =
        block_on(recovered_bus.subscribe(SubscribeRequest::new("recovered-codec", topic).unwrap())).unwrap();
    for (id, bytes) in [
        ("recovered-event", b"ok".as_slice()),
        ("invalid-codec-event", &[0xff_u8][..]),
    ] {
        recovered_spi.enqueue(InboundMessage::new(
            TopicAddress::new("async.panic-codec").unwrap(),
            EventId::new(id).unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Encoded(EncodedPayload::new(Arc::from(bytes), ContentType::TEXT_PLAIN, None)),
            Some(SettlementToken::new(recovered.id(), id)),
            Default::default(),
        ));
    }
    let recovered_handler = handled.clone();
    let recovered_runner = std::thread::spawn(move || {
        block_on(recovered.run(move |_| {
            recovered_handler.fetch_add(1, Ordering::AcqRel);
            async { Ok(()) }
        }))
    });
    for _ in 0..100 {
        if recovered_spi.settlement_dispositions().len() == 2 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert_eq!(handled.load(Ordering::Acquire), 1);
    assert_eq!(
        recovered_spi.settlement_dispositions(),
        [DeliveryDisposition::Accept, DeliveryDisposition::Reject],
        "repaired codec accepts valid bytes and ordinary decode errors reject"
    );
    let _ = block_on(recovered_bus.shutdown(ShutdownMode::Immediate)).unwrap();
    recovered_runner.join().unwrap().unwrap();
}

#[test]
fn test_async_subscription_resolves_encoded_payload_codec_from_facade_registry() {
    struct Utf8Codec(ContentType);

    impl EventCodec<String> for Utf8Codec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }

        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }

        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }

        fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
            let bytes = payload.bytes();
            String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
                source: Box::new(source),
            })
        }
    }

    let capabilities = EventBusCapabilities::new(
        PayloadModes::Encoded,
        SettlementCapabilities::AcceptOnly,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let spi = Arc::new(OrderingTestSpi {
        inner: FakeAsyncEventBusSpi::with_capabilities(capabilities),
        capabilities,
        subscribe_calls: AtomicUsize::new(0),
    });
    let mut codecs = CodecRegistry::new();
    codecs
        .register::<String>(Arc::new(Utf8Codec(ContentType::TEXT_PLAIN)))
        .expect("codec registration succeeds");
    let config = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config)
        .expect("valid provider capabilities");
    let topic = Topic::<String>::new("async.registry-codec").unwrap();
    let received = Arc::new(std::sync::Mutex::new(None));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("registry-codec", topic).expect("subscriber is valid"))
            .await
            .expect("facade codec registry supplies the encoded subscription codec");
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("async.registry-codec").unwrap(),
            EventId::new("registry-codec-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(b"decoded through registry".as_slice()),
                ContentType::TEXT_PLAIN,
                None,
            )),
            Some(SettlementToken::new(subscription.id(), "registry-codec-token")),
            Default::default(),
        ));
        let received_by_handler = received.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery| {
                *received_by_handler.lock().unwrap() = Some(delivery.payload().clone());
                async { Ok(()) }
            }))
        });
        for _ in 0..100 {
            if received.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(*received.lock().unwrap(), Some("decoded through registry".to_owned()));
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_async_encoded_subscription_without_codec_fails_before_spi_subscribe() {
    let capabilities = EventBusCapabilities::new(
        PayloadModes::Encoded,
        SettlementCapabilities::AcceptOnly,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    );
    let spi = Arc::new(OrderingTestSpi {
        inner: FakeAsyncEventBusSpi::with_capabilities(capabilities),
        capabilities,
        subscribe_calls: AtomicUsize::new(0),
    });
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let request = SubscribeRequest::new(
        "missing-async-codec",
        Topic::<String>::new("async.missing-codec").unwrap(),
    )
    .unwrap();

    block_on(async {
        let error = match bus.subscribe(request).await {
            Ok(_) => panic!("encoded provider must require a codec"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            SubscribeError::Capability(CapabilityError::CodecRequired)
        ));
    });
    assert_eq!(0, spi.subscribe_calls.load(Ordering::Acquire));
}

#[test]
fn test_shutdown_takes_over_a_paused_async_session_and_finishes_its_owned_task() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let started = Arc::new(AtomicBool::new(false));
    let finish = Arc::new(AtomicBool::new(false));
    let handler_calls = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("paused-shutdown", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "paused-shutdown-token",
        ))));
        let started_by_handler = started.clone();
        let finish_by_handler = finish.clone();
        let calls_by_handler = handler_calls.clone();
        let mut run = Box::pin(subscription.run(move |_| {
            let started = started_by_handler.clone();
            let finish = finish_by_handler.clone();
            let calls = calls_by_handler.clone();
            async move {
                calls.fetch_add(1, Ordering::AcqRel);
                started.store(true, Ordering::Release);
                std::future::poll_fn(|context| {
                    if finish.load(Ordering::Acquire) {
                        std::task::Poll::Ready(Ok(()))
                    } else {
                        context.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                })
                .await
            }
        }));
        for _ in 0..100 {
            let _ = crate::support::manual_async::poll_once(run.as_mut());
            if started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(started.load(Ordering::Acquire));
        drop(run);
        finish.store(true, Ordering::Release);

        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(handler_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        spi.settlement_count(),
        1,
        "shutdown must settle the paused session's completed task"
    );
}

#[test]
fn test_async_success_settles_the_provider_token_after_handler_completion() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let request = SubscribeRequest::new("async-settle", topic()).expect("valid subscriber ID");

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "settlement-1",
        ))));
        let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
        for _ in 0..100 {
            if spi.settlement_count() == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(spi.settlement_count(), 1);
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_cancelling_run_during_settle_keeps_token_for_idempotent_retry() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("settle-cancel", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "cancelled-settle",
        ))));
        spi.pause_next_settle();
        {
            let first_calls = handler_calls.clone();
            let mut first_run = Box::pin(subscription.run(move |_| {
                first_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }));
            std::future::poll_fn(|cx| {
                let _ = first_run.as_mut().poll(cx);
                if spi.operation_log().iter().filter(|op| **op == "settle").count() > 0 {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }
        let next_calls = handler_calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                next_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }))
        });
        for _ in 0..100 {
            if spi.operation_log().iter().filter(|op| **op == "settle").count() >= 2 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(spi.operation_log().iter().filter(|op| **op == "settle").count(), 2);
        assert_eq!(spi.settlement_count(), 1);
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            1,
            "resuming settlement must not rerun a completed handler"
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn test_dropping_idle_wait_unregisters_signal_waker() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(std::sync::Mutex::new(None::<Waker>));
    let runner = {
        let bus = bus.clone();
        let started = started.clone();
        let release = release.clone();
        let handler_waker = handler_waker.clone();
        let spi = spi.clone();
        std::thread::spawn(move || {
            block_on(async move {
                let subscription = bus
                    .subscribe(SubscribeRequest::new("stale-waker", topic()).expect("valid subscriber ID"))
                    .await
                    .unwrap();
                spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
                    subscription.id(),
                    "stale-waker-token",
                ))));
                subscription
                    .run(move |_| {
                        let started = started.clone();
                        let release = release.clone();
                        let waker = handler_waker.clone();
                        async move {
                            started.store(true, Ordering::Release);
                            std::future::poll_fn(move |cx| {
                                if release.load(Ordering::Acquire) {
                                    return Poll::Ready(Ok(()));
                                }
                                *waker.lock().unwrap() = Some(cx.waker().clone());
                                Poll::Pending
                            })
                            .await
                        }
                    })
                    .await
            })
        })
    };
    for _ in 0..100 {
        if started.load(Ordering::Acquire) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert!(started.load(Ordering::Acquire));
    let (sender, receiver) = std::sync::mpsc::channel();
    let waker = Waker::from(Arc::new(WakeOnSignal(sender)));
    let mut context = Context::from_waker(&waker);
    let waiting_topic = topic();
    let mut waiting = Box::pin(bus.wait_for_received_deliveries(&waiting_topic, None));
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    drop(waiting);

    release.store(true, Ordering::Release);
    if let Some(waker) = handler_waker.lock().unwrap().take() {
        waker.wake();
    }
    block_on(async {
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
    runner.join().unwrap().unwrap();
    assert!(
        receiver.try_recv().is_err(),
        "dropped waiters must not leave stale wakers registered"
    );
}

#[test]
fn test_async_settlement_failure_retries_the_same_token_without_rerunning_handler() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new();
    let timer = Arc::new(ManualTimer::from_clock(&clock));
    let config = EventBusFacadeConfig::new().with_settlement_retry(
        SettlementRetryConfig::new(
            NonZeroU32::new(3).expect("positive attempts"),
            Duration::from_secs(1),
            Duration::from_millis(10),
            Duration::from_millis(10),
        )
        .expect("valid retry policy"),
    );
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("fake").expect("provider ID"),
        spi.clone(),
        config,
        timer,
    )
    .expect("valid provider capabilities");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let request = SubscribeRequest::new("async-settle-failure", topic()).expect("valid subscriber ID");
    let subscription = block_on(bus.subscribe(request)).expect("subscription");
    spi.fail_next_settle();
    spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "settlement-fail",
    ))));
    let calls = handler_calls.clone();
    let mut runner = Box::pin(subscription.run(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    }));
    let mut context = Context::from_waker(Waker::noop());
    let mut first_settle_registered = false;
    for _ in 0..8 {
        assert!(
            runner.as_mut().poll(&mut context).is_pending(),
            "runner remains active while it reaches the first settlement"
        );
        if spi.operation_log().iter().filter(|op| **op == "settle").count() == 1 {
            first_settle_registered = true;
            break;
        }
    }
    assert!(
        first_settle_registered,
        "first failed settlement registers the retry delay"
    );
    assert_eq!(0, spi.settlement_count(), "a failed attempt is not recorded as settled");
    assert_eq!(1, spi.operation_log().iter().filter(|op| **op == "settle").count());
    assert_eq!(1, handler_calls.load(Ordering::SeqCst), "handler has run once");
    assert_eq!(
        WaitOutcome::TimedOut,
        block_on(bus.wait_for_received_deliveries(&topic(), Some(Duration::ZERO))).expect("zero-time wait"),
        "received guard remains held during settlement backoff",
    );

    clock.advance(Duration::from_millis(10)).expect("advance retry clock");
    let mut retry_completed = false;
    for _ in 0..8 {
        assert!(
            runner.as_mut().poll(&mut context).is_pending(),
            "runner waits for another message"
        );
        if spi.operation_log().iter().filter(|op| **op == "settle").count() == 2 {
            retry_completed = true;
            break;
        }
    }
    assert!(
        retry_completed,
        "one manual backoff advance permits the retry to finish"
    );
    assert_eq!(1, spi.settlement_count(), "successful settlement is recorded once");
    assert_eq!(
        1,
        handler_calls.load(Ordering::SeqCst),
        "settlement retry does not rerun handler"
    );
    assert_eq!(2, spi.operation_log().iter().filter(|op| **op == "settle").count());
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    let mut shutdown_report = None;
    let mut runner_finished = false;
    for _ in 0..16 {
        if let Poll::Ready(result) = shutdown.as_mut().poll(&mut context) {
            shutdown_report = Some(result.expect("bus shutdown"));
            break;
        }
        if !runner_finished {
            runner_finished = matches!(runner.as_mut().poll(&mut context), Poll::Ready(Ok(())));
        }
    }
    assert_eq!(
        Some(ShutdownOutcome::Complete),
        shutdown_report.map(|report| report.outcome),
        "bus shutdown completes while the runner is manually polled"
    );
    for _ in 0..8 {
        if runner_finished {
            break;
        }
        runner_finished = matches!(runner.as_mut().poll(&mut context), Poll::Ready(Ok(())));
    }
    assert!(runner_finished, "closed receiver ends runner successfully");
    assert_eq!(
        WaitOutcome::Idle,
        block_on(bus.wait_for_received_deliveries(&topic(), Some(Duration::ZERO))).expect("zero-time wait"),
        "successful settlement releases the received-delivery guard",
    );
}

#[test]
fn test_cancelling_pending_provider_shutdown_can_be_retried_after_partial_progress() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    spi.pause_shutdown();
    let mut first_shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));

    assert!(matches!(
        crate::support::manual_async::poll_once(first_shutdown.as_mut()),
        Poll::Pending
    ));
    assert_eq!(1, spi.shutdown_transition_count());
    drop(first_shutdown);

    spi.release_shutdown();
    assert_eq!(
        ShutdownOutcome::Complete,
        block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap().outcome
    );
    assert_eq!(
        1,
        spi.shutdown_transition_count(),
        "the provider resumes its original shutdown"
    );
    assert_eq!(
        2,
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "shutdown")
            .count()
    );
}

#[test]
fn test_async_shutdown_stops_permanent_settlement_retry_after_receiver_close() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("permanent-settlement-failure", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.fail_all_settles();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "permanent-failure-token",
        ))));
        let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
        for _ in 0..100 {
            if spi.operation_log().contains(&"settle") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(spi.operation_log().contains(&"settle"));
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        assert!(matches!(
            runner.join().expect("runner"),
            Err(ReceiveError::Stopped(reason))
                if matches!(
                    reason.as_ref(),
                    SubscriptionStopReason::Settlement {
                        termination: SettlementTermination::PermanentError,
                        ..
                    }
                )
        ));
        assert_eq!(spi.settlement_count(), 0, "provider never accepted this token");
        assert!(
            spi.operation_log().contains(&"close"),
            "receiver close owns final cleanup"
        );
    });
}

#[test]
fn test_async_settlement_call_panic_is_reported_once_and_does_not_retry() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("async-settle-panic", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.panic_on_settle_call();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "settle-panic-token",
        ))));
        let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
        for _ in 0..100 {
            if spi.operation_log().contains(&"settle") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(spi.operation_log().contains(&"settle"));
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        assert!(matches!(
            runner.join().expect("runner"),
            Err(ReceiveError::Stopped(reason))
                if matches!(
                    reason.as_ref(),
                    SubscriptionStopReason::Settlement {
                        termination: SettlementTermination::ProviderPanicked,
                        ..
                    }
                )
        ));
        assert_eq!(
            1,
            spi.operation_log()
                .iter()
                .filter(|operation| **operation == "settle")
                .count(),
            "a settlement panic must not create a retry loop"
        );
    });
}

#[test]
fn test_permanent_settlement_failure_yields_to_same_executor_shutdown() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").expect("provider"), spi.clone()).expect("bus");
    let subscription =
        block_on(bus.subscribe(SubscribeRequest::new("same-executor", topic()).expect("request"))).expect("subscribe");
    spi.fail_all_settles();
    spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "token",
    ))));
    let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
    let mut result = Poll::Pending;
    for _ in 0..16 {
        result = crate::support::manual_async::poll_once(runner.as_mut());
        if result.is_ready() {
            break;
        }
    }
    assert!(matches!(result, Poll::Ready(Err(ReceiveError::Stopped(_)))));
    drop(runner);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
    assert_eq!(
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "settle")
            .count(),
        1
    );
}

#[test]
fn test_shutdown_report_counts_ephemeral_delivery_after_handler_before_settlement() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").expect("provider"), spi.clone()).expect("bus");
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("report-unsettled", topic()).expect("request")))
        .expect("subscribe");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    spi.fail_all_settles();
    spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "report-unsettled-token",
    ))));
    let calls = handler_calls.clone();
    let mut runner = Box::pin(subscription.run(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    }));
    let mut result = Poll::Pending;
    for _ in 0..16 {
        result = crate::support::manual_async::poll_once(runner.as_mut());
        if result.is_ready() {
            break;
        }
    }
    assert!(matches!(result, Poll::Ready(Err(ReceiveError::Stopped(_)))));
    assert_eq!(1, handler_calls.load(Ordering::SeqCst));
    drop(runner);

    let report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
    assert_eq!(1, report.known_abandoned_deliveries);
    assert_eq!(
        1,
        handler_calls.load(Ordering::SeqCst),
        "cleanup must not rerun the handler"
    );
}

#[test]
fn test_async_decode_settlement_failure_diagnostic_keeps_inbound_identity_without_event() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let observed = Arc::new(std::sync::Mutex::new(None));
    let observed_by_callback = observed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::SettlementFailed { event_id, topic, .. } = diagnostic {
            *observed_by_callback.lock().unwrap() = Some((event_id.as_str().to_owned(), topic.to_string()));
        }
    });
    struct InvalidUtf8Codec(ContentType);
    impl EventCodec<String> for InvalidUtf8Codec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }
        fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
            String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
                source: Box::new(source),
            })
        }
    }
    let string_topic = Topic::<String>::new("test.topic")
        .unwrap()
        .with_codec(InvalidUtf8Codec(ContentType::TEXT_PLAIN));
    let request = SubscribeRequest::new("decode-settle-failure", string_topic).expect("valid subscriber ID");

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        spi.fail_next_settle();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").unwrap(),
            EventId::new("event-test").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Encoded(EncodedPayload::new(Arc::from([0xff_u8]), ContentType::TEXT_PLAIN, None)),
            Some(SettlementToken::new(subscription.id(), "decode-fail")),
            Default::default(),
        ));
        let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
        for _ in 0..100 {
            if observed.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
        assert_eq!(
            *observed.lock().unwrap(),
            Some(("event-test".into(), "test.topic".into()))
        );
    });
}

#[test]
fn test_async_spi_receive_poll_panic_is_converted_to_a_structured_error_and_closed() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let request = SubscribeRequest::new("async-spi-panic", topic()).expect("valid subscriber ID");

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        spi.panic_next_receive();
        let error = subscription.run(|_| async { Ok(()) }).await.unwrap_err();
        let ReceiveError::Stopped(reason) = error else {
            panic!("provider receive panic must cache a structured stop reason");
        };
        let SubscriptionStopReason::Provider { error } = reason.as_ref() else {
            panic!("provider panic must remain a provider cause");
        };
        assert_eq!(error.kind(), "provider_panicked");
        assert_eq!(error.operation(), "receive");
        assert!(
            std::error::Error::source(error.as_ref()).is_some(),
            "provider panic source chain is retained"
        );
        assert!(Arc::ptr_eq(
            &reason,
            &subscription.terminal_failure().expect("cached provider failure")
        ));
        let ReceiveError::Stopped(repeated) = subscription
            .run(|_| async { Ok(()) })
            .await
            .expect_err("stopped subscription cannot receive again")
        else {
            panic!("repeated run must return cached stop reason");
        };
        assert!(Arc::ptr_eq(&reason, &repeated));
        assert_eq!(
            spi.operation_log()
                .iter()
                .filter(|operation| **operation == "receive")
                .count(),
            1,
            "repeated run does not poll the provider again"
        );
        assert!(spi.operation_log().contains(&"close"));
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn test_async_spi_operation_call_panics_are_returned_as_structured_errors() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    spi.panic_on_subscribe_call();
    let subscribe_error = match block_on(
        bus.subscribe(SubscribeRequest::new("async-subscribe-panic", topic()).expect("valid subscriber ID")),
    ) {
        Ok(_) => panic!("subscribe panic must be returned as an error"),
        Err(error) => error,
    };
    assert!(matches!(subscribe_error, SubscribeError::Spi(error) if error.kind() == "provider_panicked"));

    spi.panic_on_shutdown_call();
    let shutdown_error = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap_err();
    assert!(matches!(shutdown_error, ShutdownError::Spi(error) if error.kind() == "provider_panicked"));
    assert_eq!(
        None,
        bus.shutdown_report(),
        "failed provider shutdown must not publish a report"
    );
}

#[test]
fn test_async_receiver_close_call_panic_is_reported_during_shutdown() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");

    block_on(async {
        let _subscription = bus
            .subscribe(SubscribeRequest::new("async-close-panic", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        let _other_subscription = bus
            .subscribe(SubscribeRequest::new("async-close-after-panic", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.panic_on_close_call();
        let error = bus.shutdown(ShutdownMode::Immediate).await.unwrap_err();
        assert!(error.to_string().contains("provider_panicked"));
        assert_eq!(
            spi.close_call_count(),
            2,
            "a close failure must not block other receiver cleanup"
        );
    });
}

#[test]
fn test_concurrent_async_shutdown_calls_close_the_provider_once() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let first = bus.clone();
    let second = bus.clone();

    let first = std::thread::spawn(move || block_on(first.shutdown(ShutdownMode::Immediate)));
    let second = std::thread::spawn(move || block_on(second.shutdown(ShutdownMode::Immediate)));

    assert_eq!(first.join().unwrap().unwrap().outcome, ShutdownOutcome::Complete);
    assert_eq!(second.join().unwrap().unwrap().outcome, ShutdownOutcome::Complete);
    assert_eq!(
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "shutdown")
            .count(),
        1
    );
}

#[test]
fn test_immediate_shutdown_waits_for_runner_settlement_and_close_before_provider_shutdown() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let handlers = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("shutdown-order", topic()).expect("valid subscriber ID"))
            .await
            .unwrap();
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "shutdown-order",
        ))));
        spi.enqueue(crate::support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "shutdown-queued",
        ))));
        let handler_started = started.clone();
        let handler_release = release.clone();
        let handler_count = handlers.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let handler_started = handler_started.clone();
                let handler_release = handler_release.clone();
                let handler_count = handler_count.clone();
                async move {
                    handler_count.fetch_add(1, Ordering::AcqRel);
                    handler_started.store(true, Ordering::Release);
                    while !handler_release.load(Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                    Ok(())
                }
            }))
        });
        for _ in 0..100 {
            if started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(started.load(Ordering::Acquire));
        let shutdown_bus = bus.clone();
        let shutdown_polled = Arc::new(AtomicBool::new(false));
        let shutdown_polled_by_thread = shutdown_polled.clone();
        let shutdown = std::thread::spawn(move || {
            let mut shutdown = Box::pin(shutdown_bus.shutdown(ShutdownMode::Immediate));
            block_on(std::future::poll_fn(|context| match shutdown.as_mut().poll(context) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    shutdown_polled_by_thread.store(true, Ordering::Release);
                    Poll::Pending
                }
            }))
        });
        for _ in 0..500 {
            if shutdown_polled.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(shutdown_polled.load(Ordering::Acquire));
        let shutdown_started_early = spi.operation_log().contains(&"shutdown");
        release.store(true, Ordering::Release);
        runner.join().unwrap().unwrap();
        let _ = shutdown.join().unwrap().unwrap();
        assert!(
            !shutdown_started_early,
            "provider shutdown must wait for the active handler"
        );
        let operations = spi.operation_log();
        let settle = operations.iter().position(|operation| *operation == "settle").unwrap();
        let close = operations.iter().position(|operation| *operation == "close").unwrap();
        let shutdown = operations
            .iter()
            .position(|operation| *operation == "shutdown")
            .unwrap();
        assert!(
            settle < close && close < shutdown,
            "expected settle < close < shutdown, got {operations:?}"
        );
        assert_eq!(
            handlers.load(Ordering::Acquire),
            1,
            "queued delivery must not start after shutdown request"
        );
        assert_eq!(spi.settlement_count(), 1);
    });
}
