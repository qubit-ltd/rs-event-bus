// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for asynchronous shutdown mode escalation.
mod support;

use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::io::Error;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_clock::ClockDomain;
use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
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
use qubit_event_bus::spi::ShutdownOutcome as SpiShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;

use crate::support::fake_spi::FakeAsyncEventBusSpi;

type ShutdownReentryObservation = Arc<Mutex<Option<Poll<Result<(), ShutdownError>>>>>;
type SubscriptionRunObservation = Arc<Mutex<Option<Poll<Result<(), ReceiveError>>>>>;

struct ReenterOnPollTimer {
    clock: Arc<ManualMonotonicClock>,
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    observed: ShutdownReentryObservation,
}

struct ReenterOnDropTimer {
    clock: Arc<ManualMonotonicClock>,
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    observed: ShutdownReentryObservation,
}

struct ReenterOnDropTimerFuture {
    inner: TimerFuture,
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    observed: ShutdownReentryObservation,
}

struct ReenterOnSampleClock {
    clock: ManualMonotonicClock,
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    observed: ShutdownReentryObservation,
    samples: AtomicUsize,
    elapsed_before_provider_sample: Duration,
}

impl MonotonicClock for ReenterOnSampleClock {
    fn domain(&self) -> ClockDomain {
        self.clock.domain()
    }

    fn now(&self) -> MonotonicInstant {
        if self.samples.fetch_add(1, Ordering::SeqCst) == 1 {
            self.clock
                .advance(self.elapsed_before_provider_sample)
                .expect("advance clock during leader sampling");
            let bus = self.bus.lock().expect("bus lock").clone().expect("bus installed");
            let mut reentrant = Box::pin(bus.shutdown(ShutdownMode::Immediate));
            let result = reentrant
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .map(|result| result.map(|_| ()));
            *self.observed.lock().expect("observation lock") = Some(result);
        }
        self.clock.now()
    }

    fn new_timer(&self) -> Arc<dyn Timer> {
        Arc::new(self.clock.new_timer())
    }
}

struct ReenterOnSampleTimer(Arc<ReenterOnSampleClock>);

impl Timer for ReenterOnSampleTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        self.0.as_ref()
    }

    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        self.0.clock.new_timer().at(deadline)
    }
}

impl Timer for ReenterOnPollTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        let mut inner = self.clock.new_timer().at(deadline)?;
        let bus = self.bus.clone();
        let observed = self.observed.clone();
        Ok(Box::pin(std::future::poll_fn(move |cx| {
            if observed.lock().expect("observation lock").is_none() {
                let bus = bus.lock().expect("bus lock").clone().expect("bus installed");
                let mut reentrant = Box::pin(bus.shutdown(ShutdownMode::Immediate));
                let result = reentrant.as_mut().poll(cx).map(|result| result.map(|_| ()));
                *observed.lock().expect("observation lock") = Some(result);
            }
            inner.as_mut().poll(cx)
        })))
    }
}

impl Timer for ReenterOnDropTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        self.clock.as_ref()
    }

    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        Ok(Box::pin(ReenterOnDropTimerFuture {
            inner: self.clock.new_timer().at(deadline)?,
            bus: self.bus.clone(),
            observed: self.observed.clone(),
        }))
    }
}

impl Future for ReenterOnDropTimerFuture {
    type Output = Result<(), TimeError>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

impl Drop for ReenterOnDropTimerFuture {
    fn drop(&mut self) {
        let Some(bus) = self.bus.lock().expect("bus lock").clone() else {
            return;
        };
        let mut reentrant = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        let result = reentrant
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .map(|result| result.map(|_| ()));
        *self.observed.lock().expect("observation lock") = Some(result);
    }
}

#[test]
fn test_graceful_deadline_poll_rejects_same_bus_shutdown() {
    let clock = ManualMonotonicClock::new_shared();
    let provider = Arc::new(FakeAsyncEventBusSpi::new());
    provider.pause_close();
    let bus_slot = Arc::new(Mutex::new(None));
    let observed = Arc::new(Mutex::new(None));
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("timer-reenter").expect("provider ID"),
        provider.clone(),
        Arc::new(ReenterOnPollTimer {
            clock: clock.clone(),
            bus: bus_slot.clone(),
            observed: observed.clone(),
        }),
    )
    .expect("facade");
    *bus_slot.lock().expect("bus lock") = Some(bus.clone());
    let topic = Topic::<u32>::new("timer-reenter").expect("topic");
    let _subscription =
        block_on_shutdown(bus.subscribe(SubscribeRequest::new("timer-reenter", topic).expect("request")))
            .expect("subscription");

    let timeout = Duration::from_secs(1);
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful { timeout }));
    assert!(
        graceful
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        matches!(
            observed.lock().expect("observation lock").as_ref(),
            Some(Poll::Ready(Err(ShutdownError::Lifecycle(
                LifecycleError::WouldDeadlock { operation: "shutdown" }
            ))))
        ),
        "observed reentry result: {:?}",
        observed.lock().expect("observation lock")
    );
    clock.advance(timeout).expect("advance graceful deadline");
    assert!(matches!(
        graceful.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(ShutdownError::TimedOut { timeout: elapsed })) if elapsed == timeout
    ));
    drop(graceful);
    *bus_slot.lock().expect("bus lock") = None;
    provider.release_close();
    let _ = block_on_shutdown(bus.shutdown(ShutdownMode::Immediate));
}

#[test]
fn test_graceful_provider_clock_sample_rejects_same_bus_shutdown() {
    let bus_slot = Arc::new(Mutex::new(None));
    let observed = Arc::new(Mutex::new(None));
    let clock = Arc::new(ReenterOnSampleClock {
        clock: ManualMonotonicClock::new(),
        bus: bus_slot.clone(),
        observed: observed.clone(),
        samples: AtomicUsize::new(0),
        elapsed_before_provider_sample: Duration::from_nanos(7),
    });
    let provider = Arc::new(ShutdownGateSpi::new(false));
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("clock-reenter").expect("provider ID"),
        provider.clone(),
        Arc::new(ReenterOnSampleTimer(clock)),
    )
    .expect("facade");
    *bus_slot.lock().expect("bus lock") = Some(bus.clone());

    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(1),
    }));
    let _ = graceful.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert!(
        matches!(
            observed.lock().expect("observation lock").as_ref(),
            Some(Poll::Ready(Err(ShutdownError::Lifecycle(
                LifecycleError::WouldDeadlock { operation: "shutdown" }
            ))))
        ),
        "observed reentry result: {:?}",
        observed.lock().expect("observation lock")
    );
    assert_eq!(
        vec![ShutdownMode::Graceful {
            timeout: Duration::from_secs(1) - Duration::from_nanos(7),
        }],
        *provider.calls.lock().expect("provider calls lock"),
        "provider receives the original budget minus synchronous clock progress",
    );
    drop(graceful);
    *bus_slot.lock().expect("bus lock") = None;
    let _ = block_on_shutdown(bus.shutdown(ShutdownMode::Immediate));
}

#[test]
fn test_graceful_deadline_drop_rejects_same_bus_shutdown() {
    let provider = Arc::new(FakeAsyncEventBusSpi::new());
    provider.pause_close();
    let bus_slot = Arc::new(Mutex::new(None));
    let observed = Arc::new(Mutex::new(None));
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("timer-drop-reenter").expect("provider ID"),
        provider.clone(),
        Arc::new(ReenterOnDropTimer {
            clock: ManualMonotonicClock::new_shared(),
            bus: bus_slot.clone(),
            observed: observed.clone(),
        }),
    )
    .expect("facade");
    *bus_slot.lock().expect("bus lock") = Some(bus.clone());
    let topic = Topic::<u32>::new("timer-drop-reenter").expect("topic");
    let _subscription =
        block_on_shutdown(bus.subscribe(SubscribeRequest::new("timer-drop-reenter", topic).expect("request")))
            .expect("subscription");

    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(1),
    }));
    assert!(
        graceful
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(graceful);
    assert!(
        matches!(
            observed.lock().expect("observation lock").as_ref(),
            Some(Poll::Ready(Err(ShutdownError::Lifecycle(
                LifecycleError::WouldDeadlock { operation: "shutdown" }
            ))))
        ),
        "timer future drop must reject reentrant shutdown: {:?}",
        observed.lock().expect("observation lock")
    );

    *bus_slot.lock().expect("bus lock") = None;
    provider.release_close();
    assert!(block_on_shutdown(bus.shutdown(ShutdownMode::Immediate)).is_ok());
}

struct WakeCounter(AtomicUsize);

struct ReenterOnWake {
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    observed: ShutdownReentryObservation,
    subscription: Arc<Mutex<Option<Weak<qubit_event_bus::AsyncSubscription<u32>>>>>,
    run_observed: SubscriptionRunObservation,
    handler_calls: Arc<AtomicUsize>,
    used: std::sync::atomic::AtomicBool,
}

impl Wake for ReenterOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.used.swap(true, Ordering::SeqCst) {
            return;
        }
        let Some(bus) = self.bus.lock().expect("bus lock").clone() else {
            return;
        };
        let mut nested = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        let result = nested
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .map(|result| result.map(|_| ()));
        *self.observed.lock().expect("observation lock") = Some(result);
        let subscription = self
            .subscription
            .lock()
            .expect("subscription slot")
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(subscription) = subscription {
            let handler_calls = self.handler_calls.clone();
            let mut nested = Box::pin(subscription.run(move |_| {
                handler_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<(), qubit_event_bus::DeliveryError>(()) }
            }));
            let result = nested.as_mut().poll(&mut Context::from_waker(Waker::noop()));
            *self.run_observed.lock().expect("run observation") = Some(result);
        }
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ShutdownGateSpi {
    calls: Mutex<Vec<ShutdownMode>>,
    capabilities: EventBusCapabilities,
    fail_immediate: bool,
}

impl ShutdownGateSpi {
    fn new(fail_immediate: bool) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail_immediate,
            capabilities: EventBusCapabilities::new(
                PayloadModes::Native,
                SettlementCapabilities::AcceptRetryReject,
                OrderingCapability::None,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                SubscriptionModes::EPHEMERAL,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            ),
        }
    }
}

impl AsyncEventBusSpi for ShutdownGateSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async { Ok(PublishAcknowledgement::DroppedByInterceptor) })
    }

    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async {
            Err(SpiError::Operation {
                provider_id: "shutdown-gate".into(),
                operation: "subscribe",
                resource: None,
                kind: "unsupported_test_operation",
                retryable: Some(false),
                source: Box::new(Error::other("unused")),
            })
        })
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<SpiShutdownOutcome, SpiError>> {
        self.calls.lock().expect("calls lock").push(mode);
        let fail_immediate = self.fail_immediate;
        Box::pin(async move {
            if matches!(mode, ShutdownMode::Graceful { .. }) {
                pending::<()>().await;
            }
            if fail_immediate && mode == ShutdownMode::Immediate {
                return Err(SpiError::Operation {
                    provider_id: "shutdown-gate".into(),
                    operation: "shutdown",
                    resource: None,
                    kind: "immediate_shutdown_failed",
                    retryable: Some(false),
                    source: Box::new(Error::other("injected immediate shutdown failure")),
                });
            }
            Ok(SpiShutdownOutcome::Complete)
        })
    }
}

#[test]
fn test_immediate_shutdown_escalates_active_graceful_provider_call() {
    let provider = Arc::new(ShutdownGateSpi::new(false));
    let clock = ManualMonotonicClock::new();
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("shutdown-gate").expect("provider ID"),
        provider.clone(),
        clock.new_timer(),
    )
    .expect("facade");
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(
        graceful.as_mut().poll(&mut Context::from_waker(&waker)).is_pending(),
        "graceful provider shutdown should remain pending",
    );
    assert_eq!(
        vec![ShutdownMode::Graceful {
            timeout: Duration::from_secs(30)
        }],
        *provider.calls.lock().expect("calls lock"),
        "the provider should receive the initial graceful shutdown request",
    );

    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(
        immediate
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "immediate shutdown should wait while escalating the active provider call",
    );
    assert!(
        counter.0.load(Ordering::SeqCst) > 0,
        "escalating shutdown should wake the graceful shutdown task",
    );
    let Poll::Ready(Ok(graceful_report)) = graceful.as_mut().poll(&mut Context::from_waker(&waker)) else {
        panic!("escalated graceful shutdown must return a successful report");
    };
    assert_eq!(graceful_report.outcome, SpiShutdownOutcome::Complete);
    assert_eq!(graceful_report.known_abandoned_deliveries, 0);
    assert_eq!(
        vec![
            ShutdownMode::Graceful {
                timeout: Duration::from_secs(30)
            },
            ShutdownMode::Immediate,
        ],
        *provider.calls.lock().expect("calls lock"),
        "the provider should receive graceful and immediate shutdown requests in order",
    );
    let Poll::Ready(Ok(immediate_report)) = immediate.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("immediate shutdown must return a successful report");
    };
    assert_eq!(immediate_report.outcome, SpiShutdownOutcome::Complete);
    assert_eq!(immediate_report.known_abandoned_deliveries, 0);
}

#[test]
fn test_immediate_escalation_wake_rejects_same_bus_shutdown() {
    let provider = Arc::new(ShutdownGateSpi::new(false));
    let bus_slot = Arc::new(Mutex::new(None));
    let observed = Arc::new(Mutex::new(None));
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("wake-reenter").expect("provider ID"),
        provider.clone(),
        ManualMonotonicClock::new().new_timer(),
    )
    .expect("facade");
    *bus_slot.lock().expect("bus lock") = Some(bus.clone());
    let waker = Waker::from(Arc::new(ReenterOnWake {
        bus: bus_slot.clone(),
        observed: observed.clone(),
        subscription: Arc::new(Mutex::new(None)),
        run_observed: Arc::new(Mutex::new(None)),
        handler_calls: Arc::new(AtomicUsize::new(0)),
        used: std::sync::atomic::AtomicBool::new(false),
    }));

    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(graceful.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    assert_eq!(
        vec![ShutdownMode::Graceful {
            timeout: Duration::from_secs(30)
        }],
        *provider.calls.lock().unwrap()
    );

    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(
        immediate
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        matches!(
            observed.lock().unwrap().as_ref(),
            Some(Poll::Ready(Err(ShutdownError::Lifecycle(
                LifecycleError::WouldDeadlock { operation: "shutdown" }
            ))))
        ),
        "nested shutdown observation: {:?}",
        observed.lock().unwrap()
    );
    let graceful_result = graceful.as_mut().poll(&mut Context::from_waker(&waker));
    assert_eq!(
        vec![
            ShutdownMode::Graceful {
                timeout: Duration::from_secs(30)
            },
            ShutdownMode::Immediate
        ],
        *provider.calls.lock().unwrap(),
    );

    let Poll::Ready(Ok(graceful_report)) = graceful_result else {
        panic!("graceful shutdown must complete after immediate escalation");
    };
    let Poll::Ready(Ok(immediate_report)) = immediate.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("immediate shutdown must share the completed result");
    };
    assert_eq!(graceful_report, immediate_report);
    *bus_slot.lock().unwrap() = None;
}

#[test]
fn test_subscription_stop_wake_rejects_same_bus_shutdown() {
    let provider = Arc::new(FakeAsyncEventBusSpi::new());
    let bus_slot = Arc::new(Mutex::new(None));
    let observed = Arc::new(Mutex::new(None));
    let subscription_slot = Arc::new(Mutex::new(None));
    let run_observed = Arc::new(Mutex::new(None));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("stop-wake-reenter").expect("provider ID"),
        provider,
        qubit_clock::StdMonotonicClock::new().new_timer(),
    )
    .expect("facade");
    let topic = Topic::<u32>::new("stop-wake-reenter").expect("topic");
    let subscription = Arc::new(
        block_on_shutdown(bus.subscribe(SubscribeRequest::new("stop-wake-reenter", topic).expect("request")))
            .expect("subscription"),
    );
    let reenter = Arc::new(ReenterOnWake {
        bus: bus_slot.clone(),
        observed: observed.clone(),
        subscription: subscription_slot.clone(),
        run_observed: run_observed.clone(),
        handler_calls: handler_calls.clone(),
        used: std::sync::atomic::AtomicBool::new(false),
    });
    let waker = Waker::from(reenter.clone());
    let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(runner.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    // Ignore initialization wakes, then let shutdown's stop signal invoke this
    // waker.
    reenter.used.store(false, Ordering::SeqCst);
    *bus_slot.lock().expect("bus slot") = Some(bus.clone());
    *subscription_slot.lock().expect("subscription slot") = Some(Arc::downgrade(&subscription));

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        matches!(
            observed.lock().expect("observation lock").as_ref(),
            Some(Poll::Ready(Err(ShutdownError::Lifecycle(
                LifecycleError::WouldDeadlock { operation: "shutdown" }
            ))))
        ),
        "nested shutdown observation: {:?}",
        observed.lock().expect("observation lock")
    );
    assert!(
        matches!(
            run_observed.lock().expect("run observation").as_ref(),
            Some(Poll::Ready(Err(ReceiveError::WouldDeadlock)))
        ),
        "nested run must be rejected during stop wake: {:?}",
        run_observed.lock().expect("run observation")
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0, "nested handler must not run");

    assert!(runner.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
    let Poll::Ready(Ok(report)) = shutdown.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("outer shutdown must complete after the runner releases its lease");
    };
    assert_eq!(report.outcome, SpiShutdownOutcome::Complete);
    *bus_slot.lock().expect("bus slot") = None;
}

struct PublishGateSpi {
    inner: FakeAsyncEventBusSpi,
    released: Arc<std::sync::atomic::AtomicBool>,
    publish_waker: Arc<Mutex<Option<Waker>>>,
    shutdown_calls: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for PublishGateSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        let released = self.released.clone();
        let publish_waker = self.publish_waker.clone();
        Box::pin(poll_fn(move |cx| {
            if released.load(Ordering::SeqCst) {
                Poll::Ready(Ok(PublishAcknowledgement::DroppedByInterceptor))
            } else {
                *publish_waker.lock().expect("publish waker") = Some(cx.waker().clone());
                Poll::Pending
            }
        }))
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.inner.subscribe(request)
    }

    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<SpiShutdownOutcome, SpiError>> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(SpiShutdownOutcome::Complete) })
    }
}

#[test]
fn test_publish_completion_wake_rejects_same_bus_shutdown() {
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let publish_waker = Arc::new(Mutex::new(None));
    let shutdown_calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(PublishGateSpi {
        inner: FakeAsyncEventBusSpi::new(),
        released: released.clone(),
        publish_waker: publish_waker.clone(),
        shutdown_calls: shutdown_calls.clone(),
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("publish-finish-wake").expect("provider ID"), provider)
        .expect("facade");
    let bus_slot = Arc::new(Mutex::new(Some(bus.clone())));
    let observed = Arc::new(Mutex::new(None));
    let reenter = Arc::new(ReenterOnWake {
        bus: bus_slot.clone(),
        observed: observed.clone(),
        subscription: Arc::new(Mutex::new(None)),
        run_observed: Arc::new(Mutex::new(None)),
        handler_calls: Arc::new(AtomicUsize::new(0)),
        used: std::sync::atomic::AtomicBool::new(false),
    });
    let waker = Waker::from(reenter.clone());
    let topic = Topic::<u32>::new("publish-finish-wake").expect("topic");
    let mut publish = Box::pin(bus.publish(PublishRequest::new(topic, 7).expect("request")));
    assert!(
        publish
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(publish_waker.lock().expect("publish waker").is_some());

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    assert_eq!(
        shutdown_calls.load(Ordering::SeqCst),
        0,
        "leader must wait for active publish"
    );

    released.store(true, Ordering::SeqCst);
    publish_waker
        .lock()
        .expect("publish waker")
        .take()
        .expect("registered waker")
        .wake();
    let Poll::Ready(Ok(_)) = publish.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("released publish should complete");
    };
    assert!(
        matches!(
            observed.lock().expect("observation lock").as_ref(),
            Some(Poll::Ready(Err(ShutdownError::Lifecycle(
                LifecycleError::WouldDeadlock { operation: "shutdown" }
            ))))
        ),
        "completion wake must reject nested shutdown: {:?}",
        observed.lock().expect("observation lock")
    );
    let Poll::Ready(Ok(report)) = shutdown.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("outer shutdown must complete after publish releases its lease");
    };
    assert_eq!(report.outcome, SpiShutdownOutcome::Complete);
    assert_eq!(shutdown_calls.load(Ordering::SeqCst), 1);
    *bus_slot.lock().expect("bus slot") = None;
}

struct DropReenterSpi {
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    rejected: Arc<std::sync::atomic::AtomicBool>,
    capabilities: EventBusCapabilities,
}

impl AsyncEventBusSpi for DropReenterSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }
    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async { Ok(PublishAcknowledgement::DroppedByInterceptor) })
    }
    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async {
            Err(SpiError::Operation {
                provider_id: "drop-reenter".into(),
                operation: "subscribe",
                resource: None,
                kind: "unused",
                retryable: Some(false),
                source: Box::new(Error::other("unused")),
            })
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<SpiShutdownOutcome, SpiError>> {
        if matches!(mode, ShutdownMode::Graceful { .. }) {
            Box::pin(ReenterOnDrop {
                bus: self.bus.clone(),
                rejected: self.rejected.clone(),
            })
        } else {
            Box::pin(async { Ok(SpiShutdownOutcome::Complete) })
        }
    }
}

struct ReenterOnDrop {
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    rejected: Arc<std::sync::atomic::AtomicBool>,
}

impl Future for ReenterOnDrop {
    type Output = Result<SpiShutdownOutcome, SpiError>;
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for ReenterOnDrop {
    fn drop(&mut self) {
        let bus = self.bus.lock().expect("bus slot").as_ref().cloned();
        let Some(bus) = bus else { return };
        let mut nested = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        let result = nested.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        self.rejected.store(
            matches!(
                result,
                Poll::Ready(Err(qubit_event_bus::error::ShutdownError::Lifecycle(
                    LifecycleError::WouldDeadlock { operation: "shutdown" }
                )))
            ),
            Ordering::SeqCst,
        );
    }
}

#[test]
fn test_graceful_cancelled_spi_future_drop_rejects_same_bus_shutdown() {
    let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let slot = Arc::new(Mutex::new(None));
    let provider = Arc::new(DropReenterSpi {
        bus: slot.clone(),
        rejected: rejected.clone(),
        capabilities: EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::AcceptRetryReject,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        ),
    });
    let bus = AsyncEventBus::from_spi(ProviderId::new("drop-reenter").expect("provider ID"), provider).expect("facade");
    *slot.lock().expect("bus slot") = Some(bus.clone());

    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(
        graceful
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(graceful);
    assert!(
        rejected.load(Ordering::SeqCst),
        "same-bus shutdown from cancelled future Drop is rejected"
    );

    assert!(block_on_shutdown(bus.shutdown(ShutdownMode::Immediate)).is_ok());
}

fn drop_reenter_bus(
    slot: Arc<Mutex<Option<AsyncEventBus>>>,
    rejected: Arc<std::sync::atomic::AtomicBool>,
    clock: &ManualMonotonicClock,
) -> AsyncEventBus {
    let provider = Arc::new(DropReenterSpi {
        bus: slot.clone(),
        rejected,
        capabilities: EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::AcceptRetryReject,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        ),
    });
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("drop-reenter").expect("provider ID"),
        provider,
        clock.new_timer(),
    )
    .expect("facade");
    *slot.lock().expect("bus slot") = Some(bus.clone());
    bus
}

#[test]
fn test_graceful_timeout_cancelled_spi_future_drop_rejects_same_bus_shutdown() {
    let clock = ManualMonotonicClock::new();
    let slot = Arc::new(Mutex::new(None));
    let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let bus = drop_reenter_bus(slot, rejected.clone(), &clock);
    let timeout = Duration::from_nanos(17);
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful { timeout }));
    assert!(
        graceful
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    clock.advance(timeout).expect("advance timeout");
    assert!(matches!(
        graceful.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(ShutdownError::TimedOut { timeout: elapsed })) if elapsed == timeout
    ));
    assert!(
        rejected.load(Ordering::SeqCst),
        "timeout cancellation Drop rejects same-bus shutdown"
    );
    assert!(block_on_shutdown(bus.shutdown(ShutdownMode::Immediate)).is_ok());
}

#[test]
fn test_immediate_upgrade_cancelled_spi_future_drop_rejects_same_bus_shutdown() {
    let clock = ManualMonotonicClock::new();
    let slot = Arc::new(Mutex::new(None));
    let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let bus = drop_reenter_bus(slot, rejected.clone(), &clock);
    let waker = Waker::noop();
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(graceful.as_mut().poll(&mut Context::from_waker(waker)).is_pending());
    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(immediate.as_mut().poll(&mut Context::from_waker(waker)).is_pending());

    let mut graceful_result = None;
    let mut immediate_result = None;
    for _ in 0..4 {
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
    assert!(
        graceful_result.is_some_and(|result| result.is_ok()),
        "escalated graceful driver completes"
    );
    assert!(
        immediate_result.is_some_and(|result| result.is_ok()),
        "immediate waiter completes"
    );
    assert!(
        rejected.load(Ordering::SeqCst),
        "upgrade cancellation Drop rejects same-bus shutdown"
    );
}

struct DropReenterCloseSpi {
    inner: FakeAsyncEventBusSpi,
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    rejected: Arc<std::sync::atomic::AtomicBool>,
    closes: Arc<AtomicUsize>,
    subscription: Arc<Mutex<Option<Weak<qubit_event_bus::AsyncSubscription<u32>>>>>,
    run_rejected: Arc<Mutex<Option<bool>>>,
    handler_calls: Arc<AtomicUsize>,
}

impl AsyncEventBusSpi for DropReenterCloseSpi {
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
        let bus = self.bus.clone();
        let rejected = self.rejected.clone();
        let closes = self.closes.clone();
        let subscription = self.subscription.clone();
        let run_rejected = self.run_rejected.clone();
        let handler_calls = self.handler_calls.clone();
        Box::pin(async move {
            let receiver = inner.subscribe(request).await?;
            Ok(Box::new(DropReenterCloseReceiver {
                inner: receiver,
                bus,
                rejected,
                closes,
                subscription,
                run_rejected,
                handler_calls,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<SpiShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}

struct DropReenterCloseReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    rejected: Arc<std::sync::atomic::AtomicBool>,
    closes: Arc<AtomicUsize>,
    subscription: Arc<Mutex<Option<Weak<qubit_event_bus::AsyncSubscription<u32>>>>>,
    run_rejected: Arc<Mutex<Option<bool>>>,
    handler_calls: Arc<AtomicUsize>,
}

impl AsyncEventSubscriptionSpi for DropReenterCloseReceiver {
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
            Box::pin(ReenterOnCloseDrop {
                bus: self.bus.clone(),
                rejected: self.rejected.clone(),
                subscription: self.subscription.clone(),
                run_rejected: self.run_rejected.clone(),
                handler_calls: self.handler_calls.clone(),
            })
        } else {
            self.inner.close()
        }
    }
}

struct ReenterOnCloseDrop {
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    rejected: Arc<std::sync::atomic::AtomicBool>,
    subscription: Arc<Mutex<Option<Weak<qubit_event_bus::AsyncSubscription<u32>>>>>,
    run_rejected: Arc<Mutex<Option<bool>>>,
    handler_calls: Arc<AtomicUsize>,
}

impl Future for ReenterOnCloseDrop {
    type Output = Result<(), SpiError>;
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for ReenterOnCloseDrop {
    fn drop(&mut self) {
        let bus = self.bus.lock().expect("bus slot").as_ref().cloned();
        let Some(bus) = bus else { return };
        let mut nested = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        let result = nested.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        self.rejected.store(
            matches!(
                result,
                Poll::Ready(Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                    operation: "shutdown"
                })))
            ),
            Ordering::SeqCst,
        );
        let subscription = self
            .subscription
            .lock()
            .expect("subscription slot")
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(subscription) = subscription {
            let nested_handler_calls = self.handler_calls.clone();
            let mut nested = Box::pin(subscription.run(move |_| {
                nested_handler_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<(), qubit_event_bus::DeliveryError>(()) }
            }));
            let result = nested.as_mut().poll(&mut Context::from_waker(Waker::noop()));
            *self.run_rejected.lock().expect("run result") = Some(
                matches!(result, Poll::Ready(Err(ReceiveError::WouldDeadlock)))
                    && self.handler_calls.load(Ordering::SeqCst) == 0,
            );
        }
    }
}

#[test]
fn test_receiver_close_cancelled_by_graceful_timeout_drop_rejects_same_bus_shutdown() {
    let clock = ManualMonotonicClock::new();
    let slot = Arc::new(Mutex::new(None));
    let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let closes = Arc::new(AtomicUsize::new(0));
    let subscription_slot = Arc::new(Mutex::new(None));
    let run_rejected = Arc::new(Mutex::new(None));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(DropReenterCloseSpi {
        inner: FakeAsyncEventBusSpi::new(),
        bus: slot.clone(),
        rejected: rejected.clone(),
        closes: closes.clone(),
        subscription: subscription_slot.clone(),
        run_rejected: run_rejected.clone(),
        handler_calls: handler_calls.clone(),
    });
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("close-drop-reenter").expect("provider ID"),
        spi,
        clock.new_timer(),
    )
    .expect("facade");
    *slot.lock().expect("bus slot") = Some(bus.clone());
    let topic = Topic::<u32>::new("close-drop-reenter").expect("topic");
    let subscription = Arc::new(
        block_on_shutdown(bus.subscribe(SubscribeRequest::new("close-drop-reenter", topic).unwrap()))
            .expect("subscribe"),
    );
    *subscription_slot.lock().expect("subscription slot") = Some(Arc::downgrade(&subscription));

    let timeout = Duration::from_nanos(23);
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful { timeout }));
    assert!(
        graceful
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    clock.advance(timeout).expect("advance timeout");
    assert!(matches!(
        graceful.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(ShutdownError::TimedOut { timeout: elapsed })) if elapsed == timeout
    ));
    assert!(
        rejected.load(Ordering::SeqCst),
        "cancelled receiver close Drop rejects reentrant shutdown"
    );
    assert_eq!(Some(true), *run_rejected.lock().expect("run result"));
    assert_eq!(0, handler_calls.load(Ordering::SeqCst));
    assert_eq!(1, closes.load(Ordering::SeqCst));

    assert!(block_on_shutdown(bus.shutdown(ShutdownMode::Immediate)).is_ok());
    assert_eq!(2, closes.load(Ordering::SeqCst), "retry closes the retained receiver");
}

struct ReceiverCloseLeaseReturnFixture {
    provider: Arc<FakeAsyncEventBusSpi>,
    clock: Arc<ManualMonotonicClock>,
    bus: AsyncEventBus,
    subscription: Arc<qubit_event_bus::AsyncSubscription<u32>>,
    observed: ShutdownReentryObservation,
    run_observed: SubscriptionRunObservation,
    handler_calls: Arc<AtomicUsize>,
    bus_slot: Arc<Mutex<Option<AsyncEventBus>>>,
    wake: Waker,
}

fn receiver_close_lease_return_fixture(name: &'static str) -> ReceiverCloseLeaseReturnFixture {
    let provider = Arc::new(FakeAsyncEventBusSpi::new());
    provider.pause_close();
    let clock = ManualMonotonicClock::new_shared();
    let bus_slot = Arc::new(Mutex::new(None));
    let observed = Arc::new(Mutex::new(None));
    let run_observed = Arc::new(Mutex::new(None));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let bus = AsyncEventBus::with_timer(
        ProviderId::new(name).expect("provider ID"),
        provider.clone(),
        clock.clone().new_timer(),
    )
    .expect("facade");
    *bus_slot.lock().expect("bus lock") = Some(bus.clone());
    let topic = Topic::<u32>::new(name).expect("topic");
    let subscription = Arc::new(
        block_on_shutdown(bus.subscribe(SubscribeRequest::new(name, topic).expect("request"))).expect("subscription"),
    );
    let wake = Waker::from(Arc::new(ReenterOnWake {
        bus: bus_slot.clone(),
        observed: observed.clone(),
        subscription: Arc::new(Mutex::new(Some(Arc::downgrade(&subscription)))),
        run_observed: run_observed.clone(),
        handler_calls: handler_calls.clone(),
        used: std::sync::atomic::AtomicBool::new(false),
    }));
    ReceiverCloseLeaseReturnFixture {
        provider,
        clock,
        bus,
        subscription,
        observed,
        run_observed,
        handler_calls,
        bus_slot,
        wake,
    }
}

fn assert_lease_return_reentry(
    observed: &ShutdownReentryObservation,
    run_observed: &SubscriptionRunObservation,
    handler_calls: &AtomicUsize,
) {
    assert!(
        matches!(
            observed.lock().expect("observation lock").as_ref(),
            Some(Poll::Ready(Err(ShutdownError::Lifecycle(
                LifecycleError::WouldDeadlock { operation: "shutdown" }
            ))))
        ),
        "lease return waker must reject nested shutdown: {:?}",
        observed.lock().expect("observation lock")
    );
    assert!(run_observed.lock().expect("run observation").is_some());
    assert_eq!(0, handler_calls.load(Ordering::SeqCst));
}

#[test]
fn test_receiver_close_lease_return_wake_rejects_shutdown_on_completion() {
    let fixture = receiver_close_lease_return_fixture("close-lease-complete");
    let mut shutdown = Box::pin(fixture.bus.shutdown(ShutdownMode::Immediate));
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        fixture.provider.close_poll_count() > 0,
        "shutdown holds the receiver lease in close"
    );
    let mut run = Box::pin(fixture.subscription.run({
        let handler_calls = fixture.handler_calls.clone();
        move |_| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<(), qubit_event_bus::DeliveryError>(()) }
        }
    }));
    assert!(run.as_mut().poll(&mut Context::from_waker(&fixture.wake)).is_pending());

    fixture.provider.release_close();
    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
    assert_lease_return_reentry(&fixture.observed, &fixture.run_observed, &fixture.handler_calls);
    drop(run);
    *fixture.bus_slot.lock().expect("bus slot") = None;
}

#[test]
fn test_receiver_close_lease_return_wake_rejects_shutdown_on_timeout_cancel() {
    let fixture = receiver_close_lease_return_fixture("close-lease-timeout");
    let timeout = Duration::from_secs(1);
    let mut shutdown = Box::pin(fixture.bus.shutdown(ShutdownMode::Graceful { timeout }));
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        fixture.provider.close_poll_count() > 0,
        "shutdown holds the receiver lease in close"
    );
    let mut run = Box::pin(fixture.subscription.run({
        let handler_calls = fixture.handler_calls.clone();
        move |_| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<(), qubit_event_bus::DeliveryError>(()) }
        }
    }));
    assert!(run.as_mut().poll(&mut Context::from_waker(&fixture.wake)).is_pending());

    fixture.clock.advance(timeout).expect("advance graceful deadline");
    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(ShutdownError::TimedOut { timeout: elapsed })) if elapsed == timeout
    ));
    assert_lease_return_reentry(&fixture.observed, &fixture.run_observed, &fixture.handler_calls);

    drop(run);
    *fixture.bus_slot.lock().expect("bus slot") = None;
    fixture.provider.release_close();
    assert!(block_on_shutdown(fixture.bus.shutdown(ShutdownMode::Immediate)).is_ok());
}

fn block_on_shutdown<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn test_immediate_shutdown_provider_error_reaches_both_waiters() {
    let provider = Arc::new(ShutdownGateSpi::new(true));
    let clock = ManualMonotonicClock::new();
    let bus = AsyncEventBus::with_timer(
        ProviderId::new("shutdown-gate").expect("provider ID"),
        provider.clone(),
        clock.new_timer(),
    )
    .expect("facade");
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(graceful.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(immediate.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    for result in [
        graceful.as_mut().poll(&mut Context::from_waker(&waker)),
        immediate.as_mut().poll(&mut Context::from_waker(&waker)),
    ] {
        let Poll::Ready(Err(ShutdownError::Spi(SpiError::Operation {
            operation: "shutdown",
            kind: "immediate_shutdown_failed",
            source,
            ..
        }))) = result
        else {
            panic!("both shutdown waiters must observe the provider failure");
        };
        assert_eq!(
            ErrorKind::Other,
            source.downcast_ref::<Error>().expect("source is IO error").kind()
        );
    }
    assert_eq!(
        None,
        bus.shutdown_report(),
        "SPI shutdown failure must not publish a report"
    );
    let calls = provider.calls.lock().expect("calls lock");
    assert_eq!(
        &[
            ShutdownMode::Graceful {
                timeout: Duration::from_secs(30)
            },
            ShutdownMode::Immediate,
        ],
        &calls[..2],
        "graceful shutdown is escalated before any immediate retry",
    );
    assert!(calls[2..].iter().all(|mode| *mode == ShutdownMode::Immediate));
}
