// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Reproduces synchronous reentrancy from async waiter waker callbacks.

mod support;

use std::env::var;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::SystemTime;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::AsyncSubscription;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::facade::AsyncSubscriptionRunState;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use support::manual_async::block_on;
use support::manual_async::poll_once;

struct SwitchingTimer {
    normal: ManualMonotonicClock,
    foreign: ManualMonotonicClock,
    switched: AtomicBool,
}

impl Timer for SwitchingTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        if self.switched.load(Ordering::SeqCst) {
            &self.foreign
        } else {
            &self.normal
        }
    }

    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        self.clock().new_timer().at(deadline)
    }
}

struct MetricsWake {
    subscription: Arc<AsyncSubscription<u32>>,
    calls: AtomicUsize,
}

impl Wake for MetricsWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!("A: stop waker callback reached before delivery_metrics reentry (calls={calls})");
        let _ = self.subscription.delivery_metrics();
    }
}

fn reentrant_delivery_metrics_on_clock_failure() {
    let fake = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let timer = Arc::new(SwitchingTimer {
        normal: ManualMonotonicClock::new(),
        foreign: ManualMonotonicClock::new(),
        switched: AtomicBool::new(false),
    });
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("waker-reentry-a").expect("provider"),
        fake,
        EventBusFacadeConfig::default(),
        timer.clone(),
    )
    .expect("bus");
    let subscription = Arc::new(
        block_on(bus.subscribe(
            SubscribeRequest::new("reentrant", Topic::<u32>::new("waker.reentry.a").expect("topic")).expect("request"),
        ))
        .expect("subscription"),
    );
    let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
    let wake = Arc::new(MetricsWake {
        subscription: subscription.clone(),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    assert!(
        poll_once(runner.as_mut()).is_pending(),
        "runner installs its stop waiter"
    );
    // The support poller has its own waker, so poll again with the reentrant one.
    assert!(runner.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    timer.switched.store(true, Ordering::SeqCst);
    let _ = subscription.delivery_metrics();
    assert!(
        wake.calls.load(Ordering::SeqCst) > 0,
        "the stop waiter invoked its callback"
    );
    let terminal_failure = subscription.terminal_failure();
    let Some(SubscriptionStopReason::Provider { error }) = terminal_failure.as_deref() else {
        panic!("delivery metrics clock failure is retained as the terminal cause");
    };
    assert_eq!(error.operation(), "delivery_metrics");
    assert_eq!(error.kind(), "delivery_metrics_clock_failure");
    drop(runner);
    let _ = block_on(bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate));
}

struct PublishOnDrop {
    spi: Arc<AsyncLocalEventBusSpi>,
    message: Option<OutboundMessage>,
    drops: Arc<AtomicUsize>,
    publishes: Arc<AtomicUsize>,
}

impl Wake for PublishOnDrop {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {}
}

impl Drop for PublishOnDrop {
    fn drop(&mut self) {
        let drops = self.drops.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(message) = self.message.take() {
            eprintln!("B: final old Waker drop reached before same-SPI publish (drops={drops})");
            let _ = block_on(self.spi.publish(message));
            let publishes = self.publishes.fetch_add(1, Ordering::SeqCst) + 1;
            eprintln!("B: reentrant publish returned (calls={publishes})");
        }
    }
}

fn old_waker_drop_reenters_same_spi_publish() {
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::default()).expect("local SPI"));
    let mut receiver = block_on(spi.subscribe(support::fake_spi::subscription_request())).expect("receiver");
    let message = OutboundMessage::new(
        TopicAddress::new("test.topic").expect("topic"),
        EventId::new("drop-reentry").expect("event id"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(71_u32)),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let publishes = Arc::new(AtomicUsize::new(0));
    let probe = Arc::new(PublishOnDrop {
        spi: spi.clone(),
        message: Some(message),
        drops: drops.clone(),
        publishes: publishes.clone(),
    });
    let waker = Waker::from(probe.clone());
    let mut receive = receiver.receive(Duration::MAX);
    assert!(receive.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(waker);
    drop(probe);
    assert!(
        receive
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "old registration released its final waker reference"
    );
    assert_eq!(
        publishes.load(Ordering::SeqCst),
        1,
        "Drop callback completed same-SPI publish"
    );
}

#[derive(Default)]
struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct SettleOnClone {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

unsafe fn clone_settle_on_clone(pointer: *const ()) -> RawWaker {
    // SAFETY: The pointer is an owned Arc<SettleOnClone> for each raw waker.
    let probe = unsafe { &*pointer.cast::<SettleOnClone>() };
    if let Some(callback) = probe.callback.lock().expect("callback lock").take() {
        callback();
    }
    // SAFETY: Cloning adds one owned strong reference for the returned RawWaker.
    unsafe { Arc::<SettleOnClone>::increment_strong_count(pointer.cast()) };
    RawWaker::new(pointer, &SETTLE_ON_CLONE_VTABLE)
}

unsafe fn drop_settle_on_clone(pointer: *const ()) {
    // SAFETY: This callback consumes one reference originating from Arc::into_raw.
    drop(unsafe { Arc::<SettleOnClone>::from_raw(pointer.cast()) });
}

unsafe fn wake_settle_on_clone(pointer: *const ()) {
    // SAFETY: wake consumes the same owned reference as drop.
    unsafe { drop_settle_on_clone(pointer) };
}

unsafe fn wake_settle_on_clone_by_ref(_: *const ()) {}

static SETTLE_ON_CLONE_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_settle_on_clone,
    wake_settle_on_clone,
    wake_settle_on_clone_by_ref,
    drop_settle_on_clone,
);

fn raw_waker_settling_on_clone(callback: impl FnOnce() + Send + 'static) -> (Waker, Arc<SettleOnClone>) {
    let probe = Arc::new(SettleOnClone {
        callback: Mutex::new(Some(Box::new(callback))),
    });
    let pointer = Arc::into_raw(probe.clone()).cast();
    // SAFETY: The vtable owns and releases the Arc reference represented by
    // pointer.
    let waker = unsafe { Waker::from_raw(RawWaker::new(pointer, &SETTLE_ON_CLONE_VTABLE)) };
    (waker, probe)
}

struct RunOnDrop {
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    drops: Arc<AtomicUsize>,
}

// This custom waker is used only to observe destruction; wake notifications
// intentionally have no behavior.
#[allow(unknown_lints, clippy::manual_noop_waker)]
impl Wake for RunOnDrop {
    fn wake(self: Arc<Self>) {}

    fn wake_by_ref(self: &Arc<Self>) {}
}

impl Drop for RunOnDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(callback) = self.callback.lock().expect("callback lock").take() {
            callback();
        }
    }
}

fn session_lease_reentrant_setup() -> Arc<AsyncSubscription<u32>> {
    let fake = Arc::new(support::fake_spi::FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::with_config(
        ProviderId::new("session-lease-reentry").expect("provider"),
        fake,
        EventBusFacadeConfig::default(),
    )
    .expect("bus");
    Arc::new(
        block_on(bus.subscribe(
            SubscribeRequest::new("lease", Topic::<u32>::new("lease.reentry").expect("topic")).expect("request"),
        ))
        .expect("subscription"),
    )
}

fn session_lease_clone_reentry() {
    let subscription = session_lease_reentrant_setup();
    let mut active = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(poll_once(active.as_mut()).is_pending(), "first run owns the lease");
    let nested_subscription = subscription.clone();
    let (waker, _probe) = raw_waker_settling_on_clone(move || {
        let mut nested = Box::pin(nested_subscription.run(|_| async { Ok(()) }));
        assert!(poll_once(nested.as_mut()).is_pending(), "nested run waits for lease");
        drop(nested);
    });
    let mut waiting = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(waiting.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(waiting);
    drop(waker);
    drop(active);
}

fn session_lease_drop_reentry() {
    let subscription = session_lease_reentrant_setup();
    let mut active = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(poll_once(active.as_mut()).is_pending(), "first run owns the lease");
    let nested_subscription = subscription.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = Arc::new(RunOnDrop {
        callback: Mutex::new(Some(Box::new(move || {
            let mut nested = Box::pin(nested_subscription.run(|_| async { Ok(()) }));
            assert!(poll_once(nested.as_mut()).is_pending(), "nested run waits for lease");
            drop(nested);
        }))),
        drops: drops.clone(),
    });
    let waker = Waker::from(probe.clone());
    let mut waiting = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(waiting.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(waker);
    drop(probe);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1, "retired waker was dropped once");
    drop(waiting);
    drop(active);
}

fn session_lease_waiter_cancellation_and_return() {
    let subscription = session_lease_reentrant_setup();
    let mut active = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(poll_once(active.as_mut()).is_pending(), "first run owns the lease");

    let cancelled_wake = Arc::new(CountWake::default());
    let cancelled_waker = Waker::from(cancelled_wake.clone());
    let mut cancelled = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(&cancelled_waker))
            .is_pending()
    );
    drop(cancelled);
    cancelled_wake.0.store(0, Ordering::SeqCst);

    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut waiting = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(waiting.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(active);
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "returning the lease wakes its waiter"
    );
    assert!(waiting.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    assert_eq!(
        subscription.run_state(),
        AsyncSubscriptionRunState::Running,
        "the woken waiter acquires the session before waiting for receive"
    );
    assert_eq!(
        cancelled_wake.0.load(Ordering::SeqCst),
        0,
        "cancelled waiter is not woken"
    );
    drop(waiting);
}

fn busy_local_mailbox() -> (
    Arc<AsyncLocalEventBusSpi>,
    Box<dyn AsyncEventSubscriptionSpi>,
    SettlementToken,
) {
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::default()).expect("local SPI"));
    let mut receiver = block_on(spi.subscribe(support::fake_spi::subscription_request())).expect("receiver");
    let message = OutboundMessage::new(
        TopicAddress::new("test.topic").expect("topic"),
        EventId::new("graceful-shutdown").expect("event id"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(73_u32)),
    );
    let _ = block_on(spi.publish(message)).expect("message admitted");
    let ReceiveOutcome::Message(mut message) = block_on(receiver.receive(Duration::ZERO)).expect("receive") else {
        panic!("message must enter flight before graceful shutdown");
    };
    let token = message
        .take_settlement()
        .expect("in-flight message has settlement token");
    (spi, receiver, token)
}

fn graceful_shutdown_registers_before_last_settlement() {
    let (spi, receiver, token) = busy_local_mailbox();
    let receiver = Arc::new(Mutex::new(receiver));
    let settled = Arc::new(AtomicBool::new(false));
    let callback_receiver = receiver.clone();
    let callback_settled = settled.clone();
    let (waker, _probe) = raw_waker_settling_on_clone(move || {
        let mut receiver = callback_receiver.lock().expect("receiver lock");
        block_on(receiver.settle(&token, DeliveryDisposition::Accept)).expect("settle last delivery");
        callback_settled.store(true, Ordering::SeqCst);
    });
    let mut shutdown = spi.shutdown(qubit_event_bus::spi::ShutdownMode::Graceful { timeout: Duration::MAX });
    assert!(
        matches!(
            shutdown.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(qubit_event_bus::spi::ShutdownOutcome::Complete))
        ),
        "poll must observe the settlement performed by waker clone"
    );
    assert!(
        settled.load(Ordering::SeqCst),
        "clone settled the in-flight delivery exactly during poll"
    );
}

fn graceful_shutdown_last_settlement_wakes_pending_waiter() {
    let (spi, mut receiver, token) = busy_local_mailbox();
    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut shutdown = spi.shutdown(qubit_event_bus::spi::ShutdownMode::Graceful { timeout: Duration::MAX });
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    block_on(receiver.settle(&token, DeliveryDisposition::Accept)).expect("settle last delivery");
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "settlement wakes the registered shutdown task"
    );
    assert!(
        matches!(
            shutdown.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(qubit_event_bus::spi::ShutdownOutcome::Complete))
        ),
        "next poll observes an idle mailbox"
    );
}

fn graceful_shutdown_old_waker_drop_reenters_publish() {
    let (spi, _receiver, _token) = busy_local_mailbox();
    let message = OutboundMessage::new(
        TopicAddress::new("test.topic").expect("topic"),
        EventId::new("drop-reentry-shutdown").expect("event id"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(74_u32)),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let publishes = Arc::new(AtomicUsize::new(0));
    let probe = Arc::new(PublishOnDrop {
        spi: spi.clone(),
        message: Some(message),
        drops: drops.clone(),
        publishes: publishes.clone(),
    });
    let waker = Waker::from(probe.clone());
    let mut shutdown = spi.shutdown(qubit_event_bus::spi::ShutdownMode::Graceful { timeout: Duration::MAX });
    assert!(shutdown.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(waker);
    drop(probe);
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(
        1,
        drops.load(Ordering::SeqCst),
        "retired registration drops its final old waker"
    );
    assert_eq!(
        1,
        publishes.load(Ordering::SeqCst),
        "Drop callback's same-SPI publish returns"
    );
}

fn test_local_waiter_interleavings() {
    let topic = TopicAddress::new("test.topic").expect("topic");
    let message = || {
        OutboundMessage::new(
            topic.clone(),
            EventId::new("waiter-interleaving").expect("event id"),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            None,
            TransportPayload::Native(Arc::new(72_u32)),
        )
    };

    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::default()).expect("local SPI"));
    let mut receiver = block_on(spi.subscribe(support::fake_spi::subscription_request())).expect("receiver");
    let _ = block_on(spi.publish(message())).expect("publish before registration");
    assert!(matches!(
        block_on(receiver.receive(Duration::MAX)),
        Ok(ReceiveOutcome::Message(_))
    ));
    drop(receiver);

    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::default()).expect("local SPI"));
    let mut receiver = block_on(spi.subscribe(support::fake_spi::subscription_request())).expect("receiver");
    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut receive = receiver.receive(Duration::MAX);
    assert!(receive.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    let _ = block_on(spi.publish(message())).expect("publish after registration");
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "registered waiter is woken by publish"
    );
    assert!(matches!(
        receive.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(ReceiveOutcome::Message(_)))
    ));
    drop(receive);
    drop(receiver);

    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::default()).expect("local SPI"));
    let mut receiver = block_on(spi.subscribe(support::fake_spi::subscription_request())).expect("receiver");
    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut receive = receiver.receive(Duration::MAX);
    assert!(receive.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(receive);
    wake.0.store(0, Ordering::SeqCst);
    let _ = block_on(spi.publish(message())).expect("publish after cancellation");
    assert_eq!(wake.0.load(Ordering::SeqCst), 0, "cancelled waiter is removed");
    drop(receiver);

    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::default()).expect("local SPI"));
    let mut receiver = block_on(spi.subscribe(support::fake_spi::subscription_request())).expect("receiver");
    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut receive = receiver.receive(Duration::MAX);
    assert!(receive.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    let _ = block_on(spi.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)).expect("shutdown");
    assert!(wake.0.load(Ordering::SeqCst) > 0, "shutdown wakes the pending receive");
    assert!(matches!(
        receive.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(ReceiveOutcome::Closed))
    ));
}

#[test]
fn test_a_fail_receive_wake_reentry() {
    if var("QUBIT_EVENT_BUS_ISOLATED_CASE").is_err() {
        support::isolated_process::run_case("test_a_fail_receive_wake_reentry", "a");
        return;
    }
    reentrant_delivery_metrics_on_clock_failure();
}

#[test]
fn test_b_old_registration_drop_reenters_publish() {
    if var("QUBIT_EVENT_BUS_ISOLATED_CASE").is_err() {
        support::isolated_process::run_case("test_b_old_registration_drop_reenters_publish", "b");
        return;
    }
    old_waker_drop_reenters_same_spi_publish();
    test_local_waiter_interleavings();
}

#[test]
fn test_graceful_shutdown_registers_before_last_settlement() {
    graceful_shutdown_registers_before_last_settlement();
}

#[test]
fn test_graceful_shutdown_last_settlement_wakes_pending_waiter() {
    graceful_shutdown_last_settlement_wakes_pending_waiter();
}

#[test]
fn test_graceful_shutdown_old_waker_drop_reenters_publish() {
    if var("QUBIT_EVENT_BUS_ISOLATED_CASE").is_err() {
        support::isolated_process::run_case(
            "test_graceful_shutdown_old_waker_drop_reenters_publish",
            "graceful-shutdown",
        );
        return;
    }
    graceful_shutdown_old_waker_drop_reenters_publish();
}

#[test]
fn test_session_lease_clone_reentry() {
    if var("QUBIT_EVENT_BUS_ISOLATED_CASE").is_err() {
        support::isolated_process::run_case("test_session_lease_clone_reentry", "session-lease-clone");
        return;
    }
    session_lease_clone_reentry();
}

#[test]
fn test_session_lease_drop_reentry() {
    if var("QUBIT_EVENT_BUS_ISOLATED_CASE").is_err() {
        support::isolated_process::run_case("test_session_lease_drop_reentry", "session-lease-drop");
        return;
    }
    session_lease_drop_reentry();
}

#[test]
fn test_session_lease_waiter_cancellation_and_return() {
    session_lease_waiter_cancellation_and_return();
}
