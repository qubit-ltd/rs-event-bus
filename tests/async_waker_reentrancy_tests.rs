// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
// =============================================================================
//! Reproduces synchronous reentrancy from async waiter waker callbacks.

mod support;

use std::env::var;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
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
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
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
