// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async subscription behavior for provider-reported delivery gaps.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::AsyncSubscription;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::model::GapPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::manual_async::block_on;

fn wait_for(calls: &AtomicUsize, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while calls.load(Ordering::Acquire) < expected {
        assert!(Instant::now() < deadline, "handler did not start before deadline");
        std::thread::yield_now();
    }
}

fn installed(handle: &Mutex<Option<Arc<AsyncSubscription<u32>>>>) -> Arc<AsyncSubscription<u32>> {
    handle
        .lock()
        .unwrap()
        .as_ref()
        .expect("subscription handle installed")
        .clone()
}

#[test]
fn test_default_gap_policy_stops_async_run_with_the_gap_reason() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("gap-test").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("gap.events").unwrap();
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("gap-test", topic).unwrap())).unwrap();
    spi.inject_gap();

    let error = block_on(subscription.run(|_| async { Ok::<(), DeliveryError>(()) }))
        .expect_err("default stop policy must terminate after a gap");
    match error {
        qubit_event_bus::ReceiveError::Stopped(reason) => match reason.as_ref() {
            SubscriptionStopReason::Gap { gap } => {
                assert_eq!(gap.reason.as_ref(), "fake gap");
                assert_eq!(gap.missed, Some(1));
            }
            other => panic!("expected gap reason, got {other:?}"),
        },
        other => panic!("expected stopped receive, got {other:?}"),
    }
}

#[test]
fn test_stop_gap_publishes_stopped_state_before_blocked_diagnostic() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("async-gap-stop").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("async.gap.stop").unwrap();
    let handle: Arc<Mutex<Option<Arc<AsyncSubscription<u32>>>>> = Arc::new(Mutex::new(None));
    let observed_handle = handle.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::ReceiveGap { .. }) {
            let subscription = installed(&observed_handle);
            let stopped = matches!(
                subscription.terminal_failure().as_deref(),
                Some(SubscriptionStopReason::Gap { .. })
            );
            entered_tx.send(stopped).unwrap();
            release_rx.lock().unwrap().recv_timeout(Duration::from_secs(2)).unwrap();
        }
    });
    let subscription =
        Arc::new(block_on(bus.subscribe(SubscribeRequest::new("async-gap-stop", topic.clone()).unwrap())).unwrap());
    *handle.lock().unwrap() = Some(subscription.clone());
    let runner_subscription = subscription.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let runner = std::thread::spawn(move || {
        block_on(runner_subscription.run(move |_| {
            let calls = handler_calls.clone();
            async move {
                calls.fetch_add(1, Ordering::Release);
                Ok::<(), DeliveryError>(())
            }
        }))
    });
    spi.inject_gap();
    let stopped_in_observer = entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("gap observer entered");
    let _ = block_on(bus.publish(PublishRequest::new(topic, 1).unwrap())).unwrap();
    release_tx.send(()).unwrap();
    let result = runner.join().unwrap();
    assert!(stopped_in_observer, "stop barrier must be visible in the observer");
    assert_eq!(
        0,
        calls.load(Ordering::Acquire),
        "queued work must not cross the stop barrier"
    );
    assert!(matches!(result, Err(qubit_event_bus::ReceiveError::Stopped(reason))
        if matches!(reason.as_ref(), SubscriptionStopReason::Gap { .. })));
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn test_continue_gap_emits_diagnostic_and_keeps_receiving() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("async-gap-continue").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("async.gap.continue").unwrap();
    let options = SubscribeOptions::builder().gap_policy(GapPolicy::Continue).build();
    let handle: Arc<Mutex<Option<Arc<AsyncSubscription<u32>>>>> = Arc::new(Mutex::new(None));
    let observed_handle = handle.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::ReceiveGap { .. }) {
            let subscription = installed(&observed_handle);
            let continuing = subscription.terminal_failure().is_none();
            entered_tx.send(continuing).unwrap();
            release_rx.lock().unwrap().recv_timeout(Duration::from_secs(2)).unwrap();
        }
    });
    let subscription = Arc::new(
        block_on(
            bus.subscribe(
                SubscribeRequest::new("async-gap-continue", topic.clone())
                    .unwrap()
                    .with_options(options),
            ),
        )
        .unwrap(),
    );
    *handle.lock().unwrap() = Some(subscription.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let runner_subscription = subscription.clone();
    let runner = std::thread::spawn(move || {
        block_on(runner_subscription.run(move |_| {
            let calls = handler_calls.clone();
            async move {
                calls.fetch_add(1, Ordering::Release);
                Ok::<(), DeliveryError>(())
            }
        }))
    });
    spi.inject_gap();
    let continuing_in_observer = entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("gap observer entered");
    let _ = block_on(bus.publish(PublishRequest::new(topic, 1).unwrap())).unwrap();
    release_tx.send(()).unwrap();
    assert!(
        continuing_in_observer,
        "Continue policy must leave receiving active in the observer"
    );
    wait_for(&calls, 1);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    let _ = runner.join().unwrap();
}
