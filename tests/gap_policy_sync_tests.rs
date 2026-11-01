// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sync subscription behavior for provider-reported delivery gaps.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::GapPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

use crate::support::fake_spi::FakeEventBusSpi;

fn wait_for(calls: &AtomicUsize, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while calls.load(Ordering::Acquire) < expected {
        assert!(Instant::now() < deadline, "handler did not start before deadline");
        std::thread::yield_now();
    }
}

#[test]
fn test_default_gap_policy_stops_sync_subscription_with_the_gap_reason() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("gap-test").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("gap.events").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let subscription = bus
        .subscribe(SubscribeRequest::new("gap-test", topic.clone()).unwrap(), move |_| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), DeliveryError>(())
        })
        .unwrap();
    spi.inject_gap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let reason = loop {
        if let Some(reason) = subscription.terminal_failure() {
            break reason;
        }
        assert!(Instant::now() < deadline, "gap was not retained as terminal cause");
        std::thread::yield_now();
    };
    match reason.as_ref() {
        SubscriptionStopReason::Gap { gap } => {
            assert_eq!(gap.reason.as_ref(), "fake gap");
            assert_eq!(gap.missed, Some(1));
        }
        other => panic!("expected gap reason, got {other:?}"),
    }

    let _ = bus.publish(PublishRequest::new(topic, 7).unwrap()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn test_stop_gap_publishes_stopped_state_before_blocked_diagnostic() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("gap-stop").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("gap.stop").unwrap();
    let handle: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
    let observed_handle = handle.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::ReceiveGap { .. }) {
            let subscription = observed_handle.lock().unwrap();
            let subscription = subscription.as_ref().expect("subscription handle installed");
            let stopped = matches!(
                subscription.terminal_failure().as_deref(),
                Some(SubscriptionStopReason::Gap { .. })
            ) && subscription.is_cancelled();
            entered_tx.send(stopped).unwrap();
            release_rx.lock().unwrap().recv_timeout(Duration::from_secs(2)).unwrap();
        }
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let subscription = bus
        .subscribe(SubscribeRequest::new("gap-stop", topic.clone()).unwrap(), move |_| {
            handler_calls.fetch_add(1, Ordering::Release);
            Ok::<(), DeliveryError>(())
        })
        .unwrap();
    *handle.lock().unwrap() = Some(subscription);
    spi.inject_gap();
    let stopped_in_observer = entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("gap observer entered");
    let _ = bus.publish(PublishRequest::new(topic, 1).unwrap()).unwrap();
    assert_eq!(
        0,
        calls.load(Ordering::Acquire),
        "queued work must not cross the stop barrier"
    );
    release_tx.send(()).unwrap();
    assert!(stopped_in_observer, "stop barrier must be visible in the observer");
    let subscription = handle.lock().unwrap().take().unwrap();
    subscription.cancel().unwrap();
    assert_eq!(
        0,
        calls.load(Ordering::Acquire),
        "stopped subscription must discard later work"
    );
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn test_continue_gap_emits_diagnostic_and_keeps_receiving() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("gap-continue").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("gap.continue").unwrap();
    let handle: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
    let observed_handle = handle.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::ReceiveGap { .. }) {
            let subscription = observed_handle.lock().unwrap();
            let subscription = subscription.as_ref().expect("subscription handle installed");
            let continuing = subscription.terminal_failure().is_none() && !subscription.is_cancelled();
            entered_tx.send(continuing).unwrap();
            release_rx.lock().unwrap().recv_timeout(Duration::from_secs(2)).unwrap();
        }
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let options = SubscribeOptions::builder().gap_policy(GapPolicy::Continue).build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("gap-continue", topic.clone())
                .unwrap()
                .with_options(options),
            move |_| {
                handler_calls.fetch_add(1, Ordering::Release);
                Ok::<(), DeliveryError>(())
            },
        )
        .unwrap();
    *handle.lock().unwrap() = Some(subscription);
    spi.inject_gap();
    let continuing_in_observer = entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("gap observer entered");
    let _ = bus.publish(PublishRequest::new(topic, 1).unwrap()).unwrap();
    release_tx.send(()).unwrap();
    assert!(
        continuing_in_observer,
        "Continue policy must leave receiving active in the observer"
    );
    wait_for(&calls, 1);
    let subscription = handle.lock().unwrap().take().unwrap();
    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
}
