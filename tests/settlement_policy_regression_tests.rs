// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sync settlement retryability and opaque-token ownership regressions.
mod support;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DeliveryDisposition;
use support::settlement_probe::CancelOnDrop;
use support::settlement_probe::ProbeBus;
use support::settlement_probe::SettlementProbe;

/// Starts a test subscription with a handler invocation counter.
fn subscribe(bus: &EventBus, name: &str, calls: Arc<AtomicUsize>) -> Subscription {
    bus.subscribe(
        SubscribeRequest::new(name, Topic::<u32>::new(name).expect("valid topic"))
            .expect("valid subscriber"),
        move |_: Delivery<u32>| {
            calls.fetch_add(1, Ordering::SeqCst);
        },
    )
    .expect("subscription starts")
}
/// Publishes a scalar through the real facade.
fn publish(bus: &EventBus, topic: &str) {
    let _ = bus
        .publish(
            PublishRequest::new(Topic::new(topic).expect("valid topic"), 42_u32)
                .expect("valid request"),
        )
        .expect("publish succeeds");
}

#[test]
fn test_sync_permanent_settlement_stops_subscription_without_cancel() {
    let spi = ProbeBus::new();
    let bad = SettlementProbe::new(false, usize::MAX);
    let healthy = SettlementProbe::new(true, 0);
    spi.register("bad", bad.clone());
    spi.register("healthy", healthy.clone());
    let bus = EventBus::from_spi(ProviderId::new("probe").expect("valid provider"), spi)
        .expect("valid capabilities");
    let calls = Arc::new(AtomicUsize::new(0));
    let subscription = Arc::new(subscribe(&bus, "bad", calls.clone()));
    let healthy_calls = Arc::new(AtomicUsize::new(0));
    let healthy_subscription = subscribe(&bus, "healthy", healthy_calls.clone());
    let _cancel_on_unwind = CancelOnDrop(vec![&subscription, &healthy_subscription]);
    let _release_settle = bad.settle_gate.release_on_drop();
    let _release_close = bad.close_gate.release_on_drop();
    publish(&bus, "bad");
    assert!(bad.entered.wait(1), "provider settlement entered");
    let observed = subscription.clone();
    let (terminal_tx, terminal_rx) = mpsc::channel();
    let observer = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        while observed.terminal_failure().is_none() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let _ = terminal_tx.send(observed.terminal_failure().is_some());
    });
    let terminal = terminal_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("status observer watchdog");
    observer.join().expect("observer exits");
    // Capture the natural-stop evidence before cleanup cancellation.
    if terminal {
        assert!(bad.closed.wait(1), "terminal stop reaches provider close");
    }
    let attempts = bad.entered.count();
    let closed = bad.closed.count();
    publish(&bus, "healthy");
    assert!(
        healthy.entered.wait(1),
        "healthy subscription settles independently"
    );
    subscription.cancel().expect("failure-safe cleanup");
    healthy_subscription.cancel().expect("healthy cleanup");
    assert!(
        terminal,
        "retryable=false must notify terminal failure without cancellation"
    );
    assert_eq!(attempts, 1, "permanent failure must never retry");
    assert_eq!(closed, 1, "permanent stop closes receiver exactly once");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(healthy_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn test_sync_settlement_retry_preserves_token_and_disposition() {
    let spi = ProbeBus::new();
    let probe = SettlementProbe::new(true, 1);
    spi.register("retry", probe.clone());
    let bus = EventBus::from_spi(ProviderId::new("probe").expect("valid provider"), spi)
        .expect("valid capabilities");
    let calls = Arc::new(AtomicUsize::new(0));
    let subscription = subscribe(&bus, "retry", calls.clone());
    let _cancel_on_unwind = CancelOnDrop(vec![&subscription]);
    publish(&bus, "retry");
    let retried = probe.entered.wait(2);
    subscription.cancel().expect("cleanup");
    assert!(
        retried,
        "explicit retryable failure receives a second settlement attempt"
    );
    let attempts = probe.attempts.lock().expect("attempts lock");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
    let (_, _, disposition) = attempts[0];
    assert_eq!(disposition, DeliveryDisposition::Accept);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// A non-returning owner SPI must not retain the only real handler thread.
#[test]
fn test_sync_blocked_settlement_releases_real_handler_thread_and_serializes_receiver() {
    let spi = ProbeBus::new();
    let blocked = Arc::new(SettlementProbe::default());
    blocked.close_gate.release();
    let healthy = SettlementProbe::new(true, 0);
    spi.register("blocked", blocked.clone());
    spi.register("healthy", healthy.clone());
    let positive = |n| NonZeroUsize::new(n).expect("positive limit");
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(positive(1), positive(6), positive(3), positive(2))
            .expect("valid config"),
    );
    let bus = EventBus::with_config(
        ProviderId::new("probe").expect("valid provider"),
        spi,
        config,
    )
    .expect("bus starts");
    let first_handler = Arc::new(support::settlement_probe::Gate::default());
    let first_entered = Arc::new(support::settlement_probe::Signal::default());
    let handler_gate = first_handler.clone();
    let handler_entered = first_entered.clone();
    let blocked_subscription = bus
        .subscribe(
            SubscribeRequest::new("blocked", Topic::<u32>::new("blocked").expect("topic"))
                .expect("request"),
            move |_| {
                handler_entered.enter();
                handler_gate.wait();
            },
        )
        .expect("blocked subscription");
    let healthy_calls = Arc::new(AtomicUsize::new(0));
    let healthy_subscription = subscribe(&bus, "healthy", healthy_calls.clone());
    let _cancel = CancelOnDrop(vec![&blocked_subscription, &healthy_subscription]);
    let _release = blocked.settle_gate.release_on_drop();
    let _handler_release = first_handler.release_on_drop();
    publish(&bus, "blocked");
    assert!(first_entered.wait(1), "first handler is gated");
    publish(&bus, "blocked");
    assert!(
        blocked.received.wait(2),
        "another runnable delivery is queued before settlement"
    );
    first_handler.release();
    assert!(
        blocked.entered.wait(1),
        "blocked owner is inside settlement"
    );
    publish(&bus, "blocked");
    publish(&bus, "healthy");
    assert!(
        healthy.entered.wait(1),
        "the sole real pool thread runs a healthy subscription during blocked settlement"
    );
    assert_eq!(healthy_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        blocked.received.count(),
        2,
        "the blocked receiver cannot concurrently receive its third message"
    );
    let metrics = blocked_subscription.delivery_metrics().metrics;
    assert_eq!(
        metrics.running_handlers, 0,
        "handler completion releases the execution gauge while SPI is blocked"
    );
    assert!(
        (1..=2).contains(&metrics.settling),
        "one or two owned deliveries may await owner settlement while the receiver is blocked"
    );
    assert_eq!(metrics.completed, 0);
    assert_eq!(
        metrics.settlement_attempts, 1,
        "only the first settlement has entered the blocked provider call"
    );
    assert!(
        (1..=2).contains(&metrics.handler_duration_count),
        "one or two accepted handlers may finish while the provider settlement is blocked"
    );
    assert_eq!(
        metrics.settlement_duration_count, 0,
        "in-flight settlement has no completed duration sample"
    );
    blocked.settle_gate.release();
}
