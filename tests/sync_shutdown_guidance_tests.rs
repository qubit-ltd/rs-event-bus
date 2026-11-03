// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous cancellation guidance contracts for the built-in local provider.

mod support;

use std::env;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

const CHILD_MODE: &str = "local-sync-cancel-backlog";
const GRACEFUL_CHILD_MODE: &str = "local-graceful-shutdown-timeout";

#[test]
fn test_local_cancel_waits_for_active_handler_and_discards_queued_delivery() {
    if env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() == Ok(CHILD_MODE) {
        run_blocked_local_cancel_scenario();
        return;
    }
    support::isolated_process::run_case(
        "test_local_cancel_waits_for_active_handler_and_discards_queued_delivery",
        CHILD_MODE,
    );
}

#[test]
fn test_graceful_shutdown_timeout_can_be_observed_again_after_handler_release() {
    if env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() == Ok(GRACEFUL_CHILD_MODE) {
        run_graceful_shutdown_timeout_scenario();
        return;
    }
    support::isolated_process::run_case(
        "test_graceful_shutdown_timeout_can_be_observed_again_after_handler_release",
        GRACEFUL_CHILD_MODE,
    );
}

fn run_blocked_local_cancel_scenario() {
    let bus = EventBus::local(LocalEventBusConfig::new().queue_capacity(4)).expect("local bus");
    let topic = Topic::<String>::new("shutdown.local-cancel").expect("topic");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let release_rx_by_handler = release_rx.clone();
    let subscription = Arc::new(
        bus.subscribe(
            SubscribeRequest::new("shutdown-local-cancel", topic.clone())
                .expect("request")
                .with_options(
                    SubscribeOptions::builder()
                        .ordering_policy(OrderingPolicy::PerKey)
                        .build(),
                ),
            move |_| {
                let call = calls_by_handler.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    let _ = entered_tx.send(());
                    let _ = release_rx_by_handler
                        .lock()
                        .expect("release receiver lock")
                        .recv_timeout(Duration::from_secs(5));
                }
            },
        )
        .expect("subscription"),
    );

    let _ = bus
        .publish(
            PublishRequest::builder()
                .topic(topic.clone())
                .payload("active".to_owned())
                .ordering_key("same")
                .build()
                .expect("keyed request"),
        )
        .expect("publish active delivery");
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first handler is blocked");
    let _ = bus
        .publish(
            PublishRequest::builder()
                .topic(topic)
                .payload("queued".to_owned())
                .ordering_key("same")
                .build()
                .expect("keyed request"),
        )
        .expect("publish queued delivery");

    let (cancelled_tx, cancelled_rx) = mpsc::channel();
    let cancellation_handle = Arc::clone(&subscription);
    let canceller = thread::spawn(move || {
        let _ = cancelled_tx.send(cancellation_handle.cancel());
    });
    let remained_blocked = cancelled_rx.recv_timeout(Duration::from_millis(100)).is_err();

    // Always release the handler before assertions and before joining the caller.
    let _ = release_tx.send(());
    let cancel_result = cancelled_rx.recv_timeout(Duration::from_secs(2));
    canceller.join().expect("cancel thread exits");

    assert!(remained_blocked, "cancel waits for the running handler");
    cancel_result
        .expect("cancel completes after handler release")
        .expect("cancel closes local receiver");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "queued delivery is not handled");
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("bus shutdown completes");
}

fn run_graceful_shutdown_timeout_scenario() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
    let topic = Topic::<String>::new("shutdown.graceful-timeout").expect("topic");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let release_rx_by_handler = release_rx.clone();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("shutdown-graceful-timeout", topic.clone()).expect("request"),
            move |_| {
                let _ = entered_tx.send(());
                let _ = release_rx_by_handler
                    .lock()
                    .expect("release receiver lock")
                    .recv_timeout(Duration::from_secs(5));
            },
        )
        .expect("subscription");
    let _ = bus
        .publish(PublishRequest::new(topic, "blocked".to_owned()).expect("request"))
        .expect("publish blocked delivery");
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler is blocked");

    let first = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_millis(30),
    });
    let first_timed_out = matches!(first, Err(qubit_event_bus::error::ShutdownError::TimedOut { .. }));

    // Release before assertions so every failure path lets the worker finish.
    let _ = release_tx.send(());
    assert!(first_timed_out, "the short graceful observation times out");
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(2),
        })
        .expect("later graceful observation completes cleanup");
    assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
}
