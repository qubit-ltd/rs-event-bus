// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal subscription cancellation lifecycle contracts.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

const LIMIT: Duration = Duration::from_secs(5);

#[test]
fn test_cancel_waits_for_started_handler_to_finish() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
    let topic = Topic::<String>::new("subscription.cancel").expect("topic");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let subscription = Arc::new(
        bus.subscribe(
            SubscribeRequest::new("cancel-handler", topic.clone()).expect("request"),
            move |_| {
                entered_tx.send(()).expect("entered signal");
                release_rx
                    .lock()
                    .expect("release receiver lock")
                    .recv()
                    .expect("test releases handler");
            },
        )
        .expect("subscription"),
    );
    bus.publish(PublishRequest::new(topic, "payload".to_owned()).expect("publish request"))
        .expect("publish");
    entered_rx
        .recv_timeout(LIMIT)
        .expect("handler entered before cancellation");

    let (cancelled_tx, cancelled_rx) = mpsc::channel();
    let cancellation_handle = Arc::clone(&subscription);
    let canceller = thread::spawn(move || {
        cancelled_tx
            .send(cancellation_handle.cancel())
            .expect("cancellation result receiver");
    });
    let deadline = Instant::now() + LIMIT;
    while !subscription.is_cancelled() && Instant::now() < deadline {
        thread::yield_now();
    }
    assert!(subscription.is_cancelled(), "cancel request becomes visible");
    assert!(cancelled_rx.try_recv().is_err(), "cancel waits for the active handler");

    release_tx.send(()).expect("release blocked handler");
    cancelled_rx
        .recv_timeout(LIMIT)
        .expect("cancel completes after handler release")
        .expect("worker closes successfully");
    canceller.join().expect("cancellation thread exits");
    bus.shutdown(ShutdownMode::Immediate).expect("bus shutdown completes");
}
