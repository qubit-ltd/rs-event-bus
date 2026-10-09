// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Real handlers remain blocked until the application releases their gates.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use event_bus_documentation_consumer::bounded_shutdown::try_shutdown;
use event_bus_documentation_consumer::bounded_shutdown::try_shutdown_async;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use tokio::sync::Notify;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::timeout;

#[test]
fn test_sync_uncooperative_handler_returns_control_before_gate_release() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local bus");
    let topic = Topic::<String>::new("shutdown.gate").expect("topic");
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let release_receiver = Mutex::new(release_receiver);
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("gate", topic.clone()).expect("request"),
            move |_| {
                entered_sender.send(()).expect("notify handler entry");
                release_receiver
                    .lock()
                    .expect("gate lock")
                    .recv()
                    .expect("release handler gate");
            },
        )
        .expect("subscription");
    bus.publish(PublishRequest::new(topic, String::new()).expect("publication"))
        .expect("admission");
    entered_receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("handler entered");
    let (result_sender, result_receiver) = mpsc::channel();
    let shutdown_bus = bus.clone();
    let waiter = thread::spawn(move || {
        result_sender
            .send(try_shutdown(&shutdown_bus))
            .expect("report shutdown result");
    });
    // This watchdog prevents a broken example from hanging the suite. It is
    // deliberately not a scheduler-sensitive assertion about exact elapsed time.
    let result = result_receiver.recv_timeout(Duration::from_secs(30));
    release_sender
        .send(())
        .expect("always release handler gate");
    waiter.join().expect("shutdown thread");
    assert!(
        !result
            .expect("bounded caller returned")
            .expect("only deadline elapsed")
    );
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(30),
        })
        .expect("cleanup");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    drop(subscription);
}

#[test]
fn test_sync_idle_shutdown_completes() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local bus");
    assert!(try_shutdown(&bus).expect("shutdown"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_async_uncooperative_handler_returns_control_before_gate_release() {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new())
        .await
        .expect("local bus");
    let topic = Topic::<String>::new("shutdown.gate").expect("topic");
    let mut subscription = bus
        .subscribe(SubscribeRequest::new("gate", topic.clone()).expect("request"))
        .await
        .expect("subscription");
    let (entered_sender, mut entered_receiver) = unbounded_channel();
    let gate = Arc::new(Notify::new());
    let handler_gate = gate.clone();
    let runner = tokio::spawn(async move {
        subscription
            .run(move |_| {
                let entered_sender = entered_sender.clone();
                let gate = handler_gate.clone();
                async move {
                    entered_sender.send(()).expect("notify entry");
                    gate.notified().await;
                    Ok(())
                }
            })
            .await
    });
    bus.publish(PublishRequest::new(topic, String::new()).expect("publication"))
        .await
        .expect("admission");
    timeout(Duration::from_secs(30), entered_receiver.recv())
        .await
        .expect("handler entry timed out")
        .expect("handler exited before entry");
    let result = timeout(Duration::from_secs(30), try_shutdown_async(&bus)).await;
    gate.notify_one();
    assert!(
        !result
            .expect("bounded caller returned")
            .expect("only deadline elapsed")
    );
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(30),
        })
        .await
        .expect("cleanup");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    timeout(Duration::from_secs(30), runner)
        .await
        .expect("runner finishes")
        .expect("runner task")
        .expect("run result");
}

#[tokio::test(flavor = "current_thread")]
async fn test_async_idle_shutdown_completes() {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new())
        .await
        .expect("local bus");
    assert!(try_shutdown_async(&bus).await.expect("shutdown"));
}
