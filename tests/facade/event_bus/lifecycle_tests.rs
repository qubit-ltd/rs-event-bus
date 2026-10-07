// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public event-bus lifecycle wait contracts.

use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[test]
fn test_wait_for_idle_from_owned_handler_returns_would_deadlock() {
    let bus = EventBus::local(LocalEventBusConfig::default())
        .expect("valid local event bus configuration");
    let topic = Topic::<String>::new("lifecycle.wait-idle-reentrant").expect("valid topic");
    let callback_bus = bus.clone();
    let callback_topic = topic.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("lifecycle-wait-idle", topic.clone())
                .expect("valid subscriber request"),
            move |_| {
                result_tx
                    .send(callback_bus.wait_for_idle(&callback_topic, Some(Duration::from_secs(1))))
                    .expect("test receiver remains available");
            },
        )
        .expect("subscription starts");

    let _ = bus
        .publish(PublishRequest::new(topic, "work".to_owned()).expect("valid publish request"))
        .expect("publish succeeds");

    assert!(matches!(
        result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("handler reports its wait result"),
        Err(LifecycleError::WouldDeadlock {
            operation: "wait_for_idle"
        })
    ));
    subscription.cancel().expect("subscription cancels");
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down");
}
