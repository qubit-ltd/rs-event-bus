// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public delivery checks for retry directives and retry-policy gating.

use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_retry::RetryPolicy;

#[test]
fn test_retry_directive_retries_only_when_policy_is_configured() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus starts");
    let topic = Topic::<u32>::new("pipeline.failure-decision").unwrap();
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let options = SubscribeOptions::<u32>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .error_handler(|_, _| FailureDirective::Retry)
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("retry-policy-gated", topic.clone())
                .unwrap()
                .with_options(options),
            move |_: Delivery<u32>| {
                attempt_tx.send(()).unwrap();
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("retry this delivery")),
                })
            },
        )
        .unwrap();

    let _ = bus
        .publish(PublishRequest::new(topic, 7_u32).unwrap())
        .unwrap();

    attempt_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first handler attempt runs");
    attempt_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("Retry directive permits another attempt under the configured policy");

    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate);
}

#[test]
fn test_retry_directive_without_policy_does_not_schedule_another_attempt() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus starts");
    let topic = Topic::<u32>::new("pipeline.failure-decision.no-policy").unwrap();
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let options = SubscribeOptions::<u32>::builder()
        .error_handler(|_, _| FailureDirective::Retry)
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("retry-without-policy", topic.clone())
                .unwrap()
                .with_options(options),
            move |_: Delivery<u32>| {
                attempt_tx.send(()).unwrap();
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("retry requires a policy")),
                })
            },
        )
        .unwrap();

    let _ = bus
        .publish(PublishRequest::new(topic, 9_u32).unwrap())
        .unwrap();

    attempt_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first handler attempt runs");
    assert!(attempt_rx.recv_timeout(Duration::from_millis(100)).is_err());

    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate);
}
