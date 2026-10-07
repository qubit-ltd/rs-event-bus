// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public facade contracts for terminal subscriber failures.

use std::io::Error;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_retry::RetryPolicy;

#[test]
fn test_terminal_handler_failure_is_forwarded_as_dead_letter_once() {
    let bus =
        EventBus::local(LocalEventBusConfig::new().queue_capacity(8)).expect("valid local event bus configuration");
    let dead_letter_topic = Topic::<DeadLetterEvent<String>>::new("failure.dead").expect("valid dead-letter topic");
    let (dead_letter_tx, dead_letter_rx) = mpsc::channel();
    let dead_letter_subscription = bus
        .subscribe(
            SubscribeRequest::new("failure-dead-letter-reader", dead_letter_topic)
                .expect("valid dead-letter subscriber ID"),
            move |delivery: Delivery<DeadLetterEvent<String>>| {
                let event = delivery.payload();
                dead_letter_tx
                    .send((
                        event.original_event().payload().clone(),
                        event.subscriber_id().as_str().to_owned(),
                        event.reason().to_owned(),
                    ))
                    .expect("dead-letter receiver remains available");
            },
        )
        .expect("dead-letter subscription starts");
    let (failure_tx, failure_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { attempts, .. } = diagnostic {
            let _ = failure_tx.send(*attempts);
        }
    });
    let source_topic = Topic::<String>::new("failure.source").expect("valid source topic");
    let options = SubscribeOptions::<String>::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(1)
                .build()
                .expect("valid retry policy"),
        )
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::with_topic_name("failure.dead").expect("valid dead-letter destination"))
        .build();
    let source_subscription = bus
        .subscribe(
            SubscribeRequest::new("failure-source", source_topic.clone())
                .expect("valid source subscriber ID")
                .with_options(options),
            |_: Delivery<String>| {
                Err(DeliveryError::Handler {
                    source: Box::new(Error::other("terminal handler failure")),
                })
            },
        )
        .expect("source subscription starts");

    let _ = bus
        .publish(PublishRequest::new(source_topic, "original payload".to_owned()).expect("valid source message"))
        .expect("source message publishes");

    let (payload, subscriber_id, reason) = dead_letter_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("terminal failure is forwarded to the dead-letter subscription");
    let attempts = failure_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("terminal failure emits a diagnostic");
    assert_eq!(payload, "original payload");
    assert_eq!(subscriber_id, "failure-source");
    assert!(!reason.is_empty());
    assert_eq!(attempts, 1);

    source_subscription.cancel().expect("source subscription cancels");
    dead_letter_subscription
        .cancel()
        .expect("dead-letter subscription cancels");
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("event bus shuts down");
}
