// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public dead-letter payload behavior tests.

use std::io::Error as IoError;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::error::DeliveryError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

struct NonClonePayload(u8);

#[test]
fn dead_letter_topic_exposes_original_payload_and_terminal_reason() {
    let bus = EventBus::local(LocalEventBusConfig::new().queue_capacity(4)).expect("local bus");
    let source = Topic::<NonClonePayload>::new("model.dead.source").expect("valid topic");
    let dead_topic = Topic::<DeadLetterEvent<NonClonePayload>>::new("model.dead.records").expect("valid topic");
    let (sender, receiver) = mpsc::channel();
    let dead_subscription = bus
        .subscribe(
            SubscribeRequest::new("model-dead-reader", dead_topic).expect("valid request"),
            move |delivery| {
                let dead = delivery.payload();
                sender
                    .send((dead.original_event().payload().0, dead.reason().to_owned()))
                    .expect("receiver remains available");
            },
        )
        .expect("dead-letter subscription");
    let source_subscription = bus
        .subscribe(
            SubscribeRequest::builder()
                .subscriber_id(SubscriberId::new("model-dead-source").expect("valid subscriber ID"))
                .topic(source.clone())
                .error_handler(|_, _| FailureDirective::DeadLetter)
                .dead_letter(DeadLetterPolicy::with_topic_name("model.dead.records").expect("valid policy"))
                .build()
                .expect("valid request"),
            |_| -> Result<(), DeliveryError> {
                Err(DeliveryError::Handler {
                    source: Box::new(IoError::other("model dead-letter failure")),
                })
            },
        )
        .expect("source subscription");

    let _ = bus
        .publish(
            PublishRequest::builder()
                .topic(source)
                .payload(NonClonePayload(9))
                .build()
                .expect("valid publication"),
        )
        .expect("publication accepted");

    let (payload, reason) = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("dead letter delivered");
    assert_eq!(payload, 9);
    assert!(reason.contains("model dead-letter failure"));
    source_subscription.cancel().expect("source cancellation");
    dead_subscription.cancel().expect("dead-letter cancellation");
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("bus shutdown");
}
