// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::mpsc;

use qubit_event_bus::DeadLetterOutcome;
use qubit_event_bus::EventBusError;
use qubit_event_bus::LocalEventBus;
use qubit_event_bus::PublishOutcome;
use qubit_event_bus::Topic;

#[test]
fn test_delivery_failure_exposes_terminal_delivery_context() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = Topic::<String>::try_new("delivery-failure-contract").expect("topic should build");
    let (sender, receiver) = mpsc::channel();
    bus.add_delivery_failure_observer(move |failure| {
        sender
            .send(failure.clone())
            .expect("failure observer should send report");
    })
    .expect("failure observer should register");
    let subscription = bus
        .subscribe("failing-subscriber", &topic, |_event| {
            Err(EventBusError::handler_failed("terminal failure"))
        })
        .expect("subscription should register");

    let receipt = bus
        .publish(&topic, "payload".to_owned())
        .expect("publish should be admitted");
    bus.wait_for_idle(&topic).expect("delivery should complete");
    let failure = receiver.try_recv().expect("failure report should be emitted");

    assert_eq!(receipt.input_event_id(), receipt.dispatched_event_id().unwrap());
    let PublishOutcome::Dispatched(deliveries) = receipt.outcome() else {
        panic!("failure delivery should have been dispatched");
    };
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].subscription_id(), failure.subscription_id());
    assert_eq!(deliveries[0].subscriber_id(), failure.subscriber_id());
    assert_eq!(failure.event_id(), receipt.dispatched_event_id().unwrap());
    assert_eq!(failure.topic_name(), topic.name());
    assert!(failure.subscription_id() > 0);
    assert_eq!(failure.subscriber_id(), "failing-subscriber");
    assert_eq!(failure.subscriber_id(), subscription.subscriber_id());
    assert_eq!(failure.error(), &EventBusError::handler_failed("terminal failure"));
    assert!(!failure.acknowledged_by_error_handler());
    assert_eq!(failure.dead_letter(), &DeadLetterOutcome::NotConfigured);
}
