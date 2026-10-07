// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public subscription-handle identity and cancellation behavior.

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[test]
fn test_subscription_handle_exposes_identity_and_repeated_cancel_is_safe() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local event bus starts");
    let expected_id = SubscriberId::new("handle-contract").expect("valid subscriber ID");
    let topic = Topic::<String>::new("subscription.handle").expect("valid topic");
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(expected_id.as_str(), topic).expect("validated subscriber ID"),
            |_| {},
        )
        .expect("subscription starts");

    let object_id = subscription.id();
    assert_eq!(&expected_id, subscription.subscriber_id());
    assert!(!subscription.is_cancelled());
    subscription.cancel().expect("first cancel joins worker");
    assert!(subscription.is_cancelled());
    assert_eq!(object_id, subscription.id());
    subscription
        .cancel()
        .expect("repeated cancel remains idempotent");
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("bus shuts down after explicit cancellation");
}
