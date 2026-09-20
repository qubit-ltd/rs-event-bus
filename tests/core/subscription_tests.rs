// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_event_bus::LocalEventBus;
use qubit_event_bus::SubscriptionHandle;
use qubit_event_bus::Topic;

#[test]
fn test_subscription_exposes_id_topic_options_and_active_state() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = Topic::<String>::try_new("subscription").expect("topic should build");
    let subscription = bus
        .subscribe("sub-1", &topic, |_| ())
        .expect("subscription should be created");

    assert_eq!(subscription.subscriber_id(), "sub-1");
    assert_eq!(subscription.topic(), &topic);
    assert_eq!(subscription.options().priority(), 0);
    assert!(subscription.is_active());

    let handle: &dyn SubscriptionHandle<String> = &subscription;
    assert_eq!(handle.subscriber_id(), "sub-1");
    assert_eq!(handle.topic(), &topic);
    assert_eq!(handle.options().priority(), 0);
    assert!(handle.is_active());
    handle.cancel().expect("cancel should succeed");
    assert!(!handle.is_active());
}
