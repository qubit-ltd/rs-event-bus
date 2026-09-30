// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sync handler fairness while one ordering lane consumes the old admission
//! budget.

mod support;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use support::settlement_probe::CancelOnDrop;
use support::settlement_probe::Gate;
use support::settlement_probe::ProbeBus;
use support::settlement_probe::SettlementProbe;
use support::settlement_probe::Signal;

/// Publishes a keyed payload using the production facade.
fn publish(bus: &EventBus, topic: &str, payload: &str, key: &str) {
    let _ = bus
        .publish(
            PublishRequest::builder()
                .topic(Topic::<String>::new(topic).expect("valid topic"))
                .payload(payload.to_owned())
                .ordering_key(key)
                .build()
                .expect("valid keyed request"),
        )
        .expect("publish succeeds");
}

/// Exercises either one subscription with two keys or two separate
/// subscriptions.
fn assert_hot_lane_does_not_block_independent_handler(two_subscriptions: bool) {
    let spi = ProbeBus::new();
    let hot = SettlementProbe::new(true, 0);
    spi.register("hot", hot.clone());
    let independent = SettlementProbe::new(true, 0);
    if two_subscriptions {
        spi.register("independent", independent);
    }
    let bus = EventBus::from_spi(ProviderId::new("probe").expect("valid provider"), spi).expect("valid capabilities");
    let blocked = Arc::new(Gate::default());
    let barrier = Arc::new(Signal::default());
    let callback_gate = blocked.clone();
    let callback_barrier = barrier.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let same_subscription_tx = entered_tx.clone();
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let hot_subscription = bus
        .subscribe(
            SubscribeRequest::new("hot", Topic::<String>::new("hot").expect("valid topic"))
                .expect("valid subscriber")
                .with_options(options.clone()),
            move |delivery: Delivery<String>| {
                if delivery.payload() == "A0" {
                    callback_barrier.enter();
                    callback_gate.wait();
                } else if delivery.payload() == "B0" {
                    let _ = same_subscription_tx.send(());
                }
            },
        )
        .expect("hot subscription starts");
    let independent_subscription = if two_subscriptions {
        Some(
            bus.subscribe(
                SubscribeRequest::new("independent", Topic::<String>::new("independent").expect("valid topic"))
                    .expect("valid subscriber")
                    .with_options(options),
                move |_: Delivery<String>| {
                    let _ = entered_tx.send(());
                },
            )
            .expect("independent subscription starts"),
        )
    } else {
        None
    };
    let mut cleanup = vec![&hot_subscription];
    if let Some(subscription) = independent_subscription.as_ref() {
        cleanup.push(subscription);
    }
    let _cancel_on_unwind = CancelOnDrop(cleanup);
    // Declared after the bus, this guard releases A0 even if an assertion unwinds.
    let _release_on_unwind = blocked.release_on_drop();
    publish(&bus, "hot", "A0", "A");
    assert!(barrier.wait(1), "A0 enters handler at explicit startup barrier");
    for payload in ["A1", "A2", "A3", "A4"] {
        publish(&bus, "hot", payload, "A");
    }
    let received_hot = hot.received.wait(5);
    if received_hot {
        publish(&bus, if two_subscriptions { "independent" } else { "hot" }, "B0", "B");
    }
    let independent_entered = received_hot && entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    // Release before joining: failure cannot deadlock on the still-running handler.
    blocked.release();
    hot_subscription.cancel().expect("hot cleanup");
    if let Some(subscription) = independent_subscription.as_ref() {
        subscription.cancel().expect("independent cleanup");
    }
    assert!(
        received_hot,
        "fake receive gate confirms A0 through A4 are owned before B0 is published"
    );
    assert!(
        independent_entered,
        "B0 must enter its handler while A0 is blocked; queued same-key work must not exhaust the running budget"
    );
}

#[test]
fn test_sync_hot_key_preserves_independent_key_progress() {
    assert_hot_lane_does_not_block_independent_handler(false);
}

#[test]
fn test_sync_hot_subscription_preserves_independent_subscription_progress() {
    assert_hot_lane_does_not_block_independent_handler(true);
}
