// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous subscription capacity contract tests.

use std::num::NonZeroUsize;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::error::SubscribeError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_spi::ServiceProvider;

fn limit(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test capacities must be positive")
}

#[test]
fn subscription_capacity_rejects_at_limit_and_reuses_cancelled_slot() {
    let spi = LocalEventBusProvider
        .create_configured(&EventBusConfig::default())
        .expect("local provider configuration is valid");
    let scheduling = DeliverySchedulingConfig::new(limit(8), limit(16), limit(8), limit(8))
        .expect("scheduling capacities are valid");
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(scheduling);
    let bus = EventBus::with_config(
        ProviderId::new("subscription-capacity-test").expect("provider ID is valid"),
        spi,
        config,
    )
    .expect("local SPI capabilities are valid");
    let topic = Topic::<u32>::new("subscription.capacity").expect("topic is valid");

    let mut subscriptions = (0..8)
        .map(|index| {
            bus.subscribe(
                SubscribeRequest::new(&format!("capacity-{index}"), topic.clone())
                    .expect("request is valid"),
                |_: Delivery<u32>| (),
            )
            .expect("subscription fits under the capacity limit")
        })
        .collect::<Vec<_>>();

    let rejected = bus.subscribe(
        SubscribeRequest::new("over-capacity", topic.clone()).expect("request is valid"),
        |_: Delivery<u32>| (),
    );
    assert!(matches!(
        rejected,
        Err(SubscribeError::ResourceLimit {
            resource: "subscriptions",
            limit: 8,
        })
    ));

    subscriptions
        .remove(0)
        .cancel()
        .expect("cancellation releases its slot");
    let replacement = bus
        .subscribe(
            SubscribeRequest::new("capacity-reused", topic).expect("request is valid"),
            |_: Delivery<u32>| (),
        )
        .expect("released capacity can be reused");
    subscriptions.push(replacement);

    for subscription in subscriptions {
        subscription
            .cancel()
            .expect("subscription cancellation succeeds");
    }
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: std::time::Duration::from_secs(2),
        })
        .expect("shutdown succeeds");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
}
