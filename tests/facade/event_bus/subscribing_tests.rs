// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public subscription creation and delivery contracts.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

use crate::support::fake_spi::FakeEventBusSpi;
use crate::support::fake_spi::inbound_message;

#[test]
fn test_subscribe_starts_worker_and_delivers_provider_message() {
    let provider = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(
        ProviderId::new("subscribe-contract").expect("valid provider ID"),
        provider.clone(),
    )
    .expect("provider capabilities are valid");
    let topic = Topic::<u32>::new("test.topic").expect("valid topic");
    let (delivery_tx, delivery_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("subscribe-contract", topic).expect("valid subscriber ID"),
            move |delivery: Delivery<u32>| {
                delivery_tx
                    .send(*delivery.payload())
                    .expect("delivery receiver remains available");
            },
        )
        .expect("subscription starts");

    provider.enqueue(inbound_message(None));

    assert_eq!(
        delivery_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("provider message reaches the handler"),
        42
    );
    subscription.cancel().expect("subscription cancels");
    let _ = bus
        .shutdown(ShutdownMode::Immediate)
        .expect("event bus shuts down");
}
