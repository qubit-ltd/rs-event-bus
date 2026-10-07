// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public contracts of the synchronous `EventBus` facade.

use qubit_event_bus::EventBus;
use qubit_event_bus::PublishError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[test]
fn test_event_bus_clones_share_shutdown_state() {
    let bus = EventBus::local(LocalEventBusConfig::default()).expect("local event bus is valid");
    let shutdown_handle = bus.clone();

    let _ = shutdown_handle
        .shutdown(ShutdownMode::Immediate)
        .expect("clone shuts down the shared event bus");

    let request = PublishRequest::new(
        Topic::<String>::new("facade.clone").expect("topic is valid"),
        "after shutdown".to_owned(),
    )
    .expect("publish request is valid");
    let failure = bus.publish(request).expect_err("shutdown clone closes bus");
    assert!(
        matches!(failure.cause(), PublishError::Closed),
        "publishing after shutdown should report a closed event bus"
    );
}
