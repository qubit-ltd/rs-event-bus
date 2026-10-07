// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public synchronous event-bus construction contracts.

use std::sync::Arc;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::FacadeBuildError;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscriberNext;
use qubit_event_bus::spi::ShutdownMode;

use crate::support::fake_spi::FakeEventBusSpi;
use crate::support::fake_spi::full_capabilities;

#[test]
fn test_sync_facade_exposes_cached_provider_capabilities() {
    let provider_id = ProviderId::new("cached-sync").expect("valid provider ID");
    let spi = Arc::new(FakeEventBusSpi::with_capabilities(full_capabilities()));
    let bus = EventBus::from_spi(provider_id.clone(), spi.clone()).expect("facade constructs");
    let clone = bus.clone();

    assert_eq!(spi.capabilities_calls(), 1);
    assert_eq!(bus.capabilities(), full_capabilities());
    assert_eq!(bus.capabilities(), full_capabilities());
    assert_eq!(clone.capabilities(), full_capabilities());
    assert_eq!(bus.provider_id(), &provider_id);
    assert_eq!(clone.provider_id(), &provider_id);
    assert_eq!(spi.capabilities_calls(), 1);

    let _ = bus.shutdown(ShutdownMode::Immediate).expect("bus shuts down");

    assert_eq!(clone.capabilities(), full_capabilities());
    assert_eq!(clone.provider_id(), &provider_id);
    assert_eq!(spi.capabilities_calls(), 1);
}

/// Rejects an async middleware map before querying the synchronous SPI.
#[test]
fn test_sync_facade_rejects_async_middleware_before_querying_capabilities() {
    let spi = Arc::new(FakeEventBusSpi::with_capabilities(full_capabilities()));
    let config = EventBusFacadeConfig::new()
        .subscriber_interceptor(|delivery: Delivery<u64>, next: SubscriberNext<u64>| next(delivery))
        .async_subscriber_interceptor(|delivery: Delivery<u32>, next: AsyncSubscriberNext<u32>| next(delivery));

    let result = EventBus::with_config(
        ProviderId::new("invalid-sync").expect("valid provider ID"),
        spi.clone(),
        config,
    );

    assert!(matches!(
        result,
        Err(FacadeBuildError::Configuration(ConfigurationError::InvalidField {
            field: "async_subscriber_interceptor",
            ..
        }))
    ));
    assert_eq!(spi.capabilities_calls(), 0);
}
