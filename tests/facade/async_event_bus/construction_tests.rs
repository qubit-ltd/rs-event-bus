// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public asynchronous event-bus construction contracts.

use std::sync::Arc;

use qubit_clock::MonotonicClock;
use qubit_clock::StdMonotonicClock;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::manual_async::block_on;

#[test]
fn test_async_facade_capability_panic_is_a_terminal_spi_error() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.panic_on_capabilities();
    let result =
        AsyncEventBus::from_spi(ProviderId::new("panic-capabilities").unwrap(), spi.clone());
    match result {
        Err(SpiError::Operation {
            operation: "capabilities",
            kind: "provider_panicked",
            retryable: Some(false),
            ..
        }) => {}
        Err(error) => panic!("unexpected SPI error: {error}"),
        Ok(_) => panic!("capabilities panic must fail facade construction"),
    }
    assert_eq!(spi.capabilities_calls(), 1);
}

#[test]
fn test_async_facade_constructors_initialize_provider_capabilities_once() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let timer = StdMonotonicClock::new().new_timer();

    let buses = [
        AsyncEventBus::from_spi(ProviderId::new("from-spi").unwrap(), spi.clone())
            .expect("from_spi constructs the facade"),
        AsyncEventBus::with_config(
            ProviderId::new("with-config").unwrap(),
            spi.clone(),
            EventBusFacadeConfig::default(),
        )
        .expect("with_config constructs the facade"),
        AsyncEventBus::with_timer(
            ProviderId::new("with-timer").unwrap(),
            spi.clone(),
            timer.clone(),
        )
        .expect("with_timer constructs the facade"),
        AsyncEventBus::with_config_and_timer(
            ProviderId::new("with-config-and-timer").unwrap(),
            spi.clone(),
            EventBusFacadeConfig::default(),
            timer,
        )
        .expect("with_config_and_timer constructs the facade"),
    ];

    assert_eq!(buses.len(), 4);
    assert_eq!(spi.capabilities_calls(), 4);
}

#[test]
fn test_async_facade_exposes_cached_provider_capabilities() {
    let provider_id = ProviderId::new("cached-async").expect("valid provider ID");
    let capabilities = crate::support::fake_spi::full_capabilities();
    let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(capabilities));
    let bus = AsyncEventBus::from_spi(provider_id.clone(), spi.clone()).expect("facade constructs");
    let clone = bus.clone();

    assert_eq!(spi.capabilities_calls(), 1);
    assert_eq!(bus.capabilities(), capabilities);
    assert_eq!(bus.capabilities(), capabilities);
    assert_eq!(clone.capabilities(), capabilities);
    assert_eq!(bus.provider_id(), &provider_id);
    assert_eq!(clone.provider_id(), &provider_id);
    assert_eq!(spi.capabilities_calls(), 1);

    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("bus shuts down");

    assert_eq!(clone.capabilities(), capabilities);
    assert_eq!(clone.provider_id(), &provider_id);
    assert_eq!(spi.capabilities_calls(), 1);
}

#[test]
fn test_async_local_constructor_returns_a_shutdown_capable_facade() {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::default()))
        .expect("local facade constructs");
    let outcome = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("local facade shuts down");

    assert_eq!(outcome.outcome, ShutdownOutcome::Complete);
}
