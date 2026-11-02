// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Facade ownership contracts for shared provider SPI instances.

mod support;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::local::AsyncLocalEventBusProvider;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ServiceProvider;

#[test]
fn test_sync_facade_clone_shares_lifecycle_and_subscription_identity_space() {
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok("sync") {
        support::isolated_process::run_case(
            "test_sync_facade_clone_shares_lifecycle_and_subscription_identity_space",
            "sync",
        );
        return;
    }
    let spi = LocalEventBusProvider
        .create_configured(&EventBusConfig::default())
        .expect("local provider is configured");
    let bus = EventBus::from_spi(ProviderId::new("shared-sync").expect("provider id"), spi).expect("facade constructs");
    let clone = bus.clone();
    let topic = Topic::<u32>::new("ownership.sync").expect("topic");
    let first = bus
        .subscribe(SubscribeRequest::new("first", topic.clone()).expect("request"), |_| {
            Ok(())
        })
        .expect("first subscription");
    let second = clone
        .subscribe(SubscribeRequest::new("second", topic).expect("request"), |_| Ok(()))
        .expect("second subscription");
    assert_ne!(first.id(), second.id());
    first.cancel().expect("first subscription cancels");
    second.cancel().expect("second subscription cancels");
    let _ = bus.shutdown(ShutdownMode::Immediate).expect("shared bus shuts down");
    assert!(matches!(
        clone.publish(
            qubit_event_bus::model::PublishRequest::new(Topic::<u32>::new("ownership.sync").expect("topic"), 1,)
                .expect("publish request")
        ),
        Err(failure) if matches!(failure.cause(), PublishError::Closed)
    ));
}

#[test]
fn test_async_facade_clone_shares_lifecycle_and_subscription_identity_space() {
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok("async") {
        support::isolated_process::run_case(
            "test_async_facade_clone_shares_lifecycle_and_subscription_identity_space",
            "async",
        );
        return;
    }
    let config = EventBusConfig::default();
    let spi = support::manual_async::block_on(AsyncLocalEventBusProvider.create_configured(&config))
        .expect("async local provider is configured");
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("shared-async").expect("provider id"), spi).expect("facade constructs");
    let clone = bus.clone();
    let topic = Topic::<u32>::new("ownership.async").expect("topic");
    let mut first =
        support::manual_async::block_on(bus.subscribe(SubscribeRequest::new("first", topic.clone()).expect("request")))
            .expect("first subscription");
    let mut second =
        support::manual_async::block_on(clone.subscribe(SubscribeRequest::new("second", topic).expect("request")))
            .expect("second subscription");
    assert_ne!(first.id(), second.id());
    support::manual_async::block_on(first.close()).expect("first subscription closes");
    support::manual_async::block_on(second.close()).expect("second subscription closes");
    let _ = support::manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shared bus shuts down");
    assert!(matches!(
        support::manual_async::block_on(
            clone.publish(
                qubit_event_bus::model::PublishRequest::new(Topic::<u32>::new("ownership.async").expect("topic"), 1,)
                    .expect("publish request")
            )
        ),
        Err(failure) if matches!(failure.cause(), PublishError::Closed)
    ));
}
