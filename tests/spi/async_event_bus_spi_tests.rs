// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Contract checks for asynchronous event-bus SPI implementations.

use std::any::TypeId;
use std::sync::Arc;
use std::time::SystemTime;

use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

#[test]
fn test_async_spi_provider_id_defaults_to_unassigned() {
    let spi: Arc<dyn AsyncEventBusSpi> = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).expect("valid local configuration"),
    );

    assert_eq!(None, spi.provider_id());
}

#[test]
fn test_async_spi_capabilities_remain_stable_for_instance_lifetime() {
    let spi: Arc<dyn AsyncEventBusSpi> = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).expect("valid local configuration"),
    );

    let initial = spi.capabilities();
    let later = spi.capabilities();

    assert_eq!(initial, later);
}

#[test]
fn test_async_spi_object_safe_operations_return_send_futures() {
    fn assert_send<T: Send>(_: &T) {}

    let spi: Arc<dyn AsyncEventBusSpi> = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).expect("valid local configuration"),
    );
    let message = OutboundMessage::new(
        TopicAddress::new("spi.contract").expect("valid topic address"),
        EventId::new("event-contract").expect("valid event ID"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(1_u32)),
    );
    let request = SpiSubscriptionRequest::new(
        Id::new(1),
        TopicAddress::new("spi.contract").expect("valid topic address"),
        SubscriberId::new("spi-contract").expect("valid subscriber ID"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    );

    let publish = spi.publish(message);
    let subscribe = spi.subscribe(request);
    let shutdown = spi.shutdown(ShutdownMode::Immediate);

    assert_send(&publish);
    assert_send(&subscribe);
    assert_send(&shutdown);
}
