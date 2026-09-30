// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public facade checks for publisher interceptor ordering and drop behavior.

use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::model::PublishMetadata;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[test]
fn test_typed_and_global_interceptors_run_in_order_before_drop() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let typed_order = order.clone();
    let global_order = order.clone();
    let config = EventBusFacadeConfig::new().publisher_interceptor(move |metadata: &mut PublishMetadata| {
        global_order.lock().unwrap().push("global");
        assert_eq!(metadata.header("origin"), Some("typed"));
        Ok(false)
    });
    let registry = EventBusRegistry::with_local().expect("local provider registers");
    let event_config = EventBusConfig::default()
        .with_provider_options(qubit_event_bus::local::LocalEventBusConfig::new().provider_options())
        .with_facade_config(config);
    let bus = registry
        .create(&event_config)
        .expect("local event bus accepts facade configuration");
    let options = PublishOptions::<u32>::builder()
        .interceptor(move |mut envelope| {
            typed_order.lock().unwrap().push("typed");
            envelope.set_header("origin", "typed").expect("valid header");
            Ok(Some(envelope))
        })
        .build();

    let receipt = bus
        .publish(
            PublishRequest::new(Topic::new("pipeline.publisher").unwrap(), 17_u32)
                .unwrap()
                .with_options(options),
        )
        .unwrap();

    assert!(receipt.acknowledgement().is_dropped());
    assert_eq!(*order.lock().unwrap(), ["typed", "global"]);
    assert_eq!(bus.publish_metrics().dropped, 1);
    let _ = bus.shutdown(ShutdownMode::Immediate);
}
