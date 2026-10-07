// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public facade checks for publisher failure identity and cause retention.

use std::error::Error;

use qubit_event_bus::EventBus;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[test]
fn test_interceptor_panic_retains_public_failure_identity_and_origin_scope() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus starts");
    let options = PublishOptions::<String>::builder()
        .interceptor(|_| -> Result<Option<EventEnvelope<String>>, PublishError> {
            panic!("interceptor failed")
        })
        .build();
    let request = PublishRequest::new(
        Topic::new("pipeline.failure").unwrap(),
        "payload".to_owned(),
    )
    .unwrap()
    .with_options(options);
    let event_id = request.envelope().id().clone();

    let failure = bus.publish(request).unwrap_err();

    assert_eq!(failure.event_id(), &event_id);
    assert!(matches!(
        failure.cause(),
        PublishError::InterceptorPanicked { scope: "typed", .. }
    ));
    assert!(Error::source(failure.cause()).is_none());
    let _ = bus.shutdown(ShutdownMode::Immediate);
}
