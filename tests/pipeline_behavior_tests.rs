// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior checks for caller-supplied event identity in pipeline
//! failures.

use qubit_event_bus::EventBus;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[test]
fn test_caller_supplied_event_identity_survives_pipeline_failure() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus starts");
    let event_id = EventId::new("caller-identity").expect("caller event identity must be valid");
    let request = PublishRequest::builder()
        .topic(Topic::new("pipeline.failure.identity").expect("static test topic must be valid"))
        .payload("payload".to_owned())
        .event_id(event_id.clone())
        .options(PublishOptions::new())
        .build()
        .expect("caller request must be valid");
    let options = PublishOptions::<String>::builder()
        .interceptor(|_| -> Result<Option<EventEnvelope<String>>, PublishError> {
            panic!("interceptor failed")
        })
        .build();
    let request = request.with_options(options);

    let failure = bus
        .publish(request)
        .expect_err("interceptor panic must fail publication");

    assert_eq!(failure.event_id(), &event_id);
    assert!(matches!(
        failure.cause(),
        PublishError::InterceptorPanicked { .. }
    ));
    let _ = bus.shutdown(ShutdownMode::Immediate);
}
