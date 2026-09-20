// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::EventBusResult;
use qubit_event_bus::EventEnvelope;
use qubit_event_bus::IntoPublisherInterceptorResult;
use qubit_event_bus::LocalEventBusFactory;
use qubit_event_bus::Topic;

#[test]
fn test_publisher_interceptor_entry_can_enrich_matching_payload_type() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(|event: EventEnvelope<String>| Some(event.with_header("seen", "true")))
        .expect("interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = Topic::<String>::try_new("publisher-interceptor").expect("topic should build");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    bus.subscribe("sub", &topic, move |event| {
        captured
            .lock()
            .expect("received headers should lock")
            .push(event.headers().get("seen").cloned());
    })
    .expect("subscription should succeed");
    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        received.lock().expect("received headers should lock").as_slice(),
        [Some("true".to_string())]
    );
}

#[test]
fn test_publisher_interceptor_result_conversions_preserve_envelopes() {
    let topic = Topic::<String>::try_new("publisher-result-conversions").expect("topic should build");
    let envelope = EventEnvelope::create(topic, "payload".to_owned());

    let direct = envelope
        .clone()
        .into_publisher_interceptor_result()
        .expect("direct envelope should convert")
        .expect("direct envelope should remain present");
    assert_eq!(direct.payload(), "payload");

    let result: EventBusResult<EventEnvelope<String>> = Ok(envelope);
    let converted = result
        .into_publisher_interceptor_result()
        .expect("successful result should convert")
        .expect("successful result should remain present");
    assert_eq!(converted.payload(), "payload");
}
