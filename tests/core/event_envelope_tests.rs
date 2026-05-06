/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for event envelopes.

use std::collections::HashMap;
use std::time::{
    Duration,
    SystemTime,
};

use qubit_event_bus::{
    Acknowledgement,
    EventBusError,
    EventEnvelope,
    Topic,
};

fn create_topic() -> Topic<String> {
    Topic::try_new("orders.created").expect("topic should build")
}

#[test]
fn test_create_sets_required_metadata() {
    let topic = create_topic();
    let envelope = EventEnvelope::create(topic.clone(), "payload".to_string());

    assert!(!envelope.id().is_empty());
    assert_eq!(envelope.topic(), &topic);
    assert_eq!(envelope.payload(), "payload");
    assert!(envelope.headers().is_empty());
    assert_eq!(envelope.ordering_key(), None);
    assert_eq!(envelope.delay(), None);
    assert!(!envelope.is_dead_letter());
    assert!(envelope.acknowledgement().is_none());
    assert!(envelope.timestamp() <= SystemTime::now());
}

#[test]
fn test_builder_sets_optional_metadata() {
    let topic = create_topic();
    let envelope = EventEnvelope::builder()
        .id("event-1")
        .topic(topic.clone())
        .payload("payload".to_string())
        .header("trace-id", "trace-1")
        .ordering_key("order-1")
        .delay(Duration::from_millis(25))
        .dead_letter(true)
        .build()
        .expect("complete envelope should build");

    assert_eq!(envelope.id(), "event-1");
    assert_eq!(envelope.topic(), &topic);
    assert_eq!(
        envelope.headers().get("trace-id"),
        Some(&"trace-1".to_string())
    );
    assert_eq!(envelope.ordering_key(), Some("order-1"));
    assert_eq!(envelope.delay(), Some(Duration::from_millis(25)));
    assert!(envelope.is_dead_letter());
}

#[test]
fn test_builder_sets_headers_timestamp_and_acknowledgement() {
    let topic = create_topic();
    let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let acknowledgement = Acknowledgement::default();
    let mut headers = HashMap::new();
    headers.insert("trace-id".to_string(), "trace-3".to_string());

    let envelope = EventEnvelope::builder()
        .id("event-2")
        .topic(topic)
        .payload("payload".to_string())
        .headers(headers)
        .timestamp(timestamp)
        .acknowledgement(acknowledgement.clone())
        .build()
        .expect("complete envelope should build");

    assert_eq!(envelope.timestamp(), timestamp);
    assert_eq!(
        envelope.headers().get("trace-id"),
        Some(&"trace-3".to_string())
    );
    assert!(
        !envelope
            .acknowledgement()
            .expect("ack should be present")
            .is_completed()
    );
}

#[test]
fn test_builder_rejects_missing_topic() {
    let error = EventEnvelope::<String>::builder()
        .payload("payload".to_string())
        .build()
        .expect_err("topic is required");

    assert_eq!(error, EventBusError::missing_field("topic"));
}

#[test]
fn test_builder_rejects_missing_payload_and_blank_id() {
    let topic = create_topic();

    let missing_payload = EventEnvelope::<String>::builder()
        .topic(topic)
        .build()
        .expect_err("payload is required");
    let blank_id = EventEnvelope::builder()
        .id(" ")
        .topic(create_topic())
        .payload("payload".to_string())
        .build()
        .expect_err("id must be non-blank");

    assert_eq!(missing_payload, EventBusError::missing_field("payload"));
    assert_eq!(
        blank_id,
        EventBusError::invalid_argument("id", "event id must not be blank")
    );
}

#[test]
fn test_with_methods_return_modified_envelope() {
    let topic = create_topic();
    let envelope = EventEnvelope::create(topic, "payload".to_string())
        .with_header("trace-id", "trace-2")
        .with_ordering_key("order-2")
        .with_delay(Duration::from_secs(1))
        .as_dead_letter();

    assert_eq!(
        envelope.headers().get("trace-id"),
        Some(&"trace-2".to_string())
    );
    assert_eq!(envelope.ordering_key(), Some("order-2"));
    assert_eq!(envelope.delay(), Some(Duration::from_secs(1)));
    assert!(envelope.is_dead_letter());
}
