// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::any::TypeId;
use std::any::type_name;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use qubit_event_bus::SubscriberId;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::GapPolicy;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

/// Payload deliberately lacking `Clone` to verify shared event ownership.
struct NonClonePayload(String);

const STATIC_TOPIC: Topic<NonClonePayload> = Topic::new_static("events.static");

/// Hashes a topic with the standard default hasher for equality contracts.
fn topic_hash<T: 'static>(topic: &Topic<T>) -> u64 {
    let mut hasher = DefaultHasher::new();
    topic.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn test_static_topic_constants_preserve_typed_topic_identity() -> Result<(), Box<dyn Error>> {
    let runtime_static_topic = Topic::<NonClonePayload>::new_static("events.static");
    let runtime_topic = Topic::<NonClonePayload>::new("events.static")?;
    assert_eq!(STATIC_TOPIC.name(), "events.static");
    assert_eq!(runtime_static_topic, STATIC_TOPIC);
    assert_eq!(
        STATIC_TOPIC.payload_type_id(),
        TypeId::of::<NonClonePayload>()
    );
    assert_eq!(
        STATIC_TOPIC.payload_type_name(),
        type_name::<NonClonePayload>()
    );
    assert_eq!(STATIC_TOPIC, runtime_topic);
    assert_eq!(topic_hash(&STATIC_TOPIC), topic_hash(&runtime_topic));
    assert_eq!(STATIC_TOPIC.clone(), runtime_topic);
    Ok(())
}

#[test]
fn test_subscribe_request_new_validates_string_id_and_takes_topic_by_value()
-> Result<(), Box<dyn Error>> {
    let request = SubscribeRequest::new("audit-log", STATIC_TOPIC)?;
    assert_eq!(request.subscriber_id().as_str(), "audit-log");
    assert_eq!(request.topic(), &STATIC_TOPIC);
    assert!(matches!(
        SubscribeRequest::new("_audit", STATIC_TOPIC),
        Err(ConfigurationError::InvalidSubscriberId { value })
            if value.as_ref() == "_audit"
    ));
    assert_eq!(STATIC_TOPIC.name(), "events.static");
    Ok(())
}

#[test]
fn test_gap_policy_defaults_to_stop_and_can_be_overridden() -> Result<(), Box<dyn Error>> {
    let request = SubscribeRequest::new("gap-check", Topic::<String>::new("events.gap")?)?;
    assert_eq!(request.options().gap_policy(), GapPolicy::Stop);
    assert_eq!(
        SubscribeOptions::<String>::builder()
            .gap_policy(GapPolicy::Continue)
            .build()
            .gap_policy(),
        GapPolicy::Continue
    );
    let request = SubscribeRequest::builder()
        .subscriber_id(SubscriberId::new("gap-check")?)
        .topic(Topic::<String>::new("events.gap")?)
        .gap_policy(GapPolicy::Continue)
        .build()?;
    assert_eq!(request.options().gap_policy(), GapPolicy::Continue);
    Ok(())
}

#[test]
fn test_generated_event_ids_are_uuid_v4_values() -> Result<(), Box<dyn Error>> {
    let envelope = EventEnvelope::new(
        Topic::<String>::new("orders.created")?,
        "payload".to_owned(),
    )?;
    let id = envelope.id().as_str();
    assert_eq!(id.len(), 36);
    assert_eq!(&id[8..9], "-");
    assert_eq!(&id[13..14], "-");
    assert_eq!(&id[18..19], "-");
    assert_eq!(&id[23..24], "-");
    assert_eq!(&id[14..15], "4");
    assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"));
    Ok(())
}

#[test]
fn test_custom_event_ids_keep_validation_and_do_not_require_random_generation()
-> Result<(), Box<dyn Error>> {
    assert!(EventId::new("").is_err());
    assert!(EventId::new(" invalid").is_err());
    assert_eq!(EventId::new("caller-event-1")?.as_str(), "caller-event-1");
    Ok(())
}

#[test]
fn test_caller_cannot_forge_reserved_dead_letter_header() -> Result<(), Box<dyn Error>> {
    let mut envelope =
        EventEnvelope::new(Topic::<String>::new("events.header")?, "payload".to_owned())?;
    assert!(
        envelope
            .set_header("X-Qubit-Event-Bus-Dead-Letter", "v1")
            .is_err()
    );
    assert!(
        envelope
            .remove_header("X-Qubit-Event-Bus-Dead-Letter")
            .is_err()
    );
    assert!(
        PublishRequest::builder()
            .topic(Topic::<String>::new("events.header")?)
            .payload("payload".to_owned())
            .header("x-qubit-event-bus-dead-letter", "v1")
            .build()
            .is_err()
    );
    Ok(())
}

#[test]
fn test_subscriber_id_enforces_portable_syntax() {
    assert!(SubscriberId::new("audit-1:primary").is_ok());
    assert!(SubscriberId::new("").is_err());
    assert!(SubscriberId::new(" audit").is_err());
    assert!(SubscriberId::new("_audit").is_err());
    assert!(SubscriberId::new("审计").is_err());
    assert!(SubscriberId::new("a".repeat(129)).is_err());
}

#[test]
fn test_shared_event_payload_preserves_arc_identity_without_clone_bounds()
-> Result<(), Box<dyn Error>> {
    let payload = Arc::new(NonClonePayload("shared".into()));
    let event = EventEnvelope::from_shared_payload(
        Topic::<NonClonePayload>::new("events.shared")?,
        payload.clone(),
    )?;
    assert_eq!(event.payload().0, "shared");
    assert!(Arc::ptr_eq(&event.clone().into_payload(), &payload));

    let custom_id = EventId::new("caller-event")?;
    let event = EventEnvelope::with_id_and_shared_payload(
        Topic::<NonClonePayload>::new("events.shared")?,
        payload.clone(),
        custom_id,
    );
    assert_eq!(event.id().as_str(), "caller-event");
    assert!(Arc::ptr_eq(&event.into_payload(), &payload));
    Ok(())
}
