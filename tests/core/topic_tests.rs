/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Tests for type-safe topics.

use std::any::TypeId;
use std::collections::HashSet;

use qubit_event_bus::{EventBusError, Topic};

#[test]
fn test_try_new_creates_type_safe_topic() {
    let topic = Topic::<String>::try_new("orders.created").expect("topic name should be accepted");

    assert_eq!(topic.name(), "orders.created");
    assert_eq!(topic.payload_type_id(), TypeId::of::<String>());
    assert!(topic.payload_type_name().contains("String"));
    assert!(topic.to_string().ends_with(".orders.created"));
}

#[test]
fn test_try_new_rejects_blank_topic_name() {
    let error = Topic::<String>::try_new("  ").expect_err("blank topic name should be rejected");

    assert_eq!(
        error,
        EventBusError::invalid_argument("name", "topic name must not be blank")
    );
}

#[test]
fn test_topic_equality_uses_name_and_payload_type() {
    let left = Topic::<String>::try_new("events").expect("topic should build");
    let right = Topic::<String>::try_new("events").expect("topic should build");
    let other_payload = Topic::<u32>::try_new("events").expect("topic should build");
    let other_name = Topic::<String>::try_new("other").expect("topic should build");

    assert_eq!(left, right);
    assert_ne!(left.key(), other_payload.key());
    assert_ne!(left, other_name);

    let mut topics = HashSet::new();
    topics.insert(left);
    assert!(topics.contains(&right));
}
