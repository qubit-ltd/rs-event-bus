/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Tests for publish and subscribe options.

use std::time::Duration;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use qubit_event_bus::{
    AckMode, EventBusError, EventEnvelope, LocalEventBus, PublishOptions, RetryOptions,
    SubscribeOptions, Topic,
};

#[test]
fn test_retry_options_validate_attempts() {
    let retry = RetryOptions::new(3, Duration::from_millis(10))
        .expect("positive attempts should be accepted");

    assert_eq!(retry.max_attempts(), 3);
    assert_eq!(retry.delay(), Duration::from_millis(10));
    assert!(RetryOptions::new(0, Duration::ZERO).is_err());
}

#[test]
fn test_publish_options_builder_sets_retry_and_error_handler() {
    let options = PublishOptions::<String>::builder()
        .retry_options(RetryOptions::new(2, Duration::ZERO).expect("retry should build"))
        .error_handler(|envelope, error| {
            assert_eq!(envelope.payload(), "payload");
            assert!(error.to_string().contains("publish"));
        })
        .build();

    assert_eq!(
        options
            .retry_options()
            .expect("retry should exist")
            .max_attempts(),
        2
    );
    assert_eq!(options.error_handler_count(), 1);
}

#[test]
fn test_publish_options_default_clone_and_error_handler_invocation() {
    let topic = Topic::<String>::try_new("publish-options").expect("topic should build");
    let envelope = EventEnvelope::create(topic, "payload".to_string());
    let errors = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&errors);
    let options = PublishOptions::builder()
        .error_handler(
            move |event: &EventEnvelope<String>, error: &EventBusError| {
                assert_eq!(event.payload(), "payload");
                assert_eq!(error, &EventBusError::not_started());
                captured.fetch_add(1, Ordering::SeqCst);
            },
        )
        .build();
    let cloned = options.clone();

    assert_eq!(PublishOptions::<String>::default().error_handler_count(), 0);
    assert_eq!(cloned.error_handler_count(), 1);
    let error = LocalEventBus::new()
        .publish_envelope_with_options(envelope, cloned)
        .expect_err("stopped bus should reject publish");

    assert_eq!(error, EventBusError::not_started());
    assert_eq!(errors.load(Ordering::SeqCst), 1);
}

#[test]
fn test_subscribe_options_defaults_and_builder() {
    let topic = Topic::<String>::try_new("orders.created").expect("topic should build");
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .priority(10)
        .retry_options(RetryOptions::new(4, Duration::ZERO).expect("retry should build"))
        .filter(|envelope| envelope.payload() == "accepted")
        .build();

    let accepted = EventEnvelope::create(topic.clone(), "accepted".to_string());
    let rejected = EventEnvelope::create(topic, "rejected".to_string());

    assert_eq!(
        SubscribeOptions::<String>::empty().ack_mode(),
        AckMode::Auto
    );
    assert_eq!(options.ack_mode(), AckMode::Manual);
    assert_eq!(options.priority(), 10);
    assert_eq!(
        options
            .retry_options()
            .expect("retry should exist")
            .max_attempts(),
        4
    );
    assert!(options.should_handle(&accepted));
    assert!(!options.should_handle(&rejected));
    assert_eq!(options.error_handler_count(), 0);
    assert_eq!(SubscribeOptions::<String>::default().priority(), 0);
}
