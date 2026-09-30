// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public publication-options builder contract tests.

use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::model::DuplicateRiskPolicy;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishOptionsBuilder;
use qubit_event_bus::model::Topic;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryPolicy;

#[test]
fn test_builder_defaults_and_default_trait_match() {
    let from_new = PublishOptions::<String>::builder().build();
    let from_default = PublishOptionsBuilder::<String>::default().build();

    for options in [&from_new, &from_default] {
        assert_eq!(options.duplicate_risk_policy(), DuplicateRiskPolicy::Forbid);
        assert!(options.retry_policy().is_none());
        assert!(options.retry_rule().is_none());
        assert!(options.retry_cancellation_token().is_none());
        assert_eq!(options.error_handler_count(), 0);
        assert!(options.interceptors().is_empty());
    }
}

#[test]
fn test_builder_keeps_retry_policy_cancellation_and_duplicate_risk_settings() {
    let cancellation = RetryCancellationToken::new();
    let options = PublishOptions::<String>::builder()
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .build()
                .expect("retry policy should be valid"),
        )
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::UseDefault)
        .retry_cancellation_token(cancellation.clone())
        .build();

    assert_eq!(options.duplicate_risk_policy(), DuplicateRiskPolicy::AllowDuplicates);
    assert!(options.retry_policy().is_some());
    assert!(options.retry_rule().is_some());
    assert!(options.retry_cancellation_token().is_some());
}

#[test]
fn test_builder_appends_handlers_and_interceptors_in_registration_order() {
    let handler_calls = Arc::new(Mutex::new(Vec::new()));
    let interceptor_calls = Arc::new(Mutex::new(Vec::new()));
    let first_handler_calls = Arc::clone(&handler_calls);
    let second_handler_calls = Arc::clone(&handler_calls);
    let first_interceptor_calls = Arc::clone(&interceptor_calls);
    let second_interceptor_calls = Arc::clone(&interceptor_calls);

    let options = PublishOptions::<String>::builder()
        .error_handler(move |_, _| first_handler_calls.lock().unwrap().push(1))
        .error_handler(move |_, _| second_handler_calls.lock().unwrap().push(2))
        .interceptor(move |event| {
            first_interceptor_calls.lock().unwrap().push(1);
            Ok(Some(event))
        })
        .interceptor(move |event| {
            second_interceptor_calls.lock().unwrap().push(2);
            Ok(Some(event))
        })
        .build();

    assert_eq!(options.error_handler_count(), 2);
    assert_eq!(options.interceptors().len(), 2);

    let event = EventEnvelope::new(
        Topic::<String>::new("builder.test").expect("topic should be valid"),
        "payload".to_owned(),
    )
    .expect("event ID generation should succeed");
    let event = (options.interceptors()[0])(event)
        .expect("first interceptor should succeed")
        .expect("first interceptor should retain the event");
    let result = (options.interceptors()[1])(event).expect("second interceptor should succeed");

    assert!(result.is_some());
    assert_eq!(*interceptor_calls.lock().unwrap(), vec![1, 2]);
    assert!(handler_calls.lock().unwrap().is_empty());
}

#[test]
fn test_builder_preserves_interceptor_drop_and_error_results() {
    let options = PublishOptions::<String>::builder()
        .interceptor(|_| Ok(None))
        .interceptor(|_| Err(qubit_event_bus::PublishError::Closed))
        .build();
    let event = || {
        EventEnvelope::new(
            Topic::<String>::new("builder.failure").expect("topic should be valid"),
            "payload".to_owned(),
        )
        .expect("event ID generation should succeed")
    };

    assert!(
        (options.interceptors()[0])(event())
            .expect("drop should be successful")
            .is_none()
    );
    assert!(matches!(
        (options.interceptors()[1])(event()),
        Err(qubit_event_bus::PublishError::Closed),
    ));
}
