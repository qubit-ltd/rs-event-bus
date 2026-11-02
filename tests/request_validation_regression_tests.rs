// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression coverage for shared subscription input validation.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventId;
use qubit_event_bus::PublishError;
use qubit_event_bus::SubscribeError;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::model::DeadLetterAdmissionPolicy;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::PublishRequestBuildError;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscribeRequestBuildError;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryDecision;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;
use support::fake_spi::FakeAsyncEventBusSpi;
use support::fake_spi::FakeEventBusSpi;

fn options_with(key: &str, value: &str) -> SubscribeOptions<u32> {
    let mut values = BTreeMap::new();
    values.insert(key.to_owned(), value.to_owned());
    SubscribeOptions::builder().provider_options(values).build()
}

fn invalid_provider_options_error(error: SubscribeError) {
    assert!(matches!(
        error,
        SubscribeError::Configuration(ConfigurationError::InvalidField {
            field: "provider_options",
            ..
        })
    ));
}

struct NoopPublishRetryRule;

impl RetryRule<qubit_event_bus::error::PublishAttemptError> for NoopPublishRetryRule {
    fn decide(
        &self,
        _failure: &qubit_retry::AttemptFailure<qubit_event_bus::error::PublishAttemptError>,
        _context: &qubit_retry::RetryContext,
    ) -> RetryDecision {
        RetryDecision::UseDefault
    }
}

#[test]
fn test_publish_builder_rejects_rule_and_token_without_policy() {
    let topic = Topic::<u32>::new("events.publish").expect("topic");
    for options in [
        PublishOptions::builder().retry_rule(NoopPublishRetryRule).build(),
        PublishOptions::builder()
            .retry_cancellation_token(RetryCancellationToken::new())
            .build(),
        PublishOptions::builder()
            .retry_rule(NoopPublishRetryRule)
            .retry_cancellation_token(RetryCancellationToken::new())
            .build(),
    ] {
        let result = PublishRequest::builder()
            .topic(topic.clone())
            .payload(1_u32)
            .options(options)
            .build();
        assert!(matches!(
            result,
            Err(PublishRequestBuildError::InvalidRetryConfiguration)
        ));
    }
}

#[test]
fn test_subscribe_builder_rejects_rule_and_token_without_policy() {
    for options in [
        SubscribeOptions::builder()
            .retry_rule(
                |_: &qubit_retry::AttemptFailure<qubit_event_bus::error::DeliveryAttemptError>,
                 _: &qubit_retry::RetryContext| RetryDecision::UseDefault,
            )
            .build(),
        SubscribeOptions::builder()
            .retry_cancellation_token(RetryCancellationToken::new())
            .build(),
        SubscribeOptions::builder()
            .retry_rule(
                |_: &qubit_retry::AttemptFailure<qubit_event_bus::error::DeliveryAttemptError>,
                 _: &qubit_retry::RetryContext| RetryDecision::UseDefault,
            )
            .retry_cancellation_token(RetryCancellationToken::new())
            .build(),
    ] {
        let result = SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("worker").expect("subscriber ID"))
            .topic(Topic::<u32>::new("events.subscribe").expect("topic"))
            .options(options)
            .build();
        assert!(matches!(
            result,
            Err(SubscribeRequestBuildError::InvalidRetryConfiguration)
        ));
    }
}

#[test]
fn test_retry_rule_and_token_are_valid_when_policy_is_present() {
    let retry_policy = RetryPolicy::builder().build().expect("default retry policy");
    let publish = PublishRequest::builder()
        .topic(Topic::<u32>::new("events.publish").expect("topic"))
        .payload(1_u32)
        .retry_policy(retry_policy.clone())
        .retry_rule(NoopPublishRetryRule)
        .retry_cancellation_token(RetryCancellationToken::new())
        .build();
    assert!(publish.is_ok());

    let subscribe = SubscribeRequest::builder()
        .subscriber_id(SubscriberId::new("worker").expect("subscriber ID"))
        .topic(Topic::<u32>::new("events.subscribe").expect("topic"))
        .retry_policy(retry_policy)
        .retry_rule(
            |_: &qubit_retry::AttemptFailure<qubit_event_bus::error::DeliveryAttemptError>,
             _: &qubit_retry::RetryContext| RetryDecision::UseDefault,
        )
        .retry_cancellation_token(RetryCancellationToken::new())
        .build();
    assert!(subscribe.is_ok());
}

#[test]
fn test_sync_publish_options_route_rejects_orphan_retry_before_callbacks_and_spi() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("test.publish").expect("provider ID"), spi.clone())
        .expect("facade construction");
    let interceptor_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handler_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let options = PublishOptions::builder()
        .retry_cancellation_token(RetryCancellationToken::new())
        .interceptor({
            let calls = interceptor_calls.clone();
            move |event| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Some(event))
            }
        })
        .error_handler({
            let calls = handler_calls.clone();
            move |_, _| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        })
        .build();
    let event_id = EventId::new("fixed-event").expect("event ID");
    let request = PublishRequest::builder()
        .topic(Topic::<u32>::new("events.publish").expect("topic"))
        .payload(1_u32)
        .event_id(event_id.clone())
        .build()
        .expect("valid default request")
        .with_options(options);
    let error = bus.publish(request).expect_err("orphan token is invalid");
    assert_eq!(error.event_id(), &event_id);
    assert_eq!(error.effect(), PublishEffect::NotAccepted);
    assert!(
        matches!(error.cause(), PublishError::Configuration(ConfigurationError::InvalidField { field: "retry_configuration", message }) if message.contains("cancellation") && message.contains("policy"))
    );
    assert_eq!(interceptor_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(handler_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(!spi.operation_log().contains(&"publish"));
    let _ = bus.shutdown(ShutdownMode::Immediate);
}

#[test]
fn test_async_publish_options_route_rejects_orphan_retry_before_spi() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("test.async.publish").expect("provider ID"), spi.clone())
        .expect("facade construction");
    let request = PublishRequest::new(Topic::<u32>::new("events.publish").expect("topic"), 1_u32)
        .expect("request")
        .with_options(
            PublishOptions::builder()
                .retry_cancellation_token(RetryCancellationToken::new())
                .build(),
        );
    let error = support::manual_async::block_on(bus.publish(request)).expect_err("orphan token is invalid");
    assert!(matches!(
        error.cause(),
        PublishError::Configuration(ConfigurationError::InvalidField {
            field: "retry_configuration",
            ..
        })
    ));
    assert_eq!(error.effect(), PublishEffect::NotAccepted);
    assert!(!spi.operation_log().contains(&"publish"));
    let _ = support::manual_async::block_on(bus.shutdown(ShutdownMode::Immediate));
}

#[test]
fn test_sync_and_async_subscribe_options_routes_reject_orphan_retry_before_spi() {
    let sync_spi = Arc::new(FakeEventBusSpi::new());
    let sync_bus = EventBus::from_spi(
        ProviderId::new("test.sync.subscribe.retry").expect("provider ID"),
        sync_spi.clone(),
    )
    .expect("facade construction");
    let sync_request = SubscribeRequest::new("worker", Topic::<u32>::new("events.subscribe").expect("topic"))
        .expect("request")
        .with_options(
            SubscribeOptions::builder()
                .retry_cancellation_token(RetryCancellationToken::new())
                .build(),
        );
    assert!(
        matches!(sync_bus.subscribe(sync_request, |_| Ok::<(), DeliveryError>(())), Err(SubscribeError::Configuration(ConfigurationError::InvalidField { field: "retry_configuration", message })) if message.contains("cancellation") && message.contains("policy"))
    );
    assert!(!sync_spi.operation_log().contains(&"subscribe"));
    let _ = sync_bus.shutdown(ShutdownMode::Immediate);

    let async_spi = Arc::new(FakeAsyncEventBusSpi::new());
    let async_bus = AsyncEventBus::from_spi(
        ProviderId::new("test.async.subscribe.retry").expect("provider ID"),
        async_spi.clone(),
    )
    .expect("facade construction");
    let async_request = SubscribeRequest::new("worker", Topic::<u32>::new("events.subscribe").expect("topic"))
        .expect("request")
        .with_options(
            SubscribeOptions::builder()
                .retry_cancellation_token(RetryCancellationToken::new())
                .build(),
        );
    assert!(
        matches!(support::manual_async::block_on(async_bus.subscribe(async_request)), Err(SubscribeError::Configuration(ConfigurationError::InvalidField { field: "retry_configuration", message })) if message.contains("cancellation") && message.contains("policy"))
    );
    assert!(!async_spi.operation_log().contains(&"subscribe"));
    let _ = support::manual_async::block_on(async_bus.shutdown(ShutdownMode::Immediate));
}

#[test]
fn test_dead_letter_policy_uses_topic_byte_and_text_validation() {
    let ascii_255 = "a".repeat(255);
    let ascii_256 = "a".repeat(256);
    let unicode_255 = "界".repeat(85);
    let unicode_258 = "界".repeat(86);

    for valid in [&ascii_255, &unicode_255] {
        assert!(DeadLetterPolicy::with_topic_name(valid).is_ok());
        assert!(DeadLetterPolicy::with_known_destination(valid).is_ok());
        assert!(DeadLetterPolicy::with_admission(valid, DeadLetterAdmissionPolicy::KnownDestination).is_ok());
    }
    for invalid in [&ascii_256, &unicode_258, "", " leading", "trailing ", "bad\nname"] {
        for result in [
            DeadLetterPolicy::with_topic_name(invalid),
            DeadLetterPolicy::with_known_destination(invalid),
            DeadLetterPolicy::with_admission(invalid, DeadLetterAdmissionPolicy::TransportAccepted),
        ] {
            assert!(matches!(
                result,
                Err(ConfigurationError::InvalidField {
                    field: "dead_letter",
                    ..
                })
            ));
        }
    }

    let topic = Topic::<u32>::new(&unicode_255).expect("255-byte topic is valid");
    let policy = DeadLetterPolicy::with_topic(&topic);
    assert_eq!(policy.topic_name(), unicode_255);
}

#[test]
fn test_builder_provider_options_reject_invalid_keys_and_controls() {
    for (key, value) in [
        ("transport.key", "ok"),
        ("transport", "ok"),
        (".transport", "ok"),
        ("transport.", "ok"),
        ("transport.\nkey", "ok"),
        ("transport.key", "bad\tvalue"),
    ] {
        let result = SubscribeRequest::builder()
            .subscriber_id(SubscriberId::new("worker").expect("subscriber ID"))
            .topic(Topic::<u32>::new("events.input").expect("valid topic"))
            .provider_option(key, value)
            .build();
        if key == "transport.key" && value == "ok" {
            assert!(result.is_ok(), "valid option should build");
        } else {
            assert!(matches!(
                result,
                Err(SubscribeRequestBuildError::InvalidProviderOption(_))
            ));
        }
    }
}

#[test]
fn test_sync_options_route_rejects_invalid_provider_options_before_spi() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("test.provider").expect("provider ID"), spi.clone())
        .expect("facade construction");

    let valid = SubscribeRequest::new("valid", Topic::<u32>::new("events.valid").expect("topic"))
        .expect("request")
        .with_options(options_with("transport.key", "ok"));
    let subscription = bus
        .subscribe(valid, |_| Ok::<(), DeliveryError>(()))
        .expect("valid options reach provider");
    assert_eq!(
        spi.operation_log().iter().filter(|call| **call == "subscribe").count(),
        1
    );
    subscription.cancel().expect("cancel subscription");

    for (key, value) in [
        ("transport", "ok"),
        (".transport", "ok"),
        ("transport.", "ok"),
        ("transport.\nkey", "ok"),
        ("transport.key", "bad\tvalue"),
    ] {
        let invalid = SubscribeRequest::new("invalid", Topic::<u32>::new("events.invalid").expect("topic"))
            .expect("request")
            .with_options(options_with(key, value));
        match bus.subscribe(invalid, |_| Ok::<(), DeliveryError>(())) {
            Err(error) => invalid_provider_options_error(error),
            Ok(_) => panic!("invalid options must be rejected"),
        }
    }
    assert_eq!(
        spi.operation_log().iter().filter(|call| **call == "subscribe").count(),
        1
    );
    let _ = bus.shutdown(ShutdownMode::Immediate);
}

#[test]
fn test_async_options_route_rejects_invalid_provider_options_before_spi() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("test.async").expect("provider ID"), spi.clone())
        .expect("facade construction");
    let valid = SubscribeRequest::new("valid", Topic::<u32>::new("events.valid").expect("topic"))
        .expect("request")
        .with_options(options_with("transport.key", "ok"));
    let subscription = support::manual_async::block_on(bus.subscribe(valid)).expect("valid options reach provider");
    assert_eq!(
        spi.operation_log().iter().filter(|call| **call == "subscribe").count(),
        1
    );
    drop(subscription);

    for (key, value) in [
        ("transport", "ok"),
        (".transport", "ok"),
        ("transport.", "ok"),
        ("transport.\nkey", "ok"),
        ("transport.key", "bad\tvalue"),
    ] {
        let request = SubscribeRequest::new("invalid", Topic::<u32>::new("events.invalid").expect("topic"))
            .expect("request")
            .with_options(options_with(key, value));

        match support::manual_async::block_on(bus.subscribe(request)) {
            Err(error) => invalid_provider_options_error(error),
            Ok(_) => panic!("invalid options must be rejected"),
        }
    }
    assert_eq!(
        spi.operation_log().iter().filter(|call| **call == "subscribe").count(),
        1
    );
    let _ = support::manual_async::block_on(bus.shutdown(ShutdownMode::Immediate));
}
