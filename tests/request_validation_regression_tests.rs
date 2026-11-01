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
use qubit_event_bus::SubscribeError;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::model::DeadLetterAdmissionPolicy;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscribeRequestBuildError;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
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
