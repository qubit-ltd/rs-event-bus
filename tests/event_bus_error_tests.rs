// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public conversion and source-chain contracts for aggregate bus failures.

use std::error::Error;
use std::io::Error as IoError;

use qubit_event_bus::error::CapabilityError;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::DeliveryError;
use qubit_event_bus::error::EventBusError;
use qubit_event_bus::error::LifecycleError;
use qubit_event_bus::error::ProviderError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::PublishFailure;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::error::SettlementError;
use qubit_event_bus::error::SubscribeError;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishEffect;

#[test]
fn test_event_bus_error_configuration_conversion_retains_field() {
    let error = EventBusError::from(ConfigurationError::MissingField { field: "topic" });

    assert_eq!(error.to_string(), "missing required field topic");
    assert!(matches!(
        &error,
        EventBusError::Configuration(ConfigurationError::MissingField { field: "topic" })
    ));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_event_bus_error_capability_conversion_retains_capability_name() {
    let error = EventBusError::from(CapabilityError::Unsupported { capability: "ordering" });

    assert_eq!(error.to_string(), "unsupported event bus capability: ordering");
    assert!(matches!(
        &error,
        EventBusError::Capability(CapabilityError::Unsupported { capability: "ordering" })
    ));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_event_bus_error_codec_conversion_retains_codec_source() {
    let error = EventBusError::from(CodecError::NativeTypeMismatch);

    assert_eq!(error.to_string(), "native payload type does not match subscribed topic");
    assert!(matches!(&error, EventBusError::Codec(CodecError::NativeTypeMismatch)));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_event_bus_error_publish_conversion_retains_pipeline_error() {
    let error = EventBusError::from(PublishError::Closed);

    assert_eq!(error.to_string(), "cannot publish after event bus shutdown");
    assert!(matches!(&error, EventBusError::Publish(PublishError::Closed)));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_event_bus_error_publish_failure_conversion_retains_identity_and_cause() {
    let failure = PublishFailure::new(
        EventId::new("order-42").expect("event ID should be valid"),
        PublishEffect::NotAccepted,
        PublishError::Closed,
    );
    let error = EventBusError::from(failure);

    assert!(error.to_string().contains("order-42"));
    match &error {
        EventBusError::PublishFailure(failure) => {
            assert_eq!(failure.event_id().as_str(), "order-42");
            assert_eq!(failure.effect(), PublishEffect::NotAccepted);
            assert!(matches!(failure.cause(), PublishError::Closed));
        }
        _ => panic!("publish failure conversion should preserve the aggregate variant"),
    }
    assert!(Error::source(&error).is_some_and(|source| source.is::<PublishError>()));
}

#[test]
fn test_event_bus_error_subscribe_conversion_retains_closed_state() {
    let error = EventBusError::from(SubscribeError::Closed);

    assert_eq!(error.to_string(), "cannot subscribe after event bus shutdown");
    assert!(matches!(&error, EventBusError::Subscribe(SubscribeError::Closed)));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_event_bus_error_receive_conversion_retains_closed_state() {
    let error = EventBusError::from(ReceiveError::Closed);

    assert_eq!(error.to_string(), "cannot receive after subscription close");
    assert!(matches!(&error, EventBusError::Receive(ReceiveError::Closed)));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_event_bus_error_delivery_conversion_retains_handler_cause() {
    let delivery = DeliveryError::Handler {
        source: Box::new(IoError::other("handler rejected event")),
    };
    let error = EventBusError::from(delivery);

    assert_eq!(error.to_string(), "event handler failed: handler rejected event");
    assert_eq!(
        Error::source(&error).map(ToString::to_string).as_deref(),
        Some("handler rejected event")
    );
    assert!(matches!(&error, EventBusError::Delivery(DeliveryError::Handler { .. })));
}

#[test]
fn test_event_bus_error_settlement_conversion_retains_terminal_decision() {
    let error = EventBusError::from(SettlementError::AlreadySettled);

    assert_eq!(error.to_string(), "delivery has already been settled");
    assert!(matches!(
        &error,
        EventBusError::Settlement(SettlementError::AlreadySettled)
    ));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_event_bus_error_lifecycle_conversion_retains_closed_state() {
    let error = EventBusError::from(LifecycleError::Closed);

    assert_eq!(error.to_string(), "event bus is closed");
    assert!(matches!(&error, EventBusError::Lifecycle(LifecycleError::Closed)));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_event_bus_error_provider_conversion_retains_resolution_source() {
    let error = EventBusError::from(ProviderError::Resolution {
        source: Box::new(IoError::other("no matching provider")),
    });

    assert_eq!(
        error.to_string(),
        "event bus provider resolution failed: no matching provider"
    );
    assert_eq!(
        Error::source(&error).map(ToString::to_string).as_deref(),
        Some("no matching provider")
    );
    assert!(matches!(
        &error,
        EventBusError::Provider(ProviderError::Resolution { .. })
    ));
}
