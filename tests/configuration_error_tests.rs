// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for public configuration error constructors and formatting.

use qubit_event_bus::error::ConfigurationError;

#[test]
fn test_invalid_subscriber_id_retains_value_and_formats_error() {
    let error = ConfigurationError::invalid_subscriber_id(" bad-id");

    assert!(matches!(
        &error,
        ConfigurationError::InvalidSubscriberId { value } if value.as_ref() == " bad-id"
    ));
    assert_eq!(error.to_string(), "invalid subscriber ID: \" bad-id\"");
}

#[test]
fn test_invalid_event_id_retains_value_and_formats_error() {
    let error = ConfigurationError::invalid_event_id(" bad-event");

    assert!(matches!(
        &error,
        ConfigurationError::InvalidEventId { value } if value.as_ref() == " bad-event"
    ));
    assert_eq!(error.to_string(), "invalid event ID: \" bad-event\"");
}
