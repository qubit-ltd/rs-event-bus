// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for event bus errors and acknowledgement state.

use qubit_event_bus::Acknowledgement;
use qubit_event_bus::EventBusError;

#[test]
fn test_acknowledgement_default_and_nack_state() {
    let acknowledgement = Acknowledgement::default();
    assert!(!acknowledgement.is_completed());

    acknowledgement.nack();

    assert!(acknowledgement.is_completed());
    assert!(acknowledgement.is_nacked());
    assert!(!acknowledgement.is_acked());
}

#[test]
fn test_event_bus_error_display_covers_variants() {
    let errors = [
        EventBusError::not_started(),
        EventBusError::start_failed("pool build failed"),
        EventBusError::invalid_argument("field", "bad value"),
        EventBusError::missing_field("topic"),
        EventBusError::handler_failed("boom"),
        EventBusError::interceptor_failed("publish", "interceptor boom"),
        EventBusError::error_handler_failed("subscribe", "handler boom"),
        EventBusError::dead_letter_failed("publish rejected"),
        EventBusError::lock_poisoned("subscriptions"),
        EventBusError::type_mismatch("String", "u32"),
        EventBusError::unsupported_operation("flow"),
    ];
    let messages = errors.iter().map(ToString::to_string).collect::<Vec<_>>();

    assert!(messages[0].contains("not been started"));
    assert!(messages[1].contains("failed to start"));
    assert!(messages[2].contains("invalid argument"));
    assert!(messages[3].contains("missing required field"));
    assert!(messages[4].contains("handler failed"));
    assert!(messages[5].contains("interceptor failed"));
    assert!(messages[6].contains("error handler failed"));
    assert!(messages[7].contains("dead-letter routing failed"));
    assert!(messages[8].contains("poisoned"));
    assert!(messages[9].contains("type mismatch"));
    assert!(messages[10].contains("unsupported"));
}
