/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for event bus errors and acknowledgement state.

use qubit_event_bus::{
    Acknowledgement,
    EventBusError,
};

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
        EventBusError::invalid_argument("field", "bad value"),
        EventBusError::missing_field("topic"),
        EventBusError::handler_failed("boom"),
        EventBusError::lock_poisoned("subscriptions"),
        EventBusError::type_mismatch("String", "u32"),
        EventBusError::ThreadJoinFailed,
        EventBusError::unsupported_operation("flow"),
    ];
    let messages = errors.iter().map(ToString::to_string).collect::<Vec<_>>();

    assert!(messages[0].contains("not been started"));
    assert!(messages[1].contains("invalid argument"));
    assert!(messages[2].contains("missing required field"));
    assert!(messages[3].contains("handler failed"));
    assert!(messages[4].contains("poisoned"));
    assert!(messages[5].contains("type mismatch"));
    assert!(messages[6].contains("panicked"));
    assert!(messages[7].contains("unsupported"));
}
