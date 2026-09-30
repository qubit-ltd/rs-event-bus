// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public acknowledgement state tests.

use qubit_event_bus::model::Acknowledgement;
use qubit_event_bus::model::AcknowledgementError;
use qubit_event_bus::model::AcknowledgementState;

#[test]
fn clones_observe_one_terminal_decision_and_reject_conflicts() {
    let original = Acknowledgement::new();
    let clone = original.clone();

    clone.ack().expect("first ACK should complete the shared state");
    original.ack().expect("repeating the same decision should succeed");

    assert_eq!(original.state(), AcknowledgementState::Acknowledged);
    assert!(clone.is_completed());
    assert!(matches!(clone.nack(), Err(AcknowledgementError::AlreadyCompleted)));
}
