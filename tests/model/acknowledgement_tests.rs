// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public acknowledgement state tests.

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

use qubit_event_bus::model::Acknowledgement;
use qubit_event_bus::model::AcknowledgementError;
use qubit_event_bus::model::AcknowledgementState;

#[test]
fn test_clones_observe_one_terminal_decision_and_reject_conflicts() {
    let original = Acknowledgement::new();
    let clone = original.clone();

    clone.ack().expect("first ACK should complete the shared state");
    original.ack().expect("repeating the same decision should succeed");

    assert_eq!(original.state(), AcknowledgementState::Acknowledged);
    assert!(clone.is_completed(), "clone should observe the shared terminal state");
    assert!(
        matches!(clone.nack(), Err(AcknowledgementError::AlreadyCompleted)),
        "conflicting decision should be rejected after completion"
    );
}

#[test]
fn test_competing_clones_publish_exactly_one_terminal_state() {
    let state = Acknowledgement::new();
    let barrier = Arc::new(Barrier::new(3));
    let ack_state = state.clone();
    let ack_barrier = Arc::clone(&barrier);
    let ack_thread = thread::spawn(move || {
        ack_barrier.wait();
        ack_state.ack()
    });
    let nack_state = state.clone();
    let nack_barrier = Arc::clone(&barrier);
    let nack_thread = thread::spawn(move || {
        nack_barrier.wait();
        nack_state.nack()
    });

    barrier.wait();
    let ack_result = ack_thread.join().expect("ACK worker should complete");
    let nack_result = nack_thread.join().expect("NACK worker should complete");

    assert_ne!(
        ack_result.is_ok(),
        nack_result.is_ok(),
        "exactly one competing decision should succeed"
    );
    assert!(matches!(
        (ack_result, nack_result),
        (Ok(()), Err(AcknowledgementError::AlreadyCompleted)) | (Err(AcknowledgementError::AlreadyCompleted), Ok(()))
    ));
    assert!(matches!(
        state.state(),
        AcknowledgementState::Acknowledged | AcknowledgementState::NegativelyAcknowledged
    ));
}
