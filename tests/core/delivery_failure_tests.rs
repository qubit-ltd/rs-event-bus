// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_event_bus::DeadLetterOutcome;
use qubit_event_bus::EventBusError;
use qubit_event_bus::PublishOutcome;
use qubit_event_bus::PublishReceipt;

#[test]
fn rejected_dead_letter_outcome_retains_admission_receipt() {
    let receipt = PublishReceipt::new("event-id".to_string(), None, PublishOutcome::Dropped);

    let outcome = DeadLetterOutcome::Rejected(receipt.clone());
    assert_eq!(outcome, DeadLetterOutcome::Rejected(receipt));
}

#[test]
fn test_dead_letter_outcomes_preserve_terminal_views() {
    let receipt = PublishReceipt::new(
        "event-id".to_string(),
        Some("dead-letter-id".to_string()),
        PublishOutcome::Dropped,
    );

    assert!(matches!(
        DeadLetterOutcome::NotConfigured,
        DeadLetterOutcome::NotConfigured
    ));
    assert!(matches!(
        DeadLetterOutcome::DroppedByStrategy,
        DeadLetterOutcome::DroppedByStrategy
    ));
    assert_eq!(
        DeadLetterOutcome::Publication(receipt.clone()),
        DeadLetterOutcome::Publication(receipt)
    );
    assert!(matches!(
        DeadLetterOutcome::Failed(EventBusError::dead_letter_failed("failed")),
        DeadLetterOutcome::Failed(error) if error.kind() == "dead_letter_failed"
    ));
}
