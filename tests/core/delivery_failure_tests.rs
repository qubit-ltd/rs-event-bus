// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_event_bus::DeadLetterOutcome;
use qubit_event_bus::PublishOutcome;
use qubit_event_bus::PublishReceipt;

#[test]
fn rejected_dead_letter_outcome_retains_admission_receipt() {
    let receipt = PublishReceipt::new("event-id".to_string(), None, PublishOutcome::Dropped);

    let outcome = DeadLetterOutcome::Rejected(receipt.clone());
    assert_eq!(outcome, DeadLetterOutcome::Rejected(receipt));
}
