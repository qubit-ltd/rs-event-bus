// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public publication receipt behavior tests.

use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishReceipt;

#[test]
fn test_retry_evidence_is_preserved_without_changing_provider_acknowledgement() {
    let acknowledgement = PublishAcknowledgement::Accepted {
        provider_message_id: Some("broker-17".into()),
        metadata: Default::default(),
    };
    let receipt = PublishReceipt::new(
        EventId::new("event-17").expect("valid event ID"),
        Some(EventId::new("event-17").expect("valid event ID")),
        ProviderId::new("broker").expect("valid provider ID"),
        acknowledgement.clone(),
    )
    .with_duplicate_possible(true);

    assert!(
        receipt.duplicate_possible(),
        "retry evidence should mark the receipt as possibly duplicated"
    );
    assert_eq!(
        receipt.acknowledgement(),
        &acknowledgement,
        "retry evidence should preserve the provider acknowledgement"
    );
    assert!(
        receipt
            .check_admission(AdmissionRequirement::AtLeastOneAccepted)
            .is_err(),
        "possibly duplicated delivery must not satisfy the admission requirement"
    );
}
