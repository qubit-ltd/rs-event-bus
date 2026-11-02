// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public publication receipt behavior tests.

use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::AdmissionSummary;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::SubscriberId;
use qubit_id::Id;

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
    for duplicate_possible in [false, true] {
        let opaque = PublishReceipt::new(
            EventId::new("opaque-event").expect("valid event ID"),
            None,
            ProviderId::new("opaque-provider").expect("valid provider ID"),
            acknowledgement.clone(),
        )
        .with_duplicate_possible(duplicate_possible);
        assert!(matches!(
            opaque.check_admission(AdmissionRequirement::AtLeastOneAccepted),
            Err(AdmissionCheckError::VisibilityUnavailable)
        ));
        assert!(
            opaque
                .check_admission(AdmissionRequirement::ProviderOrDestinationAccepted)
                .is_ok()
        );
    }
}

#[test]
fn test_empty_visible_snapshot_and_opaque_acceptance_keep_distinct_summaries() {
    let empty = PublishReceipt::new(
        EventId::new("receipt-event").expect("valid event ID"),
        None,
        ProviderId::new("receipt-provider").expect("valid provider ID"),
        PublishAcknowledgement::DestinationAdmissions(Vec::new()),
    );
    let opaque = PublishReceipt::new(
        EventId::new("receipt-event").expect("valid event ID"),
        None,
        ProviderId::new("receipt-provider").expect("valid provider ID"),
        PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        },
    );

    assert_eq!(
        empty.admission_summary(),
        Some(AdmissionSummary::default()),
        "an empty visible snapshot must produce an empty admission summary",
    );
    assert_eq!(
        opaque.admission_summary(),
        None,
        "opaque acceptance must not expose a destination summary",
    );
}

#[test]
fn test_duplicate_evidence_does_not_override_visible_admission() {
    let acknowledgement = PublishAcknowledgement::DestinationAdmissions(vec![DestinationAdmission::new(
        Id::new(1),
        SubscriberId::new("audit").expect("valid subscriber ID"),
        AdmissionStatus::Accepted,
    )]);
    let receipt = PublishReceipt::new(
        EventId::new("duplicate-admission-event").expect("valid event ID"),
        None,
        ProviderId::new("duplicate-admission-provider").expect("valid provider ID"),
        acknowledgement.clone(),
    )
    .with_duplicate_possible(true);

    assert!(receipt.duplicate_possible());
    assert_eq!(receipt.acknowledgement(), &acknowledgement);
    for requirement in [
        AdmissionRequirement::ProviderOrDestinationAccepted,
        AdmissionRequirement::AtLeastOneAccepted,
        AdmissionRequirement::AtLeastOneAcceptedAndNoRejected,
    ] {
        assert!(receipt.check_admission(requirement).is_ok());
    }
    assert_eq!(receipt.acknowledgement(), &acknowledgement);
    assert!(receipt.duplicate_possible());
}
