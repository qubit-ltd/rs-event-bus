// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal publication acknowledgement classification tests.

use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::AdmissionSummary;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SubscriberId;
use qubit_id::Id;

fn destination(id: u64, status: AdmissionStatus) -> DestinationAdmission {
    DestinationAdmission::new(
        Id::new(id),
        SubscriberId::new(format!("subscriber-{id}")).expect("test subscriber ID should be valid"),
        status,
    )
}

#[test]
fn test_opaque_acceptance_and_interceptor_drop_have_distinct_classifications() {
    let accepted = PublishAcknowledgement::Accepted {
        provider_message_id: Some("message-7".to_owned()),
        metadata: Default::default(),
    };
    let dropped = PublishAcknowledgement::DroppedByInterceptor;

    assert_eq!(accepted.admission_outcome(), AdmissionOutcome::OpaqueAccepted);
    assert!(!accepted.is_dropped());
    assert_eq!(dropped.admission_outcome(), AdmissionOutcome::Dropped);
    assert!(dropped.is_dropped());
}

#[test]
fn test_empty_destination_snapshot_is_not_treated_as_rejection() {
    let acknowledgement = PublishAcknowledgement::DestinationAdmissions(Vec::new());

    assert_eq!(acknowledgement.admission_outcome(), AdmissionOutcome::NoDestinations);
    assert!(!acknowledgement.is_dropped());
}

#[test]
fn test_destination_counts_classify_all_filtered_or_rejected_as_none_accepted() {
    let acknowledgement = PublishAcknowledgement::DestinationAdmissions(vec![
        destination(1, AdmissionStatus::Filtered),
        destination(2, AdmissionStatus::Rejected("capacity".into())),
    ]);

    assert_eq!(
        acknowledgement.admission_outcome(),
        AdmissionOutcome::NoneAccepted(AdmissionSummary {
            accepted: 0,
            filtered: 1,
            rejected: 1,
        }),
    );
}

#[test]
fn test_destination_counts_distinguish_full_and_partial_acceptance() {
    let full = PublishAcknowledgement::DestinationAdmissions(vec![
        destination(1, AdmissionStatus::Accepted),
        destination(2, AdmissionStatus::Filtered),
    ]);
    let partial = PublishAcknowledgement::DestinationAdmissions(vec![
        destination(3, AdmissionStatus::Accepted),
        destination(4, AdmissionStatus::Rejected("queue full".into())),
        destination(5, AdmissionStatus::Filtered),
    ]);

    assert_eq!(
        full.admission_outcome(),
        AdmissionOutcome::Accepted(AdmissionSummary {
            accepted: 1,
            filtered: 1,
            rejected: 0,
        }),
    );
    assert_eq!(
        partial.admission_outcome(),
        AdmissionOutcome::PartiallyAccepted(AdmissionSummary {
            accepted: 1,
            filtered: 1,
            rejected: 1,
        }),
    );
}
