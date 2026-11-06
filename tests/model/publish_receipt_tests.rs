// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public publication receipt behavior tests.

use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionOutcome;
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

#[test]
fn test_receipt_classification_is_stable_across_clone_and_duplicate_evidence() {
    let accepted = || AdmissionStatus::Accepted;
    let filtered = || AdmissionStatus::Filtered;
    let rejected = || AdmissionStatus::Rejected("full".into());
    let admission = |id, status| {
        DestinationAdmission::new(
            Id::new(id),
            SubscriberId::new(format!("subscriber-{id}")).expect("valid subscriber ID"),
            status,
        )
    };
    let cases = [
        (
            "empty",
            PublishAcknowledgement::DestinationAdmissions(Vec::new()),
            AdmissionOutcome::NoDestinations,
            Some(AdmissionSummary::default()),
            [
                Err(AdmissionCheckError::NoAcceptedDestination),
                Err(AdmissionCheckError::NoAcceptedDestination),
                Err(AdmissionCheckError::NoAcceptedDestination),
            ],
        ),
        (
            "all-filtered",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(1, filtered())]),
            AdmissionOutcome::NoneAccepted(AdmissionSummary {
                accepted: 0,
                filtered: 1,
                rejected: 0,
            }),
            Some(AdmissionSummary {
                accepted: 0,
                filtered: 1,
                rejected: 0,
            }),
            [
                Err(AdmissionCheckError::NoAcceptedDestination),
                Err(AdmissionCheckError::NoAcceptedDestination),
                Err(AdmissionCheckError::NoAcceptedDestination),
            ],
        ),
        (
            "all-rejected",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(2, rejected())]),
            AdmissionOutcome::NoneAccepted(AdmissionSummary {
                accepted: 0,
                filtered: 0,
                rejected: 1,
            }),
            Some(AdmissionSummary {
                accepted: 0,
                filtered: 0,
                rejected: 1,
            }),
            [
                Err(AdmissionCheckError::NoAcceptedDestination),
                Err(AdmissionCheckError::NoAcceptedDestination),
                Err(AdmissionCheckError::NoAcceptedDestination),
            ],
        ),
        (
            "all-accepted",
            PublishAcknowledgement::DestinationAdmissions(vec![admission(3, accepted())]),
            AdmissionOutcome::Accepted(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 0,
            }),
            Some(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 0,
            }),
            [Ok(()), Ok(()), Ok(())],
        ),
        (
            "mixed",
            PublishAcknowledgement::DestinationAdmissions(vec![
                admission(4, rejected()),
                admission(5, accepted()),
                admission(6, filtered()),
            ]),
            AdmissionOutcome::PartiallyAccepted(AdmissionSummary {
                accepted: 1,
                filtered: 1,
                rejected: 1,
            }),
            Some(AdmissionSummary {
                accepted: 1,
                filtered: 1,
                rejected: 1,
            }),
            [
                Ok(()),
                Ok(()),
                Err(AdmissionCheckError::RejectedDestinations { count: 1 }),
            ],
        ),
        (
            "opaque",
            PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            },
            AdmissionOutcome::OpaqueAccepted,
            None,
            [
                Ok(()),
                Err(AdmissionCheckError::VisibilityUnavailable),
                Err(AdmissionCheckError::VisibilityUnavailable),
            ],
        ),
        (
            "dropped",
            PublishAcknowledgement::DroppedByInterceptor,
            AdmissionOutcome::Dropped,
            None,
            [
                Err(AdmissionCheckError::Dropped),
                Err(AdmissionCheckError::Dropped),
                Err(AdmissionCheckError::Dropped),
            ],
        ),
    ];

    let requirements = [
        AdmissionRequirement::ProviderOrDestinationAccepted,
        AdmissionRequirement::AtLeastOneAccepted,
        AdmissionRequirement::AtLeastOneAcceptedAndNoRejected,
    ];
    for (name, acknowledgement, outcome, summary, checks) in cases {
        let receipt = PublishReceipt::new(
            EventId::new(format!("event-{name}")).expect("valid event ID"),
            None,
            ProviderId::new("receipt-provider").expect("valid provider ID"),
            acknowledgement.clone(),
        );
        for candidate in [receipt.clone(), receipt.clone().with_duplicate_possible(true)] {
            assert_eq!(outcome, candidate.admission_outcome(), "{name}");
            assert_eq!(summary, candidate.admission_summary(), "{name}");
            assert_eq!(&acknowledgement, candidate.acknowledgement(), "{name}");
            for (requirement, expected) in requirements.into_iter().zip(checks) {
                assert_eq!(
                    expected,
                    candidate.check_admission(requirement),
                    "{name}: {requirement:?}"
                );
            }
        }
        let toggled = receipt.with_duplicate_possible(true).with_duplicate_possible(false);
        assert!(!toggled.duplicate_possible(), "{name}");
        assert_eq!(outcome, toggled.admission_outcome(), "{name}");
        assert_eq!(summary, toggled.admission_summary(), "{name}");
    }

    let ordered = PublishAcknowledgement::DestinationAdmissions(vec![
        admission(7, accepted()),
        admission(8, rejected()),
        admission(9, filtered()),
    ]);
    let reversed = PublishAcknowledgement::DestinationAdmissions(vec![
        admission(9, filtered()),
        admission(8, rejected()),
        admission(7, accepted()),
    ]);
    let receipt = |acknowledgement| {
        PublishReceipt::new(
            EventId::new("ordered-event").expect("valid event ID"),
            None,
            ProviderId::new("receipt-provider").expect("valid provider ID"),
            acknowledgement,
        )
    };
    assert_eq!(
        receipt(ordered.clone()).admission_outcome(),
        receipt(reversed.clone()).admission_outcome()
    );
    assert_eq!(
        receipt(ordered).admission_summary(),
        receipt(reversed).admission_summary()
    );
}
