// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public facade contract tests for provider admission acknowledgements.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::EventBus;
use qubit_event_bus::error::SpiError;
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
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_id::Id;

struct AdmissionProvider {
    acknowledgements: Mutex<VecDeque<PublishAcknowledgement>>,
}

impl EventBusSpi for AdmissionProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::DestinationAdmissions,
        )
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(self
            .acknowledgements
            .lock()
            .expect("acknowledgement queue lock")
            .pop_front()
            .expect("test acknowledgement was configured"))
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(provider_error("subscribe"))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

fn provider_error(operation: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "admission-test".into(),
        operation,
        resource: None,
        kind: "unsupported",
        retryable: Some(false),
        source: Box::new(std::io::Error::other("unsupported in this test provider")),
    }
}

fn publish(acknowledgement: PublishAcknowledgement) -> PublishAcknowledgement {
    let provider = Arc::new(AdmissionProvider {
        acknowledgements: Mutex::new(VecDeque::from([acknowledgement])),
    });
    let bus = EventBus::from_spi(ProviderId::new("admission-test").expect("valid provider ID"), provider);
    let topic = Topic::<String>::new("orders.created").expect("valid topic");
    let request = PublishRequest::new(topic, "order-1".to_owned()).expect("request builds");
    let receipt = bus
        .publish(request)
        .expect("admission is a successful publication receipt");
    receipt.acknowledgement().clone()
}

/// Builds a receipt whose admission result can be checked without a provider.
fn receipt(acknowledgement: PublishAcknowledgement) -> PublishReceipt {
    PublishReceipt::new(
        EventId::new("event-1").expect("valid event ID"),
        None,
        ProviderId::new("admission-test").expect("valid provider ID"),
        acknowledgement,
    )
}

/// Builds one destination with a stable ID for each table row.
fn destination(id: u64, status: AdmissionStatus) -> DestinationAdmission {
    DestinationAdmission::new(
        Id::new(id),
        SubscriberId::new(format!("subscriber-{id}")).expect("valid subscriber ID"),
        status,
    )
}

#[test]
fn test_admission_checks_cover_every_acknowledgement_outcome() {
    use AdmissionCheckError::Dropped;
    use AdmissionCheckError::NoAcceptedDestination;
    use AdmissionCheckError::RejectedDestinations;
    use AdmissionCheckError::VisibilityUnavailable;
    use AdmissionRequirement::AtLeastOneAccepted;
    use AdmissionRequirement::AtLeastOneAcceptedAndNoRejected;

    let cases = [
        (
            "opaque accepted",
            PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            },
            None,
            Err(VisibilityUnavailable),
            Err(VisibilityUnavailable),
        ),
        (
            "interceptor dropped",
            PublishAcknowledgement::DroppedByInterceptor,
            None,
            Err(Dropped),
            Err(Dropped),
        ),
        (
            "empty snapshot",
            PublishAcknowledgement::DestinationAdmissions(vec![]),
            Some(AdmissionSummary::default()),
            Err(NoAcceptedDestination),
            Err(NoAcceptedDestination),
        ),
        (
            "filtered only",
            PublishAcknowledgement::DestinationAdmissions(vec![destination(1, AdmissionStatus::Filtered)]),
            Some(AdmissionSummary {
                accepted: 0,
                filtered: 1,
                rejected: 0,
            }),
            Err(NoAcceptedDestination),
            Err(NoAcceptedDestination),
        ),
        (
            "rejected only",
            PublishAcknowledgement::DestinationAdmissions(vec![
                destination(1, AdmissionStatus::Rejected("full".into())),
                destination(2, AdmissionStatus::Rejected("closed".into())),
            ]),
            Some(AdmissionSummary {
                accepted: 0,
                filtered: 0,
                rejected: 2,
            }),
            Err(NoAcceptedDestination),
            Err(NoAcceptedDestination),
        ),
        (
            "accepted only",
            PublishAcknowledgement::DestinationAdmissions(vec![destination(1, AdmissionStatus::Accepted)]),
            Some(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 0,
            }),
            Ok(()),
            Ok(()),
        ),
        (
            "partial acceptance",
            PublishAcknowledgement::DestinationAdmissions(vec![
                destination(1, AdmissionStatus::Accepted),
                destination(2, AdmissionStatus::Rejected("full".into())),
            ]),
            Some(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 1,
            }),
            Ok(()),
            Err(RejectedDestinations { count: 1 }),
        ),
    ];

    for (name, acknowledgement, summary, at_least_one, no_rejected) in cases {
        let receipt = receipt(acknowledgement.clone());
        assert_eq!(summary, receipt.admission_summary(), "{name}");
        assert_eq!(at_least_one, receipt.check_admission(AtLeastOneAccepted), "{name}");
        assert_eq!(
            no_rejected,
            receipt.check_admission(AtLeastOneAcceptedAndNoRejected),
            "{name}"
        );
        assert_eq!(
            &acknowledgement,
            receipt.acknowledgement(),
            "{name}: original receipt changed"
        );
    }
}

#[test]
fn test_admission_outcome_classifies_every_provider_result() {
    let cases = [
        (
            PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            },
            AdmissionOutcome::OpaqueAccepted,
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(Vec::new()),
            AdmissionOutcome::NoDestinations,
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![destination(1, AdmissionStatus::Filtered)]),
            AdmissionOutcome::NoneAccepted(AdmissionSummary {
                accepted: 0,
                filtered: 1,
                rejected: 0,
            }),
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![destination(
                1,
                AdmissionStatus::Rejected("full".into()),
            )]),
            AdmissionOutcome::NoneAccepted(AdmissionSummary {
                accepted: 0,
                filtered: 0,
                rejected: 1,
            }),
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![destination(1, AdmissionStatus::Accepted)]),
            AdmissionOutcome::Accepted(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 0,
            }),
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![
                destination(1, AdmissionStatus::Accepted),
                destination(2, AdmissionStatus::Filtered),
            ]),
            AdmissionOutcome::Accepted(AdmissionSummary {
                accepted: 1,
                filtered: 1,
                rejected: 0,
            }),
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![
                destination(1, AdmissionStatus::Accepted),
                destination(2, AdmissionStatus::Rejected("full".into())),
            ]),
            AdmissionOutcome::PartiallyAccepted(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 1,
            }),
        ),
        (PublishAcknowledgement::DroppedByInterceptor, AdmissionOutcome::Dropped),
    ];

    for (acknowledgement, expected) in cases {
        let receipt = receipt(acknowledgement.clone());
        assert_eq!(expected, acknowledgement.admission_outcome());
        assert_eq!(expected, receipt.admission_outcome());
        let expected_summary = match expected {
            AdmissionOutcome::Accepted(summary)
            | AdmissionOutcome::PartiallyAccepted(summary)
            | AdmissionOutcome::NoneAccepted(summary) => Some(summary),
            AdmissionOutcome::NoDestinations => Some(AdmissionSummary::default()),
            AdmissionOutcome::OpaqueAccepted | AdmissionOutcome::Dropped => None,
            _ => None,
        };
        assert_eq!(expected_summary, receipt.admission_summary());
    }
}

#[test]
fn test_admission_summary_ignores_destination_order() {
    let statuses = [
        AdmissionStatus::Filtered,
        AdmissionStatus::Rejected("full".into()),
        AdmissionStatus::Accepted,
        AdmissionStatus::Rejected("closed".into()),
        AdmissionStatus::Accepted,
    ];
    let expected = Some(AdmissionSummary {
        accepted: 2,
        filtered: 1,
        rejected: 2,
    });
    for reversed in [false, true] {
        let mut destinations = statuses
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, status)| destination(index as u64 + 1, status))
            .collect::<Vec<_>>();
        if reversed {
            destinations.reverse();
        }
        let receipt = receipt(PublishAcknowledgement::DestinationAdmissions(destinations));
        assert_eq!(expected, receipt.admission_summary());
    }
}

#[test]
fn publish_receipt_preserves_empty_destination_snapshot() {
    assert_eq!(
        PublishAcknowledgement::DestinationAdmissions(Vec::new()),
        publish(PublishAcknowledgement::DestinationAdmissions(Vec::new()))
    );
}

#[test]
fn publish_receipt_preserves_partial_destination_admission() {
    let accepted_id = SubscriberId::new("accepted-subscriber").expect("valid subscriber ID");
    let rejected_id = SubscriberId::new("full-subscriber").expect("valid subscriber ID");
    let acknowledgement = publish(PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(Id::new(1), accepted_id, AdmissionStatus::Accepted),
        DestinationAdmission::new(
            Id::new(2),
            rejected_id,
            AdmissionStatus::Rejected("subscription queue is full".into()),
        ),
    ]));
    let PublishAcknowledgement::DestinationAdmissions(destinations) = acknowledgement else {
        panic!("provider destination admissions must be preserved");
    };
    assert_eq!(2, destinations.len());
    assert_eq!("accepted-subscriber", destinations[0].subscriber_id().as_str());
    assert_eq!(&AdmissionStatus::Accepted, destinations[0].status());
    assert_eq!("full-subscriber", destinations[1].subscriber_id().as_str());
    assert_eq!(
        &AdmissionStatus::Rejected("subscription queue is full".into()),
        destinations[1].status()
    );
}

#[test]
fn publish_receipt_is_successful_when_every_destination_rejects_admission() {
    let acknowledgement = publish(PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(
            Id::new(3),
            SubscriberId::new("full-subscriber").expect("valid subscriber ID"),
            AdmissionStatus::Rejected("subscription queue is full".into()),
        ),
    ]));
    let PublishAcknowledgement::DestinationAdmissions(destinations) = acknowledgement else {
        panic!("provider destination admissions must be preserved");
    };
    assert_eq!(1, destinations.len());
    assert!(matches!(destinations[0].status(), AdmissionStatus::Rejected(_)));
}
