// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises decisions using real receipts, including retained uncertain history.

use std::time::Duration;

use event_bus_documentation_consumer::receipt_safety::republish_action;
use event_bus_documentation_consumer::republish_action::RepublishAction;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

/// Builds a real public receipt; uncertainty is the evidence attached by retry.
fn receipt(acknowledgement: PublishAcknowledgement, unknown_history: bool) -> PublishReceipt {
    let id = EventId::new("order-42").expect("valid ID");
    let dispatched = (!matches!(acknowledgement, PublishAcknowledgement::DroppedByInterceptor)).then(|| id.clone());
    PublishReceipt::new(
        id,
        dispatched,
        ProviderId::new("receipt-example").expect("valid provider ID"),
        acknowledgement,
    )
    .with_duplicate_possible(unknown_history)
}

#[test]
fn test_unknown_history_precedes_final_no_accepted_destination() {
    let receipt = receipt(PublishAcknowledgement::DestinationAdmissions(Vec::new()), true);
    assert_eq!(
        receipt.check_admission(AdmissionRequirement::AtLeastOneAccepted),
        Err(AdmissionCheckError::NoAcceptedDestination),
    );
    assert_eq!(republish_action(&receipt), RepublishAction::ReconcileByEventId);
}

#[test]
fn test_certain_empty_destination_snapshot_can_republish_whole() {
    let receipt = receipt(PublishAcknowledgement::DestinationAdmissions(Vec::new()), false);
    assert_eq!(republish_action(&receipt), RepublishAction::RepublishWhole);
}

#[test]
fn test_interceptor_drop_does_not_automatically_republish() {
    let receipt = receipt(PublishAcknowledgement::DroppedByInterceptor, false);
    assert_eq!(republish_action(&receipt), RepublishAction::NoRepublish);
}

#[test]
fn test_opaque_acceptance_does_not_automatically_republish() {
    let receipt = receipt(
        PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        },
        false,
    );
    assert_eq!(republish_action(&receipt), RepublishAction::NoRepublish);
}

#[tokio::test(flavor = "current_thread")]
async fn test_history_precedes_final_rejection_and_partial_acceptance_repairs_only_rejected() {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new()).await.expect("local bus");
    let topic = Topic::<String>::new("receipt.test").expect("topic");
    let subscription = bus
        .subscribe(SubscribeRequest::new("rejected", topic.clone()).expect("request"))
        .await
        .expect("subscription");
    let accepting_subscription = bus
        .subscribe(SubscribeRequest::new("accepted", topic).expect("request"))
        .await
        .expect("subscription");
    let rejected = DestinationAdmission::new(
        subscription.id(),
        subscription.subscriber_id().clone(),
        AdmissionStatus::Rejected("full".into()),
    );
    let no_accepted = PublishAcknowledgement::DestinationAdmissions(vec![rejected.clone()]);
    let uncertain = receipt(no_accepted.clone(), true);
    assert!(matches!(uncertain.admission_outcome(), AdmissionOutcome::NoneAccepted(_)));
    assert_eq!(republish_action(&uncertain), RepublishAction::ReconcileByEventId);
    assert_eq!(republish_action(&receipt(no_accepted, false)), RepublishAction::RepublishWhole);
    let accepted = DestinationAdmission::new(
        accepting_subscription.id(),
        accepting_subscription.subscriber_id().clone(),
        AdmissionStatus::Accepted,
    );
    let full = receipt(PublishAcknowledgement::DestinationAdmissions(vec![accepted.clone()]), false);
    assert_eq!(republish_action(&full), RepublishAction::NoRepublish);
    let partial = receipt(PublishAcknowledgement::DestinationAdmissions(vec![accepted, rejected]), false);
    assert_eq!(republish_action(&partial), RepublishAction::RetryRejectedDestinations);
    let _ = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })
    .await
    .expect("shutdown");
}
