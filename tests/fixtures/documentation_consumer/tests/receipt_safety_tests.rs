// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises decisions using real receipts, including retained uncertain history.

use std::time::Duration;

use event_bus_documentation_consumer::receipt_safety::{
    check_order_admission, inspect_order_receipt, republish_action,
};
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
    let bus = AsyncEventBus::local(LocalEventBusConfig::new())
        .await
        .expect("local bus");
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
    assert!(matches!(
        uncertain.admission_outcome(),
        AdmissionOutcome::NoneAccepted(_)
    ));
    assert_eq!(republish_action(&uncertain), RepublishAction::ReconcileByEventId);
    assert_eq!(
        republish_action(&receipt(no_accepted, false)),
        RepublishAction::RepublishWhole
    );
    let accepted = DestinationAdmission::new(
        accepting_subscription.id(),
        accepting_subscription.subscriber_id().clone(),
        AdmissionStatus::Accepted,
    );
    let full = receipt(
        PublishAcknowledgement::DestinationAdmissions(vec![accepted.clone()]),
        false,
    );
    assert_eq!(republish_action(&full), RepublishAction::NoRepublish);
    let partial = receipt(
        PublishAcknowledgement::DestinationAdmissions(vec![accepted, rejected]),
        false,
    );
    assert_eq!(republish_action(&partial), RepublishAction::RetryRejectedDestinations);
    let _ = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(3),
        })
        .await
        .expect("shutdown");
}

#[tokio::test(flavor = "current_thread")]
async fn test_strict_admission_check_reports_each_receipt_shape_without_consuming_it() {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new())
        .await
        .expect("local bus");
    let topic = Topic::<String>::new("receipt.strict-check").expect("topic");
    let accepted_subscription = bus
        .subscribe(SubscribeRequest::new("accepted", topic.clone()).expect("request"))
        .await
        .expect("subscription");
    let rejected_subscription = bus
        .subscribe(SubscribeRequest::new("rejected", topic).expect("request"))
        .await
        .expect("subscription");
    let accepted = DestinationAdmission::new(
        accepted_subscription.id(),
        accepted_subscription.subscriber_id().clone(),
        AdmissionStatus::Accepted,
    );
    let rejected = DestinationAdmission::new(
        rejected_subscription.id(),
        rejected_subscription.subscriber_id().clone(),
        AdmissionStatus::Rejected("full".into()),
    );

    let all_accepted = receipt(
        PublishAcknowledgement::DestinationAdmissions(vec![accepted.clone()]),
        false,
    );
    let before = all_accepted.clone();
    assert_eq!(check_order_admission(&all_accepted), Ok(()));
    assert_eq!(all_accepted, before);

    let partial = receipt(
        PublishAcknowledgement::DestinationAdmissions(vec![accepted.clone(), rejected.clone()]),
        false,
    );
    let before = partial.clone();
    assert!(matches!(
        check_order_admission(&partial),
        Err(AdmissionCheckError::RejectedDestinations { .. })
    ));
    assert_eq!(partial, before);

    let all_rejected = receipt(PublishAcknowledgement::DestinationAdmissions(vec![rejected]), false);
    let before = all_rejected.clone();
    assert_eq!(
        check_order_admission(&all_rejected),
        Err(AdmissionCheckError::NoAcceptedDestination),
    );
    assert_eq!(all_rejected, before);

    let no_destinations = receipt(PublishAcknowledgement::DestinationAdmissions(Vec::new()), false);
    let before = no_destinations.clone();
    assert_eq!(
        check_order_admission(&no_destinations),
        Err(AdmissionCheckError::NoAcceptedDestination),
    );
    assert_eq!(no_destinations, before);

    let opaque = receipt(
        PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        },
        false,
    );
    let before = opaque.clone();
    assert_eq!(
        check_order_admission(&opaque),
        Err(AdmissionCheckError::VisibilityUnavailable),
    );
    assert_eq!(opaque, before);

    let uncertain = receipt(PublishAcknowledgement::DestinationAdmissions(vec![accepted]), true);
    assert_eq!(republish_action(&uncertain), RepublishAction::ReconcileByEventId,);

    let _ = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(3),
        })
        .await
        .expect("shutdown");
}

#[tokio::test(flavor = "current_thread")]
async fn test_inspection_reconciles_uncertain_history_before_strict_admission() {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new())
        .await
        .expect("local bus");
    let topic = Topic::<String>::new("receipt.inspection").expect("topic");
    let accepted_subscription = bus
        .subscribe(SubscribeRequest::new("accepted", topic.clone()).expect("request"))
        .await
        .expect("subscription");
    let rejected_subscription = bus
        .subscribe(SubscribeRequest::new("rejected", topic).expect("request"))
        .await
        .expect("subscription");
    let accepted = DestinationAdmission::new(
        accepted_subscription.id(),
        accepted_subscription.subscriber_id().clone(),
        AdmissionStatus::Accepted,
    );
    let rejected = DestinationAdmission::new(
        rejected_subscription.id(),
        rejected_subscription.subscriber_id().clone(),
        AdmissionStatus::Rejected("full".into()),
    );

    let uncertain = receipt(
        PublishAcknowledgement::DestinationAdmissions(vec![accepted.clone()]),
        true,
    );
    let before = uncertain.clone();
    match inspect_order_receipt(uncertain) {
        Err((retained, None, RepublishAction::ReconcileByEventId)) => assert_eq!(retained, before),
        result => panic!("unexpected uncertain-history result: {result:?}"),
    }

    let all_accepted = receipt(
        PublishAcknowledgement::DestinationAdmissions(vec![accepted.clone()]),
        false,
    );
    let before = all_accepted.clone();
    assert_eq!(inspect_order_receipt(all_accepted), Ok(before));

    let partial = receipt(
        PublishAcknowledgement::DestinationAdmissions(vec![accepted, rejected]),
        false,
    );
    let before = partial.clone();
    match inspect_order_receipt(partial) {
        Err((retained, Some(AdmissionCheckError::RejectedDestinations { .. }), action)) => {
            assert_eq!(retained, before);
            assert_eq!(action, RepublishAction::RetryRejectedDestinations);
        }
        result => panic!("unexpected strict-check failure result: {result:?}"),
    }

    let _ = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(3),
        })
        .await
        .expect("shutdown");
}
