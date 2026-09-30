// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the pipeline dead-letter admission contract.

use qubit_id::Id;

use crate::model::AdmissionOutcome;
use crate::model::AdmissionStatus;
use crate::model::DeadLetterAdmissionPolicy;
use crate::model::DestinationAdmission;
use crate::model::EventId;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::model::PublishReceipt;
use crate::model::SubscriberId;
use crate::pipeline::dead_letter_was_accepted as was_accepted;
use crate::spi::DelayedDeliveryCapability;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::OrderingCapability;
use crate::spi::PayloadModes;
use crate::spi::PublishGuarantee;
use crate::spi::PublishVisibility;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::SubscriptionModes;

/// Builds a capability set with the supplied publish guarantee.
fn capabilities(guarantee: PublishGuarantee) -> EventBusCapabilities {
    EventBusCapabilities::builder()
        .payload_modes(PayloadModes::Native)
        .settlement(SettlementCapabilities::AcceptRetryReject)
        .ordering(OrderingCapability::None)
        .delayed_delivery(DelayedDeliveryCapability::None)
        .durability(DurabilityCapability::Ephemeral)
        .subscription_modes(SubscriptionModes::EPHEMERAL)
        .consumer_groups(false)
        .replay(ReplayCapability::None)
        .publish_guarantee(guarantee)
        .publish_visibility(PublishVisibility::Opaque)
        .build()
        .expect("all dead-letter test capabilities are configured")
}

/// Creates a receipt for the supplied test acknowledgement.
fn receipt(acknowledgement: PublishAcknowledgement) -> PublishReceipt {
    PublishReceipt::new(
        EventId::new("dead-letter-event").expect("valid dead-letter event ID"),
        None,
        ProviderId::new("test").expect("valid test provider ID"),
        acknowledgement,
    )
}

#[test]
fn test_dead_letter_acceptance_distinguishes_empty_and_dropped() {
    for acknowledgement in [
        PublishAcknowledgement::DestinationAdmissions(Vec::new()),
        PublishAcknowledgement::DroppedByInterceptor,
    ] {
        assert!(!was_accepted(
            &receipt(acknowledgement),
            capabilities(PublishGuarantee::Accepted),
            DeadLetterAdmissionPolicy::TransportAccepted
        ));
    }
    assert_eq!(
        AdmissionOutcome::NoDestinations,
        receipt(PublishAcknowledgement::DestinationAdmissions(Vec::new())).admission_outcome(),
    );
}

#[test]
fn test_dead_letter_acceptance_accepts_partial_and_guaranteed_opaque() {
    let partial = PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(
            Id::new(1),
            SubscriberId::new("accepted").expect("valid accepted subscriber ID"),
            AdmissionStatus::Accepted,
        ),
        DestinationAdmission::new(
            Id::new(2),
            SubscriberId::new("rejected").expect("valid rejected subscriber ID"),
            AdmissionStatus::Rejected("full".into()),
        ),
    ]);
    assert!(was_accepted(
        &receipt(partial),
        capabilities(PublishGuarantee::Accepted),
        DeadLetterAdmissionPolicy::TransportAccepted
    ));
    assert!(was_accepted(
        &receipt(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        }),
        capabilities(PublishGuarantee::DurablyStored),
        DeadLetterAdmissionPolicy::TransportAccepted,
    ));
    assert!(!was_accepted(
        &receipt(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        }),
        capabilities(PublishGuarantee::FireAndForget),
        DeadLetterAdmissionPolicy::TransportAccepted,
    ));
    let opaque = receipt(PublishAcknowledgement::Accepted {
        provider_message_id: None,
        metadata: Default::default(),
    });
    assert!(!was_accepted(
        &opaque,
        capabilities(PublishGuarantee::Accepted),
        DeadLetterAdmissionPolicy::KnownDestination,
    ));
}
