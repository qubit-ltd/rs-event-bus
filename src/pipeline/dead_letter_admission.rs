// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Applies the facade dead-letter admission rule to a publish receipt.

use crate::model::AdmissionOutcome;
use crate::model::DeadLetterAdmissionPolicy;
use crate::model::PublishReceipt;
use crate::spi::EventBusCapabilities;
use crate::spi::PublishGuarantee;

/// Returns whether the dead-letter publication reached a provider acceptance.
///
/// Known accepted and partially accepted destinations count as forwarding.
/// Opaque acceptance counts only when the provider promises at least an
/// accepted result. Empty destination reports, dropped publication, and
/// destination reports with no acceptance remain failures.
///
/// # Parameters
///
/// - `receipt`: Publication result whose admission outcome is evaluated.
/// - `capabilities`: Provider guarantees used to interpret opaque acceptance.
/// - `policy`: Admission evidence required by the facade.
///
/// # Returns
///
/// `true` when the configured policy considers the dead-letter event forwarded.
pub(crate) fn was_accepted(
    receipt: &PublishReceipt,
    capabilities: EventBusCapabilities,
    policy: DeadLetterAdmissionPolicy,
) -> bool {
    match (receipt.admission_outcome(), policy) {
        (AdmissionOutcome::Accepted(_), _) | (AdmissionOutcome::PartiallyAccepted(_), _) => true,
        (AdmissionOutcome::OpaqueAccepted, DeadLetterAdmissionPolicy::TransportAccepted) => {
            matches!(
                capabilities.publish_guarantee(),
                PublishGuarantee::Accepted | PublishGuarantee::Confirmed | PublishGuarantee::DurablyStored
            )
        }
        (AdmissionOutcome::OpaqueAccepted, DeadLetterAdmissionPolicy::KnownDestination)
        | (AdmissionOutcome::NoneAccepted(_), _)
        | (AdmissionOutcome::NoDestinations, _)
        | (AdmissionOutcome::Dropped, _) => false,
    }
}

#[cfg(test)]
mod tests {
    use qubit_id::Id;

    use super::was_accepted;
    use crate::model::AdmissionOutcome;
    use crate::model::AdmissionSummary;
    use crate::model::DeadLetterAdmissionPolicy;
    use crate::model::EventId;
    use crate::model::ProviderId;
    use crate::model::PublishAcknowledgement;
    use crate::model::PublishReceipt;
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
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::AcceptRetryReject,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            guarantee,
            PublishVisibility::Opaque,
        )
    }

    /// Creates a receipt for the supplied test acknowledgement.
    fn receipt(acknowledgement: PublishAcknowledgement) -> PublishReceipt {
        PublishReceipt::new(
            EventId::new("dead-letter-event").unwrap(),
            None,
            ProviderId::new("test").unwrap(),
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
            crate::model::DestinationAdmission::new(
                Id::new(1),
                crate::model::SubscriberId::new("accepted").unwrap(),
                crate::model::AdmissionStatus::Accepted,
            ),
            crate::model::DestinationAdmission::new(
                Id::new(2),
                crate::model::SubscriberId::new("rejected").unwrap(),
                crate::model::AdmissionStatus::Rejected("full".into()),
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
        let _: AdmissionSummary = AdmissionSummary::default();
    }
}
