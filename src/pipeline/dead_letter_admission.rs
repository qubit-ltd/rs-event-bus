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
#[must_use]
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
                PublishGuarantee::Accepted
                    | PublishGuarantee::Confirmed
                    | PublishGuarantee::DurablyStored
            )
        }
        (AdmissionOutcome::OpaqueAccepted, DeadLetterAdmissionPolicy::KnownDestination)
        | (AdmissionOutcome::NoneAccepted(_), _)
        | (AdmissionOutcome::NoDestinations, _)
        | (AdmissionOutcome::Dropped, _) => false,
    }
}
