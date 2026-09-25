// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Classifies the provider's report after one publication attempt.

use super::AdmissionSummary;

/// Classifies what a provider reported after one publication attempt.
///
/// This outcome describes admission only; it does not report subscriber
/// handler completion. An opaque provider acceptance cannot be interpreted as
/// a known destination count, and a partial acceptance must not be retried as
/// a whole event without an application idempotency policy.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AdmissionOutcome;
/// use qubit_event_bus::model::PublishAcknowledgement;
///
/// let acknowledgement = PublishAcknowledgement::DestinationAdmissions(Vec::new());
/// assert_eq!(AdmissionOutcome::NoDestinations, acknowledgement.admission_outcome());
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AdmissionOutcome {
    /// The provider accepted the message without reporting destination details.
    OpaqueAccepted,
    /// At least one destination accepted and none rejected admission.
    Accepted(AdmissionSummary),
    /// At least one destination accepted and at least one rejected admission.
    PartiallyAccepted(AdmissionSummary),
    /// Destinations were reported, but none accepted admission.
    NoneAccepted(AdmissionSummary),
    /// The provider reported an empty destination snapshot.
    NoDestinations,
    /// A publisher interceptor stopped dispatch before provider admission.
    Dropped,
}
