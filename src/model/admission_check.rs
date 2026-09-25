// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Checks for per-destination publication admission.

/// Counts the destination outcomes reported in a publication receipt.
///
/// These counts describe admission only, not subscriber handler completion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdmissionSummary {
    /// Number of destinations that accepted the event for dispatch.
    pub accepted: usize,
    /// Number of destinations excluded by subscription filters.
    pub filtered: usize,
    /// Number of destinations that rejected admission.
    pub rejected: usize,
}

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

/// The admission condition a caller requires before treating a receipt as
/// usable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionRequirement {
    /// Require at least one destination to accept the event.
    AtLeastOneAccepted,
    /// Require at least one acceptance and no destination rejections.
    AtLeastOneAcceptedAndNoRejected,
}

/// Why a receipt does not meet an admission requirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdmissionCheckError {
    /// The provider accepted the message without reporting individual
    /// destinations.
    #[error("destination admission visibility is unavailable")]
    VisibilityUnavailable,
    /// A publisher interceptor stopped dispatch before provider admission.
    #[error("publication was dropped by an interceptor")]
    Dropped,
    /// No reported destination accepted the event.
    #[error("no destination accepted the publication")]
    NoAcceptedDestination,
    /// Some destinations rejected admission despite at least one acceptance.
    #[error("{count} destinations rejected the publication")]
    RejectedDestinations {
        /// Number of destinations that rejected admission.
        count: usize,
    },
}
