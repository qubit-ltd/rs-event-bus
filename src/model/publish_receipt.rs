// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receipt for one publication attempt.

use super::AdmissionCheckError;
use super::AdmissionOutcome;
use super::AdmissionRequirement;
use super::AdmissionSummary;
use super::EventId;
use super::ProviderId;
use super::PublishAcknowledgement;

/// Publication admission, not subscriber handler completion.
///
/// Inspect [`Self::acknowledgement`] to learn what the provider reported. A
/// receipt with destination admissions can contain both accepted and rejected
/// destinations; retrying the original event may duplicate work for accepted
/// destinations. Use an application idempotency key or retry only work that
/// the application's delivery policy can safely repeat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishReceipt {
    input_event_id: EventId,
    dispatched_event_id: Option<EventId>,
    provider_id: ProviderId,
    acknowledgement: PublishAcknowledgement,
}

impl PublishReceipt {
    /// Creates a receipt after interception and provider admission.
    pub fn new(
        input_event_id: EventId,
        dispatched_event_id: Option<EventId>,
        provider_id: ProviderId,
        acknowledgement: PublishAcknowledgement,
    ) -> Self {
        Self {
            input_event_id,
            dispatched_event_id,
            provider_id,
            acknowledgement,
        }
    }
    /// Returns the original event ID before publisher interception.
    pub fn input_event_id(&self) -> &EventId {
        &self.input_event_id
    }
    /// Returns the dispatched event ID, or `None` when interception dropped it.
    pub fn dispatched_event_id(&self) -> Option<&EventId> {
        self.dispatched_event_id.as_ref()
    }
    /// Returns the provider that produced the admission result.
    pub fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }
    /// Returns provider admission information; handlers may still be pending.
    ///
    /// `DestinationAdmissions([])` means no destinations were reported. It
    /// does not prove that handler work completed or that a remote consumer
    /// was globally idle.
    pub fn acknowledgement(&self) -> &PublishAcknowledgement {
        &self.acknowledgement
    }

    /// Returns a stable classification of the provider's admission result.
    ///
    /// This is a convenience view of [`Self::acknowledgement`]. It does not
    /// report handler completion or change the result of the publication.
    ///
    /// # Returns
    /// The admission outcome reported by the provider or publisher interceptor.
    pub fn admission_outcome(&self) -> AdmissionOutcome {
        self.acknowledgement.admission_outcome()
    }

    /// Counts reported destination admissions, independent of their order.
    ///
    /// Returns `Some` for per-destination results, including an empty list.
    /// Returns `None` when the provider hides destinations or interception
    /// dropped the publication before dispatch.
    pub fn admission_summary(&self) -> Option<AdmissionSummary> {
        match self.admission_outcome() {
            AdmissionOutcome::Accepted(summary)
            | AdmissionOutcome::PartiallyAccepted(summary)
            | AdmissionOutcome::NoneAccepted(summary) => Some(summary),
            AdmissionOutcome::NoDestinations => Some(AdmissionSummary::default()),
            AdmissionOutcome::OpaqueAccepted | AdmissionOutcome::Dropped => None,
        }
    }

    /// Checks whether reported destination admissions meet `requirement`.
    ///
    /// This checks admission, not handler completion. Returns
    /// [`AdmissionCheckError::Dropped`] when interception stopped dispatch,
    /// [`AdmissionCheckError::VisibilityUnavailable`] when a provider did not
    /// identify destinations, and
    /// [`AdmissionCheckError::NoAcceptedDestination`] when none accepted.
    /// The stricter requirement returns
    /// [`AdmissionCheckError::RejectedDestinations`] if any reported
    /// destination rejected admission after at least one accepted.
    pub fn check_admission(&self, requirement: AdmissionRequirement) -> Result<(), AdmissionCheckError> {
        match self.admission_outcome() {
            AdmissionOutcome::OpaqueAccepted => Err(AdmissionCheckError::VisibilityUnavailable),
            AdmissionOutcome::Dropped => Err(AdmissionCheckError::Dropped),
            AdmissionOutcome::NoDestinations | AdmissionOutcome::NoneAccepted(_) => {
                Err(AdmissionCheckError::NoAcceptedDestination)
            }
            AdmissionOutcome::Accepted(_) => Ok(()),
            AdmissionOutcome::PartiallyAccepted(summary) => {
                if requirement == AdmissionRequirement::AtLeastOneAcceptedAndNoRejected {
                    Err(AdmissionCheckError::RejectedDestinations {
                        count: summary.rejected,
                    })
                } else {
                    Ok(())
                }
            }
        }
    }
}
