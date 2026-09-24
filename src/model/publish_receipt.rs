// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receipt for one publication attempt.

use super::AdmissionCheckError;
use super::AdmissionRequirement;
use super::AdmissionStatus;
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

    /// Counts reported destination admissions, independent of their order.
    ///
    /// Returns `Some` for per-destination results, including an empty list.
    /// Returns `None` when the provider hides destinations or interception
    /// dropped the publication before dispatch.
    pub fn admission_summary(&self) -> Option<AdmissionSummary> {
        let PublishAcknowledgement::DestinationAdmissions(destinations) = &self.acknowledgement else {
            return None;
        };
        let mut summary = AdmissionSummary::default();
        for destination in destinations {
            match destination.status() {
                AdmissionStatus::Accepted => summary.accepted += 1,
                AdmissionStatus::Filtered => summary.filtered += 1,
                AdmissionStatus::Rejected(_) => summary.rejected += 1,
            }
        }
        Some(summary)
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
        match &self.acknowledgement {
            PublishAcknowledgement::DroppedByInterceptor => return Err(AdmissionCheckError::Dropped),
            PublishAcknowledgement::Accepted { .. } => return Err(AdmissionCheckError::VisibilityUnavailable),
            PublishAcknowledgement::DestinationAdmissions(_) => {}
        }
        let Some(summary) = self.admission_summary() else {
            return Err(AdmissionCheckError::VisibilityUnavailable);
        };
        if summary.accepted == 0 {
            return Err(AdmissionCheckError::NoAcceptedDestination);
        }
        if requirement == AdmissionRequirement::AtLeastOneAcceptedAndNoRejected && summary.rejected > 0 {
            return Err(AdmissionCheckError::RejectedDestinations {
                count: summary.rejected,
            });
        }
        Ok(())
    }
}
