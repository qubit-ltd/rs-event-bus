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
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::ProviderId;
/// use qubit_event_bus::model::PublishAcknowledgement;
/// use qubit_event_bus::model::PublishReceipt;
///
/// let receipt = PublishReceipt::new(
///     EventId::new("order-42").unwrap(),
///     Some(EventId::new("order-42").unwrap()),
///     ProviderId::new("local").unwrap(),
///     PublishAcknowledgement::Accepted {
///         provider_message_id: None,
///         metadata: Default::default(),
///     },
/// );
/// assert_eq!(receipt.provider_id().as_str(), "local");
/// assert!(!receipt.duplicate_possible());
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub struct PublishReceipt {
    /// Event identifier supplied by the original caller.
    input_event_id: EventId,
    /// Event identifier after interception, absent when dropped.
    dispatched_event_id: Option<EventId>,
    /// Provider that returned the admission result.
    provider_id: ProviderId,
    /// Provider or interceptor admission outcome.
    acknowledgement: PublishAcknowledgement,
    /// Whether an earlier failed attempt may also have admitted this event.
    duplicate_possible: bool,
}

impl PublishReceipt {
    /// Creates a receipt after interception and provider admission.
    ///
    /// # Parameters
    /// - `input_event_id`: original caller-provided event identifier.
    /// - `dispatched_event_id`: identifier sent to the provider, or `None` if
    ///   interception dropped the event.
    /// - `provider_id`: provider that handled the publication.
    /// - `acknowledgement`: admission information returned by the publish path.
    ///
    /// # Returns
    /// A receipt preserving input and dispatched identity separately.
    #[inline]
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
            duplicate_possible: false,
        }
    }
    /// Returns whether an earlier failed attempt may also have admitted this
    /// event.
    ///
    /// # Returns
    /// `true` when prior failed attempts left admission uncertain, otherwise
    /// `false`.
    #[must_use]
    #[inline]
    pub fn duplicate_possible(&self) -> bool {
        self.duplicate_possible
    }
    /// Returns the original event ID before publisher interception.
    ///
    /// # Returns
    /// The identifier supplied by the caller before interceptors ran.
    #[must_use = "the input event ID identifies the original publication"]
    #[inline]
    pub fn input_event_id(&self) -> &EventId {
        &self.input_event_id
    }
    /// Returns the dispatched event ID, or `None` when interception dropped it.
    ///
    /// # Returns
    /// The dispatched identifier, or `None` when publication was dropped.
    #[must_use = "Use the returned dispatched event id."]
    #[inline]
    pub fn dispatched_event_id(&self) -> Option<&EventId> {
        self.dispatched_event_id.as_ref()
    }
    /// Returns the provider that produced the admission result.
    ///
    /// # Returns
    /// The identifier of the provider that handled the publication.
    #[must_use]
    #[inline]
    pub fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }
    /// Returns provider admission information; handlers may still be pending.
    ///
    /// `DestinationAdmissions([])` means no destinations were reported. It
    /// does not prove that handler work completed or that a remote consumer
    /// was globally idle.
    ///
    /// # Returns
    /// The provider or interceptor's admission report.
    #[inline]
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
    #[must_use = "Use the returned admission outcome."]
    #[inline]
    pub fn admission_outcome(&self) -> AdmissionOutcome {
        self.acknowledgement.admission_outcome()
    }

    /// Counts reported destination admissions, independent of their order.
    ///
    /// Returns `Some` for per-destination results, including an empty list.
    /// Returns `None` when the provider hides destinations or interception
    /// dropped the publication before dispatch.
    ///
    /// # Returns
    /// Counts for reported destinations, including zero counts for an empty
    /// snapshot, or `None` when destination admission is not visible.
    #[must_use = "Use the returned query result."]
    #[inline]
    pub fn admission_summary(&self) -> Option<AdmissionSummary> {
        match self.admission_outcome() {
            AdmissionOutcome::Accepted(summary)
            | AdmissionOutcome::PartiallyAccepted(summary)
            | AdmissionOutcome::NoneAccepted(summary) => Some(summary),
            AdmissionOutcome::NoDestinations => Some(AdmissionSummary::default()),
            AdmissionOutcome::OpaqueAccepted | AdmissionOutcome::Dropped => None,
        }
    }

    /// Attaches retry evidence without changing provider admission information.
    ///
    /// # Parameters
    /// - `value`: whether an earlier failed attempt had uncertain admission.
    ///
    /// # Returns
    /// This receipt with the supplied duplicate possibility.
    #[must_use]
    #[inline]
    pub fn with_duplicate_possible(mut self, value: bool) -> Self {
        self.duplicate_possible = value;
        self
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
    ///
    /// # Parameters
    /// - `requirement`: minimum admission condition to enforce.
    ///
    /// # Returns
    /// `Ok(())` when the visible admission result satisfies `requirement`.
    ///
    /// # Errors
    /// Returns [`AdmissionCheckError::Dropped`] if interception stopped
    /// dispatch, [`AdmissionCheckError::VisibilityUnavailable`] if the
    /// provider hid destination details,
    /// [`AdmissionCheckError::NoAcceptedDestination`] if none accepted, or
    /// [`AdmissionCheckError::RejectedDestinations`] when the strict
    /// requirement observes any rejection.
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
