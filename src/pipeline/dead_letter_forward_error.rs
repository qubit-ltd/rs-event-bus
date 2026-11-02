// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Failures from forwarding a dead-letter event.

use crate::error::PublishFailure;
use crate::model::AdmissionOutcome;

/// Failure to publish or admit a dead-letter event.
///
/// This error retains the structured publish failure or the provider's
/// admission evidence. It is exposed as the application error inside a
/// [`qubit_retry::RetryError`] when dead-letter forwarding exhausts its retry
/// policy.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AdmissionOutcome;
/// use qubit_event_bus::pipeline::DeadLetterForwardError;
///
/// let error = DeadLetterForwardError::NotAdmitted(AdmissionOutcome::NoDestinations);
/// assert!(error.to_string().contains("not admitted"));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum DeadLetterForwardError {
    /// The provider or publish pipeline failed before a receipt was returned.
    #[error("dead-letter publish failed: {0}")]
    Publish(
        /// Provider or facade error retained for retry classification.
        #[source]
        PublishFailure,
    ),
    /// A receipt reported no admission allowed by the configured policy.
    #[error("dead-letter event was not admitted: {0:?}")]
    NotAdmitted(
        /// Admission evidence reported by the provider or interceptor.
        AdmissionOutcome,
    ),
}
