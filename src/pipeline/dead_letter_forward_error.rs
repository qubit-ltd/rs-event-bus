// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Failures from forwarding a dead-letter event.

use crate::error::PublishError;
use crate::model::AdmissionOutcome;

/// Failure that may be retried without settling the original delivery.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DeadLetterForwardError {
    /// The provider or publish pipeline failed before a receipt was returned.
    #[error(transparent)]
    Publish(
        /// Provider or facade error retained for retry classification.
        #[from]
        PublishError,
    ),
    /// A non-publish pipeline stage failed before provider admission.
    #[error("dead-letter pipeline failed: {0}")]
    Pipeline(
        /// Description of the stage failure that prevented publication.
        Box<str>,
    ),
    /// A receipt reported no admission allowed by the configured policy.
    #[error("dead-letter event was not admitted: {0:?}")]
    NotAdmitted(
        /// Admission evidence reported by the provider or interceptor.
        AdmissionOutcome,
    ),
}
