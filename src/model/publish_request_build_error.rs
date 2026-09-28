// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validation failures returned while building a publication request.

use crate::error::EventIdGenerationError;

/// Invalid publication request metadata or policy.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::PublishRequestBuildError;
///
/// let error = PublishRequestBuildError::MissingField("topic");
/// assert!(matches!(error, PublishRequestBuildError::MissingField("topic")));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum PublishRequestBuildError {
    /// Random ID generation failed before an event ID was explicitly supplied.
    #[error("failed to generate publish event ID")]
    EventIdGeneration(
        /// Underlying failure from the event ID generator.
        #[source]
        EventIdGenerationError,
    ),
    /// A required request field was omitted.
    #[error("missing required publish request field: {0}")]
    MissingField(
        /// Name of the omitted request field.
        &'static str,
    ),
    /// A header key or value is invalid.
    #[error("invalid publish header {0:?}")]
    InvalidHeader(
        /// Invalid header key.
        String,
    ),
    /// The ordering key is empty or contains controls.
    #[error("invalid publish ordering key")]
    InvalidOrderingKey,
    /// Retry classification or cancellation requires a retry policy.
    #[error("retry rule or cancellation requires a retry policy")]
    InvalidRetryConfiguration,
}
