// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validation failures returned while building a subscription request.

/// Invalid subscription request identity or policy.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::SubscribeRequestBuildError;
///
/// let error = SubscribeRequestBuildError::MissingField("topic");
/// assert!(matches!(error, SubscribeRequestBuildError::MissingField("topic")));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum SubscribeRequestBuildError {
    /// A required field was omitted.
    #[error("missing required subscribe request field: {0}")]
    MissingField(
        /// Name of the omitted request field.
        &'static str,
    ),
    /// Retry classification or cancellation requires a retry policy.
    #[error("retry rule or cancellation token requires a retry policy")]
    InvalidRetryConfiguration,
    /// Provider option keys must be namespaced and values must be printable.
    #[error("invalid provider option {0:?}")]
    InvalidProviderOption(
        /// Invalid provider option key.
        String,
    ),
}
