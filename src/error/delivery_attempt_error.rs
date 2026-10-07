// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Classified failure of one subscriber handler attempt.

use std::error::Error;

/// A failure supplied to `qubit-retry` for one delivery attempt.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::DeliveryAttemptError;
///
/// let failure = DeliveryAttemptError::new("handler", None, std::io::Error::other("unavailable"));
/// assert_eq!(failure.kind(), "handler");
/// assert_eq!(failure.retryable(), None);
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum DeliveryAttemptError {
    /// The delivery attempt failed with a stable kind and original source.
    #[error("delivery attempt failed ({kind}): {source}")]
    Failure {
        /// Stable event-bus classification.
        kind: &'static str,
        /// Optional application override of default retry classification.
        retryable: Option<bool>,
        /// Original failure preserved for diagnostics.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
}

impl DeliveryAttemptError {
    /// Wraps a single attempt failure while preserving its source.
    ///
    /// # Parameters
    /// - `kind`: stable classification used by retry policy.
    /// - `retryable`: optional application override of retry classification.
    /// - `source`: original handler or delivery error to retain.
    ///
    /// # Returns
    /// A classified attempt error that exposes `source` through its error
    /// chain.
    pub fn new(kind: &'static str, retryable: Option<bool>, source: impl Error + Send + Sync + 'static) -> Self {
        Self::Failure {
            kind,
            retryable,
            source: Box::new(source),
        }
    }

    /// Returns the stable failure classification.
    ///
    /// # Returns
    /// The static kind supplied when the failure was created.
    #[must_use]
    #[inline]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Failure { kind, .. } => kind,
        }
    }

    /// Returns an explicit retry override, or `None` for default
    /// classification.
    ///
    /// # Returns
    /// The optional application retry override attached to this failure.
    #[must_use]
    #[inline]
    pub fn retryable(&self) -> Option<bool> {
        match self {
            Self::Failure { retryable, .. } => *retryable,
        }
    }
}
