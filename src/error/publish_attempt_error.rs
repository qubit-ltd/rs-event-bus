// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Classified failure of one publication attempt.

use std::error::Error;

/// A failure supplied to `qubit-retry` for one publish attempt.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::PublishAttemptError;
///
/// let failure = PublishAttemptError::new("unavailable", Some(true), std::io::Error::other("offline"));
/// assert_eq!(failure.kind(), "unavailable");
/// assert_eq!(failure.retryable(), Some(true));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum PublishAttemptError {
    /// The publish attempt failed with a stable kind and original source.
    #[error("publish attempt failed ({kind}): {source}")]
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

impl PublishAttemptError {
    /// Wraps a single attempt failure while preserving its source.
    ///
    /// # Parameters
    /// - `kind`: stable failure classification.
    /// - `retryable`: optional override of the default retry decision.
    /// - `source`: underlying failure to retain in the error chain.
    ///
    /// # Returns
    /// A classified publish-attempt error.
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
    /// The static classification associated with this failure.
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
    /// The configured override, or `None` to use the default classification.
    #[inline]
    #[must_use = "Use the returned query result."]
    pub fn retryable(&self) -> Option<bool> {
        match self {
            Self::Failure { retryable, .. } => *retryable,
        }
    }
}
