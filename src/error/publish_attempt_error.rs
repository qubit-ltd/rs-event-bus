//! Classified failure of one publication attempt.

use std::error::Error;

/// A failure supplied to `qubit-retry` for one publish attempt.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
    pub fn new(kind: &'static str, retryable: Option<bool>, source: impl Error + Send + Sync + 'static) -> Self {
        Self::Failure {
            kind,
            retryable,
            source: Box::new(source),
        }
    }
    /// Returns the stable failure classification.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Failure { kind, .. } => kind,
        }
    }
    /// Returns an explicit retry override, or `None` for default
    /// classification.
    pub fn retryable(&self) -> Option<bool> {
        match self {
            Self::Failure { retryable, .. } => *retryable,
        }
    }
}
