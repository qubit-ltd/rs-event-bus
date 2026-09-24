// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event publication failures.

use qubit_retry::RetryError;

use crate::error::CapabilityError;
use crate::error::CodecError;
use crate::error::ConfigurationError;
use crate::error::PublishAttemptError;
use crate::error::SpiError;

/// An event could not be published.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::PublishError;
///
/// fn was_closed(error: &PublishError) -> bool {
///     matches!(error, PublishError::Closed)
/// }
///
/// assert!(was_closed(&PublishError::Closed));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PublishError {
    /// Publication metadata was changed to an invalid value by an interceptor.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// Required backend behavior is unavailable.
    #[error(transparent)]
    Capability(#[from] CapabilityError),
    /// Payload encoding failed.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// The selected provider failed to publish.
    #[error(transparent)]
    Spi(#[from] SpiError),
    /// All configured provider publish attempts reached a terminal retry
    /// outcome.
    #[error(transparent)]
    Retry(Box<RetryError<PublishAttemptError>>),
    /// A publisher interceptor panicked; `scope` identifies which chain ran.
    #[error("{scope} publisher interceptor panicked: {message}")]
    InterceptorPanicked {
        /// Either `typed` or `global`.
        scope: &'static str,
        /// Panic text when the panic payload is a string.
        message: Box<str>,
    },
    /// A publish error handler panicked while observing the terminal failure.
    #[error("publish error handler panicked: {message}")]
    ErrorHandlerPanicked {
        /// Panic text when the panic payload is a string.
        message: Box<str>,
        /// Original terminal publication failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The event bus has already closed.
    #[error("cannot publish after event bus shutdown")]
    Closed,
}

impl From<RetryError<PublishAttemptError>> for PublishError {
    /// Boxes the comparatively large retry report to keep this public error
    /// inexpensive to return by value.
    fn from(error: RetryError<PublishAttemptError>) -> Self {
        Self::Retry(Box::new(error))
    }
}
