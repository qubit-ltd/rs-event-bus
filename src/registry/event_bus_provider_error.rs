// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors returned while validating event-bus provider output.

use std::error::Error;
use std::fmt;

/// Domain error used by event-bus providers during service creation.
///
/// Provider crates may use this type for configuration and initialization
/// failures; the registry additionally uses it to classify capability mismatch
/// as an unsupported candidate, which allows `qubit-spi` fallback policy to
/// act.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::registry::EventBusProviderError;
///
/// let error = EventBusProviderError::provider(std::io::Error::other("offline"));
/// assert!(std::error::Error::source(&error).is_some());
/// ```
#[derive(Debug)]
#[non_exhaustive]
#[must_use]
pub enum EventBusProviderError {
    /// The created SPI does not satisfy one or more configured requirements.
    UnsupportedCapabilities {
        /// Stable capability labels absent from the provider output.
        missing: Vec<&'static str>,
    },
    /// Provider-specific construction failed.
    Provider(
        /// Source error returned by the provider implementation.
        Box<dyn Error + Send + Sync>,
    ),
}

impl fmt::Display for EventBusProviderError {
    /// Formats the provider classification and any retained source.
    ///
    /// # Parameters
    /// - `formatter`: output formatter receiving the error text.
    ///
    /// # Returns
    /// The formatter result, including any output error.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCapabilities { missing } => {
                write!(
                    formatter,
                    "provider lacks required capabilities: {}",
                    missing.join(", ")
                )
            }
            Self::Provider(source) => write!(formatter, "event-bus provider failed: {source}"),
        }
    }
}

impl Error for EventBusProviderError {
    /// Returns the wrapped provider error when this value has one.
    ///
    /// # Returns
    /// The provider source for `Provider`, or `None` for a capability mismatch.
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedCapabilities { .. } => None,
            Self::Provider(source) => Some(source.as_ref()),
        }
    }
}

impl EventBusProviderError {
    /// Wraps a provider-specific error without changing its source chain.
    ///
    /// # Type Parameters
    /// - `E`: concrete error type supplied by the provider.
    ///
    /// # Parameters
    /// - `source`: provider error retained as the source of this value.
    ///
    /// # Returns
    /// A provider error that preserves `source` for error-chain inspection.
    pub fn provider<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Provider(Box::new(source))
    }
}
