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
#[derive(Debug)]
#[non_exhaustive]
pub enum EventBusProviderError {
    /// The created SPI does not satisfy one or more configured requirements.
    UnsupportedCapabilities {
        /// Stable capability labels absent from the provider output.
        missing: Vec<&'static str>,
    },
    /// Provider-specific construction failed.
    Provider(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for EventBusProviderError {
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
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedCapabilities { .. } => None,
            Self::Provider(source) => Some(source.as_ref()),
        }
    }
}

impl EventBusProviderError {
    /// Wraps a provider-specific error without changing its source chain.
    pub fn provider<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Provider(Box::new(source))
    }
}
