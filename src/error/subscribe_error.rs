// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscription creation failures.

use crate::error::CapabilityError;
use crate::error::ConfigurationError;
use crate::error::SpiError;

/// A subscription could not be created.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::SubscribeError;
///
/// let error = SubscribeError::ResourceLimit {
///     resource: "subscription workers",
///     limit: 4,
/// };
/// assert!(matches!(
///     error,
///     SubscribeError::ResourceLimit {
///         resource: "subscription workers",
///         limit: 4,
///     }
/// ));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum SubscribeError {
    /// Subscriber configuration is invalid.
    #[error(transparent)]
    Configuration(
        /// Validation failure for the subscription configuration.
        #[from]
        ConfigurationError,
    ),
    /// Required backend behavior is unavailable.
    #[error(transparent)]
    Capability(
        /// Required provider capability that is unavailable.
        #[from]
        CapabilityError,
    ),
    /// The configured subscription worker budget is exhausted.
    #[error("event bus resource limit reached for {resource} (limit {limit})")]
    ResourceLimit {
        /// Stable resource name.
        resource: &'static str,
        /// Configured maximum number of resources.
        limit: usize,
    },
    /// The selected provider failed to subscribe.
    #[error(transparent)]
    Spi(
        /// Failure returned by the selected provider.
        #[from]
        SpiError,
    ),
    /// The event bus has already closed.
    #[error("cannot subscribe after event bus shutdown")]
    Closed,
}
