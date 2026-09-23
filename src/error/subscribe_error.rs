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
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubscribeError {
    /// Subscriber configuration is invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// Required backend behavior is unavailable.
    #[error(transparent)]
    Capability(#[from] CapabilityError),
    /// The selected provider failed to subscribe.
    #[error(transparent)]
    Spi(#[from] SpiError),
    /// The event bus has already closed.
    #[error("cannot subscribe after event bus shutdown")]
    Closed,
}
