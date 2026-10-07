// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors raised while constructing a synchronous or asynchronous facade.

use crate::error::ConfigurationError;
use crate::error::SpiError;

/// A facade could not be built from its configuration or provider SPI.
///
/// Each variant retains its source so callers can distinguish an invalid
/// middleware execution model from a provider capability failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum FacadeBuildError {
    /// The facade configuration contains middleware for another execution
    /// model.
    #[error("invalid facade configuration: {0}")]
    Configuration(#[from] ConfigurationError),
    /// Querying provider capabilities failed or panicked.
    #[error("provider capability query failed: {0}")]
    Spi(#[from] SpiError),
}
