// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider selection and creation failures.

use std::error::Error;

use crate::error::SpiError;

/// A provider could not be resolved, validated, or created.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProviderError {
    /// Provider resolution failed before SPI creation.
    #[error("event bus provider resolution failed: {source}")]
    Resolution {
        /// Original provider registry failure.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// A provider failed to create or validate its SPI.
    #[error(transparent)]
    Spi(#[from] SpiError),
}
