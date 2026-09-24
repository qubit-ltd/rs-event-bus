// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors produced while constructing a standard dead-letter event.

use crate::error::ConfigurationError;
use crate::error::EventIdGenerationError;

/// Failure while validating or creating the standard dead-letter event.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DeadLetterBuildError {
    /// The configured dead-letter topic is invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// A new event identifier could not be generated.
    #[error(transparent)]
    EventId(#[from] EventIdGenerationError),
}
