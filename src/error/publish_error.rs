// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event publication failures.

use crate::error::CapabilityError;
use crate::error::CodecError;
use crate::error::SpiError;

/// An event could not be published.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PublishError {
    /// Required backend behavior is unavailable.
    #[error(transparent)]
    Capability(#[from] CapabilityError),
    /// Payload encoding failed.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// The selected provider failed to publish.
    #[error(transparent)]
    Spi(#[from] SpiError),
    /// The event bus has already closed.
    #[error("cannot publish after event bus shutdown")]
    Closed,
}
