// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receiving failures.

use qubit_clock::TimeError;

use crate::error::CodecError;
use crate::error::SpiError;

/// A subscription could not receive or decode its next delivery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReceiveError {
    /// The backend could not receive an event.
    #[error(transparent)]
    Spi(#[from] SpiError),
    /// An inbound payload could not be decoded.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// The injected timer failed while backing off a provider settlement retry.
    #[error("event bus timer failed while retrying settlement: {0}")]
    Timer(#[from] TimeError),
    /// The subscription has closed.
    #[error("cannot receive after subscription close")]
    Closed,
}
