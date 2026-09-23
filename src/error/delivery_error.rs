// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Handler and delivery processing failures.

use std::error::Error;

use crate::error::CodecError;
use crate::error::SpiError;

/// An event delivery could not complete successfully.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeliveryError {
    /// The application handler returned an error.
    #[error("event handler failed: {source}")]
    Handler {
        /// Original application error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// The inbound payload could not be decoded.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// The backend failed while processing the delivery.
    #[error(transparent)]
    Spi(#[from] SpiError),
}
