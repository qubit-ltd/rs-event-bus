// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Payload encoding and decoding failures.

use std::error::Error;

/// A registered codec failed to convert an event payload.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodecError {
    /// Encoding failed; the codec error remains available through `source()`.
    #[error("failed to encode event payload: {source}")]
    Encode {
        /// Original codec failure.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// Decoding failed; the codec error remains available through `source()`.
    #[error("failed to decode event payload: {source}")]
    Decode {
        /// Original codec failure.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
}
