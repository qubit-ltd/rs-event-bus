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
    /// Encoded payload exceeds the facade's configured byte limit.
    #[error("encoded event payload is {actual} bytes, exceeding the {limit}-byte limit")]
    PayloadTooLarge {
        /// Number of bytes produced by the codec.
        actual: usize,
        /// Maximum number of encoded bytes allowed.
        limit: usize,
    },
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
    /// The codec panicked while performing an operation.
    #[error("codec {operation} panicked: {message}")]
    Panicked {
        /// Codec operation that panicked.
        operation: &'static str,
        /// Panic payload rendered as text when possible.
        message: Box<str>,
    },
}
