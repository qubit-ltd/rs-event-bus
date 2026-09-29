// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Payload encoding and decoding failures.

use std::error::Error;

use crate::model::ContentType;
use crate::model::PayloadDirection;
use crate::model::SchemaId;

/// A registered codec failed to convert an event payload.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::CodecError;
///
/// let error = CodecError::PayloadTooLarge { direction: qubit_event_bus::model::PayloadDirection::Publish, actual: 128, limit: 64 };
/// assert!(matches!(error, CodecError::PayloadTooLarge { actual: 128, limit: 64, .. }));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum CodecError {
    /// Encoded payload exceeds the facade's configured byte limit.
    #[error("{direction:?} encoded event payload is {actual} bytes, exceeding the {limit}-byte limit")]
    PayloadTooLarge {
        /// Boundary whose positive byte limit was exceeded.
        direction: PayloadDirection,
        /// Number of bytes produced by the codec.
        actual: usize,
        /// Maximum number of encoded bytes allowed.
        limit: usize,
    },
    /// Received metadata is outside the codec's declared compatibility set.
    #[error(
        "encoded metadata mismatch: expected {expected_content_type:?}/{expected_schema_id:?}, received {actual_content_type:?}/{actual_schema_id:?}"
    )]
    MetadataMismatch {
        /// Required MIME text, preserving case.
        expected_content_type: ContentType,
        /// Received MIME text.
        actual_content_type: ContentType,
        /// Required schema, including explicit absence.
        expected_schema_id: Option<SchemaId>,
        /// Received schema, including explicit absence.
        actual_schema_id: Option<SchemaId>,
    },
    /// A native payload violated the subscribed Rust type contract.
    #[error("native payload type does not match subscribed topic")]
    NativeTypeMismatch,
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
