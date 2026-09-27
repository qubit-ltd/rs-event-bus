// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed codec contract used at the facade boundary.

use std::sync::Arc;

use crate::error::CodecError;
use crate::model::ContentType;
use crate::model::SchemaId;

/// Encodes and decodes payloads of type `T` without choosing a wire format
/// globally.
///
/// # Type Parameters
/// * `T` — application payload type handled by the codec.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::codec::EventCodec;
///
/// fn round_trip<T: Send + Sync + 'static>(codec: &dyn EventCodec<T>, value: &T) {
///     let bytes = codec.encode(value).expect("encoding succeeds");
///     let _decoded = codec.decode(&bytes).expect("decoding succeeds");
/// }
/// ```
pub trait EventCodec<T>: Send + Sync + 'static {
    /// Returns the MIME content type assigned to encoded payloads.
    ///
    /// # Returns
    /// The stable content type that accompanies bytes produced by
    /// [`Self::encode`].
    #[must_use]
    fn content_type(&self) -> &ContentType;
    /// Returns the schema identifier associated with encoded payloads.
    ///
    /// # Returns
    /// `Some` when the codec uses a schema, or `None` when its format is
    /// self-describing or has no schema identifier.
    #[must_use]
    fn schema_id(&self) -> Option<&SchemaId>;
    /// Encodes one application value into shared immutable bytes.
    ///
    /// # Parameters
    /// * `value` — payload to encode without taking ownership.
    ///
    /// # Returns
    /// Encoded bytes suitable for transport.
    ///
    /// # Errors
    /// Returns [`CodecError`] when the value cannot be represented by this
    /// codec; the underlying codec failure remains available as the source.
    fn encode(&self, value: &T) -> Result<Arc<[u8]>, CodecError>;
    /// Decodes bytes produced by this codec into an application value.
    ///
    /// # Parameters
    /// * `bytes` — encoded payload to decode without taking ownership.
    ///
    /// # Returns
    /// The decoded payload value.
    ///
    /// # Errors
    /// Returns [`CodecError`] when the bytes are invalid or cannot be
    /// interpreted by this codec; the underlying failure remains the source.
    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError>;
}
