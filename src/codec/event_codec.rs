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
use crate::spi::EncodedPayload;

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
///     let payload = qubit_event_bus::spi::EncodedPayload::new(bytes, codec.content_type().clone(), codec.schema_id().cloned());
///     codec.validate_metadata(&payload).expect("compatible metadata");
///     let _decoded = codec.decode(&payload).expect("decoding succeeds");
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
    /// Validates received content type and optional schema before decoding.
    ///
    /// Exact text and Option equality is the default. Override this method to
    /// permit a documented set of compatible MIME texts or schema versions.
    ///
    /// # Returns
    /// `Ok(())` when the payload metadata matches the codec configuration.
    ///
    /// # Errors
    /// Returns [`CodecError::MetadataMismatch`] when either the content type
    /// or schema identifier differs, without inspecting the payload bytes.
    fn validate_metadata(&self, payload: &EncodedPayload) -> Result<(), CodecError> {
        if self.content_type() == payload.content_type() && self.schema_id() == payload.schema_id() {
            Ok(())
        } else {
            Err(CodecError::MetadataMismatch {
                expected_content_type: self.content_type().clone(),
                actual_content_type: payload.content_type().clone(),
                expected_schema_id: self.schema_id().cloned(),
                actual_schema_id: payload.schema_id().cloned(),
            })
        }
    }
    /// Decodes a complete payload with content type and schema metadata.
    ///
    /// # Parameters
    /// * `payload` — encoded bytes and metadata to decode without taking
    ///   ownership.
    ///
    /// # Returns
    /// The decoded payload value.
    ///
    /// # Errors
    /// Returns [`CodecError`] when the bytes are invalid or cannot be
    /// interpreted by this codec; the underlying failure remains the source.
    fn decode(&self, payload: &EncodedPayload) -> Result<T, CodecError>;
}
