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
/// use std::sync::Arc;
///
/// use qubit_event_bus::codec::EventCodec;
/// use qubit_event_bus::error::CodecError;
/// use qubit_event_bus::model::ContentType;
/// use qubit_event_bus::model::SchemaId;
/// use qubit_event_bus::spi::EncodedPayload;
///
/// struct StringCodec;
///
/// static CONTENT_TYPE: ContentType = ContentType::new_static("text/plain");
///
/// impl StringCodec {
///     fn new() -> Self {
///         Self
///     }
/// }
///
/// impl EventCodec<String> for StringCodec {
///     fn content_type(&self) -> &ContentType {
///         &CONTENT_TYPE
///     }
///
///     fn schema_id(&self) -> Option<&SchemaId> {
///         None
///     }
///
///     fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
///         Ok(Arc::from(value.as_bytes()))
///     }
///
///     fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
///         String::from_utf8(payload.bytes().to_vec()).map_err(|error| CodecError::Decode {
///             source: Box::new(error),
///         })
///     }
/// }
///
/// let codec = StringCodec::new();
/// let value = String::from("hello");
/// let bytes = codec.encode(&value).expect("encoding succeeds");
/// let payload = EncodedPayload::new(
///     bytes,
///     codec.content_type().clone(),
///     codec.schema_id().cloned(),
/// );
/// codec.validate_metadata(&payload).expect("compatible metadata");
/// let decoded = codec.decode(&payload).expect("decoding succeeds");
/// assert_eq!(decoded, value);
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
    /// # Parameters
    /// - `payload`: Encoded payload whose content type and optional schema ID
    ///   must be compatible with this codec.
    ///
    /// # Returns
    /// `Ok(())` when the payload metadata satisfies this codec's policy. The
    /// default implementation requires exact content type and schema equality.
    ///
    /// # Errors
    /// The default implementation returns [`CodecError::MetadataMismatch`]
    /// when either metadata value differs. An override may return another
    /// documented [`CodecError`] for incompatible metadata. The payload bytes
    /// are not inspected.
    fn validate_metadata(&self, payload: &EncodedPayload) -> Result<(), CodecError> {
        if self.content_type() == payload.content_type() && self.schema_id() == payload.schema_id()
        {
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
