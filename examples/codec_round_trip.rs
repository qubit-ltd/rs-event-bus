// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Minimal executable example of an event codec round trip.

use std::error::Error;
use std::sync::Arc;

use qubit_event_bus::CodecError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;

/// Encodes and decodes UTF-8 strings with the `text/plain` content type.
struct Utf8Codec(
    /// Media type reported for the encoded bytes.
    ContentType,
);

impl EventCodec<String> for Utf8Codec {
    /// Returns the codec's configured media type.
    fn content_type(&self) -> &ContentType {
        &self.0
    }

    /// Reports that this codec has no schema identifier.
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    /// Encodes the string as UTF-8 bytes.
    ///
    /// # Parameters
    /// - `value`: string payload to encode.
    ///
    /// # Returns
    /// The UTF-8 bytes shared through an `Arc`.
    ///
    /// # Errors
    /// This implementation does not fail while encoding UTF-8.
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    /// Decodes UTF-8 bytes into a string.
    ///
    /// # Parameters
    /// - `payload`: encoded UTF-8 bytes and codec metadata.
    ///
    /// # Returns
    /// The decoded string.
    ///
    /// # Errors
    /// Returns a decode error when the payload bytes are not valid UTF-8.
    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        let bytes = payload.bytes();
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let codec = Utf8Codec(ContentType::new("text/plain")?);
    let original = String::from("order-created");
    let encoded = codec.encode(&original)?;
    let payload = EncodedPayload::new(encoded, codec.content_type().clone(), None);
    codec.validate_metadata(&payload)?;
    let decoded = codec.decode(&payload)?;
    assert_eq!(decoded, original);
    Ok(())
}
