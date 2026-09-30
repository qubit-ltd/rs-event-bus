// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests of the public typed codec registry contract.

use std::sync::Arc;

use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;

/// Encodes UTF-8 strings while exposing a configurable registry marker.
struct TestStringCodec {
    content_type: ContentType,
}

impl EventCodec<String> for TestStringCodec {
    /// Returns the marker identifying this codec instance.
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    /// Reports that the test codec does not use a schema identifier.
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    /// Encodes a string as shared UTF-8 bytes.
    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    /// Decodes UTF-8 bytes from the supplied payload.
    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

/// Creates a string codec with a valid marker for registry assertions.
fn string_codec(content_type: &str) -> Arc<dyn EventCodec<String>> {
    Arc::new(TestStringCodec {
        content_type: ContentType::new(content_type).expect("test content type is valid"),
    })
}

#[test]
fn test_codec_registry_returns_none_for_unregistered_payload_type() {
    let registry = CodecRegistry::new();

    assert!(registry.get::<String>().is_none());
}

#[test]
fn test_codec_registry_returns_shared_codec_by_payload_type() {
    let mut registry = CodecRegistry::new();
    let codec = string_codec("text/plain");
    registry.register(codec.clone());

    let retrieved = registry.get::<String>().expect("string codec is registered");
    assert!(Arc::ptr_eq(&codec, &retrieved));
    assert_eq!(retrieved.content_type().as_str(), "text/plain");
    assert!(registry.get::<u32>().is_none());
}

#[test]
fn test_codec_registry_replaces_codec_for_same_payload_type() {
    let mut registry = CodecRegistry::new();
    registry.register(string_codec("text/plain"));
    registry.register(string_codec("text/csv"));

    let retrieved = registry.get::<String>().expect("replacement codec is registered");
    assert_eq!(retrieved.content_type().as_str(), "text/csv");
}
