// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests of the public typed event codec metadata contract.

use std::sync::Arc;

use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;

/// Codec that uses the trait's exact content-type and schema default.
struct StrictTextCodec {
    content_type: ContentType,
    schema_id: Option<SchemaId>,
}

impl EventCodec<String> for StrictTextCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        self.schema_id.as_ref()
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

/// Codec that accepts one documented alias for its canonical MIME type.
struct CompatibleTextCodec {
    content_type: ContentType,
    compatible_content_type: ContentType,
}

impl EventCodec<String> for CompatibleTextCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    fn validate_metadata(&self, payload: &EncodedPayload) -> Result<(), CodecError> {
        let content_type_matches =
            payload.content_type() == &self.content_type || payload.content_type() == &self.compatible_content_type;
        if content_type_matches && payload.schema_id().is_none() {
            Ok(())
        } else {
            Err(CodecError::MetadataMismatch {
                expected_content_type: self.content_type.clone(),
                actual_content_type: payload.content_type().clone(),
                expected_schema_id: None,
                actual_schema_id: payload.schema_id().cloned(),
            })
        }
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

fn encoded_payload(content_type: &str, schema_id: Option<&str>) -> EncodedPayload {
    EncodedPayload::new(
        Arc::from(b"value".as_slice()),
        ContentType::new(content_type).expect("test content type is valid"),
        schema_id.map(|value| SchemaId::new(value).expect("test schema ID is valid")),
    )
}

#[test]
fn test_event_codec_default_validation_accepts_exact_metadata() {
    let codec = StrictTextCodec {
        content_type: ContentType::new("text/plain").expect("codec content type is valid"),
        schema_id: Some(SchemaId::new("string-v1").expect("codec schema ID is valid")),
    };
    let payload = encoded_payload("text/plain", Some("string-v1"));

    codec
        .validate_metadata(&payload)
        .expect("matching content type and schema are accepted");
}

#[test]
fn test_event_codec_default_validation_reports_metadata_mismatch() {
    let codec = StrictTextCodec {
        content_type: ContentType::new("text/plain").expect("codec content type is valid"),
        schema_id: Some(SchemaId::new("string-v1").expect("codec schema ID is valid")),
    };
    let payload = encoded_payload("application/octet-stream", Some("string-v2"));

    let Err(CodecError::MetadataMismatch {
        expected_content_type,
        actual_content_type,
        expected_schema_id,
        actual_schema_id,
    }) = codec.validate_metadata(&payload)
    else {
        panic!("different content type and schema must report MetadataMismatch");
    };

    assert_eq!(expected_content_type.as_str(), "text/plain");
    assert_eq!(actual_content_type.as_str(), "application/octet-stream");
    assert_eq!(expected_schema_id.as_ref().map(SchemaId::as_str), Some("string-v1"));
    assert_eq!(actual_schema_id.as_ref().map(SchemaId::as_str), Some("string-v2"));
}

#[test]
fn test_event_codec_override_accepts_compatible_content_type() {
    let codec = CompatibleTextCodec {
        content_type: ContentType::new("text/plain").expect("codec content type is valid"),
        compatible_content_type: ContentType::new("text/x-string").expect("compatible content type is valid"),
    };
    let payload = encoded_payload("text/x-string", None);

    codec
        .validate_metadata(&payload)
        .expect("documented MIME alias is accepted");
}

#[test]
fn test_event_codec_override_reports_unsupported_metadata() {
    let codec = CompatibleTextCodec {
        content_type: ContentType::new("text/plain").expect("codec content type is valid"),
        compatible_content_type: ContentType::new("text/x-string").expect("compatible content type is valid"),
    };
    let payload = encoded_payload("application/json", None);

    let Err(CodecError::MetadataMismatch {
        expected_content_type,
        actual_content_type,
        expected_schema_id,
        actual_schema_id,
    }) = codec.validate_metadata(&payload)
    else {
        panic!("unsupported metadata must report MetadataMismatch");
    };

    assert_eq!(expected_content_type.as_str(), "text/plain");
    assert_eq!(actual_content_type.as_str(), "application/json");
    assert!(expected_schema_id.is_none());
    assert!(actual_schema_id.is_none());
}
