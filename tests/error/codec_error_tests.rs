// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public display and source contracts for codec failures.

use std::error::Error;

use qubit_event_bus::error::CodecError;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::PayloadDirection;
use qubit_event_bus::model::SchemaId;

#[test]
fn test_codec_error_payload_limit_display_reports_boundary_and_sizes() {
    let error = CodecError::PayloadTooLarge {
        direction: PayloadDirection::Publish,
        actual: 128,
        limit: 64,
    };

    assert_eq!(
        error.to_string(),
        "Publish encoded event payload is 128 bytes, exceeding the 64-byte limit"
    );
}

#[test]
fn test_codec_error_metadata_mismatch_display_reports_expected_and_received_values() {
    let error = CodecError::MetadataMismatch {
        expected_content_type: ContentType::new_static("text/plain"),
        actual_content_type: ContentType::new_static("application/json"),
        expected_schema_id: Some(SchemaId::new_static("order-v1")),
        actual_schema_id: None,
    };

    assert_eq!(
        error.to_string(),
        "encoded metadata mismatch: expected ContentType(\"text/plain\")/Some(SchemaId(\"order-v1\")), received ContentType(\"application/json\")/None"
    );
}

#[test]
fn test_codec_error_native_type_mismatch_display_explains_contract_failure() {
    let error = CodecError::NativeTypeMismatch;

    assert_eq!(error.to_string(), "native payload type does not match subscribed topic");
}

#[test]
fn test_codec_error_encode_preserves_original_error_source() {
    let error = CodecError::Encode {
        source: Box::new(std::io::Error::other("encoder rejected value")),
    };

    assert_eq!(
        error.to_string(),
        "failed to encode event payload: encoder rejected value"
    );
    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("encoder rejected value")
    );
}

#[test]
fn test_codec_error_decode_preserves_original_error_source() {
    let error = CodecError::Decode {
        source: Box::new(std::io::Error::other("decoder rejected bytes")),
    };

    assert_eq!(
        error.to_string(),
        "failed to decode event payload: decoder rejected bytes"
    );
    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("decoder rejected bytes")
    );
}

#[test]
fn test_codec_error_panicked_display_preserves_operation_and_message() {
    let error = CodecError::Panicked {
        operation: "decode",
        message: "codec bug".into(),
    };

    assert_eq!(error.to_string(), "codec decode panicked: codec bug");
}
