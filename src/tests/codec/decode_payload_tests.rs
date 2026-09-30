// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests the crate-internal provider payload decoding contract.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::codec::EventCodec;
use crate::codec::decode_payload;
use crate::error::CodecError;
use crate::model::ContentType;
use crate::model::PayloadDirection;
use crate::model::SchemaId;
use crate::spi::EncodedPayload;
use crate::spi::TransportPayload;

/// Tracks codec callbacks so size rejection can verify callback ordering.
struct TrackingCodec {
    content_type: ContentType,
    validation_calls: Arc<AtomicUsize>,
    decode_calls: Arc<AtomicUsize>,
}

impl EventCodec<String> for TrackingCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    fn validate_metadata(&self, _: &EncodedPayload) -> Result<(), CodecError> {
        self.validation_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        self.decode_calls.fetch_add(1, Ordering::Relaxed);
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

/// Creates a codec and shared callback counters for one isolated test.
fn create_tracking_codec() -> (Arc<dyn EventCodec<String>>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let validation_calls = Arc::new(AtomicUsize::new(0));
    let decode_calls = Arc::new(AtomicUsize::new(0));
    let codec = Arc::new(TrackingCodec {
        content_type: ContentType::new("text/plain").expect("content type is valid"),
        validation_calls: validation_calls.clone(),
        decode_calls: decode_calls.clone(),
    });
    (codec, validation_calls, decode_calls)
}

#[test]
fn test_decode_payload_decodes_encoded_value() {
    let (codec, validation_calls, decode_calls) = create_tracking_codec();
    let payload = TransportPayload::Encoded(EncodedPayload::new(
        Arc::from(&b"order-created"[..]),
        codec.content_type().clone(),
        None,
    ));

    let decoded = decode_payload(
        Some(&codec),
        &payload,
        NonZeroUsize::new(64).expect("limit is positive"),
    )
    .expect("valid encoded payload decodes");

    assert_eq!(decoded.as_str(), "order-created");
    assert_eq!(validation_calls.load(Ordering::Relaxed), 1);
    assert_eq!(decode_calls.load(Ordering::Relaxed), 1);
}

#[test]
fn test_decode_payload_rejects_oversized_before_codec_callbacks() {
    let (codec, validation_calls, decode_calls) = create_tracking_codec();
    let payload = TransportPayload::Encoded(EncodedPayload::new(
        Arc::from(&b"large"[..]),
        codec.content_type().clone(),
        None,
    ));

    let error = decode_payload(Some(&codec), &payload, NonZeroUsize::new(4).expect("limit is positive"))
        .expect_err("payload above the configured receive limit is rejected");

    assert!(matches!(
        error,
        CodecError::PayloadTooLarge {
            direction: PayloadDirection::Receive,
            actual: 5,
            limit: 4,
        }
    ));
    assert_eq!(validation_calls.load(Ordering::Relaxed), 0);
    assert_eq!(decode_calls.load(Ordering::Relaxed), 0);
}
