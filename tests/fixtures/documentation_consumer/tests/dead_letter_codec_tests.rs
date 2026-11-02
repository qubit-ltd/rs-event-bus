// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! External dead-letter codec round-trip tests.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use event_bus_documentation_consumer::dead_letter_codec::DeadLetterCodec;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EncodedPayload;

fn make_dead_letter(reason: &str, payload: &str) -> DeadLetterEvent<String> {
    let timestamp = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
    let (envelope, _) = PublishRequest::builder()
        .topic(Topic::<String>::new("orders.dead.source").expect("valid topic"))
        .payload(payload.to_owned())
        .event_id(EventId::new("event-42").expect("valid event ID"))
        .timestamp(timestamp)
        .header("trace-id", "trace-λ")
        .header("attempt", "3")
        .ordering_key("order|42")
        .build()
        .expect("valid request")
        .into_parts();
    DeadLetterEvent::new(
        Arc::new(envelope),
        SubscriberId::new("worker-alpha").expect("valid subscriber ID"),
        reason.into(),
    )
}

fn round_trip(value: &DeadLetterEvent<String>) -> DeadLetterEvent<String> {
    let encoder = DeadLetterCodec;
    let bytes = encoder.encode(value).expect("encode succeeds");
    let payload = EncodedPayload::new(bytes, encoder.content_type().clone(), None);
    let decoder = DeadLetterCodec;
    decoder
        .validate_metadata(&payload)
        .expect("metadata is validated before decode");
    decoder.decode(&payload).expect("decode succeeds")
}

fn assert_round_trip(value: &DeadLetterEvent<String>) {
    let decoded = round_trip(value);
    let original = value.original_event();
    let decoded_original = decoded.original_event();
    assert_eq!(decoded_original.id(), original.id());
    assert_eq!(decoded_original.topic().name(), original.topic().name());
    assert_eq!(decoded_original.timestamp(), original.timestamp());
    assert_eq!(decoded_original.headers(), original.headers());
    assert_eq!(decoded_original.ordering_key(), original.ordering_key());
    assert_eq!(decoded_original.payload(), original.payload());
    assert_eq!(decoded.subscriber_id(), value.subscriber_id());
    assert_eq!(decoded.reason(), value.reason());
}

#[test]
fn test_encoded_dead_letter_round_trips_all_supported_fields() {
    let value = make_dead_letter("write failed: line 1\n分隔|符", "订单|42\nready");
    assert_round_trip(&value);
}

#[test]
fn test_encoded_dead_letter_round_trips_empty_reason() {
    let value = make_dead_letter("", "payload");
    assert_round_trip(&value);
}

#[test]
fn test_decoder_rejects_malformed_bytes_as_decode_errors() {
    let codec = DeadLetterCodec;
    let valid_bytes = codec
        .encode(&make_dead_letter("reason", "payload"))
        .expect("encode succeeds")
        .to_vec();
    let mut invalid_event_id = valid_bytes.clone();
    let event_id_length = u32::from_be_bytes(invalid_event_id[..4].try_into().expect("length"));
    invalid_event_id.splice(4..4 + event_id_length as usize, []);
    invalid_event_id[..4].copy_from_slice(&0_u32.to_be_bytes());

    let mut overflowing_timestamp = valid_bytes.clone();
    let event_id_length = u32::from_be_bytes(overflowing_timestamp[..4].try_into().expect("length"));
    let topic_length_offset = 4 + event_id_length as usize;
    let topic_length = u32::from_be_bytes(
        overflowing_timestamp[topic_length_offset..topic_length_offset + 4]
            .try_into()
            .expect("length"),
    );
    let seconds_offset = topic_length_offset + 4 + topic_length as usize;
    overflowing_timestamp[seconds_offset..seconds_offset + 8].copy_from_slice(&u64::MAX.to_be_bytes());

    for bytes in [
        vec![0],
        vec![0, 0, 0, 1, 0xff],
        invalid_event_id,
        overflowing_timestamp,
        {
            let mut bytes = valid_bytes.clone();
            bytes.push(0);
            bytes
        },
        vec![0_u8; 1_048_577],
    ] {
        let payload = EncodedPayload::new(Arc::from(bytes), codec.content_type().clone(), None);
        assert!(matches!(
            codec.decode(&payload),
            Err(qubit_event_bus::CodecError::Decode { .. })
        ));
    }
}

#[test]
fn test_decoder_rejects_metadata_before_decoding_bytes() {
    let codec = DeadLetterCodec;
    let payload = EncodedPayload::new(Arc::from([0_u8]), ContentType::APPLICATION_OCTET_STREAM, None);
    assert!(matches!(
        codec.validate_metadata(&payload),
        Err(qubit_event_bus::CodecError::MetadataMismatch { .. })
    ));
}
