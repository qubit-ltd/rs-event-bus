// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![no_main]

use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DEAD_LETTER_HEADER;
use qubit_event_bus::model::EventEnvelope;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::TopicAddress;

const METADATA_BUDGET: usize = 4 * 1024;

fuzz_target!(|input: &[u8]| {
    // Bound allocations and exercise the public transport-metadata validators.
    // The crate currently exposes no generic wire decoder; concrete codecs
    // belong to providers, so this target fuzzes the shared decode boundary.
    let bounded = &input[..input.len().min(METADATA_BUDGET)];
    let mut fields = bounded.splitn(5, |byte| *byte == 0xff);
    let event_id = fields.next().unwrap_or_default();
    let topic = fields.next().unwrap_or_default();
    let content_type = fields.next().unwrap_or_default();
    let schema_id = fields.next().unwrap_or_default();
    let header_value = fields.next().unwrap_or_default();

    if let Ok(value) = std::str::from_utf8(event_id)
        && let Ok(id) = EventId::new(value)
    {
        assert!((1..=128).contains(&id.as_str().len()));
    }
    if let Ok(value) = std::str::from_utf8(topic)
        && let Ok(address) = TopicAddress::new(value)
    {
        assert!((1..=255).contains(&address.as_str().len()));
    }
    if let Ok(value) = std::str::from_utf8(content_type) {
        let _ = ContentType::new(value);
    }
    if let Ok(value) = std::str::from_utf8(schema_id) {
        let _ = SchemaId::new(value);
    }

    if let (Ok(topic), Ok(header_value)) = (
        Topic::<Vec<u8>>::new("fuzz.transport"),
        std::str::from_utf8(header_value),
    ) {
        let id = EventId::new("fuzz-event").unwrap();
        let mut envelope = EventEnvelope::with_id_and_shared_payload(topic, Arc::new(Vec::new()), id);
        let _ = envelope.set_header("fuzz.value", header_value);
        for reserved in [DEAD_LETTER_HEADER, "X-QUBIT-EVENT-BUS-DEAD-LETTER"] {
            assert!(envelope.set_header(reserved, "secret-marker").is_err());
            assert!(envelope.remove_header(reserved).is_err());
        }
        assert!(
            envelope
                .headers()
                .keys()
                .all(|key| !key.eq_ignore_ascii_case(DEAD_LETTER_HEADER))
        );
    }
});
