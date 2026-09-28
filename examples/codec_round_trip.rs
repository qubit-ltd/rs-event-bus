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

struct Utf8Codec(ContentType);

impl EventCodec<String> for Utf8Codec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::from(value.as_bytes()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let codec = Utf8Codec(ContentType::new("text/plain")?);
    let original = String::from("order-created");
    let encoded = codec.encode(&original)?;
    let decoded = codec.decode(&encoded)?;
    assert_eq!(decoded, original);
    Ok(())
}
