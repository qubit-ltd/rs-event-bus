// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests the crate-internal topic codec selection contract.

use std::sync::Arc;

use crate::codec::CodecRegistry;
use crate::codec::EventCodec;
use crate::codec::resolve_codec;
use crate::error::CodecError;
use crate::model::ContentType;
use crate::model::SchemaId;
use crate::model::Topic;
use crate::spi::EncodedPayload;

struct TestCodec {
    content_type: ContentType,
}

impl EventCodec<String> for TestCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
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

fn create_codec(content_type: &'static str) -> Arc<dyn EventCodec<String>> {
    Arc::new(TestCodec {
        content_type: ContentType::new_static(content_type),
    })
}

#[test]
fn test_resolve_codec_prefers_topic_codec() {
    let topic_codec = create_codec("application/topic-codec");
    let registry_codec = create_codec("application/registry-codec");
    let topic = Topic::new_with_shared_codec("orders.created", Arc::clone(&topic_codec)).expect("topic name is valid");
    let mut registry = CodecRegistry::new();
    registry.register(Arc::clone(&registry_codec));

    let resolved = resolve_codec(&topic, &registry).expect("topic codec is available");

    assert!(Arc::ptr_eq(&resolved, &topic_codec));
}

#[test]
fn test_resolve_codec_falls_back_to_registry() {
    let registry_codec = create_codec("application/registry-codec");
    let topic = Topic::<String>::new("orders.created").expect("topic name is valid");
    let mut registry = CodecRegistry::new();
    registry.register(Arc::clone(&registry_codec));

    let resolved = resolve_codec(&topic, &registry).expect("registry codec is available");

    assert!(Arc::ptr_eq(&resolved, &registry_codec));
}
