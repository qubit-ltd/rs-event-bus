// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Serialized payload bytes and codec metadata.

use std::sync::Arc;

use crate::model::ContentType;
use crate::model::SchemaId;

/// Encoded event bytes together with their codec metadata.
pub struct EncodedPayload {
    bytes: Arc<[u8]>,
    content_type: ContentType,
    schema_id: Option<SchemaId>,
}

impl EncodedPayload {
    /// Creates an encoded payload from bytes, MIME content type, and optional
    /// schema ID.
    pub fn new(bytes: Arc<[u8]>, content_type: ContentType, schema_id: Option<SchemaId>) -> Self {
        Self {
            bytes,
            content_type,
            schema_id,
        }
    }

    /// Returns the encoded bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Returns the media type.
    pub fn content_type(&self) -> &ContentType {
        &self.content_type
    }
    /// Returns the optional schema identifier, or `None` when absent.
    pub fn schema_id(&self) -> Option<&SchemaId> {
        self.schema_id.as_ref()
    }
}
