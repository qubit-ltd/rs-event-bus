// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed codec contract used at the facade boundary.

use std::sync::Arc;

use crate::error::CodecError;
use crate::model::ContentType;
use crate::model::SchemaId;

/// Encodes and decodes payloads of type `T` without choosing a wire format
/// globally.
pub trait EventCodec<T>: Send + Sync + 'static {
    /// Returns the encoded MIME content type.
    #[must_use]
    fn content_type(&self) -> &ContentType;
    /// Returns a schema identifier, or `None` when the format has no schema.
    #[must_use]
    fn schema_id(&self) -> Option<&SchemaId>;
    /// Encodes `value`; codec failures retain their source in [`CodecError`].
    fn encode(&self, value: &T) -> Result<Arc<[u8]>, CodecError>;
    /// Decodes `bytes`; codec failures retain their source in [`CodecError`].
    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError>;
}
