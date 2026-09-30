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
///
/// Cloning this value shares the byte allocation and clones only the metadata.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use qubit_event_bus::model::ContentType;
/// use qubit_event_bus::spi::EncodedPayload;
///
/// let payload = EncodedPayload::new(
///     Arc::from([1_u8, 2, 3]),
///     ContentType::APPLICATION_OCTET_STREAM,
///     None,
/// );
/// assert_eq!(payload.bytes(), &[1, 2, 3]);
/// ```
#[derive(Clone)]
#[must_use]
pub struct EncodedPayload {
    /// Encoded bytes shared across clones and provider publication attempts.
    bytes: Arc<[u8]>,
    /// Media type describing how the byte allocation is encoded.
    content_type: ContentType,
    /// Optional schema identifier required by a compatible decoder.
    schema_id: Option<SchemaId>,
}

impl EncodedPayload {
    /// Creates an encoded payload from bytes, MIME content type, and optional
    /// schema ID.
    ///
    /// # Parameters
    /// - `bytes`: shared encoded byte allocation.
    /// - `content_type`: media type describing the bytes.
    /// - `schema_id`: optional schema identifier used to decode the bytes.
    ///
    /// # Returns
    /// An encoded payload retaining the byte allocation and metadata.
    pub fn new(bytes: Arc<[u8]>, content_type: ContentType, schema_id: Option<SchemaId>) -> Self {
        Self {
            bytes,
            content_type,
            schema_id,
        }
    }

    /// Returns the encoded bytes.
    ///
    /// # Returns
    /// The encoded byte slice.
    #[must_use]
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Returns the media type.
    ///
    /// # Returns
    /// The content type associated with the bytes.
    #[must_use]
    #[inline]
    pub fn content_type(&self) -> &ContentType {
        &self.content_type
    }
    /// Returns the optional schema identifier, or `None` when absent.
    ///
    /// # Returns
    /// `Some` with the schema ID when set, otherwise `None`.
    #[inline]
    #[must_use = "Use the returned schema id."]
    pub fn schema_id(&self) -> Option<&SchemaId> {
        self.schema_id.as_ref()
    }
}
