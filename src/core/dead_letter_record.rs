// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Standard dead-letter payload.

use std::any::{
    Any,
    type_name,
};
use std::sync::Arc;
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use qubit_metadata::Metadata;

use crate::{
    EventBusError,
    EventEnvelope,
    EventEnvelopeMetadata,
};

/// Metadata key containing the subscriber that produced a dead letter.
pub const DEAD_LETTER_SUBSCRIBER_ID: &str = "subscriber_id";

/// Metadata key containing the failed event identifier.
pub const DEAD_LETTER_EVENT_ID: &str = "event_id";

/// Metadata key containing the failed event topic.
pub const DEAD_LETTER_TOPIC: &str = "topic";

/// Metadata key containing the failure diagnostic text.
pub const DEAD_LETTER_FAILURE_REASON: &str = "failure_reason";

/// Metadata key containing the stable failure category.
pub const DEAD_LETTER_FAILURE_TYPE: &str = "failure_type";

/// Metadata key containing the original payload type name.
pub const DEAD_LETTER_PAYLOAD_TYPE: &str = "payload_type";

/// Metadata key containing the Unix-millisecond failure timestamp.
pub const DEAD_LETTER_FAILED_AT_UNIX_MILLIS: &str = "failed_at_unix_millis";

/// Metadata key marking a record as a dead letter.
pub const DEAD_LETTER_MARKER: &str = "dead_letter";

/// Metadata key containing an optional ordering key.
pub const DEAD_LETTER_ORDERING_KEY: &str = "ordering_key";

/// Type-erased original payload stored inside dead-letter records.
pub type DeadLetterOriginalPayload = Arc<dyn Any + Send + Sync + 'static>;

/// Standard payload used by dead-letter envelopes.
pub type DeadLetterPayload = DeadLetterRecord;

/// Standard dead-letter record with diagnostic metadata and original payload.
///
/// The standard `failure_reason` field contains caller-visible error text and
/// must be treated as untrusted diagnostic content. Consumers should apply an
/// explicit redaction policy before rendering this metadata to logs or other
/// externally visible sinks.
#[derive(Clone)]
pub struct DeadLetterRecord {
    metadata: Metadata,
    original_payload: DeadLetterOriginalPayload,
}

impl DeadLetterRecord {
    /// Creates a dead-letter record from metadata and an original payload.
    ///
    /// # Parameters
    /// - `metadata`: Diagnostic metadata for the failed delivery.
    /// - `original_payload`: Cloneable type-erased original payload.
    ///
    /// # Returns
    /// Dead-letter record ready to use as an envelope payload.
    pub fn new(
        metadata: Metadata,
        original_payload: DeadLetterOriginalPayload,
    ) -> Self {
        Self {
            metadata,
            original_payload,
        }
    }

    /// Creates a standard dead-letter record from a failed event.
    ///
    /// # Parameters
    /// - `subscriber_id`: Identifier of the failing subscriber.
    /// - `envelope`: Failed event envelope.
    /// - `error`: Final processing error.
    ///
    /// # Returns
    /// Dead-letter record containing standard metadata and the cloned payload.
    pub fn from_failure<T>(
        subscriber_id: &str,
        envelope: &EventEnvelope<T>,
        error: &EventBusError,
    ) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        let failed_at_unix_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or_default();
        let mut metadata = Metadata::new()
            .with(DEAD_LETTER_SUBSCRIBER_ID, subscriber_id)
            .with(DEAD_LETTER_EVENT_ID, envelope.id())
            .with(DEAD_LETTER_TOPIC, envelope.topic().name())
            .with(DEAD_LETTER_FAILURE_REASON, error.to_string())
            .with(DEAD_LETTER_FAILURE_TYPE, error.kind().to_string())
            .with(DEAD_LETTER_PAYLOAD_TYPE, type_name::<T>())
            .with(DEAD_LETTER_FAILED_AT_UNIX_MILLIS, failed_at_unix_millis)
            .with(DEAD_LETTER_MARKER, true);
        if let Some(ordering_key) = envelope.ordering_key() {
            metadata.set(DEAD_LETTER_ORDERING_KEY, ordering_key);
        }
        Self::new(metadata, Arc::new(envelope.payload().clone()))
    }

    /// Creates a standard dead-letter record from type-erased failure data.
    ///
    /// # Parameters
    /// - `subscriber_id`: Identifier of the failing subscriber.
    /// - `metadata`: Metadata from the failed event envelope.
    /// - `original_payload`: Type-erased cloned original payload.
    /// - `error`: Final processing error.
    ///
    /// # Returns
    /// Dead-letter record containing standard metadata and original payload.
    pub fn from_metadata_failure(
        subscriber_id: &str,
        metadata: EventEnvelopeMetadata,
        original_payload: DeadLetterOriginalPayload,
        error: &EventBusError,
    ) -> Self {
        let failed_at_unix_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or_default();
        let mut record_metadata = Metadata::new()
            .with(DEAD_LETTER_SUBSCRIBER_ID, subscriber_id)
            .with(DEAD_LETTER_EVENT_ID, metadata.id())
            .with(DEAD_LETTER_TOPIC, metadata.topic_name())
            .with(DEAD_LETTER_FAILURE_REASON, error.to_string())
            .with(DEAD_LETTER_FAILURE_TYPE, error.kind().to_string())
            .with(DEAD_LETTER_PAYLOAD_TYPE, metadata.payload_type_name())
            .with(DEAD_LETTER_FAILED_AT_UNIX_MILLIS, failed_at_unix_millis)
            .with(DEAD_LETTER_MARKER, true);
        if let Some(ordering_key) = metadata.ordering_key() {
            record_metadata.set(DEAD_LETTER_ORDERING_KEY, ordering_key);
        }
        Self::new(record_metadata, original_payload)
    }

    /// Returns diagnostic metadata for this dead-letter record.
    ///
    /// # Returns
    /// Metadata with standard failure fields and any caller-provided fields.
    ///
    /// In particular, `failure_reason` is not guaranteed to be secret-free;
    /// render the returned metadata through an explicit redaction policy when
    /// crossing a logging or external-output boundary.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Returns the type-erased original payload.
    ///
    /// # Returns
    /// Shared original payload as an [`Arc`].
    pub fn original_payload(&self) -> DeadLetterOriginalPayload {
        Arc::clone(&self.original_payload)
    }

    /// Downcasts the original payload by reference.
    ///
    /// # Returns
    /// `Some(&T)` when the original payload has type `T`.
    pub fn downcast_original_payload_ref<T>(&self) -> Option<&T>
    where
        T: 'static,
    {
        self.original_payload.as_ref().downcast_ref::<T>()
    }
}
