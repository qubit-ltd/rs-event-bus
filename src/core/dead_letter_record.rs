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
            .with("subscriber_id", subscriber_id.to_string())
            .with("event_id", envelope.id().to_string())
            .with("topic", envelope.topic().name().to_string())
            .with("failure_reason", error.to_string())
            .with("failure_type", error.kind().to_string())
            .with("payload_type", type_name::<T>().to_string())
            .with("failed_at_unix_millis", failed_at_unix_millis)
            .with("dead_letter", true);
        if let Some(ordering_key) = envelope.ordering_key() {
            metadata.set("ordering_key", ordering_key.to_string());
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
            .with("subscriber_id", subscriber_id.to_string())
            .with("event_id", metadata.id().to_string())
            .with("topic", metadata.topic_name().to_string())
            .with("failure_reason", error.to_string())
            .with("failure_type", error.kind().to_string())
            .with("payload_type", metadata.payload_type_name().to_string())
            .with("failed_at_unix_millis", failed_at_unix_millis)
            .with("dead_letter", true);
        if let Some(ordering_key) = metadata.ordering_key() {
            record_metadata.set("ordering_key", ordering_key.to_string());
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
