// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Standard event envelope.
// qubit-style: allow multiple-public-types

use std::collections::HashMap;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};
use std::time::{
    Duration,
    SystemTime,
};

use crate::{
    Acknowledgement,
    EventEnvelopeBuilder,
    Topic,
};

static NEXT_EVENT_ID: AtomicU64 = AtomicU64::new(1);

/// Standard message structure flowing through the event bus.
#[derive(Debug, Clone)]
pub struct EventEnvelope<T: 'static> {
    id: String,
    topic: Topic<T>,
    payload: T,
    headers: HashMap<String, String>,
    ordering_key: Option<String>,
    timestamp: SystemTime,
    delay: Option<Duration>,
    acknowledgement: Option<Acknowledgement>,
    dead_letter: bool,
}

/// Type-erased event metadata exposed to global interceptors.
#[derive(Debug, Clone)]
pub struct EventEnvelopeMetadata {
    id: String,
    topic_name: String,
    payload_type_name: &'static str,
    headers: HashMap<String, String>,
    ordering_key: Option<String>,
    timestamp: SystemTime,
    delay: Option<Duration>,
    dead_letter: bool,
}

impl EventEnvelopeMetadata {
    /// Returns the event ID.
    ///
    /// # Returns
    /// Stable event identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the topic name.
    ///
    /// # Returns
    /// Topic name without payload type information.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the Rust payload type name.
    ///
    /// # Returns
    /// Fully qualified payload type name.
    pub fn payload_type_name(&self) -> &'static str {
        self.payload_type_name
    }

    /// Returns event headers.
    ///
    /// # Returns
    /// Immutable header map.
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Returns the optional ordering key.
    ///
    /// # Returns
    /// `Some` when an ordering key was configured.
    pub fn ordering_key(&self) -> Option<&str> {
        self.ordering_key.as_deref()
    }

    /// Returns event creation timestamp.
    ///
    /// # Returns
    /// Timestamp assigned when the envelope was built.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns optional delivery delay.
    ///
    /// # Returns
    /// `Some` when delayed delivery metadata was configured.
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }

    /// Returns whether this metadata represents a dead letter.
    ///
    /// # Returns
    /// `true` if the source envelope is already a dead letter.
    pub fn is_dead_letter(&self) -> bool {
        self.dead_letter
    }

    /// Adds or replaces one header.
    ///
    /// # Parameters
    /// - `key`: Header key.
    /// - `value`: Header value converted to string.
    ///
    /// # Returns
    /// Updated metadata.
    pub fn with_header(mut self, key: &str, value: impl ToString) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    /// Removes one header.
    ///
    /// # Parameters
    /// - `key`: Header key to remove.
    ///
    /// # Returns
    /// Updated metadata.
    pub fn without_header(mut self, key: &str) -> Self {
        self.headers.remove(key);
        self
    }

    /// Sets the ordering key.
    ///
    /// # Parameters
    /// - `ordering_key`: Ordering key used by supporting backends.
    ///
    /// # Returns
    /// Updated metadata.
    pub fn with_ordering_key(mut self, ordering_key: &str) -> Self {
        self.ordering_key = Some(ordering_key.to_string());
        self
    }

    /// Clears the ordering key.
    ///
    /// # Returns
    /// Updated metadata without an ordering key.
    pub fn without_ordering_key(mut self) -> Self {
        self.ordering_key = None;
        self
    }

    /// Sets delayed delivery metadata.
    ///
    /// # Parameters
    /// - `delay`: Requested delivery delay.
    ///
    /// # Returns
    /// Updated metadata.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Clears delayed delivery metadata.
    ///
    /// # Returns
    /// Updated metadata without a delay.
    pub fn without_delay(mut self) -> Self {
        self.delay = None;
        self
    }
}

impl<T: 'static> EventEnvelope<T> {
    /// Creates a new event envelope with generated ID and current timestamp.
    ///
    /// # Parameters
    /// - `topic`: Topic associated with the payload.
    /// - `payload`: Business payload.
    ///
    /// # Returns
    /// A new event envelope with empty headers.
    pub fn create(topic: Topic<T>, payload: T) -> Self {
        Self {
            id: generate_event_id(),
            topic,
            payload,
            headers: HashMap::new(),
            ordering_key: None,
            timestamp: SystemTime::now(),
            delay: None,
            acknowledgement: None,
            dead_letter: false,
        }
    }

    /// Creates an event envelope builder.
    ///
    /// # Returns
    /// A builder with generated ID and current timestamp defaults.
    pub fn builder() -> EventEnvelopeBuilder<T> {
        EventEnvelopeBuilder::new()
    }

    /// Creates an event envelope from validated builder fields.
    ///
    /// # Parameters
    /// - `builder`: Builder with all required fields present.
    ///
    /// # Returns
    /// An immutable event envelope.
    pub(crate) fn from_builder(builder: EventEnvelopeBuilder<T>) -> Self {
        Self {
            id: builder.id,
            topic: builder
                .topic
                .expect("validated builder should contain a topic"),
            payload: builder
                .payload
                .expect("validated builder should contain a payload"),
            headers: builder.headers,
            ordering_key: builder.ordering_key,
            timestamp: builder.timestamp,
            delay: builder.delay,
            acknowledgement: builder.acknowledgement,
            dead_letter: builder.dead_letter,
        }
    }

    /// Returns the event ID.
    ///
    /// # Returns
    /// Stable event identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the event topic.
    ///
    /// # Returns
    /// Type-safe topic metadata.
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }

    /// Returns the event payload.
    ///
    /// # Returns
    /// Immutable payload reference.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Returns event headers.
    ///
    /// # Returns
    /// Immutable header map.
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Returns type-erased metadata for global interception.
    ///
    /// # Returns
    /// Cloned event metadata without exposing the typed payload.
    pub fn metadata(&self) -> EventEnvelopeMetadata {
        EventEnvelopeMetadata {
            id: self.id.clone(),
            topic_name: self.topic.name().to_string(),
            payload_type_name: self.topic.payload_type_name(),
            headers: self.headers.clone(),
            ordering_key: self.ordering_key.clone(),
            timestamp: self.timestamp,
            delay: self.delay,
            dead_letter: self.dead_letter,
        }
    }

    /// Returns the optional ordering key.
    ///
    /// # Returns
    /// `Some` when an ordering key was configured.
    pub fn ordering_key(&self) -> Option<&str> {
        self.ordering_key.as_deref()
    }

    /// Returns event creation timestamp.
    ///
    /// # Returns
    /// Timestamp assigned when the envelope was built.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns optional delivery delay.
    ///
    /// # Returns
    /// `Some` when delayed delivery metadata was configured.
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }

    /// Returns optional acknowledgement handle.
    ///
    /// # Returns
    /// `Some` for envelopes delivered to subscriber handlers.
    pub fn acknowledgement(&self) -> Option<&Acknowledgement> {
        self.acknowledgement.as_ref()
    }

    /// Returns whether this envelope represents a dead letter.
    ///
    /// # Returns
    /// `true` if the envelope has already been routed to a dead-letter flow.
    pub fn is_dead_letter(&self) -> bool {
        self.dead_letter
    }

    /// Adds or replaces one header.
    ///
    /// # Parameters
    /// - `key`: Header key.
    /// - `value`: Header value converted to string.
    ///
    /// # Returns
    /// Updated envelope.
    pub fn with_header(
        mut self,
        key: impl Into<String>,
        value: impl ToString,
    ) -> Self {
        self.headers.insert(key.into(), value.to_string());
        self
    }

    /// Sets the ordering key.
    ///
    /// # Parameters
    /// - `ordering_key`: Ordering key used by backends that support ordering.
    ///
    /// # Returns
    /// Updated envelope.
    pub fn with_ordering_key(
        mut self,
        ordering_key: impl Into<String>,
    ) -> Self {
        self.ordering_key = Some(ordering_key.into());
        self
    }

    /// Sets delayed delivery metadata.
    ///
    /// # Parameters
    /// - `delay`: Requested delivery delay.
    ///
    /// # Returns
    /// Updated envelope.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Injects an acknowledgement handle.
    ///
    /// # Parameters
    /// - `acknowledgement`: Handle shared with processing code.
    ///
    /// # Returns
    /// Updated envelope.
    pub fn with_acknowledgement(
        mut self,
        acknowledgement: Acknowledgement,
    ) -> Self {
        self.acknowledgement = Some(acknowledgement);
        self
    }

    /// Marks the envelope as a dead letter.
    ///
    /// # Returns
    /// Updated envelope with dead-letter marker enabled.
    pub fn as_dead_letter(mut self) -> Self {
        self.dead_letter = true;
        self
    }

    /// Applies mutable metadata fields returned by a global interceptor.
    pub(crate) fn apply_metadata(&mut self, metadata: EventEnvelopeMetadata) {
        self.headers = metadata.headers;
        self.ordering_key = metadata.ordering_key;
        self.delay = metadata.delay;
    }
}

/// Generates a process-local event ID.
///
/// # Returns
/// Monotonic event ID string.
pub(crate) fn generate_event_id() -> String {
    let id = NEXT_EVENT_ID.fetch_add(1, Ordering::SeqCst);
    format!("event-{id}")
}
