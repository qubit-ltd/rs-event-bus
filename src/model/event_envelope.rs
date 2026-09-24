// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Published event data and metadata without delivery acknowledgement state.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use super::EventId;
use super::Topic;
use crate::error::ConfigurationError;
use crate::error::EventIdGenerationError;

/// String headers carried with an event.
pub type Headers = BTreeMap<String, String>;

/// Reserved portable header identifying events published as dead letters.
///
/// Providers must preserve this header when transporting an event. Applications
/// and interceptors must not set or remove this reserved header themselves.
pub const DEAD_LETTER_HEADER: &str = "x-qubit-event-bus-dead-letter";
/// Current value of [`DEAD_LETTER_HEADER`].
pub const DEAD_LETTER_HEADER_VALUE: &str = "v1";

/// A type-safe event before any subscriber delivery is created.
pub struct EventEnvelope<T: 'static> {
    pub(crate) id: EventId,
    pub(crate) topic: Topic<T>,
    pub(crate) payload: Arc<T>,
    pub(crate) headers: Headers,
    pub(crate) ordering_key: Option<Box<str>>,
    pub(crate) timestamp: SystemTime,
    pub(crate) delay: Option<Duration>,
}

impl<T: 'static> EventEnvelope<T> {
    /// Creates an event with a generated UUID v4, current timestamp, and empty
    /// metadata.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] if the operating-system random source
    /// cannot provide bytes for the generated identifier.
    pub fn new(topic: Topic<T>, payload: T) -> Result<Self, EventIdGenerationError> {
        Ok(Self::with_id(topic, payload, EventId::generate()?))
    }

    /// Creates an event around an existing shared payload and generated UUID
    /// v4.
    ///
    /// Sharing avoids copying non-`Clone` native values when an SPI fans one
    /// event out to multiple facade subscriptions.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] when the operating-system random
    /// source cannot provide the generated identifier.
    pub fn from_shared_payload(topic: Topic<T>, payload: Arc<T>) -> Result<Self, EventIdGenerationError> {
        Ok(Self::with_id_and_shared_payload(topic, payload, EventId::generate()?))
    }

    /// Creates an event around an existing shared payload and validated ID.
    pub fn with_id_and_shared_payload(topic: Topic<T>, payload: Arc<T>, id: EventId) -> Self {
        Self {
            id,
            topic,
            payload,
            headers: Headers::new(),
            ordering_key: None,
            timestamp: SystemTime::now(),
            delay: None,
        }
    }

    /// Creates an event using an already-validated identifier.
    ///
    /// The identifier is supplied by a request builder when the caller
    /// explicitly provides one, avoiding random generation on that path.
    pub(crate) fn with_id(topic: Topic<T>, payload: T, id: EventId) -> Self {
        Self {
            id,
            topic,
            payload: Arc::new(payload),
            headers: Headers::new(),
            ordering_key: None,
            timestamp: SystemTime::now(),
            delay: None,
        }
    }

    /// Returns the event identifier.
    pub fn id(&self) -> &EventId {
        &self.id
    }
    /// Returns the typed topic.
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }
    /// Returns the payload without requiring it to be cloneable.
    pub fn payload(&self) -> &T {
        self.payload.as_ref()
    }
    /// Returns all event headers.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    /// Returns one header, or `None` when absent.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(String::as_str)
    }
    /// Inserts or replaces a validated portable header.
    ///
    /// Publisher interceptors may use this to add tracing, tenancy, or other
    /// portable metadata without reconstructing the event or changing its ID.
    /// The reserved dead-letter marker is managed only by the facade and
    /// cannot be supplied through this method.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] when the key is empty,
    /// contains characters outside ASCII alphanumerics, `-`, `_`, and `.`, or
    /// the value contains a control character.
    pub fn set_header(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, ConfigurationError> {
        let key = key.into();
        let value = value.into();
        if key.eq_ignore_ascii_case(DEAD_LETTER_HEADER) {
            return Err(ConfigurationError::InvalidField {
                field: "event_header",
                message: "the dead-letter header is reserved for facade use".into(),
            });
        }
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || value.chars().any(char::is_control)
        {
            return Err(ConfigurationError::InvalidField {
                field: "event_header",
                message: "invalid key or control character in value".into(),
            });
        }
        Ok(self.headers.insert(key, value))
    }
    /// Removes a header and returns its previous value, if present.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] when asked to remove the
    /// reserved dead-letter marker.
    pub fn remove_header(&mut self, key: &str) -> Result<Option<String>, ConfigurationError> {
        if key.eq_ignore_ascii_case(DEAD_LETTER_HEADER) {
            return Err(ConfigurationError::InvalidField {
                field: "event_header",
                message: "the dead-letter header is reserved for facade use".into(),
            });
        }
        Ok(self.headers.remove(key))
    }

    /// Sets a facade-owned header after public caller validation is bypassed.
    pub(crate) fn set_system_header(&mut self, key: &str, value: &str) {
        self.headers.insert(key.into(), value.into());
    }
    /// Returns the ordering key, or `None` when delivery is unordered.
    pub fn ordering_key(&self) -> Option<&str> {
        self.ordering_key.as_deref()
    }
    /// Returns the creation timestamp.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
    /// Returns the requested delay, or `None` for immediate delivery.
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }
    /// Consumes the envelope and returns its payload.
    pub fn into_payload(self) -> Arc<T> {
        self.payload
    }
}

impl<T: 'static> Clone for EventEnvelope<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            topic: self.topic.clone(),
            payload: self.payload.clone(),
            headers: self.headers.clone(),
            ordering_key: self.ordering_key.clone(),
            timestamp: self.timestamp,
            delay: self.delay,
        }
    }
}
