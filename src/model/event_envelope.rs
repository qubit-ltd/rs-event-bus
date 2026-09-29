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
///
/// Header names and values are stored as owned UTF-8 strings in key order.
pub type Headers = BTreeMap<String, String>;

/// Reserved portable header identifying events published as dead letters.
///
/// Providers must preserve this header when transporting an event. Applications
/// and interceptors must not set or remove this reserved header themselves.
pub const DEAD_LETTER_HEADER: &str = "x-qubit-event-bus-dead-letter";
/// Current value of [`DEAD_LETTER_HEADER`].
pub const DEAD_LETTER_HEADER_VALUE: &str = "v1";

/// A type-safe event before any subscriber delivery is created.
///
/// # Type Parameters
/// - `T`: event payload type retained by shared ownership.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::EventEnvelope;
/// use qubit_event_bus::model::Topic;
///
/// let topic = Topic::<String>::new("orders.created").unwrap();
/// let event = EventEnvelope::new(topic, "order-1".to_owned()).unwrap();
/// assert_eq!(event.payload(), "order-1");
/// ```
#[must_use]
pub struct EventEnvelope<T: 'static> {
    /// Validated identifier shared with provider retries.
    pub(crate) id: EventId,
    /// Typed destination selected by the publisher.
    pub(crate) topic: Topic<T>,
    /// Shared payload allocation used across provider fan-out.
    pub(crate) payload: Arc<T>,
    /// Portable event headers in deterministic key order.
    pub(crate) headers: Headers,
    /// Optional provider ordering key.
    pub(crate) ordering_key: Option<Box<str>>,
    /// Event creation timestamp.
    pub(crate) timestamp: SystemTime,
    /// Requested provider delivery delay.
    pub(crate) delay: Option<Duration>,
}

impl<T: 'static> EventEnvelope<T> {
    /// Creates an event with a generated UUID v4, current timestamp, and empty
    /// metadata.
    ///
    /// # Parameters
    /// - `topic`: typed destination for the event.
    /// - `payload`: value retained by the envelope.
    ///
    /// # Returns
    /// An envelope with generated identity, current timestamp, and empty
    /// optional metadata.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] if UUID generation fails.
    pub fn new(topic: Topic<T>, payload: T) -> Result<Self, EventIdGenerationError> {
        Ok(Self::with_id(topic, payload, EventId::generate()?))
    }

    /// Creates an event around an existing shared payload and generated UUID
    /// v4.
    ///
    /// Sharing avoids copying non-`Clone` native values when an SPI fans one
    /// event out to multiple facade subscriptions.
    ///
    /// # Parameters
    /// - `topic`: typed destination for the event.
    /// - `payload`: shared payload allocation retained by the envelope.
    ///
    /// # Returns
    /// An envelope that shares `payload` and has generated event metadata.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] if UUID generation fails.
    pub fn from_shared_payload(topic: Topic<T>, payload: Arc<T>) -> Result<Self, EventIdGenerationError> {
        Ok(Self::with_id_and_shared_payload(topic, payload, EventId::generate()?))
    }

    /// Creates an event around an existing shared payload and validated ID.
    ///
    /// # Parameters
    /// - `topic`: typed destination for the event.
    /// - `payload`: shared payload allocation retained by the envelope.
    /// - `id`: validated event identifier to preserve.
    ///
    /// # Returns
    /// An envelope with the supplied identity and default metadata.
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
    ///
    /// # Parameters
    /// - `topic`: typed destination for the event.
    /// - `payload`: value to retain by shared ownership.
    /// - `id`: previously validated event identifier.
    ///
    /// # Returns
    /// An envelope with current timestamp and empty optional metadata.
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
    ///
    /// # Returns
    /// The validated identifier retained by the envelope.
    #[must_use = "Use the returned id."]
    #[inline]
    pub fn id(&self) -> &EventId {
        &self.id
    }
    /// Returns the typed topic.
    ///
    /// # Returns
    /// The typed destination borrowed from the envelope.
    #[must_use = "Use the returned topic."]
    #[inline]
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }
    /// Returns the payload without requiring it to be cloneable.
    ///
    /// # Returns
    /// The payload borrowed from its shared allocation.
    #[must_use = "Use the returned payload."]
    #[inline]
    pub fn payload(&self) -> &T {
        self.payload.as_ref()
    }
    /// Returns all event headers.
    ///
    /// # Returns
    /// The complete portable header map.
    #[must_use]
    #[inline]
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    /// Returns one header, or `None` when absent.
    ///
    /// # Parameters
    /// - `key`: header name to look up.
    ///
    /// # Returns
    /// The header value when present, otherwise `None`.
    #[inline]
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
    /// # Parameters
    /// - `key`: portable header name to insert or replace.
    /// - `value`: header text to associate with `key`.
    ///
    /// # Returns
    /// The previous value for the key, or `None` if it was absent.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] if the key is empty,
    /// contains unsupported characters, is reserved, or the value contains a
    /// control character.
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
    /// # Parameters
    /// - `key`: header name to remove.
    ///
    /// # Returns
    /// The removed value when present.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] if `key` names the
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

    /// Returns the ordering key, or `None` when delivery is unordered.
    ///
    /// # Returns
    /// The requested ordering key, or `None` when no key was set.
    #[inline]
    #[must_use = "Use the returned ordering key."]
    pub fn ordering_key(&self) -> Option<&str> {
        self.ordering_key.as_deref()
    }
    /// Returns the creation timestamp.
    ///
    /// # Returns
    /// The event creation time.
    #[must_use]
    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
    /// Returns the requested delay, or `None` for immediate delivery.
    ///
    /// # Returns
    /// The requested delivery delay, or `None` when delivery is immediate.
    #[inline]
    #[must_use = "Use the returned delay."]
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }
    /// Consumes the envelope and returns its payload.
    ///
    /// # Returns
    /// The shared payload owner without the envelope metadata.
    #[must_use]
    pub fn into_payload(self) -> Arc<T> {
        self.payload
    }

    /// Sets a facade-owned header after public caller validation is bypassed.
    ///
    /// # Parameters
    /// - `key`: system header name.
    /// - `value`: system header value.
    pub(crate) fn set_system_header(&mut self, key: &str, value: &str) {
        self.headers.insert(key.into(), value.into());
    }
}

impl<T: 'static> Clone for EventEnvelope<T> {
    /// Clones the metadata and shares the original payload allocation.
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
