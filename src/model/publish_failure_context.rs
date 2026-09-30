// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event data made available to terminal publish error observers.

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use super::EventEnvelope;
use super::EventId;
use super::Headers;
use super::Topic;

/// Immutable event information supplied after a publish attempt is terminal.
///
/// The context owns a shared payload, so native publication failures can be
/// inspected even when the payload does not implement [`Clone`].
///
/// # Type Parameters
/// - `T`: payload type attached to the failed event.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::PublishFailureContext;
///
/// fn inspect<T: 'static>(context: &PublishFailureContext<T>) -> (&str, &str) {
///     (context.topic().name(), context.event_id().as_str())
/// }
/// ```
pub struct PublishFailureContext<T: 'static> {
    /// Shared ownership of the original event payload.
    payload: Arc<T>,
    /// Identifier of the event whose publication failed.
    event_id: EventId,
    /// Typed destination selected for the event.
    topic: Topic<T>,
    /// Portable headers carried by the event.
    headers: Headers,
    /// Optional key used to request per-key ordering.
    ordering_key: Option<Box<str>>,
    /// Event creation timestamp.
    timestamp: SystemTime,
    /// Requested delay before provider delivery.
    delay: Option<Duration>,
}

impl<T: 'static> Clone for PublishFailureContext<T> {
    /// Clones metadata and the shared payload owner without cloning `T`.
    ///
    /// # Returns
    /// A new context with cloned metadata and shared access to the same
    /// payload.
    fn clone(&self) -> Self {
        Self {
            payload: self.payload.clone(),
            event_id: self.event_id.clone(),
            topic: self.topic.clone(),
            headers: self.headers.clone(),
            ordering_key: self.ordering_key.clone(),
            timestamp: self.timestamp,
            delay: self.delay,
        }
    }
}

impl<T: 'static> PublishFailureContext<T> {
    /// Creates an observer context by moving fields out of a failed envelope.
    ///
    /// # Parameters
    /// - `envelope`: event whose publish operation reached a terminal failure.
    ///
    /// # Returns
    /// An immutable context sharing the original payload allocation.
    pub(crate) fn from_envelope(envelope: EventEnvelope<T>) -> Self {
        Self {
            payload: envelope.payload,
            event_id: envelope.id,
            topic: envelope.topic,
            headers: envelope.headers,
            ordering_key: envelope.ordering_key,
            timestamp: envelope.timestamp,
            delay: envelope.delay,
        }
    }

    /// Returns the payload by reference without requiring `T: Clone`.
    ///
    /// # Returns
    /// The original payload borrowed from its shared owner.
    #[must_use = "Use the returned payload."]
    #[inline]
    pub fn payload(&self) -> &T {
        self.payload.as_ref()
    }

    /// Returns a shared handle to the original payload.
    ///
    /// # Returns
    /// A new shared owner of the original payload allocation.
    #[must_use]
    #[inline]
    pub fn payload_arc(&self) -> Arc<T> {
        self.payload.clone()
    }

    /// Returns the event identifier.
    ///
    /// # Returns
    /// The event identifier retained in this context.
    #[must_use = "Use the returned event id."]
    #[inline]
    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    /// Returns the typed topic.
    ///
    /// # Returns
    /// The typed destination borrowed from this context.
    #[must_use = "Use the returned topic."]
    #[inline]
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }

    /// Returns all portable headers.
    ///
    /// # Returns
    /// The complete event header map.
    #[must_use]
    #[inline]
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Returns one header value, if present.
    ///
    /// # Parameters
    /// - `key`: header name to look up.
    ///
    /// # Returns
    /// The matching header value, or `None` when absent.
    #[must_use]
    #[inline]
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(String::as_str)
    }

    /// Returns the ordering key, if the event requests per-key ordering.
    ///
    /// # Returns
    /// The ordering key borrowed from the event, or `None` when absent.
    #[must_use = "Use the returned ordering key."]
    #[inline]
    pub fn ordering_key(&self) -> Option<&str> {
        self.ordering_key.as_deref()
    }

    /// Returns the event creation timestamp.
    ///
    /// # Returns
    /// The timestamp carried by the original envelope.
    #[must_use]
    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns the requested delivery delay, if any.
    ///
    /// # Returns
    /// The requested delay, or `None` when the event has no delay.
    #[must_use = "Use the returned delay."]
    #[inline]
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }
}
