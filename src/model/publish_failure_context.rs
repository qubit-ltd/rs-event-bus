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
pub struct PublishFailureContext<T: 'static> {
    payload: Arc<T>,
    event_id: EventId,
    topic: Topic<T>,
    headers: Headers,
    ordering_key: Option<Box<str>>,
    timestamp: SystemTime,
    delay: Option<Duration>,
}

impl<T: 'static> Clone for PublishFailureContext<T> {
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
    pub fn payload(&self) -> &T {
        self.payload.as_ref()
    }

    /// Returns a shared handle to the original payload.
    pub fn payload_arc(&self) -> Arc<T> {
        self.payload.clone()
    }

    /// Returns the event identifier.
    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    /// Returns the typed topic.
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }

    /// Returns all portable headers.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Returns one header value, if present.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(String::as_str)
    }

    /// Returns the ordering key, if the event requests per-key ordering.
    pub fn ordering_key(&self) -> Option<&str> {
        self.ordering_key.as_deref()
    }

    /// Returns the event creation timestamp.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns the requested delivery delay, if any.
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }
}
