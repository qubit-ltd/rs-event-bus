/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Builder for event envelopes.

use std::collections::HashMap;
use std::time::{
    Duration,
    SystemTime,
};

use crate::event_envelope::generate_event_id;
use crate::{
    Acknowledgement,
    EventBusError,
    EventBusResult,
    EventEnvelope,
    Topic,
};

/// Builder used to create [`EventEnvelope`] values with optional metadata.
#[derive(Debug)]
pub struct EventEnvelopeBuilder<T: 'static> {
    pub(crate) id: String,
    pub(crate) topic: Option<Topic<T>>,
    pub(crate) payload: Option<T>,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) ordering_key: Option<String>,
    pub(crate) timestamp: SystemTime,
    pub(crate) delay: Option<Duration>,
    pub(crate) acknowledgement: Option<Acknowledgement>,
    pub(crate) dead_letter: bool,
}

impl<T: 'static> EventEnvelopeBuilder<T> {
    /// Creates a builder with generated ID and current timestamp defaults.
    ///
    /// # Returns
    /// A builder requiring topic and payload before build.
    pub(crate) fn new() -> Self {
        Self {
            id: generate_event_id(),
            topic: None,
            payload: None,
            headers: HashMap::new(),
            ordering_key: None,
            timestamp: SystemTime::now(),
            delay: None,
            acknowledgement: None,
            dead_letter: false,
        }
    }

    /// Sets the event ID.
    ///
    /// # Parameters
    /// - `id`: Non-blank event ID.
    ///
    /// # Returns
    /// Updated builder.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Sets the event topic.
    ///
    /// # Parameters
    /// - `topic`: Event topic.
    ///
    /// # Returns
    /// Updated builder.
    pub fn topic(mut self, topic: Topic<T>) -> Self {
        self.topic = Some(topic);
        self
    }

    /// Sets the event payload.
    ///
    /// # Parameters
    /// - `payload`: Event payload.
    ///
    /// # Returns
    /// Updated builder.
    pub fn payload(mut self, payload: T) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Adds or replaces one header.
    ///
    /// # Parameters
    /// - `key`: Header key.
    /// - `value`: Header value converted to string.
    ///
    /// # Returns
    /// Updated builder.
    pub fn header(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.headers.insert(key.into(), value.to_string());
        self
    }

    /// Adds multiple headers.
    ///
    /// # Parameters
    /// - `headers`: Header map to merge.
    ///
    /// # Returns
    /// Updated builder.
    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers.extend(headers);
        self
    }

    /// Sets the ordering key.
    ///
    /// # Parameters
    /// - `ordering_key`: Ordering key.
    ///
    /// # Returns
    /// Updated builder.
    pub fn ordering_key(mut self, ordering_key: impl Into<String>) -> Self {
        self.ordering_key = Some(ordering_key.into());
        self
    }

    /// Sets the timestamp.
    ///
    /// # Parameters
    /// - `timestamp`: Event timestamp.
    ///
    /// # Returns
    /// Updated builder.
    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Sets delivery delay metadata.
    ///
    /// # Parameters
    /// - `delay`: Requested delivery delay.
    ///
    /// # Returns
    /// Updated builder.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Sets acknowledgement metadata.
    ///
    /// # Parameters
    /// - `acknowledgement`: Acknowledgement handle.
    ///
    /// # Returns
    /// Updated builder.
    pub fn acknowledgement(mut self, acknowledgement: Acknowledgement) -> Self {
        self.acknowledgement = Some(acknowledgement);
        self
    }

    /// Sets the dead-letter marker.
    ///
    /// # Parameters
    /// - `dead_letter`: Whether the envelope is a dead letter.
    ///
    /// # Returns
    /// Updated builder.
    pub fn dead_letter(mut self, dead_letter: bool) -> Self {
        self.dead_letter = dead_letter;
        self
    }

    /// Builds the envelope.
    ///
    /// # Returns
    /// A complete event envelope.
    ///
    /// # Errors
    /// Returns an error when ID, topic, or payload is missing or invalid.
    pub fn build(self) -> EventBusResult<EventEnvelope<T>> {
        if self.id.trim().is_empty() {
            return Err(EventBusError::invalid_argument(
                "id",
                "event id must not be blank",
            ));
        }
        if self.topic.is_none() {
            return Err(EventBusError::missing_field("topic"));
        }
        if self.payload.is_none() {
            return Err(EventBusError::missing_field("payload"));
        }
        Ok(EventEnvelope::from_builder(self))
    }
}
