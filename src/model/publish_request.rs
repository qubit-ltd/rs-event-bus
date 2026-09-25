// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One typed publication input for the facade.

use super::EventEnvelope;
use super::PublishOptions;
use super::PublishRequestBuilder;
use super::Topic;
use crate::error::EventIdGenerationError;

/// An envelope and its per-publication policy.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::PublishRequest;
/// use qubit_event_bus::model::Topic;
///
/// let topic = Topic::<String>::new("orders.created").unwrap();
/// let request = PublishRequest::new(topic, "order-42".to_owned()).unwrap();
/// assert_eq!(request.envelope().payload(), "order-42");
/// ```
pub struct PublishRequest<T: 'static> {
    envelope: EventEnvelope<T>,
    options: PublishOptions<T>,
}

impl<T: Send + Sync + 'static> PublishRequest<T> {
    /// Creates a simple request with generated UUID v4 metadata and default
    /// options.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] if the operating-system random source
    /// cannot provide bytes for the generated identifier.
    pub fn new(topic: Topic<T>, payload: T) -> Result<Self, EventIdGenerationError> {
        Ok(Self::from_envelope(EventEnvelope::new(topic, payload)?))
    }
    /// Creates a request from an existing envelope for replay or forwarding.
    pub fn from_envelope(envelope: EventEnvelope<T>) -> Self {
        Self {
            envelope,
            options: PublishOptions::default(),
        }
    }
    /// Starts a builder for metadata and policy overrides.
    pub fn builder() -> PublishRequestBuilder<T> {
        PublishRequestBuilder::new()
    }
    /// Replaces all per-publication policy with reusable options.
    pub fn with_options(mut self, options: PublishOptions<T>) -> Self {
        self.options = options;
        self
    }
    /// Returns the typed topic.
    #[must_use]
    #[inline]
    pub fn topic(&self) -> &Topic<T> {
        self.envelope.topic()
    }
    /// Returns one header, or `None` when absent.
    #[must_use]
    pub fn header(&self, key: &str) -> Option<&str> {
        self.envelope.header(key)
    }
    /// Returns the complete envelope.
    #[must_use]
    #[inline]
    pub fn envelope(&self) -> &EventEnvelope<T> {
        &self.envelope
    }
    /// Returns per-publication options.
    #[must_use]
    #[inline]
    pub fn options(&self) -> &PublishOptions<T> {
        &self.options
    }
    /// Consumes the request into its envelope and options.
    pub fn into_parts(self) -> (EventEnvelope<T>, PublishOptions<T>) {
        (self.envelope, self.options)
    }
}
