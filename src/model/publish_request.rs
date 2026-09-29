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
/// # Type Parameters
/// - `T`: payload type carried by the event.
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
#[must_use]
pub struct PublishRequest<T: 'static> {
    /// Validated topic, payload, and event metadata.
    envelope: EventEnvelope<T>,
    /// Retry policy and callback configuration for this publication.
    options: PublishOptions<T>,
}

impl<T: Send + Sync + 'static> PublishRequest<T> {
    /// Creates a simple request with generated UUID v4 metadata and default
    /// options.
    ///
    /// # Parameters
    /// - `topic`: typed destination for the publication.
    /// - `payload`: event payload to publish.
    ///
    /// # Returns
    /// A request with a generated event identifier and default options.
    ///
    /// # Errors
    /// Returns [`EventIdGenerationError`] if the operating-system random source
    /// cannot provide bytes for the generated identifier.
    pub fn new(topic: Topic<T>, payload: T) -> Result<Self, EventIdGenerationError> {
        Ok(Self::from_envelope(EventEnvelope::new(topic, payload)?))
    }
    /// Creates a request from an existing envelope for replay or forwarding.
    ///
    /// # Parameters
    /// - `envelope`: validated event to publish.
    ///
    /// # Returns
    /// A request with default per-publication options.
    pub fn from_envelope(envelope: EventEnvelope<T>) -> Self {
        Self {
            envelope,
            options: PublishOptions::default(),
        }
    }
    /// Starts a builder for metadata and policy overrides.
    ///
    /// # Returns
    /// An empty request builder requiring a topic and payload.
    #[inline]
    pub fn builder() -> PublishRequestBuilder<T> {
        PublishRequestBuilder::new()
    }
    /// Returns the typed topic.
    ///
    /// # Returns
    /// The topic carried by the envelope.
    #[must_use = "Use the returned topic."]
    #[inline]
    pub fn topic(&self) -> &Topic<T> {
        self.envelope.topic()
    }
    /// Returns one header, or `None` when absent.
    ///
    /// # Parameters
    /// - `key`: header name to look up.
    ///
    /// # Returns
    /// The matching header value, or `None` when absent.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.envelope.header(key)
    }
    /// Returns the complete envelope.
    ///
    /// # Returns
    /// The validated event envelope borrowed from this request.
    #[must_use = "Use the returned envelope."]
    #[inline]
    pub fn envelope(&self) -> &EventEnvelope<T> {
        &self.envelope
    }
    /// Returns per-publication options.
    ///
    /// # Returns
    /// The retry and callback policy associated with the request.
    #[must_use]
    #[inline]
    pub fn options(&self) -> &PublishOptions<T> {
        &self.options
    }
    /// Replaces all per-publication policy with reusable options.
    ///
    /// # Parameters
    /// - `options`: policy applied to this publication.
    ///
    /// # Returns
    /// The request with its policy replaced.
    pub fn with_options(mut self, options: PublishOptions<T>) -> Self {
        self.options = options;
        self
    }
    /// Consumes the request into its envelope and options.
    ///
    /// # Returns
    /// The envelope and policy as separate owned values.
    pub fn into_parts(self) -> (EventEnvelope<T>, PublishOptions<T>) {
        (self.envelope, self.options)
    }
}
