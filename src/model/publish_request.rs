//! One typed publication input for the facade.

use super::EventEnvelope;
use super::PublishOptions;
use super::PublishRequestBuilder;
use super::Topic;

/// An envelope and its per-publication policy.
pub struct PublishRequest<T: 'static> {
    envelope: EventEnvelope<T>,
    options: PublishOptions<T>,
}

impl<T: Send + Sync + 'static> PublishRequest<T> {
    /// Creates a simple request with generated envelope metadata and default
    /// options.
    pub fn new(topic: Topic<T>, payload: T) -> Self {
        Self::from_envelope(EventEnvelope::new(topic, payload))
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
    pub fn topic(&self) -> &Topic<T> {
        self.envelope.topic()
    }
    /// Returns one header, or `None` when absent.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.envelope.header(key)
    }
    /// Returns the complete envelope.
    pub fn envelope(&self) -> &EventEnvelope<T> {
        &self.envelope
    }
    /// Returns per-publication options.
    pub fn options(&self) -> &PublishOptions<T> {
        &self.options
    }
    /// Consumes the request into its envelope and options.
    pub fn into_parts(self) -> (EventEnvelope<T>, PublishOptions<T>) {
        (self.envelope, self.options)
    }
}
