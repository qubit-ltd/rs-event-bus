// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One typed subscriber registration input for the facade.

use super::SubscribeOptions;
use super::SubscribeRequestBuilder;
use super::SubscriberId;
use super::Topic;
use crate::error::ConfigurationError;

/// A logical subscriber identity, typed topic, and processing policy.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::SubscribeRequest;
/// use qubit_event_bus::model::Topic;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let topic = Topic::<String>::new("orders.created")?;
/// let request = SubscribeRequest::new("audit", topic)?;
/// assert_eq!(request.subscriber_id().as_str(), "audit");
/// # Ok(())
/// # }
/// ```
pub struct SubscribeRequest<T: 'static> {
    subscriber_id: SubscriberId,
    topic: Topic<T>,
    options: SubscribeOptions<T>,
}

impl<T: Send + Sync + 'static> SubscribeRequest<T> {
    /// Creates a subscription from a subscriber ID string and a typed topic.
    ///
    /// The request takes ownership of `topic`; clone it at the call site if it
    /// must be reused. The subscriber ID is validated with
    /// [`SubscriberId::new`].
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidSubscriberId`] when `subscriber_id`
    /// does not follow the portable subscriber-name syntax.
    pub fn new(subscriber_id: &str, topic: Topic<T>) -> Result<Self, ConfigurationError> {
        let subscriber_id = SubscriberId::new(subscriber_id)?;
        Ok(Self::from_validated_parts(
            subscriber_id,
            topic,
            SubscribeOptions::default(),
        ))
    }

    /// Creates a request from an already validated identity, topic, and
    /// options.
    pub(super) fn from_validated_parts(
        subscriber_id: SubscriberId,
        topic: Topic<T>,
        options: SubscribeOptions<T>,
    ) -> Self {
        Self {
            subscriber_id,
            topic,
            options,
        }
    }
    /// Starts a builder for complete subscriber configuration.
    pub fn builder() -> SubscribeRequestBuilder<T> {
        SubscribeRequestBuilder::new()
    }
    /// Replaces all subscription options with reusable options.
    pub fn with_options(mut self, options: SubscribeOptions<T>) -> Self {
        self.options = options;
        self
    }
    /// Returns the logical subscriber ID.
    #[must_use = "the subscriber ID identifies the logical consumer"]
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }
    /// Returns the typed topic.
    #[must_use]
    #[inline]
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }
    /// Returns subscription options.
    #[must_use]
    #[inline]
    pub fn options(&self) -> &SubscribeOptions<T> {
        &self.options
    }
    /// Consumes the request into its identity, topic, and options.
    pub fn into_parts(self) -> (SubscriberId, Topic<T>, SubscribeOptions<T>) {
        (self.subscriber_id, self.topic, self.options)
    }
}
