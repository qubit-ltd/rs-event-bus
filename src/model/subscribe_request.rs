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
/// # Type Parameters
/// - `T`: payload type received from the selected topic.
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
#[must_use]
pub struct SubscribeRequest<T: 'static> {
    /// Validated identity of the logical subscriber.
    subscriber_id: SubscriberId,
    /// Typed topic from which this request receives events.
    topic: Topic<T>,
    /// Handler and provider policy applied to the registration.
    options: SubscribeOptions<T>,
}

impl<T: Send + Sync + 'static> SubscribeRequest<T> {
    /// Creates a subscription from a subscriber ID string and a typed topic.
    ///
    /// The request takes ownership of `topic`; clone it at the call site if it
    /// must be reused. The subscriber ID is validated with
    /// [`SubscriberId::new`].
    ///
    /// # Parameters
    /// - `subscriber_id`: logical subscriber name to validate.
    /// - `topic`: typed source from which to receive events.
    ///
    /// # Returns
    /// A request with default options and a validated subscriber identity.
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

    /// Starts a builder for complete subscriber configuration.
    ///
    /// # Returns
    /// An empty request builder requiring an identity and topic.
    #[inline]
    pub fn builder() -> SubscribeRequestBuilder<T> {
        SubscribeRequestBuilder::new()
    }
    /// Returns the logical subscriber ID.
    ///
    /// # Returns
    /// The validated logical identity for this subscription.
    #[must_use = "the subscriber ID identifies the logical consumer"]
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }
    /// Returns the typed topic.
    ///
    /// # Returns
    /// The source topic borrowed from this request.
    #[must_use = "Use the returned topic."]
    #[inline]
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }
    /// Returns subscription options.
    ///
    /// # Returns
    /// The handler, retry, and provider policy for the registration.
    #[inline]
    pub fn options(&self) -> &SubscribeOptions<T> {
        &self.options
    }
    /// Replaces all subscription options with reusable options.
    ///
    /// # Parameters
    /// - `options`: complete policy to apply to this request.
    ///
    /// # Returns
    /// The request with the supplied options.
    #[inline]
    pub fn with_options(mut self, options: SubscribeOptions<T>) -> Self {
        self.options = options;
        self
    }
    /// Consumes the request into its identity, topic, and options.
    ///
    /// # Returns
    /// The validated identity, typed topic, and options as owned values.
    #[inline]
    pub fn into_parts(self) -> (SubscriberId, Topic<T>, SubscribeOptions<T>) {
        (self.subscriber_id, self.topic, self.options)
    }

    /// Creates a request from an already validated identity, topic, and
    /// options.
    ///
    /// # Parameters
    /// - `subscriber_id`: previously validated logical identity.
    /// - `topic`: typed source topic.
    /// - `options`: validated handler and provider policies.
    ///
    /// # Returns
    /// A request retaining the supplied identity, topic, and options.
    #[inline]
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
}
