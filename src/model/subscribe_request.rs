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

/// A logical subscriber identity, typed topic, and processing policy.
pub struct SubscribeRequest<T: 'static> {
    subscriber_id: SubscriberId,
    topic: Topic<T>,
    options: SubscribeOptions<T>,
}

impl<T: Send + Sync + 'static> SubscribeRequest<T> {
    /// Creates a subscription with automatic ACK and other default options.
    pub fn new(subscriber_id: SubscriberId, topic: Topic<T>) -> Self {
        Self {
            subscriber_id,
            topic,
            options: SubscribeOptions::default(),
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
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }
    /// Returns the typed topic.
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }
    /// Returns subscription options.
    pub fn options(&self) -> &SubscribeOptions<T> {
        &self.options
    }
    /// Consumes the request into its identity, topic, and options.
    pub fn into_parts(self) -> (SubscriberId, Topic<T>, SubscribeOptions<T>) {
        (self.subscriber_id, self.topic, self.options)
    }
}
