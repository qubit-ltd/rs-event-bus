// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Complete subscription request construction and validation.

use std::sync::Arc;

use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::AckMode;
use super::AsyncSubscriberNext;
use super::ConsumerGroup;
use super::DeadLetterPolicy;
use super::Delivery;
use super::EventEnvelope;
use super::FailureDirective;
use super::OrderingPolicy;
use super::ProviderOptions;
use super::StartPosition;
use super::SubscribeOptions;
use super::SubscribeRequest;
use super::SubscriberId;
use super::SubscriberNext;
use super::SubscriptionDurability;
use super::Topic;
use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;

/// Invalid subscription request identity or policy.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubscribeRequestBuildError {
    /// A required field was omitted.
    #[error("missing required subscribe request field: {0}")]
    MissingField(&'static str),
    /// Retry classification or cancellation requires a retry policy.
    #[error("retry rule or cancellation token requires a retry policy")]
    InvalidRetryConfiguration,
    /// Provider option keys must be namespaced and values must be printable.
    #[error("invalid provider option {0:?}")]
    InvalidProviderOption(String),
}

/// Builds one subscription request. Scalars use their last value, handlers and
/// interceptors append in call order, and `options` replaces policy at its
/// call position. Later policy calls can override or append to that value.
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::model::{AckMode, FailureDirective, SubscribeOptions, SubscribeRequest, Topic};
/// use qubit_event_bus::SubscriberId;
/// let options = SubscribeOptions::<String>::builder()
///     .ack_mode(AckMode::Manual)
///     .error_handler(|_, _| FailureDirective::Discard)
///     .interceptor(|delivery, next| next(delivery))
///     .build();
/// let request = SubscribeRequest::builder()
///     .subscriber_id(SubscriberId::new("old")?)
///     .subscriber_id(SubscriberId::new("audit")?)
///     .topic(Topic::<String>::new("orders.old")?)
///     .topic(Topic::<String>::new("orders.created")?)
///     .error_handler(|_, _| panic!("replaced by options"))
///     .interceptor(|delivery, next| next(delivery))
///     .options(options)
///     .ack_mode(AckMode::Auto)
///     .error_handler(|_, _| FailureDirective::Discard)
///     .interceptor(|delivery, next| next(delivery))
///     .build()?;
/// assert_eq!(request.subscriber_id().as_str(), "audit");
/// assert_eq!(request.topic().name(), "orders.created");
/// assert_eq!(request.options().ack_mode(), AckMode::Auto);
/// assert_eq!(request.options().error_handlers().len(), 2);
/// assert_eq!(request.options().interceptors().len(), 2);
/// # Ok(())
/// # }
/// ```
pub struct SubscribeRequestBuilder<T: 'static> {
    subscriber_id: Option<SubscriberId>,
    topic: Option<Topic<T>>,
    options: SubscribeOptions<T>,
}

impl<T: Send + Sync + 'static> SubscribeRequestBuilder<T> {
    /// Starts an empty builder requiring subscriber ID and topic.
    pub fn new() -> Self {
        Self {
            subscriber_id: None,
            topic: None,
            options: SubscribeOptions::default(),
        }
    }
    /// Replaces the logical subscriber ID.
    pub fn subscriber_id(mut self, value: SubscriberId) -> Self {
        self.subscriber_id = Some(value);
        self
    }
    /// Replaces the typed topic.
    pub fn topic(mut self, value: Topic<T>) -> Self {
        self.topic = Some(value);
        self
    }
    /// Replaces the acknowledgement mode.
    pub fn ack_mode(mut self, value: AckMode) -> Self {
        self.options.ack_mode = value;
        self
    }
    /// Replaces the subscriber filter.
    pub fn filter<F>(mut self, value: F) -> Self
    where
        F: Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static,
    {
        self.options.filter = Some(Arc::new(value));
        self
    }
    /// Replaces the retry policy directly from `qubit-retry`.
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.options.retry_policy = Some(value);
        self
    }
    /// Replaces the typed retry rule directly from `qubit-retry`.
    pub fn retry_rule<R>(mut self, value: R) -> Self
    where
        R: RetryRule<DeliveryAttemptError>,
    {
        self.options.retry_rule = Some(Arc::new(value));
        self
    }
    /// Replaces the retry cancellation token.
    pub fn retry_cancellation_token(mut self, value: RetryCancellationToken) -> Self {
        self.options.retry_cancellation_token = Some(value);
        self
    }
    /// Appends an error handler after the existing handlers.
    pub fn error_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&EventEnvelope<T>, &DeliveryError) -> FailureDirective + Send + Sync + 'static,
    {
        self.options.error_handlers.push(Arc::new(handler));
        self
    }
    /// Appends a typed subscriber interceptor in registration order.
    pub fn interceptor<F>(mut self, value: F) -> Self
    where
        F: Fn(Delivery<T>, SubscriberNext<T>) -> Result<(), DeliveryError> + Send + Sync + 'static,
    {
        self.options.interceptors.push(Arc::new(value));
        self
    }
    /// Appends runtime-neutral async middleware in registration order.
    pub fn async_interceptor<F>(mut self, value: F) -> Self
    where
        F: Fn(Delivery<T>, AsyncSubscriberNext<T>) -> crate::spi::SpiFuture<'static, Result<(), DeliveryError>>
            + Send
            + Sync
            + 'static,
    {
        self.options.async_interceptors.push(Arc::new(value));
        self
    }
    /// Replaces the dead-letter policy.
    pub fn dead_letter(mut self, value: DeadLetterPolicy) -> Self {
        self.options.dead_letter = Some(value);
        self
    }
    /// Replaces the ordering policy.
    pub fn ordering_policy(mut self, value: OrderingPolicy) -> Self {
        self.options.ordering_policy = value;
        self
    }
    /// Replaces the provider consumer group.
    pub fn consumer_group(mut self, value: ConsumerGroup) -> Self {
        self.options.consumer_group = Some(value);
        self
    }
    /// Replaces subscription durability.
    pub fn durability(mut self, value: SubscriptionDurability) -> Self {
        self.options.durability = value;
        self
    }
    /// Replaces the requested start position.
    pub fn start_position(mut self, value: StartPosition) -> Self {
        self.options.start_position = value;
        self
    }
    /// Adds or replaces one namespaced provider option.
    pub fn provider_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.provider_options.insert(key.into(), value.into());
        self
    }
    /// Merges provider options in iteration order, replacing earlier values by
    /// key.
    pub fn provider_options(mut self, values: ProviderOptions) -> Self {
        self.options.provider_options.extend(values);
        self
    }
    /// Replaces all policy state; later policy calls override or append.
    pub fn options(mut self, value: SubscribeOptions<T>) -> Self {
        self.options = value;
        self
    }

    /// Consumes the builder after validating identity, topic and portable
    /// options. A later `options` call replaces earlier policy callbacks.
    /// Backend capability checks occur when the facade opens the subscription.
    ///
    /// # Errors
    /// Returns `MissingField` without a subscriber ID or topic,
    /// `InvalidRetryConfiguration` when a rule or cancellation token has no
    /// retry policy, and `InvalidProviderOption` for a non-namespaced option
    /// key or a control character in its key or value.
    pub fn build(self) -> Result<SubscribeRequest<T>, SubscribeRequestBuildError> {
        let subscriber_id = self
            .subscriber_id
            .ok_or(SubscribeRequestBuildError::MissingField("subscriber_id"))?;
        let topic = self.topic.ok_or(SubscribeRequestBuildError::MissingField("topic"))?;
        if self.options.retry_policy.is_none()
            && (self.options.retry_rule.is_some() || self.options.retry_cancellation_token.is_some())
        {
            return Err(SubscribeRequestBuildError::InvalidRetryConfiguration);
        }
        for (key, value) in &self.options.provider_options {
            if !key.contains('.')
                || key.starts_with('.')
                || key.ends_with('.')
                || key.chars().any(char::is_control)
                || value.chars().any(char::is_control)
            {
                return Err(SubscribeRequestBuildError::InvalidProviderOption(key.clone()));
            }
        }
        Ok(SubscribeRequest::new(subscriber_id, topic).with_options(self.options))
    }
}

impl<T: Send + Sync + 'static> Default for SubscribeRequestBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}
