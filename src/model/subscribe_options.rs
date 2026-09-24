// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Per-subscription processing, retry and provider options.

use std::collections::BTreeMap;
use std::sync::Arc;

use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::Delivery;
use super::EventEnvelope;
use crate::error::ConfigurationError;
use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;
use crate::spi::SpiFuture;

/// The next action requested by a subscriber delivery error handler.
///
/// This directive applies only to subscriber delivery lifecycle decisions;
/// publish error handlers are terminal observers and return `()`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FailureDirective {
    /// Let the configured delivery retry policy make another attempt.
    Retry,
    /// Ask a capable provider to deliver the message again.
    Requeue,
    /// Send the failed event through the dead-letter policy.
    DeadLetter,
    /// Stop processing this delivery failure.
    Discard,
}

/// How handler completion is acknowledged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AckMode {
    /// Handler success automatically accepts the delivery.
    Auto,
    /// Handler must explicitly ACK and return success.
    Manual,
}

/// Ordering requested by a subscriber.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum OrderingPolicy {
    /// No ordering guarantee is required.
    #[default]
    Unordered,
    /// Preserve order for events with the same ordering key.
    PerKey,
}

/// Whether a subscription survives an application disconnect.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum SubscriptionDurability {
    /// The provider may remove state when the subscriber leaves.
    #[default]
    Ephemeral,
    /// The provider retains subscription state across disconnects.
    Durable,
}

/// Position from which a capable provider starts a subscription.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum StartPosition {
    /// Begin with events published after subscription creation.
    #[default]
    New,
    /// Begin with the earliest retained event.
    Earliest,
    /// Resume at a provider-specific position.
    At(Box<str>),
}

/// A validated provider consumer group name.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConsumerGroup(Box<str>);
impl ConsumerGroup {
    /// Creates a nonblank group name without surrounding whitespace.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
            return Err(ConfigurationError::InvalidField {
                field: "consumer_group",
                message: "must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self(value.into()))
    }
    /// Returns the group name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Namespaced, non-sensitive provider configuration values.
pub type ProviderOptions = BTreeMap<String, String>;

/// Where a terminal delivery failure should be published.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeadLetterPolicy {
    /// Publish a standard dead-letter record to this topic name.
    Topic(Box<str>),
}
impl DeadLetterPolicy {
    /// Validates a dead-letter topic name.
    pub fn topic(name: &str) -> Result<Self, ConfigurationError> {
        if name.is_empty() || name.trim() != name || name.chars().any(char::is_control) {
            return Err(ConfigurationError::InvalidField {
                field: "dead_letter",
                message: "topic must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self::Topic(name.into()))
    }
}

/// A subscriber filter evaluated by the facade before handler invocation.
pub type EventFilter<T> = dyn Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static;
/// A subscriber failure callback evaluated in registration order.
pub type SubscribeErrorHandler<T> =
    dyn Fn(&EventEnvelope<T>, &DeliveryError) -> FailureDirective + Send + Sync + 'static;
/// Single-use synchronous middleware continuation for the next subscriber
/// stage. Dropping it short-circuits the inner middleware and handler;
/// consuming it more than once is prevented by its `FnOnce` type.
pub type SubscriberNext<T> = Box<dyn FnOnce(Delivery<T>) -> Result<(), DeliveryError> + Send + 'static>;
/// Synchronous typed subscriber middleware; invoke `next` to continue.
pub type SubscriberInterceptor<T> =
    dyn Fn(Delivery<T>, SubscriberNext<T>) -> Result<(), DeliveryError> + Send + Sync + 'static;
/// Single-use runtime-neutral async continuation. The returned future owns its
/// stage inputs, and dropping the continuation short-circuits inner work.
pub type AsyncSubscriberNext<T> =
    Box<dyn FnOnce(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + 'static>;
/// Runtime-neutral async typed subscriber middleware; invoke `next` to
/// continue.
pub type AsyncSubscriberInterceptor<T> = dyn Fn(Delivery<T>, AsyncSubscriberNext<T>) -> SpiFuture<'static, Result<(), DeliveryError>>
    + Send
    + Sync
    + 'static;

/// Immutable options applied to one subscription request.
pub struct SubscribeOptions<T: 'static> {
    pub(crate) ack_mode: AckMode,
    pub(crate) filter: Option<Arc<EventFilter<T>>>,
    pub(crate) retry_policy: Option<RetryPolicy>,
    pub(crate) retry_rule: Option<Arc<dyn RetryRule<DeliveryAttemptError>>>,
    pub(crate) retry_cancellation_token: Option<RetryCancellationToken>,
    pub(crate) error_handlers: Vec<Arc<SubscribeErrorHandler<T>>>,
    pub(crate) interceptors: Vec<Arc<SubscriberInterceptor<T>>>,
    pub(crate) async_interceptors: Vec<Arc<AsyncSubscriberInterceptor<T>>>,
    pub(crate) dead_letter: Option<DeadLetterPolicy>,
    pub(crate) priority: i32,
    pub(crate) ordering_policy: OrderingPolicy,
    pub(crate) consumer_group: Option<ConsumerGroup>,
    pub(crate) durability: SubscriptionDurability,
    pub(crate) start_position: StartPosition,
    pub(crate) provider_options: ProviderOptions,
}

impl<T: 'static> Default for SubscribeOptions<T> {
    fn default() -> Self {
        Self {
            ack_mode: AckMode::Auto,
            filter: None,
            retry_policy: None,
            retry_rule: None,
            retry_cancellation_token: None,
            error_handlers: Vec::new(),
            interceptors: Vec::new(),
            async_interceptors: Vec::new(),
            dead_letter: None,
            priority: 0,
            ordering_policy: OrderingPolicy::Unordered,
            consumer_group: None,
            durability: SubscriptionDurability::Ephemeral,
            start_position: StartPosition::New,
            provider_options: ProviderOptions::new(),
        }
    }
}

impl<T: 'static> Clone for SubscribeOptions<T> {
    fn clone(&self) -> Self {
        Self {
            ack_mode: self.ack_mode,
            filter: self.filter.clone(),
            retry_policy: self.retry_policy.clone(),
            retry_rule: self.retry_rule.clone(),
            retry_cancellation_token: self.retry_cancellation_token.clone(),
            error_handlers: self.error_handlers.clone(),
            interceptors: self.interceptors.clone(),
            async_interceptors: self.async_interceptors.clone(),
            dead_letter: self.dead_letter.clone(),
            priority: self.priority,
            ordering_policy: self.ordering_policy,
            consumer_group: self.consumer_group.clone(),
            durability: self.durability,
            start_position: self.start_position.clone(),
            provider_options: self.provider_options.clone(),
        }
    }
}

impl<T: 'static> SubscribeOptions<T> {
    /// Returns default options: automatic ACK, ephemeral, and new messages.
    pub fn new() -> Self {
        Self::default()
    }
    /// Starts a builder for reusable subscription policy.
    pub fn builder() -> SubscribeOptionsBuilder<T> {
        SubscribeOptionsBuilder::new()
    }
    /// Returns the acknowledgement mode.
    pub fn ack_mode(&self) -> AckMode {
        self.ack_mode
    }
    /// Returns the filter, or `None` when all events pass.
    pub fn filter(&self) -> Option<&Arc<EventFilter<T>>> {
        self.filter.as_ref()
    }
    /// Returns retry policy, or `None` when retry is disabled.
    pub fn retry_policy(&self) -> Option<&RetryPolicy> {
        self.retry_policy.as_ref()
    }
    /// Returns custom retry classification, or `None` for the default rule.
    pub fn retry_rule(&self) -> Option<&Arc<dyn RetryRule<DeliveryAttemptError>>> {
        self.retry_rule.as_ref()
    }
    /// Returns cancellation token, or `None` when absent.
    pub fn retry_cancellation_token(&self) -> Option<&RetryCancellationToken> {
        self.retry_cancellation_token.as_ref()
    }
    /// Returns subscriber error handlers in registration order.
    pub fn error_handlers(&self) -> &[Arc<SubscribeErrorHandler<T>>] {
        &self.error_handlers
    }
    /// Returns typed subscriber interceptors in registration order.
    pub fn interceptors(&self) -> &[Arc<SubscriberInterceptor<T>>] {
        &self.interceptors
    }
    /// Returns async typed subscriber middleware in registration order.
    pub fn async_interceptors(&self) -> &[Arc<AsyncSubscriberInterceptor<T>>] {
        &self.async_interceptors
    }
    /// Returns dead-letter policy, or `None` when disabled.
    pub fn dead_letter(&self) -> Option<&DeadLetterPolicy> {
        self.dead_letter.as_ref()
    }
    /// Returns handler scheduling priority.
    pub fn priority(&self) -> i32 {
        self.priority
    }
    /// Returns requested ordering behavior.
    pub fn ordering_policy(&self) -> OrderingPolicy {
        self.ordering_policy
    }
    /// Returns consumer group, or `None` for a standalone subscriber.
    pub fn consumer_group(&self) -> Option<&ConsumerGroup> {
        self.consumer_group.as_ref()
    }
    /// Returns the durability request.
    pub fn durability(&self) -> SubscriptionDurability {
        self.durability
    }
    /// Returns the requested start position.
    pub fn start_position(&self) -> &StartPosition {
        &self.start_position
    }
    /// Returns namespaced provider options.
    pub fn provider_options(&self) -> &ProviderOptions {
        &self.provider_options
    }
}

/// Builds reusable subscription policy independently of an identity and topic.
pub struct SubscribeOptionsBuilder<T: 'static> {
    options: SubscribeOptions<T>,
}

impl<T: 'static> SubscribeOptionsBuilder<T> {
    /// Starts with automatic ACK, ephemeral durability, and new messages.
    pub fn new() -> Self {
        Self {
            options: SubscribeOptions::default(),
        }
    }
    /// Replaces acknowledgement mode.
    pub fn ack_mode(mut self, value: AckMode) -> Self {
        self.options.ack_mode = value;
        self
    }
    /// Replaces the filter.
    pub fn filter<F: Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static>(mut self, value: F) -> Self {
        self.options.filter = Some(Arc::new(value));
        self
    }
    /// Replaces the retry policy from `qubit-retry`.
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.options.retry_policy = Some(value);
        self
    }
    /// Replaces the typed retry rule from `qubit-retry`.
    pub fn retry_rule<R: RetryRule<DeliveryAttemptError>>(mut self, value: R) -> Self {
        self.options.retry_rule = Some(Arc::new(value));
        self
    }
    /// Replaces the retry cancellation token.
    pub fn retry_cancellation_token(mut self, value: RetryCancellationToken) -> Self {
        self.options.retry_cancellation_token = Some(value);
        self
    }
    /// Appends an error handler in registration order.
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
    /// Appends runtime-neutral async typed middleware in registration order.
    pub fn async_interceptor<F>(mut self, value: F) -> Self
    where
        F: Fn(Delivery<T>, AsyncSubscriberNext<T>) -> SpiFuture<'static, Result<(), DeliveryError>>
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
    /// Replaces handler priority.
    pub fn priority(mut self, value: i32) -> Self {
        self.options.priority = value;
        self
    }
    /// Replaces the ordering policy.
    pub fn ordering_policy(mut self, value: OrderingPolicy) -> Self {
        self.options.ordering_policy = value;
        self
    }
    /// Replaces consumer group.
    pub fn consumer_group(mut self, value: ConsumerGroup) -> Self {
        self.options.consumer_group = Some(value);
        self
    }
    /// Replaces durability.
    pub fn durability(mut self, value: SubscriptionDurability) -> Self {
        self.options.durability = value;
        self
    }
    /// Replaces start position.
    pub fn start_position(mut self, value: StartPosition) -> Self {
        self.options.start_position = value;
        self
    }
    /// Adds or replaces a namespaced provider option.
    pub fn provider_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.provider_options.insert(key.into(), value.into());
        self
    }
    /// Finishes construction; request builders validate policy combinations.
    pub fn build(self) -> SubscribeOptions<T> {
        self.options
    }
}

impl<T: 'static> Default for SubscribeOptionsBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}
