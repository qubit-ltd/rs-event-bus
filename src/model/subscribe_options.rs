// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-subscription processing, retry, and provider options.

use std::collections::BTreeMap;
use std::sync::Arc;

use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::AckMode;
use super::ConsumerGroup;
use super::DeadLetterPolicy;
use super::Delivery;
use super::EventEnvelope;
use super::FailureDirective;
use super::GapPolicy;
use super::OrderingPolicy;
use super::StartPosition;
use super::SubscriptionDurability;
use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;
use crate::spi::SpiFuture;

/// Namespaced, non-sensitive provider configuration values.
///
/// Each key must include a namespace, and values must be printable text.
pub type ProviderOptions = BTreeMap<String, String>;

/// A subscriber filter evaluated by the facade before handler invocation.
/// The callback receives the immutable event and returns whether the event is
/// eligible for this subscriber.
///
/// # Type Parameters
/// - `T`: event payload type examined by the predicate.
pub type EventFilter<T> = dyn Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static;

/// A subscriber failure callback evaluated in registration order.
/// The returned directive controls retry, requeue, dead-letter, or discard.
///
/// # Type Parameters
/// - `T`: event payload type associated with the failed delivery.
pub type SubscribeErrorHandler<T> =
    dyn Fn(&EventEnvelope<T>, &DeliveryError) -> FailureDirective + Send + Sync + 'static;

/// Single-use synchronous middleware continuation for the next subscriber
/// stage. Dropping it short-circuits the inner middleware and handler;
/// consuming it more than once is prevented by its `FnOnce` type.
///
/// # Type Parameters
/// - `T`: event payload type passed to the next middleware stage.
pub type SubscriberNext<T> =
    Box<dyn FnOnce(Delivery<T>) -> Result<(), DeliveryError> + Send + 'static>;

/// Synchronous typed subscriber middleware; invoke `next` to continue.
///
/// # Type Parameters
/// - `T`: payload type processed by the middleware.
pub type SubscriberInterceptor<T> =
    dyn Fn(Delivery<T>, SubscriberNext<T>) -> Result<(), DeliveryError> + Send + Sync + 'static;

/// Single-use runtime-neutral async continuation. The returned future owns its
/// stage inputs, and dropping the continuation short-circuits inner work.
///
/// # Type Parameters
/// - `T`: payload type passed to the next asynchronous stage.
pub type AsyncSubscriberNext<T> =
    Box<dyn FnOnce(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + 'static>;

/// Runtime-neutral async typed subscriber middleware; invoke `next` to
/// continue.
///
/// # Type Parameters
/// - `T`: payload type processed by the middleware.
pub type AsyncSubscriberInterceptor<T> = dyn Fn(Delivery<T>, AsyncSubscriberNext<T>) -> SpiFuture<'static, Result<(), DeliveryError>>
    + Send
    + Sync
    + 'static;

/// Immutable options applied to one subscription request.
///
/// # Type Parameters
/// - `T`: payload type received from the subscribed topic.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AckMode;
/// use qubit_event_bus::model::SubscribeOptions;
///
/// let options = SubscribeOptions::<String>::builder()
///     .ack_mode(AckMode::Manual)
///     .build();
/// assert_eq!(options.ack_mode(), AckMode::Manual);
/// ```
#[must_use = "subscription options must be applied to a subscription request"]
pub struct SubscribeOptions<T: 'static> {
    /// Behavior when the provider reports that messages were missed.
    pub(crate) gap_policy: GapPolicy,
    /// Handler acknowledgement behavior.
    pub(crate) ack_mode: AckMode,
    /// Optional predicate applied before invoking handler middleware.
    pub(crate) filter: Option<Arc<EventFilter<T>>>,
    /// Provider and facade retry schedule, when retries are enabled.
    pub(crate) retry_policy: Option<RetryPolicy>,
    /// Optional rule overriding default retry classification.
    pub(crate) retry_rule: Option<Arc<dyn RetryRule<DeliveryAttemptError>>>,
    /// Shared cancellation signal for retry waits.
    pub(crate) retry_cancellation_token: Option<RetryCancellationToken>,
    /// Terminal error callbacks in registration order.
    pub(crate) error_handlers: Vec<Arc<SubscribeErrorHandler<T>>>,
    /// Synchronous middleware in registration order.
    pub(crate) interceptors: Vec<Arc<SubscriberInterceptor<T>>>,
    /// Runtime-neutral asynchronous middleware in registration order.
    pub(crate) async_interceptors: Vec<Arc<AsyncSubscriberInterceptor<T>>>,
    /// Optional terminal dead-letter behavior.
    pub(crate) dead_letter: Option<DeadLetterPolicy>,
    /// Requested provider ordering behavior.
    pub(crate) ordering_policy: OrderingPolicy,
    /// Optional shared provider consumer group.
    pub(crate) consumer_group: Option<ConsumerGroup>,
    /// Requested persistence guarantee.
    pub(crate) durability: SubscriptionDurability,
    /// Provider offset from which this subscription should begin.
    pub(crate) start_position: StartPosition,
    /// Namespaced options passed to the selected provider.
    pub(crate) provider_options: ProviderOptions,
}

impl<T: 'static> Default for SubscribeOptions<T> {
    /// Creates automatic-ACK ephemeral options that start with new events.
    fn default() -> Self {
        Self {
            gap_policy: GapPolicy::Stop,
            ack_mode: AckMode::Auto,
            filter: None,
            retry_policy: None,
            retry_rule: None,
            retry_cancellation_token: None,
            error_handlers: Vec::new(),
            interceptors: Vec::new(),
            async_interceptors: Vec::new(),
            dead_letter: None,
            ordering_policy: OrderingPolicy::Unordered,
            consumer_group: None,
            durability: SubscriptionDurability::Ephemeral,
            start_position: StartPosition::New,
            provider_options: ProviderOptions::new(),
        }
    }
}

impl<T: 'static> Clone for SubscribeOptions<T> {
    /// Clones policy values and shares callback and middleware allocations.
    fn clone(&self) -> Self {
        Self {
            gap_policy: self.gap_policy,
            ack_mode: self.ack_mode,
            filter: self.filter.clone(),
            retry_policy: self.retry_policy.clone(),
            retry_rule: self.retry_rule.clone(),
            retry_cancellation_token: self.retry_cancellation_token.clone(),
            error_handlers: self.error_handlers.clone(),
            interceptors: self.interceptors.clone(),
            async_interceptors: self.async_interceptors.clone(),
            dead_letter: self.dead_letter.clone(),
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
    ///
    /// # Returns
    /// A new options value with the same defaults as [`Self::default`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts a builder for reusable subscription policy.
    ///
    /// # Returns
    /// A builder initialized with default subscription options.
    #[inline]
    pub fn builder() -> super::SubscribeOptionsBuilder<T> {
        super::SubscribeOptionsBuilder::new()
    }

    /// Returns the behavior used after a delivery gap.
    #[must_use = "Use the returned gap policy."]
    #[inline]
    pub fn gap_policy(&self) -> GapPolicy {
        self.gap_policy
    }

    /// Returns the acknowledgement mode.
    ///
    /// # Returns
    /// The configured handler acknowledgement behavior.
    #[must_use = "Use the returned ack mode."]
    #[inline]
    pub fn ack_mode(&self) -> AckMode {
        self.ack_mode
    }

    /// Returns the filter, or `None` when all events pass.
    ///
    /// # Returns
    /// The filter callback when one is configured, otherwise `None`.
    #[inline]
    #[must_use = "Use the returned filter."]
    pub fn filter(&self) -> Option<&Arc<EventFilter<T>>> {
        self.filter.as_ref()
    }

    /// Returns retry policy, or `None` when retry is disabled.
    ///
    /// # Returns
    /// The retry schedule when configured, otherwise `None`.
    #[inline]
    #[must_use = "Use the returned retry policy."]
    pub fn retry_policy(&self) -> Option<&RetryPolicy> {
        self.retry_policy.as_ref()
    }

    /// Returns custom retry classification, or `None` for the default rule.
    ///
    /// # Returns
    /// The custom retry rule when configured, otherwise `None`.
    #[inline]
    #[must_use = "Use the returned retry rule."]
    pub fn retry_rule(&self) -> Option<&Arc<dyn RetryRule<DeliveryAttemptError>>> {
        self.retry_rule.as_ref()
    }

    /// Returns cancellation token, or `None` when absent.
    ///
    /// # Returns
    /// The shared cancellation token when configured, otherwise `None`.
    #[inline]
    #[must_use = "Use the returned retry cancellation token."]
    pub fn retry_cancellation_token(&self) -> Option<&RetryCancellationToken> {
        self.retry_cancellation_token.as_ref()
    }

    /// Returns subscriber error handlers in registration order.
    ///
    /// # Returns
    /// The terminal error callbacks without cloning them.
    #[must_use]
    #[inline]
    pub fn error_handlers(&self) -> &[Arc<SubscribeErrorHandler<T>>] {
        &self.error_handlers
    }

    /// Returns typed subscriber interceptors in registration order.
    ///
    /// # Returns
    /// The synchronous middleware chain without cloning it.
    #[must_use]
    #[inline]
    pub fn interceptors(&self) -> &[Arc<SubscriberInterceptor<T>>] {
        &self.interceptors
    }

    /// Returns async typed subscriber middleware in registration order.
    ///
    /// # Returns
    /// The runtime-neutral asynchronous middleware chain.
    #[must_use]
    #[inline]
    pub fn async_interceptors(&self) -> &[Arc<AsyncSubscriberInterceptor<T>>] {
        &self.async_interceptors
    }

    /// Returns dead-letter policy, or `None` when disabled.
    ///
    /// # Returns
    /// The terminal failure policy when configured, otherwise `None`.
    #[inline]
    #[must_use = "Use the returned dead letter."]
    pub fn dead_letter(&self) -> Option<&DeadLetterPolicy> {
        self.dead_letter.as_ref()
    }

    /// Returns requested ordering behavior.
    ///
    /// # Returns
    /// The provider ordering requirement.
    #[must_use = "Use the returned ordering policy."]
    #[inline]
    pub fn ordering_policy(&self) -> OrderingPolicy {
        self.ordering_policy
    }

    /// Returns consumer group, or `None` for a standalone subscriber.
    ///
    /// # Returns
    /// The shared consumer group when configured, otherwise `None`.
    #[inline]
    #[must_use = "Use the returned consumer group."]
    pub fn consumer_group(&self) -> Option<&ConsumerGroup> {
        self.consumer_group.as_ref()
    }

    /// Returns the durability request.
    ///
    /// # Returns
    /// The requested persistence guarantee.
    #[must_use = "Use the returned durability."]
    #[inline]
    pub fn durability(&self) -> SubscriptionDurability {
        self.durability
    }

    /// Returns the requested start position.
    ///
    /// # Returns
    /// The provider offset or cursor request.
    #[must_use = "Use the returned start position."]
    #[inline]
    pub fn start_position(&self) -> &StartPosition {
        &self.start_position
    }

    /// Returns namespaced provider options.
    ///
    /// # Returns
    /// The provider option map without cloning it.
    #[must_use]
    #[inline]
    pub fn provider_options(&self) -> &ProviderOptions {
        &self.provider_options
    }
}
