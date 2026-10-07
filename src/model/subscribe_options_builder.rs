// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Construction of reusable subscription policy values.

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
use super::GapPolicy;
use super::OrderingPolicy;
use super::ProviderOptions;
use super::StartPosition;
use super::SubscribeOptions;
use super::SubscriberNext;
use super::SubscriptionDurability;
use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;
use crate::spi::SpiFuture;

/// Builds reusable subscription policy independently of identity and topic.
///
/// # Type Parameters
/// - `T`: payload type received by the configured subscriber.
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
#[must_use]
pub struct SubscribeOptionsBuilder<T: 'static> {
    /// Mutable policy state accumulated by builder methods.
    options: SubscribeOptions<T>,
}

impl<T: 'static> SubscribeOptionsBuilder<T> {
    /// Starts with automatic ACK, ephemeral durability, and new messages.
    ///
    /// # Returns
    /// A builder containing default subscription options.
    #[inline]
    pub fn new() -> Self {
        Self {
            options: SubscribeOptions::default(),
        }
    }

    /// Sets whether receiving stops after a delivery gap.
    #[must_use = "Use the returned builder."]
    pub fn gap_policy(mut self, value: GapPolicy) -> Self {
        self.options.gap_policy = value;
        self
    }

    /// Replaces acknowledgement mode.
    ///
    /// # Parameters
    /// - `value`: acknowledgement behavior for handler completion.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn ack_mode(mut self, value: AckMode) -> Self {
        self.options.ack_mode = value;
        self
    }

    /// Replaces the event filter.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe predicate callable type.
    ///
    /// # Parameters
    /// - `value`: predicate evaluated before subscriber middleware.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn filter<F>(mut self, value: F) -> Self
    where
        F: Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static,
    {
        self.options.filter = Some(Arc::new(value));
        self
    }

    /// Replaces the retry policy from `qubit-retry`.
    ///
    /// # Parameters
    /// - `value`: policy that schedules and classifies delivery retries.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.options.retry_policy = Some(value);
        self
    }

    /// Replaces the typed retry rule from `qubit-retry`.
    ///
    /// # Type Parameters
    /// - `R`: retry rule implementation for delivery attempt failures.
    ///
    /// # Parameters
    /// - `value`: rule used to classify handler failures.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn retry_rule<R>(mut self, value: R) -> Self
    where
        R: RetryRule<DeliveryAttemptError>,
    {
        self.options.retry_rule = Some(Arc::new(value));
        self
    }

    /// Replaces the retry cancellation token.
    ///
    /// # Parameters
    /// - `value`: shared signal used to cancel retry waits.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn retry_cancellation_token(mut self, value: RetryCancellationToken) -> Self {
        self.options.retry_cancellation_token = Some(value);
        self
    }

    /// Appends an error handler in registration order.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe delivery error handler callable type.
    ///
    /// # Parameters
    /// - `handler`: callback that selects a terminal failure directive.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn error_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&EventEnvelope<T>, &DeliveryError) -> FailureDirective + Send + Sync + 'static,
    {
        self.options.error_handlers.push(Arc::new(handler));
        self
    }

    /// Appends a typed subscriber interceptor in registration order.
    ///
    /// # Type Parameters
    /// - `F`: synchronous middleware callable type.
    ///
    /// # Parameters
    /// - `value`: middleware appended to the subscriber chain.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn interceptor<F>(mut self, value: F) -> Self
    where
        F: Fn(Delivery<T>, SubscriberNext<T>) -> Result<(), DeliveryError> + Send + Sync + 'static,
    {
        self.options.interceptors.push(Arc::new(value));
        self
    }

    /// Appends runtime-neutral asynchronous middleware in registration order.
    ///
    /// # Type Parameters
    /// - `F`: asynchronous middleware callable type.
    ///
    /// # Parameters
    /// - `value`: middleware appended to the async subscriber chain.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
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
    ///
    /// # Parameters
    /// - `value`: terminal delivery failure policy.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn dead_letter(mut self, value: DeadLetterPolicy) -> Self {
        self.options.dead_letter = Some(value);
        self
    }

    /// Replaces the ordering policy.
    ///
    /// # Parameters
    /// - `value`: ordering guarantee requested from the provider.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn ordering_policy(mut self, value: OrderingPolicy) -> Self {
        self.options.ordering_policy = value;
        self
    }

    /// Replaces the provider consumer group.
    ///
    /// # Parameters
    /// - `value`: group shared by provider-managed consumer instances.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn consumer_group(mut self, value: ConsumerGroup) -> Self {
        self.options.consumer_group = Some(value);
        self
    }

    /// Replaces subscription durability.
    ///
    /// # Parameters
    /// - `value`: persistence guarantee requested from the provider.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn durability(mut self, value: SubscriptionDurability) -> Self {
        self.options.durability = value;
        self
    }

    /// Replaces the starting position.
    ///
    /// # Parameters
    /// - `value`: provider offset from which delivery should begin.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    #[inline]
    pub fn start_position(mut self, value: StartPosition) -> Self {
        self.options.start_position = value;
        self
    }

    /// Adds or replaces a namespaced provider option.
    ///
    /// # Parameters
    /// - `key`: provider-defined option name, including its namespace.
    /// - `value`: printable provider option value.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn provider_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options
            .provider_options
            .insert(key.into(), value.into());
        self
    }

    /// Merges provider options in iteration order, replacing earlier values.
    ///
    /// # Parameters
    /// - `values`: namespaced provider settings to merge.
    ///
    /// # Returns
    /// The updated builder.
    #[must_use = "Use the returned builder."]
    pub fn provider_options(mut self, values: ProviderOptions) -> Self {
        self.options.provider_options.extend(values);
        self
    }

    /// Consumes the builder and returns the reusable options.
    ///
    /// Request construction performs cross-field validation for retry policy
    /// and provider options.
    ///
    /// # Returns
    /// The configured subscription options.
    #[inline]
    pub fn build(self) -> SubscribeOptions<T> {
        self.options
    }
}

impl<T: 'static> Default for SubscribeOptionsBuilder<T> {
    /// Creates a builder with automatic ACK, ephemeral durability, and new
    /// events.
    ///
    /// # Returns
    /// A builder with the default subscription options.
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}
