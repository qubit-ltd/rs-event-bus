// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Builder for type-erased provider subscription requests.

use std::any::TypeId;

use qubit_id::Id;

use super::SpiSubscriptionRequest;
use super::SpiSubscriptionRequestBuildError;
use super::TopicAddress;
use crate::model::ConsumerGroup;
use crate::model::ProviderOptions;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;

/// Configures all transport-only settings for a provider subscription.
///
/// Every field is required so the builder never invents a subscriber identity,
/// persistence mode, position, or payload type.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::SpiSubscriptionRequest;
///
/// let error = match SpiSubscriptionRequest::builder().build() {
///     Ok(_) => panic!("all provider request fields are required"),
///     Err(error) => error,
/// };
/// assert_eq!(error.missing_field(), "subscription_id");
/// ```
#[must_use]
#[derive(Debug, Default)]
pub struct SpiSubscriptionRequestBuilder {
    /// Bus-local receiver identity, when configured.
    subscription_id: Option<Id>,
    /// Validated provider destination, when configured.
    topic: Option<TopicAddress>,
    /// Logical subscriber identity, when configured.
    subscriber_id: Option<SubscriberId>,
    /// Optional shared group, explicitly set to `None` for independent use.
    group: Option<Option<ConsumerGroup>>,
    /// Requested persistence mode, when configured.
    durability: Option<SubscriptionDurability>,
    /// Requested initial provider position, when configured.
    start_position: Option<StartPosition>,
    /// Namespaced backend options, when configured.
    provider_options: Option<ProviderOptions>,
    /// Native payload type identity, when configured.
    payload_type_id: Option<TypeId>,
}

impl SpiSubscriptionRequestBuilder {
    /// Sets the bus-local receiver identity used for settlement tokens.
    ///
    /// # Parameters
    /// `subscription_id` is the identity assigned by the event bus to this
    /// receiver.
    ///
    /// # Returns
    /// The builder with the receiver identity configured.
    #[inline]
    pub fn subscription_id(mut self, subscription_id: Id) -> Self {
        self.subscription_id = Some(subscription_id);
        self
    }

    /// Sets the validated provider destination for the subscription.
    ///
    /// # Parameters
    /// `topic` is the destination previously validated for the provider.
    ///
    /// # Returns
    /// The builder with the provider destination configured.
    #[inline]
    pub fn topic(mut self, topic: TopicAddress) -> Self {
        self.topic = Some(topic);
        self
    }

    /// Sets the validated logical subscriber identity.
    ///
    /// # Parameters
    /// `subscriber_id` identifies the logical subscriber independently of
    /// this bus-local receiver.
    ///
    /// # Returns
    /// The builder with the logical subscriber identity configured.
    #[inline]
    pub fn subscriber_id(mut self, subscriber_id: SubscriberId) -> Self {
        self.subscriber_id = Some(subscriber_id);
        self
    }

    /// Sets the shared consumer group, or explicitly sets `None` for no group.
    ///
    /// # Parameters
    /// `group` is `Some` for shared consumption or `None` for independent use.
    ///
    /// # Returns
    /// The builder with the shared group setting configured.
    #[inline]
    pub fn group(mut self, group: Option<ConsumerGroup>) -> Self {
        self.group = Some(group);
        self
    }

    /// Sets the requested subscription persistence mode.
    ///
    /// # Parameters
    /// `durability` selects the persistence behavior requested from the
    /// provider.
    ///
    /// # Returns
    /// The builder with the persistence mode configured.
    #[inline]
    pub fn durability(mut self, durability: SubscriptionDurability) -> Self {
        self.durability = Some(durability);
        self
    }

    /// Sets the provider position from which the subscription should begin.
    ///
    /// # Parameters
    /// `start_position` specifies the initial position requested from the
    /// provider.
    ///
    /// # Returns
    /// The builder with the initial provider position configured.
    #[inline]
    pub fn start_position(mut self, start_position: StartPosition) -> Self {
        self.start_position = Some(start_position);
        self
    }

    /// Sets the namespaced options passed through to the provider.
    ///
    /// # Parameters
    /// `provider_options` contains backend-specific options for the provider.
    ///
    /// # Returns
    /// The builder with the provider options configured.
    #[inline]
    pub fn provider_options(mut self, provider_options: ProviderOptions) -> Self {
        self.provider_options = Some(provider_options);
        self
    }

    /// Sets the Rust payload type used by native routing providers.
    ///
    /// # Parameters
    /// `payload_type_id` identifies the native payload type expected by the
    /// provider.
    ///
    /// # Returns
    /// The builder with the native payload type configured.
    #[inline]
    pub fn payload_type_id(mut self, payload_type_id: TypeId) -> Self {
        self.payload_type_id = Some(payload_type_id);
        self
    }

    /// Creates a request after every transport field has been supplied.
    ///
    /// # Returns
    /// A provider request containing the configured transport settings.
    ///
    /// # Errors
    /// Returns the first missing field in declaration order.
    pub fn build(self) -> Result<SpiSubscriptionRequest, SpiSubscriptionRequestBuildError> {
        let subscription_id = self
            .subscription_id
            .ok_or_else(|| SpiSubscriptionRequestBuildError::new("subscription_id"))?;
        let topic = self
            .topic
            .ok_or_else(|| SpiSubscriptionRequestBuildError::new("topic"))?;
        let subscriber_id = self
            .subscriber_id
            .ok_or_else(|| SpiSubscriptionRequestBuildError::new("subscriber_id"))?;
        let group = self
            .group
            .ok_or_else(|| SpiSubscriptionRequestBuildError::new("group"))?;
        let durability = self
            .durability
            .ok_or_else(|| SpiSubscriptionRequestBuildError::new("durability"))?;
        let start_position = self
            .start_position
            .ok_or_else(|| SpiSubscriptionRequestBuildError::new("start_position"))?;
        let provider_options = self
            .provider_options
            .ok_or_else(|| SpiSubscriptionRequestBuildError::new("provider_options"))?;
        let payload_type_id = self
            .payload_type_id
            .ok_or_else(|| SpiSubscriptionRequestBuildError::new("payload_type_id"))?;

        Ok(SpiSubscriptionRequest::new(
            subscription_id,
            topic,
            subscriber_id,
            group,
            durability,
            start_position,
            provider_options,
            payload_type_id,
        ))
    }
}
