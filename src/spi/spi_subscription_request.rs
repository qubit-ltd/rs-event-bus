// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-erased subscription request passed to a provider.

use std::any::TypeId;

use qubit_id::Id;

use super::TopicAddress;
use crate::model::ConsumerGroup;
use crate::model::ProviderOptions;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;

/// Transport-only subscription settings; application pipeline policy stays in
/// the facade.
///
/// # Examples
///
/// ```
/// use std::any::TypeId;
/// use qubit_event_bus::model::{ProviderOptions, StartPosition, SubscriberId, SubscriptionDurability};
/// use qubit_event_bus::spi::{SpiSubscriptionRequest, TopicAddress};
/// use qubit_id::Id;
///
/// let request = SpiSubscriptionRequest::new(
///     Id::new(1), TopicAddress::new("orders.created").unwrap(),
///     SubscriberId::new("audit").unwrap(), None,
///     SubscriptionDurability::Ephemeral, StartPosition::New,
///     ProviderOptions::new(), TypeId::of::<String>(),
/// );
/// assert_eq!(request.topic().as_str(), "orders.created");
/// ```
#[must_use]
pub struct SpiSubscriptionRequest {
    subscription_id: Id,
    topic: TopicAddress,
    subscriber_id: SubscriberId,
    group: Option<ConsumerGroup>,
    durability: SubscriptionDurability,
    start_position: StartPosition,
    provider_options: ProviderOptions,
    payload_type_id: TypeId,
}

impl SpiSubscriptionRequest {
    /// Creates a provider subscription request.
    ///
    /// # Parameters
    /// - `subscription_id`: bus-local subscription identity.
    /// - `topic`: validated provider destination.
    /// - `subscriber_id`: validated logical subscriber identity.
    /// - `group`: optional shared consumer group.
    /// - `durability`: requested persistence mode.
    /// - `start_position`: requested initial provider position.
    /// - `provider_options`: namespaced options passed to the backend.
    /// - `payload_type_id`: Rust payload type for native routing.
    ///
    /// # Returns
    /// A provider request containing the supplied transport settings.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subscription_id: Id,
        topic: TopicAddress,
        subscriber_id: SubscriberId,
        group: Option<ConsumerGroup>,
        durability: SubscriptionDurability,
        start_position: StartPosition,
        provider_options: ProviderOptions,
        payload_type_id: TypeId,
    ) -> Self {
        Self {
            subscription_id,
            topic,
            subscriber_id,
            group,
            durability,
            start_position,
            provider_options,
            payload_type_id,
        }
    }
    /// Returns the bus-local subscription ID.
    ///
    /// # Returns
    /// The identity used to associate receiver settlement tokens.
    #[inline]
    pub fn subscription_id(&self) -> Id {
        self.subscription_id
    }
    /// Returns the topic address.
    ///
    /// # Returns
    /// The validated provider destination.
    #[inline]
    pub fn topic(&self) -> &TopicAddress {
        &self.topic
    }
    /// Returns the logical subscriber ID.
    ///
    /// # Returns
    /// The validated logical subscriber identity.
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }
    /// Returns the optional consumer group.
    ///
    /// # Returns
    /// `Some` with the shared group when set, otherwise `None`.
    #[must_use]
    #[inline]
    pub fn group(&self) -> Option<&ConsumerGroup> {
        self.group.as_ref()
    }
    /// Returns the requested durability.
    ///
    /// # Returns
    /// The subscription persistence mode.
    #[inline]
    pub fn durability(&self) -> SubscriptionDurability {
        self.durability
    }
    /// Returns the requested starting position.
    ///
    /// # Returns
    /// The provider offset or cursor request.
    #[inline]
    pub fn start_position(&self) -> &StartPosition {
        &self.start_position
    }
    /// Returns provider-specific options.
    ///
    /// # Returns
    /// The namespaced provider option map.
    #[must_use]
    #[inline]
    pub fn provider_options(&self) -> &ProviderOptions {
        &self.provider_options
    }

    /// Returns the Rust payload type associated with the typed topic.
    ///
    /// Providers that route encoded payloads may ignore this in-process type
    /// identity. The local native provider uses it to reject same-name topics
    /// with incompatible payload types.
    ///
    /// # Returns
    /// The `TypeId` of the typed topic payload.
    #[must_use]
    #[inline]
    pub fn payload_type_id(&self) -> TypeId {
        self.payload_type_id
    }
}
