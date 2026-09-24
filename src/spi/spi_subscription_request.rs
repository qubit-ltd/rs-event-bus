// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Type-erased subscription request passed to a provider.

use qubit_id::Id;

use super::TopicAddress;
use crate::model::ConsumerGroup;
use crate::model::ProviderOptions;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;

/// Transport-only subscription settings; application pipeline policy stays in
/// the facade.
pub struct SpiSubscriptionRequest {
    subscription_id: Id,
    topic: TopicAddress,
    subscriber_id: SubscriberId,
    group: Option<ConsumerGroup>,
    durability: SubscriptionDurability,
    start_position: StartPosition,
    provider_options: ProviderOptions,
}

impl SpiSubscriptionRequest {
    /// Creates a provider subscription request.
    pub fn new(
        subscription_id: Id,
        topic: TopicAddress,
        subscriber_id: SubscriberId,
        group: Option<ConsumerGroup>,
        durability: SubscriptionDurability,
        start_position: StartPosition,
        provider_options: ProviderOptions,
    ) -> Self {
        Self {
            subscription_id,
            topic,
            subscriber_id,
            group,
            durability,
            start_position,
            provider_options,
        }
    }
    /// Returns the bus-local subscription ID.
    pub fn subscription_id(&self) -> Id {
        self.subscription_id
    }
    /// Returns the topic address.
    pub fn topic(&self) -> &TopicAddress {
        &self.topic
    }
    /// Returns the logical subscriber ID.
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }
    /// Returns the optional consumer group.
    pub fn group(&self) -> Option<&ConsumerGroup> {
        self.group.as_ref()
    }
    /// Returns the requested durability.
    pub fn durability(&self) -> SubscriptionDurability {
        self.durability
    }
    /// Returns the requested starting position.
    pub fn start_position(&self) -> &StartPosition {
        &self.start_position
    }
    /// Returns provider-specific options.
    pub fn provider_options(&self) -> &ProviderOptions {
        &self.provider_options
    }
}
