// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Backend capability value assembled from its typed dimensions.

use super::DelayedDeliveryCapability;
use super::DurabilityCapability;
use super::OrderingCapability;
use super::PayloadModes;
use super::PublishGuarantee;
use super::PublishVisibility;
use super::ReplayCapability;
use super::SettlementCapabilities;
use super::SubscriptionModes;

/// Immutable capabilities declared by one backend instance.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventBusCapabilities {
    payload_modes: PayloadModes,
    settlement: SettlementCapabilities,
    ordering: OrderingCapability,
    delayed_delivery: DelayedDeliveryCapability,
    durability: DurabilityCapability,
    subscription_modes: SubscriptionModes,
    consumer_groups: bool,
    replay: ReplayCapability,
    publish_guarantee: PublishGuarantee,
    publish_visibility: PublishVisibility,
}

impl EventBusCapabilities {
    /// Creates a capability declaration for a backend instance.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        payload_modes: PayloadModes,
        settlement: SettlementCapabilities,
        ordering: OrderingCapability,
        delayed_delivery: DelayedDeliveryCapability,
        durability: DurabilityCapability,
        consumer_groups: bool,
        replay: ReplayCapability,
        publish_guarantee: PublishGuarantee,
        publish_visibility: PublishVisibility,
    ) -> Self {
        let subscription_modes = match durability {
            DurabilityCapability::Ephemeral => SubscriptionModes::EPHEMERAL,
            DurabilityCapability::Durable => SubscriptionModes::DURABLE,
        };
        Self {
            payload_modes,
            settlement,
            ordering,
            delayed_delivery,
            durability,
            subscription_modes,
            consumer_groups,
            replay,
            publish_guarantee,
            publish_visibility,
        }
    }

    /// Overrides the accepted subscription modes.
    ///
    /// The default inferred by [`Self::new`] accepts the mode matching the
    /// provider's durability capability. Use this method when the provider
    /// accepts a different set, including both modes.
    #[must_use]
    pub const fn with_subscription_modes(mut self, modes: SubscriptionModes) -> Self {
        self.subscription_modes = modes;
        self
    }

    /// Returns the supported payload representations.
    pub const fn payload_modes(self) -> PayloadModes {
        self.payload_modes
    }
    /// Returns supported settlement actions.
    pub const fn settlement(self) -> SettlementCapabilities {
        self.settlement
    }
    /// Returns the strongest ordering scope guaranteed by this provider.
    pub const fn ordering(self) -> OrderingCapability {
        self.ordering
    }
    /// Returns the native delayed-delivery capability.
    pub const fn delayed_delivery(self) -> DelayedDeliveryCapability {
        self.delayed_delivery
    }
    /// Returns the durability capability.
    pub const fn durability(self) -> DurabilityCapability {
        self.durability
    }
    /// Returns the subscription modes accepted by this provider.
    pub const fn subscription_modes(self) -> SubscriptionModes {
        self.subscription_modes
    }
    /// Returns whether consumer groups are supported.
    pub const fn consumer_groups(self) -> bool {
        self.consumer_groups
    }
    /// Returns the replay capability.
    pub const fn replay(self) -> ReplayCapability {
        self.replay
    }
    /// Returns the maximum publish guarantee.
    pub const fn publish_guarantee(self) -> PublishGuarantee {
        self.publish_guarantee
    }
    /// Returns the publish visibility capability.
    pub const fn publish_visibility(self) -> PublishVisibility {
        self.publish_visibility
    }
}
