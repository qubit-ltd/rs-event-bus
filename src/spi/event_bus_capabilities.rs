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
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::{
///     DelayedDeliveryCapability, DurabilityCapability, EventBusCapabilities,
///     OrderingCapability, PayloadModes, PublishGuarantee, PublishVisibility,
///     ReplayCapability, SettlementCapabilities, SubscriptionModes,
/// };
///
/// let capabilities = EventBusCapabilities::new(
///     PayloadModes::Native, SettlementCapabilities::None,
///     OrderingCapability::None, DelayedDeliveryCapability::None,
///     DurabilityCapability::Ephemeral, SubscriptionModes::EPHEMERAL,
///     false, ReplayCapability::None, PublishGuarantee::Accepted,
///     PublishVisibility::Opaque,
/// );
/// assert_eq!(capabilities.payload_modes(), PayloadModes::Native);
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
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
    ///
    /// # Parameters
    /// - `payload_modes`: supported native and encoded payload forms.
    /// - `settlement`: supported terminal delivery actions.
    /// - `ordering`: strongest ordering guarantee.
    /// - `delayed_delivery`: native provider delay support.
    /// - `durability`: whether messages persist while subscribers are absent.
    /// - `subscription_modes`: accepted subscription persistence modes.
    /// - `consumer_groups`: whether provider-managed consumer groups are
    ///   supported.
    /// - `replay`: supported historical starting positions.
    /// - `publish_guarantee`: strongest successful publish guarantee.
    /// - `publish_visibility`: visibility of per-destination admissions.
    ///
    /// # Returns
    /// An immutable capability declaration containing the supplied values.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
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
    ) -> Self {
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

    /// Returns the supported payload representations.
    ///
    /// # Returns
    /// Supported native and encoded payload modes.
    #[must_use]
    #[inline]
    pub const fn payload_modes(self) -> PayloadModes {
        self.payload_modes
    }
    /// Returns supported settlement actions.
    ///
    /// # Returns
    /// The provider's settlement capability.
    #[must_use]
    #[inline]
    pub const fn settlement(self) -> SettlementCapabilities {
        self.settlement
    }
    /// Returns the strongest ordering scope guaranteed by this provider.
    ///
    /// # Returns
    /// The strongest provider ordering capability.
    #[must_use]
    #[inline]
    pub const fn ordering(self) -> OrderingCapability {
        self.ordering
    }
    /// Returns the native delayed-delivery capability.
    ///
    /// # Returns
    /// The provider's delayed-delivery capability.
    #[must_use]
    #[inline]
    pub const fn delayed_delivery(self) -> DelayedDeliveryCapability {
        self.delayed_delivery
    }
    /// Returns the durability capability.
    ///
    /// # Returns
    /// The provider's message durability.
    #[must_use]
    #[inline]
    pub const fn durability(self) -> DurabilityCapability {
        self.durability
    }
    /// Returns the subscription modes accepted by this provider.
    ///
    /// # Returns
    /// The set of accepted durability modes.
    #[must_use]
    #[inline]
    pub const fn subscription_modes(self) -> SubscriptionModes {
        self.subscription_modes
    }
    /// Returns whether consumer groups are supported.
    ///
    /// # Returns
    /// `true` when consumer groups are supported.
    #[must_use]
    #[inline]
    pub const fn consumer_groups(self) -> bool {
        self.consumer_groups
    }
    /// Returns the replay capability.
    ///
    /// # Returns
    /// The provider's historical replay capability.
    #[must_use]
    #[inline]
    pub const fn replay(self) -> ReplayCapability {
        self.replay
    }
    /// Returns the maximum publish guarantee.
    ///
    /// # Returns
    /// The strongest publish guarantee represented by success.
    #[must_use]
    #[inline]
    pub const fn publish_guarantee(self) -> PublishGuarantee {
        self.publish_guarantee
    }
    /// Returns the publish visibility capability.
    ///
    /// # Returns
    /// Whether individual destination admission is visible.
    #[must_use]
    #[inline]
    pub const fn publish_visibility(self) -> PublishVisibility {
        self.publish_visibility
    }
}
