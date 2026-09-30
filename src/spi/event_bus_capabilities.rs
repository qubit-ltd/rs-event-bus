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
use super::EventBusCapabilitiesBuilder;
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
/// use qubit_event_bus::spi::DelayedDeliveryCapability;
/// use qubit_event_bus::spi::DurabilityCapability;
/// use qubit_event_bus::spi::EventBusCapabilities;
/// use qubit_event_bus::spi::OrderingCapability;
/// use qubit_event_bus::spi::PayloadModes;
/// use qubit_event_bus::spi::PublishGuarantee;
/// use qubit_event_bus::spi::PublishVisibility;
/// use qubit_event_bus::spi::ReplayCapability;
/// use qubit_event_bus::spi::SettlementCapabilities;
/// use qubit_event_bus::spi::SubscriptionModes;
///
/// let capabilities = EventBusCapabilities::builder()
///     .payload_modes(PayloadModes::Native)
///     .settlement(SettlementCapabilities::None)
///     .ordering(OrderingCapability::None)
///     .delayed_delivery(DelayedDeliveryCapability::None)
///     .durability(DurabilityCapability::Ephemeral)
///     .subscription_modes(SubscriptionModes::EPHEMERAL)
///     .consumer_groups(false)
///     .replay(ReplayCapability::None)
///     .publish_guarantee(PublishGuarantee::Accepted)
///     .publish_visibility(PublishVisibility::Opaque)
///     .build()
///     .expect("all capability dimensions are configured");
/// assert_eq!(capabilities.payload_modes(), PayloadModes::Native);
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub struct EventBusCapabilities {
    /// Native and encoded payload forms accepted by this backend instance.
    payload_modes: PayloadModes,
    /// Delivery settlement operations supported by its subscriptions.
    settlement: SettlementCapabilities,
    /// Strongest ordering scope guaranteed by the transport.
    ordering: OrderingCapability,
    /// Whether the backend natively schedules delayed delivery.
    delayed_delivery: DelayedDeliveryCapability,
    /// Message retention while subscribers are absent.
    durability: DurabilityCapability,
    /// Ephemeral and durable subscription modes accepted by the backend.
    subscription_modes: SubscriptionModes,
    /// Whether the backend supports provider-managed consumer groups.
    consumer_groups: bool,
    /// Historical starting positions supported by new subscriptions.
    replay: ReplayCapability,
    /// Guarantee represented by a successful provider publish result.
    publish_guarantee: PublishGuarantee,
    /// Whether publication reports individual destination admission outcomes.
    publish_visibility: PublishVisibility,
}

impl EventBusCapabilities {
    /// Starts building a capability declaration without assigning implicit
    /// defaults to any provider capability.
    ///
    /// # Returns
    /// A builder whose fields must be configured before
    /// [`build`](EventBusCapabilitiesBuilder::build) can create a
    /// declaration.
    ///
    /// # Examples
    ///
    /// ```
    /// use qubit_event_bus::spi::EventBusCapabilities;
    ///
    /// let error = EventBusCapabilities::builder()
    ///     .build()
    ///     .expect_err("an empty builder has missing capability fields");
    /// assert_eq!(error.missing_field(), "payload_modes");
    /// ```
    #[inline]
    pub fn builder() -> EventBusCapabilitiesBuilder {
        EventBusCapabilitiesBuilder::default()
    }

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
    #[inline]
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
    /// The set of accepted subscription modes.
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
