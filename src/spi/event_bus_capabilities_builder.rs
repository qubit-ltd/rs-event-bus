// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Builder for explicit backend capability declarations.

use super::DelayedDeliveryCapability;
use super::DurabilityCapability;
use super::EventBusCapabilities;
use super::EventBusCapabilitiesBuildError;
use super::OrderingCapability;
use super::PayloadModes;
use super::PublishGuarantee;
use super::PublishVisibility;
use super::ReplayCapability;
use super::SettlementCapabilities;
use super::SubscriptionModes;

/// Configures each capability dimension before creating an immutable
/// [`EventBusCapabilities`] value.
///
/// All fields are required because omitting a declaration must not silently
/// assert that a provider lacks a capability or uses a particular delivery
/// policy.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::EventBusCapabilities;
/// use qubit_event_bus::spi::PayloadModes;
///
/// let error = EventBusCapabilities::builder()
///     .payload_modes(PayloadModes::Native)
///     .build()
///     .expect_err("the remaining capability fields are required");
/// assert_eq!(error.missing_field(), "settlement");
/// ```
#[must_use]
#[derive(Debug, Default)]
pub struct EventBusCapabilitiesBuilder {
    /// Supported native and encoded payload forms, when configured.
    payload_modes: Option<PayloadModes>,
    /// Supported terminal delivery actions, when configured.
    settlement: Option<SettlementCapabilities>,
    /// Strongest provider ordering guarantee, when configured.
    ordering: Option<OrderingCapability>,
    /// Native provider delay support, when configured.
    delayed_delivery: Option<DelayedDeliveryCapability>,
    /// Message retention policy, when configured.
    durability: Option<DurabilityCapability>,
    /// Accepted subscription modes, when configured.
    subscription_modes: Option<SubscriptionModes>,
    /// Provider-managed consumer-group support, when configured.
    consumer_groups: Option<bool>,
    /// Supported historical starting positions, when configured.
    replay: Option<ReplayCapability>,
    /// Successful publish guarantee, when configured.
    publish_guarantee: Option<PublishGuarantee>,
    /// Per-destination admission visibility, when configured.
    publish_visibility: Option<PublishVisibility>,
}

impl EventBusCapabilitiesBuilder {
    /// Sets the payload representations accepted by the provider.
    #[inline]
    pub fn payload_modes(mut self, payload_modes: PayloadModes) -> Self {
        self.payload_modes = Some(payload_modes);
        self
    }

    /// Sets the delivery settlement actions supported by the provider.
    #[inline]
    pub fn settlement(mut self, settlement: SettlementCapabilities) -> Self {
        self.settlement = Some(settlement);
        self
    }

    /// Sets the strongest ordering guarantee provided by the transport.
    #[inline]
    pub fn ordering(mut self, ordering: OrderingCapability) -> Self {
        self.ordering = Some(ordering);
        self
    }

    /// Sets whether the provider natively schedules delayed delivery.
    #[inline]
    pub fn delayed_delivery(mut self, delayed_delivery: DelayedDeliveryCapability) -> Self {
        self.delayed_delivery = Some(delayed_delivery);
        self
    }

    /// Sets whether messages remain available while subscribers are absent.
    #[inline]
    pub fn durability(mut self, durability: DurabilityCapability) -> Self {
        self.durability = Some(durability);
        self
    }

    /// Sets the subscription modes accepted by the provider.
    #[inline]
    pub fn subscription_modes(mut self, subscription_modes: SubscriptionModes) -> Self {
        self.subscription_modes = Some(subscription_modes);
        self
    }

    /// Sets whether the provider manages consumer groups.
    #[inline]
    pub fn consumer_groups(mut self, consumer_groups: bool) -> Self {
        self.consumer_groups = Some(consumer_groups);
        self
    }

    /// Sets the historical starting positions supported by new subscriptions.
    #[inline]
    pub fn replay(mut self, replay: ReplayCapability) -> Self {
        self.replay = Some(replay);
        self
    }

    /// Sets the strongest guarantee represented by a successful publish.
    #[inline]
    pub fn publish_guarantee(mut self, publish_guarantee: PublishGuarantee) -> Self {
        self.publish_guarantee = Some(publish_guarantee);
        self
    }

    /// Sets whether individual destination admissions are reported.
    #[inline]
    pub fn publish_visibility(mut self, publish_visibility: PublishVisibility) -> Self {
        self.publish_visibility = Some(publish_visibility);
        self
    }

    /// Creates an immutable capability declaration from all configured fields.
    ///
    /// # Returns
    /// The completed capability declaration.
    ///
    /// # Errors
    /// Returns an error containing the first required field that was not set.
    pub fn build(self) -> Result<EventBusCapabilities, EventBusCapabilitiesBuildError> {
        let payload_modes = self
            .payload_modes
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("payload_modes"))?;
        let settlement = self
            .settlement
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("settlement"))?;
        let ordering = self
            .ordering
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("ordering"))?;
        let delayed_delivery = self
            .delayed_delivery
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("delayed_delivery"))?;
        let durability = self
            .durability
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("durability"))?;
        let subscription_modes = self
            .subscription_modes
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("subscription_modes"))?;
        let consumer_groups = self
            .consumer_groups
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("consumer_groups"))?;
        let replay = self
            .replay
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("replay"))?;
        let publish_guarantee = self
            .publish_guarantee
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("publish_guarantee"))?;
        let publish_visibility = self
            .publish_visibility
            .ok_or_else(|| EventBusCapabilitiesBuildError::new("publish_visibility"))?;

        Ok(EventBusCapabilities::new(
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
        ))
    }
}
