// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Creation-time requirements checked against provider SPI capabilities.

use crate::spi::DelayedDeliveryCapability;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::OrderingCapability;
use crate::spi::PayloadModes;
use crate::spi::PublishGuarantee;
use crate::spi::PublishVisibility;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;

/// Optional minimum behavior required from a provider before facade creation.
///
/// Requirements are checked once, after SPI creation and before a facade is
/// returned. They do not cause failover for errors from later operations.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::registry::EventBusRegistry;
/// use qubit_event_bus::registry::RequiredCapabilities;
/// use qubit_event_bus::spi::PayloadModes;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let requirements = RequiredCapabilities::new().with_payload(PayloadModes::Native);
/// let config = EventBusConfig::default().with_required_capabilities(requirements);
/// let registry = EventBusRegistry::with_local()?;
/// let bus = registry.create(&config)?;
/// bus.shutdown(ShutdownMode::Immediate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RequiredCapabilities {
    /// Required payload representations, or `None` to accept any declaration.
    payload: Option<PayloadModes>,
    /// Minimum settlement operations, or `None` when unconstrained.
    settlement: Option<SettlementCapabilities>,
    /// Required ordering scope, or `None` when ordering is unconstrained.
    ordering: Option<OrderingCapability>,
    /// Native delay requirement, or `None` to accept any delay behavior.
    delayed_delivery: Option<DelayedDeliveryCapability>,
    /// Minimum retention behavior, or `None` when persistence is unconstrained.
    durability: Option<DurabilityCapability>,
    /// Whether provider-managed consumer groups are mandatory.
    consumer_groups: bool,
    /// Required replay positions, or `None` when replay is unconstrained.
    replay: Option<ReplayCapability>,
    /// Minimum successful publication guarantee, or `None` when unconstrained.
    publish_guarantee: Option<PublishGuarantee>,
    /// Destination admission visibility requirement, or `None` when
    /// unconstrained.
    publish_visibility: Option<PublishVisibility>,
}

impl RequiredCapabilities {
    /// Creates a requirement set that accepts any provider capabilities.
    ///
    /// # Returns
    /// An empty requirement set.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            payload: None,
            settlement: None,
            ordering: None,
            delayed_delivery: None,
            durability: None,
            consumer_groups: false,
            replay: None,
            publish_guarantee: None,
            publish_visibility: None,
        }
    }

    /// Requires a provider to support durable message retention.
    ///
    /// # Returns
    /// The updated requirements with durable retention enabled.
    #[must_use]
    #[inline]
    pub const fn durable(mut self) -> Self {
        self.durability = Some(DurabilityCapability::Durable);
        self
    }

    /// Sets the payload mode that the selected SPI must accept.
    ///
    /// # Parameters
    /// - `payload`: required payload mode.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_payload(mut self, payload: PayloadModes) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Sets the minimum settlement operations required from the SPI.
    ///
    /// # Parameters
    /// - `settlement`: minimum settlement capability.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_settlement(mut self, settlement: SettlementCapabilities) -> Self {
        self.settlement = Some(settlement);
        self
    }

    /// Requires the selected SPI to provide the named ordering guarantee.
    ///
    /// # Parameters
    /// - `ordering`: ordering behavior the SPI must support.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_ordering(mut self, ordering: OrderingCapability) -> Self {
        self.ordering = Some(ordering);
        self
    }

    /// Sets whether native delayed delivery is required.
    ///
    /// # Parameters
    /// - `delayed`: delayed-delivery capability required from the SPI.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_delayed_delivery(mut self, delayed: DelayedDeliveryCapability) -> Self {
        self.delayed_delivery = Some(delayed);
        self
    }

    /// Sets the minimum durability behavior required of a provider.
    ///
    /// # Parameters
    /// - `durability`: persistence behavior required from the SPI.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_durability(mut self, durability: DurabilityCapability) -> Self {
        self.durability = Some(durability);
        self
    }

    /// Requires provider support for consumer groups when set to `true`.
    ///
    /// # Parameters
    /// - `required`: whether consumer-group support is mandatory.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_consumer_groups(mut self, required: bool) -> Self {
        self.consumer_groups = required;
        self
    }

    /// Sets the historical replay mode required from the provider.
    ///
    /// # Parameters
    /// - `replay`: replay capability required from the SPI.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_replay(mut self, replay: ReplayCapability) -> Self {
        self.replay = Some(replay);
        self
    }

    /// Sets the minimum publish acknowledgement guarantee required.
    ///
    /// # Parameters
    /// - `guarantee`: minimum publish guarantee required from the SPI.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_publish_guarantee(mut self, guarantee: PublishGuarantee) -> Self {
        self.publish_guarantee = Some(guarantee);
        self
    }

    /// Sets the per-destination publish visibility requirement.
    ///
    /// # Parameters
    /// - `visibility`: publication visibility required from the SPI.
    ///
    /// # Returns
    /// The updated requirement set.
    #[must_use]
    #[inline]
    pub const fn with_publish_visibility(mut self, visibility: PublishVisibility) -> Self {
        self.publish_visibility = Some(visibility);
        self
    }

    /// Requires the requested provider behavior and returns missing labels.
    ///
    /// # Parameters
    /// - `capabilities`: actual capabilities reported by the provider SPI.
    ///
    /// # Returns
    /// Stable labels for requirements the provider does not satisfy.
    #[must_use]
    pub(crate) fn missing_from(self, capabilities: EventBusCapabilities) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if let Some(required) = self.payload
            && !payload_satisfies(capabilities.payload_modes(), required)
        {
            missing.push("payload");
        }
        if let Some(required) = self.settlement
            && !settlement_satisfies(capabilities.settlement(), required)
        {
            missing.push("settlement");
        }
        if let Some(required) = self.ordering {
            let actual = capabilities.ordering();
            let satisfied = if required == OrderingCapability::PerKey {
                actual.supports_per_key()
            } else {
                actual == required
            };
            if !satisfied {
                missing.push("ordering");
            }
        }
        if let Some(required) = self.delayed_delivery
            && required == DelayedDeliveryCapability::Native
            && capabilities.delayed_delivery() != DelayedDeliveryCapability::Native
        {
            missing.push("delayed_delivery");
        }
        if let Some(required) = self.durability
            && required == DurabilityCapability::Durable
            && capabilities.durability() != DurabilityCapability::Durable
        {
            missing.push("durability");
        }
        if self.consumer_groups && !capabilities.consumer_groups() {
            missing.push("consumer_groups");
        }
        if let Some(required) = self.replay
            && !replay_satisfies(capabilities.replay(), required)
        {
            missing.push("replay");
        }
        if let Some(required) = self.publish_guarantee
            && !publish_guarantee_satisfies(capabilities.publish_guarantee(), required)
        {
            missing.push("publish_guarantee");
        }
        if let Some(required) = self.publish_visibility
            && required == PublishVisibility::DestinationAdmissions
            && capabilities.publish_visibility() != PublishVisibility::DestinationAdmissions
        {
            missing.push("publish_visibility");
        }
        missing
    }
}

/// Reports whether the actual payload mode includes the requested mode.
///
/// # Parameters
/// - `actual`: payload modes supported by the provider.
/// - `required`: payload mode requested by the caller.
///
/// # Returns
/// `true` when `actual` includes every behavior required by `required`.
fn payload_satisfies(actual: PayloadModes, required: PayloadModes) -> bool {
    match required {
        PayloadModes::Native => matches!(
            actual,
            PayloadModes::Native | PayloadModes::NativeAndEncoded
        ),
        PayloadModes::Encoded => matches!(
            actual,
            PayloadModes::Encoded | PayloadModes::NativeAndEncoded
        ),
        PayloadModes::NativeAndEncoded => actual == PayloadModes::NativeAndEncoded,
    }
}

/// Reports whether the actual settlement operations include the requested set.
///
/// # Parameters
/// - `actual`: settlement operations supported by the provider.
/// - `required`: minimum operations requested by the caller.
///
/// # Returns
/// `true` when `actual` supports the requested settlement operations.
fn settlement_satisfies(actual: SettlementCapabilities, required: SettlementCapabilities) -> bool {
    match required {
        SettlementCapabilities::None => true,
        SettlementCapabilities::AcceptOnly => matches!(
            actual,
            SettlementCapabilities::AcceptOnly | SettlementCapabilities::AcceptRetryReject
        ),
        SettlementCapabilities::AcceptRetryReject => {
            actual == SettlementCapabilities::AcceptRetryReject
        }
    }
}

/// Reports whether the actual replay capability includes the requested mode.
///
/// # Parameters
/// - `actual`: replay behavior supported by the provider.
/// - `required`: replay behavior requested by the caller.
///
/// # Returns
/// `true` when `actual` supports the requested replay behavior.
fn replay_satisfies(actual: ReplayCapability, required: ReplayCapability) -> bool {
    match required {
        ReplayCapability::None => true,
        ReplayCapability::Position => matches!(
            actual,
            ReplayCapability::Position | ReplayCapability::Timestamp
        ),
        ReplayCapability::Timestamp => actual == ReplayCapability::Timestamp,
    }
}

/// Compares publication guarantees by their increasing strength.
///
/// # Parameters
/// - `actual`: guarantee provided by the SPI.
/// - `required`: minimum guarantee requested by the caller.
///
/// # Returns
/// `true` when the actual guarantee is at least as strong as required.
fn publish_guarantee_satisfies(actual: PublishGuarantee, required: PublishGuarantee) -> bool {
    let rank = |value| match value {
        PublishGuarantee::FireAndForget => 0,
        PublishGuarantee::Accepted => 1,
        PublishGuarantee::Confirmed => 2,
        PublishGuarantee::DurablyStored => 3,
    };
    rank(actual) >= rank(required)
}
