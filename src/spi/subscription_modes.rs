// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscription modes accepted by a provider.

use crate::model::SubscriptionDurability;

/// The set of subscription durability modes accepted by a provider.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::SubscriptionModes;
///
/// assert!(SubscriptionModes::BOTH.supports(
///     qubit_event_bus::model::SubscriptionDurability::Durable,
/// ));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SubscriptionModes(
    /// Bit mask whose low bits enable ephemeral and durable subscriptions.
    u8,
);

impl SubscriptionModes {
    /// Provider accepts ephemeral subscriptions.
    pub const EPHEMERAL: Self = Self(0b01);
    /// Provider accepts durable subscriptions.
    pub const DURABLE: Self = Self(0b10);
    /// Provider accepts both ephemeral and durable subscriptions.
    pub const BOTH: Self = Self(0b11);

    /// Returns whether this provider accepts the requested subscription mode.
    ///
    /// # Parameters
    /// - `durability`: subscription persistence mode to check.
    ///
    /// # Returns
    /// `true` when the provider accepts the requested mode.
    #[must_use]
    #[inline]
    pub const fn supports(self, durability: SubscriptionDurability) -> bool {
        let required = match durability {
            SubscriptionDurability::Ephemeral => Self::EPHEMERAL.0,
            SubscriptionDurability::Durable => Self::DURABLE.0,
        };
        self.0 & required != 0
    }
}
