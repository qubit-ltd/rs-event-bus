// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscription state retention preferences.

/// Whether a subscription survives an application disconnect.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::SubscriptionDurability;
///
/// let durability = SubscriptionDurability::Durable;
/// assert_eq!(durability, SubscriptionDurability::Durable);
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum SubscriptionDurability {
    /// The provider may remove state when the subscriber leaves.
    #[default]
    Ephemeral,
    /// The provider retains subscription state across disconnects.
    Durable,
}
