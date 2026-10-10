// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider ordering capabilities.

/// Message ordering scope guaranteed by a provider.
///
/// Each variant describes the boundary within which message order is preserved;
/// messages outside that boundary have no ordering guarantee from this
/// capability.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::OrderingCapability;
///
/// let capability = OrderingCapability::PerKey;
/// assert!(capability.supports_per_key());
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OrderingCapability {
    /// The provider does not guarantee message ordering.
    None,
    /// Message order is preserved within each subscription, but not across
    /// subscriptions.
    PerSubscription,
    /// Message order is preserved among messages with the same key.
    PerKey,
    /// Message order is preserved among messages in the same partition.
    PerPartition,
}

impl OrderingCapability {
    /// Returns whether this capability guarantees ordering for each key.
    /// Per-subscription ordering also preserves order for each key.
    ///
    /// # Returns
    /// `true` for per-key and per-subscription ordering guarantees.
    #[must_use]
    #[inline]
    pub const fn supports_per_key(self) -> bool {
        matches!(self, Self::PerKey | Self::PerSubscription)
    }
}
