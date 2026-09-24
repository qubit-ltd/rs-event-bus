// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider ordering capabilities.

/// Strongest message ordering scope guaranteed by a provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OrderingCapability {
    /// No ordering guarantee.
    None,
    /// Ordering is maintained per subscription.
    PerSubscription,
    /// Ordering is maintained for each key.
    PerKey,
    /// Ordering is maintained within each partition.
    PerPartition,
}

impl OrderingCapability {
    /// Returns whether this capability guarantees ordering for each key.
    /// Per-subscription ordering also preserves order for each key.
    #[must_use]
    pub const fn supports_per_key(self) -> bool {
        matches!(self, Self::PerKey | Self::PerSubscription)
    }
}
