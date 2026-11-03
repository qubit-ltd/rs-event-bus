// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Stable categories for per-destination admission rejection.

/// Stable reason category for a destination rejected by a provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AdmissionRejectionKind {
    /// The subscription was closed when admission was attempted.
    SubscriptionClosed,
    /// The subscription queue reached its pending and in-flight limit.
    SubscriptionCapacity,
    /// The provider-wide outstanding delivery count was exhausted.
    ProviderCountCapacity,
    /// The provider-wide declared weight budget was exhausted.
    ProviderWeightCapacity,
}
