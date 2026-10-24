// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Requested event ordering behavior.

/// Ordering requested by a subscriber.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::OrderingPolicy;
/// use qubit_event_bus::model::SubscribeOptions;
///
/// let policy = OrderingPolicy::PerKey;
/// let options = SubscribeOptions::<String>::builder()
///     .ordering_policy(policy)
///     .build();
/// assert_eq!(options.ordering_policy(), OrderingPolicy::PerKey);
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum OrderingPolicy {
    /// No ordering guarantee is required.
    #[default]
    Unordered,
    /// Preserve order for events with the same ordering key.
    PerKey,
}
