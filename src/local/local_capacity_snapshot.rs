// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Point-in-time local provider capacity and rejection totals.

use std::num::NonZeroUsize;

/// Capacity and target rejection counters copied from a local provider.
///
/// Outstanding values count accepted delivery copies that have not reached
/// terminal release, including pending and in-flight deliveries. Declared
/// weight is application supplied accounting, not process memory usage. The
/// rejection counters count destination attempts, so fanout and retries can
/// each add multiple counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct LocalCapacitySnapshot {
    /// Accepted delivery copies not yet terminally released.
    pub outstanding_count: usize,
    /// Configured provider-wide outstanding delivery count limit.
    pub max_total_outstanding: usize,
    /// Reserved declared weight in bytes; zero when weight accounting is disabled.
    pub outstanding_weight_bytes: usize,
    /// Declared-weight limit, or `None` when weight accounting is disabled.
    pub max_total_outstanding_weight_bytes: Option<NonZeroUsize>,
    /// Pending plus in-flight capacity for each individual subscription.
    pub queue_capacity: usize,
    /// Targets rejected because their subscription was closed.
    pub rejected_subscription_closed: u64,
    /// Targets rejected because their subscription queue was full.
    pub rejected_subscription_capacity: u64,
    /// Targets rejected because provider count capacity was full.
    pub rejected_provider_count_capacity: u64,
    /// Targets rejected because provider weight capacity was full.
    pub rejected_provider_weight_capacity: u64,
}
