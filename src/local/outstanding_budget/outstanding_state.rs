// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Integer counters owned by the outstanding admission budget.

/// Integer reservations updated together under the provider admission lock.
#[derive(Default)]
pub(super) struct OutstandingState {
    /// Number of queued or unsettled delivery copies.
    pub(super) count_used: usize,
    /// Sum of their declared weights, or zero when weight accounting is
    /// disabled.
    pub(super) weight_used: usize,
    /// Cumulative target attempts rejected because their subscription closed.
    /// Fanout and retries can count the same logical publication more than
    /// once; increments saturate at `u64::MAX`.
    pub(super) rejected_subscription_closed: u64,
    /// Cumulative target attempts rejected by a full subscription queue.
    /// Fanout and retries can count the same logical publication more than
    /// once; increments saturate at `u64::MAX`.
    pub(super) rejected_subscription_capacity: u64,
    /// Cumulative target attempts rejected by the provider-wide count limit.
    /// Fanout and retries can count the same logical publication more than
    /// once; increments saturate at `u64::MAX`.
    pub(super) rejected_provider_count_capacity: u64,
    /// Cumulative target attempts rejected by the provider-wide weight limit.
    /// Fanout and retries can count the same logical publication more than
    /// once; increments saturate at `u64::MAX`.
    pub(super) rejected_provider_weight_capacity: u64,
}
