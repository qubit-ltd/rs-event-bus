// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Delivery lifecycle gauges and cumulative counters.

use std::time::Duration;

/// A delivery lifecycle snapshot. Concurrent reads need not be transactional.
/// Cumulative counters and duration totals saturate at `u64::MAX`; ages use
/// checked monotonic time. Active gauges come from currently owned scheduler
/// metadata, and `lane_waiting` is a subset of `queued`.
///
/// Bus counters survive subscription closure without retaining historical
/// subscriptions. A closed subscription handle retains its own final counters.
/// Snapshot reads do not call the provider or retain payloads and settlement
/// tokens.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::facade::DeliveryMetricsSnapshot;
///
/// let snapshot = DeliveryMetricsSnapshot { completed: 3, ..Default::default() };
/// assert_eq!(snapshot.completed, 3);
/// assert_eq!(snapshot.oldest_owned_age, None);
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[must_use]
pub struct DeliveryMetricsSnapshot {
    /// Reserved receive slots without a message yet.
    pub reserved_receives: u64,
    /// Owned messages waiting for handler execution.
    pub queued: u64,
    /// Delivery pipelines currently holding a handler slot, including
    /// middleware.
    pub running_handlers: u64,
    /// Owned messages awaiting terminal provider settlement.
    pub settling: u64,
    /// Queued messages waiting for an ordering lane; a subset of queued.
    pub lane_waiting: u64,
    /// Actual provider settlement calls, including first attempts.
    pub settlement_attempts: u64,
    /// Actual provider settlement calls after the first attempt for each
    /// delivery. A deadline or cancellation that prevents SPI entry does
    /// not add a retry.
    pub settlement_retries: u64,
    /// Settlements that stopped permanently.
    pub settlement_terminal_failures: u64,
    /// Successfully completed delivery lifecycles, including settled
    /// rejection/retry dispositions; unresolved cleanup and permanent
    /// settlement failures are excluded.
    pub completed: u64,
    /// Known ephemeral deliveries abandoned on cleanup.
    pub abandoned_ephemeral: u64,
    /// Actual user handler invocation samples, including failed, panicked, and
    /// local retry invocations; decoding, filters, and middleware
    /// short-circuits add none.
    pub handler_duration_count: u64,
    /// Saturating sum of actual user handler invocation durations in
    /// nanoseconds.
    pub handler_duration_total_nanos: u64,
    /// Longest handler duration in nanoseconds.
    pub handler_duration_max_nanos: u64,
    /// Settlement cycles measured from the first SPI attempt to success or
    /// terminal stop, including retry waits; a cycle contributes one
    /// sample.
    pub settlement_duration_count: u64,
    /// Saturating sum of settlement durations in nanoseconds.
    pub settlement_duration_total_nanos: u64,
    /// Longest settlement duration in nanoseconds.
    pub settlement_duration_max_nanos: u64,
    /// Age of the oldest owner-marked receive reservation or delivery. `None`
    /// means no owned entry has a timestamp, or checked clock validation
    /// failed and was diagnosed. Unclaimed reservations contribute to
    /// gauges but have no age.
    pub oldest_owned_age: Option<Duration>,
}
