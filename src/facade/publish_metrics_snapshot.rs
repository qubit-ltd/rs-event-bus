// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Snapshot of facade publication counters.

/// A point-in-time snapshot of facade publication counters.
///
/// Each field is loaded independently, so concurrent publishes can make the
/// fields reflect slightly different instants. These counts report provider
/// admission outcomes; they do not report subscriber handler completion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PublishMetricsSnapshot {
    /// Number of public publish attempts, including calls rejected as closed.
    ///
    /// A synchronous call becomes an attempt when the method is entered. An
    /// asynchronous call becomes an attempt when its future is first polled;
    /// creating and dropping an unpolled future does not count.
    pub attempts: u64,
    /// Number of calls that returned a publication error.
    pub errors: u64,
    /// Number of calls intentionally dropped by a publisher interceptor.
    pub dropped: u64,
    /// Number of calls accepted by a provider that does not expose
    /// destinations.
    pub opaque_accepted: u64,
    /// Number of receipts that report no destinations.
    pub zero_destinations: u64,
    /// Number of accepted destinations reported by providers.
    pub accepted_destinations: u64,
    /// Number of filtered destinations reported by providers.
    pub filtered_destinations: u64,
    /// Number of rejected destinations reported by providers.
    pub rejected_destinations: u64,
}
