// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable counters protected by the asynchronous tracker lock.

use std::collections::HashMap;

/// Counts active operations and received deliveries while the tracker lock is
/// held.
#[derive(Default)]
pub(super) struct TrackerState {
    /// Number of active subscription runners.
    pub(super) active_runners: usize,
    /// Number of admitted publish operations.
    pub(super) active_publishes: usize,
    /// Number of admitted subscribe operations.
    pub(super) active_subscribes: usize,
    /// Number of active close operations.
    pub(super) active_closes: usize,
    /// Received deliveries grouped by topic name.
    pub(super) in_flight: HashMap<Box<str>, usize>,
}
