// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable worker and per-topic delivery counters.

use std::collections::HashMap;

/// Counts active workers and received deliveries by topic.
#[derive(Default)]
pub(in crate::facade) struct TrackerState {
    /// Number of subscription workers that have not completed cleanup.
    pub(in crate::facade) active_workers: usize,
    /// Number of deliveries currently being processed for each topic.
    pub(in crate::facade) in_flight_by_topic: HashMap<Box<str>, usize>,
}
