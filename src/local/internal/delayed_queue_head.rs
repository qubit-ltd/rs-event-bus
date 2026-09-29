// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Focused owner for local provider state.

use std::cmp::Ordering;
use std::time::Instant;

use crate::spi::OrderingKey;

/// A delayed lane head ordered by its deadline and insertion sequence.
pub(in crate::local) struct DelayedQueueHead {
    /// Earliest instant at which this lane head may be received.
    pub(in crate::local) deadline: Instant,
    /// Stable tie breaker for equal deadlines.
    pub(in crate::local) sequence: u64,
    /// Lane whose current head is delayed.
    pub(in crate::local) key: Option<OrderingKey>,
    /// Lane generation used to discard stale heap entries.
    pub(in crate::local) version: u64,
}

impl PartialEq for DelayedQueueHead {
    /// Compares heap identity by deadline and insertion sequence.
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.sequence == other.sequence
    }
}

impl Eq for DelayedQueueHead {}

impl PartialOrd for DelayedQueueHead {
    /// Orders delayed heads by deadline, then by insertion sequence.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedQueueHead {
    /// Orders delayed heads by deadline, then by insertion sequence.
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}
