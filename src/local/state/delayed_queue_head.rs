// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deadline ordering metadata for delayed local queue heads.

use std::cmp::Ordering;
use std::time::Instant;

use crate::spi::OrderingKey;

/// Key that groups events subject to the same per-key ordering constraint.
pub(in crate::local) type QueueKey = Option<OrderingKey>;

/// A delayed lane head ordered by its deadline and insertion sequence.
pub(in crate::local) struct DelayedQueueHead {
    /// Earliest instant at which this lane head may be received.
    pub(in crate::local) deadline: Instant,
    /// Stable tie breaker for equal deadlines.
    pub(in crate::local) sequence: u64,
    /// Lane whose current head is delayed.
    pub(in crate::local) key: QueueKey,
    /// Lane generation used to discard stale heap entries.
    pub(in crate::local) version: u64,
}

impl PartialEq for DelayedQueueHead {
    /// Compares heap identity by deadline and insertion sequence.
    ///
    /// # Returns
    /// Whether both delayed heads have the same deadline and sequence.
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.sequence == other.sequence
    }
}

impl Eq for DelayedQueueHead {}

impl PartialOrd for DelayedQueueHead {
    /// Orders delayed heads by deadline, then by insertion sequence.
    ///
    /// # Returns
    /// The ordering wrapped in `Some`; delayed heads always have a total order.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedQueueHead {
    /// Orders delayed heads by deadline, then by insertion sequence.
    ///
    /// # Returns
    /// The ordering by deadline, using sequence to break ties.
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}
