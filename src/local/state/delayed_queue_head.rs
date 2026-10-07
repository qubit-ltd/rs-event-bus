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

/// Optional ordering key used to identify the lane represented by a delayed
/// head.
///
/// `None` represents the unkeyed lane; `Some` identifies a keyed lane whose
/// events share the same ordering constraint.
pub(in crate::local) type QueueKey = Option<OrderingKey>;

/// Heap metadata for a lane head that cannot yet be received.
///
/// Ordering uses `deadline` and then `sequence`, so equal deadlines are
/// received in insertion order. `key` and `version` identify the lane entry but
/// do not participate in heap ordering or equality.
pub(in crate::local) struct DelayedQueueHead {
    /// Earliest instant at which the lane head is eligible to be received.
    pub(in crate::local) deadline: Instant,
    /// Monotonically assigned insertion sequence used to break equal deadlines.
    pub(in crate::local) sequence: u64,
    /// Optional ordering key identifying the lane whose head is delayed.
    pub(in crate::local) key: QueueKey,
    /// Generation captured when this heap entry was created, for stale-entry
    /// detection.
    pub(in crate::local) version: u64,
}

impl PartialEq for DelayedQueueHead {
    /// Compares heap identity by deadline and insertion sequence.
    ///
    /// # Parameters
    /// `other` is the delayed head to compare with this one.
    ///
    /// # Returns
    /// `true` when both delayed heads have the same deadline and insertion
    /// sequence. Lane key and generation do not affect equality.
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.sequence == other.sequence
    }
}

impl Eq for DelayedQueueHead {}

impl PartialOrd for DelayedQueueHead {
    /// Orders delayed heads by deadline, then by insertion sequence.
    ///
    /// # Parameters
    /// `other` is the delayed head to compare with this one.
    ///
    /// # Returns
    /// The ordering wrapped in `Some`; delayed heads always have a total order.
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedQueueHead {
    /// Orders delayed heads by deadline, then by insertion sequence.
    ///
    /// # Parameters
    /// `other` is the delayed head to compare with this one.
    ///
    /// # Returns
    /// The ordering by deadline, using sequence to break ties.
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}
