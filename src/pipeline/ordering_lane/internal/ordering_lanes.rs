// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Blocking FIFO ordering lanes used by synchronous subscribers.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::OrderingLaneKey;
use super::ordering_turn::OrderingTurn;
use super::sync_lane::SyncLane;

/// Blocking synchronous lanes for ordered subscriber work.
///
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
#[cfg(test)]
#[must_use]
pub(crate) struct OrderingLanes<T> {
    /// Weak lane registry so unused ordering keys can be reclaimed.
    lanes: Mutex<HashMap<OrderingLaneKey, Weak<SyncLane<T>>>>,
    /// Generates FIFO ticket identities.
    next_ticket: AtomicU64,
}

#[cfg(test)]
impl<T> OrderingLanes<T> {
    /// Creates an empty lane collection.
    ///
    /// # Returns
    /// A collection with no live keys and ticket numbering starting at one.
    pub(crate) fn new() -> Self {
        Self {
            lanes: Mutex::new(HashMap::new()),
            next_ticket: AtomicU64::new(1),
        }
    }

    /// Enqueues an item and returns a blocking turn ticket.
    ///
    /// # Parameters
    /// - `key`: lane key that scopes ordering.
    /// - `value`: item retained until its turn is acquired.
    ///
    /// # Returns
    /// A ticket that waits until earlier lane items finish.
    pub(crate) fn enqueue(&self, key: OrderingLaneKey, value: T) -> OrderingTurn<T> {
        let lane = {
            let mut lanes = self.lanes.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            lanes.retain(|_, lane| lane.strong_count() != 0);
            lanes.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
                let lane = Arc::new(SyncLane::default());
                lanes.insert(key, Arc::downgrade(&lane));
                lane
            })
        };
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        let is_leader = {
            let mut state = lane.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let is_leader = !state.active && state.queue.is_empty();
            state.queue.push_back((ticket, Some(value)));
            is_leader
        };
        OrderingTurn {
            lane,
            ticket: Some(ticket),
            is_leader,
        }
    }
}

#[cfg(test)]
impl<T> Default for OrderingLanes<T> {
    /// Creates an empty lane collection with ticket numbering at its initial
    /// value.
    fn default() -> Self {
        Self::new()
    }
}
