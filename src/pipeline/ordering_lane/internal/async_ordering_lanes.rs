// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous FIFO ordering lanes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::OrderingLaneKey;
use super::async_lane::AsyncLane;
use super::async_ordering_turn::AsyncOrderingTurn;

/// Nonblocking future-based lanes for runtime-neutral asynchronous facades.
///
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
#[must_use]
pub(crate) struct AsyncOrderingLanes<T> {
    /// Weak lane registry so inactive ordering keys can be reclaimed.
    lanes: Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>,
    /// Generates FIFO ticket identities.
    next_ticket: AtomicU64,
}
impl<T> AsyncOrderingLanes<T> {
    /// Creates an empty async lane collection.
    ///
    /// # Returns
    /// A collection with no live keys and ticket numbering starting at one.
    pub(crate) fn new() -> Self {
        Self {
            lanes: Mutex::new(HashMap::new()),
            next_ticket: AtomicU64::new(1),
        }
    }

    /// Enqueues an item and returns its nonblocking lane-turn future.
    ///
    /// # Parameters
    /// - `key`: lane identity that scopes ordering.
    /// - `value`: item retained until its lane turn is acquired.
    ///
    /// # Returns
    /// A future that resolves when earlier values release the lane.
    pub(crate) fn enqueue(&self, key: OrderingLaneKey, value: T) -> AsyncOrderingTurn<T> {
        let lane = {
            let mut lanes = self.lanes.lock().unwrap_or_else(PoisonError::into_inner);
            lanes.retain(|_, lane| lane.strong_count() != 0);
            lanes.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
                let lane = Arc::new(AsyncLane::default());
                lanes.insert(key, Arc::downgrade(&lane));
                lane
            })
        };
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        lane.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .queue
            .push_back((ticket, Some(value), None));
        AsyncOrderingTurn {
            lane,
            ticket: Some(ticket),
        }
    }
}
impl<T> Default for AsyncOrderingLanes<T> {
    /// Creates an empty lane collection with ticket numbering at its initial
    /// value.
    fn default() -> Self {
        Self::new()
    }
}
