// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ticket that waits for a synchronous ordering lane.

use std::sync::Arc;

use super::ordering_guard::OrderingGuard;
use super::sync_lane::SyncLane;

/// A synchronous lane ticket that waits until earlier work has completed.
///
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
#[cfg(test)]
#[must_use = "dropping the ticket cancels this ordering turn"]
pub(crate) struct OrderingTurn<T> {
    /// Lane containing this ticket and its value.
    pub(super) lane: Arc<SyncLane<T>>,
    /// Queue identity removed if this waiting future is dropped.
    pub(super) ticket: Option<u64>,
    /// Whether the lane was empty when this ticket was enqueued.
    pub(super) is_leader: bool,
}
#[cfg(test)]
impl<T> OrderingTurn<T> {
    /// Returns whether no earlier work was present at enqueue time.
    ///
    /// # Returns
    /// `true` when this ticket was the first queued value in its lane.
    #[must_use]
    #[inline]
    pub(crate) fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// Waits until this ticket reaches the head and acquires its lane guard.
    ///
    /// # Returns
    /// The ordered value guard, or `None` if the queued value was canceled.
    pub(crate) fn take(mut self) -> Option<OrderingGuard<T>> {
        let ticket = self.ticket?;
        let lane = self.lane.clone();
        let mut state = lane.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if !state.active && state.queue.front().is_some_and(|(queued, _)| *queued == ticket) {
                let (_, value) = state.queue.pop_front()?;
                state.active = true;
                self.ticket = None;
                return value.map(|value| OrderingGuard {
                    lane: lane.clone(),
                    value: Some(value),
                });
            }
            state = self
                .lane
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}
#[cfg(test)]
impl<T> Drop for OrderingTurn<T> {
    /// Removes this ticket and wakes the next lane waiter.
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let mut state = self
            .lane
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = state.queue.iter().position(|(queued, _)| *queued == ticket) {
            state.queue.remove(index);
        }
        self.lane.changed.notify_all();
    }
}
