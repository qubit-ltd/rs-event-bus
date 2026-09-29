// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Future that acquires one ticket in an asynchronous ordering lane.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use super::AsyncOrderingGuard;
use super::async_lane::AsyncLane;
use super::async_lane_state::front_waker;

/// A future that resolves when its async lane turn becomes available.
///
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
#[must_use = "the ordering turn must be polled or dropped to cancel its ticket"]
pub(crate) struct AsyncOrderingTurn<T> {
    /// Lane containing this ticket and its value.
    pub(super) lane: Arc<AsyncLane<T>>,
    /// Queue identity removed if this future is dropped.
    pub(super) ticket: Option<u64>,
}
impl<T> Future for AsyncOrderingTurn<T> {
    /// Guard for the lane turn, or `None` after this ticket was consumed.
    type Output = Option<AsyncOrderingGuard<T>>;

    /// Acquires the lane when this ticket reaches its queue head.
    ///
    /// # Parameters
    /// - `self`: pinned lane ticket being polled.
    /// - `context`: task context whose waker is registered while waiting.
    ///
    /// # Returns
    /// `Ready(Some(guard))` when the ticket acquires the lane, `Ready(None)` if
    /// the ticket was already consumed, or `Pending` while an earlier turn is
    /// active.
    ///
    /// # Panics
    /// Panics if the lane queue loses its head between checking and removing
    /// the matching ticket, which violates the lane mutex invariant.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(ticket) = self.ticket else {
            return Poll::Ready(None);
        };
        let lane = self.lane.clone();
        let mut state = lane.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.active && state.queue.front().is_some_and(|(queued, _, _)| *queued == ticket) {
            let (_, value, _) = state.queue.pop_front().expect("front ticket was observed");
            state.active = true;
            self.ticket = None;
            return Poll::Ready(value.map(|value| AsyncOrderingGuard {
                lane: lane.clone(),
                _value: Some(value),
            }));
        }
        let Some(waiter) = state
            .queue
            .iter_mut()
            .find(|(queued, _, _)| *queued == ticket)
            .map(|(_, _, waiter)| waiter)
        else {
            return Poll::Pending;
        };
        if waiter.as_ref().is_none_or(|old| !old.will_wake(context.waker())) {
            *waiter = Some(context.waker().clone());
        }
        Poll::Pending
    }
}
impl<T> Drop for AsyncOrderingTurn<T> {
    /// Removes this ticket and wakes the next lane waiter.
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let waker = {
            let mut state = self
                .lane
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(index) = state.queue.iter().position(|(queued, _, _)| *queued == ticket) {
                state.queue.remove(index);
            }
            front_waker(&state)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
