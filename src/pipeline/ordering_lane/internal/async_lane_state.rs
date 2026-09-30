// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ticket queue and wakers retained by one asynchronous lane.

use std::collections::VecDeque;
use std::task::Waker;

/// Queued tickets retained by one asynchronous lane.
///
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
pub(super) struct AsyncLaneState<T> {
    /// Whether one ticket currently owns the lane.
    pub(super) active: bool,
    /// FIFO tickets paired with optional values and their latest wakers.
    pub(super) queue: VecDeque<(u64, Option<T>, Option<Waker>)>,
}

/// Clones the current head ticket's waker, if one has been registered.
///
/// # Type Parameters
/// - `T`: payload value paired with each queued ticket.
///
/// # Parameters
/// - `state`: lane queue whose head waiter may need waking.
///
/// # Returns
/// The front ticket's waker, or `None` when the queue is empty or unpolled.
#[must_use = "wake the returned waker to resume the front ticket"]
#[inline]
pub(super) fn front_waker<T>(state: &AsyncLaneState<T>) -> Option<Waker> {
    state.queue.front().and_then(|(_, _, waker)| waker.clone())
}
