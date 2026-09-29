// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable state and synchronization for one asynchronous ordering lane.

use std::collections::VecDeque;
use std::sync::Mutex;

use super::async_lane_state::AsyncLaneState;

/// One asynchronous FIFO lane and its waiter state.
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
pub(super) struct AsyncLane<T> {
    /// Active turn and queued values with their registered wakers.
    pub(super) state: Mutex<AsyncLaneState<T>>,
}
impl<T> Default for AsyncLane<T> {
    /// Creates an idle lane with an empty FIFO and no registered waiters.
    fn default() -> Self {
        Self {
            state: Mutex::new(AsyncLaneState {
                active: false,
                queue: VecDeque::new(),
            }),
        }
    }
}
