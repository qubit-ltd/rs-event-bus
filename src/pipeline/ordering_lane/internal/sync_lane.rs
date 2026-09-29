// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable synchronization state for one blocking ordering lane.

use std::collections::VecDeque;
use std::sync::Condvar;
use std::sync::Mutex;

use super::sync_lane_state::SyncLaneState;

/// Synchronization state and wakeups for one blocking ordering lane.
///
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
#[cfg(test)]
pub(super) struct SyncLane<T> {
    /// Active ticket and queued values protected by the lane mutex.
    pub(super) state: Mutex<SyncLaneState<T>>,
    /// Wakes tickets after the active value leaves the lane.
    pub(super) changed: Condvar,
}
#[cfg(test)]
impl<T> Default for SyncLane<T> {
    /// Creates an idle lane with an empty FIFO and a fresh wake condition.
    fn default() -> Self {
        Self {
            state: Mutex::new(SyncLaneState {
                active: false,
                queue: VecDeque::new(),
            }),
            changed: Condvar::new(),
        }
    }
}
