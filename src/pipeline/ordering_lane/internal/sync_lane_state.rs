// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ticket queue retained by one blocking ordering lane.

use std::collections::VecDeque;

/// Queue state protected by one blocking ordering lane's mutex.
///
/// # Type Parameters
/// - `T`: ordered value retained by the lane.
#[cfg(test)]
pub(super) struct SyncLaneState<T> {
    /// Whether a ticket currently owns the lane.
    pub(super) active: bool,
    /// FIFO tickets paired with values that may be canceled on drop.
    pub(super) queue: VecDeque<(u64, Option<T>)>,
}
