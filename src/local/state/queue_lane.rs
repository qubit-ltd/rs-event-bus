// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! FIFO event storage for one local ordering lane.

use std::collections::VecDeque;

use super::event::LocalEvent;

/// Pending events and scheduling generations for one ordering lane.
///
/// The event deque preserves per-key FIFO order. `version` invalidates ready
/// and delayed entries created for an earlier head, while `delayed_version`
/// identifies the generation currently represented in the delayed heap.
#[derive(Default)]
pub(in crate::local) struct QueueLane {
    /// Pending events belonging to this ordering key.
    pub(in crate::local) events: VecDeque<LocalEvent>,
    /// Generation of the current head, used to ignore stale scheduling entries.
    pub(in crate::local) version: u64,
    /// Generation currently represented by a live delayed heap entry, if any.
    pub(in crate::local) delayed_version: Option<u64>,
}
