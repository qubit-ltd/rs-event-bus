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

/// FIFO queue and generation for one ordering lane.
#[derive(Default)]
pub(in crate::local) struct QueueLane {
    /// Pending events belonging to this ordering key.
    pub(in crate::local) events: VecDeque<LocalEvent>,
    /// Generation of the current head, invalidating older cached entries.
    pub(in crate::local) version: u64,
    /// Generation currently represented by a live delayed heap entry.
    pub(in crate::local) delayed_version: Option<u64>,
}
