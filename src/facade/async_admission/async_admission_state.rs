// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! In-flight permit count and FIFO admission waiters.

use std::collections::VecDeque;
use std::task::Waker;

/// Queue state guarded by the parent admission mutex.
#[derive(Default)]
pub(in crate::facade) struct AsyncAdmissionState {
    /// Number of active admission permits.
    pub(super) in_flight: usize,
    /// FIFO waiter IDs paired with their latest registered waker.
    pub(super) waiters: VecDeque<(u64, Waker)>,
}
