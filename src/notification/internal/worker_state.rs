// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal worker state observed by publisher close callers.

use super::worker_exit::WorkerExit;

/// Completion state protected by the notification publisher's state mutex.
pub(in crate::notification) struct WorkerState {
    /// Immutable terminal outcome, or `None` while cleanup remains in progress.
    pub(in crate::notification) exit: Option<WorkerExit>,
}
