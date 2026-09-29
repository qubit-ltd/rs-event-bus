// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal result shared by all notification close callers.

/// Final outcome after worker processing and owned-resource cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::notification) enum WorkerExit {
    /// All accepted notifications and worker resources were drained.
    Drained,
    /// Processing or owned-resource cleanup panicked.
    Panicked,
}
