// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Completion flag observed by publisher close callers.

pub(in crate::notification) struct WorkerState {
    pub(in crate::notification) finished: bool,
}
