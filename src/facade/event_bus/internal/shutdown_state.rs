// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cached report shared by concurrent synchronous shutdown callers.

use crate::facade::ShutdownReport;

/// State protected by the facade shutdown report mutex.
pub(in crate::facade) struct ShutdownState {
    /// Report from the completed provider shutdown, if available.
    pub(in crate::facade) report: Option<ShutdownReport>,
}
