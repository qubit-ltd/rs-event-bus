// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal sync facade state owner.

use crate::ShutdownReport;

pub(in crate::facade) struct ShutdownState {
    pub(in crate::facade) report: Option<ShutdownReport>,
}
