// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal sync facade state owner.

/// Counts calls that entered the facade while it was still accepting
/// operations.
#[derive(Default)]
pub(in crate::facade) struct OperationGateState {
    pub(in crate::facade) closing: bool,
    pub(in crate::facade) active: usize,
}
