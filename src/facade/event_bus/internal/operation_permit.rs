// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Drop-based release of one operation admission.

use super::operation_gate::OperationGate;

/// Releases one operation admission after its provider call sequence returns.
pub(in crate::facade) struct OperationPermit<'a> {
    /// Gate whose active operation count this permit owns.
    pub(in crate::facade::event_bus) gate: &'a OperationGate,
}

impl Drop for OperationPermit<'_> {
    /// Releases one admitted operation and wakes shutdown when admission
    /// drains.
    fn drop(&mut self) {
        let mut state = self.gate.lock_state();
        self.gate.release(&mut state);
    }
}
