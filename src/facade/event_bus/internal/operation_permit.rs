// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal sync facade state owner.

use crate::facade::event_bus::OperationGate;

/// Releases one operation admission when its complete SPI call sequence
/// returns.
pub(in crate::facade) struct OperationPermit<'a> {
    pub(in crate::facade) gate: &'a OperationGate,
}

impl Drop for OperationPermit<'_> {
    /// Releases one in-progress operation and wakes shutdown when admission
    /// drains.
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.gate.changed.notify_all();
        }
    }
}
