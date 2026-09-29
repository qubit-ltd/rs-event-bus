// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal sync facade state owner.

use crate::facade::event_bus::Condvar;
use crate::facade::event_bus::Mutex;
use crate::facade::event_bus::internal::OperationGateState;
use crate::facade::event_bus::internal::OperationPermit;

/// Linearizes new public publish/subscribe calls against provider shutdown.

#[derive(Default)]
pub(in crate::facade) struct OperationGate {
    pub(in crate::facade) state: Mutex<OperationGateState>,
    pub(in crate::facade) changed: Condvar,
}

impl OperationGate {
    /// Admits a facade operation unless shutdown has closed admission.
    pub(in crate::facade) fn enter(&self) -> Option<OperationPermit<'_>> {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closing {
            return None;
        }
        state.active += 1;
        Some(OperationPermit { gate: self })
    }

    /// Closes operation admission without waiting for existing calls.
    pub(in crate::facade) fn close_admission(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closing = true;
        self.changed.notify_all();
    }

    /// Waits for every previously admitted SPI call to finish.
    pub(in crate::facade) fn wait_for_idle(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.active != 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}
