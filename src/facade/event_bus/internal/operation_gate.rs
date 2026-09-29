// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Gate for new public calls while admitted SPI operations drain.

use std::sync::Condvar;
use std::sync::Mutex;

use super::operation_gate_state::OperationGateState;
use super::operation_permit::OperationPermit;

/// Linearizes new public publish and subscribe calls against provider shutdown.
#[derive(Default)]
pub(in crate::facade) struct OperationGate {
    /// Admission state protected against shutdown races.
    state: Mutex<OperationGateState>,
    /// Wakes callers waiting for admitted operations to finish.
    changed: Condvar,
}

impl OperationGate {
    /// Admits a facade operation unless shutdown has closed admission.
    ///
    /// # Returns
    /// A permit when admission is open, otherwise None.
    #[must_use]
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

    /// Releases one admitted operation and wakes shutdown when the gate drains.
    ///
    /// # Parameters
    /// - `state`: gate state whose active operation count is decremented.
    pub(super) fn release(&self, state: &mut OperationGateState) {
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.changed.notify_all();
        }
    }

    /// Locks the gate state while recovering from internal mutex poison.
    ///
    /// # Returns
    /// A guard for the current operation admission state.
    pub(super) fn lock_state(&self) -> std::sync::MutexGuard<'_, OperationGateState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
