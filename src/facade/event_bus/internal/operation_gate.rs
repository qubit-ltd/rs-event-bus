// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Gate for new public calls while admitted SPI operations drain.

use std::sync::PoisonError;

use super::operation_gate_state::OperationGateState;
use super::operation_permit::OperationPermit;
use crate::internal::sync::Condvar;
use crate::internal::sync::Mutex;
use crate::internal::sync::MutexGuard;

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
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closing {
            return None;
        }
        state.active += 1;
        Some(OperationPermit { gate: self })
    }

    /// Closes operation admission without waiting for existing calls.
    pub(in crate::facade) fn close_admission(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.closing = true;
        self.changed.notify_all();
    }

    /// Waits for every previously admitted SPI call to finish.
    pub(in crate::facade) fn wait_for_idle(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.active != 0 {
            state = self.changed.wait(state).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Releases one admitted operation and wakes shutdown when the gate drains.
    ///
    /// # Parameters
    /// - `state`: gate state whose active operation count is decremented.
    #[inline]
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
    pub(super) fn lock_state(&self) -> MutexGuard<'_, OperationGateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(all(test, loom))]
mod tests {
    use loom::model::Builder;
    use loom::sync::Arc;
    use loom::sync::mpsc;
    use loom::thread;

    use super::OperationGate;

    /// Exhausts schedules with three threads and at most two preemptions.
    fn check_model(model: impl Fn() + Send + Sync + 'static) {
        let mut builder = Builder::new();
        builder.max_threads = 3;
        builder.preemption_bound = Some(2);
        builder.max_branches = 1_000;
        builder.check(model);
    }

    /// Checks admission and close share the real gate's linearization lock.
    #[test]
    fn test_loom_production_gate_close_races_admission() {
        check_model(|| {
            let gate = Arc::new(OperationGate::default());
            let admitted_gate = Arc::clone(&gate);
            let admitted = thread::spawn(move || {
                if let Some(permit) = admitted_gate.enter() {
                    assert_eq!(admitted_gate.lock_state().active, 1);
                    thread::yield_now();
                    drop(permit);
                }
            });
            let closing_gate = Arc::clone(&gate);
            let closing = thread::spawn(move || {
                closing_gate.close_admission();
                assert!(closing_gate.enter().is_none());
                closing_gate.wait_for_idle();
                assert_eq!(closing_gate.lock_state().active, 0);
            });
            admitted.join().expect("admission thread completes");
            closing.join().expect("shutdown waiter completes");
            let state = gate.lock_state();
            assert!(state.closing);
            assert_eq!(state.active, 0);
        });
    }

    /// Checks each real permit Drop releases once and the last wakes idle.
    #[test]
    fn test_loom_production_permit_drop_wakes_idle() {
        check_model(|| {
            let gate = Arc::new(OperationGate::default());
            let (ready_tx, ready_rx) = mpsc::channel();
            let owned_gate = Arc::clone(&gate);
            let owner = thread::spawn(move || {
                let first = owned_gate.enter().expect("first admission is open");
                let second = owned_gate.enter().expect("second admission is open");
                assert_eq!(owned_gate.lock_state().active, 2);
                ready_tx.send(()).expect("waiter receives admission signal");
                thread::yield_now();
                drop(first);
                assert_eq!(owned_gate.lock_state().active, 1);
                thread::yield_now();
                drop(second);
                assert_eq!(owned_gate.lock_state().active, 0);
            });
            ready_rx.recv().expect("owner admits before shutdown");
            gate.close_admission();
            gate.wait_for_idle();
            assert!(gate.enter().is_none());
            owner.join().expect("permit owner completes");
            assert_eq!(gate.lock_state().active, 0);
        });
    }
}
