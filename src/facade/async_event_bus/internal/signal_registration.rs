// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Registers one task waker and removes it when the waiting future is dropped.

use super::AsyncSignal;

/// Waiter registration that unregisters itself when its owner is dropped.
pub(in crate::facade) struct SignalRegistration<'a> {
    /// Signal whose waiter list contains this registration.
    signal: &'a AsyncSignal,
    /// Identifier used to remove this waiter on drop.
    id: u64,
}

impl SignalRegistration<'_> {
    /// Registers a waiter for the supplied signal.
    ///
    /// # Parameters
    /// - signal: signal to observe until this registration is dropped.
    ///
    /// # Returns
    /// A registration that unregisters itself on drop.
    pub(in crate::facade) fn new(signal: &AsyncSignal) -> SignalRegistration<'_> {
        SignalRegistration {
            signal,
            id: signal.next_waiter_id(),
        }
    }

    /// Updates this waiter's registered waker.
    ///
    /// # Parameters
    /// - waker: task waker to use when the signal changes.
    pub(in crate::facade) fn register(&self, waker: &std::task::Waker) {
        self.signal.register_waiter(self.id, waker);
    }
}

impl Drop for SignalRegistration<'_> {
    /// Removes this waiter from the signal.
    fn drop(&mut self) {
        self.signal.unregister(self.id);
    }
}
