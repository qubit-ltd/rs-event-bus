// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Registers one task waker and removes it when the waiting future is dropped.

use std::task::Waker;

use super::AsyncSignal;

/// Waiter registration that unregisters itself when its owner is dropped.
#[must_use = "Keep the registration alive while waiting."]
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
    #[inline]
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
    #[inline]
    pub(in crate::facade) fn register(&self, waker: &Waker) {
        self.signal.register_waiter(self.id, waker);
    }
}

impl Drop for SignalRegistration<'_> {
    /// Removes this waiter from the signal.
    fn drop(&mut self) {
        self.signal.unregister(self.id);
    }
}
