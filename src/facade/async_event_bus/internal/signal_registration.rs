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
use crate::facade::async_subscription::with_bus_scope;

/// Waiter registration that unregisters itself when its owner is dropped.
#[must_use = "keep the registration alive while the waiter is registered"]
pub(in crate::facade) struct SignalRegistration<'a> {
    /// Signal whose waiter list contains this registration.
    signal: &'a AsyncSignal,
    /// Identifier used to remove this waiter on drop.
    id: u64,
    /// Optional bus whose callback context must cover waker clone/drop.
    bus_key: Option<usize>,
}

impl SignalRegistration<'_> {
    /// Registers a waiter for the supplied signal.
    ///
    /// # Parameters
    /// - signal: signal to observe until this registration is dropped.
    ///
    /// # Returns
    /// A registration that unregisters itself on drop.
    ///
    /// The identifier is allocated when this value is constructed; no waker
    /// is stored until [`register`](Self::register) is called.
    #[inline]
    pub(in crate::facade) fn new(signal: &AsyncSignal) -> SignalRegistration<'_> {
        SignalRegistration {
            signal,
            id: signal.next_waiter_id(),
            bus_key: None,
        }
    }

    /// Registers a waiter whose waker callbacks belong to one bus poll.
    ///
    /// # Parameters
    /// - `signal`: signal to observe until this registration is dropped.
    /// - `bus_key`: identity of the bus whose callback context scopes waker
    ///   operations.
    ///
    /// # Returns
    /// A registration that allocates its identifier immediately and unregisters
    /// itself on drop. It does not store a waker until [`register`](Self::register)
    /// is called; registration clones the supplied waker and replaces any
    /// previous waker for this identifier. The bus scope covers cloning,
    /// replaced-waker destruction, and unregister cleanup, including
    /// destruction of the stored waker.
    #[inline]
    pub(in crate::facade) fn new_with_bus_scope(signal: &AsyncSignal, bus_key: usize) -> SignalRegistration<'_> {
        SignalRegistration {
            signal,
            id: signal.next_waiter_id(),
            bus_key: Some(bus_key),
        }
    }

    /// Updates this waiter's registered waker.
    ///
    /// # Parameters
    /// - waker: task waker to use when the signal changes.
    #[inline]
    pub(in crate::facade) fn register(&self, waker: &Waker) {
        if let Some(bus_key) = self.bus_key {
            with_bus_scope(bus_key, || self.signal.register_waiter(self.id, waker));
        } else {
            self.signal.register_waiter(self.id, waker);
        }
    }
}

impl Drop for SignalRegistration<'_> {
    /// Removes this waiter from the signal.
    fn drop(&mut self) {
        if let Some(bus_key) = self.bus_key {
            with_bus_scope(bus_key, || self.signal.unregister(self.id));
        } else {
            self.signal.unregister(self.id);
        }
    }
}
