// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tracks synchronous facade execution contexts on each thread.

use std::cell::RefCell;

thread_local! {
    /// Stack of bus identities whose synchronous facade work is active here.
    static CURRENT_BUS_CONTEXTS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Temporarily marks a thread as executing synchronous work owned by a bus.
#[must_use = "the bus context guard must remain alive for the execution scope"]
pub(in crate::facade) struct BusContextGuard {
    /// Identity of the bus whose callback or worker scope is active.
    bus_identity: usize,
}

impl BusContextGuard {
    /// Installs the bus identity for deadlock checks during callbacks and
    /// worker execution.
    ///
    /// # Parameters
    /// - `bus_identity`: the address-derived identity of the owning bus.
    ///
    /// # Returns
    /// A guard that removes the identity when this execution scope ends.
    pub(in crate::facade) fn enter(bus_identity: usize) -> Self {
        CURRENT_BUS_CONTEXTS.with(|contexts| contexts.borrow_mut().push(bus_identity));
        Self { bus_identity }
    }
}

impl Drop for BusContextGuard {
    /// Restores the prior bus identity after the synchronous execution scope
    /// ends.
    fn drop(&mut self) {
        CURRENT_BUS_CONTEXTS.with(|contexts| {
            let removed = contexts.borrow_mut().pop();
            debug_assert_eq!(removed, Some(self.bus_identity));
        });
    }
}

/// Returns whether the current thread is executing synchronous work owned by
/// `bus_identity`.
///
/// # Parameters
/// - `bus_identity`: the bus identity to search for on the current thread.
///
/// # Returns
/// `true` when that bus currently owns a synchronous execution scope on this
/// thread.
#[must_use = "Use the returned query result."]
pub(in crate::facade) fn is_current_bus_context(bus_identity: usize) -> bool {
    CURRENT_BUS_CONTEXTS.with(|contexts| contexts.borrow().contains(&bus_identity))
}
