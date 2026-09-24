// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Blocking lifecycle outcomes and per-bus execution-context deadlock tracking.

use std::cell::RefCell;
use std::time::Duration;

/// Result of waiting for topic work reported by a provider or tracked by a
/// facade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WaitOutcome {
    /// The selected provider or facade reports no outstanding work for the
    /// topic.
    Idle,
    /// The deadline elapsed while outstanding work remained.
    TimedOut,
}

/// Internal state of one facade instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleState {
    /// The facade accepts new publish and subscribe calls.
    Running,
    /// New work is rejected while subscriptions and provider are closing.
    Closing,
    /// Subscription workers and provider have completed shutdown.
    Closed,
}

thread_local! {
    static CURRENT_BUS_CONTEXTS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Temporarily marks a thread as executing synchronous work owned by a bus.
pub(crate) struct BusContextGuard {
    bus_identity: usize,
}

impl BusContextGuard {
    /// Installs the bus identity for deadlock checks during callbacks and
    /// worker execution.
    pub(crate) fn enter(bus_identity: usize) -> Self {
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
pub(crate) fn is_current_bus_context(bus_identity: usize) -> bool {
    CURRENT_BUS_CONTEXTS.with(|contexts| contexts.borrow().contains(&bus_identity))
}

/// Returns a finite polling interval used to observe cancellation of a blocking
/// SPI receive.
pub(crate) fn receive_poll_interval() -> Duration {
    Duration::from_millis(50)
}
