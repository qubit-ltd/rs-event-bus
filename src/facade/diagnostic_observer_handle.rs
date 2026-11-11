// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Handle controlling one diagnostic observer registration.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::observer_entry::ObserverEntry;

/// A registration that remains active until this handle is dropped.
///
/// Create a handle with [`EventBus::observe_diagnostics`] or
/// [`AsyncEventBus::observe_diagnostics`]. Dropping the handle prevents
/// inclusion in later observer snapshots. Snapshots that already selected the
/// callback may still invoke it and retain its captures after drop returns.
/// Dropping the handle does not wait for callbacks to finish.
///
/// [`EventBus::observe_diagnostics`]: super::EventBus::observe_diagnostics
/// [`AsyncEventBus::observe_diagnostics`]: super::AsyncEventBus::observe_diagnostics
///
/// # Examples
///
/// ```
/// use qubit_event_bus::DiagnosticObserverHandle;
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let bus = EventBus::local(LocalEventBusConfig::new()).unwrap();
/// let observer: DiagnosticObserverHandle = bus.observe_diagnostics(|_| {});
/// drop(observer); // Later snapshots no longer select this observer.
/// bus.shutdown(ShutdownMode::Immediate).unwrap();
/// ```
#[must_use = "keep this handle alive while observing diagnostics"]
pub struct DiagnosticObserverHandle {
    /// Shared registration entry deactivated when this handle is dropped.
    entry: Arc<ObserverEntry>,
}

impl DiagnosticObserverHandle {
    /// Creates a handle for a newly registered observer entry.
    ///
    /// # Parameters
    /// - `entry`: shared active observer registration.
    ///
    /// # Returns
    /// A handle that keeps the registration active while it is alive.
    #[inline]
    pub(super) fn new(entry: Arc<ObserverEntry>) -> Self {
        Self { entry }
    }
}

impl Drop for DiagnosticObserverHandle {
    /// Marks the observer inactive so subsequent dispatches skip it.
    fn drop(&mut self) {
        self.entry.active.store(false, Ordering::Release);
    }
}
