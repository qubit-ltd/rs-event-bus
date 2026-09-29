// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus diagnostics operations.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::AsyncEventBus;
use crate::Diagnostic;
use crate::DiagnosticObserverHandle;
use crate::facade::observer_entry::ObserverEntry;
use crate::pipeline::DiagnosticObserver;

impl AsyncEventBus {
    /// Registers a synchronous diagnostic observer until the returned handle is
    /// dropped.
    ///
    /// # Type Parameters
    /// - `F`: Thread-safe callback type.
    ///
    /// # Parameters
    /// - `observer`: Callback invoked for emitted diagnostics.
    ///
    /// # Returns
    /// A handle that unregisters the observer when dropped.
    #[must_use]
    pub fn observe_diagnostics<F>(&self, observer: F) -> DiagnosticObserverHandle
    where
        F: Fn(&Diagnostic) + Send + Sync + 'static,
    {
        let entry = Arc::new(ObserverEntry {
            active: AtomicBool::new(true),
            callback: Arc::new(observer),
        });
        let mut entries = self
            .inner
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|entry| entry.strong_count() > 0);
        entries.push(Arc::downgrade(&entry));
        DiagnosticObserverHandle::new(entry)
    }

    /// Returns the live diagnostic callbacks registered with this bus.
    ///
    /// # Returns
    /// Strong references to observers that are still registered.
    #[must_use]
    pub(super) fn observer_snapshot(&self) -> Vec<Arc<DiagnosticObserver>> {
        self.inner.observer_snapshot()
    }
}
