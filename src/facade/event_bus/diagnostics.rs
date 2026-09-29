// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus diagnostics operations.

use std::sync::Arc;

use crate::Diagnostic;
use crate::DiagnosticObserverHandle;
use crate::EventBus;
use crate::facade::observer_entry::ObserverEntry;

impl EventBus {
    /// Registers an observer until the returned handle is dropped.
    ///
    /// Observers run synchronously in registration order and outside facade
    /// locks. Panics are contained and do not recursively emit diagnostics.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe callback type.
    ///
    /// # Parameters
    /// - `observer`: callback invoked for each emitted diagnostic.
    ///
    /// # Returns
    /// A handle that unregisters the observer when dropped.
    #[must_use]
    pub fn observe_diagnostics<F>(&self, observer: F) -> DiagnosticObserverHandle
    where
        F: Fn(&Diagnostic) + Send + Sync + 'static,
    {
        let entry = Arc::new(ObserverEntry {
            active: std::sync::atomic::AtomicBool::new(true),
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
}
