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

use super::diagnostic_observer::ObserverEntry;

/// A registration that remains active until this handle is dropped.
pub struct DiagnosticObserverHandle {
    entry: Arc<ObserverEntry>,
}

impl DiagnosticObserverHandle {
    pub(super) fn new(entry: Arc<ObserverEntry>) -> Self {
        Self { entry }
    }
}

impl Drop for DiagnosticObserverHandle {
    fn drop(&mut self) {
        self.entry.active.store(false, Ordering::Release);
    }
}
