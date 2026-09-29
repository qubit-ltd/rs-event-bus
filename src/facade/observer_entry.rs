// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared ownership for synchronous and asynchronous diagnostic observers.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::pipeline::DiagnosticObserver;

/// Shared liveness and callback storage for a diagnostic observer.
pub(super) struct ObserverEntry {
    /// Whether dispatch should continue invoking the callback.
    pub(super) active: AtomicBool,
    /// Thread-safe diagnostic callback shared by facade clones.
    pub(super) callback: Arc<DiagnosticObserver>,
}
