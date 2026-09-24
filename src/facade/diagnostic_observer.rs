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

pub(super) struct ObserverEntry {
    pub(super) active: AtomicBool,
    pub(super) callback: Arc<DiagnosticObserver>,
}
