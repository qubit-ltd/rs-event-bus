// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Safely disposes panic payloads captured at error conversion boundaries.
//!
//! A payload is dropped normally. If that destructor panics, the secondary
//! payload is forgotten to avoid recursively invoking another destructor.
//! This may leak resources owned by the secondary payload. It cannot recover
//! from aborting panic hooks, `panic=abort`, or a second panic during
//! unwinding.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

/// Drops a captured payload while containing one destructor panic.
pub(crate) fn dispose_panic_payload(payload: Box<dyn Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(secondary);
    }
}
