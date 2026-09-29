// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Selects instrumented synchronization for the production concurrency models.
//!
//! Only isolated admission primitives use these aliases. Runtime threads,
//! callbacks, queues, and asynchronous waiting retain their normal primitives.

#[cfg(not(loom))]
pub(crate) use std::sync::Condvar;
#[cfg(not(loom))]
pub(crate) use std::sync::Mutex;
#[cfg(not(loom))]
pub(crate) use std::sync::MutexGuard;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicUsize;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::Ordering;

#[cfg(loom)]
pub(crate) use loom::sync::Condvar;
#[cfg(loom)]
pub(crate) use loom::sync::Mutex;
#[cfg(loom)]
pub(crate) use loom::sync::MutexGuard;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicUsize;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::Ordering;
