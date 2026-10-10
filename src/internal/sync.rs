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

/// Provides condition-variable waiting for production builds.
#[cfg(not(loom))]
pub(crate) use std::sync::Condvar;
/// Provides mutual exclusion for production builds.
#[cfg(not(loom))]
pub(crate) use std::sync::Mutex;
/// Provides scoped access to a production mutex's protected value.
#[cfg(not(loom))]
pub(crate) use std::sync::MutexGuard;

/// Provides instrumented condition-variable waiting for Loom builds.
#[cfg(loom)]
pub(crate) use loom::sync::Condvar;
/// Provides instrumented mutual exclusion for Loom builds.
#[cfg(loom)]
pub(crate) use loom::sync::Mutex;
/// Provides scoped access to a Loom mutex's protected value.
#[cfg(loom)]
pub(crate) use loom::sync::MutexGuard;
