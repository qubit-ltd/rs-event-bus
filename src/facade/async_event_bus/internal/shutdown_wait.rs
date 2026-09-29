// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcomes of waiting for provider shutdown.

/// Result of waiting for provider shutdown, a timeout, or mode escalation.
///
/// # Type Parameters
/// - `T`: output produced by the provider shutdown future.
pub(in crate::facade) enum ShutdownWait<T> {
    /// The provider future completed with this output.
    Complete(T),
    /// The caller's deadline elapsed.
    TimedOut,
    /// A caller requested immediate shutdown.
    ImmediateRequested,
}
