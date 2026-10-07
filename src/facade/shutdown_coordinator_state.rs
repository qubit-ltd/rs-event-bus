// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! State shared by callers and the synchronous shutdown coordinator.

use std::collections::HashMap;
use std::task::Waker;

use super::shutdown_result::ShutdownResult;
use crate::spi::ShutdownMode;

/// Tracks shutdown attempts, observers, and retained results for concurrent
/// callers.
pub(super) struct ShutdownCoordinatorState {
    /// Whether a shutdown worker currently owns an attempt.
    pub(super) active: bool,
    /// Monotonic generation distinguishing shutdown attempts.
    pub(super) generation: u64,
    /// Strongest shutdown mode requested for the active generation.
    pub(super) mode: ShutdownMode,
    /// Number of live observation tickets retaining each generation.
    pub(super) waiters: HashMap<u64, usize>,
    /// Monotonic identity for independent asynchronous registrations.
    pub(super) next_registration: u64,
    /// Independent asynchronous observers indexed by generation and token.
    pub(super) wakers: HashMap<u64, HashMap<u64, Waker>>,
    /// Completed results retained until every generation ticket leaves.
    pub(super) results: HashMap<u64, ShutdownResult>,
}

impl ShutdownCoordinatorState {
    /// Creates an idle coordinator state before the first shutdown request.
    ///
    /// # Returns
    /// An inactive state ready for its first shutdown generation.
    #[must_use = "Use the initialized shutdown coordinator state."]
    pub(super) fn new() -> Self {
        Self {
            active: false,
            generation: 0,
            mode: ShutdownMode::Immediate,
            waiters: HashMap::new(),
            results: HashMap::new(),
            next_registration: 0,
            wakers: HashMap::new(),
        }
    }
}
