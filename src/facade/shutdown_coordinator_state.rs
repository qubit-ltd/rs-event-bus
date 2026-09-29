// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! State shared by callers and the synchronous shutdown coordinator.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::SpiError;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

/// One shutdown attempt and its result shared by concurrent callers.
pub(super) struct ShutdownCoordinatorState {
    /// Whether a shutdown worker currently owns an attempt.
    pub(super) active: bool,
    /// Monotonic generation distinguishing shutdown attempts.
    pub(super) generation: u64,
    /// Strongest shutdown mode requested for the active generation.
    pub(super) mode: ShutdownMode,
    /// Number of callers currently waiting for each generation.
    pub(super) waiters: HashMap<u64, usize>,
    /// Completed results retained until every generation waiter leaves.
    pub(super) results: HashMap<u64, Result<ShutdownOutcome, Arc<SpiError>>>,
}

impl ShutdownCoordinatorState {
    /// Creates an idle coordinator state before the first shutdown request.
    ///
    /// # Returns
    /// An inactive state ready for its first shutdown generation.
    pub(super) fn new() -> Self {
        Self {
            active: false,
            generation: 0,
            mode: ShutdownMode::Immediate,
            waiters: HashMap::new(),
            results: HashMap::new(),
        }
    }
}
