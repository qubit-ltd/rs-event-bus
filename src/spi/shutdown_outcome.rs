// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result of closing an SPI backend.

/// Summary of provider shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShutdownOutcome {
    /// Shutdown completed without abandoning known work.
    Complete,
    /// Shutdown completed after the grace period with work abandoned.
    TimedOut,
}
