// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result of closing an SPI backend.

/// Provider's result after its shutdown operation finishes.
///
/// This value describes provider resource cleanup. It does not report
/// facade-owned deliveries or guarantee that business handlers completed;
/// inspect the facade's [`crate::ShutdownReport`] for abandonment details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShutdownOutcome {
    /// The provider completed its shutdown procedure.
    ///
    /// This does not prove that no provider-owned or facade-owned delivery was
    /// abandoned; providers that cannot report delivery counts should leave
    /// that information to the facade report's uncertainty flag.
    Complete,
    /// The provider completed cleanup after its own grace period expired.
    ///
    /// The facade caller may still have timed out before shutdown completed;
    /// this variant is not the same as `ShutdownError::TimedOut`.
    TimedOut,
}
