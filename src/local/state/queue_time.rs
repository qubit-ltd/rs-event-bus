// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Time operations required by local queue scheduling.

use std::time::Duration;
use std::time::Instant;

/// Ordered time representations supported by the shared local queue.
///
/// # Type Parameters
/// Implementations carry standard monotonic instants or elapsed values from
/// an injected clock domain.
pub(in crate::local) trait QueueTime: Copy + Ord {
    /// Returns the zero point used by convenience queue operations.
    fn now() -> Self;

    /// Returns the non-negative duration until this deadline.
    ///
    /// # Parameters
    /// - `now`: current value in the same time representation.
    fn until(self, now: Self) -> Duration;
}

impl QueueTime for Instant {
    /// Samples the standard monotonic clock.
    fn now() -> Self {
        Instant::now()
    }

    /// Measures time remaining on a standard monotonic deadline.
    ///
    /// # Parameters
    /// - `now`: current instant in the same monotonic clock.
    fn until(self, now: Self) -> Duration {
        self.saturating_duration_since(now)
    }
}

impl QueueTime for Duration {
    /// Returns the elapsed-time origin for injected clock values.
    fn now() -> Self {
        Duration::ZERO
    }

    /// Measures time remaining on an elapsed-time deadline.
    ///
    /// # Parameters
    /// - `now`: current elapsed time in the same clock domain.
    fn until(self, now: Self) -> Duration {
        self.saturating_sub(now)
    }
}
