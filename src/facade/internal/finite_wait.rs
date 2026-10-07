// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounds each finite condition-variable wait while preserving its total
//! budget.

use std::time::Duration;
use std::time::Instant;

/// Maximum duration passed to a single condition-variable wait.
const MAX_WAIT_SLICE: Duration = Duration::from_secs(60 * 60);

/// Tracks a finite timeout using monotonic elapsed time.
pub(in crate::facade) struct FiniteWait {
    /// Monotonic instant when this finite budget started.
    started: Instant,
    /// Total duration allowed by this finite budget.
    timeout: Duration,
}

impl FiniteWait {
    /// Starts tracking a finite timeout.
    ///
    /// # Parameters
    /// - `timeout`: Total duration allowed for the operation.
    ///
    /// # Returns
    /// A budget whose remaining duration is measured from this call.
    pub(in crate::facade) fn new(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    /// Returns the remaining budget, capped to one condition-variable slice.
    ///
    /// # Returns
    /// `Some` with a positive wait slice while the total budget remains, or
    /// `None` once the total timeout has elapsed.
    pub(in crate::facade) fn remaining(&self) -> Option<Duration> {
        let remaining = self.timeout.saturating_sub(self.started.elapsed());
        (!remaining.is_zero()).then_some(remaining.min(MAX_WAIT_SLICE))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::facade::internal::FiniteWait;

    /// Verifies that a zero-duration finite budget is immediately exhausted.
    #[test]
    fn test_finite_wait_zero_duration_is_exhausted() {
        let budget = FiniteWait::new(Duration::ZERO);
        assert_eq!(budget.remaining(), None);
    }

    /// Verifies that a regular finite budget remains positive and bounded.
    #[test]
    fn test_finite_wait_five_seconds_is_bounded() {
        let budget = FiniteWait::new(Duration::from_secs(5));
        let remaining = budget.remaining().expect("finite budget remains finite");
        assert!(remaining > Duration::ZERO);
        assert!(remaining <= Duration::from_secs(5));
    }

    /// Verifies that the maximum duration is split into a bounded wait slice.
    #[test]
    fn test_finite_wait_max_duration_is_sliced() {
        let budget = FiniteWait::new(Duration::MAX);
        let slice = budget.remaining().expect("finite budget remains finite");
        assert!(slice > Duration::ZERO);
        assert!(slice <= Duration::from_secs(60 * 60));
    }
}
