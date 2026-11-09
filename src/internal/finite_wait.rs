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
#[must_use]
pub(crate) struct FiniteWait {
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
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    /// Returns the next wait slice, not the total remaining budget.
    ///
    /// Callers must recheck their completion condition after every slice,
    /// including a timed-out slice, and call this method again before waiting.
    ///
    /// # Returns
    /// `Some` with a positive wait slice while the total budget remains, or
    /// `None` once the total timeout has elapsed.
    pub(crate) fn remaining(&self) -> Option<Duration> {
        wait_slice(self.timeout, self.started.elapsed())
    }
}

/// Computes a positive wait slice for deterministic budget arithmetic.
fn wait_slice(timeout: Duration, elapsed: Duration) -> Option<Duration> {
    wait_slice_with_max(timeout, elapsed, MAX_WAIT_SLICE)
}

/// Computes a positive wait slice with an explicit cap for deterministic tests.
fn wait_slice_with_max(timeout: Duration, elapsed: Duration, max_slice: Duration) -> Option<Duration> {
    let remaining = timeout.saturating_sub(elapsed);
    (!remaining.is_zero()).then_some(remaining.min(max_slice))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::FiniteWait;
    use super::MAX_WAIT_SLICE;
    use super::wait_slice;
    use super::wait_slice_with_max;

    #[test]
    fn test_wait_slice_tracks_total_budget_and_caps_individual_waits() {
        assert_eq!(wait_slice(Duration::ZERO, Duration::ZERO), None);
        assert_eq!(wait_slice(Duration::from_secs(2), Duration::from_secs(2)), None);
        assert_eq!(wait_slice(Duration::from_secs(2), Duration::from_secs(3)), None);
        assert_eq!(
            wait_slice(Duration::from_secs(2), Duration::from_secs(1)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(wait_slice(Duration::MAX, Duration::ZERO), Some(MAX_WAIT_SLICE));
        assert_eq!(
            wait_slice(Duration::from_secs(7201), Duration::from_secs(3600)),
            Some(MAX_WAIT_SLICE)
        );
        assert_eq!(
            wait_slice(Duration::from_secs(7201), Duration::from_secs(7200)),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn test_timed_out_slices_recheck_state_without_ending_total_budget() {
        let total = Duration::from_secs(3);
        let max_slice = Duration::from_secs(1);
        let mut elapsed = Duration::ZERO;
        let mut simulated_wakes = 0;
        let completion_observations = [false; 4];

        loop {
            // Model the completion check after each timed-out Condvar slice.
            let complete = completion_observations[simulated_wakes];
            if complete {
                break;
            }
            let Some(slice) = wait_slice_with_max(total, elapsed, max_slice) else {
                break;
            };
            assert_eq!(slice, max_slice);

            // A timed-out slice wakes the waiter, but does not itself exhaust
            // the three-second operation budget.
            simulated_wakes += 1;
            elapsed += slice;
            assert_eq!(
                wait_slice_with_max(total, elapsed, max_slice).is_some(),
                elapsed < total
            );
        }

        assert_eq!(simulated_wakes, 3);
        assert_eq!(elapsed, total);
    }

    #[test]
    fn test_notification_completion_and_join_share_one_total_budget() {
        let total = Duration::from_secs(2);
        let max_slice = Duration::from_secs(1);
        let join_poll_limit = Duration::from_millis(100);

        // The completion condition wait consumes its first one-second slice.
        let completion_slice =
            wait_slice_with_max(total, Duration::ZERO, max_slice).expect("completion wait has a slice");
        assert_eq!(completion_slice, max_slice);

        // Join observation gets only the second-stage remainder, even though
        // it polls in shorter intervals.
        let mut elapsed = completion_slice;
        assert_eq!(
            wait_slice_with_max(total, elapsed, max_slice),
            Some(Duration::from_secs(1))
        );
        for _ in 0..10 {
            let join_poll = wait_slice_with_max(total, elapsed, max_slice)
                .expect("the shared close budget remains during join")
                .min(join_poll_limit);
            assert_eq!(join_poll, join_poll_limit);
            elapsed += join_poll;
        }

        assert_eq!(elapsed, total);
        assert_eq!(wait_slice_with_max(total, elapsed, max_slice), None);
        assert_eq!(
            wait_slice_with_max(total, Duration::ZERO, max_slice),
            Some(completion_slice)
        );
    }

    #[test]
    fn test_finite_wait_zero_duration_is_exhausted() {
        assert_eq!(FiniteWait::new(Duration::ZERO).remaining(), None);
    }

    #[test]
    fn test_finite_wait_five_seconds_is_bounded() {
        let remaining = FiniteWait::new(Duration::from_secs(5))
            .remaining()
            .expect("budget remains");
        assert!(remaining > Duration::ZERO);
        assert!(remaining <= Duration::from_secs(5));
    }

    #[test]
    fn test_finite_wait_max_duration_is_sliced() {
        assert_eq!(FiniteWait::new(Duration::MAX).remaining(), Some(MAX_WAIT_SLICE));
    }
}
