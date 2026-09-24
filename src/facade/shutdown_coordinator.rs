// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deadline-aware caller coordination for synchronous bus shutdown.

use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Instant;

use super::shutdown_coordinator_state::ShutdownCoordinatorState;
use crate::error::SpiError;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

/// Serializes shutdown attempts and lets graceful callers leave at a deadline.
pub(crate) struct ShutdownCoordinator {
    state: Mutex<ShutdownCoordinatorState>,
    changed: Condvar,
}

impl ShutdownCoordinator {
    /// Creates a coordinator that has not started a shutdown attempt.
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ShutdownCoordinatorState::new()),
            changed: Condvar::new(),
        }
    }

    /// Starts an attempt or joins the active attempt, strengthening its mode.
    pub(crate) fn begin(&self, mode: ShutdownMode) -> (bool, u64) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active {
            let generation = state.generation;
            if matches!(mode, ShutdownMode::Immediate) {
                state.mode = ShutdownMode::Immediate;
            }
            *state.waiters.entry(generation).or_default() += 1;
            return (false, generation);
        }
        state.active = true;
        state.generation = state.generation.wrapping_add(1);
        state.mode = mode;
        let generation = state.generation;
        *state.waiters.entry(generation).or_default() += 1;
        (true, generation)
    }

    /// Returns the strongest shutdown mode requested for this active attempt.
    pub(crate) fn mode(&self, generation: u64) -> ShutdownMode {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation == generation {
            state.mode
        } else {
            ShutdownMode::Immediate
        }
    }

    /// Publishes an attempt result and wakes every caller waiting on it.
    pub(crate) fn finish(&self, generation: u64, result: Result<ShutdownOutcome, SpiError>) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation == generation {
            if state.waiters.contains_key(&generation) {
                state.results.insert(generation, result.map_err(std::sync::Arc::new));
            }
            state.active = false;
            self.changed.notify_all();
        }
    }

    /// Releases callers when a coordinator thread could not be created.
    pub(crate) fn abort_start(&self, generation: u64) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation == generation {
            state.active = false;
            self.release_waiter(&mut state, generation);
            self.changed.notify_all();
        }
    }

    /// Waits for one generation, returning whether its caller deadline elapsed.
    pub(crate) fn wait(
        &self,
        generation: u64,
        deadline: Option<Instant>,
    ) -> (bool, Option<Result<ShutdownOutcome, std::sync::Arc<SpiError>>>) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.active && state.generation == generation {
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    self.release_waiter(&mut state, generation);
                    return (true, None);
                }
                let (next_state, result) = self
                    .changed
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state = next_state;
                if result.timed_out() && state.active && state.generation == generation {
                    self.release_waiter(&mut state, generation);
                    return (true, None);
                }
            } else {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
        let result = state.results.get(&generation).cloned();
        self.release_waiter(&mut state, generation);
        (false, result)
    }

    /// Removes one caller and discards a result after its last waiter leaves.
    fn release_waiter(&self, state: &mut ShutdownCoordinatorState, generation: u64) {
        if let Some(waiters) = state.waiters.get_mut(&generation) {
            *waiters = waiters.saturating_sub(1);
            if *waiters == 0 {
                state.waiters.remove(&generation);
                state.results.remove(&generation);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::ShutdownCoordinator;
    use crate::spi::ShutdownMode;

    #[test]
    fn immediate_request_strengthens_active_attempt() {
        let coordinator = ShutdownCoordinator::new();
        let graceful = ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        };
        let (leader, generation) = coordinator.begin(graceful);
        assert!(leader);
        assert_eq!(coordinator.begin(ShutdownMode::Immediate), (false, generation));
        assert_eq!(coordinator.mode(generation), ShutdownMode::Immediate);
    }

    #[test]
    fn stale_generation_uses_immediate_mode_and_cannot_finish_current_attempt() {
        let coordinator = ShutdownCoordinator::new();
        let (leader, generation) = coordinator.begin(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        });
        assert!(leader);
        assert_eq!(coordinator.mode(generation + 1), ShutdownMode::Immediate);
        coordinator.finish(generation + 1, Ok(crate::spi::ShutdownOutcome::Complete));
        assert_eq!(
            coordinator.mode(generation),
            ShutdownMode::Graceful {
                timeout: Duration::from_secs(1)
            }
        );
        coordinator.abort_start(generation);
    }

    #[test]
    fn failed_start_releases_waiter_and_allows_retry() {
        let coordinator = ShutdownCoordinator::new();
        let (_, generation) = coordinator.begin(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        });
        coordinator.abort_start(generation);
        let (leader, retry_generation) = coordinator.begin(ShutdownMode::Immediate);
        assert!(leader);
        assert_ne!(retry_generation, generation);
        coordinator.abort_start(retry_generation);
    }

    #[test]
    fn expired_deadline_releases_waiter_while_attempt_continues() {
        let coordinator = ShutdownCoordinator::new();
        let (_, generation) = coordinator.begin(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        });
        let (timed_out, result) = coordinator.wait(generation, Some(Instant::now() - Duration::from_millis(1)));
        assert!(timed_out);
        assert!(result.is_none());
        coordinator.finish(generation, Ok(crate::spi::ShutdownOutcome::Complete));
    }
}
