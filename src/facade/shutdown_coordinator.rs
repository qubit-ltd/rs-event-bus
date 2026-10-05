// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deadline-aware caller coordination for synchronous bus shutdown.

use std::io::Error as IoError;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use super::shutdown_coordinator_state::ShutdownCoordinatorState;
use super::shutdown_result::ShutdownResult;
use crate::error::SpiError;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

/// Serializes shutdown attempts and lets graceful callers leave at a deadline.
pub(crate) struct ShutdownCoordinator {
    /// Active shutdown generation, requested mode, and results for its waiters.
    state: Mutex<ShutdownCoordinatorState>,
    /// Wakes callers when a shutdown attempt completes or fails to start.
    changed: Condvar,
}

impl ShutdownCoordinator {
    /// Creates a coordinator that has not started a shutdown attempt.
    ///
    /// # Returns
    /// A coordinator with no active generation or waiting callers.
    #[must_use]
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ShutdownCoordinatorState::new()),
            changed: Condvar::new(),
        }
    }

    /// Starts an attempt or joins the active attempt, strengthening its mode.
    ///
    /// # Parameters
    /// - `mode`: shutdown policy requested by this caller.
    ///
    /// # Returns
    /// Whether this caller should start the worker and the generation to join.
    #[must_use]
    pub(crate) fn begin(&self, mode: ShutdownMode) -> (bool, u64) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
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
    ///
    /// # Parameters
    /// - `generation`: shutdown attempt whose mode is requested.
    ///
    /// # Returns
    /// The strongest mode, or `Immediate` if the generation is stale.
    pub(crate) fn mode(&self, generation: u64) -> ShutdownMode {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.generation == generation {
            state.mode
        } else {
            ShutdownMode::Immediate
        }
    }

    /// Publishes provider completion, retaining its result for every ticket.
    ///
    /// # Parameters
    /// - `generation`: attempt whose waiters should observe this result.
    /// - `result`: provider outcome or failure to retain for that generation.
    pub(crate) fn finish(&self, generation: u64, result: Result<ShutdownOutcome, SpiError>) {
        self.complete(generation, ShutdownResult::Provider(result.map_err(Arc::new)));
    }

    /// Records a thread start error for this generation and allows retry.
    ///
    /// # Parameters
    /// - `generation`: attempt whose worker could not be started.
    /// - `error`: operating system error to retain for current waiters.
    pub(crate) fn abort_start(&self, generation: u64, error: IoError) {
        self.complete(generation, ShutdownResult::StartFailed(Arc::new(error)));
    }

    /// Waits for this exact generation without releasing the ticket's observer.
    /// Returns a timeout flag and a retained result; never joins another
    /// attempt.
    ///
    /// # Parameters
    /// - `generation`: attempt generation represented by the caller's ticket.
    /// - `deadline`: optional absolute deadline; expiration leaves the ticket
    ///   registered so the caller can observe a later completion.
    ///
    /// # Returns
    /// Whether the deadline elapsed and, after completion, the retained result
    /// for this generation. A timeout returns `None` without releasing the
    /// caller's ticket.
    pub(crate) fn wait(&self, generation: u64, deadline: Option<Instant>) -> (bool, Option<ShutdownResult>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.active && state.generation == generation {
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return (true, None);
                }
                let (next, timed) = self
                    .changed
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(PoisonError::into_inner);
                state = next;
                if timed.timed_out() && state.active && state.generation == generation {
                    return (true, None);
                }
            } else {
                state = self.changed.wait(state).unwrap_or_else(PoisonError::into_inner);
            }
        }
        (false, state.results.get(&generation).cloned())
    }

    /// Atomically checks completion and installs or refreshes one future waker.
    /// Clones the supplied waker before locking and drops a replaced waker
    /// after unlocking, because either operation may execute executor
    /// callbacks.
    ///
    /// # Parameters
    /// - `generation`: attempt whose result the future is polling.
    /// - `token`: registration identity reused while this future remains
    ///   pending.
    /// - `cx`: task context supplying the observer waker.
    ///
    /// # Returns
    /// The retained result when complete, `None` for a stale generation, or
    /// `Pending` after registering the current waker.
    pub(crate) fn poll_result(
        &self,
        generation: u64,
        token: &mut Option<u64>,
        cx: &Context<'_>,
    ) -> Poll<Option<ShutdownResult>> {
        let next_waker = cx.waker().clone();
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(result) = state.results.get(&generation) {
                return Poll::Ready(Some(result.clone()));
            }
            if !state.active || state.generation != generation {
                return Poll::Ready(None);
            }
            let registration = *token.get_or_insert_with(|| {
                state.next_registration = state.next_registration.wrapping_add(1);
                state.next_registration
            });
            state
                .wakers
                .entry(generation)
                .or_default()
                .insert(registration, next_waker)
        };
        drop(retired);
        Poll::Pending
    }

    /// Cancels only the asynchronous registration identified by this token.
    /// The removed user waker is destroyed after releasing coordinator state.
    ///
    /// # Parameters
    /// - `generation`: attempt containing the registration.
    /// - `token`: registration identity returned to the polling future.
    pub(crate) fn unregister(&self, generation: u64, token: u64) {
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(wakers) = state.wakers.get_mut(&generation) {
                let removed = wakers.remove(&token);
                if wakers.is_empty() {
                    state.wakers.remove(&generation);
                }
                removed
            } else {
                None
            }
        };
        drop(retired);
    }

    /// Releases one ticket, reclaiming its generation after the final ticket.
    /// Retired results and user wakers are destroyed outside coordinator state.
    ///
    /// # Parameters
    /// - `generation`: attempt ticket being released.
    pub(crate) fn release(&self, generation: u64) {
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(waiters) = state.waiters.get_mut(&generation) {
                *waiters = waiters.saturating_sub(1);
                if *waiters == 0 {
                    state.waiters.remove(&generation);
                    Some((state.results.remove(&generation), state.wakers.remove(&generation)))
                } else {
                    None
                }
            } else {
                None
            }
        };
        drop(retired);
    }

    /// Saves completion under the state lock, then invokes wakers outside it.
    /// A stale or inactive generation is ignored. Waker callbacks run without
    /// the state lock, and one panicking callback does not prevent others.
    ///
    /// # Parameters
    /// - `generation`: active attempt that may accept the result.
    /// - `result`: immutable completion shared with that generation's tickets.
    fn complete(&self, generation: u64, result: ShutdownResult) {
        let wakers = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.generation != generation || !state.active {
                return;
            }
            if state.waiters.contains_key(&generation) {
                state.results.insert(generation, result);
            }
            state.active = false;
            self.changed.notify_all();
            state.wakers.remove(&generation).unwrap_or_default()
        };
        for waker in wakers.into_values() {
            // One executor's broken observer must not strand the remaining tickets.
            let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::sync::Arc;
    use std::sync::Weak;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;
    use std::time::Instant;

    use super::ShutdownCoordinator;
    use crate::facade::shutdown_result::ShutdownResult;
    use crate::spi::ShutdownMode;
    use crate::spi::ShutdownOutcome;

    #[test]
    fn test_immediate_request_strengthens_active_attempt() {
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
    fn test_stale_generation_uses_immediate_mode_and_cannot_finish_current_attempt() {
        let coordinator = ShutdownCoordinator::new();
        let (leader, generation) = coordinator.begin(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        });
        assert!(leader);
        assert_eq!(coordinator.mode(generation + 1), ShutdownMode::Immediate);
        coordinator.finish(generation + 1, Ok(ShutdownOutcome::Complete));
        assert_eq!(
            coordinator.mode(generation),
            ShutdownMode::Graceful {
                timeout: Duration::from_secs(1)
            }
        );
        coordinator.abort_start(generation, Error::other("failed start"));
    }

    #[test]
    fn test_failed_start_publishes_result_and_allows_retry() {
        let coordinator = ShutdownCoordinator::new();
        let (_, generation) = coordinator.begin(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        });
        coordinator.abort_start(generation, Error::other("failed start"));
        let (leader, retry_generation) = coordinator.begin(ShutdownMode::Immediate);
        assert!(leader);
        assert_ne!(retry_generation, generation);
        coordinator.abort_start(retry_generation, Error::other("failed start"));
    }

    #[test]
    fn test_expired_deadline_preserves_ticket_while_attempt_continues() {
        let coordinator = ShutdownCoordinator::new();
        let (_, generation) = coordinator.begin(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        });
        let (timed_out, result) = coordinator.wait(generation, Some(Instant::now() - Duration::from_millis(1)));
        assert!(timed_out);
        assert!(result.is_none());
        coordinator.finish(generation, Ok(ShutdownOutcome::Complete));
    }
    #[test]
    fn test_async_registration_cancel_and_failed_generation_survives_retry() {
        let coordinator = ShutdownCoordinator::new();
        let (_, generation) = coordinator.begin(ShutdownMode::Immediate);
        let mut first = None;
        let mut second = None;
        let cx = Context::from_waker(Waker::noop());
        assert!(coordinator.poll_result(generation, &mut first, &cx).is_pending());
        assert!(coordinator.poll_result(generation, &mut second, &cx).is_pending());
        assert_eq!(coordinator.state.lock().expect("state").wakers[&generation].len(), 2);
        coordinator.unregister(generation, first.take().expect("first registration"));
        assert_eq!(coordinator.state.lock().expect("state").wakers[&generation].len(), 1);
        coordinator.abort_start(generation, Error::other("spawn failed"));
        let (leader, retry) = coordinator.begin(ShutdownMode::Immediate);
        assert!(leader);
        assert_ne!(retry, generation);
        assert!(matches!(
            coordinator.poll_result(generation, &mut second, &cx),
            Poll::Ready(Some(ShutdownResult::StartFailed(_)))
        ));
        coordinator.finish(retry, Ok(ShutdownOutcome::Complete));
        assert!(matches!(
            coordinator.poll_result(generation, &mut second, &cx),
            Poll::Ready(Some(ShutdownResult::StartFailed(_)))
        ));
        coordinator.release(generation);
        coordinator.release(retry);
    }

    #[test]
    fn test_completion_wakes_outside_state_lock() {
        struct Reentrant(Arc<ShutdownCoordinator>, u64);
        impl Wake for Reentrant {
            fn wake(self: Arc<Self>) {
                let state = self.0.state.try_lock().expect("wake runs outside state lock");
                assert!(!state.active);
                assert_eq!(state.generation, self.1);
            }
        }
        let coordinator = Arc::new(ShutdownCoordinator::new());
        let (_, generation) = coordinator.begin(ShutdownMode::Immediate);
        let waker = Waker::from(Arc::new(Reentrant(coordinator.clone(), generation)));
        let mut token = None;
        assert!(
            coordinator
                .poll_result(generation, &mut token, &Context::from_waker(&waker))
                .is_pending()
        );
        coordinator.finish(generation, Ok(ShutdownOutcome::Complete));
        coordinator.release(generation);
    }

    #[test]
    fn test_completion_wakes_remaining_observers_after_waker_panic() {
        struct ConditionalWake {
            panic_on_wake: AtomicBool,
            calls: AtomicUsize,
        }
        impl Wake for ConditionalWake {
            fn wake(self: Arc<Self>) {
                self.calls.fetch_add(1, Ordering::SeqCst);
                assert!(!self.panic_on_wake.load(Ordering::SeqCst), "broken observer waker");
            }
        }
        let coordinator = ShutdownCoordinator::new();
        let (_, generation) = coordinator.begin(ShutdownMode::Immediate);
        let mut panic_token = None;
        let mut count_token = None;
        let first = Arc::new(ConditionalWake {
            panic_on_wake: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        let second = Arc::new(ConditionalWake {
            panic_on_wake: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        let first_waker = Waker::from(first.clone());
        let second_waker = Waker::from(second.clone());
        assert!(
            coordinator
                .poll_result(generation, &mut panic_token, &Context::from_waker(&first_waker))
                .is_pending()
        );
        assert!(
            coordinator
                .poll_result(generation, &mut count_token, &Context::from_waker(&second_waker))
                .is_pending()
        );
        first.panic_on_wake.store(true, Ordering::SeqCst);
        second.panic_on_wake.store(true, Ordering::SeqCst);

        let completion = catch_unwind(AssertUnwindSafe(|| {
            coordinator.finish(generation, Ok(ShutdownOutcome::Complete));
        }));
        assert!(completion.is_ok(), "observer waker panic must not escape completion");
        assert_eq!(first.calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.calls.load(Ordering::SeqCst), 1);
        coordinator.release(generation);
        coordinator.release(generation);
    }

    #[test]
    fn test_waker_update_and_last_ticket_release_reclaim_generation() {
        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let coordinator = ShutdownCoordinator::new();
        let (_, generation) = coordinator.begin(ShutdownMode::Immediate);
        let (starts_worker, joined_generation) = coordinator.begin(ShutdownMode::Immediate);
        assert!(!starts_worker);
        assert_eq!(joined_generation, generation);
        let first = Arc::new(Count(AtomicUsize::new(0)));
        let latest = Arc::new(Count(AtomicUsize::new(0)));
        let mut token = None;
        assert!(
            coordinator
                .poll_result(
                    generation,
                    &mut token,
                    &Context::from_waker(&Waker::from(first.clone()))
                )
                .is_pending()
        );
        assert!(
            coordinator
                .poll_result(
                    generation,
                    &mut token,
                    &Context::from_waker(&Waker::from(latest.clone()))
                )
                .is_pending()
        );
        coordinator.release(generation);
        coordinator.finish(generation, Ok(ShutdownOutcome::Complete));
        assert_eq!(first.0.load(Ordering::SeqCst), 0);
        assert_eq!(latest.0.load(Ordering::SeqCst), 1);
        assert!(
            coordinator
                .poll_result(generation, &mut token, &Context::from_waker(Waker::noop()))
                .is_ready()
        );
        coordinator.release(generation);
        let state = coordinator.state.lock().expect("state");
        assert!(state.waiters.is_empty());
        assert!(state.wakers.is_empty());
        assert!(state.results.is_empty());
    }

    /// Tests user Wake::Drop without leaving a deliberately deadlocked thread.
    /// The destructor records lock availability and reenters begin/release only
    /// when available; outer assertions make a locked destructor a stable RED.
    fn assert_waker_drop_reenters_outside_state(operation: &str) {
        struct ReentrantDrop {
            coordinator: Weak<ShutdownCoordinator>,
            dropped: mpsc::Sender<bool>,
            wake_count: Arc<AtomicUsize>,
        }
        impl Wake for ReentrantDrop {
            fn wake(self: Arc<Self>) {
                self.wake_count.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl Drop for ReentrantDrop {
            fn drop(&mut self) {
                let coordinator = self.coordinator.upgrade().expect("coordinator alive");
                let unlocked = coordinator.state.try_lock().is_ok();
                if unlocked {
                    let (leader, generation) = coordinator.begin(ShutdownMode::Immediate);
                    coordinator.release(generation);
                    if leader {
                        coordinator.finish(generation, Ok(ShutdownOutcome::Complete));
                    }
                }
                self.dropped.send(unlocked).expect("drop observer");
            }
        }
        let coordinator = Arc::new(ShutdownCoordinator::new());
        let (_, generation) = coordinator.begin(ShutdownMode::Immediate);
        let (tx, rx) = mpsc::channel();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(ReentrantDrop {
            coordinator: Arc::downgrade(&coordinator),
            dropped: tx,
            wake_count: wake_count.clone(),
        }));
        let mut token = None;
        assert!(
            coordinator
                .poll_result(generation, &mut token, &Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        match operation {
            "cancel" => coordinator.unregister(generation, token.expect("token")),
            "replace" => {
                assert!(
                    coordinator
                        .poll_result(generation, &mut token, &Context::from_waker(Waker::noop()))
                        .is_pending()
                );
            }
            "release" => coordinator.release(generation),
            "finish" => coordinator.finish(generation, Ok(ShutdownOutcome::Complete)),
            "abort" => coordinator.abort_start(generation, Error::other("start failure")),
            _ => panic!("unknown operation"),
        }
        assert_eq!(
            wake_count.load(Ordering::SeqCst),
            usize::from(matches!(operation, "finish" | "abort")),
            "only completion should wake a pending observer",
        );
        assert!(
            rx.recv_timeout(Duration::from_secs(5)).expect("waker destructor runs"),
            "{operation} must drop the waker outside coordinator state"
        );
        coordinator.release(generation);
    }

    #[test]
    fn test_waker_drop_cancel_can_reenter_coordinator() {
        assert_waker_drop_reenters_outside_state("cancel");
    }

    #[test]
    fn test_waker_drop_replace_can_reenter_coordinator() {
        assert_waker_drop_reenters_outside_state("replace");
    }

    #[test]
    fn test_waker_drop_last_ticket_release_can_reenter_coordinator() {
        assert_waker_drop_reenters_outside_state("release");
    }

    #[test]
    fn test_waker_drop_completion_and_start_failure_can_reenter_coordinator() {
        assert_waker_drop_reenters_outside_state("finish");
        assert_waker_drop_reenters_outside_state("abort");
    }
}
