// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared bounded dispatcher for synchronous subscription deliveries.

mod internal;

use std::io;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;

use qubit_id::Id;

use self::internal::ScheduledJob;
use self::internal::SchedulerReservation;
use self::internal::SchedulerState;
use crate::facade::SyncDeliverySchedulerConfig;
use crate::pipeline::AdmissionTracker;
use crate::pipeline::OrderingLaneKey;

/// Bus-wide dispatcher with bounded admission and fair per-subscription queues.
pub(super) struct SyncDeliveryScheduler {
    /// Immutable queue and worker limits.
    config: SyncDeliverySchedulerConfig,
    /// Shared bound for queued and active handler tasks.
    admission: AdmissionTracker,
    /// Queues, fairness order, and dispatcher lifecycle state.
    state: Mutex<SchedulerState>,
    /// Wakes workers after reservations, cancellation, or shutdown changes.
    changed: Condvar,
    /// Handles for the fixed handler worker set.
    workers: Mutex<Vec<JoinHandle<()>>>,
    /// Worker index where tests inject a spawn failure.
    #[cfg(test)]
    fail_spawn_at: AtomicUsize,
}

impl SyncDeliveryScheduler {
    /// Creates an idle scheduler; worker threads are started lazily by
    /// subscribe.
    ///
    /// # Parameters
    /// - `config`: validated admission, worker, and handler queue limits.
    ///
    /// # Returns
    /// A shared scheduler with empty queues and no started workers.
    ///
    /// # Panics
    /// Panics if `config.max_in_flight()` is zero, violating the validated
    /// scheduler configuration invariant.
    #[must_use = "retain the scheduler to coordinate deliveries"]
    pub(super) fn new(config: SyncDeliverySchedulerConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            admission: AdmissionTracker::new(config.max_in_flight()).expect("validated scheduler config"),
            state: Mutex::new(SchedulerState {
                accepting: true,
                ..SchedulerState::default()
            }),
            changed: Condvar::new(),
            workers: Mutex::new(Vec::new()),
            #[cfg(test)]
            fail_spawn_at: AtomicUsize::new(usize::MAX),
        })
    }

    /// Returns the number of cancellation tombstones retained for active
    /// subscriptions.
    ///
    /// # Returns
    /// The number of canceled subscriptions still registered.
    #[cfg(test)]
    #[must_use]
    pub(super) fn cancelled_subscription_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancelled_subscriptions
            .len()
    }

    /// Configures a synthetic thread-spawn failure for scheduler tests.
    ///
    /// # Parameters
    /// - `worker_index`: zero-based worker index that fails to spawn.
    #[cfg(test)]
    #[inline]
    pub(super) fn fail_spawn_at(&self, worker_index: usize) {
        self.fail_spawn_at.store(worker_index, Ordering::Release);
    }

    /// Starts the fixed worker set once, returning a spawn failure to
    /// subscribe.
    ///
    /// # Returns
    /// Success after all workers start, or the first worker spawn error.
    ///
    /// # Errors
    /// Returns the operating-system error when a handler worker cannot start.
    #[must_use = "handle a worker startup error"]
    pub(super) fn start(self: &Arc<Self>) -> io::Result<()> {
        let mut handles = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.started {
            return Ok(());
        }
        state.started = true;
        drop(state);
        for index in 0..self.config.max_in_flight() {
            let scheduler = self.clone();
            #[cfg(test)]
            let spawn_result = if self.fail_spawn_at.load(Ordering::Acquire) == index {
                Err(io::Error::other("synthetic scheduler worker spawn failure"))
            } else {
                thread::Builder::new()
                    .name(format!("event-bus-handler-{index}"))
                    .spawn(move || scheduler.worker_loop())
            };
            #[cfg(not(test))]
            let spawn_result = thread::Builder::new()
                .name(format!("event-bus-handler-{index}"))
                .spawn(move || scheduler.worker_loop());
            match spawn_result {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                    state.accepting = false;
                    state.stopped = true;
                    self.changed.notify_all();
                    drop(state);
                    for handle in handles.drain(..) {
                        let _ = handle.join();
                    }
                    let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                    state.started = false;
                    state.accepting = true;
                    state.stopped = false;
                    state.idle_workers = 0;
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// Attempts admission without blocking the subscription coordinator.
    ///
    /// # Parameters
    /// - `subscription_id`: subscription whose task requests admission.
    /// - `ordering_key`: optional exclusive per-key lane for the task.
    ///
    /// # Returns
    /// A queue and admission reservation, or `None` when capacity is
    /// unavailable.
    #[must_use = "submit or release the scheduler reservation"]
    pub(super) fn try_reserve(
        self: &Arc<Self>,
        subscription_id: Id,
        ordering_key: Option<OrderingLaneKey>,
    ) -> Option<SchedulerReservation> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let queue_is_full = if self.config.handler_queue_capacity() == 0 {
            state.idle_workers <= state.reserved_queue
                || ordering_key.as_ref().is_some_and(|key| state.active_keys.contains(key))
        } else {
            state.queued + state.reserved_queue >= self.config.handler_queue_capacity()
        };
        if !state.accepting || state.cancelled_subscriptions.contains(&subscription_id) || queue_is_full {
            return None;
        }
        let permit = self.admission.try_acquire()?;
        state.reserved_queue += 1;
        Some(SchedulerReservation {
            scheduler: self.clone(),
            subscription_id,
            ordering_key,
            permit: Some(permit),
            committed: false,
        })
    }

    /// Stops future admission and optionally returns queued jobs for retry.
    ///
    /// # Parameters
    /// - `immediate`: whether queued jobs should be canceled for provider
    ///   retry.
    pub(super) fn stop_admission(&self, immediate: bool) {
        let canceled = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.accepting = false;
            state.stopping_immediate |= immediate;
            if immediate {
                state.queued = 0;
                state.round_robin.clear();
                state.queues.drain().flat_map(|(_, jobs)| jobs).collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        };
        self.changed.notify_all();
        for job in canceled {
            let ScheduledJob { run, permit, .. } = job;
            let _permit = permit;
            let _ = catch_unwind(AssertUnwindSafe(|| run(true)));
        }
        self.changed.notify_all();
    }

    /// Stops admission and transfers queued cancellation to scheduler workers.
    /// Never executes queued callbacks, settlements or provider code on the
    /// caller's thread. Immediate requests monotonically strengthen shutdown.
    ///
    /// # Parameters
    /// - `immediate`: whether queued work is canceled as part of shutdown.
    pub(super) fn request_stop(&self, immediate: bool) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.accepting = false;
        state.stopping_immediate |= immediate;
        if state.stopping_immediate {
            state.queued = 0;
            state.round_robin.clear();
            let canceled: Vec<_> = state.queues.drain().flat_map(|(_, jobs)| jobs).collect();
            state.cancelled_jobs.extend(canceled);
        }
        self.changed.notify_all();
    }

    /// Requeues queued work owned by a canceled subscription.
    ///
    /// # Parameters
    /// - `subscription_id`: subscription whose queued jobs are canceled.
    pub(super) fn cancel_subscription(&self, subscription_id: Id) {
        let canceled = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.cancelled_subscriptions.insert(subscription_id);
            let jobs = state.queues.remove(&subscription_id).unwrap_or_default();
            state.queued = state.queued.saturating_sub(jobs.len());
            state.round_robin.retain(|id| *id != subscription_id);
            jobs
        };
        self.changed.notify_all();
        for job in canceled {
            let ScheduledJob { run, permit, .. } = job;
            let _permit = permit;
            let _ = catch_unwind(AssertUnwindSafe(|| run(true)));
        }
    }

    /// Removes a completed subscription from the cancellation registry after
    /// its coordinator drains.
    ///
    /// # Parameters
    /// - `subscription_id`: completed subscription whose tombstone is removed.
    pub(super) fn finish_subscription(&self, subscription_id: Id) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancelled_subscriptions
            .remove(&subscription_id);
    }

    /// Joins the shared handler workers after all subscription coordinators
    /// finish.
    pub(super) fn join(&self) {
        let mut handles = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
        for handle in handles.drain(..) {
            let _ = handle.join();
        }
        self.state.lock().unwrap_or_else(PoisonError::into_inner).stopped = true;
    }

    /// Selects an eligible task using round-robin subscription fairness.
    ///
    /// # Parameters
    /// - `state`: mutable queue state selected while holding the scheduler
    ///   lock.
    ///
    /// # Returns
    /// The next eligible job, or `None` when no queued job can run.
    #[must_use]
    fn take_ready(&self, state: &mut SchedulerState) -> Option<ScheduledJob> {
        let rounds = state.round_robin.len();
        for _ in 0..rounds {
            let subscription_id = state.round_robin.pop_front()?;
            let queue = state.queues.get_mut(&subscription_id)?;
            let eligible_index = queue.iter().position(|job| {
                job.ordering_key
                    .as_ref()
                    .is_none_or(|key| !state.active_keys.contains(key))
            });
            let Some(eligible_index) = eligible_index else {
                state.round_robin.push_back(subscription_id);
                continue;
            };
            let job = queue.remove(eligible_index)?;
            if let Some(key) = job.ordering_key.as_ref() {
                state.active_keys.insert(key.clone());
            }
            state.queued = state.queued.saturating_sub(1);
            if queue.is_empty() {
                state.queues.remove(&subscription_id);
            } else {
                state.round_robin.push_back(subscription_id);
            }
            return Some(job);
        }
        None
    }

    /// Runs tasks until shutdown has stopped admission and drained the queue.
    fn worker_loop(self: Arc<Self>) {
        loop {
            let job = {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                let mut counted_idle = false;
                loop {
                    if let Some(job) = state.cancelled_jobs.pop_front().map(|job| (job, true)) {
                        if counted_idle {
                            state.idle_workers = state.idle_workers.saturating_sub(1);
                        }
                        break Some(job);
                    }
                    if let Some(job) = self.take_ready(&mut state) {
                        if counted_idle {
                            state.idle_workers = state.idle_workers.saturating_sub(1);
                        }
                        break Some((job, false));
                    }
                    if !state.accepting
                        && state.queued == 0
                        && state.reserved_queue == 0
                        && state.cancelled_jobs.is_empty()
                    {
                        if counted_idle {
                            state.idle_workers = state.idle_workers.saturating_sub(1);
                        }
                        break None;
                    }
                    if !counted_idle {
                        state.idle_workers += 1;
                        counted_idle = true;
                        self.changed.notify_all();
                    }
                    state = self.changed.wait(state).unwrap_or_else(PoisonError::into_inner);
                }
            };
            let Some((job, cancelled)) = job else {
                return;
            };
            let ScheduledJob {
                subscription_id: _,
                ordering_key,
                run,
                permit,
            } = job;
            let _permit = permit;
            let _ = catch_unwind(AssertUnwindSafe(|| run(cancelled)));
            if let Some(key) = ordering_key {
                self.state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .active_keys
                    .remove(&key);
            }
            self.changed.notify_all();
        }
    }
}

#[cfg(test)]
#[path = "../../tests/support/scheduler_race_tests.rs"]
mod scheduler_race_tests;

#[cfg(test)]
mod shutdown_request_tests {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use qubit_id::Id;

    use super::SyncDeliveryScheduler;
    use crate::facade::SyncDeliverySchedulerConfig;

    #[test]
    fn test_shutdown_request_never_runs_queued_cancellation_on_caller() {
        let scheduler = SyncDeliveryScheduler::new(SyncDeliverySchedulerConfig::new(1, 1).expect("config"));
        let reservation = scheduler.try_reserve(Id::new(1), None).expect("queued capacity");
        let (tx, rx) = mpsc::channel();
        reservation.submit(move |cancelled| tx.send((cancelled, thread::current().id())).expect("callback result"));
        let caller = thread::current().id();
        scheduler.request_stop(true);
        assert!(rx.try_recv().is_err(), "request cannot execute a queued callback");
        assert!(scheduler.try_reserve(Id::new(1), None).is_none());
        let worker = scheduler.clone();
        let handle = thread::spawn(move || worker.worker_loop());
        let (cancelled, callback_thread) = rx.recv_timeout(Duration::from_secs(5)).expect("worker cancels");
        assert!(cancelled);
        assert_ne!(caller, callback_thread);
        handle.join().expect("worker exits");
    }
}
