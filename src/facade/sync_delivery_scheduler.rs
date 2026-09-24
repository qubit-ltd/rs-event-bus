// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Shared bounded dispatcher for synchronous subscription deliveries.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;

use qubit_id::Id;

use crate::facade::SyncDeliverySchedulerConfig;
use crate::pipeline::AdmissionPermit;
use crate::pipeline::AdmissionTracker;
use crate::pipeline::OrderingLaneKey;

/// One accepted handler task; its permit remains held until `run` returns.
pub(super) struct ScheduledJob {
    subscription_id: Id,
    ordering_key: Option<OrderingLaneKey>,
    run: Box<dyn FnOnce(bool) + Send + 'static>,
    permit: Option<AdmissionPermit>,
}

/// Reserves both global admission and one bounded queue slot before a job is
/// built.
pub(super) struct SchedulerReservation {
    scheduler: Arc<SyncDeliveryScheduler>,
    subscription_id: Id,
    ordering_key: Option<OrderingLaneKey>,
    permit: Option<AdmissionPermit>,
    committed: bool,
}

impl SchedulerReservation {
    /// Commits a reserved delivery to the shared dispatcher.
    pub(super) fn submit<F>(mut self, run: F)
    where
        F: FnOnce(bool) + Send + 'static,
    {
        let mut state = self
            .scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reserved_queue = state.reserved_queue.saturating_sub(1);
        if state.stopping_immediate || state.cancelled_subscriptions.contains(&self.subscription_id) {
            state.cancelled_jobs.push_back(ScheduledJob {
                subscription_id: self.subscription_id,
                ordering_key: None,
                run: Box::new(run),
                permit: self.permit.take(),
            });
            self.committed = true;
            self.scheduler.changed.notify_all();
            return;
        }
        let job = ScheduledJob {
            subscription_id: self.subscription_id,
            ordering_key: self.ordering_key.clone(),
            run: Box::new(run),
            permit: self.permit.take(),
        };
        let subscription_id = job.subscription_id;
        let queue_is_empty = state.queues.get(&subscription_id).is_none_or(VecDeque::is_empty);
        if queue_is_empty {
            state.round_robin.push_back(job.subscription_id);
        }
        state.queues.entry(subscription_id).or_default().push_back(job);
        state.queued += 1;
        self.committed = true;
        self.scheduler.changed.notify_all();
    }
}

impl Drop for SchedulerReservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut state = self
                .scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.reserved_queue = state.reserved_queue.saturating_sub(1);
            self.scheduler.changed.notify_all();
        }
    }
}

/// Bus-wide dispatcher with bounded admission and fair per-subscription queues.
pub(super) struct SyncDeliveryScheduler {
    config: SyncDeliverySchedulerConfig,
    admission: AdmissionTracker,
    state: Mutex<SchedulerState>,
    changed: Condvar,
    workers: Mutex<Vec<JoinHandle<()>>>,
    #[cfg(test)]
    fail_spawn_at: AtomicUsize,
}

#[derive(Default)]
struct SchedulerState {
    queues: HashMap<Id, VecDeque<ScheduledJob>>,
    cancelled_jobs: VecDeque<ScheduledJob>,
    round_robin: VecDeque<Id>,
    active_keys: HashSet<OrderingLaneKey>,
    cancelled_subscriptions: HashSet<Id>,
    queued: usize,
    reserved_queue: usize,
    idle_workers: usize,
    started: bool,
    accepting: bool,
    stopping_immediate: bool,
    stopped: bool,
}

impl SyncDeliveryScheduler {
    /// Creates an idle scheduler; worker threads are started lazily by
    /// subscribe.
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

    /// Starts the fixed worker set once, returning a spawn failure to
    /// subscribe.
    pub(super) fn start(self: &Arc<Self>) -> io::Result<()> {
        let mut handles = self.workers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
                    let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.accepting = false;
                    state.stopped = true;
                    self.changed.notify_all();
                    drop(state);
                    for handle in handles.drain(..) {
                        let _ = handle.join();
                    }
                    let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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

    #[cfg(test)]
    pub(super) fn fail_spawn_at(&self, worker_index: usize) {
        self.fail_spawn_at.store(worker_index, Ordering::Release);
    }

    /// Attempts admission without blocking the subscription coordinator.
    pub(super) fn try_reserve(
        self: &Arc<Self>,
        subscription_id: Id,
        ordering_key: Option<OrderingLaneKey>,
    ) -> Option<SchedulerReservation> {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
    pub(super) fn stop_admission(&self, immediate: bool) {
        let canceled = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.accepting = false;
            state.stopping_immediate = immediate;
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
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(true)));
        }
        self.changed.notify_all();
    }

    /// Requeues queued work owned by a canceled subscription.
    pub(super) fn cancel_subscription(&self, subscription_id: Id) {
        let canceled = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(true)));
        }
    }

    /// Removes a completed subscription from the cancellation registry after
    /// its coordinator drains.
    pub(super) fn finish_subscription(&self, subscription_id: Id) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancelled_subscriptions
            .remove(&subscription_id);
    }

    /// Joins the shared handler workers after all subscription coordinators
    /// finish.
    pub(super) fn join(&self) {
        let mut handles = self.workers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for handle in handles.drain(..) {
            let _ = handle.join();
        }
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped = true;
    }

    /// Returns the number of cancellation tombstones retained for active
    /// subscriptions.
    #[cfg(test)]
    pub(super) fn cancelled_subscription_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancelled_subscriptions
            .len()
    }

    /// Selects an eligible task using round-robin subscription fairness.
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
                let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(cancelled)));
            if let Some(key) = ordering_key {
                self.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
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
