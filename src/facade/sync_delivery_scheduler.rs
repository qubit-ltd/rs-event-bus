// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fixed handler pool and wake routing for the shared scheduling core.

mod internal;

use std::collections::HashMap;
use std::collections::HashSet;
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
use std::thread::Thread;

use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_id::Id;

use self::internal::SchedulerState;
use crate::facade::DeliveryMetricsSnapshot;
use crate::facade::DeliverySchedulingConfig;
use crate::facade::internal::DeliverySchedulerCore;
use crate::pipeline::OrderingLaneKey;

/// Shares metadata admission across receiver owners and executes granted jobs.
pub(super) struct SyncDeliveryScheduler {
    /// Validated independent resource bounds.
    config: DeliverySchedulingConfig,
    /// Provider-independent ownership and lane state.
    core: DeliverySchedulerCore,
    /// Fixed pool queue and lifecycle.
    state: Mutex<SchedulerState>,
    /// Wakes idle handler threads for granted jobs or shutdown.
    changed: Condvar,
    /// Owner thread handles used for coalesced, lossless unparks.
    owners: Mutex<HashMap<Id, Thread>>,
    /// Registered identities, including owners that have not started yet.
    registered: Mutex<HashSet<Id>>,
    /// Handler worker handles retained until shutdown.
    workers: Mutex<Vec<JoinHandle<()>>>,
    /// Injects a worker spawn failure in internal lifecycle tests.
    #[cfg(test)]
    fail_spawn_at: AtomicUsize,
}

impl SyncDeliveryScheduler {
    /// Creates an idle pool and empty scheduling core from validated bounds.
    ///
    /// # Parameters
    /// - `config`: independent validated H/D/P/S resource bounds.
    ///
    /// # Returns
    /// An idle shared scheduler with no allocated worker threads.
    pub(super) fn new(config: DeliverySchedulingConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            core: DeliverySchedulerCore::new(config),
            state: Mutex::new(SchedulerState::default()),
            changed: Condvar::new(),
            owners: Mutex::new(HashMap::new()),
            registered: Mutex::new(HashSet::new()),
            workers: Mutex::new(Vec::new()),
            #[cfg(test)]
            fail_spawn_at: AtomicUsize::new(usize::MAX),
        })
    }

    /// Starts exactly H reusable handler threads; unwinds partial startup on
    /// failure.
    ///
    /// # Errors
    /// Returns the thread-spawn I/O error after stopping and joining any
    /// partial pool.
    ///
    /// # Side Effects
    /// Starts H threads and retains their join handles until shutdown.
    pub(super) fn start(self: &Arc<Self>) -> io::Result<()> {
        let mut handles = self.workers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !handles.is_empty() {
            return Ok(());
        }
        for index in 0..self.config.max_running_handlers().get() {
            let scheduler = self.clone();
            #[cfg(test)]
            let result = if self.fail_spawn_at.load(Ordering::Acquire) == index {
                Err(io::Error::other("synthetic scheduler worker spawn failure"))
            } else {
                thread::Builder::new()
                    .name(format!("event-bus-handler-{index}"))
                    .spawn(move || scheduler.worker_loop())
            };
            #[cfg(not(test))]
            let result = thread::Builder::new()
                .name(format!("event-bus-handler-{index}"))
                .spawn(move || scheduler.worker_loop());
            match result {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    self.state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .stopped = true;
                    self.changed.notify_all();
                    for handle in handles.drain(..) {
                        let _ = handle.join();
                    }
                    self.state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .stopped = false;
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// Configures the zero-based worker whose startup should fail.
    ///
    /// # Parameters
    /// - `index`: zero-based worker index whose test-only spawn must fail.
    #[cfg(test)]
    pub(super) fn fail_spawn_at(&self, index: usize) {
        self.fail_spawn_at.store(index, Ordering::Release);
    }

    /// Registers one identity before starting its receiver owner; false means
    /// invariant failure or full capacity.
    ///
    /// # Parameters
    /// - `id`: identity whose registration is serialized with cancellation and
    ///   removal.
    ///
    /// # Returns
    /// True when admitted; false for duplicate identities or full subscription
    /// capacity.
    #[must_use]
    pub(super) fn register(&self, id: Id) -> bool {
        let mut identities = self
            .registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let registered = self.core.register(id);
        if registered {
            identities.insert(id);
        }
        drop(identities);
        self.route_notifications();
        registered
    }

    /// Attaches the current owner thread and activates dispatch before its
    /// first loop.
    ///
    /// # Parameters
    /// - `id`: registered identity associated with the current receiver thread.
    ///
    /// # Side Effects
    /// Activates dispatch and wakes owners affected by core admission.
    pub(super) fn attach_owner(&self, id: Id) {
        self.owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, thread::current());
        self.core.set_dispatch_active(id, true);
        self.route_notifications();
    }

    /// Removes a receiver blocked inside SPI from the runnable owner rotation.
    ///
    /// # Parameters
    /// - `id`: owner entering or leaving a potentially blocking SPI call.
    /// - `active`: whether this owner can presently claim grants.
    pub(super) fn set_dispatch_active(&self, id: Id, active: bool) {
        self.core.set_dispatch_active(id, active);
        self.route_notifications();
    }

    /// Routes all coalesced core notifications after the metadata lock is
    /// released.
    pub(super) fn route_notifications(&self) {
        let notifications = self.core.take_notifications();
        let owners = self.owners.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for id in notifications {
            if let Some(owner) = owners.get(&id) {
                owner.unpark();
            }
        }
    }

    /// Wakes a receiver owner after its completion channel or cancellation
    /// changes.
    ///
    /// # Parameters
    /// - `id`: receiver to unpark; missing or closed identities are ignored.
    pub(super) fn notify(&self, id: Id) {
        if let Some(owner) = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&id)
        {
            owner.unpark();
        }
    }

    /// Registers receive demand and routes newly available capacity.
    ///
    /// # Parameters
    /// - `id`: active subscription requesting at most one receive reservation.
    pub(super) fn request_receive(&self, id: Id) {
        self.core.request_receive(id);
        self.route_notifications();
    }
    /// Claims the reserved receive lease, or None while capacity is
    /// unavailable.
    ///
    /// # Parameters
    /// - `id`: receiver claiming its pending credit.
    ///
    /// # Returns
    /// Some lease when reserved; None while capacity or eligibility is
    /// unavailable.
    #[must_use]
    pub(super) fn take_receive_reservation(&self, id: Id) -> Option<u64> {
        let result = self.core.take_receive_reservation(id);
        self.route_notifications();
        result
    }
    /// Transfers a received message's credit to its optional ordering lane.
    ///
    /// # Parameters
    /// - `lease`: claimed receive credit now retaining an owned payload.
    /// - `lane`: optional ordering key whose FIFO covers handler and
    ///   settlement.
    pub(super) fn enqueue(&self, lease: u64, lane: Option<OrderingLaneKey>) {
        self.core.enqueue(lease, lane);
        self.route_notifications();
    }
    /// Claims a fair handler grant, or None while another owner or lane must
    /// proceed.
    ///
    /// # Parameters
    /// - `id`: receiver attempting to claim the next fair handler grant.
    ///
    /// # Returns
    /// Some owned lease with H and lane authorization; None when no grant is
    /// available.
    #[must_use]
    pub(super) fn take_ready(&self, id: Id) -> Option<u64> {
        let result = self.core.take_ready(id);
        self.route_notifications();
        result
    }
    /// Returns execution capacity while retaining delivery ownership and its
    /// lane.
    ///
    /// # Parameters
    /// - `lease`: granted callback that has actually returned or unwound.
    ///
    /// # Side Effects
    /// Returns H immediately and wakes other owners; retains the lane and owned
    /// credit.
    pub(super) fn handler_finished(&self, lease: u64) {
        self.core.handler_finished(lease);
        self.route_notifications();
    }
    /// Preserves `lease` and its lane while the owner waits between attempts.
    pub(super) fn finish_attempt_waiting(&self, lease: u64) {
        self.core.finish_attempt_waiting(lease);
        self.route_notifications();
    }

    /// Requeues the existing due `lease` and wakes the next eligible owner.
    pub(super) fn wake_retry(&self, lease: u64) {
        self.core.wake_retry(lease);
        self.route_notifications();
    }

    /// Returns `lease` ownership and lane credits after its resources are
    /// released. Repeated calls are harmless and wake eligible owners.
    pub(super) fn complete(&self, lease: u64) {
        self.core.complete(lease);
        self.route_notifications();
    }
    /// Reports permanently exhausted lease identifiers without wrapping them.
    #[must_use]
    #[inline]
    pub(super) fn lease_ids_exhausted(&self) -> bool {
        self.core.lease_ids_exhausted()
    }

    /// Marks an actually claimed reservation with its same-domain ownership
    /// origin.
    ///
    /// # Parameters
    /// - `lease`: newly claimed receive credit.
    /// - `now`: injected-clock instant sampled outside core locks.
    pub(super) fn record_owned_start(&self, lease: u64, now: MonotonicInstant) {
        self.core.record_owned_start(lease, now);
        self.route_notifications();
    }
    /// Captures metadata before sampling the clock outside locks, avoiding
    /// races with newer leases.
    ///
    /// # Parameters
    /// - `scope`: subscription identity, or None for all active deliveries.
    /// - `clock`: source sampled after capturing metadata under the core lock.
    ///
    /// # Returns
    /// Exact captured gauges and the oldest owned age, if any.
    ///
    /// # Errors
    /// Returns TimeError when sampled instants belong to different domains or
    /// regress.
    pub(super) fn snapshot(
        &self,
        scope: Option<Id>,
        clock: &dyn MonotonicClock,
    ) -> Result<DeliveryMetricsSnapshot, TimeError> {
        let captured = self.core.snapshot_input(scope);
        captured.at(clock.now())
    }
    /// Returns exact active gauges without computing a possibly invalid age.
    ///
    /// # Parameters
    /// - `scope`: subscription identity, or None for the entire bus.
    ///
    /// # Returns
    /// Captured live gauges with unknown age and zero cumulative counters.
    pub(super) fn snapshot_gauges(&self, scope: Option<Id>) -> DeliveryMetricsSnapshot {
        self.core.snapshot_gauges(scope)
    }
    /// Queues a decode rejection in its ordering lane without consuming a
    /// handler slot.
    ///
    /// # Parameters
    /// - `lease`: claimed receive credit whose decode rejection is ready.
    /// - `lane`: optional key preserving FIFO through the entire rejection
    ///   cycle.
    pub(super) fn enqueue_settlement(&self, lease: u64, lane: Option<OrderingLaneKey>) {
        self.core.enqueue_settlement(lease, lane);
        self.route_notifications();
    }
    /// Claims lane authorization for owner settlement independently of handler
    /// capacity.
    ///
    /// # Parameters
    /// - `id`: owner requesting the next fair rejection lane.
    ///
    /// # Returns
    /// Some lease authorized for settlement; None when no lane grant is
    /// available.
    #[must_use]
    pub(super) fn take_settlement_ready(&self, id: Id) -> Option<u64> {
        let result = self.core.take_settlement_ready(id);
        self.route_notifications();
        result
    }

    /// Enqueues a previously granted job without invoking application work
    /// inline.
    ///
    /// # Parameters
    /// - `run`: callback already holding an execution grant, invoked on the
    ///   pool.
    ///
    /// # Side Effects
    /// Wakes one worker; this method never waits for settlement or user code.
    pub(super) fn submit(&self, run: impl FnOnce() + Send + 'static) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .jobs
            .push_back(Box::new(run));
        self.changed.notify_one();
    }

    /// Stops new admission; receiver owners release unstarted deliveries
    /// themselves.
    ///
    /// # Parameters
    /// - `immediate`: true cancels admission; false permits draining owned
    ///   work.
    ///
    /// # Side Effects
    /// Publishes shutdown policy before waking affected receiver owners.
    pub(super) fn stop_admission(&self, immediate: bool) {
        {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.draining = true;
            state.immediate |= immediate;
        }
        let ids: Vec<_> = self
            .registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        for id in ids {
            if immediate {
                self.cancel_subscription(id);
            } else {
                self.notify(id);
            }
        }
    }

    /// Begins shutdown without running delivery callbacks on the requesting
    /// thread.
    ///
    /// Immediate cancellation is routed through receiver owners, which release
    /// their own leases and retained payloads; granted handler work remains on
    /// the fixed pool. Repeated requests may strengthen graceful drain to
    /// immediate cancellation.
    ///
    /// # Parameters
    /// - `immediate`: whether to cancel owned work that has not started.
    ///
    /// # Side Effects
    /// Closes admission and wakes every registered receiver owner.
    pub(super) fn request_stop(&self, immediate: bool) {
        self.stop_admission(immediate);
    }

    /// Fences a canceled subscription and wakes its owner for retained-payload
    /// cleanup.
    ///
    /// # Parameters
    /// - `id`: still-registered owner to fence; historical identities are
    ///   ignored.
    ///
    /// # Side Effects
    /// Atomically coordinates the cancellation index with registration removal.
    pub(super) fn cancel_subscription(&self, id: Id) {
        let identities = self
            .registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !identities.contains(&id) {
            return;
        }
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancelled
            .insert(id);
        self.core.stop_subscription(id);
        drop(identities);
        self.route_notifications();
        self.notify(id);
    }

    /// Checks actual handler admission against cancellation and shutdown.
    ///
    /// # Parameters
    /// - `id`: subscription attempting to start user code.
    /// - `cancelled`: cancellation sampled under the subscription start gate.
    ///
    /// # Returns
    /// True for active delivery work or already owned work during graceful
    /// drain.
    #[must_use]
    pub(super) fn handler_may_start(&self, id: Id, cancelled: bool) -> bool {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        !state.immediate && !state.cancelled.contains(&id) && (!cancelled || state.draining)
    }

    /// Returns whether graceful shutdown may finish this owner's already
    /// received work.
    ///
    /// # Parameters
    /// - `id`: owner whose cancellation and graceful policy are being examined.
    ///
    /// # Returns
    /// True only during graceful shutdown without individual or immediate
    /// cancellation.
    #[must_use]
    pub(super) fn should_drain(&self, id: Id) -> bool {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.draining && !state.immediate && !state.cancelled.contains(&id)
    }

    /// Removes an empty registration after provider close and owner cleanup.
    ///
    /// # Parameters
    /// - `id`: receiver that has closed and released all owned deliveries.
    ///
    /// # Panics
    /// Debug builds assert that unregister succeeds; claimed leases must be
    /// RAII-owned.
    pub(super) fn finish_subscription(&self, id: Id) {
        let mut identities = self
            .registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.core.stop_subscription(id);
        let removed = self.core.unregister(id);
        debug_assert!(removed, "finished subscription must own no leases");
        identities.remove(&id);
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancelled
            .remove(&id);
        drop(identities);
        self.owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
        self.route_notifications();
    }

    /// Stops and joins the pool after all receiver owners have completed.
    ///
    /// # Side Effects
    /// Blocks until all pool threads exit; call only after receiver owners
    /// finish.
    pub(super) fn join(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped = true;
        self.changed.notify_all();
        for handle in self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            let _ = handle.join();
        }
    }

    /// Runs granted callbacks and immediately returns the real worker thread to
    /// the pool.
    ///
    /// # Side Effects
    /// Waits for granted jobs, contains callback panics and exits when the pool
    /// stops.
    fn worker_loop(&self) {
        loop {
            let job = {
                let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                loop {
                    if let Some(job) = state.jobs.pop_front() {
                        break Some(job);
                    }
                    if state.stopped {
                        break None;
                    }
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            };
            let Some(job) = job else {
                return;
            };
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
        }
    }
}

#[cfg(test)]
mod scheduler_race_tests {
    use std::num::NonZeroUsize;
    use std::sync::mpsc;
    use std::time::Duration;

    use qubit_id::Id;

    use crate::facade::DeliverySchedulingConfig;
    use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;

    #[test]
    fn test_reservation_cancel_race_does_not_run_owner_settlement_inline() {
        let one = NonZeroUsize::new(1).expect("positive limit");
        let scheduler =
            SyncDeliveryScheduler::new(DeliverySchedulingConfig::new(one, one, one, one).expect("valid config"));
        scheduler.start().expect("workers start");
        let id = Id::new(44);
        assert!(scheduler.register(id));
        scheduler.attach_owner(id);
        scheduler.request_receive(id);
        let lease = scheduler.take_receive_reservation(id).expect("receive credit");
        scheduler.enqueue(lease, None);
        assert_eq!(scheduler.take_ready(id), Some(lease));
        scheduler.cancel_subscription(id);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (returned_tx, returned_rx) = mpsc::channel();
        let submit_scheduler = scheduler.clone();
        let submitter = std::thread::spawn(move || {
            submit_scheduler.submit(move || {
                started_tx.send(()).expect("observer alive");
                release_rx.recv().expect("release callback");
            });
            returned_tx.send(()).expect("observer alive");
        });
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("callback starts");
        let returned = returned_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        release_tx.send(()).expect("callback alive");
        submitter.join().expect("submitter exits");
        assert!(returned, "submission must leave the owner available for settlement");
        scheduler.handler_finished(lease);
        assert!(
            !scheduler.core.unregister(id),
            "cancellation and handler finish retain owned credit"
        );
        scheduler.complete(lease);
        scheduler.finish_subscription(id);
        scheduler.stop_admission(false);
        scheduler.join();
    }

    #[test]
    fn test_closed_subscription_recancellation_cannot_recreate_tombstones() {
        let one = NonZeroUsize::new(1).expect("positive limit");
        let scheduler =
            SyncDeliveryScheduler::new(DeliverySchedulingConfig::new(one, one, one, one).expect("valid config"));
        for index in 1..=1000 {
            let id = Id::new(index);
            assert!(scheduler.register(id));
            scheduler.cancel_subscription(id);
            assert_eq!(scheduler.state.lock().expect("pool state").cancelled.len(), 1);
            scheduler.finish_subscription(id);
            scheduler.cancel_subscription(id);
            assert!(
                scheduler.state.lock().expect("pool state").cancelled.is_empty(),
                "closed IDs cannot reenter cancellation metadata"
            );
        }
        for index in 1001..=1100 {
            let id = Id::new(index);
            assert!(scheduler.register(id));
            let gate = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    gate.wait();
                    scheduler.cancel_subscription(id);
                });
                gate.wait();
                scheduler.finish_subscription(id);
            });
            assert!(
                scheduler.state.lock().expect("pool state").cancelled.is_empty(),
                "finish and cancellation share one lifecycle fence"
            );
        }
    }
}
