// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared cancellation, worker, and close-error state for one subscription.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use qubit_id::Id;

use self::worker_join_state::WorkerJoinState;
use crate::error::SubscriptionCloseFailure;
use crate::facade::internal::DeliveryMetrics;
use crate::internal::panic_payload::dispose_panic_payload;
use crate::model::SubscriberId;
use crate::model::SubscriptionStopReason;

mod worker_join_state;

/// Coordination state shared by a subscription handle and its worker.
pub(crate) struct SubscriptionControl {
    /// Cumulative counters retained by the public handle after receiver close.
    pub(in crate::facade) delivery_metrics: Arc<DeliveryMetrics>,
    /// Bus-local object ID used in provider settlement context.
    pub(in crate::facade) id: Id,
    /// Logical subscriber identity used by provider operations.
    pub(in crate::facade) subscriber_id: SubscriberId,
    /// Cancellation flag observed by the worker loop.
    cancelled: AtomicBool,
    /// First terminal receive cause, retained independently of close failures.
    terminal_failure: Mutex<Option<Arc<SubscriptionStopReason>>>,
    /// Linearizes new delivery work against terminal receive failure.
    start_gate: Mutex<()>,
    /// Shared worker join state and cached exit classification.
    worker: Mutex<WorkerJoinState>,
    /// Wakes callers waiting for another caller to join the worker.
    worker_joined: Condvar,
    /// Canonical provider receiver close failure.
    pub(in crate::facade) close_error: Mutex<Option<Arc<SubscriptionCloseFailure>>>,
    /// Whether receiver close and worker cleanup have completed.
    finished: Mutex<bool>,
    /// Wakes cancellation callers waiting for worker completion.
    finished_changed: Condvar,
    /// Optional per-subscription causal trace used by isolated tests.
    #[cfg(test)]
    cancellation_probe: Mutex<Option<CancellationProbe>>,
    /// Per-subscription receive poll override used by isolated tests.
    #[cfg(test)]
    receive_poll_interval: Mutex<Option<Duration>>,
}

#[cfg(test)]
#[derive(Debug)]
struct CancellationProbe {
    started: Instant,
    requested: Option<Duration>,
    receive_returns_after_request: Vec<Duration>,
    worker_finished: Option<Duration>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct CancellationProbeSnapshot {
    pub(crate) requested: Option<Duration>,
    pub(crate) receive_returns_after_request: Vec<Duration>,
    pub(crate) worker_finished: Option<Duration>,
}

impl SubscriptionControl {
    /// Creates an active control block before its worker thread is spawned.
    ///
    /// # Parameters
    /// - `id`: bus-local subscription identity.
    /// - `subscriber_id`: logical subscriber identity.
    ///
    /// # Returns
    /// A shared control block ready to receive its worker handle.
    #[cfg(test)]
    pub(in crate::facade) fn new(id: Id, subscriber_id: SubscriberId) -> Arc<Self> {
        Self::with_metrics(id, subscriber_id, Arc::new(DeliveryMetrics::default()))
    }

    /// Creates one control with its retained counters forwarding to the bus
    /// accumulator.
    ///
    /// # Parameters
    /// - `id`: active bus-local receiver identity.
    /// - `subscriber_id`: logical subscriber name for provider diagnostics.
    /// - `delivery_metrics`: fixed cumulative counters forwarding to the bus
    ///   parent.
    ///
    /// # Returns
    /// Shared control state independent of the public handle lifetime.
    pub(in crate::facade) fn with_metrics(
        id: Id,
        subscriber_id: SubscriberId,
        delivery_metrics: Arc<DeliveryMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            delivery_metrics,
            id,
            subscriber_id,
            cancelled: AtomicBool::new(false),
            terminal_failure: Mutex::new(None),
            start_gate: Mutex::new(()),
            worker: Mutex::new(WorkerJoinState::Unregistered),
            worker_joined: Condvar::new(),
            close_error: Mutex::new(None),
            finished: Mutex::new(false),
            finished_changed: Condvar::new(),
            #[cfg(test)]
            cancellation_probe: Mutex::new(None),
            #[cfg(test)]
            receive_poll_interval: Mutex::new(None),
        })
    }

    /// Returns the first terminal receive cause, or None while healthy.
    ///
    /// # Returns
    /// Some canonical first cause after failure, or None while no terminal
    /// cause exists.
    pub(in crate::facade) fn terminal_failure(&self) -> Option<Arc<SubscriptionStopReason>> {
        self.terminal_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Returns whether cancellation was requested for this subscription.
    ///
    /// # Returns
    /// True after cancellation is requested, otherwise false.
    #[must_use = "observe whether cancellation was requested"]
    #[inline]
    pub(in crate::facade) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Linearizes the start of owned delivery work against a receive stop.
    /// Admitted work completes normally even if a later receive failure stops
    /// future work.
    ///
    /// # Returns
    /// `true` when the subscription has no terminal failure; `false` otherwise.
    #[must_use = "observe whether delivery work was admitted"]
    pub(in crate::facade) fn try_start(&self) -> bool {
        let _start = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        self.terminal_failure().is_none()
    }

    /// Caches a receive failure once and prevents subsequent receives.
    /// Returns true only for the first cause, so callers emit one diagnostic.
    ///
    /// # Parameters
    /// - `reason`: terminal source retained only if no previous cause was
    ///   published.
    ///
    /// # Returns
    /// True only for the first cause; callers must fence scheduling before
    /// diagnostics.
    #[must_use = "observe whether this call published the first receive failure"]
    pub(in crate::facade) fn fail_receive(&self, reason: SubscriptionStopReason) -> bool {
        let _start = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        let mut stored = self.terminal_failure.lock().unwrap_or_else(PoisonError::into_inner);
        let first = stored.is_none();
        if first {
            *stored = Some(Arc::new(reason));
        }
        self.cancelled.store(true, Ordering::Release);
        first
    }

    /// Linearizes one actual handler invocation with stop publication.
    ///
    /// # Parameters
    /// - `allowed`: checks scheduler shutdown policy while the start gate is
    ///   held.
    ///
    /// # Returns
    /// True when this invocation is admitted; the lock is released before user
    /// code.
    #[must_use = "observe whether the handler invocation was admitted"]
    pub(in crate::facade) fn try_start_handler(&self, allowed: impl FnOnce(bool) -> bool) -> bool {
        let _start = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        self.terminal_failure().is_none() && allowed(self.is_cancelled())
    }

    /// Publishes the worker join handle after a successful thread spawn.
    ///
    /// # Parameters
    /// - `worker`: running worker thread to join during cancellation.
    pub(in crate::facade) fn set_worker(&self, worker: JoinHandle<()>) {
        *self.worker.lock().unwrap_or_else(PoisonError::into_inner) = WorkerJoinState::Ready { worker };
    }

    /// Joins the worker once and shares its panic classification with every
    /// caller.
    pub(in crate::facade) fn join_worker_panicked(&self) -> bool {
        let worker = loop {
            let mut state = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *state {
                WorkerJoinState::Unregistered => return false,
                WorkerJoinState::Ready { .. } => {
                    let WorkerJoinState::Ready { worker } = std::mem::replace(&mut *state, WorkerJoinState::Joining)
                    else {
                        unreachable!("worker state was checked while locked");
                    };
                    #[cfg(test)]
                    crate::internal::test_wait_probe::announce("subscription_worker_join");
                    break worker;
                }
                WorkerJoinState::Joining => {
                    #[cfg(test)]
                    crate::internal::test_wait_probe::announce("subscription_worker_join_waiting");
                    state = self.worker_joined.wait(state).unwrap_or_else(PoisonError::into_inner);
                    drop(state);
                }
                WorkerJoinState::Joined { panicked } => return *panicked,
            }
        };

        let result = worker.join();
        let panicked = result.is_err();
        {
            let mut state = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
            *state = WorkerJoinState::Joined { panicked };
            self.worker_joined.notify_all();
        }
        if let Err(payload) = result {
            dispose_panic_payload(payload);
        }
        panicked
    }

    /// Requests worker cancellation without waiting for an executing handler.
    ///
    /// # Side Effects
    /// Publishes cancellation under the same short gate as actual handler
    /// admission. The gate is never held across user callbacks.
    pub(in crate::facade) fn request_cancel(&self) {
        let _start = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        #[cfg(test)]
        {
            let mut probe = self.cancellation_probe.lock().unwrap_or_else(PoisonError::into_inner);
            self.cancelled.store(true, Ordering::Release);
            if let Some(probe) = probe.as_mut() {
                probe.requested.get_or_insert_with(|| probe.started.elapsed());
            }
        }
        #[cfg(not(test))]
        self.cancelled.store(true, Ordering::Release);
        #[cfg(test)]
        crate::internal::test_wait_probe::announce("sync_cancel_requested");
    }

    /// Enables an opt-in trace for this subscription alone.
    #[cfg(test)]
    pub(crate) fn enable_cancellation_probe(&self) {
        *self.cancellation_probe.lock().unwrap_or_else(PoisonError::into_inner) = Some(CancellationProbe {
            started: Instant::now(),
            requested: None,
            receive_returns_after_request: Vec::new(),
            worker_finished: None,
        });
    }

    /// Records one SPI receive return after cancellation has been requested.
    #[cfg(test)]
    pub(crate) fn record_receive_return_after_cancel(&self) {
        if let Some(probe) = self
            .cancellation_probe
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            && probe.requested.is_some()
        {
            probe.receive_returns_after_request.push(probe.started.elapsed());
        }
    }

    /// Records a test-only receive poll override for this subscription.
    ///
    /// # Parameters
    /// - `interval`: Receive polling interval used by this subscription's test.
    #[cfg(test)]
    pub(crate) fn set_receive_poll_interval(&self, interval: Duration) {
        *self
            .receive_poll_interval
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(interval);
    }

    /// Returns the test-only poll override, if configured.
    ///
    /// # Returns
    /// The configured interval, or `None` when this subscription has no
    /// override.
    #[cfg(test)]
    pub(crate) fn receive_poll_interval_override(&self) -> Option<Duration> {
        *self
            .receive_poll_interval
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns the causal trace captured for this subscription.
    ///
    /// # Returns
    /// A snapshot of the cancellation trace, or `None` when tracing is
    /// disabled.
    #[cfg(test)]
    pub(crate) fn cancellation_probe_snapshot(&self) -> Option<CancellationProbeSnapshot> {
        self.cancellation_probe
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|probe| CancellationProbeSnapshot {
                requested: probe.requested,
                receive_returns_after_request: probe.receive_returns_after_request.clone(),
                worker_finished: probe.worker_finished,
            })
    }

    /// Stores the provider close failure for lifecycle callers to observe.
    ///
    /// # Parameters
    /// - `error`: canonical provider close failure to retain.
    pub(in crate::facade) fn record_close_error(&self, error: Arc<SubscriptionCloseFailure>) {
        let mut slot = self.close_error.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    /// Marks receiver cleanup complete and wakes concurrent cancellers.
    pub(in crate::facade) fn mark_finished(&self) {
        *self.finished.lock().unwrap_or_else(PoisonError::into_inner) = true;
        #[cfg(test)]
        if let Some(probe) = self
            .cancellation_probe
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            probe.worker_finished = Some(probe.started.elapsed());
        }
        self.finished_changed.notify_all();
    }

    /// Waits until the receiver worker has completed close and cleanup.
    ///
    /// This blocks only callers outside the worker's bus context.
    pub(in crate::facade) fn wait_finished(&self) {
        let mut finished = self.finished.lock().unwrap_or_else(PoisonError::into_inner);
        while !*finished {
            finished = self
                .finished_changed
                .wait(finished)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

#[cfg(test)]
mod worker_join_tests {
    use std::panic::AssertUnwindSafe;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use qubit_id::Id;

    use super::SubscriptionControl;
    use crate::model::SubscriberId;

    fn control() -> Arc<SubscriptionControl> {
        SubscriptionControl::new(
            Id::new(1),
            SubscriberId::new("worker-join-test").expect("valid subscriber ID"),
        )
    }

    #[test]
    fn test_concurrent_joiners_share_worker_panic() {
        let control = control();
        let (release_tx, release_rx) = mpsc::channel();
        control.set_worker(thread::spawn(move || {
            release_rx.recv().expect("test releases worker");
            panic!("controlled worker panic");
        }));

        let join_owner = crate::internal::test_wait_probe::install("subscription_worker_join");
        let owner_control = control.clone();
        let (results_tx, results_rx) = mpsc::channel();
        let owner = thread::spawn(move || {
            results_tx
                .send(owner_control.join_worker_panicked())
                .expect("results receiver is active");
        });
        join_owner
            .recv_timeout(Duration::from_secs(2))
            .expect("first caller took the join handle");

        let joining_waiter = crate::internal::test_wait_probe::install("subscription_worker_join_waiting");
        let waiting_control = control.clone();
        let (waiting_result_tx, waiting_result_rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            waiting_result_tx
                .send(waiting_control.join_worker_panicked())
                .expect("waiter result receiver is active");
        });
        joining_waiter
            .recv_timeout(Duration::from_secs(2))
            .expect("second caller waits for the active join");

        release_tx.send(()).expect("worker remains active");
        assert!(
            results_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("first join completes")
        );
        assert!(
            waiting_result_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("second join completes")
        );
        owner.join().expect("join owner exits");
        waiter.join().expect("join waiter exits");
    }

    struct PanickingDrop(Arc<AtomicUsize>);

    impl Drop for PanickingDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("panic payload destructor panic");
        }
    }

    #[test]
    fn test_join_contains_panic_payload_destructor_panic_and_caches_result() {
        let control = control();
        let drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = drops.clone();
        control.set_worker(thread::spawn(move || {
            std::panic::panic_any(PanickingDrop(payload_drops));
        }));

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| control.join_worker_panicked()));
        assert!(result.expect("payload destructor panic is contained"));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(control.join_worker_panicked());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
