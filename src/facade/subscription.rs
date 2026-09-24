// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! A cancellable handle for a facade-managed synchronous subscription.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use qubit_id::Id;

use crate::error::LifecycleError;
use crate::error::SpiError;
use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;
use crate::facade::is_current_bus_context;
use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;
use crate::model::SubscriberId;

/// A single subscription worker's coordination state.
pub(crate) struct SubscriptionControl {
    pub(super) id: Id,
    pub(super) subscriber_id: SubscriberId,
    cancelled: AtomicBool,
    pub(super) worker: Mutex<Option<JoinHandle<()>>>,
    pub(super) close_error: Mutex<Option<Arc<SubscriptionCloseFailure>>>,
    finished: Mutex<bool>,
    finished_changed: Condvar,
}

impl SubscriptionControl {
    /// Creates an active control block before its worker thread is spawned.
    pub(crate) fn new(id: Id, subscriber_id: SubscriberId) -> Arc<Self> {
        Arc::new(Self {
            id,
            subscriber_id,
            cancelled: AtomicBool::new(false),
            worker: Mutex::new(None),
            close_error: Mutex::new(None),
            finished: Mutex::new(false),
            finished_changed: Condvar::new(),
        })
    }

    /// Publishes the worker join handle after a successful thread spawn.
    pub(crate) fn set_worker(&self, worker: JoinHandle<()>) {
        *self.worker.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
    }

    /// Returns whether cancellation was requested for this subscription.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Requests worker cancellation without waiting for an executing handler.
    pub(crate) fn request_cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Stores a provider close failure for the next lifecycle caller to
    /// observe.
    pub(crate) fn record_close_error(&self, error: Arc<SubscriptionCloseFailure>) {
        let mut slot = self
            .close_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    /// Marks the receiver worker fully closed and wakes concurrent cancellers.
    pub(crate) fn mark_finished(&self) {
        *self.finished.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.finished_changed.notify_all();
    }

    /// Waits until the receiver worker has completed close and cleanup.
    fn wait_finished(&self) {
        let mut finished = self.finished.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*finished {
            finished = self
                .finished_changed
                .wait(finished)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

/// Handle for one typed subscription managed by an [`super::EventBus`].
///
/// Dropping the handle does not cancel its worker. Call [`Self::cancel`] or
/// shut down the owning bus explicitly.
pub struct Subscription {
    control: Arc<SubscriptionControl>,
    bus_identity: usize,
    scheduler: Arc<SyncDeliveryScheduler>,
}

impl Subscription {
    /// Creates a public handle for a successfully started subscription worker.
    pub(super) fn new(
        control: Arc<SubscriptionControl>,
        bus_identity: usize,
        scheduler: Arc<SyncDeliveryScheduler>,
    ) -> Self {
        Self {
            control,
            bus_identity,
            scheduler,
        }
    }

    /// Returns the bus-local object ID, distinct from the logical subscriber
    /// ID.
    pub fn id(&self) -> Id {
        self.control.id
    }

    /// Returns the caller-supplied logical subscriber identity.
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.control.subscriber_id
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.control.is_cancelled()
    }

    /// Stops receiving and waits for the worker to finish when called
    /// externally. Any worker owned by the same bus only requests
    /// cancellation and does not join, preventing cross-subscription join
    /// cycles.
    ///
    /// # Errors
    /// Returns [`LifecycleError::SubscriptionClose`] if the worker cannot close
    /// its provider subscription. A handler calling this method from any worker
    /// owned by the same bus requests cancellation without joining a worker.
    pub fn cancel(&self) -> Result<(), LifecycleError> {
        self.scheduler.cancel_subscription(self.control.id);
        self.control.request_cancel();
        if is_current_bus_context(self.bus_identity) {
            return Ok(());
        }
        self.join_worker()?;
        self.control.wait_finished();
        if let Some(error) = self
            .control
            .close_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            let errors = Arc::new(SubscriptionCloseErrors::from_failures(vec![error]));
            return Err(LifecycleError::SubscriptionClose(errors));
        }
        Ok(())
    }

    /// Joins the worker when it has not already been joined.
    fn join_worker(&self) -> Result<(), LifecycleError> {
        let worker = self
            .control
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            worker.join().map_err(|_| {
                LifecycleError::Spi(SpiError::Operation {
                    provider_id: "event-bus".into(),
                    operation: "subscription_worker",
                    resource: Some(self.control.subscriber_id.as_str().into()),
                    kind: "worker_panicked",
                    retryable: None,
                    source: Box::new(std::io::Error::other("subscription worker panicked")),
                })
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use qubit_id::Id;

    use super::SubscriptionControl;
    use crate::model::SubscriberId;

    #[test]
    fn test_wait_finished_observes_worker_completion() {
        let control = SubscriptionControl::new(
            Id::new(1),
            SubscriberId::new("wait-finished-test").expect("valid subscriber ID"),
        );
        let worker_control = control.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).expect("test receiver is active");
            release_rx.recv().expect("test releases worker completion");
            worker_control.mark_finished();
        });
        started_rx.recv().expect("worker reached the completion gate");

        release_tx.send(()).expect("worker remains active");
        control.wait_finished();

        worker.join().expect("completion worker exits cleanly");
    }
}
