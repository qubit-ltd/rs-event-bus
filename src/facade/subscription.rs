// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! A cancellable handle for a facade-managed synchronous subscription.

mod internal;

use std::sync::Arc;

pub(crate) use internal::SubscriptionControl;
use qubit_id::Id;

use super::internal::is_current_bus_context;
use crate::error::LifecycleError;
use crate::error::SpiError;
use crate::error::SubscriptionCloseErrors;
use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;
use crate::model::SubscriberId;

/// Handle for one typed subscription managed by an [`super::EventBus`].
///
/// Dropping the handle does not cancel its worker. Call [`Self::cancel`] or
/// shut down the owning bus explicitly.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::Subscription;
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::model::SubscribeRequest;
/// use qubit_event_bus::model::Topic;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let bus = EventBus::local(LocalEventBusConfig::new()).unwrap();
/// let topic = Topic::<String>::new("orders.created").unwrap();
/// let subscription: Subscription = bus.subscribe(
///     SubscribeRequest::new("audit", topic).unwrap(), |_| {},
/// ).unwrap();
/// assert_eq!(subscription.subscriber_id().as_str(), "audit");
/// subscription.cancel().unwrap();
/// assert!(subscription.is_cancelled());
/// bus.shutdown(ShutdownMode::Immediate).unwrap();
/// ```
#[must_use = "dropping a subscription handle does not cancel it; call cancel() or shut down the bus"]
pub struct Subscription {
    /// Shared worker cancellation and completion state.
    control: Arc<SubscriptionControl>,
    /// Identity used to detect waits from this bus's own callbacks.
    bus_identity: usize,
    /// Scheduler used to release queued work when cancellation begins.
    scheduler: Arc<SyncDeliveryScheduler>,
}

impl Subscription {
    /// Creates a public handle for a successfully started subscription worker.
    ///
    /// # Parameters
    /// - `control`: worker state for the provider receiver.
    /// - `bus_identity`: identity used to recognize calls from this bus.
    /// - `scheduler`: scheduler whose queued work is released on cancellation.
    ///
    /// # Returns
    /// A handle that can cancel and join the worker.
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
    ///
    /// # Returns
    /// The provider token identity used by this subscription.
    #[must_use = "Use the returned id."]
    #[inline]
    pub fn id(&self) -> Id {
        self.control.id
    }

    /// Returns the caller-supplied logical subscriber identity.
    ///
    /// # Returns
    /// The validated logical subscriber name.
    #[must_use = "Use the returned subscriber id."]
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.control.subscriber_id
    }

    /// Returns whether cancellation has been requested.
    ///
    /// # Returns
    /// True after cancellation begins, otherwise false.
    #[must_use]
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.control.is_cancelled()
    }

    /// Stops receiving and waits for the worker to finish when called
    /// externally. Any worker owned by the same bus only requests
    /// cancellation and does not join, preventing cross-subscription join
    /// cycles.
    ///
    /// # Returns
    /// Success after worker cleanup, or immediately after cancellation from a
    /// callback owned by this bus.
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
    ///
    /// # Returns
    /// Success if no worker remains or the worker joined normally.
    ///
    /// # Errors
    /// Returns an SPI lifecycle error if the worker thread panicked.
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
