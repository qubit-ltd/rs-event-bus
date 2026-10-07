// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared asynchronous facade state and its admission and diagnostic methods.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_clock::Timer;
use qubit_id::Id;

use super::AsyncPublishGuard;
use super::AsyncSignal;
use super::AsyncSubscribeGuard;
use super::AsyncTracker;
use super::async_shutdown_driver::AsyncShutdownDriver;
use super::bus_state::BusState;
use crate::error::SpiError;
use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;
use crate::facade::event_bus_facade_config::EventBusFacadeConfig;
use crate::facade::internal::DeliveryMetrics;
use crate::facade::internal::DeliverySchedulerCore;
use crate::facade::observer_entry::ObserverEntry;
use crate::facade::publish_metrics::PublishMetrics;
use crate::facade::shutdown_report::ShutdownReport;
use crate::model::ProviderId;
use crate::model::SubscriberId;
use crate::pipeline::Diagnostic;
use crate::pipeline::DiagnosticObserver;
use crate::pipeline::PublisherPipeline;
use crate::pipeline::emit_diagnostic;
use crate::spi::AsyncEventBusSpi;
use crate::spi::EventBusCapabilities;

/// Shared provider, lifecycle, and pipeline state used by every facade clone.
pub(in crate::facade) struct AsyncEventBusInner {
    /// Provider implementation used for asynchronous SPI operations.
    pub(in crate::facade) spi: Arc<dyn AsyncEventBusSpi>,
    /// Stable identifier used in diagnostics and provider errors.
    pub(in crate::facade) provider_id: ProviderId,
    /// Capabilities reported by the provider at construction.
    pub(in crate::facade) capabilities: EventBusCapabilities,
    /// Publisher middleware and retry pipeline shared by facade clones.
    pub(in crate::facade) publisher: PublisherPipeline,
    /// Immutable middleware, codec, and delivery configuration.
    pub(in crate::facade) facade_config: EventBusFacadeConfig,
    /// Lifecycle state guarding new facade operations.
    pub(in crate::facade) state: Mutex<BusState>,
    /// Whether a shutdown caller currently owns shutdown work.
    pub(in crate::facade) shutdown_active: AtomicBool,
    /// Whether any caller has escalated shutdown to immediate mode.
    pub(in crate::facade) shutdown_immediate: AtomicBool,
    /// Wakes callers waiting for the active shutdown worker.
    pub(in crate::facade) shutdown_signal: AsyncSignal,
    /// Wakes provider shutdown when a caller requests immediate mode.
    pub(in crate::facade) shutdown_mode_signal: AsyncSignal,
    /// Cached report returned after provider shutdown succeeds.
    pub(in crate::facade) shutdown_report: Mutex<Option<ShutdownReport>>,
    /// Count of facade-known deliveries abandoned during shutdown.
    pub(in crate::facade) abandoned_deliveries: AtomicU64,
    /// Generates provider subscription identifiers.
    pub(in crate::facade) next_subscription_id: AtomicU64,
    /// Active or unstarted subscription shutdown controls.
    pub(in crate::facade) controls: Mutex<HashMap<Id, Arc<dyn AsyncShutdownDriver>>>,
    /// Subscription receiver close failures accumulated during cleanup.
    pub(in crate::facade) close_errors: Mutex<Vec<Arc<SubscriptionCloseFailure>>>,
    /// Cached aggregate view of receiver close failures.
    pub(in crate::facade) close_error_snapshot: Mutex<Option<Arc<SubscriptionCloseErrors>>>,
    /// Weak references to registered diagnostic observers.
    pub(in crate::facade) observers: Mutex<Vec<Weak<ObserverEntry>>>,
    /// Tracks in-flight publishes, subscriptions, and runners.
    pub(in crate::facade) tracker: Arc<AsyncTracker>,
    /// Shared bounded receive and handler scheduler.
    pub(in crate::facade) scheduler: Arc<DeliverySchedulerCore>,
    /// Runtime-neutral timer used for deadlines and delays.
    pub(in crate::facade) timer: Arc<dyn Timer>,
    /// Fixed-size delivery lifecycle counters, shared by all subscriptions.
    pub(in crate::facade) delivery_metrics: Arc<DeliveryMetrics>,
    /// Publication counters shared by facade clones.
    pub(in crate::facade) publish_metrics: PublishMetrics,
}

impl AsyncEventBusInner {
    /// Routes coalesced scheduler wakeups after releasing the scheduler lock.
    pub(in crate::facade) fn notify_scheduler(&self) {
        let ids = self.scheduler.take_notifications();
        let controls = self.controls.lock().unwrap_or_else(PoisonError::into_inner);
        let targets: Vec<_> = ids
            .iter()
            .filter_map(|id| controls.get(id).cloned())
            .collect();
        drop(controls);
        for control in targets {
            control.notify();
        }
    }

    /// Starts a publish operation while the facade is running.
    ///
    /// # Returns
    /// A guard that releases the publish counter, or None after shutdown
    /// starts.
    #[must_use]
    pub(in crate::facade) fn begin_publish(self: &Arc<Self>) -> Option<super::AsyncPublishGuard> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if *state != BusState::Running {
            return None;
        }
        self.tracker.publish_started();
        drop(state);
        Some(AsyncPublishGuard::after_start(self.tracker.clone()))
    }

    /// Starts a subscribe operation while the facade is running.
    ///
    /// # Returns
    /// A guard that releases the subscribe counter, or None after shutdown
    /// starts.
    #[must_use]
    pub(in crate::facade) fn begin_subscribe(
        self: &Arc<Self>,
    ) -> Option<super::AsyncSubscribeGuard> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if *state != BusState::Running {
            return None;
        }
        self.tracker.subscribe_started();
        drop(state);
        Some(AsyncSubscribeGuard::after_start(self.tracker.clone()))
    }

    /// Returns the currently active diagnostic observer callbacks.
    ///
    /// # Returns
    /// Strong references to active callbacks for one emission pass.
    #[must_use]
    pub(in crate::facade) fn observer_snapshot(&self) -> Vec<Arc<DiagnosticObserver>> {
        let mut observers = self
            .observers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        observers.retain(|entry| entry.strong_count() > 0);
        observers
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|entry| entry.active.load(Ordering::Acquire))
            .map(|entry| entry.callback.clone())
            .collect()
    }

    /// Sends a diagnostic to currently registered observers.
    ///
    /// # Parameters
    /// - `diagnostic`: event delivered to active observers.
    pub(in crate::facade) fn emit(&self, diagnostic: &Diagnostic) {
        emit_diagnostic(&self.observer_snapshot(), diagnostic);
    }

    /// Records the first close failure for a subscription control.
    ///
    /// # Parameters
    /// - `control`: subscription retaining the canonical failure.
    /// - `subscriber_id`: identity associated with the provider receiver.
    /// - `error`: provider error reported while closing the receiver.
    ///
    /// # Returns
    /// The canonical failure retained by the subscription and bus.
    pub(in crate::facade) fn record_close_error(
        &self,
        control: &dyn AsyncShutdownDriver,
        subscriber_id: &SubscriberId,
        error: SpiError,
    ) -> Arc<SubscriptionCloseFailure> {
        if let Some(failure) = control.close_error() {
            return failure.clone();
        }
        let failure = Arc::new(SubscriptionCloseFailure::new(subscriber_id.clone(), error));
        let failure = control.store_close_error(failure);
        self.close_errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(failure.clone());
        *self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        failure
    }

    /// Adds a close failure reported by an unstarted subscription.
    ///
    /// # Parameters
    /// - `failure`: close failure to include in the bus aggregate.
    pub(in crate::facade) fn record_close_failure(&self, failure: Arc<SubscriptionCloseFailure>) {
        self.close_errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(failure);
        *self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// Returns a cached aggregate of recorded close failures, if any exist.
    ///
    /// # Returns
    /// Some with all close failures recorded so far, or None when there are
    /// none.
    #[must_use]
    pub(in crate::facade) fn close_errors_snapshot(&self) -> Option<Arc<SubscriptionCloseErrors>> {
        let failures = self
            .close_errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut snapshot = self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(errors) = snapshot.as_ref() {
            return Some(errors.clone());
        }
        if failures.is_empty() {
            return None;
        }
        let errors = Arc::new(SubscriptionCloseErrors::from_failures(failures.clone()));
        *snapshot = Some(errors.clone());
        Some(errors)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use crate::facade::AsyncEventBus;
    use crate::facade::DeliverySchedulingConfig;
    use crate::facade::EventBusFacadeConfig;
    use crate::local::AsyncLocalEventBusSpi;
    use crate::model::ProviderId;
    use crate::model::SubscribeRequest;
    use crate::model::Topic;

    /// Drives a synchronous-ready local-provider operation without a runtime.
    ///
    /// # Type Parameters
    /// - `F`: Local operation expected to complete in a bounded number of
    ///   polls.
    ///
    /// # Parameters
    /// - `future`: Operation owned and polled by this helper.
    ///
    /// # Returns
    /// The operation result.
    ///
    /// # Panics
    /// Panics if the local fixture needs more than 64 polls.
    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        for _ in 0..64 {
            if let Poll::Ready(result) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                return result;
            }
        }
        panic!("local operation did not become ready within bounded polls");
    }

    /// Retained closed handles cannot turn the live bus registry into a history
    /// map.
    #[test]
    fn test_async_registry_returns_to_baseline_across_1000_sessions() {
        let limit = NonZeroUsize::new(1).expect("one live session");
        let config = EventBusFacadeConfig::new().with_delivery_scheduling(
            DeliverySchedulingConfig::new(limit, limit, limit, limit).expect("limits"),
        );
        let provider =
            Arc::new(AsyncLocalEventBusSpi::new(&Default::default()).expect("local provider"));
        let bus = AsyncEventBus::with_config(
            ProviderId::new("local").expect("provider"),
            provider,
            config,
        )
        .expect("bus");
        let topic = Topic::<u32>::new("session-registry").expect("topic");
        let mut closed_handles = Vec::new();
        for index in 0..1000 {
            let request =
                SubscribeRequest::new(&format!("session-{index}"), topic.clone()).expect("request");
            let mut subscription =
                ready(bus.subscribe(request)).expect("slot was released by prior close");
            assert_eq!(bus.inner.controls.lock().expect("controls").len(), 1);
            let mut run = Box::pin(subscription.run(|_| async { Ok(()) }));
            assert!(
                run.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(run);
            ready(subscription.close()).expect("close");
            assert!(bus.inner.controls.lock().expect("controls").is_empty());
            assert_eq!(subscription.delivery_metrics().metrics, Default::default());
            closed_handles.push(subscription);
        }
        assert_eq!(closed_handles.len(), 1000);
        assert_eq!(bus.delivery_metrics(), Default::default());
    }
}
