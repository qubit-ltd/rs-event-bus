// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared asynchronous provider, facade, and lifecycle state.

use crate::Diagnostic;
use crate::EventBusFacadeConfig;
use crate::ShutdownReport;
use crate::facade::PublishMetrics;
use crate::facade::async_event_bus::Arc;
use crate::facade::async_event_bus::AsyncAdmission;
use crate::facade::async_event_bus::AsyncShutdownDriver;
use crate::facade::async_event_bus::AsyncSignal;
use crate::facade::async_event_bus::AsyncTracker;
use crate::facade::async_event_bus::AtomicBool;
use crate::facade::async_event_bus::AtomicU64;
use crate::facade::async_event_bus::BusState;
use crate::facade::async_event_bus::HashMap;
use crate::facade::async_event_bus::Id;
use crate::facade::async_event_bus::Mutex;
use crate::facade::async_event_bus::Ordering;
use crate::facade::async_event_bus::SubscriptionCloseErrors;
use crate::facade::async_event_bus::SubscriptionCloseFailure;
use crate::facade::async_event_bus::Timer;
use crate::facade::async_event_bus::Weak;
use crate::facade::async_event_bus::internal::AsyncPublishGuard;
use crate::facade::async_event_bus::internal::AsyncSubscribeGuard;
use crate::facade::observer_entry::ObserverEntry;
use crate::model::ProviderId;
use crate::pipeline::AsyncOrderingLanes;
use crate::pipeline::DiagnosticObserver;
use crate::pipeline::PublisherPipeline;
use crate::spi::AsyncEventBusSpi;

/// State shared by every clone of an asynchronous event-bus facade.
pub(in crate::facade) struct AsyncEventBusInner {
    /// Provider implementation used for SPI operations.
    pub(in crate::facade) spi: Arc<dyn AsyncEventBusSpi>,
    /// Stable identifier used in diagnostics and provider errors.
    pub(in crate::facade) provider_id: ProviderId,
    /// Capabilities reported by the provider at construction.
    pub(in crate::facade) capabilities: crate::spi::EventBusCapabilities,
    /// Publisher middleware and retry pipeline.
    pub(in crate::facade) publisher: PublisherPipeline,
    /// Immutable facade middleware and codec configuration.
    pub(in crate::facade) facade_config: EventBusFacadeConfig,
    /// Current lifecycle state.
    pub(in crate::facade) state: Mutex<BusState>,
    /// Whether a shutdown caller currently owns shutdown work.
    pub(in crate::facade) shutdown_active: AtomicBool,
    /// Whether any caller has escalated shutdown to immediate mode.
    pub(in crate::facade) shutdown_immediate: AtomicBool,
    /// Wakes callers waiting for the active shutdown worker.
    pub(in crate::facade) shutdown_signal: AsyncSignal,
    /// Wakes provider shutdown when a caller requests immediate mode.
    pub(in crate::facade) shutdown_mode_signal: AsyncSignal,
    /// Cached report returned to callers after successful shutdown.
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
    /// Per-key ordering locks shared by subscription runners.
    pub(in crate::facade) ordering_lanes: AsyncOrderingLanes<()>,
    /// Bounds deliveries admitted for handler processing.
    pub(in crate::facade) admission: Arc<AsyncAdmission>,
    /// Runtime-neutral timer used for deadlines and delays.
    pub(in crate::facade) timer: Arc<dyn Timer>,
    /// Counters shared by facade clones.
    pub(in crate::facade) publish_metrics: PublishMetrics,
}

impl AsyncEventBusInner {
    /// Starts a publish operation while the facade is running.
    pub(in crate::facade) fn begin_publish(self: &Arc<Self>) -> Option<AsyncPublishGuard> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state != BusState::Running {
            return None;
        }
        self.tracker.publish_started();
        drop(state);
        Some(AsyncPublishGuard::after_start(self.tracker.clone()))
    }
    /// Starts a subscribe operation while the facade is running.
    pub(in crate::facade) fn begin_subscribe(self: &Arc<Self>) -> Option<AsyncSubscribeGuard> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
    pub(in crate::facade) fn observer_snapshot(&self) -> Vec<Arc<DiagnosticObserver>> {
        let mut observers = self.observers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        observers.retain(|entry| entry.strong_count() > 0);
        observers
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|entry| entry.active.load(Ordering::Acquire))
            .map(|entry| entry.callback.clone())
            .collect()
    }
    /// Sends a diagnostic to the currently registered observers.
    ///
    /// # Parameters
    /// - `diagnostic`: event delivered to active observers.
    pub(in crate::facade) fn emit(&self, diagnostic: &Diagnostic) {
        crate::pipeline::emit_diagnostic(&self.observer_snapshot(), diagnostic);
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
        subscriber_id: &crate::model::SubscriberId,
        error: crate::error::SpiError,
    ) -> Arc<SubscriptionCloseFailure> {
        if let Some(failure) = control.close_error() {
            return failure.clone();
        }
        let failure = Arc::new(SubscriptionCloseFailure::new(subscriber_id.clone(), error));
        let failure = control.store_close_error(failure);
        self.close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(failure.clone());
        *self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        failure
    }

    /// Adds a close failure reported by an unstarted subscription.
    ///
    /// # Parameters
    /// - `failure`: close failure to include in the bus aggregate.
    pub(in crate::facade) fn record_close_failure(&self, failure: Arc<SubscriptionCloseFailure>) {
        self.close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(failure);
        *self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// Returns a cached aggregate of recorded close failures, if any exist.
    pub(in crate::facade) fn close_errors_snapshot(&self) -> Option<Arc<SubscriptionCloseErrors>> {
        let failures = self
            .close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut snapshot = self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
