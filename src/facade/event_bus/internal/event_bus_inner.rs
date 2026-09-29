// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared provider, lifecycle, observer, and worker state for facade clones.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use qubit_id::Id;

use super::OperationGate;
use super::ShutdownState;
use super::SubscriptionWorkerBudget;
use crate::error::SpiError;
use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;
use crate::facade::EventBusFacadeConfig;
use crate::facade::LifecycleTracker;
use crate::facade::PublishMetrics;
use crate::facade::ShutdownReport;
use crate::facade::SubscriptionControl;
use crate::facade::internal::BusContextGuard;
use crate::facade::internal::LifecycleState;
use crate::facade::observer_entry::ObserverEntry;
use crate::facade::shutdown_coordinator::ShutdownCoordinator;
use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;
use crate::model::ProviderId;
use crate::pipeline::Diagnostic;
use crate::pipeline::DiagnosticObserver;
use crate::pipeline::PublisherPipeline;
use crate::spi::EventBusSpi;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

/// Shared provider, lifecycle, observer, and worker state for facade clones.
pub(in crate::facade) struct EventBusInner {
    /// Provider implementation shared by facade clones.
    pub(in crate::facade) spi: Arc<dyn EventBusSpi>,
    /// Immutable capabilities captured at construction.
    pub(in crate::facade) capabilities: crate::spi::EventBusCapabilities,
    /// Provider identity used for errors and diagnostics.
    pub(in crate::facade) provider_id: ProviderId,
    /// Publisher validation, retry, and interception pipeline.
    pub(in crate::facade) publisher: PublisherPipeline,
    /// Immutable facade middleware and codec settings.
    pub(in crate::facade) facade_config: EventBusFacadeConfig,
    /// Facade lifecycle state.
    pub(in crate::facade) lifecycle: Mutex<LifecycleState>,
    /// Admission gate for publish and subscribe SPI calls.
    pub(in crate::facade) operations: OperationGate,
    /// Counts workers and received deliveries for shutdown waits.
    pub(in crate::facade) tracker: LifecycleTracker,
    /// Active subscription controls indexed by bus-local ID.
    pub(in crate::facade) subscriptions: Mutex<HashMap<Id, Arc<SubscriptionControl>>>,
    /// Receiver close failures recorded by subscription workers.
    pub(in crate::facade) close_errors: Mutex<Vec<Arc<SubscriptionCloseFailure>>>,
    /// Cached aggregate of receiver close failures.
    pub(in crate::facade) close_error_snapshot: Mutex<Option<Arc<SubscriptionCloseErrors>>>,
    /// Generates bus-local subscription identities.
    pub(in crate::facade) next_subscription_id: AtomicU64,
    /// Weak references to diagnostic observer registrations.
    pub(in crate::facade) observers: Mutex<Vec<Weak<ObserverEntry>>>,
    /// Shared shutdown report cache.
    pub(in crate::facade) shutdown_gate: Mutex<ShutdownState>,
    /// Owns shutdown attempt generations, deadlines, and outcomes.
    pub(in crate::facade) shutdown_coordinator: ShutdownCoordinator,
    /// Bounded handler task scheduler shared across subscriptions.
    pub(in crate::facade) scheduler: Arc<SyncDeliveryScheduler>,
    /// Bounds the number of managed subscription worker threads.
    pub(in crate::facade) subscription_worker_budget: Arc<SubscriptionWorkerBudget>,
    /// Shared publication counters.
    pub(in crate::facade) publish_metrics: PublishMetrics,
    /// Count of facade-known ephemeral deliveries abandoned during shutdown.
    pub(in crate::facade) abandoned_deliveries: AtomicU64,
}

impl EventBusInner {
    /// Returns an immutable snapshot of currently active observer callbacks.
    ///
    /// # Returns
    /// Strong callback owners retained for one diagnostic emission.
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

    /// Emits a diagnostic with observer panic isolation and no registry lock
    /// held.
    ///
    /// # Parameters
    /// - `diagnostic`: event delivered to active observer callbacks.
    pub(in crate::facade) fn emit(&self, diagnostic: Diagnostic) {
        crate::pipeline::emit_diagnostic(&self.observer_snapshot(), &diagnostic);
    }

    /// Emits a non-fatal internal failure through the common observer path.
    ///
    /// # Parameters
    /// - origin: stable name of the operation that encountered the failure.
    /// - message: human-readable failure detail.
    pub(in crate::facade) fn emit_internal(&self, origin: &str, message: String) {
        self.emit(Diagnostic::InternalFailure {
            origin: origin.into(),
            message: message.into(),
        });
    }

    /// Returns a snapshot of active subscription worker controls.
    ///
    /// # Returns
    /// Strong references to all currently registered controls.
    #[must_use = "Use the returned query result."]
    pub(in crate::facade) fn subscription_snapshot(&self) -> Vec<Arc<SubscriptionControl>> {
        self.subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    /// Returns a stable aggregate snapshot of every worker close failure so
    /// far.
    ///
    /// # Returns
    /// Some with all failures, or None if no close failure was recorded.
    pub(in crate::facade) fn close_errors_snapshot(&self) -> Option<Arc<SubscriptionCloseErrors>> {
        let mut snapshot = self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(errors) = snapshot.as_ref() {
            return Some(errors.clone());
        }
        let failures = self
            .close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failures.is_empty() {
            return None;
        }
        let errors = Arc::new(SubscriptionCloseErrors::from_failures(failures.clone()));
        *snapshot = Some(errors.clone());
        Some(errors)
    }

    /// Joins one worker after the caller has established it has finished.
    ///
    /// # Parameters
    /// - `control`: finished worker control whose thread is joined.
    ///
    /// # Returns
    /// Success when no worker remains or its thread joined normally.
    ///
    /// # Errors
    /// Returns an SPI error when the worker thread panicked.
    pub(in crate::facade) fn join_control(&self, control: &Arc<SubscriptionControl>) -> Result<(), SpiError> {
        let worker = control
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            worker.join().map_err(|_| SpiError::Operation {
                provider_id: self.provider_id.as_str().into(),
                operation: "subscription_worker",
                resource: Some(control.subscriber_id.as_str().into()),
                kind: "worker_panicked",
                retryable: None,
                source: Box::new(std::io::Error::other("subscription worker panicked")),
            })?;
        }
        Ok(())
    }

    /// Completes one shutdown attempt on the dedicated coordinator thread.
    ///
    /// # Parameters
    /// - `generation`: shutdown attempt generation to complete.
    pub(in crate::facade) fn run_shutdown(self: Arc<Self>, generation: u64) {
        let bus_identity = Arc::as_ptr(&self) as usize;
        let _context = BusContextGuard::enter(bus_identity);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.perform_shutdown(generation)))
            .unwrap_or_else(|panic| {
                Err(SpiError::Operation {
                    provider_id: self.provider_id.as_str().into(),
                    operation: "shutdown_coordinator",
                    resource: None,
                    kind: "coordinator_panicked",
                    retryable: None,
                    source: Box::new(std::io::Error::other(crate::facade::event_bus::failure::panic_message(
                        panic.as_ref(),
                    ))),
                })
            });
        let failure_message = result.as_ref().err().map(ToString::to_string);
        self.shutdown_coordinator.finish(generation, result);
        if let Some(message) = failure_message {
            self.emit_internal("shutdown_coordinator", message);
        }
    }

    /// Waits for admitted calls and workers, then closes provider resources.
    ///
    /// # Parameters
    /// - `generation`: shutdown generation whose requested mode is applied.
    ///
    /// # Returns
    /// The provider shutdown outcome.
    ///
    /// # Errors
    /// Returns worker join or provider shutdown failures.
    fn perform_shutdown(&self, generation: u64) -> Result<ShutdownOutcome, SpiError> {
        self.operations.wait_for_idle();
        let controls = self.subscription_snapshot();
        self.scheduler.stop_admission(matches!(
            self.shutdown_coordinator.mode(generation),
            ShutdownMode::Immediate
        ));
        for control in &controls {
            control.request_cancel();
        }
        self.tracker.wait_for_workers(None);
        for control in &controls {
            self.join_control(control)?;
        }
        self.scheduler.join();
        let mode = self.shutdown_coordinator.mode(generation);
        let outcome = self.shutdown_provider_once(mode)?;
        if self.tracker.workers_are_idle() {
            *self.lifecycle.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = LifecycleState::Closed;
        }
        Ok(outcome)
    }

    /// Shuts down the provider once and caches its successful outcome.
    ///
    /// # Parameters
    /// - `mode`: shutdown policy passed to the provider.
    ///
    /// # Returns
    /// The provider shutdown outcome.
    ///
    /// # Errors
    /// Returns a structured provider shutdown error.
    fn shutdown_provider_once(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        let mut state = self
            .shutdown_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(report) = state.report {
            return Ok(report.outcome);
        }
        let outcome = crate::spi::panic_boundary::catch_spi_call(self.provider_id.as_str(), "shutdown", None, || {
            self.spi.shutdown(mode)
        })??;
        state.report = Some(ShutdownReport::new(
            outcome,
            self.abandoned_deliveries.load(Ordering::Acquire),
            self.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral
                || outcome == ShutdownOutcome::TimedOut,
        ));
        Ok(outcome)
    }
}
