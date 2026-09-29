// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared provider ownership and lifecycle operations for the sync facade.

use crate::Diagnostic;
use crate::EventBusFacadeConfig;
use crate::ShutdownReport;
use crate::SpiError;
use crate::SubscriberId;
use crate::facade::LifecycleTracker;
use crate::facade::PublishMetrics;
use crate::facade::SubscriptionControl;
use crate::facade::event_bus::Arc;
use crate::facade::event_bus::AtomicU64;
use crate::facade::event_bus::HashMap;
use crate::facade::event_bus::Id;
use crate::facade::event_bus::Mutex;
use crate::facade::event_bus::OperationGate;
use crate::facade::event_bus::Ordering;
use crate::facade::event_bus::ShutdownState;
use crate::facade::event_bus::SubscriptionCloseErrors;
use crate::facade::event_bus::SubscriptionCloseFailure;
use crate::facade::event_bus::SubscriptionWorkerBudget;
use crate::facade::event_bus::Weak;
use crate::facade::event_bus::failure::panic_message;
use crate::facade::internal::BusContextGuard;
use crate::facade::internal::LifecycleState;
use crate::facade::observer_entry::ObserverEntry;
use crate::facade::shutdown_coordinator::ShutdownCoordinator;
use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;
use crate::model::ProviderId;
use crate::pipeline::DiagnosticObserver;
use crate::pipeline::PublisherPipeline;
use crate::pipeline::emit_diagnostic;
use crate::spi::EventBusSpi;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

pub(in crate::facade) struct EventBusInner {
    pub(in crate::facade) spi: Arc<dyn EventBusSpi>,
    pub(in crate::facade) capabilities: crate::spi::EventBusCapabilities,
    pub(in crate::facade) provider_id: ProviderId,
    pub(in crate::facade) publisher: PublisherPipeline,
    pub(in crate::facade) facade_config: EventBusFacadeConfig,
    pub(in crate::facade) lifecycle: Mutex<LifecycleState>,
    pub(in crate::facade) operations: OperationGate,
    pub(in crate::facade) tracker: LifecycleTracker,
    pub(in crate::facade) subscriptions: Mutex<HashMap<Id, Arc<SubscriptionControl>>>,
    pub(in crate::facade) close_errors: Mutex<Vec<Arc<SubscriptionCloseFailure>>>,
    pub(in crate::facade) close_error_snapshot: Mutex<Option<Arc<SubscriptionCloseErrors>>>,
    pub(in crate::facade) next_subscription_id: AtomicU64,
    pub(in crate::facade) observers: Mutex<Vec<Weak<ObserverEntry>>>,
    pub(in crate::facade) shutdown_gate: Mutex<ShutdownState>,
    pub(in crate::facade) shutdown_coordinator: ShutdownCoordinator,
    pub(in crate::facade) scheduler: Arc<SyncDeliveryScheduler>,
    pub(in crate::facade) subscription_worker_budget: Arc<SubscriptionWorkerBudget>,
    pub(in crate::facade) publish_metrics: PublishMetrics,
    pub(in crate::facade) abandoned_deliveries: AtomicU64,
}

impl EventBusInner {
    /// Returns an immutable snapshot of currently active observer callbacks.
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
    pub(in crate::facade) fn emit(&self, diagnostic: Diagnostic) {
        let observers = self.observer_snapshot();
        emit_diagnostic(&observers, &diagnostic);
    }

    /// Emits a non-fatal internal failure through the common observer path.
    pub(in crate::facade) fn emit_internal(&self, origin: &str, message: String) {
        self.emit(Diagnostic::InternalFailure {
            origin: origin.into(),
            message: message.into(),
        });
    }

    /// Returns a snapshot of active subscription worker controls.
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
                    source: Box::new(std::io::Error::other(panic_message(panic.as_ref()))),
                })
            });
        let failure_message = result.as_ref().err().map(ToString::to_string);
        self.shutdown_coordinator.finish(generation, result);
        if let Some(message) = failure_message {
            self.emit_internal("shutdown_coordinator", message);
        }
    }

    /// Waits for admitted calls and workers, then closes provider resources.
    pub(in crate::facade) fn perform_shutdown(&self, generation: u64) -> Result<ShutdownOutcome, SpiError> {
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
    pub(in crate::facade) fn shutdown_provider_once(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
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

/// Recreates a shared SPI error while retaining the original error in its
/// source chain for each concurrent shutdown caller.
pub(in crate::facade) fn clone_spi_error(error: Arc<SpiError>) -> SpiError {
    match error.as_ref() {
        SpiError::Operation {
            provider_id,
            operation,
            resource,
            kind,
            retryable,
            ..
        } => SpiError::Operation {
            provider_id: provider_id.clone(),
            operation,
            resource: resource.clone(),
            kind,
            retryable: *retryable,
            source: Box::new(error),
        },
        SpiError::InvalidSettlementToken {
            provider_id,
            operation,
            resource,
            reason,
            retryable,
            ..
        } => SpiError::InvalidSettlementToken {
            provider_id: provider_id.clone(),
            operation,
            resource: resource.clone(),
            reason,
            retryable: *retryable,
            source: Box::new(error),
        },
    }
}

/// Closes a subscription while converting provider panics into source errors.
pub(in crate::facade) fn close_spi_subscription(
    inner: &EventBusInner,
    subscriber_id: &SubscriberId,
    spi_subscription: &mut dyn crate::spi::EventSubscriptionSpi,
) -> Result<(), SpiError> {
    crate::spi::panic_boundary::catch_spi_call(
        inner.provider_id.as_str(),
        "close",
        Some(subscriber_id.as_str()),
        || spi_subscription.close(),
    )?
}
