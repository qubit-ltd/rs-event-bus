// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Reusable observers of one exact synchronous event bus shutdown attempt.
use std::future::poll_fn;
use std::io::Error;
use std::sync::Arc;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use crate::LifecycleError;
use crate::ShutdownError;
use crate::ShutdownReport;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::clone_spi_error;
use crate::facade::internal::ShutdownRegistration;
use crate::facade::internal::is_current_bus_context;
use crate::facade::shutdown_result::ShutdownResult;
use crate::spi::ShutdownOutcome;

/// A reusable observer of one shutdown generation.
/// Dropping this ticket releases its retained result, without stopping
/// shutdown. Each `wait_async` borrow owns an independent cancelable waker
/// registration.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let bus = EventBus::local(LocalEventBusConfig::default()).unwrap();
/// let shutdown = bus.request_shutdown(ShutdownMode::Immediate).unwrap();
/// let _report = shutdown.wait(None).unwrap();
/// ```
#[must_use = "Wait for shutdown completion or drop the observation ticket."]
pub struct EventBusShutdown {
    /// Shared bus state and coordinator retained while this ticket is alive.
    pub(super) inner: Arc<EventBusInner>,
    /// Exact shutdown attempt observed by this ticket, or `None` for a cached
    /// result after the bus has already closed.
    pub(super) generation: Option<u64>,
}
impl EventBusShutdown {
    /// Blocks until this generation completes or the observer timeout expires.
    /// A timeout does not cancel shutdown or consume this reusable ticket.
    /// Returns the shutdown report; errors preserve start, provider and close
    /// failures. Bus-owned callbacks/workers receive `WouldDeadlock`.
    ///
    /// # Parameters
    /// - `timeout`: Maximum duration to wait; `None` waits without a deadline.
    ///
    /// # Returns
    /// The report for this shutdown generation, with facade-known abandoned
    /// deliveries.
    ///
    /// # Errors
    /// Returns `WouldDeadlock` from a bus callback or worker, `TimedOut` when
    /// the observer deadline expires, or a coordinator-start, provider, or
    /// subscription-close failure.
    pub fn wait(&self, timeout: Option<Duration>) -> Result<ShutdownReport, ShutdownError> {
        if is_current_bus_context(Arc::as_ptr(&self.inner) as usize) {
            return Err(LifecycleError::WouldDeadlock {
                operation: "shutdown",
            }
            .into());
        }
        let Some(generation) = self.generation else {
            return self.report();
        };
        let deadline = timeout.and_then(|value| Instant::now().checked_add(value));
        let (timed_out, result) = self.inner.shutdown_coordinator.wait(generation, deadline);
        if timed_out {
            return Err(ShutdownError::TimedOut {
                timeout: timeout.expect("deadline requires timeout"),
            });
        }
        self.resolve(result)
    }

    /// Observes completion without blocking a thread or requiring a runtime.
    /// Dropping the returned future cancels only its waker registration. The
    /// ticket remains reusable and retains this exact generation's result.
    /// Returns the report or the generation's start/provider/close failure.
    ///
    /// # Returns
    /// The report for this shutdown generation, with facade-known abandoned
    /// deliveries.
    ///
    /// # Errors
    /// Returns a coordinator-start, provider, or subscription-close failure.
    pub async fn wait_async(&self) -> Result<ShutdownReport, ShutdownError> {
        let Some(generation) = self.generation else {
            return self.report();
        };
        let mut registration =
            ShutdownRegistration::new(&self.inner.shutdown_coordinator, generation);
        let result = poll_fn(|cx| registration.poll(cx)).await;
        self.resolve(result)
    }

    /// Converts a generation completion after releasing the coordinator lock.
    ///
    /// # Returns
    /// The completed generation's shutdown report.
    ///
    /// # Errors
    /// Returns the provider or coordinator start error when that generation
    /// failed or ended without a result; report lookup can return close errors.
    fn resolve(&self, result: Option<ShutdownResult>) -> Result<ShutdownReport, ShutdownError> {
        match result {
            Some(ShutdownResult::Provider(result)) => {
                let _ = result.map_err(clone_spi_error)?;
                self.report()
            }
            Some(ShutdownResult::StartFailed(error)) => Err(ShutdownError::CoordinatorStart(
                error.raw_os_error().map_or_else(
                    || Error::new(error.kind(), error.to_string()),
                    Error::from_raw_os_error,
                ),
            )),
            None => Err(ShutdownError::CoordinatorStart(Error::other(
                "shutdown generation ended without a result",
            ))),
        }
    }

    /// Reads the stable report and close errors without holding coordinator
    /// state.
    ///
    /// # Returns
    /// The stable report published for this shutdown generation.
    ///
    /// # Errors
    /// Returns the snapshot of provider subscription-close errors, if present.
    fn report(&self) -> Result<ShutdownReport, ShutdownError> {
        if let Some(errors) = self.inner.close_errors_snapshot() {
            return Err(ShutdownError::SubscriptionClose(errors));
        }
        Ok(self
            .inner
            .shutdown_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .report
            .unwrap_or(ShutdownReport::new(ShutdownOutcome::Complete, 0, false)))
    }
}
impl Drop for EventBusShutdown {
    /// Releases this ticket's retained generation result without stopping
    /// shutdown.
    fn drop(&mut self) {
        if let Some(generation) = self.generation {
            self.inner.shutdown_coordinator.release(generation);
        }
    }
}
