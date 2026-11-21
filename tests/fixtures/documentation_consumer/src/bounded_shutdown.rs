// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Two bounded waits hand unresolved cleanup back to the application
//! supervisor.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusShutdown;
use qubit_event_bus::ShutdownReport;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::spi::ShutdownMode;

/// Waits twice for graceful completion, without forcing running handlers to
/// stop.
///
/// Retries once only when the caller's first deadline expires. Provider
/// reports, including `ShutdownOutcome::TimedOut`, are returned unchanged.
///
/// # Returns
///
/// The complete report from the first non-timeout result.
///
/// # Errors
///
/// Returns the second timeout, or any shutdown error other than a timeout.
pub fn try_shutdown(bus: &EventBus) -> Result<ShutdownReport, ShutdownError> {
    retry_shutdown_with(Duration::from_secs(2), Duration::from_secs(1), |timeout| {
        bus.shutdown(ShutdownMode::Graceful { timeout })
    })
}

fn retry_shutdown_with<F>(
    first_timeout: Duration,
    retry_timeout: Duration,
    mut shutdown: F,
) -> Result<ShutdownReport, ShutdownError>
where
    F: FnMut(Duration) -> Result<ShutdownReport, ShutdownError>,
{
    match shutdown(first_timeout) {
        Ok(report) => Ok(report),
        Err(ShutdownError::TimedOut { .. }) => shutdown(retry_timeout),
        Err(error) => Err(error),
    }
}

/// Applies the same bounded policy while the caller drives async shutdown.
///
/// Retries once only when the caller's first deadline expires. Provider
/// reports, including `ShutdownOutcome::TimedOut`, are returned unchanged.
/// Dropping this future stops the current cleanup driver. To
/// continue cleanup, the application must call `AsyncEventBus::shutdown`
/// again and keep polling that future; async shutdown has no background
/// coordinator.
///
/// # Returns
///
/// The complete report from the first non-timeout result.
///
/// # Errors
///
/// Returns the second timeout, or any shutdown error other than a timeout.
pub async fn try_shutdown_async(bus: &AsyncEventBus) -> Result<ShutdownReport, ShutdownError> {
    retry_shutdown_async_with(Duration::from_secs(2), Duration::from_secs(1), |timeout| {
        Box::pin(bus.shutdown(ShutdownMode::Graceful { timeout }))
    })
    .await
}

async fn retry_shutdown_async_with<'a, F>(
    first_timeout: Duration,
    retry_timeout: Duration,
    mut shutdown: F,
) -> Result<ShutdownReport, ShutdownError>
where
    F: FnMut(Duration) -> Pin<Box<dyn Future<Output = Result<ShutdownReport, ShutdownError>> + 'a>>,
{
    match shutdown(first_timeout).await {
        Ok(report) => Ok(report),
        Err(ShutdownError::TimedOut { .. }) => shutdown(retry_timeout).await,
        Err(error) => Err(error),
    }
}

/// Observes an IoC-managed shutdown ticket and preserves wait errors.
pub async fn observe_shutdown_ticket(
    bus: &EventBus,
    ticket: &EventBusShutdown,
) -> Result<ShutdownReport, ShutdownError> {
    match ticket.wait_async().await {
        Ok(report) => Ok(report),
        Err(error) => {
            if let Some(report) = bus.shutdown_report() {
                eprintln!(
                    "provider outcome={:?}, known abandoned={}, provider uncertainty={}: {report:?}",
                    report.outcome, report.known_abandoned_deliveries, report.provider_may_have_abandoned_deliveries,
                );
            }
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests;
