// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Two bounded waits hand unresolved cleanup back to the application supervisor.

use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

/// Waits twice for graceful completion, without forcing running handlers to stop.
///
/// Returns `false` after both waits expire or the provider reports incomplete
/// shutdown. The application should record metrics and hand control to its
/// external supervisor. Errors other than a caller deadline are propagated.
pub fn try_shutdown(bus: &EventBus) -> Result<bool, ShutdownError> {
    match bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(2),
    }) {
        Ok(report) if report.outcome == ShutdownOutcome::Complete => return Ok(true),
        Ok(_) | Err(ShutdownError::TimedOut { .. }) => {}
        Err(error) => return Err(error),
    }
    match bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(1),
    }) {
        Ok(report) => Ok(report.outcome == ShutdownOutcome::Complete),
        Err(ShutdownError::TimedOut { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Applies the same bounded policy while the caller drives async shutdown.
///
/// Returns `false` if cleanup remains incomplete; other shutdown errors propagate.
/// Cancelling this future leaves shutdown state for the coordinator to resume.
pub async fn try_shutdown_async(bus: &AsyncEventBus) -> Result<bool, ShutdownError> {
    match bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(2),
        })
        .await
    {
        Ok(report) if report.outcome == ShutdownOutcome::Complete => return Ok(true),
        Ok(_) | Err(ShutdownError::TimedOut { .. }) => {}
        Err(error) => return Err(error),
    }
    match bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        })
        .await
    {
        Ok(report) => Ok(report.outcome == ShutdownOutcome::Complete),
        Err(ShutdownError::TimedOut { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}
