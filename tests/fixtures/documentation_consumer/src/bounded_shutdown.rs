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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use qubit_event_bus::error::LifecycleError;
    use qubit_event_bus::spi::ShutdownOutcome;

    use super::ShutdownError;
    use super::ShutdownReport;
    use super::retry_shutdown_async_with;
    use super::retry_shutdown_with;

    fn shutdown_report(outcome: ShutdownOutcome) -> ShutdownReport {
        ShutdownReport {
            outcome,
            known_abandoned_deliveries: 7,
            provider_may_have_abandoned_deliveries: true,
        }
    }

    #[test]
    fn test_sync_retry_policy_preserves_reports_and_retries_only_caller_timeout() {
        let timed_out_report = shutdown_report(ShutdownOutcome::TimedOut);
        let mut calls = 0;
        let report = retry_shutdown_with(Duration::from_secs(2), Duration::from_secs(1), |_| {
            calls += 1;
            Ok(timed_out_report)
        })
        .expect("provider report");
        assert_eq!(calls, 1, "provider TimedOut is a terminal report");
        assert_eq!(report, timed_out_report, "all report and abandonment fields survive");

        let mut calls = 0;
        let report = retry_shutdown_with(Duration::from_secs(2), Duration::from_secs(1), |_| {
            calls += 1;
            Ok(shutdown_report(ShutdownOutcome::Complete))
        })
        .expect("first attempt succeeds");
        assert_eq!(calls, 1);
        assert_eq!(report.outcome, ShutdownOutcome::Complete);

        let mut calls = 0;
        let report = retry_shutdown_with(Duration::from_secs(2), Duration::from_secs(1), |wait| {
            calls += 1;
            if calls == 1 {
                assert_eq!(wait, Duration::from_secs(2));
                Err(ShutdownError::TimedOut { timeout: wait })
            } else {
                assert_eq!(wait, Duration::from_secs(1));
                Ok(shutdown_report(ShutdownOutcome::Complete))
            }
        })
        .expect("retry succeeds");
        assert_eq!(calls, 2);
        assert_eq!(report.outcome, ShutdownOutcome::Complete);

        let mut calls = 0;
        let error = retry_shutdown_with(Duration::from_secs(2), Duration::from_secs(1), |wait| {
            calls += 1;
            Err(ShutdownError::TimedOut { timeout: wait })
        })
        .expect_err("both caller deadlines expire");
        assert_eq!(calls, 2);
        assert!(matches!(error, ShutdownError::TimedOut { timeout } if timeout == Duration::from_secs(1)));

        let mut calls = 0;
        let error = retry_shutdown_with(Duration::from_secs(2), Duration::from_secs(1), |_| {
            calls += 1;
            Err(ShutdownError::Lifecycle(LifecycleError::Closed))
        })
        .expect_err("non-timeout failure propagates");
        assert_eq!(calls, 1);
        assert!(matches!(error, ShutdownError::Lifecycle(LifecycleError::Closed)));
    }

    #[tokio::test]
    async fn test_async_retry_policy_preserves_reports_and_retries_only_caller_timeout() {
        let timed_out_report = shutdown_report(ShutdownOutcome::TimedOut);
        let mut calls = 0;
        let report = retry_shutdown_async_with(Duration::from_secs(2), Duration::from_secs(1), |_| {
            calls += 1;
            Box::pin(async move { Ok(timed_out_report) })
        })
        .await
        .expect("provider report");
        assert_eq!(calls, 1);
        assert_eq!(report, timed_out_report);

        let mut calls = 0;
        let report = retry_shutdown_async_with(Duration::from_secs(2), Duration::from_secs(1), |wait| {
            calls += 1;
            Box::pin(async move {
                if calls == 1 {
                    Err(ShutdownError::TimedOut { timeout: wait })
                } else {
                    Ok(shutdown_report(ShutdownOutcome::Complete))
                }
            })
        })
        .await
        .expect("retry succeeds");
        assert_eq!(calls, 2);
        assert_eq!(report.outcome, ShutdownOutcome::Complete);

        let mut calls = 0;
        let error = retry_shutdown_async_with(Duration::from_secs(2), Duration::from_secs(1), |wait| {
            calls += 1;
            Box::pin(async move { Err(ShutdownError::TimedOut { timeout: wait }) })
        })
        .await
        .expect_err("both caller deadlines expire");
        assert_eq!(calls, 2);
        assert!(matches!(error, ShutdownError::TimedOut { timeout } if timeout == Duration::from_secs(1)));

        let mut calls = 0;
        let error = retry_shutdown_async_with(Duration::from_secs(2), Duration::from_secs(1), |_| {
            calls += 1;
            Box::pin(async { Err(ShutdownError::Lifecycle(LifecycleError::Closed)) })
        })
        .await
        .expect_err("non-timeout failure propagates");
        assert_eq!(calls, 1);
        assert!(matches!(error, ShutdownError::Lifecycle(LifecycleError::Closed)));
    }
}
