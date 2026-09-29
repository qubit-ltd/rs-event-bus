// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Graceful and immediate shutdown failures.

use std::sync::Arc;
use std::time::Duration;

use crate::error::LifecycleError;
use crate::error::SpiError;
use crate::error::SubscriptionCloseErrors;

/// The event bus could not finish its requested shutdown.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use qubit_event_bus::error::ShutdownError;
///
/// let error = ShutdownError::TimedOut { timeout: Duration::from_secs(1) };
/// assert!(matches!(error, ShutdownError::TimedOut { .. }));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum ShutdownError {
    /// The graceful shutdown deadline elapsed.
    #[error("event bus shutdown timed out after {timeout:?}")]
    TimedOut {
        /// Grace period that elapsed.
        timeout: Duration,
    },
    /// The dedicated synchronous shutdown coordinator could not be started.
    #[error("failed to start event bus shutdown coordinator: {0}")]
    CoordinatorStart(
        /// Operating-system error from starting the coordinator thread.
        #[source]
        std::io::Error,
    ),
    /// A lifecycle guard rejected shutdown.
    #[error(transparent)]
    Lifecycle(
        /// Error from lifecycle validation or coordination.
        #[from]
        LifecycleError,
    ),
    /// The backend failed to shut down.
    #[error(transparent)]
    Spi(
        /// Failure returned by the provider shutdown operation.
        #[from]
        SpiError,
    ),
    /// One or more provider subscriptions failed to close.
    #[error(transparent)]
    SubscriptionClose(
        /// Snapshot of provider subscription close failures.
        Arc<SubscriptionCloseErrors>,
    ),
}
