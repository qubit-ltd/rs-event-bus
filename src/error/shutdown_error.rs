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
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ShutdownError {
    /// The graceful shutdown deadline elapsed.
    #[error("event bus shutdown timed out after {timeout:?}")]
    TimedOut {
        /// Grace period that elapsed.
        timeout: Duration,
    },
    /// A lifecycle guard rejected shutdown.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    /// The backend failed to shut down.
    #[error(transparent)]
    Spi(#[from] SpiError),
    /// One or more provider subscriptions failed to close.
    #[error(transparent)]
    SubscriptionClose(Arc<SubscriptionCloseErrors>),
}
