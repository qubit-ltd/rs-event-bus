// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus lifecycle and idle-wait failures.

use std::sync::Arc;

use qubit_clock::TimeError;

use crate::error::SpiError;
use crate::error::SubscriptionCloseErrors;

/// A lifecycle operation could not complete.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LifecycleError {
    /// The injected timer failed while registering or completing a deadline.
    #[error("event bus timer failed: {0}")]
    Timer(#[from] TimeError),
    /// Waiting from a synchronous callback or worker owned by the bus would
    /// deadlock.
    #[error("{operation} would deadlock in this event bus execution context")]
    WouldDeadlock {
        /// Blocking operation requested by the caller.
        operation: &'static str,
    },
    /// The bus has already closed.
    #[error("event bus is closed")]
    Closed,
    /// The backend lifecycle operation failed.
    #[error(transparent)]
    Spi(#[from] SpiError),
    /// One or more provider subscriptions failed to close.
    #[error(transparent)]
    SubscriptionClose(Arc<SubscriptionCloseErrors>),
}
