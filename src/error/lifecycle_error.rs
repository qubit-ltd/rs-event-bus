// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus lifecycle and idle-wait failures.

use crate::error::SpiError;

/// A lifecycle operation could not complete.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LifecycleError {
    /// Waiting from the bus's own worker would deadlock.
    #[error("{operation} would deadlock on this event bus worker")]
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
}
