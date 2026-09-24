// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! SPI shutdown policy.

use std::time::Duration;

/// Provider shutdown behavior requested by its owning facade.
///
/// For the synchronous [`crate::EventBus`] facade, `Graceful` bounds how long
/// the caller waits for the complete shutdown sequence. If that deadline
/// expires, the caller receives `ShutdownError::TimedOut` while a background
/// coordinator continues cleanup and the bus rejects new operations. A later
/// shutdown call can wait again, or `Immediate` can strengthen the active
/// attempt. Rust cannot forcibly stop a blocked synchronous provider call or
/// handler. Async facade shutdown is driven by its returned future and may be
/// cancelled by dropping that future.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShutdownMode {
    /// Drain provider work for at most the given time.
    Graceful {
        /// Maximum time for the complete facade shutdown, including receiver
        /// close, active-work coordination, and provider SPI shutdown.
        timeout: Duration,
    },
    /// Stop immediately without draining.
    Immediate,
}
