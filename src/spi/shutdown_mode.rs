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
