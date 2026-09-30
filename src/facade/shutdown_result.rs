// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared result values retained while callers observe one shutdown generation.

use std::io::Error as IoError;
use std::sync::Arc;

use crate::error::SpiError;
use crate::spi::ShutdownOutcome;

/// Immutable completion retained for the tickets of one exact generation.
#[must_use = "shutdown completion must be observed"]
#[derive(Clone)]
pub(crate) enum ShutdownResult {
    /// Provider completion or a shared provider failure.
    Provider(
        /// The provider's successful outcome or shared SPI failure.
        Result<ShutdownOutcome, Arc<SpiError>>,
    ),
    /// Operating system failure while creating the shutdown worker.
    StartFailed(
        /// The operating system error shared with all generation tickets.
        Arc<IoError>,
    ),
}
