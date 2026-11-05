// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Completion result retained for observers of a shutdown generation.

use std::io::Error;
use std::sync::Arc;

use crate::error::SpiError;
use crate::spi::ShutdownOutcome;

/// Immutable completion retained for the tickets of one exact generation.
#[must_use]
#[derive(Clone)]
pub(in crate::facade) enum ShutdownResult {
    /// Provider completion or a shared provider failure.
    Provider(#[doc = "Provider completion or shared provider failure."] Result<ShutdownOutcome, Arc<SpiError>>),
    /// Operating system failure while creating the shutdown worker.
    StartFailed(#[doc = "Operating system error returned when starting the worker failed."] Arc<Error>),
}
