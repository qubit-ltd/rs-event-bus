// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors returned when an event identifier cannot be generated.

use qubit_id::IdGenerationError;

/// The operating-system random source could not provide a UUID v4.
#[derive(Debug, thiserror::Error)]
#[error("failed to generate event ID")]
pub struct EventIdGenerationError(#[source] IdGenerationError);

impl EventIdGenerationError {
    /// Wraps the underlying ID generator error while preserving it as a source.
    pub(crate) fn new(source: IdGenerationError) -> Self {
        Self(source)
    }
}
