// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors returned when an event identifier cannot be generated.

use qubit_id::IdGenerationError;
use thiserror::Error;

/// The operating-system random source could not provide a UUID v4.
///
/// # Examples
///
/// ```
/// use std::error::Error;
///
/// use qubit_event_bus::error::EventIdGenerationError;
/// use qubit_event_bus::model::EventId;
///
/// fn inspect_generation_failure(error: &EventIdGenerationError) {
///     assert!(Error::source(error).is_some());
/// }
///
/// if let Err(error) = EventId::generate() {
///     inspect_generation_failure(&error);
/// }
/// ```
#[derive(Debug, Error)]
#[must_use]
#[error("failed to generate event ID")]
pub struct EventIdGenerationError(
    /// Underlying UUID generator failure.
    #[source]
    IdGenerationError,
);

impl EventIdGenerationError {
    /// Wraps the underlying ID generator error while preserving it as a source.
    ///
    /// # Parameters
    /// - `source`: the UUID generator failure to retain.
    ///
    /// # Returns
    /// An event-ID generation error that exposes `source` through its error
    /// chain.
    #[inline]
    pub(crate) fn new(source: IdGenerationError) -> Self {
        Self(source)
    }
}
