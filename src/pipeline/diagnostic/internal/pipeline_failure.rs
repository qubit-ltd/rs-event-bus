// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publisher pipeline failure wrapper with its original source error.

use super::pipeline_failure_origin::PipelineFailureOrigin;
use crate::error::EventBusError;

/// Failure carrying its origin without inspecting or cloning the error.
#[derive(Debug, thiserror::Error)]
#[error("publisher pipeline failed at {origin:?}: {error}")]
pub(crate) struct PipelineFailure {
    /// Stage that produced the pipeline failure.
    origin: PipelineFailureOrigin,
    /// Original operation error retained as the source.
    #[source]
    error: Box<EventBusError>,
}

impl PipelineFailure {
    /// Creates a failure with explicit publisher pipeline provenance.
    ///
    /// # Parameters
    /// - `origin`: publisher pipeline stage that failed.
    /// - `error`: original operation failure.
    ///
    /// # Returns
    /// A wrapper preserving the source error and its pipeline stage.
    pub(crate) fn new(origin: PipelineFailureOrigin, error: impl Into<EventBusError>) -> Self {
        Self {
            origin,
            error: Box::new(error.into()),
        }
    }

    /// Returns the publisher pipeline failure stage.
    ///
    /// # Returns
    /// The stage that generated the wrapped error.
    #[cfg(test)]
    #[must_use = "Use the returned origin."]
    #[inline]
    pub(crate) fn origin(&self) -> PipelineFailureOrigin {
        self.origin
    }

    /// Returns the borrowed aggregate error for classification.
    ///
    /// # Returns
    /// The aggregate error retained by the wrapper.
    #[cfg(test)]
    #[must_use = "Use the returned error."]
    #[inline]
    pub(crate) fn error(&self) -> &EventBusError {
        &self.error
    }

    /// Consumes the wrapper and returns its original operation error.
    ///
    /// # Returns
    /// The wrapped publisher operation error.
    pub(crate) fn into_error(self) -> EventBusError {
        *self.error
    }
}
