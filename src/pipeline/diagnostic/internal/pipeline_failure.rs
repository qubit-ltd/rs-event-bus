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
use crate::model::PublishEffect;

/// Failure carrying its origin without inspecting or cloning the error.
#[derive(Debug, thiserror::Error)]
#[error("publisher pipeline failed at {origin:?}: {error}")]
#[must_use]
pub(crate) struct PipelineFailure {
    /// Stage that produced the pipeline failure.
    origin: PipelineFailureOrigin,
    /// Admission evidence from the publication pipeline.
    effect: PublishEffect,
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
            effect: PublishEffect::NotAccepted,
            error: Box::new(error.into()),
        }
    }

    /// Returns admission evidence carried from the terminal attempt boundary.
    #[must_use]
    #[inline]
    pub(crate) fn publish_effect(&self) -> PublishEffect {
        self.effect
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

    /// Attaches admission evidence without inspecting or replacing the source.
    pub(crate) fn with_publish_effect(mut self, effect: PublishEffect) -> Self {
        self.effect = effect;
        self
    }

    /// Consumes the wrapper and returns its original operation error.
    ///
    /// # Returns
    /// The wrapped publisher operation error.
    pub(crate) fn into_error(self) -> EventBusError {
        *self.error
    }
}
