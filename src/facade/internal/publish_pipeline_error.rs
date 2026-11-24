// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Converts a publisher pipeline failure into its operation-level error type.

use crate::ConfigurationError;
use crate::EventBusError;
use crate::PublishError;
use crate::PublishFailure;
use crate::model::EventId;
use crate::pipeline::PipelineFailure;

/// Converts a publisher pipeline failure into its operation-level error type.
///
/// # Parameters
/// - `event_id`: Original event identifier, preserved in the publish failure.
/// - `failure`: Pipeline stage and underlying event bus error.
///
/// # Returns
/// The matching public publish error variant.
pub(in crate::facade) fn publish_pipeline_error(event_id: EventId, failure: PipelineFailure) -> PublishFailure {
    let effect = failure.publish_effect();
    let cause = match failure.into_error() {
        EventBusError::Configuration(error) => PublishError::Configuration(error),
        EventBusError::Capability(error) => PublishError::Capability(error),
        EventBusError::Codec(error) => PublishError::Codec(error),
        EventBusError::Publish(error) => error,
        other => PublishError::Configuration(ConfigurationError::InvalidField {
            field: "publish_pipeline",
            message: other.to_string().into(),
        }),
    };
    PublishFailure::new(event_id, effect, cause)
}
