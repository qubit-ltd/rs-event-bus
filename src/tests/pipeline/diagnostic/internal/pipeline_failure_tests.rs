// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal checks for publisher failure provenance and evidence.

use crate::error::EventBusError;
use crate::error::PublishError;
use crate::model::PublishEffect;
use crate::pipeline::PipelineFailure;
use crate::pipeline::PipelineFailureOrigin;

#[test]
fn test_failure_preserves_origin_cause_and_admission_evidence() {
    let failure = PipelineFailure::new(PipelineFailureOrigin::Retry, PublishError::Closed)
        .with_publish_effect(PublishEffect::MayHaveBeenAccepted);

    assert_eq!(failure.origin(), PipelineFailureOrigin::Retry);
    assert_eq!(failure.publish_effect(), PublishEffect::MayHaveBeenAccepted);
    assert!(
        matches!(
            failure.error(),
            EventBusError::Publish(PublishError::Closed)
        ),
        "the failure should retain the original publish error"
    );
    assert!(
        std::error::Error::source(&failure).is_some(),
        "the failure should expose its cause"
    );

    let error = failure.into_error();
    assert!(
        matches!(error, EventBusError::Publish(PublishError::Closed)),
        "converting the failure should preserve the publish error"
    );
}
