// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Retry configuration and failures for forwarding a dead-letter event.

use qubit_retry::AttemptFailure;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFallback;
use qubit_retry::RetryPolicy;

use crate::error::PublishError;
use crate::model::AdmissionOutcome;

/// Failure that may be retried without settling the original delivery.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DeadLetterForwardError {
    /// The provider or publish pipeline failed before a receipt was returned.
    #[error(transparent)]
    Publish(#[from] PublishError),
    /// A non-publish pipeline stage failed before provider admission.
    #[error("dead-letter pipeline failed: {0}")]
    Pipeline(Box<str>),
    /// A receipt reported no admission allowed by the configured policy.
    #[error("dead-letter event was not admitted: {0:?}")]
    NotAdmitted(AdmissionOutcome),
}

/// Builds a retry flow using the subscription's bounded retry and backoff
/// budget.
pub(crate) fn retry_config(
    policy: &RetryPolicy,
) -> Result<RetryConfig<DeadLetterForwardError>, qubit_retry::RetryPolicyError> {
    RetryConfig::builder()
        .policy(policy.clone())
        .fallback(RetryFallback::Abort)
        .rule(|_: &AttemptFailure<DeadLetterForwardError>, _: &RetryContext| RetryDecision::Retry)
        .build()
}
