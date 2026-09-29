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
use qubit_retry::RetryPolicyError;

use crate::pipeline::DeadLetterForwardError;

/// Builds a retry flow using the subscription's bounded retry and backoff
/// budget.
///
/// # Parameters
///
/// - `policy`: Retry limit and backoff configuration supplied by the
///   subscription.
///
/// # Returns
///
/// A retry configuration that retries failed forwarding attempts and aborts
/// when exhausted.
///
/// # Errors
///
/// Returns an error when the policy cannot produce a valid retry configuration.
pub(crate) fn retry_config(policy: &RetryPolicy) -> Result<RetryConfig<DeadLetterForwardError>, RetryPolicyError> {
    RetryConfig::builder()
        .policy(policy.clone())
        .fallback(RetryFallback::Abort)
        .rule(|failure: &AttemptFailure<DeadLetterForwardError>, _: &RetryContext| {
            let effect = match failure.as_error() {
                Some(DeadLetterForwardError::Publish(error)) => error.effect(),
                Some(DeadLetterForwardError::NotAdmitted(
                    crate::model::AdmissionOutcome::NoneAccepted(_)
                    | crate::model::AdmissionOutcome::NoDestinations
                    | crate::model::AdmissionOutcome::Dropped,
                )) => crate::model::PublishEffect::NotAccepted,
                Some(DeadLetterForwardError::NotAdmitted(_)) | None => crate::model::PublishEffect::MayHaveBeenAccepted,
            };
            if crate::pipeline::retry::uncertainty_allows_retry(effect, crate::model::DuplicateRiskPolicy::Forbid) {
                RetryDecision::Retry
            } else {
                RetryDecision::Abort
            }
        })
        .build()
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use qubit_retry::Retry;
    use qubit_retry::RetryPolicy;

    use crate::error::PublishError;
    use crate::error::PublishFailure;
    use crate::error::SpiError;
    use crate::model::AdmissionOutcome;
    use crate::model::AdmissionSummary;
    use crate::model::EventId;
    use crate::model::PublishEffect;
    use crate::pipeline::DeadLetterForwardError;

    #[test]
    fn unknown_dead_letter_publish_is_not_retried() {
        let config = super::retry_config(&RetryPolicy::builder().max_attempts(3).build().unwrap()).unwrap();
        let calls = Cell::new(0);
        let result: Result<_, _> = Retry::new(&config).run(|| {
            calls.set(calls.get() + 1);
            Err::<(), _>(DeadLetterForwardError::Publish(PublishFailure::new(
                EventId::new("dead-letter").unwrap(),
                PublishEffect::MayHaveBeenAccepted,
                PublishError::Spi(SpiError::Operation {
                    provider_id: "script".into(),
                    operation: "publish",
                    resource: None,
                    kind: "lost_response",
                    retryable: Some(true),
                    source: Box::new(std::io::Error::other("source retained")),
                }),
            )))
        });
        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn partially_admitted_dead_letter_is_not_retried() {
        let config = super::retry_config(&RetryPolicy::builder().max_attempts(3).build().unwrap()).unwrap();
        let calls = Cell::new(0);
        let result: Result<_, _> = Retry::new(&config).run(|| {
            calls.set(calls.get() + 1);
            Err::<(), _>(DeadLetterForwardError::NotAdmitted(
                AdmissionOutcome::PartiallyAccepted(AdmissionSummary {
                    accepted: 1,
                    filtered: 0,
                    rejected: 1,
                }),
            ))
        });
        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
    }
}
