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
        .rule(|_: &AttemptFailure<DeadLetterForwardError>, _: &RetryContext| RetryDecision::Retry)
        .build()
}
