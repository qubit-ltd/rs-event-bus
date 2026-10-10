// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Publish-specific adapters for the `qubit-retry` execution API.

use std::cell::Cell;
use std::convert::identity;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

mod catch_unwind_future;
// Tracks in-flight provider attempts and conservative admission evidence.
mod internal;

use catch_unwind_future::CatchUnwindFuture;
use internal::InFlightPublish;
use qubit_clock::Timer;
use qubit_retry::AsyncRetry;
use qubit_retry::AttemptFailure;
use qubit_retry::Retry;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFallback;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use crate::error::ConfigurationError;
use crate::error::PublishAttemptError;
use crate::error::PublishError;
use crate::error::SpiError;
use crate::model::AdmissionOutcome;
use crate::model::DuplicateRiskPolicy;
use crate::model::PublishAcknowledgement;
use crate::model::PublishEffect;
use crate::spi::AsyncEventBusSpi;
use crate::spi::EventBusSpi;
use crate::spi::OutboundMessage;
use crate::spi::SpiFuture;
use crate::spi::panic_boundary::catch_spi_call;
use crate::spi::panic_boundary::provider_panic;

/// Invokes the provider once or through the configured same-thread retry.
///
/// # Type Parameters
/// - `F`: Factory that builds a fresh outbound message for each attempt.
///
/// # Parameters
/// - `spi`: Synchronous provider called for each attempt.
/// - `provider_id`: Provider identity used to classify SPI errors.
/// - `make_message`: Factory for the outbound message.
/// - `policy`: Optional retry budget and backoff policy.
/// - `rule`: Optional user retry decision rule.
/// - `cancellation`: Optional signal that cancels retry delays.
/// - `duplicate_policy`: Admission uncertainty safety policy.
/// - `seen_unknown`: Monotonic evidence across completed provider attempts.
/// - `seen_admission`: Confirmed admission retained for post-ACK retry failure.
///
/// # Returns
/// The provider acknowledgement after success.
///
/// # Errors
/// Returns provider failure, retry exhaustion/cancellation, or invalid retry
/// configuration.
#[allow(clippy::too_many_arguments)]
pub(crate) fn publish_sync<F>(
    spi: &dyn EventBusSpi,
    provider_id: &str,
    make_message: F,
    policy: Option<&RetryPolicy>,
    rule: Option<&Arc<dyn RetryRule<PublishAttemptError>>>,
    cancellation: Option<&RetryCancellationToken>,
    duplicate_policy: DuplicateRiskPolicy,
    seen_unknown: &Cell<bool>,
    seen_admission: &Cell<bool>,
) -> Result<PublishAcknowledgement, PublishError>
where
    F: Fn() -> OutboundMessage,
{
    let Some(policy) = policy else {
        return spi_publish_sync(spi, provider_id, make_message())
            .inspect(|acknowledgement| {
                seen_admission.set(seen_admission.get() || acknowledges_admission(acknowledgement));
            })
            .map_err(|error| {
                seen_unknown.set(seen_unknown.get() || error.publish_effect() == PublishEffect::MayHaveBeenAccepted);
                PublishError::from(error)
            });
    };
    let config = retry_config(policy, rule, duplicate_policy)?;
    let mut retry = Retry::new(&config);
    if let Some(cancellation) = cancellation {
        retry = retry.cancellation_token(cancellation.clone());
    }
    retry
        .run(|| {
            spi_publish_sync(spi, provider_id, make_message())
                .inspect(|acknowledgement| {
                    seen_admission.set(seen_admission.get() || acknowledges_admission(acknowledgement));
                })
                .map_err(|error| {
                    seen_unknown
                        .set(seen_unknown.get() || error.publish_effect() == PublishEffect::MayHaveBeenAccepted);
                    PublishAttemptError::new(error.kind(), error.retryable(), error.publish_effect(), error)
                })
        })
        .map(|success| success.value().clone())
        .map_err(|error| PublishError::Retry(Box::new(error)))
}

/// Invokes an async provider using runtime-neutral retry.
///
/// # Type Parameters
/// - `'a`: Lifetime shared by the provider, message factory, and attempt
///   futures.
/// - `F`: Factory that builds a fresh outbound message for each attempt.
///
/// # Parameters
/// - `spi`: Asynchronous provider called for each attempt.
/// - `provider_id`: Provider identity used to classify SPI errors.
/// - `make_message`: Factory for the outbound message.
/// - `policy`: Optional retry budget and backoff policy.
/// - `rule`: Optional user retry decision rule.
/// - `cancellation`: Optional signal that cancels retry delays.
/// - `timer`: Clock used for retry delays.
/// - `duplicate_policy`: Admission uncertainty safety policy.
/// - `seen_unknown`: Monotonic evidence across completed provider attempts.
/// - `seen_admission`: Confirmed admission retained for post-ACK retry failure.
///
/// # Returns
/// The provider acknowledgement after success.
///
/// # Errors
/// Returns provider failure, retry exhaustion/cancellation, or invalid retry
/// configuration.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn publish_async<'a, F>(
    spi: &'a dyn AsyncEventBusSpi,
    provider_id: &'a str,
    make_message: F,
    policy: Option<&RetryPolicy>,
    rule: Option<&Arc<dyn RetryRule<PublishAttemptError>>>,
    cancellation: Option<&RetryCancellationToken>,
    timer: Arc<dyn Timer>,
    duplicate_policy: DuplicateRiskPolicy,
    seen_unknown: Arc<AtomicBool>,
    seen_admission: Arc<AtomicBool>,
) -> Result<PublishAcknowledgement, PublishError>
where
    F: Fn() -> OutboundMessage + 'a,
{
    let Some(policy) = policy else {
        return spi_publish(spi, provider_id, make_message())
            .await
            .inspect(|acknowledgement| {
                if acknowledges_admission(acknowledgement) {
                    seen_admission.store(true, Ordering::Release);
                }
            })
            .map_err(|error| {
                if error.publish_effect() == PublishEffect::MayHaveBeenAccepted {
                    seen_unknown.store(true, Ordering::Release);
                }
                PublishError::from(error)
            });
    };
    let config = retry_config(policy, rule, duplicate_policy)?;
    let mut retry = AsyncRetry::new(&config);
    retry = retry.timer(timer);
    if let Some(cancellation) = cancellation {
        retry = retry.cancellation_token(cancellation.clone());
    }
    let operation = || -> SpiFuture<'a, Result<PublishAcknowledgement, PublishAttemptError>> {
        let message = make_message();
        let attempt = spi_publish(spi, provider_id, message);
        let seen_unknown = seen_unknown.clone();
        Box::pin(publish_attempt(attempt, seen_unknown, seen_admission.clone()))
    };
    retry
        .run(operation)
        .await
        .map(|success| success.value().clone())
        .map_err(|error| PublishError::Retry(Box::new(error)))
}

/// Invokes one asynchronous provider operation behind SPI panic boundaries.
///
/// # Parameters
/// - `spi`: Provider implementation receiving the message.
/// - `provider_id`: Provider identity used in operation errors.
/// - `message`: Outbound message submitted to the provider.
///
/// # Returns
/// The provider acknowledgement.
///
/// # Errors
/// Returns synchronous or asynchronous SPI operation failure.
async fn spi_publish(
    spi: &dyn AsyncEventBusSpi,
    provider_id: &str,
    message: OutboundMessage,
) -> Result<PublishAcknowledgement, SpiError> {
    let resource = message.topic().as_str().to_owned();
    let future = catch_spi_call(provider_id, "publish", Some(&resource), || spi.publish(message))?;
    match CatchUnwindFuture::new(future).await {
        Ok(result) => result,
        Err(payload) => Err(provider_panic(provider_id, "publish", Some(&resource), payload)),
    }
}

/// Invokes one synchronous provider operation behind the SPI panic boundary.
///
/// # Parameters
/// - `spi`: Provider implementation receiving the message.
/// - `provider_id`: Provider identity used in operation errors.
/// - `message`: Outbound message submitted to the provider.
///
/// # Returns
/// The provider acknowledgement.
///
/// # Errors
/// Returns provider operation failure or a caught provider panic.
fn spi_publish_sync(
    spi: &dyn EventBusSpi,
    provider_id: &str,
    message: OutboundMessage,
) -> Result<PublishAcknowledgement, SpiError> {
    let resource = message.topic().as_str().to_owned();
    catch_spi_call(provider_id, "publish", Some(&resource), || spi.publish(message)).and_then(identity)
}

/// Builds an abort-on-exhaustion retry configuration for publication attempts.
///
/// # Parameters
/// - `policy`: Retry budget and backoff settings.
/// - `rule`: Optional shared retry rule supplied by the caller.
/// - `duplicate_policy`: Safety gate applied before user rules.
///
/// # Returns
/// A configured retry policy for publish attempts.
///
/// # Errors
/// Returns a configuration error when the retry policy is invalid.
fn retry_config(
    policy: &RetryPolicy,
    rule: Option<&Arc<dyn RetryRule<PublishAttemptError>>>,
    duplicate_policy: DuplicateRiskPolicy,
) -> Result<RetryConfig<PublishAttemptError>, PublishError> {
    let mut builder = RetryConfig::<PublishAttemptError>::builder()
        .policy(policy.clone())
        .fallback(RetryFallback::Abort)
        .rule(move |failure: &AttemptFailure<PublishAttemptError>, _: &RetryContext| {
            let effect = failure
                .as_error()
                .map_or(PublishEffect::MayHaveBeenAccepted, PublishAttemptError::effect);
            if !uncertainty_allows_retry(effect, duplicate_policy) {
                RetryDecision::Abort
            } else {
                RetryDecision::UseDefault
            }
        });
    if let Some(rule) = rule {
        builder = builder.shared_rule(rule.clone());
    }
    builder = builder.rule(
        |failure: &AttemptFailure<PublishAttemptError>, _context: &RetryContext| match failure
            .as_error()
            .and_then(PublishAttemptError::retryable)
        {
            Some(true) => RetryDecision::Retry,
            Some(false) => RetryDecision::Abort,
            None => RetryDecision::UseDefault,
        },
    );
    builder.build().map_err(|source| {
        PublishError::Configuration(ConfigurationError::InvalidField {
            field: "retry_policy",
            message: source.to_string().into(),
        })
    })
}

/// Applies the admission safety gate before any user retry rule.
///
/// # Parameters
/// - `effect`: Admission evidence from the current provider attempt.
/// - `policy`: Explicit permission to risk duplicate admissions.
///
/// # Returns
/// Whether the attempt may proceed to ordinary retry decisions.
#[must_use]
#[inline]
pub(crate) fn uncertainty_allows_retry(effect: PublishEffect, policy: DuplicateRiskPolicy) -> bool {
    matches!(
        (effect, policy),
        (PublishEffect::NotAccepted, _) | (PublishEffect::MayHaveBeenAccepted, DuplicateRiskPolicy::AllowDuplicates)
    )
}

/// Tracks the outcome of an SPI future through completion or abandonment.
///
/// # Type Parameters
/// - `F`: Provider future owned by the individual attempt.
///
/// # Parameters
/// - `attempt`: Lazy provider call polled inside the evidence boundary.
/// - `seen_unknown`: Monotonic evidence for the complete publication.
/// - `seen_admission`: Confirmed admission retained independently of
///   duplicates.
///
/// # Returns
/// The acknowledgement when the provider completes successfully.
///
/// # Errors
/// Returns the original SPI error with its explicit admission evidence.
async fn publish_attempt<F>(
    attempt: F,
    seen_unknown: Arc<AtomicBool>,
    seen_admission: Arc<AtomicBool>,
) -> Result<PublishAcknowledgement, PublishAttemptError>
where
    F: Future<Output = Result<PublishAcknowledgement, SpiError>>,
{
    // Arming occurs on first poll, immediately before entering SPI.
    // A retry timeout/cancellation drops this future before a reply.
    let mut in_flight = InFlightPublish {
        seen_unknown: seen_unknown.clone(),
        completed: false,
    };
    let result = attempt.await;
    // A reply disarms the guard, including an explicit rejection or an ACK.
    in_flight.completed = true;
    if result.as_ref().is_ok_and(acknowledges_admission) {
        seen_admission.store(true, Ordering::Release);
    }
    result.map_err(|error| {
        if error.publish_effect() == PublishEffect::MayHaveBeenAccepted {
            seen_unknown.store(true, Ordering::Release);
        }
        PublishAttemptError::new(error.kind(), error.retryable(), error.publish_effect(), error)
    })
}

/// Distinguishes admitted ACKs from definite non-admission receipts.
///
/// # Parameters
/// - `acknowledgement`: The explicit response from the provider.
///
/// # Returns
/// Whether a later infrastructure failure must retain admission evidence.
#[must_use]
#[inline]
fn acknowledges_admission(acknowledgement: &PublishAcknowledgement) -> bool {
    matches!(
        acknowledgement.admission_outcome(),
        AdmissionOutcome::OpaqueAccepted | AdmissionOutcome::Accepted(_) | AdmissionOutcome::PartiallyAccepted(_)
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::Future;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;

    use qubit_clock::ManualMonotonicClock;
    use qubit_clock::MonotonicClock;
    use qubit_retry::AsyncRetry;
    use qubit_retry::AttemptFailure;
    use qubit_retry::RetryContext;
    use qubit_retry::RetryDecision;
    use qubit_retry::RetryPolicy;
    use qubit_retry::RetryRule;

    use super::publish_attempt;
    use super::retry_config;
    use crate::error::PublishAttemptError;
    use crate::model::DuplicateRiskPolicy;

    /// Drives the real publish attempt boundary through a configured hard
    /// timeout.
    fn assert_hard_timeout_preserves_uncertainty(flow_timeout: bool) {
        let clock = ManualMonotonicClock::new_shared();
        let rule: Arc<dyn RetryRule<PublishAttemptError>> =
            Arc::new(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry);
        let policy = RetryPolicy::builder().max_attempts(2).build().unwrap();
        let config = retry_config(&policy, Some(&rule), DuplicateRiskPolicy::Forbid).unwrap();
        let retry = AsyncRetry::new(&config).timer(clock.new_timer());
        let retry = if flow_timeout {
            retry.hard_flow_timeout(Duration::from_secs(1))
        } else {
            retry.hard_attempt_timeout(Duration::from_secs(1))
        };
        let calls = Cell::new(0);
        let seen_unknown = Arc::new(AtomicBool::new(false));
        let mut future = Box::pin(retry.run(|| {
            publish_attempt(
                async {
                    calls.set(calls.get() + 1);
                    pending().await
                },
                seen_unknown.clone(),
                Arc::new(AtomicBool::new(false)),
            )
        }));
        let mut context = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut context).is_pending());
        clock.advance(Duration::from_secs(1)).unwrap();
        assert!(
            matches!(future.as_mut().poll(&mut context), Poll::Ready(Err(_))),
            "unknown hard timeout must abort before custom retry"
        );
        assert_eq!(calls.get(), 1);
        assert!(seen_unknown.load(Ordering::Acquire));
    }
    #[test]
    fn test_hard_attempt_timeout_cannot_bypass_uncertainty_gate() {
        assert_hard_timeout_preserves_uncertainty(false);
    }
    #[test]
    fn test_hard_flow_timeout_preserves_in_flight_uncertainty() {
        assert_hard_timeout_preserves_uncertainty(true);
    }
}
