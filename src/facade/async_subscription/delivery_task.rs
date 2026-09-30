// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Delivery attempt execution and terminal failure handling.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use qubit_retry::AsyncRetry;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFallback;

use crate::DeliveryError;
use crate::Diagnostic;
use crate::error::DeliveryAttemptError;
use crate::facade::async_subscription::AsyncEventBusInner;
use crate::facade::async_subscription::RetryFailure;
use crate::facade::async_subscription::SharedAsyncHandler;
use crate::facade::async_subscription::choose_terminal_directive;
use crate::model::AsyncSubscriberInterceptor;
use crate::model::Delivery;
use crate::model::EventEnvelope;
use crate::model::FailureDirective;
use crate::model::SubscribeOptions;
use crate::pipeline::DeliveryOutcome;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::choose_failure_directive;
use crate::pipeline::is_retry_rule_failure;

/// Runs subscriber middleware and one handler attempt.
///
/// # Type Parameters
/// - `T`: Payload type carried by the delivery.
///
/// # Parameters
/// - `options`: middleware and acknowledgement policy.
/// - `delivery`: delivery supplied to the attempt.
/// - `handler`: application handler callback.
/// - `attempt`: one-based attempt number.
/// - `global_interceptors`: bus-wide middleware.
///
/// # Returns
/// The pipeline's success or delivery failure outcome.
pub(in crate::facade) async fn run_one_attempt<T: Send + Sync + 'static>(
    options: &SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: SharedAsyncHandler<T>,
    attempt: u32,
    global_interceptors: &[Arc<AsyncSubscriberInterceptor<T>>],
) -> DeliveryOutcome {
    let interceptors = options.async_interceptors().to_vec();
    SubscriberPipeline::attempt_async(
        options.ack_mode(),
        delivery.next_attempt(attempt),
        global_interceptors,
        &interceptors,
        move |delivery| handler(delivery),
    )
    .await
}

/// Runs subscriber middleware and handler attempts under the retry policy.
///
/// # Type Parameters
/// - `T`: payload type delivered to the handler.
///
/// # Parameters
/// - `options`: subscriber retry and error-handler policy.
/// - `delivery`: delivery supplied to each attempt.
/// - `handler`: user handler callback.
/// - `inner`: bus pipeline and observer state.
/// - `global_interceptors`: bus-wide async subscriber middleware.
///
/// # Returns
/// Successful attempt count, or failure with count and terminal directive.
///
/// # Errors
/// Returns handler, retry configuration, or retry execution failure details.
pub(in crate::facade) async fn run_with_retry<T: Send + Sync + 'static>(
    options: SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: SharedAsyncHandler<T>,
    inner: &Arc<AsyncEventBusInner>,
    global_interceptors: Vec<Arc<AsyncSubscriberInterceptor<T>>>,
) -> Result<u32, RetryFailure> {
    let Some(policy) = options.retry_policy().cloned() else {
        let outcome = run_one_attempt(&options, delivery.clone(), handler, 1, &global_interceptors).await;
        return match outcome {
            DeliveryOutcome::Success => Ok(1),
            DeliveryOutcome::Failure(error) => {
                let directive = notify_failure(&options, delivery.event(), &error, false, inner);
                Err((Box::new(error), 1, directive))
            }
        };
    };
    let directive = Arc::new(Mutex::new(None::<FailureDirective>));
    let rule_directive = directive.clone();
    let user_rule = options.retry_rule().cloned();
    let config = RetryConfig::<DeliveryAttemptError>::builder()
        .policy(policy)
        .fallback(RetryFallback::Abort)
        .rule(
            move |failure: &AttemptFailure<DeliveryAttemptError>, context: &RetryContext| {
                let requested = rule_directive
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .unwrap_or(FailureDirective::Discard);
                if requested != FailureDirective::Retry {
                    return RetryDecision::Abort;
                }
                match user_rule.as_ref().map(|rule| rule.decide(failure, context)) {
                    Some(decision) if decision != RetryDecision::UseDefault => decision,
                    _ => match failure.as_error().and_then(DeliveryAttemptError::retryable) {
                        Some(true) => RetryDecision::Retry,
                        Some(false) => RetryDecision::Abort,
                        None => RetryDecision::UseDefault,
                    },
                }
            },
        )
        .build()
        .map_err(|error| {
            (
                Box::new(DeliveryError::Handler {
                    source: Box::new(error),
                }),
                0,
                FailureDirective::Discard,
            )
        })?;
    let mut retry = AsyncRetry::new(&config).timer(inner.timer.clone());
    if let Some(cancellation) = options.retry_cancellation_token() {
        retry = retry.cancellation_token(cancellation.clone());
    }
    let attempts = Arc::new(AtomicU32::new(0));
    let operation = || {
        let options = options.clone();
        let delivery = delivery.clone();
        let handler = handler.clone();
        let directive = directive.clone();
        let attempts = attempts.clone();
        let inner = inner.clone();
        let global_interceptors = global_interceptors.clone();
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::AcqRel) + 1;
            match run_one_attempt(&options, delivery.clone(), handler, attempt, &global_interceptors).await {
                DeliveryOutcome::Success => Ok(()),
                DeliveryOutcome::Failure(error) => {
                    let requested = notify_failure(&options, delivery.event(), &error, true, &inner);
                    *directive.lock().unwrap_or_else(PoisonError::into_inner) = Some(requested);
                    Err(DeliveryAttemptError::new(
                        "delivery",
                        Some(requested == FailureDirective::Retry),
                        error,
                    ))
                }
            }
        })
    };
    match retry.run(operation).await {
        Ok(_) => Ok(attempts.load(Ordering::Acquire)),
        Err(error) => {
            let count = attempts.load(Ordering::Acquire);
            let requested = directive
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .unwrap_or(FailureDirective::Discard);
            if is_retry_rule_failure(error.reason()) {
                inner.emit(&Diagnostic::InternalFailure {
                    origin: "retry_rule".into(),
                    message: error.to_string().into(),
                });
            }
            let terminal = choose_terminal_directive(error.reason(), requested);
            Err((Box::new(DeliveryError::Retry(Box::new(error))), count, terminal))
        }
    }
}

/// Runs terminal error handlers and selects their delivery directive.
///
/// # Type Parameters
/// - `T`: payload type associated with the failed event.
///
/// # Parameters
/// - `options`: subscriber error handlers and retry policy.
/// - `event`: event that failed processing.
/// - `error`: terminal handler or middleware error.
/// - `retry_enabled`: whether a retry can be requested.
/// - `inner`: bus used to record callback panics.
///
/// # Returns
/// The selected retry, requeue, dead-letter, or discard directive.
pub(in crate::facade) fn notify_failure<T: Send + Sync + 'static>(
    options: &SubscribeOptions<T>,
    event: &EventEnvelope<T>,
    error: &DeliveryError,
    retry_enabled: bool,
    inner: &AsyncEventBusInner,
) -> FailureDirective {
    if options.error_handlers().is_empty() {
        return if retry_enabled {
            FailureDirective::Retry
        } else {
            FailureDirective::Discard
        };
    }
    let mut directives = Vec::with_capacity(options.error_handlers().len());
    for callback in options.error_handlers() {
        match catch_unwind(AssertUnwindSafe(|| callback(event, error))) {
            Ok(directive) => directives.push(Ok(directive)),
            Err(_) => {
                inner.emit(&Diagnostic::InternalFailure {
                    origin: "subscriber_error_handler".into(),
                    message: "subscriber error handler panicked".into(),
                });
                directives.push(Err(()));
            }
        }
    }
    choose_failure_directive(retry_enabled, directives)
}
