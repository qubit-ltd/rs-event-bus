// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event decoding and handler execution.

#![allow(clippy::too_many_arguments)]

use std::io::Error;
use std::num::NonZeroU32;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use qubit_id::Id;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFallback;

use super::internal::OwnerSettlementRouter;
use crate::DeliveryError;
use crate::EventId;
use crate::SubscriberId;
use crate::error::CodecError;
use crate::error::DeliveryAttemptError;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::failure::finish_failed_delivery;
use crate::facade::event_bus::failure::panic_message;
use crate::facade::event_bus::failure::settle_rejected;
use crate::facade::event_bus::failure::settle_token;
use crate::facade::event_bus::internal::HandlerStartRejected;
use crate::model::DEAD_LETTER_HEADER;
use crate::model::DEAD_LETTER_HEADER_VALUE;
use crate::model::Delivery;
use crate::model::DeliveryContext;
use crate::model::EventEnvelope;
use crate::model::FailureDirective;
use crate::model::Headers;
use crate::model::ProviderMessageMetadata;
use crate::model::SubscribeOptions;
use crate::model::SubscriberInterceptor;
use crate::model::Topic;
use crate::pipeline::DeliveryOutcome;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::choose_failure_directive;
use crate::pipeline::is_retry_rule_failure;
use crate::pipeline::terminal_directive as choose_terminal_directive;
use crate::spi::DeliveryDisposition;
use crate::spi::OrderingKey;
use crate::spi::SettlementToken;
use crate::spi::TopicAddress;

/// Builds an owner-retained delivery and applies the subscription filter.
///
/// # Type Parameters
/// - `T`: typed payload expected by the subscription.
///
/// # Parameters
/// - `inner`: bus state used for diagnostics and policy.
/// - `settler`: owner that serializes provider settlement calls.
/// - `subscription_id`: bus-local subscription identity.
/// - `subscriber_id`: logical subscriber identity.
/// - `topic`: typed event topic.
/// - `options`: subscriber middleware, retry, and settlement policy.
/// - `address`: provider topic address from the message.
/// - `event_id`: stable identity from the message.
/// - `timestamp`: creation time from the message.
/// - `headers`: portable headers from the message.
/// - `ordering_key`: optional provider ordering key.
/// - `decoded`: payload or ordinary decode failure prepared by receiver owner.
/// - `settlement`: provider token retained until terminal handling completes.
/// - `provider_metadata`: non-sensitive provider metadata.
/// - `provider_attempt`: positive attempt number reported by the provider, if
///   known.
///
/// # Returns
/// The delivery when decoding succeeds and the subscription filter accepts the
/// event; otherwise `None` after rejection, filter exclusion, or filter panic.
///
/// # Side Effects
/// Runs the filter, emits diagnostics, and routes terminal settlement.
/// The provider token stays in the owner.
pub(in crate::facade) fn prepare_delivery<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    topic: &Topic<T>,
    options: &SubscribeOptions<T>,
    address: TopicAddress,
    event_id: EventId,
    timestamp: SystemTime,
    headers: Headers,
    ordering_key: Option<OrderingKey>,
    decoded: Result<Arc<T>, CodecError>,
    settlement: &mut Option<SettlementToken>,
    provider_metadata: ProviderMessageMetadata,
    provider_attempt: Option<NonZeroU32>,
) -> Option<Delivery<T>>
where
    T: Send + Sync + 'static,
{
    let payload = match decoded.map_err(Into::into) {
        Ok(payload) => payload,
        Err(error) => {
            settle_rejected(
                inner,
                settler,
                settlement.take(),
                subscription_id,
                subscriber_id,
                event_id,
                address.as_str(),
                error,
            );
            return None;
        }
    };
    let mut event = EventEnvelope::with_id_and_shared_payload(topic.clone(), payload, event_id);
    event.timestamp = timestamp;
    event.headers = headers;
    event.ordering_key = ordering_key.map(|value| value.as_str().into());
    let event = Arc::new(event);
    if let Some(filter) = options.filter() {
        let accepted = catch_unwind(AssertUnwindSafe(|| filter(&event)));
        match accepted {
            Ok(false) => {
                settle_token(
                    inner,
                    settler,
                    settlement.take(),
                    DeliveryDisposition::Accept,
                    &event,
                    subscription_id,
                    subscriber_id,
                );
                return None;
            }
            Ok(true) => {}
            Err(_) => {
                let error = DeliveryError::Handler {
                    source: Box::new(Error::other("subscriber filter panicked")),
                };
                let directive = notify_error_handlers(inner, options, &event, &error, options.retry_policy().is_some());
                finish_failed_delivery(
                    inner,
                    settler,
                    settlement.take(),
                    event,
                    subscription_id,
                    subscriber_id,
                    options,
                    error,
                    1,
                    directive,
                );
                return None;
            }
        }
    }

    let context = DeliveryContext::new(inner.provider_id.clone(), subscription_id, subscriber_id.clone())
        .with_provider_metadata(provider_metadata)
        .with_settlement(settlement.is_some());
    let context = if let Some(attempt) = provider_attempt {
        context.with_provider_attempt(attempt.get())
    } else {
        context
    };
    let context = if event.header(DEAD_LETTER_HEADER) == Some(DEAD_LETTER_HEADER_VALUE) {
        context.as_dead_letter()
    } else {
        context
    };
    let delivery = Delivery::new(event.clone(), context);
    Some(delivery)
}

/// Builds an owned retry session for `options`, sharing the selected error
/// directive with its rule. Returns no session when retry is disabled, or a
/// terminal delivery error if configuration is invalid. The session samples
/// the bus clock but never registers a timer or sleeps.
///
/// # Parameters
/// - `inner`: bus state supplying the clock used to create the retry timer.
/// - `options`: subscription policy and optional cancellation token.
/// - `terminal_directive`: shared slot containing the selected failure
///   directive.
///
/// # Returns
/// `Some(session)` when retry is enabled, or `None` when it is disabled.
///
/// # Errors
/// Returns a `DeliveryError` when the retry configuration is invalid.
pub(in crate::facade) fn new_retry_session<T>(
    inner: &EventBusInner,
    options: &SubscribeOptions<T>,
    terminal_directive: Arc<Mutex<Option<FailureDirective>>>,
) -> Result<Option<qubit_retry::RetrySession<DeliveryAttemptError>>, DeliveryError> {
    let Some(policy) = options.retry_policy() else {
        return Ok(None);
    };
    let directive_for_rule = terminal_directive.clone();
    let user_rule = options.retry_rule().cloned();
    let builder = RetryConfig::<DeliveryAttemptError>::builder()
        .policy(policy.clone())
        .fallback(RetryFallback::Abort)
        .rule(
            move |failure: &AttemptFailure<DeliveryAttemptError>, context: &RetryContext| {
                let directive = directive_for_rule
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .unwrap_or(FailureDirective::Discard);
                if directive != FailureDirective::Retry {
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
        );
    let config = builder.build().map_err(|error| DeliveryError::Handler {
        source: Box::new(error),
    })?;
    let mut session = qubit_retry::RetrySession::new(config, inner.clock.new_timer());
    if let Some(token) = options.retry_cancellation_token() {
        session = session.with_cancellation_token(token.clone());
    }
    Ok(Some(session))
}

/// Maps a terminal retry error while preserving its diagnostics and directive.
/// Emits rule failures once; the returned error retains the retry context.
///
/// # Parameters
/// - `inner`: bus state used to emit retry-rule diagnostics.
/// - `error`: terminal retry error whose context is preserved.
/// - `directive`: previously selected failure directive.
///
/// # Returns
/// The mapped delivery error and the final failure directive.
pub(in crate::facade) fn terminal_retry_error(
    inner: &EventBusInner,
    error: qubit_retry::RetryError<DeliveryAttemptError>,
    directive: FailureDirective,
) -> (DeliveryError, FailureDirective) {
    if is_retry_rule_failure(error.reason()) {
        inner.emit_internal("retry_rule", error.to_string());
    }
    let directive = choose_terminal_directive(error.reason(), directive);
    (SubscriberPipeline::retry_error(error), directive)
}

/// Runs each terminal action callback in registration order and chooses a safe
/// directive.
///
/// # Type Parameters
/// - `T`: event payload type associated with the failed delivery.
///
/// # Parameters
/// - `inner`: bus state used to emit diagnostics for callback panics.
/// - `options`: registered error callbacks and retry policy.
/// - `event`: failed event supplied to callbacks.
/// - `error`: terminal handler or middleware failure.
/// - `retry_enabled`: whether this failure can be retried.
///
/// # Returns
/// The selected retry, requeue, dead-letter, or discard directive.
pub(in crate::facade) fn notify_error_handlers<T>(
    inner: &EventBusInner,
    options: &SubscribeOptions<T>,
    event: &EventEnvelope<T>,
    error: &DeliveryError,
    retry_enabled: bool,
) -> FailureDirective {
    if HandlerStartRejected::is_rejection(error) {
        return FailureDirective::Requeue;
    }
    if options.error_handlers().is_empty() {
        return if options.retry_policy().is_some() {
            FailureDirective::Retry
        } else {
            FailureDirective::Discard
        };
    }
    let mut directives = Vec::with_capacity(options.error_handlers().len());
    for handler in options.error_handlers() {
        match catch_unwind(AssertUnwindSafe(|| handler(event, error))) {
            Ok(directive) => directives.push(Ok(directive)),
            Err(payload) => {
                inner.emit_internal("subscriber_error_handler", panic_message(payload.as_ref()).into());
                directives.push(Err(()));
            }
        }
    }
    choose_failure_directive(retry_enabled, directives)
}

/// Invokes one handler attempt through global and typed synchronous middleware.
///
/// # Type Parameters
/// - `T`: typed payload expected by the subscription.
///
/// # Parameters
/// - `options`: subscriber acknowledgement and middleware settings.
/// - `delivery`: delivery supplied to the middleware chain.
/// - `handler`: typed subscriber callback invoked with the delivery.
/// - `_attempt`: one-based attempt number retained for the shared call shape.
/// - `global_interceptors`: bus-wide middleware callbacks.
///
/// # Returns
/// The successful outcome or handler/middleware failure.
pub(in crate::facade) fn run_delivery_attempt<T>(
    options: &SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    _attempt: u32,
    global_interceptors: &[Arc<SubscriberInterceptor<T>>],
) -> DeliveryOutcome
where
    T: Send + Sync + 'static,
{
    let handler = handler.clone();
    let rejected = Arc::new(AtomicBool::new(false));
    let handler_rejected = rejected.clone();
    let outcome = SubscriberPipeline::attempt_sync(
        options.ack_mode(),
        delivery,
        global_interceptors,
        options.interceptors(),
        move |delivery| {
            let result = handler(delivery);
            if result.as_ref().is_err_and(HandlerStartRejected::is_rejection) {
                handler_rejected.store(true, Ordering::Release);
            }
            result
        },
    );
    if rejected.load(Ordering::Acquire) {
        DeliveryOutcome::Failure(DeliveryError::Handler {
            source: Box::new(HandlerStartRejected),
        })
    } else {
        outcome
    }
}
