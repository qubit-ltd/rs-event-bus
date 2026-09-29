// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event decoding and handler execution.

#![allow(clippy::too_many_arguments)]
use crate::DeliveryError;
use crate::Diagnostic;
use crate::EventId;
use crate::SubscriberId;
use crate::facade::event_bus::Arc;
use crate::facade::event_bus::AttemptFailure;
use crate::facade::event_bus::DeliveryAttemptError;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::Id;
use crate::facade::event_bus::Mutex;
use crate::facade::event_bus::Ordering;
use crate::facade::event_bus::OwnerSettlementRouter;
use crate::facade::event_bus::Retry;
use crate::facade::event_bus::RetryConfig;
use crate::facade::event_bus::RetryContext;
use crate::facade::event_bus::RetryDecision;
use crate::facade::event_bus::RetryFallback;
use crate::facade::event_bus::choose_terminal_directive;
use crate::facade::event_bus::failure::finish_failed_delivery;
use crate::facade::event_bus::failure::panic_message;
use crate::facade::event_bus::failure::settle_rejected;
use crate::facade::event_bus::failure::settle_token;
use crate::model::Delivery;
use crate::model::DeliveryContext;
use crate::model::EventEnvelope;
use crate::model::FailureDirective;
use crate::model::Topic;
use crate::pipeline::DeliveryOutcome;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::is_retry_rule_failure;
use crate::spi::DeliveryDisposition;
use crate::spi::InboundMessage;
use crate::spi::SettlementToken;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

pub(in crate::facade) fn process_inbound<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    topic: &Topic<T>,
    codec: Option<&Arc<dyn crate::codec::EventCodec<T>>>,
    options: &crate::model::SubscribeOptions<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    message: InboundMessage,
) where
    T: Send + Sync + 'static,
{
    let (address, event_id, timestamp, headers, ordering_key, payload, mut settlement, provider_metadata) =
        message.into_parts();
    let fallback_event_id = event_id.clone();
    let fallback_topic = address.as_str().to_owned();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        process_inbound_parts(
            inner,
            settler,
            subscription_id,
            subscriber_id,
            topic,
            codec,
            options,
            handler,
            address,
            event_id,
            timestamp,
            headers,
            ordering_key,
            payload,
            &mut settlement,
            provider_metadata,
        );
    }));
    if let Err(payload) = result {
        inner.emit_internal("delivery_worker", panic_message(payload.as_ref()).into());
        if let Some(token) = settlement.take() {
            if token.belongs_to(subscription_id)
                && inner.capabilities.settlement() == crate::spi::SettlementCapabilities::AcceptRetryReject
            {
                settler.settle(
                    Some(token),
                    DeliveryDisposition::Retry,
                    fallback_event_id,
                    &fallback_topic,
                    subscription_id,
                    subscriber_id,
                );
            } else {
                inner.emit(Diagnostic::SettlementUnavailable {
                    event_id: fallback_event_id,
                    topic: fallback_topic.into(),
                    subscription_id,
                    subscriber_id: subscriber_id.clone(),
                    requested: DeliveryDisposition::Retry,
                });
            }
        }
    }
}

pub(in crate::facade) fn process_inbound_parts<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    topic: &Topic<T>,
    codec: Option<&Arc<dyn crate::codec::EventCodec<T>>>,
    options: &crate::model::SubscribeOptions<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    address: TopicAddress,
    event_id: EventId,
    timestamp: std::time::SystemTime,
    headers: crate::model::Headers,
    ordering_key: Option<crate::spi::OrderingKey>,
    payload: TransportPayload,
    settlement: &mut Option<SettlementToken>,
    provider_metadata: crate::model::ProviderMessageMetadata,
) where
    T: Send + Sync + 'static,
{
    let payload = match crate::codec::decode_payload(codec, payload).map_err(Into::into) {
        Ok(payload) => payload,
        Err(error) => {
            if matches!(
                error,
                DeliveryError::Codec(crate::error::CodecError::Panicked {
                    operation: "decode",
                    ..
                })
            ) {
                inner.emit_internal("codec_decode", error.to_string());
                if let Some(token) = settlement.take() {
                    if token.belongs_to(subscription_id)
                        && inner.capabilities.settlement() == crate::spi::SettlementCapabilities::AcceptRetryReject
                    {
                        settler.settle(
                            Some(token),
                            DeliveryDisposition::Retry,
                            event_id.clone(),
                            address.as_str(),
                            subscription_id,
                            subscriber_id,
                        );
                    } else if token.belongs_to(subscription_id) {
                        inner.emit(Diagnostic::SettlementUnavailable {
                            event_id: event_id.clone(),
                            topic: address.as_str().into(),
                            subscription_id,
                            subscriber_id: subscriber_id.clone(),
                            requested: DeliveryDisposition::Retry,
                        });
                    }
                }
                inner.emit(Diagnostic::DeliveryFailed {
                    event_id,
                    topic: address.as_str().into(),
                    subscription_id,
                    subscriber_id: subscriber_id.clone(),
                    attempts: 0,
                    error: error.to_string().into(),
                });
                return;
            }
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
            return;
        }
    };
    let mut event = EventEnvelope::with_id_and_shared_payload(topic.clone(), payload, event_id);
    event.timestamp = timestamp;
    event.headers = headers;
    event.ordering_key = ordering_key.map(|value| value.as_str().into());
    let event = Arc::new(event);
    if let Some(filter) = options.filter() {
        let accepted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| filter(&event)));
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
                return;
            }
            Ok(true) => {}
            Err(_) => {
                let error = DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("subscriber filter panicked")),
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
                return;
            }
        }
    }

    let context = DeliveryContext::new(inner.provider_id.clone(), subscription_id, subscriber_id.clone())
        .with_provider_metadata(provider_metadata)
        .with_settlement(settlement.is_some());
    let context = if event.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE) {
        context.as_dead_letter()
    } else {
        context
    };
    let delivery = Delivery::new(event.clone(), context);
    let global_interceptors = inner.facade_config.subscriber_interceptors::<T>();
    let outcome = run_delivery_with_retry(inner, options, delivery, handler, &global_interceptors);
    match outcome {
        Ok(()) => settle_token(
            inner,
            settler,
            settlement.take(),
            DeliveryDisposition::Accept,
            &event,
            subscription_id,
            subscriber_id,
        ),
        Err((error, attempts, directive)) => finish_failed_delivery(
            inner,
            settler,
            settlement.take(),
            event,
            subscription_id,
            subscriber_id,
            options,
            error,
            attempts,
            directive,
        ),
    }
}

/// Creates a typed event from a provider message using native downcast or topic
/// codec.
/// Applies configured retry to middleware and one handler attempt.
pub(in crate::facade) fn run_delivery_with_retry<T>(
    inner: &EventBusInner,
    options: &crate::model::SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    global_interceptors: &[Arc<crate::model::SubscriberInterceptor<T>>],
) -> Result<(), (DeliveryError, u32, FailureDirective)>
where
    T: Send + Sync + 'static,
{
    let Some(policy) = options.retry_policy() else {
        let event = delivery.event_arc();
        return match run_delivery_attempt(options, delivery, handler, 1, global_interceptors) {
            DeliveryOutcome::Success => Ok(()),
            DeliveryOutcome::Failure(error) => {
                let directive = notify_error_handlers(inner, options, &event, &error, options.retry_policy().is_some());
                let directive = if directive == FailureDirective::Retry {
                    FailureDirective::Discard
                } else {
                    directive
                };
                Err((error, 1, directive))
            }
        };
    };
    let terminal_directive = Arc::new(Mutex::new(None::<FailureDirective>));
    let directive_for_rule = terminal_directive.clone();
    let user_rule = options.retry_rule().cloned();
    let builder = RetryConfig::<DeliveryAttemptError>::builder()
        .policy(policy.clone())
        .fallback(RetryFallback::Abort)
        .rule(
            move |failure: &AttemptFailure<DeliveryAttemptError>, context: &RetryContext| {
                let directive = directive_for_rule
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    let config = builder.build().map_err(|error| {
        (
            DeliveryError::Handler {
                source: Box::new(error),
            },
            0,
            FailureDirective::Discard,
        )
    })?;
    let mut retry = Retry::new(&config);
    if let Some(token) = options.retry_cancellation_token() {
        retry = retry.cancellation_token(token.clone());
    }
    let attempts = std::sync::atomic::AtomicU32::new(0);
    let event = delivery.event_arc();
    match retry.run(|| {
        let attempt = attempts.fetch_add(1, Ordering::AcqRel) + 1;
        match run_delivery_attempt(
            options,
            delivery.next_attempt(attempt),
            handler,
            attempt,
            global_interceptors,
        ) {
            DeliveryOutcome::Success => Ok(()),
            DeliveryOutcome::Failure(error) => {
                let directive = notify_error_handlers(inner, options, &event, &error, options.retry_policy().is_some());
                *terminal_directive
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(directive);
                Err(DeliveryAttemptError::new(
                    "delivery",
                    Some(directive == FailureDirective::Retry),
                    error,
                ))
            }
        }
    }) {
        Ok(_) => Ok(()),
        Err(error) => {
            let count = attempts.load(Ordering::Acquire);
            let directive = terminal_directive
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .unwrap_or(FailureDirective::Discard);
            if is_retry_rule_failure(error.reason()) {
                inner.emit_internal("retry_rule", error.to_string());
            }
            let directive = choose_terminal_directive(error.reason(), directive);
            Err((SubscriberPipeline::retry_error(error), count, directive))
        }
    }
}

/// Runs each terminal action callback in registration order and chooses a safe
/// directive.
pub(in crate::facade) fn notify_error_handlers<T>(
    inner: &EventBusInner,
    options: &crate::model::SubscribeOptions<T>,
    event: &EventEnvelope<T>,
    error: &DeliveryError,
    retry_enabled: bool,
) -> FailureDirective {
    if options.error_handlers().is_empty() {
        return if options.retry_policy().is_some() {
            FailureDirective::Retry
        } else {
            FailureDirective::Discard
        };
    }
    let mut directives = Vec::with_capacity(options.error_handlers().len());
    for handler in options.error_handlers() {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(event, error))) {
            Ok(directive) => directives.push(Ok(directive)),
            Err(payload) => {
                inner.emit_internal("subscriber_error_handler", panic_message(payload.as_ref()).into());
                directives.push(Err(()));
            }
        }
    }
    crate::pipeline::choose_failure_directive(retry_enabled, directives)
}

/// Invokes one handler attempt through global and typed synchronous middleware.
pub(in crate::facade) fn run_delivery_attempt<T>(
    options: &crate::model::SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    _attempt: u32,
    global_interceptors: &[Arc<crate::model::SubscriberInterceptor<T>>],
) -> DeliveryOutcome
where
    T: Send + Sync + 'static,
{
    let handler = handler.clone();
    SubscriberPipeline::attempt_sync(
        options.ack_mode(),
        delivery,
        global_interceptors,
        options.interceptors(),
        move |delivery| handler(delivery),
    )
}
