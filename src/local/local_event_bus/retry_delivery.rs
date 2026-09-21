// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscriber retry and handler delivery.

use std::any::Any;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use qubit_retry::Retry;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryConfig;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::super::subscriber_interceptor_chain::DownstreamErrorSlot;
use super::super::subscriber_interceptor_chain::is_recorded_downstream_error;
use super::HandlerFn;
use super::LocalEventBus;
use super::dead_letter::handle_subscription_failure;
use crate::AckMode;
use crate::Acknowledgement;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventBusRetryRule;
use crate::EventEnvelope;
use crate::SubscribeOptions;
use crate::core::SubscriptionState;

mod handler_types {
    use super::Acknowledgement;
    use super::EventBusError;
    use super::EventEnvelope;

    #[derive(Clone)]
    pub struct HandlerDelivery<T: Clone + Send + Sync + 'static> {
        /// Envelope delivered to the current handler attempt.
        pub(in crate::local::local_event_bus) delivered: EventEnvelope<T>,
        /// Shared acknowledgement state for the attempt.
        pub(in crate::local::local_event_bus) acknowledgement: Acknowledgement,
    }

    pub struct HandlerRunFailure<T: Clone + Send + Sync + 'static> {
        /// Subscription that produced the terminal failure.
        pub(in crate::local::local_event_bus) subscription_id: usize,
        /// Final error after retry and acknowledgement processing.
        pub(in crate::local::local_event_bus) error: EventBusError,
        /// Last attempted delivery and its acknowledgement state.
        pub(in crate::local::local_event_bus) delivery: HandlerDelivery<T>,
    }
}

pub(super) use handler_types::HandlerDelivery;
pub(super) use handler_types::HandlerRunFailure;

impl<T> HandlerDelivery<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates an attempt delivery with a fresh acknowledgement handle.
    fn new(envelope: &EventEnvelope<T>) -> Self {
        let acknowledgement = Acknowledgement::new();
        let delivered = envelope.clone().with_acknowledgement(acknowledgement.clone());
        Self {
            delivered,
            acknowledgement,
        }
    }
}

pub(super) fn process_subscription_event<T>(
    active: Arc<SubscriptionState>,
    handler: Arc<HandlerFn<T>>,
    options: SubscribeOptions<T>,
    subscription_id: usize,
    subscriber_id: String,
    envelope: EventEnvelope<T>,
    event_bus: LocalEventBus,
) where
    T: Clone + Send + Sync + 'static,
{
    if !active.is_active() {
        return;
    }
    match run_handler_with_retry(&handler, &options, subscription_id, envelope) {
        Ok(delivery) => {
            if options.ack_mode() == AckMode::Auto && !delivery.acknowledgement.is_completed() {
                delivery.acknowledgement.ack();
            }
        }
        Err(failure) => handle_subscription_failure(&event_bus.inner, &subscriber_id, &options, *failure),
    }
}

fn run_handler_with_retry<T>(
    handler: &Arc<HandlerFn<T>>,
    options: &SubscribeOptions<T>,
    subscription_id: usize,
    envelope: EventEnvelope<T>,
) -> Result<HandlerDelivery<T>, Box<HandlerRunFailure<T>>>
where
    T: Clone + Send + Sync + 'static,
{
    let mut last_delivery = None;
    match run_with_retry(
        options.retry_options(),
        options.retry_rule(),
        options.retry_cancellation_token(),
        || {
            let delivery = HandlerDelivery::new(&envelope);
            last_delivery = Some(delivery.clone());
            call_handler(handler, delivery.delivered.clone())?;
            if delivery.acknowledgement.is_nacked() {
                Err(EventBusError::handler_failed("subscriber nacked the event"))
            } else if options.ack_mode() == AckMode::Manual && !delivery.acknowledgement.is_acked() {
                Err(EventBusError::handler_failed("manual acknowledgement missing"))
            } else {
                Ok(delivery)
            }
        },
    ) {
        Ok(delivery) => Ok(delivery),
        Err(error) => {
            let delivery = match last_delivery {
                Some(delivery) => delivery,
                None => HandlerDelivery::new(&envelope),
            };
            Err(Box::new(HandlerRunFailure {
                subscription_id,
                error,
                delivery,
            }))
        }
    }
}

fn call_handler<T>(handler: &Arc<HandlerFn<T>>, envelope: EventEnvelope<T>) -> EventBusResult<()>
where
    T: Clone + Send + Sync + 'static,
{
    match panic::catch_unwind(AssertUnwindSafe(|| handler(envelope))) {
        Ok(result) => result,
        Err(_) => Err(EventBusError::handler_panicked()),
    }
}

pub(super) fn normalize_subscriber_interceptor_result(
    result: Result<EventBusResult<()>, Box<dyn Any + Send>>,
    downstream_error: &DownstreamErrorSlot,
    panic_message: &'static str,
) -> EventBusResult<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) if is_recorded_downstream_error(downstream_error, &error) => Err(error),
        Ok(Err(error)) => Err(normalize_subscriber_interceptor_error(error)),
        Err(_) => Err(EventBusError::interceptor_failed("subscribe", panic_message)),
    }
}

fn normalize_subscriber_interceptor_error(error: EventBusError) -> EventBusError {
    if matches!(
        &error,
        EventBusError::InterceptorFailed { phase, .. } if *phase == "subscribe"
    ) {
        error
    } else {
        EventBusError::interceptor_failed("subscribe", error.to_string())
    }
}

/// Runs a dispatch operation through the shared retry implementation.
pub(super) fn run_dispatch_with_retry<T, F>(
    retry_options: Option<&RetryPolicy>,
    retry_rule: Option<&Arc<dyn RetryRule<EventBusError>>>,
    operation: F,
) -> EventBusResult<T>
where
    F: FnMut() -> EventBusResult<T>,
{
    run_with_retry(retry_options, retry_rule, None, operation)
}

fn run_with_retry<T, F>(
    retry_options: Option<&RetryPolicy>,
    retry_rule: Option<&Arc<dyn RetryRule<EventBusError>>>,
    cancellation: Option<&RetryCancellationToken>,
    operation: F,
) -> EventBusResult<T>
where
    F: FnMut() -> EventBusResult<T>,
{
    let Some(retry_options) = retry_options else {
        let mut operation = operation;
        return operation();
    };
    let mut builder = RetryConfig::<EventBusError>::builder().policy((*retry_options).clone());
    if let Some(rule) = retry_rule {
        builder = builder.shared_rule(Arc::clone(rule));
    }
    let retry = builder
        .rule(EventBusRetryRule)
        .build()
        .expect("validated event-bus retry options should build");
    let mut execution = Retry::new(&retry);
    if let Some(token) = cancellation {
        execution = execution.cancellation_token(token.clone());
    }
    match execution.run(operation) {
        Ok(value) => Ok(value.into_value_discarding_diagnostics()),
        Err(error) => Err(EventBusError::from(error)),
    }
}
