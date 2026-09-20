// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal subscription failure and dead-letter delivery.

use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use super::super::local_event_bus_inner::LocalEventBusInner;
use super::LocalEventBus;
use super::retry_delivery::HandlerDelivery;
use super::retry_delivery::HandlerRunFailure;
use crate::Acknowledgement;
use crate::DeadLetterOriginalPayload;
use crate::DeadLetterOutcome;
use crate::DeadLetterPayload;
use crate::DeliveryFailure;
use crate::DispatchStatus;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::EventEnvelopeMetadata;
use crate::PublishOutcome;
use crate::SubscribeOptions;
use crate::core::subscribe_options::DeadLetterStrategyAnyFn;
use crate::core::subscribe_options::DeadLetterStrategyFn;
use crate::core::subscribe_options::normalize_dead_letter_error;

/// Handles a terminal subscriber failure.
pub(super) fn handle_subscription_failure<T>(
    inner: &Arc<LocalEventBusInner>,
    subscriber_id: &str,
    options: &SubscribeOptions<T>,
    failure: HandlerRunFailure<T>,
) where
    T: Clone + Send + Sync + 'static,
{
    let HandlerRunFailure {
        subscription_id,
        error,
        delivery,
    } = failure;
    let HandlerDelivery {
        delivered,
        acknowledgement,
    } = delivery;
    let event_bus = LocalEventBus {
        inner: Arc::clone(inner),
    };
    let dead_letter = route_dead_letter(options, subscriber_id, &delivered, &error, &acknowledgement, &event_bus);
    let failure = DeliveryFailure::new(
        delivered.id().to_string(),
        delivered.topic().name().to_string(),
        subscription_id,
        subscriber_id.to_string(),
        error,
        acknowledgement.is_acked(),
        dead_letter,
    );
    inner.observe_delivery_failure(&failure);
}

fn route_dead_letter<T>(
    options: &SubscribeOptions<T>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    acknowledgement: &Acknowledgement,
    event_bus: &LocalEventBus,
) -> DeadLetterOutcome
where
    T: Clone + Send + Sync + 'static,
{
    for error in options.notify_subscribe_error(subscriber_id, delivered, error, acknowledgement) {
        event_bus.inner.observe_error(&error);
    }
    if !acknowledgement.is_completed() {
        acknowledgement.nack();
    }
    if acknowledgement.is_nacked() && !delivered.is_dead_letter() {
        let dead_letter = create_dead_letter_for_failure(options, subscriber_id, delivered, error, event_bus);
        let dead_letter = match dead_letter {
            DeadLetterCreation::NotConfigured => return DeadLetterOutcome::NotConfigured,
            DeadLetterCreation::Dropped => return DeadLetterOutcome::DroppedByStrategy,
            DeadLetterCreation::Failed(error) => return DeadLetterOutcome::Failed(error),
            DeadLetterCreation::Envelope(dead_letter) => dead_letter,
        };
        return match event_bus.publish_dead_letter_envelope(dead_letter.as_dead_letter()) {
            Ok(receipt) if receipt.has_rejections() => {
                let reason = match receipt.outcome() {
                    PublishOutcome::Dispatched(items) => items
                        .iter()
                        .find_map(|item| match item.status() {
                            DispatchStatus::Rejected(error) => Some(error.to_string()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "dead-letter subscriber admission was rejected".to_string()),
                    PublishOutcome::Dropped => "dead-letter was dropped by publisher interceptor".to_string(),
                };
                let observed = EventBusError::dead_letter_failed(reason);
                event_bus.inner.observe_error(&observed);
                DeadLetterOutcome::Rejected(receipt)
            }
            Ok(receipt) => DeadLetterOutcome::Publication(receipt),
            Err(error) => {
                let observed = EventBusError::dead_letter_failed(error.to_string());
                event_bus.inner.observe_error(&observed);
                DeadLetterOutcome::Failed(error)
            }
        };
    }
    DeadLetterOutcome::NotConfigured
}

enum DeadLetterCreation {
    NotConfigured,
    Dropped,
    Envelope(EventEnvelope<DeadLetterPayload>),
    Failed(EventBusError),
}

fn create_dead_letter_for_failure<T>(
    options: &SubscribeOptions<T>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    event_bus: &LocalEventBus,
) -> DeadLetterCreation
where
    T: Clone + Send + Sync + 'static,
{
    if options.has_dead_letter_strategy() {
        match options.create_dead_letter(subscriber_id, delivered, error) {
            Ok(Some(dead_letter)) => DeadLetterCreation::Envelope(dead_letter),
            Ok(None) => DeadLetterCreation::Dropped,
            Err(error) => {
                event_bus.inner.observe_error(&error);
                DeadLetterCreation::Failed(error)
            }
        }
    } else {
        match create_default_dead_letter_for_failure(options, subscriber_id, delivered, error, event_bus) {
            Ok(Some(dead_letter)) => DeadLetterCreation::Envelope(dead_letter),
            Ok(None) => DeadLetterCreation::NotConfigured,
            Err(error) => DeadLetterCreation::Failed(error),
        }
    }
}

fn create_default_dead_letter_for_failure<T>(
    options: &SubscribeOptions<T>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    event_bus: &LocalEventBus,
) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
where
    T: Clone + Send + Sync + 'static,
{
    if let Some(strategy) = event_bus.inner.default_dead_letter_strategy::<T>() {
        return match call_dead_letter_strategy(strategy, subscriber_id, delivered, error, options) {
            Ok(dead_letter) => Ok(dead_letter),
            Err(error) => {
                event_bus.inner.observe_error(&error);
                Err(error)
            }
        };
    }
    let Some(strategy) = event_bus.inner.global_default_dead_letter_strategy() else {
        return Ok(None);
    };
    match call_global_dead_letter_strategy(
        strategy,
        subscriber_id,
        delivered.metadata(),
        Arc::new(delivered.payload().clone()),
        error,
    ) {
        Ok(dead_letter) => Ok(dead_letter),
        Err(error) => {
            event_bus
                .inner
                .observe_error(&EventBusError::dead_letter_failed(error.to_string()));
            Err(error)
        }
    }
}

fn call_dead_letter_strategy<T>(
    strategy: Arc<DeadLetterStrategyFn<T>>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    options: &SubscribeOptions<T>,
) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
where
    T: Clone + Send + Sync + 'static,
{
    match panic::catch_unwind(AssertUnwindSafe(|| {
        strategy.create_dead_letter(subscriber_id, delivered, error, options)
    })) {
        Ok(Ok(dead_letter)) => Ok(dead_letter),
        Ok(Err(error)) => Err(normalize_dead_letter_error(error)),
        Err(_) => Err(EventBusError::dead_letter_failed(
            "default dead-letter strategy panicked",
        )),
    }
}

fn call_global_dead_letter_strategy(
    strategy: Arc<DeadLetterStrategyAnyFn>,
    subscriber_id: &str,
    metadata: EventEnvelopeMetadata,
    original_payload: DeadLetterOriginalPayload,
    error: &EventBusError,
) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>> {
    match panic::catch_unwind(AssertUnwindSafe(|| {
        strategy.create_dead_letter(subscriber_id, metadata, original_payload, error)
    })) {
        Ok(Ok(dead_letter)) => Ok(dead_letter),
        Ok(Err(error)) => Err(normalize_dead_letter_error(error)),
        Err(_) => Err(EventBusError::dead_letter_failed(
            "global default dead-letter strategy panicked",
        )),
    }
}
