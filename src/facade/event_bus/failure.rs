// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal failure policy, settlement, and dead-letter forwarding.

#![allow(clippy::too_many_arguments)]
use crate::Diagnostic;
use crate::SubscriberId;
use crate::error::DeliveryError;
use crate::facade::event_bus::Any;
use crate::facade::event_bus::Arc;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::Id;
use crate::facade::event_bus::Ordering;
use crate::facade::event_bus::OwnerSettlementRouter;
use crate::facade::event_bus::Retry;
use crate::facade::event_bus::RetryCancellationToken;
use crate::facade::event_bus::RetryPolicy;
use crate::facade::event_bus::publishing::publish_internal;
use crate::model::DeadLetterAdmissionPolicy;
use crate::model::DeadLetterEvent;
use crate::model::Delivery;
use crate::model::DeliveryContext;
use crate::model::EventEnvelope;
use crate::model::FailureDirective;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::pipeline::DeadLetterForwardError;
use crate::pipeline::DeliveryFailureAction;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::dead_letter_envelope;
use crate::pipeline::dead_letter_retry_config;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementToken;

pub(in crate::facade) fn finish_failed_delivery<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    token: Option<SettlementToken>,
    event: Arc<EventEnvelope<T>>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    options: &crate::model::SubscribeOptions<T>,
    error: DeliveryError,
    attempts: u32,
    directive: FailureDirective,
) where
    T: Send + Sync + 'static,
{
    let action = SubscriberPipeline::failure_action(directive);
    let capabilities = inner.capabilities.settlement();
    let mut requested_disposition = match action {
        DeliveryFailureAction::Requeue => DeliveryDisposition::Retry,
        DeliveryFailureAction::RetryLocally | DeliveryFailureAction::DeadLetter | DeliveryFailureAction::Discard => {
            DeliveryDisposition::Reject
        }
    };
    let mut disposition = if action == DeliveryFailureAction::DeadLetter {
        None
    } else {
        SubscriberPipeline::failure_disposition(action, capabilities)
    };
    let is_dead_letter = event.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE);
    let mut dead_letter_forward_failed = false;
    if action == DeliveryFailureAction::DeadLetter && !is_dead_letter {
        if let Some(policy) = options.dead_letter() {
            let context = DeliveryContext::new(inner.provider_id.clone(), subscription_id, subscriber_id.clone());
            let context =
                if event.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE) {
                    context.as_dead_letter()
                } else {
                    context
                };
            let delivery = Delivery::new(event.clone(), context);
            match dead_letter_envelope(&delivery, &error, policy.topic_name()) {
                Ok(Some(envelope)) => {
                    match publish_dead_letter_sync(
                        inner,
                        &envelope,
                        options.retry_policy(),
                        options.retry_cancellation_token(),
                        policy.admission_policy(),
                    ) {
                        Ok(receipt) => {
                            if matches!(
                                receipt.admission_outcome(),
                                crate::model::AdmissionOutcome::PartiallyAccepted(_)
                            ) {
                                inner.emit_internal(
                                    "dead_letter_partial",
                                    "dead-letter publication was partially accepted; retrying the whole record may duplicate it".into(),
                                );
                            }
                            disposition = SubscriberPipeline::failure_disposition(
                                DeliveryFailureAction::DeadLetter,
                                capabilities,
                            );
                        }
                        Err(message) => {
                            inner.emit_internal("dead_letter_publish", message);
                            dead_letter_forward_failed = true;
                            stop_after_dead_letter_failure(inner, subscription_id);
                        }
                    }
                }
                Ok(None) => {
                    inner.emit_internal("dead_letter_build", "dead-letter event was not created".into());
                    dead_letter_forward_failed = true;
                    stop_after_dead_letter_failure(inner, subscription_id);
                }
                Err(build_error) => {
                    inner.emit_internal("dead_letter_build", build_error.to_string());
                    dead_letter_forward_failed = true;
                    stop_after_dead_letter_failure(inner, subscription_id);
                }
            }
        } else {
            inner.emit_internal(
                "dead_letter_policy",
                "dead-letter directive has no configured topic".into(),
            );
            dead_letter_forward_failed = true;
            stop_after_dead_letter_failure(inner, subscription_id);
        }
    } else if action == DeliveryFailureAction::DeadLetter {
        disposition = SubscriberPipeline::failure_disposition(DeliveryFailureAction::DeadLetter, capabilities);
    }
    if action == DeliveryFailureAction::RetryLocally && disposition.is_none() {
        requested_disposition = DeliveryDisposition::Reject;
        disposition =
            SubscriberPipeline::failure_disposition(DeliveryFailureAction::Discard, inner.capabilities.settlement());
    }
    if let Some(disposition) = disposition {
        settle_token(
            inner,
            settler,
            token,
            disposition,
            &event,
            subscription_id,
            subscriber_id,
        );
    } else if dead_letter_forward_failed {
        // Keep a durable source token unsettled so the provider can recover it.
    } else if token.as_ref().is_some_and(|token| !token.belongs_to(subscription_id)) {
        inner.emit_internal(
            "settlement",
            "provider settlement token belongs to another subscription".into(),
        );
    } else {
        inner.emit(Diagnostic::SettlementUnavailable {
            event_id: event.id().clone(),
            topic: event.topic().name().into(),
            subscription_id,
            subscriber_id: subscriber_id.clone(),
            requested: requested_disposition,
        });
    }
    inner.emit(Diagnostic::DeliveryFailed {
        event_id: event.id().clone(),
        topic: event.topic().name().into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        attempts,
        error: error.to_string().into(),
    });
}

/// Settles one provider-issued token and emits a structured failure on error.
pub(in crate::facade) fn settle_token<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    token: Option<SettlementToken>,
    disposition: DeliveryDisposition,
    event: &EventEnvelope<T>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
) {
    let Some(token) = token else {
        return;
    };
    if !token.belongs_to(subscription_id) {
        inner.emit_internal(
            "settlement",
            "provider settlement token belongs to another subscription".into(),
        );
        return;
    }
    settler.settle(
        Some(token),
        disposition,
        event.id().clone(),
        event.topic().name(),
        subscription_id,
        subscriber_id,
    );
}

pub(in crate::facade) fn settle_rejected(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    token: Option<SettlementToken>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    event_id: crate::model::EventId,
    topic: &str,
    error: DeliveryError,
) {
    if let Some(token) = token {
        if token.belongs_to(subscription_id) {
            if inner.capabilities.settlement() == crate::spi::SettlementCapabilities::AcceptRetryReject {
                settler.settle(
                    Some(token),
                    DeliveryDisposition::Reject,
                    event_id.clone(),
                    topic,
                    subscription_id,
                    subscriber_id,
                );
            } else {
                inner.emit(Diagnostic::SettlementUnavailable {
                    event_id: event_id.clone(),
                    topic: topic.into(),
                    subscription_id,
                    subscriber_id: subscriber_id.clone(),
                    requested: DeliveryDisposition::Reject,
                });
            }
        } else {
            inner.emit_internal(
                "settlement",
                "provider settlement token belongs to another subscription".into(),
            );
        }
    }
    inner.emit(Diagnostic::DeliveryFailed {
        event_id,
        topic: topic.into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        attempts: 0,
        error: error.to_string().into(),
    });
}

/// Publishes one stable dead-letter envelope within the subscription retry
/// budget.
pub(in crate::facade) fn publish_dead_letter_sync<T: Send + Sync + 'static>(
    inner: &EventBusInner,
    envelope: &EventEnvelope<DeadLetterEvent<T>>,
    retry_policy: Option<&RetryPolicy>,
    cancellation: Option<&RetryCancellationToken>,
    admission_policy: DeadLetterAdmissionPolicy,
) -> Result<PublishReceipt, String> {
    let mut publish_once = || {
        let receipt = publish_internal(inner, PublishRequest::from_envelope(envelope.clone()))
            .map_err(DeadLetterForwardError::Publish)?;
        if crate::pipeline::dead_letter_was_accepted(&receipt, inner.capabilities, admission_policy) {
            Ok(receipt)
        } else {
            Err(DeadLetterForwardError::NotAdmitted(receipt.admission_outcome()))
        }
    };
    let Some(policy) = retry_policy else {
        return publish_once().map_err(|error| error.to_string());
    };
    let config = dead_letter_retry_config(policy).map_err(|error| error.to_string())?;
    let mut retry = Retry::new(&config);
    if let Some(cancellation) = cancellation {
        retry = retry.cancellation_token(cancellation.clone());
    }
    retry
        .run(&mut publish_once)
        .map(|success| success.value().clone())
        .map_err(|error| error.to_string())
}

/// Stops receiving after forwarding exhausts its budget, retaining the source
/// token for durable recovery and counting a known loss for ephemeral
/// providers.
pub(in crate::facade) fn stop_after_dead_letter_failure(inner: &EventBusInner, subscription_id: Id) {
    if inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral {
        inner.abandoned_deliveries.fetch_add(1, Ordering::AcqRel);
    }
    if let Some(control) = inner
        .subscription_snapshot()
        .iter()
        .find(|control| control.id == subscription_id)
    {
        control.request_cancel();
    }
}

/// Formats panic payloads without exposing arbitrary panic internals.
pub(in crate::facade) fn panic_message(payload: &(dyn Any + Send)) -> &'static str {
    if payload.is::<&'static str>() || payload.is::<String>() {
        "user or provider callback panicked"
    } else {
        "callback panicked with a non-string payload"
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::panic_message;

    #[test]
    fn panic_message_hides_payload_details_and_handles_non_string_payloads() {
        let string_payload: Box<dyn Any + Send> = Box::new(String::from("private panic detail"));
        assert_eq!(
            panic_message(string_payload.as_ref()),
            "user or provider callback panicked"
        );

        let opaque_payload: Box<dyn Any + Send> = Box::new(42_u32);
        assert_eq!(
            panic_message(opaque_payload.as_ref()),
            "callback panicked with a non-string payload"
        );
    }
}
