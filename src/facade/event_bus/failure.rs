// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal failure policy, settlement, and dead-letter forwarding.

#![allow(clippy::too_many_arguments)]

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use qubit_id::Id;
use qubit_retry::Retry;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;

use super::internal::OwnerSettlementRouter;
use crate::Diagnostic;
use crate::SubscriberId;
use crate::error::DeliveryError;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::publishing::publish_internal;
use crate::facade::event_bus::worker::count_abandoned;
use crate::model::AdmissionOutcome;
use crate::model::DEAD_LETTER_HEADER;
use crate::model::DEAD_LETTER_HEADER_VALUE;
use crate::model::DeadLetterAdmissionPolicy;
use crate::model::DeadLetterEvent;
use crate::model::Delivery;
use crate::model::DeliveryContext;
use crate::model::EventEnvelope;
use crate::model::EventId;
use crate::model::FailureDirective;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::model::SubscribeOptions;
use crate::pipeline::DeadLetterForwardError;
use crate::pipeline::DeliveryFailureAction;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::dead_letter_envelope;
use crate::pipeline::dead_letter_retry_config;
use crate::pipeline::dead_letter_was_accepted;
use crate::spi::DeliveryDisposition;
use crate::spi::DurabilityCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::SettlementToken;

/// Applies the terminal failure directive and records its diagnostics.
///
/// # Type Parameters
/// - `T`: event payload type retained by the failed delivery.
///
/// # Parameters
/// - `inner`: bus state used for publication, diagnostics, and recovery counts.
/// - `settler`: owner that serializes provider settlement calls.
/// - `token`: provider token for the failed delivery, when available.
/// - `event`: failed event and its portable metadata.
/// - `subscription_id`: bus-local subscription identity.
/// - `subscriber_id`: logical subscriber identity.
/// - `options`: retry and dead-letter policy for the subscription.
/// - `error`: terminal handler or middleware failure.
/// - `attempts`: number of handler attempts made.
/// - `directive`: action selected by the subscriber error policy.
///
/// # Side Effects
/// May publish a dead-letter event, settle the provider token, update
/// abandonment counts, stop a subscription, and emit diagnostics.
pub(in crate::facade) fn finish_failed_delivery<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    token: Option<SettlementToken>,
    event: Arc<EventEnvelope<T>>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    options: &SubscribeOptions<T>,
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
        DeliveryFailureAction::RetryLocally
        | DeliveryFailureAction::DeadLetter
        | DeliveryFailureAction::Discard => DeliveryDisposition::Reject,
    };
    let mut disposition = if action == DeliveryFailureAction::DeadLetter {
        None
    } else {
        SubscriberPipeline::failure_disposition(action, capabilities)
    };
    let is_dead_letter = event.header(DEAD_LETTER_HEADER) == Some(DEAD_LETTER_HEADER_VALUE);
    let mut dead_letter_forward_failed = false;
    if action == DeliveryFailureAction::DeadLetter && !is_dead_letter {
        (disposition, dead_letter_forward_failed) = forward_dead_letter(
            inner,
            &event,
            &error,
            subscription_id,
            subscriber_id,
            options,
            capabilities,
        );
    } else if action == DeliveryFailureAction::DeadLetter {
        disposition = SubscriberPipeline::failure_disposition(
            DeliveryFailureAction::DeadLetter,
            capabilities,
        );
    }
    if action == DeliveryFailureAction::RetryLocally && disposition.is_none() {
        requested_disposition = DeliveryDisposition::Reject;
        disposition = SubscriberPipeline::failure_disposition(
            DeliveryFailureAction::Discard,
            inner.capabilities.settlement(),
        );
    }
    if token.is_none() && !dead_letter_forward_failed {
        inner.emit(Diagnostic::SettlementUnavailable {
            event_id: event.id().clone(),
            topic: event.topic().name().into(),
            subscription_id,
            subscriber_id: subscriber_id.clone(),
            requested: requested_disposition,
        });
        count_abandoned(inner, subscription_id);
        settler.abandon(subscription_id);
    } else if let Some(disposition) = disposition {
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
        settler.abandon(subscription_id);
        // Keep a durable source token unsettled so the provider can recover it.
    } else if token
        .as_ref()
        .is_some_and(|token| !token.belongs_to(subscription_id))
    {
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
        count_abandoned(inner, subscription_id);
        settler.abandon(subscription_id);
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

/// Forwards one failed delivery when its directive requests dead-lettering.
///
/// Keeps policy lookup, envelope construction, retrying publication, and
/// failure diagnostics together so the caller can preserve settlement order.
///
/// # Type Parameters
/// - `T`: event payload type retained by the failed delivery.
///
/// # Parameters
/// - `inner`: bus state used for dead-letter publication and diagnostics.
/// - `event`: failed event and its portable metadata.
/// - `error`: terminal failure included in the dead-letter record.
/// - `subscription_id`: bus-local subscription identity.
/// - `subscriber_id`: logical subscriber identity.
/// - `options`: retry and dead-letter policy for the subscription.
/// - `capabilities`: provider settlement capabilities used for the final
///   action.
///
/// # Returns
/// The settlement disposition and whether forwarding failed.
///
/// # Side Effects
/// May publish a dead-letter event, emit diagnostics, and stop the subscription
/// if forwarding cannot complete.
fn forward_dead_letter<T: Send + Sync + 'static>(
    inner: &Arc<EventBusInner>,
    event: &Arc<EventEnvelope<T>>,
    error: &DeliveryError,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    options: &SubscribeOptions<T>,
    capabilities: SettlementCapabilities,
) -> (Option<DeliveryDisposition>, bool) {
    let Some(policy) = options.dead_letter() else {
        inner.emit_internal(
            "dead_letter_policy",
            "dead-letter directive has no configured topic".into(),
        );
        stop_after_dead_letter_failure(inner, subscription_id);
        return (None, true);
    };
    let context = DeliveryContext::new(
        inner.provider_id.clone(),
        subscription_id,
        subscriber_id.clone(),
    );
    let delivery = Delivery::new(event.clone(), context);
    let envelope = match dead_letter_envelope(&delivery, error, policy.topic_name()) {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            inner.emit_internal(
                "dead_letter_build",
                "dead-letter event was not created".into(),
            );
            stop_after_dead_letter_failure(inner, subscription_id);
            return (None, true);
        }
        Err(build_error) => {
            inner.emit_internal("dead_letter_build", build_error.to_string());
            stop_after_dead_letter_failure(inner, subscription_id);
            return (None, true);
        }
    };
    let receipt = match publish_dead_letter_sync(
        inner,
        &envelope,
        options.retry_policy(),
        options.retry_cancellation_token(),
        policy.admission_policy(),
    ) {
        Ok(receipt) => receipt,
        Err(message) => {
            inner.emit_internal("dead_letter_publish", message);
            stop_after_dead_letter_failure(inner, subscription_id);
            return (None, true);
        }
    };
    if matches!(
        receipt.admission_outcome(),
        AdmissionOutcome::PartiallyAccepted(_)
    ) {
        inner.emit_internal(
            "dead_letter_partial",
            "dead-letter publication was partially accepted; retrying the whole record may duplicate it".into(),
        );
    }
    (
        SubscriberPipeline::failure_disposition(DeliveryFailureAction::DeadLetter, capabilities),
        false,
    )
}

/// Settles one provider-issued token and emits a structured failure on error.
///
/// # Type Parameters
/// - `T`: event payload type associated with the settlement.
///
/// # Parameters
/// - `inner`: bus state used to emit settlement diagnostics.
/// - `settler`: owner that serializes provider settlement calls.
/// - `token`: provider token to settle, when available.
/// - `disposition`: terminal provider action to apply.
/// - `event`: event whose identity and topic label the operation.
/// - `subscription_id`: bus-local subscription identity.
/// - `subscriber_id`: logical subscriber identity.
///
/// # Side Effects
/// Dispatches settlement through the owning receiver and emits diagnostics
/// for invalid or unavailable tokens.
pub(in crate::facade) fn settle_token<T>(
    _inner: &Arc<EventBusInner>,
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
    settler.settle(
        Some(token),
        disposition,
        event.id().clone(),
        event.topic().name(),
        subscription_id,
        subscriber_id,
    );
}

/// Settles a rejected message when possible and reports its decode failure.
///
/// # Parameters
/// - `inner`: bus state used for diagnostics and settlement capabilities.
/// - `settler`: owner that serializes provider settlement calls.
/// - `token`: provider token associated with the rejected message.
/// - `subscription_id`: bus-local subscription identity.
/// - `subscriber_id`: logical subscriber identity.
/// - `event_id`: stable identity of the rejected event.
/// - `topic`: provider topic associated with the event.
/// - `error`: decode or configuration failure that rejected the message.
///
/// # Side Effects
/// May settle the token and emits settlement and delivery-failure diagnostics.
pub(in crate::facade) fn settle_rejected(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    token: Option<SettlementToken>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    event_id: EventId,
    topic: &str,
    error: DeliveryError,
) {
    match token {
        Some(token)
            if !token.belongs_to(subscription_id)
                || inner.capabilities.settlement() == SettlementCapabilities::AcceptRetryReject =>
        {
            // Invalid ownership is validated by the receiver before SPI entry,
            // even when this provider cannot otherwise perform Reject.
            settler.settle(
                Some(token),
                DeliveryDisposition::Reject,
                event_id.clone(),
                topic,
                subscription_id,
                subscriber_id,
            );
        }
        token => {
            count_abandoned(inner, subscription_id);
            settler.abandon(subscription_id);
            if token.is_some() {
                inner.emit(Diagnostic::SettlementUnavailable {
                    event_id: event_id.clone(),
                    topic: topic.into(),
                    subscription_id,
                    subscriber_id: subscriber_id.clone(),
                    requested: DeliveryDisposition::Reject,
                });
            }
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
///
/// # Type Parameters
/// - `T`: original event payload type.
///
/// # Parameters
/// - `inner`: bus and provider state used by the publisher pipeline.
/// - `envelope`: dead-letter event to publish on each attempt.
/// - `retry_policy`: optional retry budget and backoff policy.
/// - `cancellation`: optional signal that cancels retry delays.
/// - `admission_policy`: provider admission evidence required for success.
///
/// # Returns
/// The receipt when forwarding is considered accepted.
///
/// # Errors
/// Returns publication, admission, or retry failure details.
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
        if dead_letter_was_accepted(&receipt, inner.capabilities, admission_policy) {
            Ok(receipt)
        } else {
            Err(DeadLetterForwardError::NotAdmitted(
                receipt.admission_outcome(),
            ))
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
///
/// # Parameters
/// - `inner`: bus state used to update recovery information and find controls.
/// - `subscription_id`: subscription that failed dead-letter forwarding.
///
/// # Side Effects
/// Counts known ephemeral loss and requests that the subscription stop.
pub(in crate::facade) fn stop_after_dead_letter_failure(
    inner: &EventBusInner,
    subscription_id: Id,
) {
    if inner.capabilities.durability() == DurabilityCapability::Ephemeral {
        inner.abandoned_deliveries.fetch_add(1, Ordering::AcqRel);
    }
    if let Some(control) = inner
        .subscription_snapshot()
        .iter()
        .find(|control| control.id == subscription_id)
    {
        if inner.capabilities.durability() == DurabilityCapability::Ephemeral {
            control.delivery_metrics.record_abandoned_ephemeral();
        }
        control.request_cancel();
        inner.scheduler.cancel_subscription(subscription_id);
    }
}

/// Formats panic payloads without exposing arbitrary panic internals.
///
/// # Parameters
/// - `payload`: panic object captured by the unwind boundary.
///
/// # Returns
/// A stable message that reveals whether the panic payload was string-like.
#[must_use]
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
    fn test_panic_message_hides_payload_details_and_handles_non_string_payloads() {
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
