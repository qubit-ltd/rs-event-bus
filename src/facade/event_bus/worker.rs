// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-threaded provider receive, settlement, and delivery ownership.
#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::convert::identity;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread::park_timeout;
use std::time::Duration;

use qubit_id::Id;

use crate::DeliveryError;
use crate::Diagnostic;
use crate::SubscriberId;
use crate::codec::EventCodec;
use crate::codec::decode_payload;
use crate::error::SpiError;
use crate::error::SubscriptionCloseFailure;
use crate::facade::SubscriptionControl;
use crate::facade::event_bus::CoordinatorMessage;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::delivery::process_inbound;
use crate::facade::event_bus::failure::panic_message;
use crate::facade::event_bus::internal::OwnedSyncDelivery;
use crate::facade::event_bus::internal::OwnerSettlementRouter;
use crate::facade::event_bus::internal::close_spi_subscription;
use crate::facade::internal::BusContextGuard;
use crate::facade::internal::SettlementRetryDecision;
use crate::facade::internal::SettlementRetryState;
use crate::facade::lifecycle::receive_poll_interval;
use crate::model::Delivery;
use crate::model::SettlementTermination;
use crate::model::SubscribeOptions;
use crate::model::SubscriptionStopReason;
use crate::model::Topic;
use crate::pipeline::OrderingLaneKey;
use crate::spi::DeliveryDisposition;
use crate::spi::EventSubscriptionSpi;
use crate::spi::InboundMessage;
use crate::spi::ReceiveOutcome;

/// Owns a receiver on one thread until all started handlers and provider
/// cleanup finish. Payloads move to granted jobs, while tracking, lanes and
/// retries remain here. Provider failures are recorded on `control` and emitted
/// through `inner`.
///
/// # Type Parameters
/// - `T`: decoded payload shared safely with handler threads.
///
/// # Parameters
/// - `inner`: shared bus resources and scheduling metadata.
/// - `bus_identity`: reentrancy identity for callbacks on this bus.
/// - `control`: cancellation, terminal-cause and completion state.
/// - `spi_subscription`: receiver exclusively owned by this thread through
///   close.
/// - `topic`: typed destination used for tracking and ordering.
/// - `codec`: optional decoder selected when subscribing.
/// - `subscriber_id`: logical subscriber included in diagnostics.
/// - `options`: filter, middleware, ordering and retry policy.
/// - `handler`: actual user callback wrapper with final admission checks.
///
/// # Side Effects
/// Blocks in provider operations and bounded parks, dispatches pool work, and
/// closes the receiver after all started jobs finish. Claimed credits and
/// lifecycle state are guarded across injected-clock or owner unwinding.
pub(in crate::facade) fn run_subscription_worker<T>(
    inner: Arc<EventBusInner>,
    bus_identity: usize,
    control: Arc<SubscriptionControl>,
    mut spi_subscription: Box<dyn EventSubscriptionSpi>,
    topic: Topic<T>,
    codec: Option<Arc<dyn EventCodec<T>>>,
    subscriber_id: SubscriberId,
    options: SubscribeOptions<T>,
    handler: Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
) where
    T: Send + Sync + 'static,
{
    let _worker_context = BusContextGuard::enter(bus_identity);
    let _lifecycle = crate::facade::event_bus::internal::OwnerLifecycleGuard::new(
        inner.clone(),
        control.clone(),
    );
    let mut owned: HashMap<u64, OwnedSyncDelivery<'_, T>> = HashMap::new();
    let (sender, receiver) = mpsc::channel();
    let worker_result = catch_unwind(AssertUnwindSafe(|| {
        inner.scheduler.attach_owner(control.id);
        let mut receive_closed = false;
        let mut stop_published = false;
        loop {
            while let Ok(message) = receiver.try_recv() {
                match message {
                    CoordinatorMessage::Abandoned(lease) => {
                        if let Some(delivery) = owned.get_mut(&lease) {
                            delivery.abandoned = true;
                        }
                    }
                    CoordinatorMessage::HandlerFinished(lease) => {
                        if let Some(delivery) = owned.get_mut(&lease) {
                            delivery.handler_finished = true;
                        }
                    }
                    message @ CoordinatorMessage::Settlement { lease_id, .. } => {
                        if let Some(delivery) = owned.get_mut(&lease_id) {
                            if let Some(previous) = delivery.settlement.as_ref() {
                                if settlement_disposition(previous)
                                    != settlement_disposition(&message)
                                {
                                    fail_internal(
                                        &inner,
                                        &control,
                                        "conflicting_settlement_intent",
                                    );
                                }
                            } else {
                                delivery.settlement = Some(message);
                            }
                        } else {
                            inner.emit_internal(
                                "settlement_owner_disconnected",
                                "settlement lease is no longer owned".into(),
                            );
                        }
                    }
                }
            }
            let draining = inner.scheduler.should_drain(control.id);
            let stopping = control.is_cancelled() && !draining;
            if control.is_cancelled() {
                receive_closed = true;
            }
            if stopping && !stop_published {
                receive_closed = true;
                stop_published = true;
                inner.scheduler.cancel_subscription(control.id);
                for (&lease, delivery) in &mut owned {
                    if let Some((message, _)) = delivery.inbound.take() {
                        delivery.handler_finished = true;
                        if control.terminal_failure().is_none() {
                            delivery.settlement =
                                canceled_intent(&inner, lease, control.id, &subscriber_id, message);
                            delivery.abandoned = delivery.settlement.is_none();
                        } else {
                            count_abandoned(&inner, control.id);
                        }
                    }
                }
            }

            let terminal = control.terminal_failure().is_some();
            let mut completed = Vec::new();
            let mut wait = receive_poll_interval();
            if !terminal {
                if !stopping {
                    while let Some(lease) = inner.scheduler.take_settlement_ready(control.id) {
                        if let Some(delivery) = owned.get_mut(&lease) {
                            delivery.settlement_granted = true;
                        }
                    }
                }
                for (&lease, delivery) in &mut owned {
                    if !delivery.handler_finished || (!delivery.settlement_granted && !stopping) {
                        continue;
                    }
                    if delivery.settlement.is_none() {
                        completed.push(lease);
                        continue;
                    }
                    // Cancellation gets one best-effort call for a fresh intent;
                    // a previously failed token remains provider-owned on close.
                    if stopping && delivery.retry.attempts() > 0 {
                        continue;
                    }
                    match attempt_settlement(
                        &inner,
                        &control,
                        &mut *spi_subscription,
                        delivery,
                        stopping,
                    ) {
                        Some(delay) => wait = wait.min(delay),
                        None => {
                            if delivery.settlement.is_none() {
                                completed.push(lease);
                            }
                        }
                    }
                    if control.terminal_failure().is_some() {
                        break;
                    }
                }
            }
            for lease in completed {
                if let Some(delivery) = owned.remove(&lease)
                    && !delivery.abandoned
                    && !delivery.lifecycle_failed
                {
                    control.delivery_metrics.record_completed();
                }
            }
            if control.is_cancelled() && !draining {
                if !stop_published {
                    continue;
                }
                if owned.values().all(|delivery| delivery.handler_finished) {
                    break;
                }
            } else {
                while let Some(lease) = inner.scheduler.take_ready(control.id) {
                    let Some(delivery) = owned.get_mut(&lease) else {
                        fail_internal(&inner, &control, "missing_owned_delivery");
                        break;
                    };
                    let Some((message, decoded)) = delivery.inbound.take() else {
                        fail_internal(&inner, &control, "missing_owned_payload");
                        break;
                    };
                    let task_inner = inner.clone();
                    let task_control = control.clone();
                    let task_topic = topic.clone();
                    let task_options = options.clone();
                    let task_handler = handler.clone();
                    let task_subscriber = subscriber_id.clone();
                    let task_sender = sender.clone();
                    inner.scheduler.submit(move || {
                        let _completion =
                            crate::facade::event_bus::internal::HandlerCompletionGuard::new(
                                task_inner.scheduler.clone(),
                                task_control.id,
                                lease,
                                task_sender.clone(),
                            );
                        let _context = BusContextGuard::enter(bus_identity);
                        let router = OwnerSettlementRouter {
                            lease_id: lease,
                            sender: task_sender.clone(),
                            inner: Arc::downgrade(&task_inner),
                        };
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            if !task_control.try_start() {
                                count_abandoned(&task_inner, task_control.id);
                            } else if task_control.is_cancelled()
                                && !task_inner.scheduler.should_drain(task_control.id)
                            {
                                requeue_unstarted_message_via_owner(
                                    &task_inner,
                                    &router,
                                    task_control.id,
                                    &task_subscriber,
                                    message,
                                );
                            } else {
                                process_inbound(
                                    &task_inner,
                                    &router,
                                    task_control.id,
                                    &task_subscriber,
                                    &task_topic,
                                    &task_options,
                                    &task_handler,
                                    message,
                                    decoded,
                                );
                            }
                        }));
                        if let Err(payload) = result {
                            task_inner.emit_internal(
                                "delivery_worker",
                                panic_message(payload.as_ref()).into(),
                            );
                        }
                    });
                }
                if receive_closed && owned.is_empty() {
                    break;
                }
            }

            if !receive_closed && !control.is_cancelled() {
                inner.scheduler.request_receive(control.id);
                if let Some(lease) = inner.scheduler.take_receive_reservation(control.id) {
                    let lease_guard = crate::facade::event_bus::internal::ReceiveLeaseGuard::new(
                        inner.scheduler.clone(),
                        lease,
                    );
                    inner.scheduler.record_owned_start(lease, inner.clock.now());
                    inner.scheduler.set_dispatch_active(control.id, false);
                    let result = crate::spi::panic_boundary::catch_spi_call(
                        inner.provider_id.as_str(),
                        "receive",
                        Some(subscriber_id.as_str()),
                        || spi_subscription.receive(wait),
                    )
                    .and_then(identity);
                    inner.scheduler.set_dispatch_active(control.id, true);
                    match result {
                        Ok(ReceiveOutcome::Message(message)) => {
                            let (
                                address,
                                event_id,
                                timestamp,
                                headers,
                                ordering_key,
                                payload,
                                token,
                                metadata,
                            ) = message.into_parts();
                            let decoded = decode_payload(
                                codec.as_ref(),
                                &payload,
                                inner.facade_config.payload_limits().max_receive_bytes(),
                            );
                            if let Err(error) = &decoded
                                && crate::codec::receive_failure_action(error)
                                    == crate::codec::ReceiveFailureAction::StopUnsettled
                            {
                                let error = decoded.err().expect("matched decode error");
                                let text = error.to_string();
                                let first = control.fail_receive(SubscriptionStopReason::Codec {
                                    event_id,
                                    error: Arc::new(error),
                                });
                                inner.scheduler.cancel_subscription(control.id);
                                drop(token);
                                count_abandoned(&inner, control.id);
                                drop(lease_guard);
                                if first {
                                    inner.emit_internal("receive_boundary", text);
                                }
                                continue;
                            }
                            let lane = if options.ordering_policy()
                                == crate::model::OrderingPolicy::PerKey
                            {
                                Some(OrderingLaneKey::new(
                                    topic.name(),
                                    ordering_key.as_ref().map(crate::spi::OrderingKey::as_str),
                                    control.id,
                                ))
                            } else {
                                None
                            };
                            if let Err(error) = decoded {
                                owned.insert(
                                    lease,
                                    OwnedSyncDelivery {
                                        _lease: lease_guard,
                                        _tracker: inner.tracker.track_delivery(topic.name()),
                                        inbound: None,
                                        settlement: None,
                                        handler_finished: true,
                                        settlement_granted: false,
                                        abandoned: false,
                                        lifecycle_failed: false,
                                        retry: SettlementRetryState::new(
                                            inner.facade_config.settlement_retry(),
                                        ),
                                        first_attempt: None,
                                        next_attempt: Duration::ZERO,
                                        last_error: None,
                                    },
                                );
                                let router = OwnerSettlementRouter {
                                    lease_id: lease,
                                    sender: sender.clone(),
                                    inner: Arc::downgrade(&inner),
                                };
                                crate::facade::event_bus::failure::settle_rejected(
                                    &inner,
                                    &router,
                                    token,
                                    control.id,
                                    &subscriber_id,
                                    event_id,
                                    address.as_str(),
                                    error.into(),
                                );
                                inner.scheduler.enqueue_settlement(lease, lane);
                                continue;
                            }
                            let message = InboundMessage::new(
                                address,
                                event_id,
                                timestamp,
                                headers,
                                ordering_key,
                                payload,
                                token,
                                metadata,
                            );
                            owned.insert(
                                lease,
                                OwnedSyncDelivery {
                                    _lease: lease_guard,
                                    _tracker: inner.tracker.track_delivery(topic.name()),
                                    inbound: Some((message, decoded)),
                                    settlement: None,
                                    handler_finished: false,
                                    settlement_granted: true,
                                    abandoned: false,
                                    lifecycle_failed: false,
                                    retry: SettlementRetryState::new(
                                        inner.facade_config.settlement_retry(),
                                    ),
                                    first_attempt: None,
                                    next_attempt: Duration::ZERO,
                                    last_error: None,
                                },
                            );
                            inner.scheduler.enqueue(lease, lane);
                        }
                        Ok(ReceiveOutcome::TimedOut) => drop(lease_guard),
                        Ok(ReceiveOutcome::Closed) => {
                            drop(lease_guard);
                            receive_closed = true;
                        }
                        Ok(ReceiveOutcome::Gap(gap)) => {
                            drop(lease_guard);
                            let stop_gap = (options.gap_policy() == crate::model::GapPolicy::Stop)
                                .then(|| Arc::new(gap.clone()));
                            inner.emit(Diagnostic::ReceiveGap {
                                subscription_id: control.id,
                                subscriber_id: subscriber_id.clone(),
                                topic: topic.name().into(),
                                gap,
                            });
                            if let Some(gap) = stop_gap {
                                control.fail_receive(SubscriptionStopReason::Gap { gap });
                                inner.scheduler.cancel_subscription(control.id);
                            }
                        }
                        Err(error) => {
                            drop(lease_guard);
                            let text = error.to_string();
                            let first = control.fail_receive(SubscriptionStopReason::Provider {
                                error: Arc::new(error),
                            });
                            inner.scheduler.cancel_subscription(control.id);
                            if first {
                                inner.emit_internal("receive", text);
                            }
                        }
                    }
                    continue;
                }
                if inner.scheduler.lease_ids_exhausted() {
                    fail_internal(&inner, &control, "lease_id_exhausted");
                    continue;
                }
            }
            park_timeout(wait);
        }
    }));
    if let Err(payload) = worker_result {
        fail_internal(&inner, &control, "owner_panicked");
        inner.emit_internal(
            "subscription_worker",
            panic_message(payload.as_ref()).into(),
        );
        // Preserve payload/tracker lifetime even when an injected clock or an
        // internal operation unwinds while pool callbacks are still running.
        while owned
            .values()
            .any(|delivery| delivery.inbound.is_none() && !delivery.handler_finished)
        {
            match receiver.recv() {
                Ok(CoordinatorMessage::Abandoned(lease)) => {
                    if let Some(delivery) = owned.get_mut(&lease) {
                        delivery.abandoned = true;
                    }
                }
                Ok(CoordinatorMessage::HandlerFinished(lease)) => {
                    if let Some(delivery) = owned.get_mut(&lease) {
                        delivery.handler_finished = true;
                    }
                }
                Ok(message @ CoordinatorMessage::Settlement { lease_id, .. }) => {
                    if let Some(delivery) = owned.get_mut(&lease_id)
                        && delivery.settlement.is_none()
                    {
                        delivery.settlement = Some(message);
                    }
                }
                Err(_) => break,
            }
        }
    }
    if let Err(error) = close_spi_subscription(&inner, &subscriber_id, &mut *spi_subscription) {
        inner.emit_internal("subscription_close", error.to_string());
        let failure = Arc::new(SubscriptionCloseFailure::new(subscriber_id.clone(), error));
        control.record_close_error(failure.clone());
        inner
            .close_errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(failure);
    }
    // Provider close is the recovery boundary for every unresolved token.
    for (_, delivery) in owned.drain() {
        if delivery.settlement.is_some() || delivery.inbound.is_some() {
            count_abandoned(&inner, control.id);
        }
        drop(delivery);
    }
}

/// Extracts the immutable disposition for duplicate-intent invariant checks.
///
/// # Parameters
/// - `message`: owner-channel notification to inspect.
///
/// # Returns
/// Some immutable disposition for settlement messages, otherwise None.
#[inline]
fn settlement_disposition(message: &CoordinatorMessage) -> Option<DeliveryDisposition> {
    match message {
        CoordinatorMessage::Settlement { disposition, .. } => Some(*disposition),
        _ => None,
    }
}

/// Publishes an internal invariant failure as a structured first terminal
/// cause.
///
/// # Parameters
/// - `inner`: bus that fences the owner and emits diagnostics.
/// - `control`: first-cause terminal state to publish before callbacks.
/// - `kind`: stable invariant-failure identifier.
fn fail_internal(inner: &EventBusInner, control: &SubscriptionControl, kind: &'static str) {
    let error = SpiError::Operation {
        provider_id: inner.provider_id.as_str().into(),
        operation: "delivery_owner",
        resource: Some(control.subscriber_id.as_str().into()),
        kind,
        retryable: Some(false),
        source: Box::new(std::io::Error::other(kind)),
    };
    let first = control.fail_receive(SubscriptionStopReason::Provider {
        error: Arc::new(error),
    });
    inner.scheduler.cancel_subscription(control.id);
    if first {
        inner.emit_internal("delivery_owner", kind.into());
    }
}

/// Performs at most one due provider call; returns the next bounded wait when
/// retry remains. All calls remain on the receiver thread, and the
/// token/disposition stay unchanged.
///
/// # Type Parameters
/// - `T`: payload type retained by the owner metadata.
///
/// # Parameters
/// - `inner`: provider identity, clock and observer callbacks.
/// - `control`: terminal state and cumulative counters.
/// - `receiver`: exclusively owned provider receiver.
/// - `delivery`: immutable intent and finite retry state to advance.
/// - `stopping`: permits at most one fresh best-effort cancellation settlement.
///
/// # Returns
/// Some bounded delay when another attempt is due later; None after completion
/// or stop.
///
/// # Side Effects
/// May call provider settlement and observers. Failures publish structured
/// terminal context; ownership remains until success or provider close
/// recovery.
fn attempt_settlement<T>(
    inner: &EventBusInner,
    control: &SubscriptionControl,
    receiver: &mut dyn crate::spi::EventSubscriptionSpi,
    delivery: &mut OwnedSyncDelivery<'_, T>,
    stopping: bool,
) -> Option<Duration> {
    let CoordinatorMessage::Settlement {
        token,
        disposition,
        event_id,
        topic,
        subscription_id,
        subscriber_id,
        ..
    } = delivery.settlement.as_ref()?
    else {
        return None;
    };
    let Some(token) = token.as_ref() else {
        delivery.settlement = None;
        return None;
    };
    if !token.belongs_to(*subscription_id) {
        let error = Arc::new(SpiError::InvalidSettlementToken {
            provider_id: inner.provider_id.as_str().into(),
            operation: "settle",
            resource: Some(subscriber_id.as_str().into()),
            reason: "foreign_owner",
            retryable: Some(false),
            source: Box::new(std::io::Error::other(
                "provider token belongs to another subscription",
            )),
        });
        stop_settlement(
            inner,
            control,
            event_id.clone(),
            topic,
            *disposition,
            delivery.retry.attempts(),
            SettlementTermination::InvalidToken,
            error,
        );
        return None;
    }
    let now = inner.clock.now();
    let started = *delivery.first_attempt.get_or_insert(now);
    let elapsed = match now.duration_since(started) {
        Ok(elapsed) => elapsed,
        Err(error) => {
            let error = Arc::new(SpiError::Operation {
                provider_id: inner.provider_id.as_str().into(),
                operation: "settlement_clock",
                resource: Some(subscriber_id.as_str().into()),
                kind: "invalid_monotonic_clock",
                retryable: Some(false),
                source: Box::new(error),
            });
            stop_settlement(
                inner,
                control,
                event_id.clone(),
                topic,
                *disposition,
                delivery.retry.attempts(),
                SettlementTermination::InfrastructureFailure,
                error,
            );
            return None;
        }
    };
    if elapsed < delivery.next_attempt {
        return Some(delivery.next_attempt - elapsed);
    }
    let attempt = match delivery.retry.admit_attempt(elapsed) {
        Ok(attempt) => attempt,
        Err(termination) => {
            if let Some(error) = delivery.last_error.as_ref() {
                control.delivery_metrics.record_settlement_elapsed(elapsed);
                stop_settlement(
                    inner,
                    control,
                    event_id.clone(),
                    topic,
                    *disposition,
                    delivery.retry.attempts(),
                    termination,
                    error.clone(),
                );
            } else {
                fail_internal(inner, control, "settlement_budget_before_first_attempt");
            }
            return None;
        }
    };
    inner.scheduler.set_dispatch_active(control.id, false);
    control.delivery_metrics.record_settlement_attempt(attempt);
    let result = crate::spi::panic_boundary::catch_spi_call(
        inner.provider_id.as_str(),
        "settle",
        Some(subscriber_id.as_str()),
        || receiver.settle(token, *disposition),
    )
    .and_then(identity);
    inner.scheduler.set_dispatch_active(control.id, true);
    match result {
        Ok(()) => {
            if let Err(error) = control
                .delivery_metrics
                .record_settlement_duration(started, inner.clock.now())
            {
                // The SPI action succeeded: discard its intent below, but do
                // not report an infrastructure-failed lifecycle as completed.
                delivery.lifecycle_failed = true;
                let error = Arc::new(SpiError::Operation {
                    provider_id: inner.provider_id.as_str().into(),
                    operation: "settlement_clock",
                    resource: Some(subscriber_id.as_str().into()),
                    kind: "invalid_monotonic_clock",
                    retryable: Some(false),
                    source: Box::new(error),
                });
                stop_settlement(
                    inner,
                    control,
                    event_id.clone(),
                    topic,
                    *disposition,
                    attempt,
                    SettlementTermination::InfrastructureFailure,
                    error,
                );
            }
            delivery.settlement = None;
            None
        }
        Err(error) => {
            let error = Arc::new(error);
            let failed = Diagnostic::SettlementFailed {
                event_id: event_id.clone(),
                topic: topic.clone(),
                subscription_id: *subscription_id,
                subscriber_id: subscriber_id.clone(),
                disposition: *disposition,
                attempt,
                error: error.clone(),
            };
            let elapsed = match inner.clock.now().duration_since(started) {
                Ok(elapsed) => elapsed,
                Err(source) => {
                    let clock_error = Arc::new(SpiError::Operation {
                        provider_id: inner.provider_id.as_str().into(),
                        operation: "settlement_clock",
                        resource: Some(subscriber_id.as_str().into()),
                        kind: "invalid_monotonic_clock",
                        retryable: Some(false),
                        source: Box::new(source),
                    });
                    stop_settlement_with_failure(
                        inner,
                        control,
                        event_id.clone(),
                        topic,
                        *disposition,
                        attempt,
                        SettlementTermination::InfrastructureFailure,
                        clock_error,
                        Some(failed),
                    );
                    return None;
                }
            };
            delivery.last_error = Some(error.clone());
            match delivery.retry.after_error(&error, elapsed) {
                SettlementRetryDecision::RetryAfter(delay) => {
                    delivery.next_attempt = elapsed.saturating_add(delay);
                    inner.emit(failed);
                    if stopping { None } else { Some(delay) }
                }
                SettlementRetryDecision::Stop(termination) => {
                    control.delivery_metrics.record_settlement_elapsed(elapsed);
                    stop_settlement_with_failure(
                        inner,
                        control,
                        event_id.clone(),
                        topic,
                        *disposition,
                        attempt,
                        termination,
                        error,
                        Some(failed),
                    );
                    None
                }
            }
        }
    }
}

/// Records only the first terminal settlement cause, sharing the original error
/// with diagnostics.
///
/// # Parameters
/// - `inner`: bus scheduler and diagnostic observers.
/// - `control`: subscription to stop before invoking observers.
/// - `event_id`: delivery whose settlement terminated.
/// - `topic`: diagnostic topic name.
/// - `disposition`: immutable intended provider action.
/// - `attempts`: actual SPI calls already attempted.
/// - `termination`: structured terminal classification.
/// - `error`: canonical failure shared with terminal state and diagnostics.
fn stop_settlement(
    inner: &EventBusInner,
    control: &SubscriptionControl,
    event_id: crate::EventId,
    topic: &str,
    disposition: DeliveryDisposition,
    attempts: u32,
    termination: SettlementTermination,
    error: Arc<SpiError>,
) {
    stop_settlement_with_failure(
        inner,
        control,
        event_id,
        topic,
        disposition,
        attempts,
        termination,
        error,
        None,
    );
}

/// Publishes terminal state before either attempt or terminal observer
/// callbacks.
///
/// # Parameters
/// - `inner`: scheduler and observer context.
/// - `control`: subscription retaining the canonical terminal cause.
/// - `event_id`: affected delivery identity.
/// - `topic`: diagnostic destination.
/// - `disposition`: unchanged provider action.
/// - `attempts`: actual SPI attempt count.
/// - `termination`: structured reason for stopping.
/// - `error`: terminal source shared by reason and stopped diagnostic.
/// - `failed`: optional actual attempt diagnostic emitted before
///   SettlementStopped.
///
/// # Side Effects
/// Fences admission before invoking callbacks, retaining Failed then Stopped
/// order.
fn stop_settlement_with_failure(
    inner: &EventBusInner,
    control: &SubscriptionControl,
    event_id: crate::EventId,
    topic: &str,
    disposition: DeliveryDisposition,
    attempts: u32,
    termination: SettlementTermination,
    error: Arc<SpiError>,
    failed: Option<Diagnostic>,
) {
    control.delivery_metrics.record_terminal_failure();
    let first = control.fail_receive(SubscriptionStopReason::Settlement {
        event_id: event_id.clone(),
        disposition,
        attempts,
        termination,
        error: error.clone(),
    });
    inner.scheduler.cancel_subscription(control.id);
    if let Some(failed) = failed {
        inner.emit(failed);
    }
    if first {
        inner.emit(Diagnostic::SettlementStopped {
            event_id,
            topic: topic.into(),
            subscription_id: control.id,
            subscriber_id: control.subscriber_id.clone(),
            disposition,
            attempts,
            termination,
            error,
        });
    }
}

/// Counts an unresolved facade-known ephemeral delivery exactly at its cleanup
/// path.
///
/// # Parameters
/// - `inner`: bus whose capability determines ephemeral accounting.
/// - `subscription_id`: active owner whose counters receive the same increment.
pub(in crate::facade) fn count_abandoned(inner: &EventBusInner, subscription_id: Id) {
    if inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral {
        inner.abandoned_deliveries.fetch_add(1, Ordering::AcqRel);
        if let Some(control) = inner
            .subscription_snapshot()
            .into_iter()
            .find(|control| control.id == subscription_id)
        {
            control.delivery_metrics.record_abandoned_ephemeral();
        }
    }
}

/// Creates one retry intent for canceled queued work, or diagnoses unavailable
/// settlement.
///
/// # Parameters
/// - `inner`: capability and diagnostic context.
/// - `lease_id`: owned lease retaining this message.
/// - `subscription_id`: receiver that issued the token.
/// - `subscriber_id`: logical identity used by diagnostics.
/// - `message`: unstarted payload consumed at cancellation.
///
/// # Returns
/// Some Retry intent for a capable provider token; None after unavailable
/// recovery is diagnosed and ephemeral abandonment is counted.
fn canceled_intent(
    inner: &EventBusInner,
    lease_id: u64,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    message: InboundMessage,
) -> Option<CoordinatorMessage> {
    let (address, event_id, _, _, _, _, token, _) = message.into_parts();
    if token.is_some()
        && inner.capabilities.settlement() == crate::spi::SettlementCapabilities::AcceptRetryReject
    {
        return Some(CoordinatorMessage::Settlement {
            lease_id,
            token,
            disposition: DeliveryDisposition::Retry,
            event_id,
            topic: address.as_str().into(),
            subscription_id,
            subscriber_id: subscriber_id.clone(),
        });
    }
    count_abandoned(inner, subscription_id);
    inner.emit(Diagnostic::SettlementUnavailable {
        event_id,
        topic: address.as_str().into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        requested: DeliveryDisposition::Retry,
    });
    None
}

/// Routes canceled granted work to its owner without blocking the actual pool
/// worker.
///
/// # Parameters
/// - `inner`: bus capabilities and counters.
/// - `router`: nonblocking channel to the original receiver.
/// - `subscription_id`: receiver that issued the token.
/// - `subscriber_id`: subscriber identity for diagnostics.
/// - `message`: granted payload stopped before pipeline execution.
///
/// # Side Effects
/// Sends Retry or explicit abandonment; never calls the provider from the pool.
pub(in crate::facade) fn requeue_unstarted_message_via_owner(
    inner: &EventBusInner,
    router: &OwnerSettlementRouter,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    message: InboundMessage,
) {
    if let Some(CoordinatorMessage::Settlement {
        token,
        disposition,
        event_id,
        topic,
        ..
    }) = canceled_intent(
        inner,
        router.lease_id,
        subscription_id,
        subscriber_id,
        message,
    ) {
        router.settle(
            token,
            disposition,
            event_id,
            &topic,
            subscription_id,
            subscriber_id,
        );
    } else {
        router.abandon(subscription_id);
    }
}
