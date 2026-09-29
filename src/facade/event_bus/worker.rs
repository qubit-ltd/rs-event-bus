// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-owner receive and settlement coordination.

#![allow(clippy::too_many_arguments)]
use super::internal::close_spi_subscription;
use crate::DeliveryError;
use crate::Diagnostic;
use crate::EventId;
use crate::SubscriberId;
use crate::facade::DeliveryTrackerGuard;
use crate::facade::SubscriptionControl;
use crate::facade::event_bus::Arc;
use crate::facade::event_bus::CoordinatorMessage;
use crate::facade::event_bus::Duration;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::HashMap;
use crate::facade::event_bus::Id;
use crate::facade::event_bus::Ordering;
use crate::facade::event_bus::OwnerSettlementRouter;
use crate::facade::event_bus::SubscriptionCloseFailure;
use crate::facade::event_bus::VecDeque;
use crate::facade::event_bus::delivery::process_inbound;
use crate::facade::event_bus::failure::panic_message;
use crate::facade::event_bus::mpsc;
use crate::facade::internal::BusContextGuard;
use crate::facade::internal::LifecycleState;
use crate::facade::lifecycle::receive_poll_interval;
use crate::model::Delivery;
use crate::model::Topic;
use crate::pipeline::OrderingLaneKey;
use crate::spi::DeliveryDisposition;
use crate::spi::InboundMessage;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementToken;

pub(in crate::facade) fn run_subscription_worker<T>(
    inner: Arc<EventBusInner>,
    bus_identity: usize,
    control: Arc<SubscriptionControl>,
    mut spi_subscription: Box<dyn crate::spi::EventSubscriptionSpi>,
    topic: Topic<T>,
    codec: Option<Arc<dyn crate::codec::EventCodec<T>>>,
    subscriber_id: SubscriberId,
    options: crate::model::SubscribeOptions<T>,
    handler: Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
) where
    T: Send + Sync + 'static,
{
    let _worker_context = BusContextGuard::enter(bus_identity);
    let worker_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (message_tx, message_rx) = mpsc::channel();
        let mut pending: Option<(usize, InboundMessage, DeliveryTrackerGuard<'_>)> = None;
        let mut pending_settlements = VecDeque::new();
        let mut active: HashMap<usize, DeliveryTrackerGuard<'_>> = HashMap::new();
        let mut next_task = 1usize;
        let mut receive_closed = false;
        loop {
            while let Ok(message) = message_rx.try_recv() {
                match message {
                    settlement @ CoordinatorMessage::Settlement { .. } => {
                        pending_settlements.push_back(settlement);
                    }
                    CoordinatorMessage::TaskFinished(task_id) => {
                        active.remove(&task_id);
                    }
                }
            }

            let stopping = control.is_cancelled();
            if stopping {
                receive_closed = true;
                if let Some((_, message, _guard)) = pending.take() {
                    requeue_unstarted_message(&inner, &mut pending_settlements, control.id, &subscriber_id, message);
                }
            }

            // Keep failed settlements, including their non-cloneable provider
            // token, until the provider accepts the same idempotent disposition.
            // Retry only once per owner-loop iteration so receive can drain a
            // full bounded local queue that may be blocking a Requeue.
            if let Some(settlement) = pending_settlements.pop_front()
                && !apply_queued_settlement(&inner, &mut *spi_subscription, &settlement)
            {
                if stopping {
                    release_settlement_waiter(&settlement);
                } else {
                    pending_settlements.push_back(settlement);
                }
            }

            if let Some((task_id, message, guard)) = pending.take() {
                let ordering_key = if options.ordering_policy() == crate::model::OrderingPolicy::PerKey {
                    Some(OrderingLaneKey::new(
                        topic.name(),
                        message.ordering_key().map(crate::spi::OrderingKey::as_str),
                        control.id,
                    ))
                } else {
                    None
                };
                if let Some(reservation) = inner.scheduler.try_reserve(control.id, ordering_key) {
                    let task_subscription_id = control.id;
                    let task_inner = inner.clone();
                    let task_topic = topic.clone();
                    let task_codec = codec.clone();
                    let task_options = options.clone();
                    let task_handler = handler.clone();
                    let task_subscriber_id = subscriber_id.clone();
                    let task_sender = message_tx.clone();
                    let task_id_for_job = task_id;
                    reservation.submit(move |cancelled| {
                        let _task_context = BusContextGuard::enter(bus_identity);
                        let task_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            if cancelled {
                                requeue_unstarted_message_via_owner(
                                    &task_inner,
                                    &OwnerSettlementRouter {
                                        sender: task_sender.clone(),
                                    },
                                    task_subscription_id,
                                    &task_subscriber_id,
                                    message,
                                );
                            } else {
                                process_inbound(
                                    &task_inner,
                                    &OwnerSettlementRouter {
                                        sender: task_sender.clone(),
                                    },
                                    task_subscription_id,
                                    &task_subscriber_id,
                                    &task_topic,
                                    task_codec.as_ref(),
                                    &task_options,
                                    &task_handler,
                                    message,
                                );
                            }
                        }));
                        if let Err(payload) = task_result {
                            task_inner.emit_internal("delivery_worker", panic_message(payload.as_ref()).into());
                        }
                        let _ = task_sender.send(CoordinatorMessage::TaskFinished(task_id_for_job));
                    });
                    active.insert(task_id, guard);
                } else {
                    pending = Some((task_id, message, guard));
                }
            }

            if receive_closed && active.is_empty() && pending.is_none() && pending_settlements.is_empty() {
                break;
            }

            if pending.is_none() && !receive_closed {
                match crate::spi::panic_boundary::catch_spi_call(
                    inner.provider_id.as_str(),
                    "receive",
                    Some(subscriber_id.as_str()),
                    || {
                        spi_subscription.receive(if receive_closed {
                            Duration::ZERO
                        } else {
                            receive_poll_interval()
                        })
                    },
                )
                .and_then(std::convert::identity)
                {
                    Ok(ReceiveOutcome::TimedOut) => {}
                    Ok(ReceiveOutcome::Closed) => receive_closed = true,
                    Ok(ReceiveOutcome::Gap(gap)) => inner.emit(Diagnostic::ReceiveGap {
                        subscription_id: control.id,
                        subscriber_id: subscriber_id.clone(),
                        topic: topic.name().into(),
                        gap,
                    }),
                    Ok(ReceiveOutcome::Message(message)) => {
                        if control.is_cancelled() || receive_closed {
                            requeue_unstarted_message(
                                &inner,
                                &mut pending_settlements,
                                control.id,
                                &subscriber_id,
                                message,
                            );
                            receive_closed = true;
                        } else {
                            let task_id = next_task;
                            next_task = next_task.wrapping_add(1).max(1);
                            let guard = inner.tracker.track_delivery(topic.name());
                            pending = Some((task_id, message, guard));
                        }
                    }
                    Err(error) => {
                        inner.emit_internal("receive", error.to_string());
                        receive_closed = true;
                    }
                }
            } else if !active.is_empty() || pending.is_some() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }));
    if let Err(payload) = worker_result {
        inner.emit_internal("subscription_worker", panic_message(payload.as_ref()).into());
    }
    if let Err(error) = close_spi_subscription(&inner, &subscriber_id, &mut *spi_subscription) {
        inner.emit_internal("subscription_close", error.to_string());
        let failure = Arc::new(SubscriptionCloseFailure::new(subscriber_id.clone(), error));
        control.record_close_error(failure.clone());
        inner
            .close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(failure);
    }
    inner.scheduler.finish_subscription(control.id);
    inner.tracker.worker_finished();
    inner
        .subscriptions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&control.id);
    let mut lifecycle = inner
        .lifecycle
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if *lifecycle == LifecycleState::Closing
        && inner.tracker.workers_are_idle()
        && inner
            .shutdown_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .report
            .is_some()
    {
        *lifecycle = LifecycleState::Closed;
    }
    drop(lifecycle);
    control.mark_finished();
}

pub(in crate::facade) fn apply_owner_settlement(
    inner: &EventBusInner,
    spi_subscription: &mut dyn crate::spi::EventSubscriptionSpi,
    token: &SettlementToken,
    disposition: DeliveryDisposition,
    event_id: EventId,
    topic: &str,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
) -> bool {
    if !token.belongs_to(subscription_id) {
        inner.emit_internal(
            "settlement",
            "provider settlement token belongs to another subscription".into(),
        );
        return true;
    }
    let result = crate::spi::panic_boundary::catch_spi_call(
        inner.provider_id.as_str(),
        "settle",
        Some(subscriber_id.as_str()),
        || spi_subscription.settle(token, disposition),
    )
    .and_then(std::convert::identity);
    match result {
        Ok(()) => true,
        Err(error) => {
            inner.emit(Diagnostic::SettlementFailed {
                event_id: event_id.clone(),
                topic: topic.into(),
                subscription_id,
                subscriber_id: subscriber_id.clone(),
                disposition,
                error: error.to_string().into(),
            });
            if error.kind() == "provider_panicked" {
                inner.emit(Diagnostic::SettlementUnavailable {
                    event_id,
                    topic: topic.into(),
                    subscription_id,
                    subscriber_id: subscriber_id.clone(),
                    requested: disposition,
                });
                true
            } else {
                false
            }
        }
    }
}

pub(in crate::facade) fn apply_queued_settlement(
    inner: &EventBusInner,
    spi_subscription: &mut dyn crate::spi::EventSubscriptionSpi,
    message: &CoordinatorMessage,
) -> bool {
    let CoordinatorMessage::Settlement {
        token,
        disposition,
        event_id,
        topic,
        subscription_id,
        subscriber_id,
        settled,
    } = message
    else {
        return true;
    };
    let complete = token.as_ref().is_none_or(|token| {
        apply_owner_settlement(
            inner,
            spi_subscription,
            token,
            *disposition,
            event_id.clone(),
            topic,
            *subscription_id,
            subscriber_id,
        )
    });
    if complete && let Some(settled) = settled {
        let _ = settled.send(());
    }
    complete
}

/// Unblocks a facade worker after its final best-effort settlement attempt
/// fails during cancellation; receiver close then owns unresolved delivery
/// recovery according to the provider's SPI contract.
pub(in crate::facade) fn release_settlement_waiter(message: &CoordinatorMessage) {
    if let CoordinatorMessage::Settlement {
        settled: Some(settled), ..
    } = message
    {
        let _ = settled.send(());
    }
}

/// Requeues a delivery canceled before its handler starts through the SPI
/// owner.
pub(in crate::facade) fn requeue_unstarted_message_via_owner(
    inner: &EventBusInner,
    router: &OwnerSettlementRouter,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    message: InboundMessage,
) {
    let (address, event_id, _, _, _, _, token, _) = message.into_parts();
    let capability = inner.capabilities.settlement();
    if let Some(token) = token {
        if !token.belongs_to(subscription_id) {
            inner.emit_internal(
                "settlement",
                "provider settlement token belongs to another subscription".into(),
            );
            return;
        }
        if capability == crate::spi::SettlementCapabilities::AcceptRetryReject {
            router.settle(
                Some(token),
                DeliveryDisposition::Retry,
                event_id,
                address.as_str(),
                subscription_id,
                subscriber_id,
            );
            return;
        }
    }
    if inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral {
        inner.abandoned_deliveries.fetch_add(1, Ordering::AcqRel);
    }
    inner.emit(Diagnostic::SettlementUnavailable {
        event_id,
        topic: address.as_str().into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        requested: DeliveryDisposition::Retry,
    });
}

/// Retains a message received at the cancellation boundary for provider
/// requeue without invoking the subscriber handler.
pub(in crate::facade) fn requeue_unstarted_message(
    inner: &Arc<EventBusInner>,
    pending_settlements: &mut VecDeque<CoordinatorMessage>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    message: InboundMessage,
) {
    let (address, event_id, _, _, _, _, token, _) = message.into_parts();
    let capability = inner.capabilities.settlement();
    if let Some(token) = token {
        if !token.belongs_to(subscription_id) {
            inner.emit_internal(
                "settlement",
                "provider settlement token belongs to another subscription".into(),
            );
            return;
        }
        if capability == crate::spi::SettlementCapabilities::AcceptRetryReject {
            let (settled, wait) = mpsc::sync_channel(0);
            drop(wait);
            pending_settlements.push_back(CoordinatorMessage::Settlement {
                token: Some(token),
                disposition: DeliveryDisposition::Retry,
                event_id,
                topic: address.as_str().into(),
                subscription_id,
                subscriber_id: subscriber_id.clone(),
                settled: Some(settled),
            });
            return;
        }
    }
    if inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral {
        inner.abandoned_deliveries.fetch_add(1, Ordering::AcqRel);
    }
    inner.emit(Diagnostic::SettlementUnavailable {
        event_id,
        topic: address.as_str().into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        requested: DeliveryDisposition::Retry,
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::SystemTime;

    use super::CoordinatorMessage;
    use super::EventId;
    use super::Id;
    use super::OwnerSettlementRouter;
    use super::SubscriberId;
    use super::apply_queued_settlement;
    use super::mpsc;
    use super::release_settlement_waiter;
    use super::requeue_unstarted_message;
    use super::requeue_unstarted_message_via_owner;
    use crate::model::Headers;
    use crate::model::ProviderMessageMetadata;
    use crate::model::ProviderOptions;
    use crate::model::StartPosition;
    use crate::model::SubscriptionDurability;
    use crate::spi::DeliveryDisposition;
    use crate::spi::InboundMessage;
    use crate::spi::SpiSubscriptionRequest;
    use crate::spi::TopicAddress;
    use crate::spi::TransportPayload;

    #[test]
    fn cancellation_releases_only_a_waiting_settlement_sender() {
        let (settled, receiver) = mpsc::sync_channel(1);
        let message = CoordinatorMessage::Settlement {
            token: None,
            disposition: DeliveryDisposition::Retry,
            event_id: EventId::new("cancelled-delivery").unwrap(),
            topic: "orders.created".into(),
            subscription_id: Id::new(7),
            subscriber_id: SubscriberId::new("consumer").unwrap(),
            settled: Some(settled),
        };
        release_settlement_waiter(&message);
        assert!(receiver.try_recv().is_ok());

        release_settlement_waiter(&CoordinatorMessage::TaskFinished(7));
    }

    #[test]
    fn queued_task_completion_does_not_attempt_provider_settlement() {
        let bus = crate::facade::EventBus::local(crate::local::LocalEventBusConfig::default()).unwrap();
        let request = SpiSubscriptionRequest::new(
            Id::new(9),
            TopicAddress::new("worker.task-finished").unwrap(),
            SubscriberId::new("worker").unwrap(),
            None,
            SubscriptionDurability::Ephemeral,
            StartPosition::New,
            ProviderOptions::new(),
            std::any::TypeId::of::<()>(),
        );
        let mut subscription = bus.inner.spi.subscribe(request).unwrap();

        assert!(apply_queued_settlement(
            &bus.inner,
            &mut *subscription,
            &CoordinatorMessage::TaskFinished(9),
        ));
        let already_settled = CoordinatorMessage::Settlement {
            token: None,
            disposition: DeliveryDisposition::Accept,
            event_id: EventId::new("already-settled").unwrap(),
            topic: "worker.task-finished".into(),
            subscription_id: Id::new(9),
            subscriber_id: SubscriberId::new("worker").unwrap(),
            settled: None,
        };
        assert!(apply_queued_settlement(
            &bus.inner,
            &mut *subscription,
            &already_settled
        ));
    }

    #[test]
    fn cancelled_unstarted_ephemeral_delivery_is_counted_as_abandoned() {
        let bus = crate::facade::EventBus::local(crate::local::LocalEventBusConfig::default()).unwrap();
        let inner = bus.inner.clone();
        let subscriber_id = SubscriberId::new("worker").unwrap();
        let message = || {
            InboundMessage::new(
                TopicAddress::new("worker.cancelled").unwrap(),
                EventId::new("cancelled-message").unwrap(),
                SystemTime::UNIX_EPOCH,
                Headers::new(),
                None,
                TransportPayload::Native(Arc::new(())),
                None,
                ProviderMessageMetadata::new(),
            )
        };
        requeue_unstarted_message(&inner, &mut Default::default(), Id::new(1), &subscriber_id, message());
        let (sender, _receiver) = mpsc::channel();
        requeue_unstarted_message_via_owner(
            &inner,
            &OwnerSettlementRouter { sender },
            Id::new(1),
            &subscriber_id,
            message(),
        );

        assert_eq!(inner.abandoned_deliveries.load(Ordering::Acquire), 2);
    }
}
