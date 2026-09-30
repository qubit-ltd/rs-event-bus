// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async session delivery creation, decoding, and settlement.

#![allow(clippy::too_many_arguments)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::Diagnostic;
use crate::SubscriberId;
use crate::codec::EventCodec;
use crate::codec::ReceiveFailureAction;
use crate::codec::decode_payload;
use crate::codec::receive_failure_action;
use crate::facade::async_subscription::AsyncEventBusInner;
use crate::facade::async_subscription::AsyncSession;
use crate::facade::async_subscription::Id;
use crate::facade::async_subscription::SessionSignals;
use crate::facade::async_subscription::SharedAsyncHandler;
use crate::facade::async_subscription::internal::OwnedDeliveryTask;
use crate::facade::async_subscription::internal::PendingDelivery;
use crate::facade::async_subscription::internal::delivery_task_context::DeliveryTaskContext;
use crate::model::EventEnvelope;
use crate::model::EventId;
use crate::model::SubscribeOptions;
use crate::model::SubscriptionStopReason;
use crate::model::Topic;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::InboundMessage;

impl<T: Send + Sync + 'static> AsyncSession<T> {
    /// Creates a session from all owned runtime and subscription state.
    ///
    /// # Parameters
    /// - `inner`: shared bus lifecycle and provider state.
    /// - `id`: bus-local subscription ID.
    /// - `subscriber_id`: logical subscriber identity.
    /// - `topic`: typed event source.
    /// - `codec`: codec retained for encoded messages, if configured.
    /// - `options`: handler and provider subscription policies.
    /// - `receiver`: single-owner provider receiver.
    /// - `signals`: shared stop and wake state.
    ///
    /// # Returns
    /// A session ready for a caller-driven runner.
    pub(in crate::facade) fn new(
        inner: Arc<AsyncEventBusInner>,
        id: Id,
        subscriber_id: SubscriberId,
        topic: Topic<T>,
        codec: Option<Arc<dyn EventCodec<T>>>,
        options: SubscribeOptions<T>,
        receiver: Box<dyn AsyncEventSubscriptionSpi>,
        signals: Arc<SessionSignals>,
    ) -> Self {
        let metrics = Arc::new(crate::facade::internal::DeliveryMetrics::new_subscription(
            inner.delivery_metrics.clone(),
        ));
        Self {
            metrics,
            inner,
            id,
            subscriber_id,
            topic,
            codec,
            options,
            receiver: Some(receiver),
            receiver_closed: false,
            signals,
            buffered: VecDeque::new(),
            tasks: Vec::new(),
            completed: VecDeque::new(),
            completed_during_settlement: VecDeque::new(),
            handler: None,
        }
    }

    /// Transfers the pending delivery into an owned handler task.
    ///
    /// # Parameters
    /// - `pending`: delivery transferred to the new handler task.
    /// - `handler`: callback retained by the new task.
    pub(in crate::facade) fn start_pending_task(
        &mut self,
        pending: PendingDelivery<T>,
        handler: SharedAsyncHandler<T>,
    ) {
        let original_handler = handler;
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        let signals = self.signals.clone();
        let handler: SharedAsyncHandler<T> = Arc::new(move |delivery| {
            let duration = super::super::handler_duration_guard::HandlerDurationGuard::new(
                inner.clone(),
                metrics.clone(),
                signals.clone(),
            );
            let future = original_handler(delivery);
            Box::pin(async move {
                let result = future.await;
                drop(duration);
                result
            })
        });
        let started = Arc::new(AtomicBool::new(false));
        let mut task = DeliveryTaskContext {
            inner: self.inner.clone(),
            id: self.id,
            subscriber_id: self.subscriber_id.clone(),
            options: self.options.clone(),
            signals: self.signals.clone(),
            pending: Some(pending),
            started: Some(started.clone()),
        };
        self.tasks.push(OwnedDeliveryTask {
            started,
            future: Box::pin(async move {
                task.process_pending(handler).await;
                task.pending
                    .take()
                    .expect("processing task retains delivery until completion")
            }),
        });
    }

    /// Decodes a provider message and records it as the pending delivery.
    ///
    /// # Parameters
    /// - `message`: received transport message and optional settlement token.
    /// - `lease`: owned scheduler lease retained with the resulting delivery.
    pub(in crate::facade) fn prepare_message(
        &mut self,
        message: InboundMessage,
        lease: super::super::owned_delivery_lease::OwnedDeliveryLease,
    ) {
        let tracking = self.inner.tracker.track(self.topic.name());
        let (address, event_id, timestamp, headers, ordering_key, transport_payload, token, provider_metadata) =
            message.into_parts();
        let lane = (self.options.ordering_policy() == crate::model::OrderingPolicy::PerKey).then(|| {
            crate::pipeline::OrderingLaneKey::new(
                self.topic.name(),
                ordering_key.as_ref().map(|key| key.as_str()),
                self.id,
            )
        });
        let payload = match decode_payload(
            self.codec.as_ref(),
            &transport_payload,
            self.inner.facade_config.payload_limits().max_receive_bytes(),
        ) {
            Ok(payload) => payload,
            Err(error) => {
                if receive_failure_action(&error) == ReceiveFailureAction::StopUnsettled {
                    let message = error.to_string();
                    self.record_abandoned_delivery();
                    if self.signals.fail_receive(SubscriptionStopReason::Codec {
                        event_id,
                        error: Arc::new(error),
                    }) {
                        self.inner.emit(&Diagnostic::InternalFailure {
                            origin: "receive_boundary".into(),
                            message: message.into(),
                        });
                    }
                    // Receiver owns recovery. No settlement callback observes this token.
                    drop(token);
                    return;
                }
                self.inner.scheduler.enqueue_settlement(lease.id, lane);
                self.inner.notify_scheduler();
                self.buffered.push_back(PendingDelivery {
                    _tracking: tracking,
                    event_id,
                    event: None,
                    token,
                    metadata: provider_metadata,
                    settlement_intent: Some(crate::spi::DeliveryDisposition::Reject),
                    settlement: super::super::settlement_progress::SettlementProgress::new(
                        self.inner.facade_config.settlement_retry(),
                    ),
                    failure_diagnostic: Some((0, error.to_string().into())),
                    lease,
                });
                let _ = address;
                return;
            }
        };
        let mut event = EventEnvelope::with_id_and_shared_payload(self.topic.clone(), payload, event_id);
        event.timestamp = timestamp;
        event.headers = headers;
        event.ordering_key = ordering_key.map(|key| key.as_str().into());
        let event = Arc::new(event);
        let event_id = event.id().clone();
        self.inner.scheduler.enqueue(lease.id, lane);
        self.inner.notify_scheduler();
        self.buffered.push_back(PendingDelivery {
            _tracking: tracking,
            event_id,
            event: Some(event),
            token,
            metadata: provider_metadata,
            settlement_intent: None,
            settlement: super::super::settlement_progress::SettlementProgress::new(
                self.inner.facade_config.settlement_retry(),
            ),
            failure_diagnostic: None,
            lease,
        });
    }

    /// Emits a terminal delivery failure after settlement completes.
    ///
    /// # Parameters
    /// - `failure`: attempt count and message, or `None` when no handler
    ///   failed.
    /// - `event_id`: stable event identity.
    /// - `topic`: event destination.
    pub(in crate::facade) fn emit_failure_diagnostic(
        &self,
        failure: Option<(u32, Box<str>)>,
        event_id: EventId,
        topic: Box<str>,
    ) {
        if let Some((attempts, error)) = failure {
            self.inner.emit(&Diagnostic::DeliveryFailed {
                event_id,
                topic,
                subscription_id: self.id,
                subscriber_id: self.subscriber_id.clone(),
                attempts,
                error,
            });
        }
    }
}
