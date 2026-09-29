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
use crate::facade::async_event_bus::catch_spi_future;
use crate::facade::async_subscription::AsyncEventBusInner;
use crate::facade::async_subscription::AsyncSession;
use crate::facade::async_subscription::Id;
use crate::facade::async_subscription::SessionSignals;
use crate::facade::async_subscription::SharedAsyncHandler;
use crate::facade::async_subscription::internal::OwnedDeliveryTask;
use crate::facade::async_subscription::internal::PendingDelivery;
use crate::facade::async_subscription::internal::delivery_task_context::DeliveryTaskContext;
use crate::model::EventEnvelope;
use crate::model::SubscribeOptions;
use crate::model::Topic;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::DeliveryDisposition;
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
        codec: Option<Arc<dyn crate::codec::EventCodec<T>>>,
        options: SubscribeOptions<T>,
        receiver: Box<dyn AsyncEventSubscriptionSpi>,
        signals: Arc<SessionSignals>,
    ) -> Self {
        Self {
            inner,
            id,
            subscriber_id,
            topic,
            codec,
            options,
            receiver: Some(receiver),
            receiver_closed: false,
            signals,
            pending: None,
            waiting_admission: None,
            tasks: Vec::new(),
            completed: VecDeque::new(),
            defer_settlement: false,
            handler: None,
            admission_waiter: None,
        }
    }

    /// Transfers the pending delivery into an owned handler task.
    ///
    /// # Parameters
    /// - `handler`: callback retained by the new task.
    pub(in crate::facade) fn start_pending_task(&mut self, handler: SharedAsyncHandler<T>) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let started = Arc::new(AtomicBool::new(false));
        let mut task = DeliveryTaskContext {
            inner: self.inner.clone(),
            id: self.id,
            subscriber_id: self.subscriber_id.clone(),
            topic: self.topic.clone(),
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
    pub(in crate::facade) fn prepare_message(&mut self, message: InboundMessage) {
        let tracking = self.inner.tracker.track(self.topic.name());
        let (address, event_id, timestamp, headers, ordering_key, transport_payload, token, provider_metadata) =
            message.into_parts();
        let payload = match crate::codec::decode_payload(self.codec.as_ref(), transport_payload).map_err(Into::into) {
            Ok(payload) => payload,
            Err(error) => {
                self.pending = Some(PendingDelivery {
                    _tracking: tracking,
                    event_id,
                    event: None,
                    token,
                    metadata: provider_metadata,
                    decode_error: Some(error),
                    settlement_intent: None,
                    settlement_failures: 0,
                    failure_diagnostic: None,
                    admission: None,
                    lane: None,
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
        self.pending = Some(PendingDelivery {
            _tracking: tracking,
            event_id,
            event: Some(event),
            token,
            metadata: provider_metadata,
            decode_error: None,
            settlement_intent: None,
            settlement_failures: 0,
            failure_diagnostic: None,
            admission: None,
            lane: None,
        });
    }

    /// Applies or retries the selected terminal provider disposition.
    ///
    /// # Parameters
    /// - `disposition`: terminal action selected by handler policy.
    /// - `event`: decoded event used for diagnostics, when decoding succeeded.
    pub(in crate::facade) async fn settle_pending(
        &mut self,
        disposition: DeliveryDisposition,
        event: Option<&EventEnvelope<T>>,
    ) {
        let Some(pending) = self.pending.as_mut() else {
            return;
        };
        pending.settlement_intent = Some(disposition);
        if self.defer_settlement {
            return;
        }
        let failure_diagnostic = pending.failure_diagnostic.clone();
        let event_id = event
            .map(|event| event.id().clone())
            .unwrap_or_else(|| pending.event_id.clone());
        let topic = event
            .map(|event| event.topic().name().into())
            .unwrap_or_else(|| self.topic.name().into());
        let Some(token) = pending.token.as_ref() else {
            self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
            self.pending.take();
            return;
        };
        if !token.belongs_to(self.id) {
            self.inner.emit(&Diagnostic::InternalFailure {
                origin: "settlement".into(),
                message: "provider settlement token belongs to another subscription".into(),
            });
            self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
            self.pending.take();
            return;
        }
        let capability = self.inner.capabilities.settlement();
        let supported = match disposition {
            DeliveryDisposition::Accept => capability != crate::spi::SettlementCapabilities::None,
            DeliveryDisposition::Retry | DeliveryDisposition::Reject => {
                capability == crate::spi::SettlementCapabilities::AcceptRetryReject
            }
        };
        if !supported {
            self.inner.emit(&Diagnostic::SettlementUnavailable {
                event_id: event_id.clone(),
                topic: topic.clone(),
                subscription_id: self.id,
                subscriber_id: self.subscriber_id.clone(),
                requested: disposition,
            });
            self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
            self.pending.take();
            return;
        }
        let result = if let Some(receiver) = self.receiver.as_mut() {
            match crate::spi::panic_boundary::catch_spi_call(
                self.inner.provider_id.as_str(),
                "settle",
                Some(self.subscriber_id.as_str()),
                || receiver.settle(token, disposition),
            ) {
                Ok(future) => {
                    catch_spi_future(
                        future,
                        &self.inner.provider_id,
                        "settle",
                        Some(self.subscriber_id.as_str()),
                    )
                    .await
                }
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        };
        match result {
            Ok(()) => {
                self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
                self.pending.take();
            }
            Err(error) => {
                if error.kind() == "provider_panicked" {
                    self.inner.emit(&Diagnostic::SettlementUnavailable {
                        event_id: event_id.clone(),
                        topic: topic.clone(),
                        subscription_id: self.id,
                        subscriber_id: self.subscriber_id.clone(),
                        requested: disposition,
                    });
                    self.inner.emit(&Diagnostic::SettlementFailed {
                        event_id: event_id.clone(),
                        topic: topic.clone(),
                        subscription_id: self.id,
                        subscriber_id: self.subscriber_id.clone(),
                        disposition,
                        error: error.to_string().into(),
                    });
                    self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
                    self.pending.take();
                    return;
                }
                if let Some(pending) = self.pending.as_mut() {
                    pending.settlement_failures = pending.settlement_failures.saturating_add(1);
                }
                self.inner.emit(&Diagnostic::SettlementFailed {
                    event_id: event_id.clone(),
                    topic: topic.clone(),
                    subscription_id: self.id,
                    subscriber_id: self.subscriber_id.clone(),
                    disposition,
                    error: error.to_string().into(),
                });
            }
        }
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
        event_id: crate::model::EventId,
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
