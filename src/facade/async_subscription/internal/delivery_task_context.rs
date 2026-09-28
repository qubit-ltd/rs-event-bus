// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Handler and retry processing state, separate from the receiver owner.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use qubit_id::Id;

use super::super::AsyncEventBusInner;
use super::super::SharedAsyncHandler;
use super::super::notify_failure;
use super::super::publish_dead_letter_async;
use super::super::run_with_retry;
use super::BusContextFuture;
use super::PendingDelivery;
use super::SessionSignals;
use crate::error::DeliveryError;
use crate::model::Delivery;
use crate::model::DeliveryContext;
use crate::model::EventEnvelope;
use crate::model::FailureDirective;
use crate::model::ProviderMessageMetadata;
use crate::model::SubscribeOptions;
use crate::model::SubscriberId;
use crate::model::Topic;
use crate::pipeline::Diagnostic;
use crate::pipeline::OrderingLaneKey;
use crate::spi::DeliveryDisposition;

/// Processing context carried by one owned handler task.
///
/// This type deliberately has no provider receiver or session task queues.
pub(in crate::facade::async_subscription) struct DeliveryTaskContext<T: 'static> {
    pub(in crate::facade::async_subscription) inner: Arc<AsyncEventBusInner>,
    pub(in crate::facade::async_subscription) id: Id,
    pub(in crate::facade::async_subscription) subscriber_id: SubscriberId,
    pub(in crate::facade::async_subscription) topic: Topic<T>,
    pub(in crate::facade::async_subscription) options: SubscribeOptions<T>,
    pub(in crate::facade::async_subscription) signals: Arc<SessionSignals>,
    pub(in crate::facade::async_subscription) pending: Option<PendingDelivery<T>>,
    pub(in crate::facade::async_subscription) started: Option<Arc<AtomicBool>>,
}

impl<T: Send + Sync + 'static> DeliveryTaskContext<T> {
    pub(in crate::facade::async_subscription) async fn process_pending(&mut self, handler: SharedAsyncHandler<T>) {
        let Some(pending) = self.pending.as_ref() else {
            return;
        };
        if let Some(disposition) = pending.settlement_intent {
            let event = pending.event.clone();
            self.settle_pending(disposition, event.as_deref()).await;
            return;
        }
        if let Some(error) = pending.decode_error.as_ref() {
            let panicked = matches!(error, DeliveryError::Codec(crate::error::CodecError::Panicked { .. }));
            if panicked {
                self.inner.emit(&Diagnostic::InternalFailure {
                    origin: "codec_decode".into(),
                    message: error.to_string().into(),
                });
            }
            self.record_failure_diagnostic(0, error.to_string().into());
            if panicked {
                self.settle_pending(DeliveryDisposition::Retry, None).await;
            } else {
                self.settle_pending(DeliveryDisposition::Reject, None).await;
            }
            return;
        }
        let Some(event) = pending.event.as_ref().cloned() else {
            return;
        };
        if let Some(filter) = self.options.filter() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| filter(&event))) {
                Ok(false) => {
                    self.settle_pending(DeliveryDisposition::Accept, Some(&event)).await;
                    return;
                }
                Ok(true) => {}
                Err(_) => {
                    let can_settle = self.pending.as_ref().is_some_and(|pending| pending.token.is_some());
                    let metadata = self.pending.as_ref().expect("pending delivery exists").metadata.clone();
                    let delivery = Delivery::new(event.clone(), self.context(can_settle, metadata, &event));
                    let error = DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("subscriber filter panicked")),
                    };
                    let directive = notify_failure(
                        &self.options,
                        &event,
                        &error,
                        self.options.retry_policy().is_some(),
                        &self.inner,
                    );
                    self.finish_failure(delivery, error, 1, directive).await;
                    return;
                }
            }
        }
        let pending = self.pending.as_ref().expect("pending delivery exists");
        let context = self.context(pending.token.is_some(), pending.metadata.clone(), &event);
        let delivery = Delivery::new(event.clone(), context);
        let _lane = if self.options.ordering_policy() == crate::model::OrderingPolicy::PerKey {
            let key = OrderingLaneKey::new(self.topic.name(), event.ordering_key(), self.id);
            Some(self.inner.ordering_lanes.enqueue(key, ()).await)
        } else {
            None
        };
        if let Some(lane) = _lane
            && let Some(pending) = self.pending.as_mut()
        {
            pending.lane = lane;
        }
        if let Some(started) = &self.started
            && !self.signals.mark_started(started)
        {
            return;
        }
        let bus_key = Arc::as_ptr(&self.inner) as usize;
        let global_interceptors = self.inner.facade_config.async_subscriber_interceptors::<T>();
        let attempts = run_with_retry(
            self.options.clone(),
            delivery.clone(),
            handler,
            &self.inner,
            global_interceptors,
        );
        match BusContextFuture::new(bus_key, attempts).await {
            Ok(_) => self.settle_pending(DeliveryDisposition::Accept, Some(&event)).await,
            Err((error, attempts, directive)) => self.finish_failure(delivery, *error, attempts, directive).await,
        }
    }

    fn context(
        &self,
        can_settle: bool,
        metadata: ProviderMessageMetadata,
        event: &EventEnvelope<T>,
    ) -> DeliveryContext {
        let context = DeliveryContext::new(self.inner.provider_id.clone(), self.id, self.subscriber_id.clone())
            .with_provider_metadata(metadata)
            .with_settlement(can_settle);
        if event.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE) {
            context.as_dead_letter()
        } else {
            context
        }
    }

    async fn finish_failure(
        &mut self,
        delivery: Delivery<T>,
        error: DeliveryError,
        attempts: u32,
        directive: FailureDirective,
    ) {
        self.record_failure_diagnostic(attempts, error.to_string().into());
        if directive == FailureDirective::DeadLetter && !delivery.context().is_dead_letter() {
            if let Some(policy) = self.options.dead_letter() {
                if let Ok(Some(envelope)) =
                    crate::pipeline::dead_letter_envelope(&delivery, &error, policy.topic_name())
                {
                    match publish_dead_letter_async(
                        &self.inner,
                        &envelope,
                        self.options.retry_policy(),
                        self.options.retry_cancellation_token(),
                        policy.admission_policy(),
                    )
                    .await
                    {
                        Ok(receipt) => {
                            if matches!(
                                receipt.admission_outcome(),
                                crate::model::AdmissionOutcome::PartiallyAccepted(_)
                            ) {
                                self.inner.emit(&Diagnostic::InternalFailure {
                                    origin: "dead_letter_partial".into(),
                                    message: "dead-letter publication was partially accepted; it was not republished"
                                        .into(),
                                });
                            }
                        }
                        Err(message) => {
                            self.inner.emit(&Diagnostic::InternalFailure {
                                origin: "dead_letter_publish".into(),
                                message: message.clone().into(),
                            });
                            self.signals
                                .fail_dead_letter_forward(delivery.event().id().clone(), message.into());
                            return;
                        }
                    }
                } else {
                    let message = "dead-letter envelope could not be constructed";
                    self.inner.emit(&Diagnostic::InternalFailure {
                        origin: "dead_letter_build".into(),
                        message: message.into(),
                    });
                    self.signals
                        .fail_dead_letter_forward(delivery.event().id().clone(), message.into());
                    return;
                }
            } else {
                let message = "dead-letter directive has no configured policy";
                self.inner.emit(&Diagnostic::InternalFailure {
                    origin: "dead_letter_policy".into(),
                    message: message.into(),
                });
                self.signals
                    .fail_dead_letter_forward(delivery.event().id().clone(), message.into());
                return;
            }
        }
        let disposition = match directive {
            FailureDirective::Requeue => Some(DeliveryDisposition::Retry),
            FailureDirective::DeadLetter | FailureDirective::Discard | FailureDirective::Retry => {
                Some(DeliveryDisposition::Reject)
            }
        };
        if let Some(disposition) = disposition {
            self.settle_pending(disposition, Some(delivery.event())).await;
        }
    }

    async fn settle_pending(&mut self, disposition: DeliveryDisposition, _event: Option<&EventEnvelope<T>>) {
        if let Some(pending) = self.pending.as_mut() {
            pending.settlement_intent = Some(disposition);
        }
    }

    fn record_failure_diagnostic(&mut self, attempts: u32, error: Box<str>) {
        if let Some(pending) = self.pending.as_mut() {
            pending.failure_diagnostic = Some((attempts, error));
        }
    }
}
