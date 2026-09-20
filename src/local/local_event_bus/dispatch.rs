// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Local event publication and subscriber dispatch.

use std::sync::Arc;

use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::LocalEventBus;
use super::run_dispatch_with_retry;
use crate::BatchPublishItem;
use crate::BatchPublishResult;
use crate::DeadLetterPayload;
use crate::DispatchStatus;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::PublishOptions;
use crate::PublishOutcome;
use crate::PublishReceipt;
use crate::SubscriberDispatchResult;
use crate::Topic;
use crate::local::erased_subscription::DispatchAdmission;

impl LocalEventBus {
    /// Publishes a payload to a topic.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    ///
    /// # Returns
    /// Returns a [`PublishReceipt`] describing each matching subscriber.
    ///
    /// Local dispatch is non-transactional across matching subscribers. If
    /// scheduling fails for a later subscriber, earlier subscriber work may
    /// already have been accepted.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish<T>(&self, topic: &Topic<T>, payload: T) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_with_options(topic, payload, PublishOptions::empty())
    }

    /// Publishes a payload to a topic with explicit options.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    /// - `options`: Publish options merged with factory defaults.
    ///
    /// # Returns
    /// Returns a [`PublishReceipt`] describing each matching subscriber.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish_with_options<T>(
        &self,
        topic: &Topic<T>,
        payload: T,
        options: PublishOptions<T>,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options(EventEnvelope::create(topic.clone(), payload), options)
    }

    /// Publishes an existing envelope.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to dispatch.
    ///
    /// # Returns
    /// Returns a [`PublishReceipt`] describing each matching subscriber.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish_envelope<T>(&self, envelope: EventEnvelope<T>) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options(envelope, PublishOptions::empty())
    }

    /// Publishes an existing envelope with options.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to dispatch.
    /// - `options`: Publish options.
    ///
    /// # Returns
    /// `Ok(())` after subscriber work has been scheduled.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish_envelope_with_options<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        let options = options.merge_defaults(self.default_publish_options::<T>());
        self.publish_envelope_with_options_internal(envelope, options, false, true)
    }

    /// Publishes an envelope through the local dispatch path.
    pub(super) fn publish_envelope_with_options_internal<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
        allow_stopping: bool,
        require_started: bool,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Err(error) = self.ensure_started()
            && require_started
        {
            self.observe_errors(options.notify_publish_error(&envelope, &error));
            return Err(error);
        }
        let original_envelope = envelope.clone();
        let envelope = match run_dispatch_with_retry(options.retry_options(), options.retry_rule(), || {
            let Some(envelope) = self.apply_global_publisher_interceptors(original_envelope.clone())? else {
                return Ok(None);
            };
            self.apply_typed_publisher_interceptors(envelope)
        }) {
            Ok(Some(envelope)) => envelope,
            Ok(None) => {
                return Ok(PublishReceipt::new(
                    original_envelope.id().to_string(),
                    None,
                    PublishOutcome::Dropped,
                ));
            }
            Err(error) => {
                self.inner.observe_error(&error);
                self.observe_errors(options.notify_publish_error(&original_envelope, &error));
                return Err(error);
            }
        };
        let dispatch_results = self.dispatch_envelope(
            envelope.clone(),
            options.retry_options(),
            options.retry_rule(),
            allow_stopping,
        )?;
        let receipt = PublishReceipt::new(
            original_envelope.id().to_string(),
            Some(envelope.id().to_string()),
            PublishOutcome::Dispatched(dispatch_results),
        );
        if let PublishOutcome::Dispatched(items) = receipt.outcome()
            && let Some(error) = items.iter().find_map(|item| match item.status() {
                DispatchStatus::Rejected(error) => Some(error),
                _ => None,
            })
        {
            self.observe_errors(options.notify_publish_error(&envelope, error));
        }
        Ok(receipt)
    }

    /// Publishes a dead-letter envelope while graceful shutdown is draining.
    pub(super) fn publish_dead_letter_envelope(
        &self,
        envelope: EventEnvelope<DeadLetterPayload>,
    ) -> EventBusResult<PublishReceipt> {
        let options = PublishOptions::empty().merge_defaults(self.default_publish_options::<DeadLetterPayload>());
        self.publish_envelope_with_options_internal(envelope, options, true, false)
    }

    /// Publishes multiple envelopes by submitting each envelope in input order.
    ///
    /// This method preserves submission order only. Handler execution order is
    /// backend-specific because local handlers can run on multiple worker
    /// threads.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to submit in order.
    ///
    /// # Returns
    /// Summary containing per-envelope successes and failures.
    ///
    /// # Errors
    /// Returns lifecycle or option validation errors before the batch starts.
    pub fn publish_all<T>(&self, envelopes: Vec<EventEnvelope<T>>) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_all_with_options(envelopes, PublishOptions::empty())
    }

    /// Publishes multiple envelopes with explicit publish options.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to submit in order.
    /// - `options`: Publish options cloned for each envelope.
    ///
    /// # Returns
    /// Summary containing per-envelope successes and failures.
    ///
    /// # Errors
    /// Returns lifecycle or option validation errors before the batch starts.
    pub fn publish_all_with_options<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.ensure_started()?;
        let options = options.merge_defaults(self.default_publish_options::<T>());
        let mut items = Vec::with_capacity(envelopes.len());
        for (index, envelope) in envelopes.into_iter().enumerate() {
            let event_id = envelope.id().to_string();
            match self.publish_envelope_with_options_internal(envelope, options.clone(), false, true) {
                Ok(receipt) => items.push(BatchPublishItem::new(index, event_id, Ok(receipt))),
                Err(error) => items.push(BatchPublishItem::new(index, event_id, Err(error))),
            }
        }
        Ok(BatchPublishResult::new(items))
    }

    /// Returns default publish options for a payload type.
    ///
    /// # Returns
    /// Type-specific default options or empty options.
    pub(super) fn default_publish_options<T>(&self) -> PublishOptions<T>
    where
        T: 'static,
    {
        self.inner
            .default_publish_options::<T>()
            .unwrap_or_else(PublishOptions::empty)
    }

    /// Ensures the event bus is started.
    ///
    /// # Returns
    /// `Ok(())` if started.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] when the bus is stopped.
    pub(super) fn ensure_started(&self) -> EventBusResult<()> {
        if self.inner.is_started() {
            Ok(())
        } else {
            Err(EventBusError::not_started())
        }
    }

    /// Observes internal failures produced by user callbacks.
    ///
    /// # Parameters
    /// - `errors`: Callback failures to publish to registered error observers.
    pub(super) fn observe_errors(&self, errors: Vec<EventBusError>) {
        for error in errors {
            self.inner.observe_error(&error);
        }
    }

    /// Dispatches an envelope to currently registered subscribers.
    ///
    /// This dispatch loop is best-effort across subscriptions: a later
    /// submission error does not roll back subscriber tasks accepted earlier in
    /// the same publish call.
    pub(super) fn dispatch_envelope<T>(
        &self,
        envelope: EventEnvelope<T>,
        retry_options: Option<&RetryPolicy>,
        retry_rule: Option<&Arc<dyn RetryRule<EventBusError>>>,
        allow_stopping: bool,
    ) -> EventBusResult<Vec<SubscriberDispatchResult>>
    where
        T: Clone + Send + Sync + 'static,
    {
        if !allow_stopping {
            self.ensure_started()?;
        }
        let subscriptions = self.inner.subscriptions_for(&envelope.topic().key())?;
        let mut results = Vec::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            let subscription = Arc::clone(&subscription);
            let status = match run_dispatch_with_retry(retry_options, retry_rule, || {
                subscription.dispatch(Box::new(envelope.clone()), Arc::clone(&self.inner), allow_stopping)
            }) {
                Ok(DispatchAdmission::Accepted) => DispatchStatus::Accepted,
                Ok(DispatchAdmission::Filtered) => DispatchStatus::Filtered,
                Err(error) => DispatchStatus::Rejected(error),
            };
            results.push(SubscriberDispatchResult::new(
                subscription.id(),
                subscription.subscriber_id().to_string(),
                status,
            ));
        }
        Ok(results)
    }
}
