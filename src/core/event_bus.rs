/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Event bus abstraction shared by concrete backends.
// qubit-style: allow multiple-public-types

use crate::{
    DeadLetterPayload,
    EventBusError,
    EventBusResult,
    EventEnvelope,
    IntoEventBusResult,
    PublishOptions,
    SubscribeOptions,
    Subscription,
    Topic,
};
use std::time::Duration;

/// Failure captured while best-effort batch publishing continues.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchPublishFailure {
    index: usize,
    event_id: String,
    error: EventBusError,
}

impl BatchPublishFailure {
    /// Creates a batch publish failure record.
    pub(crate) fn new(index: usize, event_id: String, error: EventBusError) -> Self {
        Self {
            index,
            event_id,
            error,
        }
    }

    /// Returns the input index of the failed envelope.
    ///
    /// # Returns
    /// Zero-based index in the input batch.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns the failed event ID.
    ///
    /// # Returns
    /// Stable event identifier captured before publishing.
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Returns the final publish error.
    ///
    /// # Returns
    /// Error returned for this envelope after publish retries.
    pub fn error(&self) -> &EventBusError {
        &self.error
    }
}

/// Result summary returned by best-effort batch publishing.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchPublishResult {
    total_count: usize,
    accepted_count: usize,
    dropped_count: usize,
    failures: Vec<BatchPublishFailure>,
}

impl BatchPublishResult {
    /// Creates an empty batch publish result.
    pub(crate) fn new(total_count: usize) -> Self {
        Self {
            total_count,
            accepted_count: 0,
            dropped_count: 0,
            failures: Vec::new(),
        }
    }

    /// Records one accepted envelope submission.
    pub(crate) fn record_accepted(&mut self) {
        self.accepted_count += 1;
    }

    /// Records one envelope dropped by publisher interceptors.
    pub(crate) fn record_dropped(&mut self) {
        self.dropped_count += 1;
    }

    /// Records one failed envelope submission.
    pub(crate) fn record_failure(&mut self, failure: BatchPublishFailure) {
        self.failures.push(failure);
    }

    /// Returns the total number of envelopes in the batch.
    ///
    /// # Returns
    /// Input envelope count.
    pub fn total_count(&self) -> usize {
        self.total_count
    }

    /// Returns the number of envelopes accepted by the backend.
    ///
    /// # Returns
    /// Accepted submission count.
    pub fn accepted_count(&self) -> usize {
        self.accepted_count
    }

    /// Returns the number of envelopes dropped before dispatch.
    ///
    /// # Returns
    /// Drop count reported by publisher interceptors.
    pub fn dropped_count(&self) -> usize {
        self.dropped_count
    }

    /// Returns the number of failed envelope submissions.
    ///
    /// # Returns
    /// Failure count.
    pub fn failure_count(&self) -> usize {
        self.failures.len()
    }

    /// Returns captured per-envelope failures.
    ///
    /// # Returns
    /// Failures in input order.
    pub fn failures(&self) -> &[BatchPublishFailure] {
        &self.failures
    }

    /// Returns whether the batch completed without per-envelope failures.
    ///
    /// # Returns
    /// `true` when every envelope was accepted or intentionally dropped.
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Common event bus contract implemented by concrete backends.
///
/// The trait mirrors the Java `EventBus` interface with Rust ownership and
/// error handling. Methods are generic over the payload type, so the trait is
/// intended for static dispatch rather than `dyn EventBus` trait objects.
pub trait EventBus: Clone + Send + Sync + 'static {
    /// Starts the event bus.
    ///
    /// # Returns
    /// `Ok(true)` when this call changed the bus from stopped to started.
    ///
    /// # Errors
    /// Returns backend-specific startup errors when resources cannot be created.
    fn start(&self) -> EventBusResult<bool>;

    /// Shuts down the event bus.
    ///
    /// # Returns
    /// `true` when this call changed the bus from started to stopped.
    fn shutdown(&self) -> bool;

    /// Closes the event bus.
    ///
    /// # Returns
    /// The result returned by [`shutdown`](Self::shutdown).
    fn close(&self) -> bool {
        self.shutdown()
    }

    /// Publishes a payload to a topic.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    ///
    /// # Returns
    /// `Ok(())` after the backend accepts the event.
    ///
    /// # Errors
    /// Returns backend-specific errors such as a stopped bus or dispatch failure.
    fn publish<T>(&self, topic: &Topic<T>, payload: T) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope(EventEnvelope::create(topic.clone(), payload))
    }

    /// Publishes a payload to a topic with explicit publish options.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    /// - `options`: Publish options applied to this event.
    ///
    /// # Returns
    /// `Ok(())` after the backend accepts the event.
    ///
    /// # Errors
    /// Returns backend-specific publish errors.
    fn publish_with_options<T>(
        &self,
        topic: &Topic<T>,
        payload: T,
        options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options(EventEnvelope::create(topic.clone(), payload), options)
    }

    /// Publishes an existing envelope with default publish options.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to publish.
    ///
    /// # Returns
    /// `Ok(())` after the backend accepts the event.
    ///
    /// # Errors
    /// Returns backend-specific publishing errors.
    fn publish_envelope<T>(&self, envelope: EventEnvelope<T>) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options(envelope, PublishOptions::empty())
    }

    /// Publishes an existing envelope with explicit publish options.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to publish.
    /// - `options`: Publish options applied to this event.
    ///
    /// # Returns
    /// `Ok(())` after the backend accepts the event.
    ///
    /// # Errors
    /// Returns backend-specific publishing errors.
    fn publish_envelope_with_options<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static;

    /// Publishes a batch of envelopes with default publish options.
    ///
    /// The default implementation submits envelopes in input order. Concrete
    /// backends may still execute handlers concurrently unless they document a
    /// stronger ordering guarantee.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to submit in order.
    ///
    /// # Returns
    /// Summary containing per-envelope successes and failures.
    ///
    /// # Errors
    /// Returns backend-level batch precondition errors.
    fn publish_all<T>(&self, envelopes: Vec<EventEnvelope<T>>) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_all_with_options(envelopes, PublishOptions::empty())
    }

    /// Publishes a batch of envelopes with explicit publish options.
    ///
    /// The default implementation submits envelopes in input order. Concrete
    /// backends may still execute handlers concurrently unless they document a
    /// stronger ordering guarantee.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to submit in order.
    /// - `options`: Publish options cloned for each envelope.
    ///
    /// # Returns
    /// Summary containing per-envelope successes and failures.
    ///
    /// # Errors
    /// Returns backend-level batch precondition errors. Per-envelope publish
    /// failures are captured in [`BatchPublishResult`].
    fn publish_all_with_options<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut result = BatchPublishResult::new(envelopes.len());
        for (index, envelope) in envelopes.into_iter().enumerate() {
            let event_id = envelope.id().to_string();
            match self.publish_envelope_with_options(envelope, options.clone()) {
                Ok(()) => result.record_accepted(),
                Err(error) => {
                    result.record_failure(BatchPublishFailure::new(index, event_id, error));
                }
            }
        }
        Ok(result)
    }

    /// Subscribes a handler using backend default options.
    ///
    /// # Parameters
    /// - `subscriber_id`: Subscriber identifier.
    /// - `topic`: Topic to subscribe.
    /// - `handler`: Handler invoked for matching events.
    ///
    /// # Returns
    /// Subscription handle.
    ///
    /// # Errors
    /// Returns backend-specific subscription errors.
    fn subscribe<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.subscribe_with_options(subscriber_id, topic, handler, SubscribeOptions::empty())
    }

    /// Subscribes a handler using explicit options.
    ///
    /// # Parameters
    /// - `subscriber_id`: Subscriber identifier.
    /// - `topic`: Topic to subscribe.
    /// - `handler`: Handler invoked for matching events.
    /// - `options`: Subscription options.
    ///
    /// # Returns
    /// Subscription handle.
    ///
    /// # Errors
    /// Returns backend-specific subscription errors.
    fn subscribe_with_options<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
        options: SubscribeOptions<T>,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static;

    /// Registers a handler for standard dead-letter payloads.
    ///
    /// The default implementation adapts the handler into a normal subscription
    /// with a deterministic system subscriber ID derived from the topic name.
    ///
    /// # Parameters
    /// - `dead_letter_topic`: Topic carrying [`DeadLetterPayload`] events.
    /// - `handler`: Handler invoked for dead-letter events.
    /// - `options`: Subscription options for dead-letter consumption.
    ///
    /// # Returns
    /// Subscription handle for the dead-letter handler.
    ///
    /// # Errors
    /// Returns backend-specific subscription errors.
    fn add_dead_letter_handler<F, R>(
        &self,
        dead_letter_topic: &Topic<DeadLetterPayload>,
        handler: F,
        options: SubscribeOptions<DeadLetterPayload>,
    ) -> EventBusResult<Subscription<DeadLetterPayload>>
    where
        F: Fn(EventEnvelope<DeadLetterPayload>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.subscribe_with_options(
            format!("dead-letter:{}", dead_letter_topic.name()),
            dead_letter_topic,
            handler,
            options,
        )
    }

    /// Waits until all work for a topic is idle.
    ///
    /// # Parameters
    /// - `topic`: Topic to wait for.
    ///
    /// # Returns
    /// `Ok(())` once the topic has no active handler work.
    ///
    /// # Errors
    /// Returns backend-specific wait errors.
    fn wait_for_idle<T>(&self, topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static;

    /// Waits until all work for a topic is idle or the timeout elapses.
    ///
    /// # Parameters
    /// - `topic`: Topic to wait for.
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once the topic has no active handler work, or `Ok(false)` when
    /// the timeout elapses first.
    ///
    /// # Errors
    /// Returns backend-specific wait errors.
    fn wait_for_idle_timeout<T>(&self, topic: &Topic<T>, timeout: Duration) -> EventBusResult<bool>
    where
        T: 'static;
}
