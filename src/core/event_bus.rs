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

use crate::{
    EventBusResult, EventEnvelope, IntoEventBusResult, PublishOptions, SubscribeOptions,
    Subscription, Topic,
};

/// Completed event-bus task returned by non-blocking convenience APIs.
///
/// The local backend performs publish and subscribe scheduling synchronously and
/// returns an already-completed task instead of creating a throwaway OS thread.
pub struct EventBusTask<T> {
    result: EventBusResult<T>,
}

impl<T> EventBusTask<T> {
    /// Creates a completed task.
    ///
    /// # Parameters
    /// - `result`: Operation result stored in the task.
    ///
    /// # Returns
    /// Task whose [`join`](Self::join) method returns `result`.
    pub fn completed(result: EventBusResult<T>) -> Self {
        Self { result }
    }

    /// Returns the completed task result.
    ///
    /// # Returns
    /// Operation result captured when the task was created.
    pub fn join(self) -> EventBusResult<T> {
        self.result
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
    /// # Parameters
    /// - `envelopes`: Envelopes to publish.
    ///
    /// # Returns
    /// `Ok(())` after the backend accepts the batch.
    ///
    /// # Errors
    /// Returns the backend's batch publishing error.
    fn publish_all<T>(&self, envelopes: Vec<EventEnvelope<T>>) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        for envelope in envelopes {
            self.publish_envelope(envelope)?;
        }
        Ok(())
    }

    /// Publishes a payload and returns a completed task.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    ///
    /// # Returns
    /// Completed task resolving to the publish result.
    ///
    /// # Errors
    /// Returns backend errors when [`EventBusTask::join`] is called.
    fn publish_async<T>(&self, topic: &Topic<T>, payload: T) -> EventBusTask<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_async(EventEnvelope::create(topic.clone(), payload))
    }

    /// Publishes an envelope and returns a completed task with default options.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to publish.
    ///
    /// # Returns
    /// Completed task resolving to the publish result.
    ///
    /// # Errors
    /// Returns backend errors when [`EventBusTask::join`] is called.
    fn publish_envelope_async<T>(&self, envelope: EventEnvelope<T>) -> EventBusTask<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options_async(envelope, PublishOptions::empty())
    }

    /// Publishes an envelope with options and returns a completed task.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to publish.
    /// - `options`: Publish options applied to this event.
    ///
    /// # Returns
    /// Completed task resolving to the publish result.
    ///
    /// # Errors
    /// Returns backend errors when [`EventBusTask::join`] is called.
    fn publish_envelope_with_options_async<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusTask<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        EventBusTask::completed(self.publish_envelope_with_options(envelope, options))
    }

    /// Publishes a batch of envelopes asynchronously.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to publish.
    /// - `options`: Publish options cloned for each envelope.
    ///
    /// # Returns
    /// Completed tasks, one per envelope.
    ///
    /// # Errors
    /// Returns per-envelope backend errors when task results are joined.
    fn publish_all_async<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> Vec<EventBusTask<()>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut handles = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            handles.push(self.publish_envelope_with_options_async(envelope, options.clone()));
        }
        handles
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

    /// Subscribes a handler on a background thread using backend default options.
    ///
    /// # Parameters
    /// - `subscriber_id`: Subscriber identifier.
    /// - `topic`: Topic to subscribe.
    /// - `handler`: Handler invoked for matching events.
    ///
    /// # Returns
    /// Join handle resolving to the subscription result.
    ///
    /// # Errors
    /// Returns scheduling or backend precondition errors before the thread starts.
    fn subscribe_async<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
    ) -> EventBusTask<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.subscribe_with_options_async(subscriber_id, topic, handler, SubscribeOptions::empty())
    }

    /// Subscribes a handler with options on a background thread.
    ///
    /// # Parameters
    /// - `subscriber_id`: Subscriber identifier.
    /// - `topic`: Topic to subscribe.
    /// - `handler`: Handler invoked for matching events.
    /// - `options`: Subscription options.
    ///
    /// # Returns
    /// Completed task resolving to the subscription result.
    ///
    /// # Errors
    /// Returns backend errors when [`EventBusTask::join`] is called.
    fn subscribe_with_options_async<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
        options: SubscribeOptions<T>,
    ) -> EventBusTask<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        let subscriber_id = subscriber_id.into();
        EventBusTask::completed(self.subscribe_with_options(subscriber_id, topic, handler, options))
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
}
