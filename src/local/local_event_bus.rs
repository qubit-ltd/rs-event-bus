/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Thread-safe in-process event bus.

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use qubit_thread_pool::{ExecutorService, FixedThreadPool};

use crate::{
    AckMode, Acknowledgement, EventBusError, EventBusResult, EventEnvelope, IntoEventBusResult,
    PublishOptions, SubscribeOptions, Subscription, Topic, TopicKey,
};

use super::erased_subscription::ErasedSubscription;
use super::local_event_bus_inner::LocalEventBusInner;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;
type PublisherInterceptorFn<T> =
    dyn Fn(EventEnvelope<T>) -> Option<EventEnvelope<T>> + Send + Sync + 'static;
type SubscriberInterceptorFn<T> = dyn Fn(EventEnvelope<T>, SubscriberInterceptorChain<T>) -> EventBusResult<()>
    + Send
    + Sync
    + 'static;

/// Chain handle passed to subscriber interceptors.
///
/// Calling [`proceed`](Self::proceed) invokes the next subscriber interceptor,
/// or the original subscriber handler when the current interceptor is the last
/// one in the chain.
pub struct SubscriberInterceptorChain<T: Clone + Send + Sync + 'static> {
    next: Arc<HandlerFn<T>>,
}

impl<T> SubscriberInterceptorChain<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a chain handle around the next handler.
    ///
    /// # Parameters
    /// - `next`: Handler or interceptor wrapper to invoke next.
    ///
    /// # Returns
    /// Chain handle for one interceptor invocation.
    fn new(next: Arc<HandlerFn<T>>) -> Self {
        Self { next }
    }

    /// Continues subscriber processing.
    ///
    /// # Parameters
    /// - `envelope`: Envelope to pass to the next chain element.
    ///
    /// # Returns
    /// `Ok(())` when downstream processing succeeds.
    ///
    /// # Errors
    /// Returns the downstream handler or interceptor error.
    pub fn proceed(&self, envelope: EventEnvelope<T>) -> EventBusResult<()> {
        (self.next)(envelope)
    }
}

/// Thread-safe in-process event bus.
///
/// This backend stores subscriptions in memory and dispatches subscriber
/// handlers on background threads. Publishing schedules work and returns after
/// dispatch, while [`wait_for_idle`](Self::wait_for_idle) can be used by tests
/// to wait for all handler work for a topic.
#[derive(Clone)]
pub struct LocalEventBus {
    pub(crate) inner: Arc<LocalEventBusInner>,
}

impl LocalEventBus {
    /// Creates a stopped local event bus.
    ///
    /// # Returns
    /// A new event bus with no subscriptions.
    pub fn new() -> Self {
        Self::with_default_subscribe_options(HashMap::new())
    }

    /// Creates and starts a local event bus.
    ///
    /// # Returns
    /// A started event bus.
    pub fn started() -> Self {
        let bus = Self::new();
        bus.start();
        bus
    }

    /// Creates a stopped event bus with typed default subscribe options.
    ///
    /// # Parameters
    /// - `default_subscribe_options`: Type-erased defaults copied from a factory.
    ///
    /// # Returns
    /// A stopped event bus.
    pub(crate) fn with_default_subscribe_options(
        default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        Self::with_runtime_options(
            default_subscribe_options,
            default_subscription_handler_pool_size(),
            None,
        )
    }

    /// Creates a stopped event bus with typed defaults and local runtime options.
    ///
    /// # Parameters
    /// - `default_subscribe_options`: Type-erased defaults copied from a factory.
    /// - `subscription_handler_pool_size`: Number of subscriber handler workers.
    /// - `subscription_handler_queue_capacity`: Optional queued handler limit.
    ///
    /// # Returns
    /// A stopped event bus.
    pub(crate) fn with_runtime_options(
        default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
        subscription_handler_pool_size: usize,
        subscription_handler_queue_capacity: Option<usize>,
    ) -> Self {
        Self {
            inner: Arc::new(LocalEventBusInner::new(
                default_subscribe_options,
                subscription_handler_pool_size,
                subscription_handler_queue_capacity,
            )),
        }
    }

    /// Starts the event bus.
    ///
    /// # Returns
    /// `true` when this call changed the bus from stopped to started.
    pub fn start(&self) -> bool {
        self.inner.mark_started()
    }

    /// Shuts down the event bus.
    ///
    /// The method waits for currently scheduled handlers to finish and then
    /// clears all subscriptions.
    ///
    /// # Returns
    /// `true` when this call changed the bus from started to stopped.
    pub fn shutdown(&self) -> bool {
        let Some(executor) = self.inner.mark_stopped() else {
            return false;
        };
        executor.shutdown();
        let _ = self.inner.wait_for_all_idle();
        wait_for_executor_termination(&executor);
        self.inner.clear_subscriptions();
        true
    }

    /// Registers a typed publisher interceptor.
    ///
    /// # Parameters
    /// - `interceptor`: Callback that can modify or drop outgoing envelopes.
    ///
    /// # Returns
    /// `Ok(())` when the interceptor is stored.
    ///
    /// # Errors
    /// Returns a lock-poisoning error if interceptor state is unavailable.
    pub fn add_publisher_interceptor<T, F>(&self, interceptor: F) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(EventEnvelope<T>) -> Option<EventEnvelope<T>> + Send + Sync + 'static,
    {
        let entry = TypedPublisherInterceptor::<T> {
            interceptor: Arc::new(interceptor),
        };
        self.inner.add_publisher_interceptor(Arc::new(entry))
    }

    /// Registers a typed subscriber interceptor.
    ///
    /// Subscriber interceptors are applied in registration order and use an
    /// around-style chain. An interceptor can skip downstream processing by not
    /// calling [`SubscriberInterceptorChain::proceed`].
    ///
    /// # Parameters
    /// - `interceptor`: Callback wrapping subscriber handler execution.
    ///
    /// # Returns
    /// `Ok(())` when the interceptor is stored.
    ///
    /// # Errors
    /// Returns a lock-poisoning error if interceptor state is unavailable.
    pub fn add_subscriber_interceptor<T, F, R>(&self, interceptor: F) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(EventEnvelope<T>, SubscriberInterceptorChain<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        let entry = TypedSubscriberInterceptor::<T> {
            interceptor: Arc::new(move |event, chain| {
                interceptor(event, chain).into_event_bus_result()
            }),
        };
        self.inner.add_subscriber_interceptor(Arc::new(entry))
    }

    /// Publishes a payload to a topic.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    ///
    /// # Returns
    /// `Ok(())` after subscriber work has been scheduled.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish<T>(&self, topic: &Topic<T>, payload: T) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope(EventEnvelope::create(topic.clone(), payload))
    }

    /// Publishes an existing envelope.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to dispatch.
    ///
    /// # Returns
    /// `Ok(())` after subscriber work has been scheduled.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish_envelope<T>(&self, envelope: EventEnvelope<T>) -> EventBusResult<()>
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
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Err(error) = self.ensure_started() {
            options.notify_publish_error(&envelope, &error);
            return Err(error);
        }
        let Some(envelope) = self.apply_publisher_interceptors(envelope)? else {
            return Ok(());
        };
        if let Err(error) = self.dispatch_envelope(
            envelope.clone(),
            options.retry_options().unwrap_or_default(),
        ) {
            options.notify_publish_error(&envelope, &error);
            return Err(error);
        }
        Ok(())
    }

    /// Publishes an existing envelope on a background thread.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to dispatch.
    ///
    /// # Returns
    /// Join handle resolving to the publish result.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped before the
    /// background task is created.
    pub fn publish_envelope_async<T>(
        &self,
        envelope: EventEnvelope<T>,
    ) -> EventBusResult<JoinHandle<EventBusResult<()>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options_async(envelope, PublishOptions::empty())
    }

    /// Publishes an existing envelope with options on a background thread.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to dispatch.
    /// - `options`: Publish options.
    ///
    /// # Returns
    /// Join handle resolving to the publish result.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped before the
    /// background task is created.
    pub fn publish_envelope_with_options_async<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusResult<JoinHandle<EventBusResult<()>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Err(error) = self.ensure_started() {
            options.notify_publish_error(&envelope, &error);
            return Err(error);
        }
        let bus = self.clone();
        Ok(thread::spawn(move || {
            bus.publish_envelope_with_options(envelope, options)
        }))
    }

    /// Publishes multiple envelopes.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to publish in order.
    ///
    /// # Returns
    /// `Ok(())` after all envelopes have been scheduled.
    ///
    /// # Errors
    /// Returns the first publish error.
    pub fn publish_all<T>(&self, envelopes: Vec<EventEnvelope<T>>) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        for envelope in envelopes {
            self.publish_envelope(envelope)?;
        }
        Ok(())
    }

    /// Publishes multiple envelopes on background threads.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to publish in order.
    /// - `options`: Publish options cloned for each event.
    ///
    /// # Returns
    /// Join handles resolving to each publish result.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped before any
    /// background task is created.
    pub fn publish_all_async<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> EventBusResult<Vec<JoinHandle<EventBusResult<()>>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.ensure_started()?;
        let mut handles = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            handles.push(self.publish_envelope_with_options_async(envelope, options.clone())?);
        }
        Ok(handles)
    }

    /// Publishes a payload on a background thread.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    ///
    /// # Returns
    /// Join handle resolving to the publish result.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped before the
    /// background task is created.
    pub fn publish_async<T>(
        &self,
        topic: &Topic<T>,
        payload: T,
    ) -> EventBusResult<JoinHandle<EventBusResult<()>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_async(EventEnvelope::create(topic.clone(), payload))
    }

    /// Subscribes a handler using default options.
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
    /// Returns an error when the bus is stopped or shared state is unavailable.
    pub fn subscribe<T, S, F, R>(
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
        let options = self.default_subscribe_options::<T>();
        self.subscribe_with_options(subscriber_id, topic, handler, options)
    }

    /// Subscribes a handler using explicit options.
    ///
    /// # Parameters
    /// - `subscriber_id`: Subscriber identifier.
    /// - `topic`: Topic to subscribe.
    /// - `handler`: Handler invoked for matching events.
    /// - `options`: Subscription processing options.
    ///
    /// # Returns
    /// Subscription handle.
    ///
    /// # Errors
    /// Returns an error when the bus is stopped, the subscriber ID is blank, or
    /// shared state is unavailable.
    pub fn subscribe_with_options<T, S, F, R>(
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
        R: IntoEventBusResult + 'static,
    {
        self.ensure_started()?;
        let subscriber_id = subscriber_id.into();
        if subscriber_id.trim().is_empty() {
            return Err(EventBusError::invalid_argument(
                "subscriber_id",
                "subscriber ID must not be blank",
            ));
        }

        let id = self.inner.next_subscription_id();
        let active = Arc::new(AtomicBool::new(true));
        let topic_key = topic.key();
        let handler = Arc::new(move |event| handler(event).into_event_bus_result());
        let handler = self.apply_subscriber_interceptors(handler)?;
        let entry = TypedSubscriptionEntry {
            id,
            subscriber_id: subscriber_id.clone(),
            topic: topic.clone(),
            active: Arc::clone(&active),
            handler,
            options: options.clone(),
        };
        self.inner
            .add_subscription(topic_key.clone(), Arc::new(entry))?;

        Ok(Subscription {
            id,
            subscriber_id,
            topic: topic.clone(),
            topic_key,
            options,
            active,
            bus: Arc::downgrade(&self.inner),
        })
    }

    /// Subscribes a handler on a background thread using default options.
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
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped before the
    /// background task is created.
    pub fn subscribe_async<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
    ) -> EventBusResult<JoinHandle<EventBusResult<Subscription<T>>>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        let options = self.default_subscribe_options::<T>();
        self.subscribe_with_options_async(subscriber_id, topic, handler, options)
    }

    /// Subscribes a handler with options on a background thread.
    ///
    /// # Parameters
    /// - `subscriber_id`: Subscriber identifier.
    /// - `topic`: Topic to subscribe.
    /// - `handler`: Handler invoked for matching events.
    /// - `options`: Subscription processing options.
    ///
    /// # Returns
    /// Join handle resolving to the subscription result.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped before the
    /// background task is created.
    pub fn subscribe_with_options_async<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
        options: SubscribeOptions<T>,
    ) -> EventBusResult<JoinHandle<EventBusResult<Subscription<T>>>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.ensure_started()?;
        let bus = self.clone();
        let subscriber_id = subscriber_id.into();
        let topic = topic.clone();
        Ok(thread::spawn(move || {
            bus.subscribe_with_options(subscriber_id, &topic, handler, options)
        }))
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
    /// Returns a lock-poisoning error if tracker state is unavailable.
    pub fn wait_for_idle<T>(&self, topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        self.inner.wait_for_idle(&topic.key())
    }

    /// Returns default subscribe options for a payload type.
    ///
    /// # Returns
    /// Type-specific default options or empty options.
    fn default_subscribe_options<T>(&self) -> SubscribeOptions<T>
    where
        T: 'static,
    {
        self.inner
            .default_subscribe_options::<T>()
            .unwrap_or_else(SubscribeOptions::empty)
    }

    /// Ensures the event bus is started.
    ///
    /// # Returns
    /// `Ok(())` if started.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] when the bus is stopped.
    fn ensure_started(&self) -> EventBusResult<()> {
        if self.inner.is_started() {
            Ok(())
        } else {
            Err(EventBusError::not_started())
        }
    }

    /// Applies matching publisher interceptors.
    ///
    /// # Parameters
    /// - `envelope`: Original event envelope.
    ///
    /// # Returns
    /// Modified envelope, or `None` when an interceptor drops the event.
    ///
    /// # Errors
    /// Returns lock or type-erasure errors.
    fn apply_publisher_interceptors<T>(
        &self,
        envelope: EventEnvelope<T>,
    ) -> EventBusResult<Option<EventEnvelope<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let interceptors = self.inner.publisher_interceptors()?;
        let mut current: Option<Box<dyn Any + Send>> = Some(Box::new(envelope));
        for interceptor in interceptors {
            if interceptor.payload_type_id() == TypeId::of::<T>()
                && let Some(boxed) = current.take()
            {
                current = interceptor.intercept(boxed)?;
            }
        }
        current
            .map(|boxed| {
                boxed
                    .downcast::<EventEnvelope<T>>()
                    .map(|envelope| *envelope)
                    .map_err(|_| {
                        EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown")
                    })
            })
            .transpose()
    }

    /// Dispatches an envelope to currently registered subscribers.
    ///
    /// # Parameters
    /// - `envelope`: Envelope to dispatch.
    ///
    /// # Returns
    /// `Ok(())` once matching subscriber tasks have been accepted.
    ///
    /// # Errors
    /// Returns subscription lookup, type-erasure, or executor submission errors.
    fn dispatch_envelope<T>(
        &self,
        envelope: EventEnvelope<T>,
        retry_options: crate::RetryOptions,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        let subscriptions = self.inner.subscriptions_for(&envelope.topic().key())?;
        for subscription in subscriptions {
            let subscription = Arc::clone(&subscription);
            run_with_retry(retry_options, || {
                subscription.dispatch(Box::new(envelope.clone()), Arc::clone(&self.inner))
            })?;
        }
        Ok(())
    }

    /// Applies matching subscriber interceptors to a handler.
    ///
    /// # Parameters
    /// - `handler`: Original subscriber handler.
    ///
    /// # Returns
    /// Handler wrapped by registered subscriber interceptors.
    ///
    /// # Errors
    /// Returns lock or type-erasure errors.
    fn apply_subscriber_interceptors<T>(
        &self,
        handler: Arc<HandlerFn<T>>,
    ) -> EventBusResult<Arc<HandlerFn<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let interceptors = self.inner.subscriber_interceptors()?;
        let mut chain = handler;
        for interceptor in interceptors.into_iter().rev() {
            let Some(typed) = interceptor
                .as_ref()
                .downcast_ref::<TypedSubscriberInterceptor<T>>()
            else {
                continue;
            };
            let next = Arc::clone(&chain);
            let interceptor = Arc::clone(&typed.interceptor);
            chain = Arc::new(move |event| {
                let next_chain = SubscriberInterceptorChain::new(Arc::clone(&next));
                interceptor(event, next_chain)
            });
        }
        Ok(chain)
    }
}

impl Default for LocalEventBus {
    /// Creates a stopped local event bus.
    fn default() -> Self {
        Self::new()
    }
}

impl crate::EventBus for LocalEventBus {
    /// Starts the local event bus.
    fn start(&self) -> bool {
        Self::start(self)
    }

    /// Shuts down the local event bus.
    fn shutdown(&self) -> bool {
        Self::shutdown(self)
    }

    /// Publishes a payload using the local backend.
    fn publish<T>(&self, topic: &Topic<T>, payload: T) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish(self, topic, payload)
    }

    /// Publishes an envelope using the local backend.
    fn publish_envelope<T>(&self, envelope: EventEnvelope<T>) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_envelope(self, envelope)
    }

    /// Publishes an envelope with options using the local backend.
    fn publish_envelope_with_options<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_envelope_with_options(self, envelope, options)
    }

    /// Publishes a batch using the local backend.
    fn publish_all<T>(&self, envelopes: Vec<EventEnvelope<T>>) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_all(self, envelopes)
    }

    /// Publishes a payload on a background thread using the local backend.
    fn publish_async<T>(
        &self,
        topic: &Topic<T>,
        payload: T,
    ) -> EventBusResult<JoinHandle<EventBusResult<()>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_async(self, topic, payload)
    }

    /// Publishes an envelope on a background thread using the local backend.
    fn publish_envelope_async<T>(
        &self,
        envelope: EventEnvelope<T>,
    ) -> EventBusResult<JoinHandle<EventBusResult<()>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_envelope_async(self, envelope)
    }

    /// Publishes an envelope with options on a background thread.
    fn publish_envelope_with_options_async<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusResult<JoinHandle<EventBusResult<()>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_envelope_with_options_async(self, envelope, options)
    }

    /// Publishes a batch asynchronously using the local backend.
    fn publish_all_async<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> EventBusResult<Vec<JoinHandle<EventBusResult<()>>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_all_async(self, envelopes, options)
    }

    /// Subscribes a handler using local backend defaults.
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
        Self::subscribe(self, subscriber_id, topic, handler)
    }

    /// Subscribes a handler with options using the local backend.
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
        R: IntoEventBusResult + 'static,
    {
        Self::subscribe_with_options(self, subscriber_id, topic, handler, options)
    }

    /// Subscribes a handler on a background thread using local defaults.
    fn subscribe_async<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
    ) -> EventBusResult<JoinHandle<EventBusResult<Subscription<T>>>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Self::subscribe_async(self, subscriber_id, topic, handler)
    }

    /// Subscribes a handler with options on a background thread.
    fn subscribe_with_options_async<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
        options: SubscribeOptions<T>,
    ) -> EventBusResult<JoinHandle<EventBusResult<Subscription<T>>>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Self::subscribe_with_options_async(self, subscriber_id, topic, handler, options)
    }

    /// Waits until local topic work is idle.
    fn wait_for_idle<T>(&self, topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        Self::wait_for_idle(self, topic)
    }
}

/// Typed publisher interceptor adapter.
struct TypedPublisherInterceptor<T: Clone + Send + Sync + 'static> {
    interceptor: Arc<PublisherInterceptorFn<T>>,
}

impl<T> PublisherInterceptorEntry for TypedPublisherInterceptor<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the payload [`TypeId`] handled by this interceptor.
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    /// Downcasts and applies the typed interceptor.
    fn intercept(
        &self,
        envelope: Box<dyn Any + Send>,
    ) -> EventBusResult<Option<Box<dyn Any + Send>>> {
        let envelope = envelope.downcast::<EventEnvelope<T>>().map_err(|_| {
            EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown")
        })?;
        Ok((self.interceptor)(*envelope).map(|envelope| Box::new(envelope) as Box<dyn Any + Send>))
    }
}

/// Typed subscriber interceptor adapter.
struct TypedSubscriberInterceptor<T: Clone + Send + Sync + 'static> {
    interceptor: Arc<SubscriberInterceptorFn<T>>,
}

/// Typed subscription entry stored in the subscription map.
struct TypedSubscriptionEntry<T: Clone + Send + Sync + 'static> {
    id: usize,
    subscriber_id: String,
    topic: Topic<T>,
    active: Arc<AtomicBool>,
    handler: Arc<HandlerFn<T>>,
    options: SubscribeOptions<T>,
}

impl<T> ErasedSubscription for TypedSubscriptionEntry<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns subscription ID.
    fn id(&self) -> usize {
        self.id
    }

    /// Returns subscription priority.
    fn priority(&self) -> i32 {
        self.options.priority()
    }

    /// Downcasts and schedules handler processing.
    fn dispatch(
        &self,
        envelope: Box<dyn Any + Send>,
        bus: Arc<LocalEventBusInner>,
    ) -> EventBusResult<()> {
        if !self.active.load(Ordering::SeqCst) {
            return Ok(());
        }
        let envelope = envelope.downcast::<EventEnvelope<T>>().map_err(|_| {
            EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown")
        })?;
        if !self.options.should_handle(&envelope) {
            return Ok(());
        }
        let topic_key = self.topic.key();
        bus.start_processing(&topic_key)?;
        let guard_topic_key = topic_key.clone();
        let rejected_topic_key = topic_key.clone();

        let active = Arc::clone(&self.active);
        let handler = Arc::clone(&self.handler);
        let options = self.options.clone();
        let subscriber_id = self.subscriber_id.clone();
        let event_bus = LocalEventBus {
            inner: Arc::clone(&bus),
        };
        let bus_for_task = Arc::clone(&bus);
        if let Err(error) = bus.submit_processing_task(move || {
            let _guard = ProcessingGuard::new(Arc::clone(&bus_for_task), guard_topic_key);
            process_subscription_event(
                active,
                handler,
                options,
                subscriber_id,
                *envelope,
                event_bus,
            );
        }) {
            bus.finish_processing(&rejected_topic_key);
            return Err(error);
        }
        Ok(())
    }
}

/// Guard that decrements processing state when a subscriber task exits.
struct ProcessingGuard {
    bus: Arc<LocalEventBusInner>,
    topic_key: TopicKey,
}

impl ProcessingGuard {
    /// Creates a guard for one started processing task.
    ///
    /// # Parameters
    /// - `bus`: Shared local bus state.
    /// - `topic_key`: Topic key whose active count was incremented.
    ///
    /// # Returns
    /// Guard that finishes processing on drop.
    fn new(bus: Arc<LocalEventBusInner>, topic_key: TopicKey) -> Self {
        Self { bus, topic_key }
    }
}

impl Drop for ProcessingGuard {
    /// Marks the tracked topic processing as finished.
    fn drop(&mut self) {
        self.bus.finish_processing(&self.topic_key);
    }
}

/// Processes a subscriber event on a background thread.
///
/// # Parameters
/// - `active`: Shared subscription activity flag.
/// - `handler`: Handler closure.
/// - `options`: Subscriber options.
/// - `subscriber_id`: Subscriber identifier.
/// - `envelope`: Event envelope.
/// - `event_bus`: Event bus used to publish dead letters.
fn process_subscription_event<T>(
    active: Arc<AtomicBool>,
    handler: Arc<HandlerFn<T>>,
    options: SubscribeOptions<T>,
    subscriber_id: String,
    envelope: EventEnvelope<T>,
    event_bus: LocalEventBus,
) where
    T: Clone + Send + Sync + 'static,
{
    if !active.load(Ordering::SeqCst) {
        return;
    }
    let acknowledgement = Acknowledgement::new();
    let delivered = envelope
        .clone()
        .with_acknowledgement(acknowledgement.clone());
    match run_handler_with_retry(&handler, &options, delivered.clone()) {
        Ok(()) => {
            if acknowledgement.is_nacked() {
                let error = EventBusError::handler_failed("subscriber nacked the event");
                handle_subscription_failure(
                    &options,
                    &subscriber_id,
                    &delivered,
                    &error,
                    &acknowledgement,
                    &event_bus,
                );
            } else if options.ack_mode() == AckMode::Auto && !acknowledgement.is_completed() {
                acknowledgement.ack();
            }
        }
        Err(error) => {
            handle_subscription_failure(
                &options,
                &subscriber_id,
                &delivered,
                &error,
                &acknowledgement,
                &event_bus,
            );
        }
    }
}

/// Handles a terminal subscriber failure.
///
/// # Parameters
/// - `options`: Subscription options containing error handlers and DLQ policy.
/// - `subscriber_id`: Subscriber identifier.
/// - `delivered`: Delivered event envelope.
/// - `error`: Failure reason.
/// - `acknowledgement`: Shared acknowledgement state.
/// - `event_bus`: Bus used to publish dead-letter events.
fn handle_subscription_failure<T>(
    options: &SubscribeOptions<T>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    acknowledgement: &Acknowledgement,
    event_bus: &LocalEventBus,
) where
    T: Clone + Send + Sync + 'static,
{
    let _ = options.notify_subscribe_error(subscriber_id, delivered, error, acknowledgement);
    if !acknowledgement.is_completed() {
        acknowledgement.nack();
    }
    if acknowledgement.is_nacked()
        && !delivered.is_dead_letter()
        && let Some(dead_letter) = options.create_dead_letter(subscriber_id, delivered, error)
    {
        let _ = event_bus.publish_envelope(dead_letter.as_dead_letter());
    }
}

/// Runs a handler with retry options.
///
/// # Parameters
/// - `handler`: Subscriber handler.
/// - `options`: Subscriber options.
/// - `envelope`: Delivered envelope.
///
/// # Returns
/// `Ok(())` after a successful attempt, or the final handler error.
fn run_handler_with_retry<T>(
    handler: &Arc<HandlerFn<T>>,
    options: &SubscribeOptions<T>,
    envelope: EventEnvelope<T>,
) -> EventBusResult<()>
where
    T: Clone + Send + Sync + 'static,
{
    run_with_retry(options.retry_options().unwrap_or_default(), || {
        call_handler(handler, envelope.clone())
    })
}

/// Calls a subscriber handler while converting panics into handler errors.
///
/// # Parameters
/// - `handler`: Subscriber handler or interceptor chain.
/// - `envelope`: Envelope delivered to the handler.
///
/// # Returns
/// Handler result, with panics converted to [`EventBusError::HandlerPanicked`].
fn call_handler<T>(handler: &Arc<HandlerFn<T>>, envelope: EventEnvelope<T>) -> EventBusResult<()>
where
    T: Clone + Send + Sync + 'static,
{
    match panic::catch_unwind(AssertUnwindSafe(|| handler(envelope))) {
        Ok(result) => result,
        Err(_) => Err(EventBusError::handler_panicked()),
    }
}

/// Runs a fallible operation with the event-bus retry options.
///
/// # Parameters
/// - `retry_options`: Simple event-bus retry options.
/// - `operation`: Operation to call for each attempt.
///
/// # Returns
/// Successful operation value or the final event-bus error.
fn run_with_retry<T, F>(retry_options: crate::RetryOptions, operation: F) -> EventBusResult<T>
where
    F: FnMut() -> EventBusResult<T>,
{
    let max_attempts = u32::try_from(retry_options.max_attempts()).map_err(|_| {
        EventBusError::invalid_argument("retry_options", "retry max_attempts exceeds u32::MAX")
    })?;
    let mut builder = qubit_retry::Retry::<EventBusError>::builder().max_attempts(max_attempts);
    builder = if retry_options.delay().is_zero() {
        builder.no_delay()
    } else {
        builder.fixed_delay(retry_options.delay())
    };
    let retry = builder
        .build()
        .map_err(|error| EventBusError::invalid_argument("retry_options", error.to_string()))?;
    retry.run(operation).map_err(|error| {
        error
            .last_error()
            .cloned()
            .unwrap_or_else(|| EventBusError::handler_failed(error.to_string()))
    })
}

/// Waits for a fixed handler executor to finish after shutdown.
///
/// # Parameters
/// - `executor`: Executor whose graceful shutdown has already been requested.
fn wait_for_executor_termination(executor: &FixedThreadPool) {
    while !executor.is_terminated() {
        thread::sleep(Duration::from_millis(1));
    }
}

/// Returns the default subscription handler worker count.
///
/// # Returns
/// Available CPU parallelism, or `1` if it cannot be detected.
fn default_subscription_handler_pool_size() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}
