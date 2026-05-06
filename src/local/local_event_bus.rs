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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use crate::{
    AckMode, Acknowledgement, EventBusError, EventBusResult, EventEnvelope, IntoEventBusResult,
    PublishOptions, SubscribeOptions, Subscription, Topic,
};

use super::erased_subscription::ErasedSubscription;
use super::local_event_bus_inner::LocalEventBusInner;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;
type PublisherInterceptorFn<T> =
    dyn Fn(EventEnvelope<T>) -> Option<EventEnvelope<T>> + Send + Sync + 'static;

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
        Self {
            inner: Arc::new(LocalEventBusInner::new(default_subscribe_options)),
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
        if !self.inner.mark_stopped() {
            return false;
        }
        let _ = self.inner.wait_for_all_idle();
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
        let subscriptions = self.inner.subscriptions_for(&envelope.topic().key())?;
        for subscription in subscriptions {
            if let Err(error) =
                subscription.dispatch(Box::new(envelope.clone()), Arc::clone(&self.inner))
            {
                options.notify_publish_error(&envelope, &error);
                return Err(error);
            }
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
        let topic_key = self.topic.key();
        bus.start_processing(&topic_key)?;

        let active = Arc::clone(&self.active);
        let handler = Arc::clone(&self.handler);
        let options = self.options.clone();
        let subscriber_id = self.subscriber_id.clone();
        let event_bus = LocalEventBus {
            inner: Arc::clone(&bus),
        };
        thread::spawn(move || {
            process_subscription_event(
                active,
                handler,
                options,
                subscriber_id,
                *envelope,
                event_bus,
            );
            bus.finish_processing(&topic_key);
        });
        Ok(())
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
    if !active.load(Ordering::SeqCst) || !options.should_handle(&envelope) {
        return;
    }
    let acknowledgement = Acknowledgement::new();
    let delivered = envelope
        .clone()
        .with_acknowledgement(acknowledgement.clone());
    match run_handler_with_retry(&handler, &options, delivered.clone()) {
        Ok(()) => {
            if options.ack_mode() == AckMode::Auto && !acknowledgement.is_completed() {
                acknowledgement.ack();
            }
        }
        Err(error) => {
            options.notify_subscribe_error(&subscriber_id, &delivered, &error, &acknowledgement);
            if !acknowledgement.is_completed() {
                acknowledgement.nack();
            }
            if acknowledgement.is_nacked()
                && !delivered.is_dead_letter()
                && let Some(dead_letter) =
                    options.create_dead_letter(&subscriber_id, &delivered, &error)
            {
                let _ = event_bus.publish_envelope(dead_letter);
            }
        }
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
    let retry = options.retry_options().unwrap_or_default();
    let mut last_error = EventBusError::handler_failed("handler did not run");
    for _ in 0..retry.max_attempts() {
        match handler(envelope.clone()) {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = error;
                thread::sleep(retry.delay());
            }
        }
    }
    Err(last_error)
}

/// Helpers that exercise defensive branches for coverage-oriented tests.
#[doc(hidden)]
pub mod coverage_support {
    /// Exercises defensive local event bus branches that are hard to reach
    /// through safe public APIs.
    ///
    /// # Returns
    /// Diagnostic strings collected from covered branches.
    pub fn exercise_local_event_bus_paths() -> Vec<String> {
        let mut diagnostics = super::coverage_exercise_local_event_bus_paths();
        diagnostics
            .extend(crate::local::local_event_bus_inner::coverage_exercise_inner_poison_paths());
        diagnostics
    }
}

/// Exercises defensive local event bus branches for coverage-oriented tests.
///
/// # Returns
/// Diagnostic strings proving each branch was reached.
pub(crate) fn coverage_exercise_local_event_bus_paths() -> Vec<String> {
    let mut diagnostics = Vec::new();
    let topic = Topic::<String>::try_new("coverage.local").expect("coverage topic should build");

    let failing_bus = LocalEventBus::started();
    failing_bus
        .inner
        .add_subscription(topic.key(), Arc::new(CoverageFailingSubscription))
        .expect("coverage subscription should be stored");
    failing_bus
        .inner
        .add_subscription(topic.key(), Arc::new(CoverageFailingSubscription))
        .expect("coverage subscription should be stored");
    diagnostics.push(ErasedSubscription::priority(&CoverageFailingSubscription).to_string());
    let publish_error = failing_bus
        .publish_envelope_with_options(
            EventEnvelope::create(topic.clone(), "payload".to_string()),
            PublishOptions::builder().error_handler(|_, _| ()).build(),
        )
        .expect_err("failing subscription should reject dispatch");
    diagnostics.push(publish_error.to_string());
    failing_bus
        .inner
        .unsubscribe(&topic.key(), 10_001)
        .expect("coverage unsubscribe should be idempotent");
    failing_bus
        .inner
        .unsubscribe(&topic.key(), 404)
        .expect("coverage missing unsubscribe should be idempotent");

    let bad_interceptor_bus = LocalEventBus::started();
    diagnostics.push(
        CoverageBadPublisherInterceptor
            .payload_type_id()
            .eq(&TypeId::of::<String>())
            .to_string(),
    );
    bad_interceptor_bus
        .inner
        .add_publisher_interceptor(Arc::new(CoverageBadPublisherInterceptor))
        .expect("coverage interceptor should be stored");
    let interceptor_error = bad_interceptor_bus
        .publish(&topic, "payload".to_string())
        .expect_err("bad interceptor should return wrong payload type");
    diagnostics.push(interceptor_error.to_string());

    let typed_interceptor = TypedPublisherInterceptor::<String> {
        interceptor: Arc::new(Some),
    };
    let wrong_interceptor_payload =
        EventEnvelope::create(Topic::<u32>::try_new("coverage.u32").expect("topic"), 7_u32);
    let typed_interceptor_error = PublisherInterceptorEntry::intercept(
        &typed_interceptor,
        Box::new(wrong_interceptor_payload),
    )
    .expect_err("typed interceptor should reject wrong payload type");
    diagnostics.push(typed_interceptor_error.to_string());

    let inactive_entry = TypedSubscriptionEntry {
        id: 77,
        subscriber_id: "coverage-sub".to_string(),
        topic: topic.clone(),
        active: Arc::new(AtomicBool::new(false)),
        handler: Arc::new(|_| Ok(())),
        options: SubscribeOptions::empty(),
    };
    ErasedSubscription::dispatch(
        &inactive_entry,
        Box::new(EventEnvelope::create(topic.clone(), "inactive".to_string())),
        Arc::clone(&failing_bus.inner),
    )
    .expect("inactive subscription should skip dispatch");
    inactive_entry.active.store(true, Ordering::SeqCst);
    let wrong_subscription_payload = EventEnvelope::create(
        Topic::<u32>::try_new("coverage.u32.sub").expect("topic"),
        9_u32,
    );
    let typed_subscription_error = ErasedSubscription::dispatch(
        &inactive_entry,
        Box::new(wrong_subscription_payload),
        Arc::clone(&failing_bus.inner),
    )
    .expect_err("typed subscription should reject wrong payload type");
    diagnostics.push(typed_subscription_error.to_string());
    ErasedSubscription::dispatch(
        &inactive_entry,
        Box::new(EventEnvelope::create(topic.clone(), "handled".to_string())),
        Arc::clone(&failing_bus.inner),
    )
    .expect("active subscription should accept the right payload type");
    failing_bus
        .wait_for_idle(&topic)
        .expect("coverage handler should finish");

    let wait_key = topic.key();
    failing_bus
        .inner
        .start_processing(&wait_key)
        .expect("coverage processing should start");
    let inner = Arc::clone(&failing_bus.inner);
    let wait_key_for_worker = wait_key.clone();
    let worker = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(1));
        inner.finish_processing(&wait_key_for_worker);
    });
    failing_bus
        .inner
        .wait_for_all_idle()
        .expect("coverage wait should finish");
    worker.join().expect("coverage worker should join");
    failing_bus.inner.finish_processing(&wait_key);

    diagnostics
}

/// Subscription entry that always fails dispatch for coverage-oriented tests.
struct CoverageFailingSubscription;

impl ErasedSubscription for CoverageFailingSubscription {
    /// Returns a fixed coverage subscription ID.
    fn id(&self) -> usize {
        10_001
    }

    /// Returns neutral priority.
    fn priority(&self) -> i32 {
        0
    }

    /// Returns a synthetic type mismatch error.
    fn dispatch(
        &self,
        _envelope: Box<dyn Any + Send>,
        _bus: Arc<LocalEventBusInner>,
    ) -> EventBusResult<()> {
        Err(EventBusError::type_mismatch(
            "coverage expected",
            "coverage actual",
        ))
    }
}

/// Publisher interceptor returning the wrong boxed type for coverage-oriented tests.
struct CoverageBadPublisherInterceptor;

impl PublisherInterceptorEntry for CoverageBadPublisherInterceptor {
    /// Returns the string payload type.
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<String>()
    }

    /// Returns the wrong boxed payload to exercise final downcast errors.
    fn intercept(
        &self,
        _envelope: Box<dyn Any + Send>,
    ) -> EventBusResult<Option<Box<dyn Any + Send>>> {
        Ok(Some(Box::new("wrong-envelope".to_string())))
    }
}
