// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow type-file-name
//! Typed subscription-entry implementation.

use std::any::Any;
use std::any::type_name;
use std::sync::Arc;

use qubit_argument::StringArgument;

use super::super::erased_subscription::DispatchAdmission;
use super::super::erased_subscription::ErasedSubscription;
use super::super::ordering_lane_key::OrderingLaneKey;
use super::super::processing_task::DeliveryContext;
use super::super::processing_task::ProcessingTask;
use super::HandlerFn;
use super::LocalEventBus;
use super::LocalEventBusInner;
use super::enter_subscription_worker;
use super::local_event_bus_id;
use super::process_subscription_event;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::IntoEventBusResult;
use crate::SubscribeOptions;
use crate::Subscription;
use crate::Topic;
use crate::core::SubscriptionState;

/// Typed subscription entry stored in the subscription map.
pub(super) struct TypedSubscriptionEntry<T: Clone + Send + Sync + 'static> {
    pub(super) id: usize,
    pub(super) subscriber_id: String,
    pub(super) topic: Topic<T>,
    pub(super) active: Arc<SubscriptionState>,
    pub(super) handler: Arc<HandlerFn<T>>,
    pub(super) options: SubscribeOptions<T>,
}

impl<T> ErasedSubscription for TypedSubscriptionEntry<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn id(&self) -> usize {
        self.id
    }

    fn subscriber_id(&self) -> &str {
        &self.subscriber_id
    }

    fn priority(&self) -> i32 {
        self.options.priority()
    }

    fn deactivate(&self) {
        self.active.deactivate();
    }

    fn dispatch(
        &self,
        envelope: Box<dyn Any + Send>,
        bus: Arc<LocalEventBusInner>,
        allow_stopping: bool,
    ) -> EventBusResult<DispatchAdmission> {
        if !self.active.is_active() {
            return Ok(DispatchAdmission::Filtered);
        }
        let envelope = envelope
            .downcast::<EventEnvelope<T>>()
            .map_err(|_| EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown"))?;
        if !self.options.try_should_handle(&envelope)? {
            return Ok(DispatchAdmission::Filtered);
        }
        let topic_key = self.topic.key();
        let delivery_permit = bus.try_acquire_delivery_permit()?;
        bus.start_processing(&topic_key)?;
        let ordering_lane_key = envelope
            .ordering_key()
            .map(|ordering_key| OrderingLaneKey::new(topic_key.clone(), ordering_key, self.id));
        let delay = envelope.delay();
        let active = Arc::clone(&self.active);
        let delayed_active = Arc::clone(&self.active);
        let handler = Arc::clone(&self.handler);
        let options = self.options.clone();
        let subscriber_id = self.subscriber_id.clone();
        let subscription_id = self.id;
        let event_bus = LocalEventBus {
            inner: Arc::clone(&bus),
        };
        let bus_id = local_event_bus_id(&bus);
        let delivery_context = DeliveryContext {
            event_id: envelope.id().to_string(),
            topic_name: self.topic.name().to_string(),
            subscriber_id: self.subscriber_id.clone(),
        };
        let processing_task = ProcessingTask::with_delivery_context_and_permit(
            Arc::clone(&bus),
            topic_key,
            delivery_context,
            delivery_permit,
            move || {
                let _worker_context = enter_subscription_worker(bus_id);
                if !active.is_active() {
                    return;
                }
                process_subscription_event(
                    active,
                    handler,
                    options,
                    subscription_id,
                    subscriber_id,
                    *envelope,
                    event_bus,
                );
            },
        );
        let result = if let Some(ordering_lane_key) = ordering_lane_key {
            if let Some(delay) = delay
                && !delay.is_zero()
            {
                bus.submit_delayed_ordered_processing_task(
                    ordering_lane_key,
                    processing_task,
                    delay,
                    delayed_active,
                    allow_stopping,
                )
            } else {
                bus.submit_ordered_processing_task(ordering_lane_key, processing_task, allow_stopping)
            }
        } else if let Some(delay) = delay
            && !delay.is_zero()
        {
            bus.submit_delayed_processing_task(processing_task, delay, delayed_active, allow_stopping)
        } else {
            bus.submit_processing_task(move || processing_task.run(), allow_stopping)
        };
        result.map(|_| DispatchAdmission::Accepted)
    }
}

impl LocalEventBus {
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
        self.subscribe_with_options(subscriber_id, topic, handler, SubscribeOptions::empty())
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
        let options = options.merge_defaults(self.default_subscribe_options::<T>());
        let subscriber_id = subscriber_id
            .into()
            .require_non_blank("subscriber_id")
            .map_err(|_| EventBusError::invalid_argument("subscriber_id", "subscriber ID must not be blank"))?;

        let id = self.inner.next_subscription_id();
        let active = Arc::new(SubscriptionState::active());
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
            .register_subscription_if_started(topic_key.clone(), Arc::new(entry))?;

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

    /// Subscribes a handler to a dead-letter topic.
    ///
    /// Dead-letter payloads are type-erased, so callers can inspect the
    /// original topic, error metadata, and original payload through
    /// [`crate::DeadLetterPayload`].
    ///
    /// # Parameters
    /// - `dead_letter_topic`: Topic carrying dead-letter records.
    /// - `handler`: Handler invoked for dead-letter events.
    /// - `options`: Subscription options merged with factory defaults.
    ///
    /// # Returns
    /// Subscription handle for the dead-letter topic.
    ///
    /// # Errors
    /// Returns an error when the bus is stopped, the generated subscriber ID is
    /// invalid, or shared state is unavailable.
    pub fn add_dead_letter_handler<F, R>(
        &self,
        dead_letter_topic: &Topic<crate::DeadLetterPayload>,
        handler: F,
        options: SubscribeOptions<crate::DeadLetterPayload>,
    ) -> EventBusResult<Subscription<crate::DeadLetterPayload>>
    where
        F: Fn(EventEnvelope<crate::DeadLetterPayload>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.subscribe_with_options(
            format!("dead-letter:{}", dead_letter_topic.name()),
            dead_letter_topic,
            handler,
            options,
        )
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
}
