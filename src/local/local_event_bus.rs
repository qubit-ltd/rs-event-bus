// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Thread-safe in-process event bus.

mod dead_letter;
mod dispatch;
mod interceptor;
mod lifecycle;
mod retry_delivery;
mod subscription_entry;
mod worker_context;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub use interceptor::IntoPublisherInterceptorAnyResult;
pub use interceptor::IntoPublisherInterceptorResult;
pub use interceptor::PublisherInterceptor;
pub use interceptor::PublisherInterceptorAny;
pub use interceptor::SubscriberInterceptor;
pub use interceptor::SubscriberInterceptorAny;
pub(super) use interceptor::create_publisher_interceptor_entry;
pub(super) use interceptor::create_subscriber_interceptor_entry;

use super::local_event_bus_inner::LocalEventBusInner;
use super::local_event_bus_inner::LocalEventBusRuntimeOptions;
use crate::BatchPublishResult;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::IntoEventBusResult;
use crate::PublishOptions;
use crate::PublishReceipt;
use crate::SubscribeOptions;
use crate::Subscription;
use crate::Topic;
use crate::core::delivery_limits::DeliveryLimits;

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;
use retry_delivery::normalize_subscriber_interceptor_result;
use retry_delivery::process_subscription_event;
use retry_delivery::run_dispatch_with_retry;
use worker_context::enter_subscription_worker;
use worker_context::local_event_bus_id;

/// Thread-safe in-process event bus.
///
/// This backend stores subscriptions in memory and dispatches subscriber
/// handlers on background threads. Publishing schedules work and returns after
/// dispatch, while [`wait_for_idle`](Self::wait_for_idle) can be used by tests
/// to wait for all handler work for a topic.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::{LocalEventBus, Topic};
///
/// let bus = LocalEventBus::started().unwrap();
/// let topic = Topic::<String>::try_new("orders.created").unwrap();
/// bus.subscribe("audit", &topic, |_| {}).unwrap();
/// bus.publish(&topic, "order-1001".to_string()).unwrap();
/// bus.wait_for_idle(&topic).unwrap();
/// ```
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
        Self::with_runtime_options(LocalEventBusRuntimeOptions {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: Vec::new(),
            subscriber_interceptors: Vec::new(),
            subscription_handler_pool_size: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            delivery_limits: DeliveryLimits::default(),
        })
    }

    /// Creates and starts a local event bus.
    ///
    /// # Returns
    /// A started event bus.
    ///
    /// # Errors
    /// Returns startup errors from the handler executor.
    pub fn started() -> EventBusResult<Self> {
        let bus = Self::new();
        bus.start()?;
        Ok(bus)
    }

    /// Creates a stopped event bus with typed defaults and runtime options.
    ///
    /// # Returns
    /// A stopped event bus.
    pub(crate) fn with_runtime_options(options: LocalEventBusRuntimeOptions) -> Self {
        Self {
            inner: Arc::new(LocalEventBusInner::new(options)),
        }
    }
}

impl Default for LocalEventBus {
    /// Creates a stopped local event bus.
    fn default() -> Self {
        Self::new()
    }
}

impl crate::EventBus for LocalEventBus {
    type Subscription<T>
        = Subscription<T>
    where
        T: Clone + Send + Sync + 'static;

    /// Starts the local event bus.
    fn start(&self) -> EventBusResult<bool> {
        Self::start(self)
    }

    /// Shuts down the local event bus.
    fn shutdown(&self) -> bool {
        Self::shutdown(self)
    }

    /// Publishes a payload using the local backend.
    fn publish<T>(&self, topic: &Topic<T>, payload: T) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish(self, topic, payload)
    }

    /// Publishes a payload with options using the local backend.
    fn publish_with_options<T>(
        &self,
        topic: &Topic<T>,
        payload: T,
        options: PublishOptions<T>,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_with_options(self, topic, payload, options)
    }

    /// Publishes an envelope using the local backend.
    fn publish_envelope<T>(&self, envelope: EventEnvelope<T>) -> EventBusResult<PublishReceipt>
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
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_envelope_with_options(self, envelope, options)
    }

    /// Publishes a batch using the local backend.
    fn publish_all<T>(&self, envelopes: Vec<EventEnvelope<T>>) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_all(self, envelopes)
    }

    /// Publishes a batch with options using the local backend.
    fn publish_all_with_options<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_all_with_options(self, envelopes, options)
    }

    /// Subscribes a handler using local backend defaults.
    fn subscribe<T, S, F, R>(&self, subscriber_id: S, topic: &Topic<T>, handler: F) -> EventBusResult<Subscription<T>>
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

    /// Waits until local topic work is idle.
    fn wait_for_idle<T>(&self, topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        Self::wait_for_idle(self, topic)
    }

    /// Waits until local topic work is idle or the timeout elapses.
    fn wait_for_idle_timeout<T>(&self, topic: &Topic<T>, timeout: Duration) -> EventBusResult<bool>
    where
        T: 'static,
    {
        Self::wait_for_idle_timeout(self, topic, timeout)
    }
}
