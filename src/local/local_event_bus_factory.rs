/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Factory for local event bus instances.

use std::any::{
    Any,
    TypeId,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    DeadLetterPayload,
    EventBusError,
    EventBusFactory,
    EventBusResult,
    EventEnvelope,
    LocalEventBus,
    PublishOptions,
    PublisherInterceptor,
    SubscribeOptions,
    SubscriberInterceptor,
    UnsupportedTransactionalEventBus,
};

use super::local_event_bus::{
    create_publisher_interceptor_entry,
    create_subscriber_interceptor_entry,
};
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;

/// Returns the default subscription handler worker count.
///
/// # Returns
/// Available CPU parallelism, or `1` if it cannot be detected.
fn default_subscription_handler_pool_size() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

/// Factory used to create [`LocalEventBus`] instances with default options.
pub struct LocalEventBusFactory {
    default_publish_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    default_dead_letter_strategies: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    publisher_interceptors: Vec<Arc<dyn PublisherInterceptorEntry>>,
    subscriber_interceptors: Vec<Arc<dyn SubscriberInterceptorEntry>>,
    subscription_handler_pool_size: usize,
    subscription_handler_queue_capacity: Option<usize>,
}

impl Default for LocalEventBusFactory {
    /// Creates an empty local event bus factory with default runtime options.
    fn default() -> Self {
        Self::new()
    }
}

impl LocalEventBusFactory {
    /// Creates an empty local event bus factory.
    ///
    /// # Returns
    /// Factory with no typed defaults.
    pub fn new() -> Self {
        Self {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            publisher_interceptors: Vec::new(),
            subscriber_interceptors: Vec::new(),
            subscription_handler_pool_size: default_subscription_handler_pool_size(),
            subscription_handler_queue_capacity: None,
        }
    }

    /// Sets default publish options for a payload type.
    ///
    /// # Parameters
    /// - `options`: Options used by default publish methods for payload `T`.
    pub fn set_default_publish_options<T>(&mut self, options: PublishOptions<T>)
    where
        T: Send + Sync + 'static,
    {
        self.default_publish_options
            .insert(TypeId::of::<T>(), Arc::new(options));
    }

    /// Sets default subscribe options for a payload type.
    ///
    /// # Parameters
    /// - `options`: Options used by [`LocalEventBus::subscribe`] for payload `T`.
    pub fn set_default_subscribe_options<T>(&mut self, options: SubscribeOptions<T>)
    where
        T: Send + Sync + 'static,
    {
        self.default_subscribe_options
            .insert(TypeId::of::<T>(), Arc::new(options));
    }

    /// Sets the default dead-letter strategy for a payload type.
    ///
    /// # Parameters
    /// - `strategy`: Strategy used when subscription options do not provide one.
    pub fn set_default_dead_letter_strategy<T, F>(&mut self, strategy: F)
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(
                &str,
                &EventEnvelope<T>,
                &EventBusError,
                &SubscribeOptions<T>,
            ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
            + Send
            + Sync
            + 'static,
    {
        let strategy: Arc<crate::core::subscribe_options::DeadLetterStrategyFn<T>> =
            Arc::new(strategy);
        self.default_dead_letter_strategies
            .insert(TypeId::of::<T>(), Arc::new(strategy));
    }

    /// Adds a publisher interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Callback that can modify or drop outgoing envelopes.
    ///
    /// # Returns
    /// `Ok(())` when the interceptor is stored.
    pub fn add_publisher_interceptor<T, I>(&mut self, interceptor: I) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: PublisherInterceptor<T>,
    {
        self.publisher_interceptors
            .push(create_publisher_interceptor_entry::<T, I>(interceptor));
        Ok(())
    }

    /// Adds a subscriber interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Callback wrapping subscriber handler execution.
    ///
    /// # Returns
    /// `Ok(())` when the interceptor is stored.
    pub fn add_subscriber_interceptor<T, I>(&mut self, interceptor: I) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: SubscriberInterceptor<T>,
    {
        self.subscriber_interceptors
            .push(create_subscriber_interceptor_entry::<T, I>(interceptor));
        Ok(())
    }

    /// Sets the subscription handler worker count for created buses.
    ///
    /// # Parameters
    /// - `pool_size`: Number of worker threads used for subscriber handlers.
    ///
    /// # Returns
    /// `Ok(())` when the value is stored.
    ///
    /// # Errors
    /// Returns [`EventBusError::InvalidArgument`] when `pool_size` is zero.
    pub fn set_subscription_handler_pool_size(&mut self, pool_size: usize) -> EventBusResult<()> {
        if pool_size == 0 {
            return Err(EventBusError::invalid_argument(
                "pool_size",
                "subscription handler pool size must be greater than zero",
            ));
        }
        self.subscription_handler_pool_size = pool_size;
        Ok(())
    }

    /// Sets the optional subscription handler queue capacity.
    ///
    /// # Parameters
    /// - `capacity`: Maximum queued subscriber tasks, or `None` for unbounded.
    ///
    /// # Returns
    /// `Ok(())` when the value is stored.
    ///
    /// # Errors
    /// Returns [`EventBusError::InvalidArgument`] when a configured capacity is zero.
    pub fn set_subscription_handler_queue_capacity(
        &mut self,
        capacity: Option<usize>,
    ) -> EventBusResult<()> {
        if capacity == Some(0) {
            return Err(EventBusError::invalid_argument(
                "capacity",
                "subscription handler queue capacity must be greater than zero",
            ));
        }
        self.subscription_handler_queue_capacity = capacity;
        Ok(())
    }

    /// Creates a stopped event bus.
    ///
    /// # Returns
    /// Local event bus initialized with factory defaults.
    pub fn create(&self) -> LocalEventBus {
        LocalEventBus::with_runtime_options(
            self.default_publish_options.clone(),
            self.default_subscribe_options.clone(),
            self.default_dead_letter_strategies.clone(),
            self.publisher_interceptors.clone(),
            self.subscriber_interceptors.clone(),
            self.subscription_handler_pool_size,
            self.subscription_handler_queue_capacity,
        )
    }

    /// Creates and starts an event bus.
    ///
    /// # Returns
    /// Started local event bus initialized with factory defaults.
    ///
    /// # Errors
    /// Returns startup errors from the handler executor.
    pub fn create_started(&self) -> EventBusResult<LocalEventBus> {
        let bus = self.create();
        bus.start()?;
        Ok(bus)
    }
}

impl EventBusFactory for LocalEventBusFactory {
    type Bus = LocalEventBus;
    type TransactionalBus = UnsupportedTransactionalEventBus;

    /// Local event bus does not support transactional operations.
    fn is_transactional_supported(&self) -> bool {
        false
    }

    /// Creates a stopped local event bus.
    fn create(&self) -> Self::Bus {
        Self::create(self)
    }

    /// Creates and starts a local event bus.
    fn create_started(&self) -> EventBusResult<Self::Bus> {
        Self::create_started(self)
    }

    /// Sets typed default publish options for local buses.
    fn set_default_publish_options<T>(&mut self, options: PublishOptions<T>) -> EventBusResult<()>
    where
        T: Send + Sync + 'static,
    {
        Self::set_default_publish_options(self, options);
        Ok(())
    }

    /// Sets typed default subscribe options for local buses.
    fn set_default_subscribe_options<T>(
        &mut self,
        options: SubscribeOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Send + Sync + 'static,
    {
        Self::set_default_subscribe_options(self, options);
        Ok(())
    }

    /// Sets a typed default dead-letter strategy for local buses.
    fn set_default_dead_letter_strategy<T, F>(&mut self, strategy: F) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(
                &str,
                &EventEnvelope<T>,
                &EventBusError,
                &SubscribeOptions<T>,
            ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
            + Send
            + Sync
            + 'static,
    {
        Self::set_default_dead_letter_strategy::<T, F>(self, strategy);
        Ok(())
    }

    /// Adds a typed publisher interceptor for local buses.
    fn add_publisher_interceptor<T, I>(&mut self, interceptor: I) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: PublisherInterceptor<T>,
    {
        Self::add_publisher_interceptor::<T, I>(self, interceptor)
    }

    /// Adds a typed subscriber interceptor for local buses.
    fn add_subscriber_interceptor<T, I>(&mut self, interceptor: I) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: SubscriberInterceptor<T>,
    {
        Self::add_subscriber_interceptor::<T, I>(self, interceptor)
    }
}
