// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Factory for local event bus instances.

use std::any::{
    Any,
    TypeId,
};
use std::collections::HashMap;
use std::sync::Arc;

use qubit_argument::{
    NumericArgument,
    OptionArgument,
};

use crate::{
    DeadLetterStrategyAnyCallback,
    DeadLetterStrategyCallback,
    EventBusError,
    EventBusFactory,
    EventBusResult,
    LocalEventBus,
    PublishOptions,
    PublisherInterceptor,
    PublisherInterceptorAny,
    SubscribeOptions,
    SubscriberInterceptor,
    SubscriberInterceptorAny,
    UnsupportedTransactionalEventBus,
};

use super::local_event_bus::{
    create_publisher_interceptor_entry,
    create_subscriber_interceptor_entry,
};
use super::local_event_bus_inner::LocalEventBusRuntimeOptions;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;
use crate::core::subscribe_options::{
    DeadLetterStrategyAnyFn,
    wrap_dead_letter_strategy,
    wrap_dead_letter_strategy_any,
};

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
    global_default_dead_letter_strategy: Option<Arc<DeadLetterStrategyAnyFn>>,
    global_publisher_interceptors: Vec<Arc<dyn PublisherInterceptorAny>>,
    global_subscriber_interceptors: Vec<Arc<dyn SubscriberInterceptorAny>>,
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
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: Vec::new(),
            subscriber_interceptors: Vec::new(),
            subscription_handler_pool_size:
                default_subscription_handler_pool_size(),
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
    /// - `options`: Options used by [`LocalEventBus::subscribe`] for payload
    ///   `T`.
    pub fn set_default_subscribe_options<T>(
        &mut self,
        options: SubscribeOptions<T>,
    ) where
        T: Send + Sync + 'static,
    {
        self.default_subscribe_options
            .insert(TypeId::of::<T>(), Arc::new(options));
    }

    /// Sets the default dead-letter strategy for a payload type.
    ///
    /// # Parameters
    /// - `strategy`: Strategy used when subscription options do not provide
    ///   one.
    pub fn set_default_dead_letter_strategy<T, F>(&mut self, strategy: F)
    where
        T: Clone + Send + Sync + 'static,
        F: DeadLetterStrategyCallback<T>,
    {
        let strategy = wrap_dead_letter_strategy(strategy);
        self.default_dead_letter_strategies
            .insert(TypeId::of::<T>(), Arc::new(strategy));
    }

    /// Sets the global default dead-letter strategy.
    ///
    /// # Parameters
    /// - `strategy`: Strategy used when no subscription or typed factory
    ///   dead-letter strategy is configured.
    pub fn set_global_default_dead_letter_strategy<F>(&mut self, strategy: F)
    where
        F: DeadLetterStrategyAnyCallback,
    {
        self.global_default_dead_letter_strategy =
            Some(wrap_dead_letter_strategy_any(strategy));
    }

    /// Adds a publisher interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Callback that can modify or drop outgoing envelopes.
    ///
    /// # Returns
    /// `Ok(())` when the interceptor is stored.
    pub fn add_publisher_interceptor<T, I>(
        &mut self,
        interceptor: I,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: PublisherInterceptor<T>,
    {
        self.publisher_interceptors
            .push(create_publisher_interceptor_entry::<T, I>(interceptor));
        Ok(())
    }

    /// Adds a global publisher interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Callback that can mutate metadata or drop any event.
    ///
    /// # Returns
    /// `Ok(())` when the interceptor is stored.
    pub fn add_global_publisher_interceptor<I>(
        &mut self,
        interceptor: I,
    ) -> EventBusResult<()>
    where
        I: PublisherInterceptorAny,
    {
        self.global_publisher_interceptors
            .push(Arc::new(interceptor));
        Ok(())
    }

    /// Adds a subscriber interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Callback wrapping subscriber handler execution.
    ///
    /// # Returns
    /// `Ok(())` when the interceptor is stored.
    pub fn add_subscriber_interceptor<T, I>(
        &mut self,
        interceptor: I,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: SubscriberInterceptor<T>,
    {
        self.subscriber_interceptors
            .push(create_subscriber_interceptor_entry::<T, I>(interceptor));
        Ok(())
    }

    /// Adds a global subscriber interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Callback wrapping subscriber handling for any payload
    ///   type.
    ///
    /// # Returns
    /// `Ok(())` when the interceptor is stored.
    pub fn add_global_subscriber_interceptor<I>(
        &mut self,
        interceptor: I,
    ) -> EventBusResult<()>
    where
        I: SubscriberInterceptorAny,
    {
        self.global_subscriber_interceptors
            .push(Arc::new(interceptor));
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
    pub fn set_subscription_handler_pool_size(
        &mut self,
        pool_size: usize,
    ) -> EventBusResult<()> {
        let pool_size =
            pool_size.require_positive("pool_size").map_err(|_| {
                EventBusError::invalid_argument(
                    "pool_size",
                    "subscription handler pool size must be greater than zero",
                )
            })?;
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
    /// Returns [`EventBusError::InvalidArgument`] when a configured capacity is
    /// zero.
    pub fn set_subscription_handler_queue_capacity(
        &mut self,
        capacity: Option<usize>,
    ) -> EventBusResult<()> {
        let capacity = capacity
            .validate_some(|capacity| capacity.require_positive("capacity"))
            .map_err(|_| {
                EventBusError::invalid_argument(
                    "capacity",
                    "subscription handler queue capacity must be greater than zero",
                )
            })?;
        self.subscription_handler_queue_capacity = capacity;
        Ok(())
    }

    /// Creates a stopped event bus.
    ///
    /// # Returns
    /// Local event bus initialized with factory defaults.
    pub fn create(&self) -> LocalEventBus {
        LocalEventBus::with_runtime_options(LocalEventBusRuntimeOptions {
            default_publish_options: self.default_publish_options.clone(),
            default_subscribe_options: self.default_subscribe_options.clone(),
            default_dead_letter_strategies: self
                .default_dead_letter_strategies
                .clone(),
            global_default_dead_letter_strategy: self
                .global_default_dead_letter_strategy
                .clone(),
            global_publisher_interceptors: self
                .global_publisher_interceptors
                .clone(),
            global_subscriber_interceptors: self
                .global_subscriber_interceptors
                .clone(),
            publisher_interceptors: self.publisher_interceptors.clone(),
            subscriber_interceptors: self.subscriber_interceptors.clone(),
            subscription_handler_pool_size: self.subscription_handler_pool_size,
            subscription_handler_queue_capacity: self
                .subscription_handler_queue_capacity,
        })
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
    fn set_default_publish_options<T>(
        &mut self,
        options: PublishOptions<T>,
    ) -> EventBusResult<()>
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
    fn set_default_dead_letter_strategy<T, F>(
        &mut self,
        strategy: F,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        F: DeadLetterStrategyCallback<T>,
    {
        Self::set_default_dead_letter_strategy::<T, F>(self, strategy);
        Ok(())
    }

    /// Sets the global default dead-letter strategy for local buses.
    fn set_global_default_dead_letter_strategy<F>(
        &mut self,
        strategy: F,
    ) -> EventBusResult<()>
    where
        F: DeadLetterStrategyAnyCallback,
    {
        Self::set_global_default_dead_letter_strategy(self, strategy);
        Ok(())
    }

    /// Adds a typed publisher interceptor for local buses.
    fn add_publisher_interceptor<T, I>(
        &mut self,
        interceptor: I,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: PublisherInterceptor<T>,
    {
        Self::add_publisher_interceptor::<T, I>(self, interceptor)
    }

    /// Adds a global publisher interceptor for local buses.
    fn add_global_publisher_interceptor<I>(
        &mut self,
        interceptor: I,
    ) -> EventBusResult<()>
    where
        I: PublisherInterceptorAny,
    {
        Self::add_global_publisher_interceptor(self, interceptor)
    }

    /// Adds a typed subscriber interceptor for local buses.
    fn add_subscriber_interceptor<T, I>(
        &mut self,
        interceptor: I,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: SubscriberInterceptor<T>,
    {
        Self::add_subscriber_interceptor::<T, I>(self, interceptor)
    }

    /// Adds a global subscriber interceptor for local buses.
    fn add_global_subscriber_interceptor<I>(
        &mut self,
        interceptor: I,
    ) -> EventBusResult<()>
    where
        I: SubscriberInterceptorAny,
    {
        Self::add_global_subscriber_interceptor(self, interceptor)
    }
}
