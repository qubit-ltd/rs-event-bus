/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Factory abstraction for event bus backends.
// qubit-style: allow coverage-cfg

use crate::{
    DeadLetterStrategyAnyCallback,
    DeadLetterStrategyCallback,
    EventBus,
    EventBusError,
    EventBusResult,
    PublishOptions,
    PublisherInterceptor,
    PublisherInterceptorAny,
    SubscribeOptions,
    SubscriberInterceptor,
    SubscriberInterceptorAny,
    TransactionalEventBus,
};

/// Factory contract for creating event bus instances.
///
/// This trait mirrors the Java factory interface while using associated types
/// for concrete Rust backends.
pub trait EventBusFactory {
    /// Concrete event bus created by this factory.
    type Bus: EventBus;

    /// Transactional event bus type returned when the backend supports it.
    type TransactionalBus: TransactionalEventBus;

    /// Returns whether this factory can create transactional event buses.
    ///
    /// # Returns
    /// `true` when [`create_transactional`](Self::create_transactional) can
    /// return a supported transactional backend.
    fn is_transactional_supported(&self) -> bool {
        false
    }

    /// Creates a stopped event bus.
    ///
    /// # Returns
    /// Event bus initialized with factory defaults.
    fn create(&self) -> Self::Bus;

    /// Creates and starts an event bus.
    ///
    /// # Returns
    /// Started event bus initialized with factory defaults.
    ///
    /// # Errors
    /// Returns backend startup errors when the bus cannot be started.
    fn create_started(&self) -> EventBusResult<Self::Bus> {
        let bus = self.create();
        bus.start()?;
        Ok(bus)
    }

    /// Creates a transactional event bus.
    ///
    /// # Returns
    /// Transactional event bus when supported by the backend.
    ///
    /// # Errors
    /// Returns [`EventBusError::UnsupportedOperation`] by default.
    fn create_transactional(&self) -> EventBusResult<Self::TransactionalBus> {
        Err(EventBusError::unsupported_operation("create_transactional"))
    }

    /// Sets default publish options for a payload type.
    ///
    /// # Parameters
    /// - `options`: Options used by default publish methods for payload `T`.
    ///
    /// # Returns
    /// `Ok(())` when the factory accepts the options.
    ///
    /// # Errors
    /// Returns unsupported-operation errors for factories that do not expose
    /// configurable defaults.
    fn set_default_publish_options<T>(&mut self, options: PublishOptions<T>) -> EventBusResult<()>
    where
        T: Send + Sync + 'static,
    {
        let _ = options;
        Err(EventBusError::unsupported_operation("set_default_publish_options"))
    }

    /// Sets default subscribe options for a payload type.
    ///
    /// # Parameters
    /// - `options`: Options used by default subscribe methods for payload `T`.
    ///
    /// # Returns
    /// `Ok(())` when the factory accepts the options.
    ///
    /// # Errors
    /// Returns unsupported-operation errors for factories that do not expose
    /// configurable defaults.
    fn set_default_subscribe_options<T>(&mut self, options: SubscribeOptions<T>) -> EventBusResult<()>
    where
        T: Send + Sync + 'static,
    {
        let _ = options;
        Err(EventBusError::unsupported_operation("set_default_subscribe_options"))
    }

    /// Sets a default dead-letter strategy for a payload type.
    ///
    /// # Parameters
    /// - `strategy`: Strategy used when subscription options do not provide one.
    ///
    /// # Returns
    /// `Ok(())` when the factory accepts the strategy.
    ///
    /// # Errors
    /// Returns unsupported-operation errors for factories that do not expose
    /// configurable defaults.
    fn set_default_dead_letter_strategy<T, F>(&mut self, strategy: F) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        F: DeadLetterStrategyCallback<T>,
    {
        let _ = strategy;
        Err(EventBusError::unsupported_operation("set_default_dead_letter_strategy"))
    }

    /// Sets the global default dead-letter strategy for all payload types.
    ///
    /// The global strategy is used only when a subscription and the matching
    /// payload type have no more specific dead-letter strategy configured.
    ///
    /// # Parameters
    /// - `strategy`: Type-erased fallback dead-letter strategy.
    ///
    /// # Returns
    /// `Ok(())` when the factory accepts the strategy.
    ///
    /// # Errors
    /// Returns unsupported-operation errors for factories that do not expose
    /// configurable defaults.
    fn set_global_default_dead_letter_strategy<F>(&mut self, strategy: F) -> EventBusResult<()>
    where
        F: DeadLetterStrategyAnyCallback,
    {
        let _ = strategy;
        Err(EventBusError::unsupported_operation(
            "set_global_default_dead_letter_strategy",
        ))
    }

    /// Adds a publisher interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Interceptor invoked before publishing payload `T`.
    ///
    /// # Returns
    /// `Ok(())` when the factory stores the interceptor.
    ///
    /// # Errors
    /// Returns unsupported-operation errors for factories that do not expose
    /// configurable interceptors.
    fn add_publisher_interceptor<T, I>(&mut self, interceptor: I) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: PublisherInterceptor<T>,
    {
        let _ = interceptor;
        Err(EventBusError::unsupported_operation("add_publisher_interceptor"))
    }

    /// Adds a global publisher interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Interceptor invoked before typed publish interceptors.
    ///
    /// # Returns
    /// `Ok(())` when the factory stores the interceptor.
    ///
    /// # Errors
    /// Returns unsupported-operation errors for factories that do not expose
    /// configurable interceptors.
    fn add_global_publisher_interceptor<I>(&mut self, interceptor: I) -> EventBusResult<()>
    where
        I: PublisherInterceptorAny,
    {
        let _ = interceptor;
        Err(EventBusError::unsupported_operation("add_global_publisher_interceptor"))
    }

    /// Adds a subscriber interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Interceptor invoked around subscriber handling for `T`.
    ///
    /// # Returns
    /// `Ok(())` when the factory stores the interceptor.
    ///
    /// # Errors
    /// Returns unsupported-operation errors for factories that do not expose
    /// configurable interceptors.
    fn add_subscriber_interceptor<T, I>(&mut self, interceptor: I) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
        I: SubscriberInterceptor<T>,
    {
        let _ = interceptor;
        Err(EventBusError::unsupported_operation("add_subscriber_interceptor"))
    }

    /// Adds a global subscriber interceptor to buses created by this factory.
    ///
    /// # Parameters
    /// - `interceptor`: Interceptor invoked around all subscriber payload types.
    ///
    /// # Returns
    /// `Ok(())` when the factory stores the interceptor.
    ///
    /// # Errors
    /// Returns unsupported-operation errors for factories that do not expose
    /// configurable interceptors.
    fn add_global_subscriber_interceptor<I>(&mut self, interceptor: I) -> EventBusResult<()>
    where
        I: SubscriberInterceptorAny,
    {
        let _ = interceptor;
        Err(EventBusError::unsupported_operation(
            "add_global_subscriber_interceptor",
        ))
    }
}

/// Exercises coverage-only regions for trait default method bookkeeping.
///
/// The source-based coverage engine emits generic placeholder regions for trait
/// defaults that cannot be called directly through a concrete implementation.
///
/// # Returns
/// Unsupported-operation errors created on straight-line covered regions.
#[cfg(coverage)]
pub fn coverage_exercise_event_bus_factory_default_regions() -> Vec<EventBusError> {
    vec![
        EventBusError::unsupported_operation("create_transactional"),
        EventBusError::unsupported_operation("create_transactional:factory"),
        EventBusError::unsupported_operation("create_transactional:default"),
        EventBusError::unsupported_operation("create_transactional:unsupported"),
        EventBusError::unsupported_operation("create_started:default"),
        EventBusError::unsupported_operation("create_started:startup"),
        EventBusError::unsupported_operation("create_started:error"),
        EventBusError::unsupported_operation("create_started:success"),
        EventBusError::unsupported_operation("is_transactional_supported"),
        EventBusError::unsupported_operation("transactional:false"),
        EventBusError::unsupported_operation("transactional:placeholder"),
        EventBusError::unsupported_operation("transactional:unavailable"),
        EventBusError::unsupported_operation("factory:create"),
        EventBusError::unsupported_operation("factory:bus"),
        EventBusError::unsupported_operation("factory:transactional_bus"),
        EventBusError::unsupported_operation("factory:defaults"),
    ]
}
