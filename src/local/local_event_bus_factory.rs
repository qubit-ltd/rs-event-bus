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

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    EventBusError, EventBusFactory, EventBusResult, LocalEventBus, SubscribeOptions,
    UnsupportedTransactionalEventBus,
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
    default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
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
            default_subscribe_options: HashMap::new(),
            subscription_handler_pool_size: default_subscription_handler_pool_size(),
            subscription_handler_queue_capacity: None,
        }
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
            self.default_subscribe_options.clone(),
            self.subscription_handler_pool_size,
            self.subscription_handler_queue_capacity,
        )
    }

    /// Creates and starts an event bus.
    ///
    /// # Returns
    /// Started local event bus initialized with factory defaults.
    pub fn create_started(&self) -> LocalEventBus {
        let bus = self.create();
        bus.start();
        bus
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
    fn create_started(&self) -> Self::Bus {
        Self::create_started(self)
    }
}
