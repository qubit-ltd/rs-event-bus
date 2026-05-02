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

use crate::{EventBusFactory, LocalEventBus, SubscribeOptions, UnsupportedTransactionalEventBus};

/// Factory used to create [`LocalEventBus`] instances with default options.
#[derive(Default)]
pub struct LocalEventBusFactory {
    default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl LocalEventBusFactory {
    /// Creates an empty local event bus factory.
    ///
    /// # Returns
    /// Factory with no typed defaults.
    pub fn new() -> Self {
        Self {
            default_subscribe_options: HashMap::new(),
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

    /// Creates a stopped event bus.
    ///
    /// # Returns
    /// Local event bus initialized with factory defaults.
    pub fn create(&self) -> LocalEventBus {
        LocalEventBus::with_default_subscribe_options(self.default_subscribe_options.clone())
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
