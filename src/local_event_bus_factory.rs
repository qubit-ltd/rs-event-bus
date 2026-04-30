/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Factory for local event bus instances.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{LocalEventBus, SubscribeOptions};

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
