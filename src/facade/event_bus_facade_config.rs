// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Facade-level configuration consumed when a sync or async facade is built.

use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

use crate::codec::CodecRegistry;
use crate::error::ConfigurationError;
use crate::error::DeliveryError;
use crate::error::PublishError;
use crate::model::AsyncSubscriberInterceptor;
use crate::model::AsyncSubscriberNext;
use crate::model::Delivery;
use crate::model::PublishMetadata;
use crate::model::SubscriberInterceptor;
use crate::model::SubscriberNext;
use crate::pipeline::GlobalPublisherInterceptor;
use crate::spi::SpiFuture;

type ErasedMiddlewareList = Arc<dyn Any + Send + Sync>;

/// Bounds synchronous handler scheduling for one event-bus facade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncDeliverySchedulerConfig {
    max_in_flight: usize,
    handler_queue_capacity: usize,
}

/// Bounds asynchronous deliveries admitted by one facade across all
/// subscriptions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryAdmissionConfig {
    max_in_flight: usize,
}

impl DeliveryAdmissionConfig {
    /// Creates a positive facade-wide in-flight delivery limit.
    pub fn new(max_in_flight: usize) -> Result<Self, ConfigurationError> {
        if max_in_flight == 0 {
            return Err(ConfigurationError::InvalidField {
                field: "max_in_flight",
                message: "must be greater than zero".into(),
            });
        }
        Ok(Self { max_in_flight })
    }

    /// Returns the maximum number of admitted asynchronous deliveries.
    #[must_use]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }
}

impl Default for DeliveryAdmissionConfig {
    fn default() -> Self {
        Self { max_in_flight: 4 }
    }
}

impl SyncDeliverySchedulerConfig {
    /// Creates scheduler limits; `max_in_flight` must be greater than zero.
    /// A zero queue capacity permits direct handoff only to an idle worker.
    pub fn new(max_in_flight: usize, handler_queue_capacity: usize) -> Result<Self, ConfigurationError> {
        if max_in_flight == 0 {
            return Err(ConfigurationError::InvalidField {
                field: "max_in_flight",
                message: "must be greater than zero".into(),
            });
        }
        Ok(Self {
            max_in_flight,
            handler_queue_capacity,
        })
    }

    /// Returns the maximum number of admitted deliveries, including queued
    /// work.
    #[must_use]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Returns the maximum number of admitted tasks waiting for a handler
    /// worker. Zero allows only immediate handoff to an idle eligible
    /// worker.
    #[must_use]
    pub fn handler_queue_capacity(&self) -> usize {
        self.handler_queue_capacity
    }
}

impl Default for SyncDeliverySchedulerConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 4,
            handler_queue_capacity: 32,
        }
    }
}

/// Shared codecs, subscriber middleware, and synchronous scheduler limits
/// installed in a newly-created facade.
///
/// Middleware is registered per payload type. A facade rejects middleware
/// configured for the other execution model when a matching subscription is
/// created, rather than silently skipping or adapting it.
#[derive(Clone)]
pub struct EventBusFacadeConfig {
    /// Immutable codec table shared with the publisher pipeline.
    codecs: Arc<CodecRegistry>,
    /// Type-indexed synchronous subscriber middleware chains.
    sync_subscriber_interceptors: HashMap<TypeId, ErasedMiddlewareList>,
    /// Type-indexed runtime-neutral asynchronous middleware chains.
    async_subscriber_interceptors: HashMap<TypeId, ErasedMiddlewareList>,
    /// Ordered facade-wide publisher metadata interceptors.
    global_publisher_interceptors: Vec<GlobalPublisherInterceptor>,
    /// Shared synchronous handler worker and queue limits.
    sync_delivery_scheduler: SyncDeliverySchedulerConfig,
    delivery_admission: DeliveryAdmissionConfig,
}

impl Default for EventBusFacadeConfig {
    fn default() -> Self {
        Self {
            codecs: Arc::new(CodecRegistry::new()),
            sync_subscriber_interceptors: HashMap::new(),
            async_subscriber_interceptors: HashMap::new(),
            global_publisher_interceptors: Vec::new(),
            sync_delivery_scheduler: SyncDeliverySchedulerConfig::default(),
            delivery_admission: DeliveryAdmissionConfig::default(),
        }
    }
}

impl EventBusFacadeConfig {
    /// Creates facade configuration with an empty codec registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the synchronous scheduler limits for newly-created facades.
    #[must_use]
    pub fn with_sync_delivery_scheduler(mut self, config: SyncDeliverySchedulerConfig) -> Self {
        self.sync_delivery_scheduler = config;
        self
    }

    /// Returns synchronous scheduler limits.
    #[must_use]
    pub fn sync_delivery_scheduler(&self) -> SyncDeliverySchedulerConfig {
        self.sync_delivery_scheduler
    }

    /// Replaces facade-wide asynchronous delivery admission limits.
    #[must_use]
    pub fn with_delivery_admission(mut self, config: DeliveryAdmissionConfig) -> Self {
        self.delivery_admission = config;
        self
    }

    /// Returns facade-wide asynchronous delivery admission limits.
    #[must_use]
    pub fn delivery_admission(&self) -> DeliveryAdmissionConfig {
        self.delivery_admission
    }

    /// Installs an application-prepared shared codec registry.
    #[must_use]
    pub fn with_codec_registry(mut self, codecs: Arc<CodecRegistry>) -> Self {
        self.codecs = codecs;
        self
    }

    /// Returns the codec table that the publisher pipeline will consult.
    #[must_use]
    pub fn codec_registry(&self) -> &Arc<CodecRegistry> {
        &self.codecs
    }

    /// Appends a facade-wide publisher interceptor after request-scoped typed
    /// interceptors. It may edit validated portable headers or stop
    /// publication by returning `Ok(false)`; it cannot alter the payload or
    /// event identity.
    #[must_use]
    pub fn publisher_interceptor<F>(mut self, interceptor: F) -> Self
    where
        F: Fn(&mut PublishMetadata) -> Result<bool, PublishError> + Send + Sync + 'static,
    {
        self.global_publisher_interceptors
            .push(GlobalPublisherInterceptor::new(interceptor));
        self
    }

    pub(crate) fn global_publisher_interceptors(&self) -> &[GlobalPublisherInterceptor] {
        &self.global_publisher_interceptors
    }

    /// Appends facade-wide synchronous subscriber middleware for payload type
    /// `T`.
    ///
    /// The middleware wraps request-specific typed middleware and the handler.
    /// It runs only after the subscription filter accepts a delivery. A sync
    /// [`crate::facade::EventBus`] rejects configuration containing async
    /// middleware for the same payload type.
    #[must_use]
    pub fn subscriber_interceptor<T, F>(mut self, middleware: F) -> Self
    where
        T: 'static,
        F: Fn(Delivery<T>, SubscriberNext<T>) -> Result<(), DeliveryError> + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        let mut middleware_list = self
            .sync_subscriber_interceptors
            .get(&type_id)
            .and_then(|list| list.downcast_ref::<Vec<Arc<SubscriberInterceptor<T>>>>())
            .cloned()
            .unwrap_or_default();
        middleware_list.push(Arc::new(middleware));
        self.sync_subscriber_interceptors
            .insert(type_id, Arc::new(middleware_list));
        self
    }

    /// Appends facade-wide runtime-neutral async subscriber middleware for `T`.
    ///
    /// The middleware wraps request-specific typed middleware and the handler,
    /// and runs only after filtering accepts a delivery. An async facade
    /// rejects sync middleware configured for the same payload type.
    #[must_use]
    pub fn async_subscriber_interceptor<T, F>(mut self, middleware: F) -> Self
    where
        T: 'static,
        F: Fn(Delivery<T>, AsyncSubscriberNext<T>) -> SpiFuture<'static, Result<(), DeliveryError>>
            + Send
            + Sync
            + 'static,
    {
        let type_id = TypeId::of::<T>();
        let mut middleware_list = self
            .async_subscriber_interceptors
            .get(&type_id)
            .and_then(|list| list.downcast_ref::<Vec<Arc<AsyncSubscriberInterceptor<T>>>>())
            .cloned()
            .unwrap_or_default();
        middleware_list.push(Arc::new(middleware));
        self.async_subscriber_interceptors
            .insert(type_id, Arc::new(middleware_list));
        self
    }

    pub(crate) fn subscriber_interceptors<T: 'static>(&self) -> Vec<Arc<SubscriberInterceptor<T>>> {
        self.sync_subscriber_interceptors
            .get(&TypeId::of::<T>())
            .and_then(|list| list.downcast_ref::<Vec<Arc<SubscriberInterceptor<T>>>>())
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn async_subscriber_interceptors<T: 'static>(&self) -> Vec<Arc<AsyncSubscriberInterceptor<T>>> {
        self.async_subscriber_interceptors
            .get(&TypeId::of::<T>())
            .and_then(|list| list.downcast_ref::<Vec<Arc<AsyncSubscriberInterceptor<T>>>>())
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn has_sync_subscriber_interceptors<T: 'static>(&self) -> bool {
        self.sync_subscriber_interceptors.contains_key(&TypeId::of::<T>())
    }

    pub(crate) fn has_async_subscriber_interceptors<T: 'static>(&self) -> bool {
        self.async_subscriber_interceptors.contains_key(&TypeId::of::<T>())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::EventBusFacadeConfig;
    use crate::codec::CodecRegistry;

    #[test]
    fn test_clone_shares_the_configured_codec_registry() {
        let codecs = Arc::new(CodecRegistry::new());
        let config = EventBusFacadeConfig::new().with_codec_registry(Arc::clone(&codecs));
        let cloned = config.clone();

        assert!(Arc::ptr_eq(config.codec_registry(), cloned.codec_registry()));
        assert!(Arc::ptr_eq(config.codec_registry(), &codecs));
    }
}
