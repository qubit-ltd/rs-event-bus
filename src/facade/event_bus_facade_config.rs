// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Facade-level configuration consumed when a sync or async facade is built.

use std::any::TypeId;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::DeliveryAdmissionConfig;
use super::SyncDeliverySchedulerConfig;
use super::internal::ErasedMiddlewareList;
use crate::codec::CodecRegistry;
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

/// Shared codecs, subscriber middleware, and synchronous scheduler limits
/// installed in a newly-created facade.
///
/// Middleware is registered per payload type. A facade rejects middleware
/// configured for the other execution model when a matching subscription is
/// created, rather than silently skipping or adapting it.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::EventBusFacadeConfig;
///
/// let config = EventBusFacadeConfig::new();
/// assert_eq!(config.max_encoded_payload_bytes(), None);
/// ```
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
    /// Shared facade-wide limit for asynchronous deliveries.
    delivery_admission: DeliveryAdmissionConfig,
    /// Optional encoded payload limit applied before provider publication.
    max_encoded_payload_bytes: Option<NonZeroUsize>,
}

impl Default for EventBusFacadeConfig {
    /// Creates an empty configuration with the standard facade limits.
    fn default() -> Self {
        Self {
            codecs: Arc::new(CodecRegistry::new()),
            sync_subscriber_interceptors: HashMap::new(),
            async_subscriber_interceptors: HashMap::new(),
            global_publisher_interceptors: Vec::new(),
            sync_delivery_scheduler: SyncDeliverySchedulerConfig::default(),
            delivery_admission: DeliveryAdmissionConfig::default(),
            max_encoded_payload_bytes: None,
        }
    }
}

impl EventBusFacadeConfig {
    /// Creates facade configuration with an empty codec registry.
    ///
    /// # Returns
    /// A configuration with no middleware, default delivery limits, and no
    /// encoded-payload size limit.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the synchronous scheduler limits for newly-created facades.
    ///
    /// # Parameters
    /// - `config`: scheduler capacity and worker limits to install.
    ///
    /// # Returns
    /// This configuration with the supplied synchronous scheduler policy.
    #[must_use]
    pub fn with_sync_delivery_scheduler(mut self, config: SyncDeliverySchedulerConfig) -> Self {
        self.sync_delivery_scheduler = config;
        self
    }

    /// Returns synchronous scheduler limits.
    ///
    /// # Returns
    /// A copy of the scheduler policy used by newly-created facades.
    #[must_use]
    #[inline]
    pub fn sync_delivery_scheduler(&self) -> SyncDeliverySchedulerConfig {
        self.sync_delivery_scheduler
    }

    /// Replaces facade-wide asynchronous delivery admission limits.
    ///
    /// # Parameters
    /// - `config`: the maximum number of admitted asynchronous deliveries.
    ///
    /// # Returns
    /// This configuration with the supplied admission policy.
    #[must_use]
    pub fn with_delivery_admission(mut self, config: DeliveryAdmissionConfig) -> Self {
        self.delivery_admission = config;
        self
    }

    /// Returns facade-wide asynchronous delivery admission limits.
    ///
    /// # Returns
    /// A copy of the shared asynchronous admission policy.
    #[must_use]
    #[inline]
    pub fn delivery_admission(&self) -> DeliveryAdmissionConfig {
        self.delivery_admission
    }

    /// Sets the maximum encoded payload size; `None` disables the limit.
    ///
    /// # Parameters
    /// - `limit`: the maximum number of bytes accepted from an encoder, or
    ///   `None` to disable the byte limit.
    ///
    /// # Returns
    /// This configuration with the requested encoded-payload limit.
    #[must_use]
    pub fn with_max_encoded_payload_bytes(mut self, limit: Option<NonZeroUsize>) -> Self {
        self.max_encoded_payload_bytes = limit;
        self
    }

    /// Returns the optional maximum encoded payload size.
    ///
    /// # Returns
    /// The configured nonzero byte limit, or `None` when encoded output is
    /// unbounded by this facade.
    #[must_use]
    #[inline]
    pub const fn max_encoded_payload_bytes(&self) -> Option<NonZeroUsize> {
        self.max_encoded_payload_bytes
    }

    /// Installs an application-prepared shared codec registry.
    ///
    /// # Parameters
    /// - `codecs`: codec registry shared with the publisher pipeline.
    ///
    /// # Returns
    /// This configuration using the supplied shared registry.
    #[must_use]
    pub fn with_codec_registry(mut self, codecs: Arc<CodecRegistry>) -> Self {
        self.codecs = codecs;
        self
    }

    /// Returns the codec table that the publisher pipeline will consult.
    ///
    /// # Returns
    /// A reference to the shared codec registry handle.
    #[must_use]
    #[inline]
    pub fn codec_registry(&self) -> &Arc<CodecRegistry> {
        &self.codecs
    }

    /// Appends a facade-wide publisher interceptor after request-scoped typed
    /// interceptors. It may edit validated portable headers or stop
    /// publication by returning `Ok(false)`; it cannot alter the payload or
    /// event identity.
    ///
    /// # Type Parameters
    /// - `F`: a thread-safe, owned callback that edits publication metadata.
    ///
    /// # Parameters
    /// - `interceptor`: callback appended after request-specific interceptors.
    ///
    /// # Returns
    /// This configuration with the interceptor appended to its ordered list.
    #[must_use]
    pub fn publisher_interceptor<F>(mut self, interceptor: F) -> Self
    where
        F: Fn(&mut PublishMetadata) -> Result<bool, PublishError> + Send + Sync + 'static,
    {
        self.global_publisher_interceptors
            .push(GlobalPublisherInterceptor::new(interceptor));
        self
    }

    /// Returns facade-wide publisher interceptors in registration order.
    ///
    /// # Returns
    /// The immutable interceptor slice applied after request-scoped hooks.
    #[must_use]
    #[inline]
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
    ///
    /// # Type Parameters
    /// - `T`: the payload type accepted by this middleware.
    /// - `F`: the thread-safe middleware callback type.
    ///
    /// # Parameters
    /// - `middleware`: callback wrapping the handler and earlier middleware.
    ///
    /// # Returns
    /// This configuration with the middleware appended for `T`.
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
    ///
    /// # Type Parameters
    /// - `T`: the payload type accepted by this middleware.
    /// - `F`: the thread-safe callback returning a runtime-neutral future.
    ///
    /// # Parameters
    /// - `middleware`: callback wrapping the handler and earlier middleware.
    ///
    /// # Returns
    /// This configuration with the middleware appended for `T`.
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

    /// Clones the registered synchronous middleware chain for payload type `T`.
    ///
    /// # Type Parameters
    /// - `T`: payload type whose middleware chain is requested.
    ///
    /// # Returns
    /// The registered callbacks in append order, or an empty vector.
    #[must_use = "Use the returned synchronous middleware chain."]
    pub(crate) fn subscriber_interceptors<T: 'static>(&self) -> Vec<Arc<SubscriberInterceptor<T>>> {
        self.sync_subscriber_interceptors
            .get(&TypeId::of::<T>())
            .and_then(|list| list.downcast_ref::<Vec<Arc<SubscriberInterceptor<T>>>>())
            .cloned()
            .unwrap_or_default()
    }

    /// Clones the registered asynchronous middleware chain for payload type
    /// `T`.
    ///
    /// # Type Parameters
    /// - `T`: payload type whose middleware chain is requested.
    ///
    /// # Returns
    /// The registered callbacks in append order, or an empty vector.
    #[must_use = "Use the returned asynchronous middleware chain."]
    pub(crate) fn async_subscriber_interceptors<T: 'static>(&self) -> Vec<Arc<AsyncSubscriberInterceptor<T>>> {
        self.async_subscriber_interceptors
            .get(&TypeId::of::<T>())
            .and_then(|list| list.downcast_ref::<Vec<Arc<AsyncSubscriberInterceptor<T>>>>())
            .cloned()
            .unwrap_or_default()
    }

    /// Checks whether any synchronous middleware was registered for `T`.
    ///
    /// # Type Parameters
    /// - `T`: payload type to query.
    ///
    /// # Returns
    /// `true` when this configuration contains a synchronous middleware chain.
    #[must_use]
    #[inline]
    pub(crate) fn has_sync_subscriber_interceptors<T: 'static>(&self) -> bool {
        self.sync_subscriber_interceptors.contains_key(&TypeId::of::<T>())
    }

    /// Checks whether any asynchronous middleware was registered for `T`.
    ///
    /// # Type Parameters
    /// - `T`: payload type to query.
    ///
    /// # Returns
    /// `true` when this configuration contains an asynchronous middleware
    /// chain.
    #[must_use]
    #[inline]
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
