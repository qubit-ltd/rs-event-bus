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
use std::sync::Arc;

use super::DeliverySchedulingConfig;
use super::PayloadLimits;
use super::SettlementRetryConfig;
use super::internal::ErasedMiddlewareList;
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

/// Shared codecs, subscriber middleware, and synchronous scheduler limits
/// installed in a newly-created facade.
///
/// Middleware is registered per payload type. A facade rejects middleware
/// configured for the other execution model when the facade is constructed.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::EventBusFacadeConfig;
///
/// let config = EventBusFacadeConfig::new();
/// assert_eq!(config.payload_limits().max_publish_bytes().get(), 1_048_576);
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
    /// Independent positive byte limits for encoded publication and receiving.
    payload_limits: PayloadLimits,
    /// Unified handler, delivery ownership and subscription limits.
    delivery_scheduling: DeliverySchedulingConfig,
    /// Finite provider settlement retry policy.
    settlement_retry: SettlementRetryConfig,
}

impl Default for EventBusFacadeConfig {
    /// Creates an empty configuration with the standard facade limits.
    ///
    /// # Returns
    /// An empty configuration with the default codec, scheduling, and retry
    /// policies.
    fn default() -> Self {
        Self {
            codecs: Arc::new(CodecRegistry::new()),
            sync_subscriber_interceptors: HashMap::new(),
            async_subscriber_interceptors: HashMap::new(),
            global_publisher_interceptors: Vec::new(),
            payload_limits: PayloadLimits::default(),
            delivery_scheduling: DeliverySchedulingConfig::default(),
            settlement_retry: SettlementRetryConfig::default(),
        }
    }
}

impl EventBusFacadeConfig {
    /// Validates that no asynchronous subscriber middleware is installed for a
    /// synchronous facade.
    ///
    /// # Errors
    /// Returns the invalid middleware field when any payload type has an
    /// asynchronous chain.
    pub(crate) fn validate_for_sync(&self) -> Result<(), ConfigurationError> {
        if self.async_subscriber_interceptors.is_empty() {
            Ok(())
        } else {
            Err(ConfigurationError::InvalidField {
                field: "async_subscriber_interceptor",
                message: "asynchronous subscriber middleware requires an async facade".into(),
            })
        }
    }

    /// Validates that no synchronous subscriber middleware is installed for an
    /// asynchronous facade.
    ///
    /// # Errors
    /// Returns the invalid middleware field when any payload type has a
    /// synchronous chain.
    pub(crate) fn validate_for_async(&self) -> Result<(), ConfigurationError> {
        if self.sync_subscriber_interceptors.is_empty() {
            Ok(())
        } else {
            Err(ConfigurationError::InvalidField {
                field: "sync_subscriber_interceptor",
                message: "synchronous subscriber middleware requires a sync facade".into(),
            })
        }
    }

    /// Creates facade configuration with an empty codec registry.
    ///
    /// # Returns
    /// A configuration with no middleware, default delivery limits, and finite
    /// encoded-payload limits.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a copy of the unified delivery scheduling limits.
    ///
    /// # Returns
    /// The scheduling limits used by newly-created facades.
    #[must_use]
    #[inline]
    pub const fn delivery_scheduling(&self) -> DeliverySchedulingConfig {
        self.delivery_scheduling
    }

    /// Returns a copy of the finite settlement retry policy.
    ///
    /// # Returns
    /// The settlement retry policy used by newly-created facades.
    #[must_use]
    #[inline]
    pub const fn settlement_retry(&self) -> SettlementRetryConfig {
        self.settlement_retry
    }

    /// Returns independent positive publishing and receiving byte limits.
    ///
    /// # Returns
    /// The configured encoded publish and receive byte limits.
    #[inline]
    pub const fn payload_limits(&self) -> PayloadLimits {
        self.payload_limits
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

    /// Replaces the unified delivery scheduling limits for newly-created
    /// facades.
    ///
    /// # Parameters
    /// - `config`: the validated scheduling limits to install.
    ///
    /// # Returns
    /// This configuration with the supplied policy.
    #[must_use]
    #[inline]
    pub fn with_delivery_scheduling(mut self, config: DeliverySchedulingConfig) -> Self {
        self.delivery_scheduling = config;
        self
    }

    /// Replaces the finite settlement retry policy for newly-created facades.
    ///
    /// # Parameters
    /// - `config`: the validated settlement retry policy to install.
    ///
    /// # Returns
    /// This configuration with the supplied policy.
    #[must_use]
    #[inline]
    pub fn with_settlement_retry(mut self, config: SettlementRetryConfig) -> Self {
        self.settlement_retry = config;
        self
    }

    /// Replaces both positive encoded byte limits for newly-created facades.
    /// Exactly the supplied limit is accepted in each direction.
    ///
    /// # Parameters
    /// - `limits`: the positive encoded publish and receive byte limits.
    ///
    /// # Returns
    /// This configuration using the supplied payload limits.
    #[must_use]
    #[inline]
    pub fn with_payload_limits(mut self, limits: PayloadLimits) -> Self {
        self.payload_limits = limits;
        self
    }

    /// Installs an application-prepared shared codec registry.
    ///
    /// # Parameters
    /// - `codecs`: codec registry shared with the publisher pipeline.
    ///
    /// # Returns
    /// This configuration using the supplied shared registry.
    #[must_use]
    #[inline]
    pub fn with_codec_registry(mut self, codecs: Arc<CodecRegistry>) -> Self {
        self.codecs = codecs;
        self
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
    #[inline]
    pub(crate) fn global_publisher_interceptors(&self) -> &[GlobalPublisherInterceptor] {
        &self.global_publisher_interceptors
    }

    /// Appends facade-wide synchronous subscriber middleware for payload type
    /// `T`.
    ///
    /// The middleware wraps request-specific typed middleware and the handler.
    /// It runs only after the subscription filter accepts a delivery.
    /// [`crate::facade::AsyncEventBus`] rejects this configuration at
    /// construction.
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
    /// and runs only after filtering accepts a delivery.
    /// [`crate::facade::EventBus`] rejects this configuration at construction.
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
