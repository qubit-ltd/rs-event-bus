/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Shared state for the local event bus.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use qubit_thread_pool::{ExecutorService, FixedThreadPool, ThreadPoolBuildError};

use crate::{EventBusError, EventBusResult, SubscribeOptions, TopicKey};

use super::erased_subscription::ErasedSubscription;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;

/// Shared mutable state for [`crate::LocalEventBus`].
pub(crate) struct LocalEventBusInner {
    lifecycle: Mutex<LocalEventBusLifecycle>,
    subscriptions: Mutex<HashMap<TopicKey, Vec<Arc<dyn ErasedSubscription>>>>,
    publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorEntry>>>,
    subscriber_interceptors: Mutex<Vec<Arc<dyn Any + Send + Sync>>>,
    processing_tracker: ProcessingTracker,
    next_subscription_id: AtomicUsize,
    default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    subscription_handler_pool_size: usize,
    subscription_handler_queue_capacity: Option<usize>,
}

impl LocalEventBusInner {
    /// Creates shared local event bus state.
    ///
    /// # Parameters
    /// - `default_subscribe_options`: Typed default subscription options.
    /// - `subscription_handler_pool_size`: Worker count for subscriber handlers.
    /// - `subscription_handler_queue_capacity`: Optional queued handler limit.
    ///
    /// # Returns
    /// Shared state initialized in the stopped lifecycle state.
    pub(crate) fn new(
        default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
        subscription_handler_pool_size: usize,
        subscription_handler_queue_capacity: Option<usize>,
    ) -> Self {
        Self {
            lifecycle: Mutex::new(LocalEventBusLifecycle::stopped()),
            subscriptions: Mutex::new(HashMap::new()),
            publisher_interceptors: Mutex::new(Vec::new()),
            subscriber_interceptors: Mutex::new(Vec::new()),
            processing_tracker: ProcessingTracker::new(),
            next_subscription_id: AtomicUsize::new(1),
            default_subscribe_options,
            subscription_handler_pool_size,
            subscription_handler_queue_capacity,
        }
    }

    /// Marks the bus as started.
    ///
    /// # Returns
    /// `true` when this call changed state from stopped to started.
    pub(crate) fn mark_started(&self) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if lifecycle.started {
            return false;
        }
        let Ok(executor) = self.build_subscription_handler_executor() else {
            return false;
        };
        lifecycle.executor = Some(executor);
        lifecycle.started = true;
        true
    }

    /// Marks the bus as stopped and removes its handler executor.
    ///
    /// # Returns
    /// Handler executor when this call changed state from started to stopped.
    pub(crate) fn mark_stopped(&self) -> Option<FixedThreadPool> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        if !lifecycle.started {
            return None;
        }
        lifecycle.started = false;
        lifecycle.executor.take()
    }

    /// Returns whether the bus is currently started.
    ///
    /// # Returns
    /// `true` if publishing and subscribing are allowed.
    pub(crate) fn is_started(&self) -> bool {
        self.lifecycle
            .lock()
            .map(|lifecycle| lifecycle.started)
            .unwrap_or(false)
    }

    /// Allocates a new subscription ID.
    ///
    /// # Returns
    /// Process-local subscription ID.
    pub(crate) fn next_subscription_id(&self) -> usize {
        self.next_subscription_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns typed default subscribe options.
    ///
    /// # Returns
    /// Type-specific default options if configured.
    pub(crate) fn default_subscribe_options<T>(&self) -> Option<SubscribeOptions<T>>
    where
        T: 'static,
    {
        self.default_subscribe_options
            .get(&TypeId::of::<T>())
            .and_then(|options| options.downcast_ref::<SubscribeOptions<T>>())
            .cloned()
    }

    /// Adds a publisher interceptor.
    ///
    /// # Parameters
    /// - `interceptor`: Type-erased interceptor entry.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    pub(crate) fn add_publisher_interceptor(
        &self,
        interceptor: Arc<dyn PublisherInterceptorEntry>,
    ) -> EventBusResult<()> {
        self.publisher_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("publisher_interceptors"))?
            .push(interceptor);
        Ok(())
    }

    /// Returns registered publisher interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn publisher_interceptors(
        &self,
    ) -> EventBusResult<Vec<Arc<dyn PublisherInterceptorEntry>>> {
        Ok(self
            .publisher_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("publisher_interceptors"))?
            .clone())
    }

    /// Adds a type-erased subscriber interceptor entry.
    ///
    /// # Parameters
    /// - `interceptor`: Shared typed interceptor adapter.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    pub(crate) fn add_subscriber_interceptor(
        &self,
        interceptor: Arc<dyn Any + Send + Sync>,
    ) -> EventBusResult<()> {
        self.subscriber_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriber_interceptors"))?
            .push(interceptor);
        Ok(())
    }

    /// Returns registered subscriber interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn subscriber_interceptors(
        &self,
    ) -> EventBusResult<Vec<Arc<dyn Any + Send + Sync>>> {
        Ok(self
            .subscriber_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriber_interceptors"))?
            .clone())
    }

    /// Adds a subscription entry.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key.
    /// - `subscription`: Type-erased subscription entry.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    pub(crate) fn add_subscription(
        &self,
        topic_key: TopicKey,
        subscription: Arc<dyn ErasedSubscription>,
    ) -> EventBusResult<()> {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?;
        let entries = subscriptions.entry(topic_key).or_default();
        entries.push(subscription);
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.priority()));
        Ok(())
    }

    /// Returns subscriptions for a topic key.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to look up.
    ///
    /// # Returns
    /// A cloned list of subscription entries.
    pub(crate) fn subscriptions_for(
        &self,
        topic_key: &TopicKey,
    ) -> EventBusResult<Vec<Arc<dyn ErasedSubscription>>> {
        Ok(self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?
            .get(topic_key)
            .cloned()
            .unwrap_or_default())
    }

    /// Removes a subscription entry.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key containing the subscription.
    /// - `id`: Subscription ID.
    ///
    /// # Returns
    /// `Ok(())` after removal.
    pub(crate) fn unsubscribe(&self, topic_key: &TopicKey, id: usize) -> EventBusResult<()> {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?;
        if let Some(entries) = subscriptions.get_mut(topic_key) {
            entries.retain(|entry| entry.id() != id);
            if entries.is_empty() {
                subscriptions.remove(topic_key);
            }
        }
        Ok(())
    }

    /// Clears all subscriptions.
    pub(crate) fn clear_subscriptions(&self) {
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            subscriptions.clear();
        }
    }

    /// Increments active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic receiving new handler work.
    ///
    /// # Returns
    /// `Ok(())` after incrementing the count.
    pub(crate) fn start_processing(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        self.processing_tracker.start(topic_key)
    }

    /// Decrements active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic whose handler work finished.
    pub(crate) fn finish_processing(&self, topic_key: &TopicKey) {
        self.processing_tracker.finish(topic_key);
    }

    /// Waits until a topic has zero active work.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to wait for.
    ///
    /// # Returns
    /// `Ok(())` once the topic is idle.
    pub(crate) fn wait_for_idle(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        self.processing_tracker.wait_for_idle(topic_key)
    }

    /// Waits until all topics have zero active work.
    ///
    /// # Returns
    /// `Ok(())` once all tracked topics are idle.
    pub(crate) fn wait_for_all_idle(&self) -> EventBusResult<()> {
        self.processing_tracker.wait_for_all_idle()
    }

    /// Submits subscriber processing work to the handler pool.
    ///
    /// # Parameters
    /// - `task`: One-shot task that owns one subscriber delivery.
    ///
    /// # Returns
    /// `Ok(())` when the pool accepts the task.
    ///
    /// # Errors
    /// Returns lock-poisoning or executor rejection errors before the task runs.
    pub(crate) fn submit_processing_task<F>(&self, task: F) -> EventBusResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        let executor = lifecycle
            .executor
            .as_ref()
            .ok_or_else(EventBusError::not_started)?;
        let mut task = Some(task);
        executor
            .submit_callable(move || {
                let task = task.take().ok_or_else(|| {
                    EventBusError::handler_failed("subscription task was invoked more than once")
                })?;
                task();
                Ok::<(), EventBusError>(())
            })
            .map(|_handle| ())
            .map_err(|error| EventBusError::execution_rejected(error.to_string()))
    }

    /// Builds the subscription handler executor.
    ///
    /// # Returns
    /// A fixed thread pool configured for subscriber processing.
    ///
    /// # Errors
    /// Returns executor build errors from `rs-thread-pool`.
    fn build_subscription_handler_executor(&self) -> Result<FixedThreadPool, ThreadPoolBuildError> {
        let mut builder = FixedThreadPool::builder()
            .pool_size(self.subscription_handler_pool_size)
            .thread_name_prefix("qubit-event-bus-subscriber");
        if let Some(capacity) = self.subscription_handler_queue_capacity {
            builder = builder.queue_capacity(capacity);
        }
        builder.build()
    }
}

/// Lifecycle state protected by the local event bus lifecycle lock.
struct LocalEventBusLifecycle {
    started: bool,
    executor: Option<FixedThreadPool>,
}

impl LocalEventBusLifecycle {
    /// Creates a stopped lifecycle without a handler executor.
    ///
    /// # Returns
    /// Stopped lifecycle state.
    fn stopped() -> Self {
        Self {
            started: false,
            executor: None,
        }
    }
}

/// Tracks active handler work per topic.
struct ProcessingTracker {
    counts: Mutex<HashMap<TopicKey, usize>>,
    condvar: Condvar,
}

impl ProcessingTracker {
    /// Creates an empty processing tracker.
    ///
    /// # Returns
    /// Tracker with zero active work.
    fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
            condvar: Condvar::new(),
        }
    }

    /// Increments active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic receiving new handler work.
    ///
    /// # Returns
    /// `Ok(())` after incrementing the count.
    fn start(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        *counts.entry(topic_key.clone()).or_insert(0) += 1;
        Ok(())
    }

    /// Decrements active work for a topic.
    ///
    /// # Parameters
    /// - `topic_key`: Topic whose handler work finished.
    fn finish(&self, topic_key: &TopicKey) {
        if let Ok(mut counts) = self.counts.lock() {
            if let Some(count) = counts.get_mut(topic_key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(topic_key);
                }
            }
            self.condvar.notify_all();
        }
    }

    /// Waits until a topic has zero active work.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to wait for.
    ///
    /// # Returns
    /// `Ok(())` once the topic is idle.
    fn wait_for_idle(&self, topic_key: &TopicKey) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while counts.get(topic_key).copied().unwrap_or(0) > 0 {
            counts = self
                .condvar
                .wait(counts)
                .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        }
        Ok(())
    }

    /// Waits until all topics have zero active work.
    ///
    /// # Returns
    /// `Ok(())` once all tracked topics are idle.
    fn wait_for_all_idle(&self) -> EventBusResult<()> {
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while !counts.is_empty() {
            counts = self
                .condvar
                .wait(counts)
                .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        }
        Ok(())
    }
}
