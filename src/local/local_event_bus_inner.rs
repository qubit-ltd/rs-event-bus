// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Shared state for the local event bus.

mod admission;
mod ordering_lane;
mod processing_tracker;
use std::any::Any;
use std::any::TypeId;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

pub(crate) use admission::DeliveryPermit;
use ordering_lane::OrderedProcessingLane;
use processing_tracker::ProcessingTracker;
use qubit_collections::map::OrderedIndexMap;
use qubit_executor::CancelResult;
use qubit_executor::ExecutorService;
use qubit_executor::ExecutorServiceBuilderError;
use qubit_executor::ScheduledExecutorService;
use qubit_executor::SingleThreadScheduledExecutorService;
use qubit_thread_pool::FixedThreadPool;

use super::erased_subscription::ErasedSubscription;
use super::local_event_bus::PublisherInterceptorAny;
use super::local_event_bus::SubscriberInterceptorAny;
use super::ordering_lane_key::OrderingLaneKey;
use super::processing_task::ProcessingTask;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;
use crate::DeliveryFailure;
use crate::EventBusError;
use crate::EventBusResult;
use crate::PublishOptions;
use crate::SubscribeOptions;
use crate::TopicKey;
use crate::core::SubscriptionState;
use crate::core::delivery_limits::DeliveryLimits;
use crate::core::subscribe_options::DeadLetterStrategyAnyFn;

type ErrorObserverFn = dyn Fn(&EventBusError) + Send + Sync + 'static;
type DeliveryFailureObserverFn = dyn Fn(&DeliveryFailure) + Send + Sync + 'static;
type TypeErasedDefaults = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;
type TopicSubscriptions = OrderedIndexMap<usize, Reverse<i32>, Arc<dyn ErasedSubscription>>;

/// Runtime options used to construct a local event bus.
pub(crate) struct LocalEventBusRuntimeOptions {
    pub(crate) default_publish_options: TypeErasedDefaults,
    pub(crate) default_subscribe_options: TypeErasedDefaults,
    pub(crate) default_dead_letter_strategies: TypeErasedDefaults,
    pub(crate) global_default_dead_letter_strategy: Option<Arc<DeadLetterStrategyAnyFn>>,
    pub(crate) global_publisher_interceptors: Vec<Arc<dyn PublisherInterceptorAny>>,
    pub(crate) global_subscriber_interceptors: Vec<Arc<dyn SubscriberInterceptorAny>>,
    pub(crate) publisher_interceptors: Vec<Arc<dyn PublisherInterceptorEntry>>,
    pub(crate) subscriber_interceptors: Vec<Arc<dyn SubscriberInterceptorEntry>>,
    pub(crate) subscription_handler_pool_size: usize,
    pub(crate) delivery_limits: DeliveryLimits,
}
/// Shared mutable state for [`crate::LocalEventBus`].
pub(crate) struct LocalEventBusInner {
    pub(super) lifecycle: Mutex<LocalEventBusLifecycle>,
    subscriptions: Mutex<HashMap<TopicKey, TopicSubscriptions>>,
    global_publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorAny>>>,
    global_subscriber_interceptors: Mutex<Vec<Arc<dyn SubscriberInterceptorAny>>>,
    publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorEntry>>>,
    subscriber_interceptors: Mutex<Vec<Arc<dyn SubscriberInterceptorEntry>>>,
    error_observers: Mutex<Vec<Arc<ErrorObserverFn>>>,
    delivery_failure_observers: Mutex<Vec<Arc<DeliveryFailureObserverFn>>>,
    ordering_lanes: Mutex<HashMap<OrderingLaneKey, OrderedProcessingLane>>,
    ordered_queued_task_count: AtomicUsize,
    pub(super) in_flight_delivery_count: Arc<AtomicUsize>,
    processing_tracker: ProcessingTracker,
    next_subscription_id: AtomicUsize,
    default_publish_options: TypeErasedDefaults,
    default_subscribe_options: TypeErasedDefaults,
    default_dead_letter_strategies: TypeErasedDefaults,
    global_default_dead_letter_strategy: Option<Arc<DeadLetterStrategyAnyFn>>,
    subscription_handler_pool_size: usize,
    pub(super) delivery_limits: DeliveryLimits,
}

impl LocalEventBusInner {
    /// Creates shared local event bus state.
    ///
    /// # Parameters
    /// - `default_subscribe_options`: Typed default subscription options.
    /// - `subscription_handler_pool_size`: Worker count for subscriber
    ///   handlers.
    /// - `delivery_limits`: Independent admission and executor queue limits.
    ///
    /// # Returns
    /// Shared state initialized in the stopped lifecycle state.
    pub(crate) fn new(options: LocalEventBusRuntimeOptions) -> Self {
        Self {
            lifecycle: Mutex::new(LocalEventBusLifecycle::stopped()),
            subscriptions: Mutex::new(HashMap::new()),
            global_publisher_interceptors: Mutex::new(options.global_publisher_interceptors),
            global_subscriber_interceptors: Mutex::new(options.global_subscriber_interceptors),
            publisher_interceptors: Mutex::new(options.publisher_interceptors),
            subscriber_interceptors: Mutex::new(options.subscriber_interceptors),
            error_observers: Mutex::new(Vec::new()),
            delivery_failure_observers: Mutex::new(Vec::new()),
            ordering_lanes: Mutex::new(HashMap::new()),
            ordered_queued_task_count: AtomicUsize::new(0),
            in_flight_delivery_count: Arc::new(AtomicUsize::new(0)),
            processing_tracker: ProcessingTracker::new(),
            next_subscription_id: AtomicUsize::new(1),
            default_publish_options: options.default_publish_options,
            default_subscribe_options: options.default_subscribe_options,
            default_dead_letter_strategies: options.default_dead_letter_strategies,
            global_default_dead_letter_strategy: options.global_default_dead_letter_strategy,
            subscription_handler_pool_size: options.subscription_handler_pool_size,
            delivery_limits: options.delivery_limits,
        }
    }

    /// Marks the bus as started.
    ///
    /// # Returns
    /// `true` when this call changed state from stopped to started.
    pub(crate) fn mark_started(&self) -> EventBusResult<bool> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        match lifecycle.state {
            LifecycleState::Started => return Ok(false),
            LifecycleState::Stopping => {
                return Err(EventBusError::start_failed("previous shutdown is still in progress"));
            }
            LifecycleState::Stopped => {}
        }
        if lifecycle.executor.is_some() || lifecycle.delay_scheduler.is_some() {
            return Err(EventBusError::start_failed(
                "previous shutdown is still draining subscriber work",
            ));
        }
        if self.processing_tracker.has_active()? {
            return Err(EventBusError::start_failed(
                "previous shutdown still has active subscriber work",
            ));
        }
        let executor = self
            .build_subscription_handler_executor()
            .map_err(start_failed_from_thread_pool_error)?;
        let delay_scheduler = self
            .build_delay_scheduler()
            .map_err(start_failed_from_thread_pool_error)?;
        lifecycle.executor = Some(executor);
        lifecycle.delay_scheduler = Some(delay_scheduler);
        lifecycle.state = LifecycleState::Started;
        Ok(true)
    }

    /// Marks the bus as stopping while keeping its handler executor alive.
    ///
    /// # Returns
    /// `true` when this call changed state from started to stopping.
    pub(crate) fn mark_stopping(&self) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if lifecycle.state != LifecycleState::Started {
            return false;
        }
        lifecycle.state = LifecycleState::Stopping;
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
        if lifecycle.state != LifecycleState::Started {
            return None;
        }
        lifecycle.state = LifecycleState::Stopping;
        lifecycle.executor.take()
    }

    /// Removes the handler executor after the bus has entered stopping state.
    ///
    /// # Returns
    /// Handler executor if one is still owned by the bus.
    pub(crate) fn take_executor(&self) -> Option<FixedThreadPool> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        lifecycle.executor.take()
    }

    /// Removes the delayed-delivery scheduler after the bus has entered
    /// stopping state.
    ///
    /// # Returns
    /// Delayed-delivery scheduler if one is still owned by the bus.
    pub(crate) fn take_delay_scheduler(&self) -> Option<SingleThreadScheduledExecutorService> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        lifecycle.delay_scheduler.take()
    }

    /// Completes a shutdown after subscriptions and runtime resources are
    /// cleared.
    pub(crate) fn complete_shutdown(&self) {
        if let Ok(mut lifecycle) = self.lifecycle.lock() {
            lifecycle.state = LifecycleState::Stopped;
        }
    }

    /// Returns whether the bus is currently started.
    ///
    /// # Returns
    /// `true` if publishing and subscribing are allowed.
    pub(crate) fn is_started(&self) -> bool {
        self.lifecycle
            .lock()
            .map(|lifecycle| lifecycle.state == LifecycleState::Started)
            .unwrap_or(false)
    }

    /// Allocates a new subscription ID.
    ///
    /// # Returns
    /// Process-local subscription ID.
    pub(crate) fn next_subscription_id(&self) -> usize {
        self.next_subscription_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns typed default publish options.
    ///
    /// # Returns
    /// Type-specific default options if configured.
    pub(crate) fn default_publish_options<T>(&self) -> Option<PublishOptions<T>>
    where
        T: 'static,
    {
        self.default_publish_options
            .get(&TypeId::of::<T>())
            .and_then(|options| options.downcast_ref::<PublishOptions<T>>())
            .cloned()
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

    /// Returns a typed default dead-letter strategy.
    ///
    /// # Returns
    /// Type-specific strategy if configured.
    pub(crate) fn default_dead_letter_strategy<T>(
        &self,
    ) -> Option<Arc<crate::core::subscribe_options::DeadLetterStrategyFn<T>>>
    where
        T: 'static,
    {
        self.default_dead_letter_strategies
            .get(&TypeId::of::<T>())
            .and_then(|strategy| {
                strategy.downcast_ref::<Arc<crate::core::subscribe_options::DeadLetterStrategyFn<T>>>()
            })
            .cloned()
    }

    /// Returns the global default dead-letter strategy.
    ///
    /// # Returns
    /// Type-erased strategy if configured.
    pub(crate) fn global_default_dead_letter_strategy(&self) -> Option<Arc<DeadLetterStrategyAnyFn>> {
        self.global_default_dead_letter_strategy.clone()
    }

    /// Returns registered global publisher interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn global_publisher_interceptors(&self) -> EventBusResult<Vec<Arc<dyn PublisherInterceptorAny>>> {
        Ok(self
            .global_publisher_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("global_publisher_interceptors"))?
            .clone())
    }

    /// Returns registered publisher interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn publisher_interceptors(&self) -> EventBusResult<Vec<Arc<dyn PublisherInterceptorEntry>>> {
        Ok(self
            .publisher_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("publisher_interceptors"))?
            .clone())
    }

    /// Returns registered subscriber interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn subscriber_interceptors(&self) -> EventBusResult<Vec<Arc<dyn SubscriberInterceptorEntry>>> {
        Ok(self
            .subscriber_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriber_interceptors"))?
            .clone())
    }

    /// Returns registered global subscriber interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn global_subscriber_interceptors(&self) -> EventBusResult<Vec<Arc<dyn SubscriberInterceptorAny>>> {
        Ok(self
            .global_subscriber_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("global_subscriber_interceptors"))?
            .clone())
    }

    /// Adds an error observer.
    ///
    /// # Parameters
    /// - `observer`: Callback notified about internal callback failures.
    ///
    /// # Returns
    /// `Ok(())` when the observer is stored.
    pub(crate) fn add_error_observer(&self, observer: Arc<ErrorObserverFn>) -> EventBusResult<()> {
        self.error_observers
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("error_observers"))?
            .push(observer);
        Ok(())
    }

    pub(crate) fn add_delivery_failure_observer(&self, observer: Arc<DeliveryFailureObserverFn>) -> EventBusResult<()> {
        self.delivery_failure_observers
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("delivery_failure_observers"))?
            .push(observer);
        Ok(())
    }

    pub(crate) fn observe_delivery_failure(&self, failure: &DeliveryFailure) {
        let Ok(observers) = self
            .delivery_failure_observers
            .lock()
            .map(|observers| observers.clone())
        else {
            return;
        };
        for observer in observers {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(failure)));
        }
    }

    /// Notifies registered error observers.
    ///
    /// # Parameters
    /// - `error`: Internal failure to observe.
    pub(crate) fn observe_error(&self, error: &EventBusError) {
        let Ok(observers) = self.error_observers.lock().map(|observers| observers.clone()) else {
            return;
        };
        for observer in observers {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(error)));
        }
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
        let id = subscription.id();
        let inserted =
            subscriptions
                .entry(topic_key)
                .or_default()
                .try_insert(id, Reverse(subscription.priority()), subscription);
        assert!(inserted.is_ok(), "subscription ID must be unique");
        Ok(())
    }

    /// Registers a subscription atomically with the lifecycle start check.
    ///
    /// # Errors
    /// Returns `NotStarted` after shutdown begins or a lock error when state is
    /// unavailable.
    pub(crate) fn register_subscription_if_started(
        &self,
        topic_key: TopicKey,
        subscription: Arc<dyn ErasedSubscription>,
    ) -> EventBusResult<()> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        if lifecycle.state != LifecycleState::Started {
            return Err(EventBusError::not_started());
        }
        self.add_subscription(topic_key, subscription)
    }

    /// Returns subscriptions for a topic key.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to look up.
    ///
    /// # Returns
    /// A cloned list of subscription entries.
    pub(crate) fn subscriptions_for(&self, topic_key: &TopicKey) -> EventBusResult<Vec<Arc<dyn ErasedSubscription>>> {
        Ok(self
            .subscriptions
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriptions"))?
            .get(topic_key)
            .map(|entries| entries.values_ordered().map(Arc::clone).collect())
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
        let removed = if let Some(entries) = subscriptions.get_mut(topic_key) {
            let removed = entries.remove(&id);
            if entries.is_empty() {
                subscriptions.remove(topic_key);
            }
            removed
        } else {
            None
        };
        if let Some(entry) = removed {
            entry.into_value().deactivate();
        }
        Ok(())
    }

    /// Clears all subscriptions.
    pub(crate) fn clear_subscriptions(&self) {
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            for entries in subscriptions.values() {
                for entry in entries.values_ordered() {
                    entry.deactivate();
                }
            }
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

    /// Waits until a topic has zero active work or the timeout elapses.
    ///
    /// # Parameters
    /// - `topic_key`: Topic key to wait for.
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once the topic is idle, or `Ok(false)` when the timeout
    /// elapses first.
    pub(crate) fn wait_for_idle_timeout(&self, topic_key: &TopicKey, timeout: Duration) -> EventBusResult<bool> {
        self.processing_tracker.wait_for_idle_timeout(topic_key, timeout)
    }

    /// Waits until all topics have zero active work.
    ///
    /// # Returns
    /// `Ok(())` once all tracked topics are idle.
    pub(crate) fn wait_for_all_idle(&self) -> EventBusResult<()> {
        self.processing_tracker.wait_for_all_idle()
    }

    /// Waits until all topics have zero active work or the timeout elapses.
    ///
    /// # Parameters
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once all tracked topics are idle, or `Ok(false)` when the
    /// timeout elapses first.
    pub(crate) fn wait_for_all_idle_timeout(&self, timeout: Duration) -> EventBusResult<bool> {
        self.processing_tracker.wait_for_all_idle_timeout(timeout)
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
    /// Returns lock-poisoning or executor rejection errors before the task
    /// runs.
    pub(crate) fn submit_processing_task<F>(&self, task: F, allow_stopping: bool) -> EventBusResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        let executor = executor_for_dispatch(&lifecycle, allow_stopping)?;
        submit_processing_task_to_executor(executor, task)
    }

    /// Schedules delayed subscriber processing without occupying a handler
    /// worker.
    ///
    /// # Parameters
    /// - `task`: Subscriber processing task accepted by dispatch.
    /// - `delay`: Delay before the task can enter the handler pool.
    /// - `subscription_state`: State used to wake the delay when the
    ///   subscription is cancelled.
    ///
    /// # Returns
    /// `Ok(())` after the delay wait has been scheduled.
    ///
    /// # Errors
    /// Returns executor admission errors if the bus is not accepting dispatch
    /// or the delayed-delivery executor rejects the delay waiter.
    pub(crate) fn submit_delayed_processing_task(
        self: &Arc<Self>,
        task: ProcessingTask,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
        allow_stopping: bool,
    ) -> EventBusResult<()> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        let delay_scheduler = delay_scheduler_for_dispatch(&lifecycle, allow_stopping)?;
        let bus = Arc::clone(self);
        let task_slot = Arc::new(Mutex::new(Some(task)));
        let registration = Arc::new(Mutex::new(None::<usize>));
        let fired = Arc::new(Mutex::new(false));
        let task_for_delay = Arc::clone(&task_slot);
        let registration_for_delay = Arc::clone(&registration);
        let fired_for_delay = Arc::clone(&fired);
        let subscription_for_delay = Arc::clone(&subscription_state);
        let scheduled = delay_scheduler.schedule(delay, move || {
            if let Ok(mut fired) = fired_for_delay.lock() {
                *fired = true;
            }
            if let Ok(mut registration) = registration_for_delay.lock()
                && let Some(registration_id) = registration.take()
            {
                subscription_for_delay.unregister_delay_cancellation(registration_id);
            }
            if subscription_for_delay.is_active() {
                let task_for_executor = Arc::clone(&task_for_delay);
                let result = bus.submit_processing_task(
                    move || {
                        if let Ok(mut task) = task_for_executor.lock()
                            && let Some(task) = task.take()
                        {
                            task.run();
                        }
                    },
                    true,
                );
                match result {
                    Ok(()) => {}
                    Err(error) => {
                        let recovered_task = match task_for_delay.lock() {
                            Ok(mut task) => task.take(),
                            Err(_) => None,
                        };
                        if let Some(task) = recovered_task {
                            task.reject(&error);
                        }
                    }
                }
            }
            Ok::<(), EventBusError>(())
        });
        let handle = match scheduled {
            Ok(handle) => handle,
            Err(error) => {
                return Err(EventBusError::execution_rejected(error.to_string()));
            }
        };
        if fired.lock().map(|fired| *fired).unwrap_or(true) {
            return Ok(());
        }
        let task_for_cancel = Arc::clone(&task_slot);
        let handle_for_cancel = Arc::new(Mutex::new(Some(handle)));
        let registration_id = subscription_state.register_delay_cancellation(move || {
            if let Ok(mut handle) = handle_for_cancel.lock()
                && let Some(handle) = handle.take()
                && handle.cancel() == CancelResult::Cancelled
                && let Ok(mut task) = task_for_cancel.lock()
            {
                let _ = task.take();
            }
        });
        if let Some(registration_id) = registration_id
            && let Ok(mut registration) = registration.lock()
        {
            if fired.lock().map(|fired| *fired).unwrap_or(true) {
                subscription_state.unregister_delay_cancellation(registration_id);
            } else {
                *registration = Some(registration_id);
            }
        }
        Ok(())
    }

    /// Builds the subscription handler executor.
    ///
    /// # Returns
    /// A fixed thread pool configured for subscriber processing.
    ///
    /// # Errors
    /// Returns executor build errors from `rs-thread-pool`.
    fn build_subscription_handler_executor(&self) -> Result<FixedThreadPool, ExecutorServiceBuilderError> {
        let mut builder = FixedThreadPool::builder()
            .pool_size(self.subscription_handler_pool_size)
            .thread_name_prefix("qubit-event-bus-subscriber");
        if let Some(capacity) = self.delivery_limits.handler_queue_capacity() {
            builder = builder.queue_capacity(capacity);
        }
        builder.build()
    }

    /// Builds the delayed-delivery scheduled executor service.
    ///
    /// # Returns
    /// A scheduled executor service used to wait for delayed deliveries.
    ///
    /// # Errors
    /// Returns executor build errors from `rs-executor`.
    fn build_delay_scheduler(&self) -> Result<SingleThreadScheduledExecutorService, ExecutorServiceBuilderError> {
        SingleThreadScheduledExecutorService::new("qubit-event-bus-delay")
    }
}

/// Returns the executor if the current lifecycle allows dispatch.
pub(super) fn executor_for_dispatch(
    lifecycle: &LocalEventBusLifecycle,
    allow_stopping: bool,
) -> EventBusResult<&FixedThreadPool> {
    if lifecycle.state != LifecycleState::Started && !(allow_stopping && lifecycle.state == LifecycleState::Stopping) {
        return Err(EventBusError::not_started());
    }
    lifecycle.executor.as_ref().ok_or_else(EventBusError::not_started)
}

/// Returns the delayed-delivery scheduler if the lifecycle allows dispatch.
pub(super) fn delay_scheduler_for_dispatch(
    lifecycle: &LocalEventBusLifecycle,
    allow_stopping: bool,
) -> EventBusResult<&SingleThreadScheduledExecutorService> {
    if lifecycle.state != LifecycleState::Started && !(allow_stopping && lifecycle.state == LifecycleState::Stopping) {
        return Err(EventBusError::not_started());
    }
    lifecycle
        .delay_scheduler
        .as_ref()
        .ok_or_else(EventBusError::not_started)
}

/// Converts an executor build failure into a local event-bus startup failure.
fn start_failed_from_thread_pool_error(error: ExecutorServiceBuilderError) -> EventBusError {
    EventBusError::start_failed(error.to_string())
}

/// Submits subscriber processing work to the executor.
pub(super) fn submit_processing_task_to_executor<F>(executor: &FixedThreadPool, task: F) -> EventBusResult<()>
where
    F: FnOnce() + Send + 'static,
{
    let mut task = Some(task);
    executor
        .submit_callable(move || {
            let task = take_subscription_task(&mut task)?;
            task();
            Ok::<(), EventBusError>(())
        })
        .map(|_handle| ())
        .map_err(|error| EventBusError::execution_rejected(error.to_string()))
}

/// Takes a one-shot subscription task from executor state.
///
/// # Parameters
/// - `task`: Mutable one-shot task slot.
///
/// # Returns
/// Task to invoke exactly once.
///
/// # Errors
/// Returns [`EventBusError::HandlerFailed`] when the executor invokes the same
/// callable more than once.
fn take_subscription_task<F>(task: &mut Option<F>) -> EventBusResult<F>
where
    F: FnOnce() + Send + 'static,
{
    match task.take() {
        Some(task) => Ok(task),
        None => Err(EventBusError::handler_failed(
            "subscription task was invoked more than once",
        )),
    }
}

/// Lifecycle state protected by the local event bus lifecycle lock.
#[derive(Clone, Copy, Eq, PartialEq)]
enum LifecycleState {
    Stopped,
    Started,
    Stopping,
}

pub(super) struct LocalEventBusLifecycle {
    state: LifecycleState,
    pub(super) executor: Option<FixedThreadPool>,
    delay_scheduler: Option<SingleThreadScheduledExecutorService>,
}

impl LocalEventBusLifecycle {
    /// Creates a stopped lifecycle without a handler executor.
    ///
    /// # Returns
    /// Stopped lifecycle state.
    fn stopped() -> Self {
        Self {
            state: LifecycleState::Stopped,
            executor: None,
            delay_scheduler: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;

    use super::LocalEventBusInner;
    use super::LocalEventBusRuntimeOptions;
    use crate::EventBusError;
    use crate::EventBusResult;
    use crate::LocalEventBus;
    use crate::Topic;
    use crate::core::delivery_limits::DeliveryLimits;
    use crate::local::erased_subscription::DispatchAdmission;
    use crate::local::erased_subscription::ErasedSubscription;

    struct TestSubscription;

    impl ErasedSubscription for TestSubscription {
        fn id(&self) -> usize {
            1
        }
        fn subscriber_id(&self) -> &str {
            "test"
        }
        fn priority(&self) -> i32 {
            0
        }
        fn deactivate(&self) {}
        fn dispatch(
            &self,
            _envelope: Box<dyn Any + Send>,
            _bus: Arc<LocalEventBusInner>,
            _allow_stopping: bool,
        ) -> EventBusResult<DispatchAdmission> {
            Ok(DispatchAdmission::Accepted)
        }
    }

    /// Registration after the shutdown transition must not revive a cleared
    /// subscription.
    #[test]
    fn test_registration_rejects_after_shutdown_clears_subscriptions() {
        let bus = LocalEventBus::started().expect("bus should start");
        let topic = Topic::<String>::try_new("shutdown-registration").expect("topic should build");
        assert!(bus.inner.is_started());
        assert!(bus.inner.mark_stopping());
        bus.inner.clear_subscriptions();
        let result = bus
            .inner
            .register_subscription_if_started(topic.key(), Arc::new(TestSubscription));
        assert!(matches!(result, Err(EventBusError::NotStarted)));
        assert!(
            bus.inner
                .subscriptions_for(&topic.key())
                .expect("lookup should work")
                .is_empty()
        );
    }

    /// A cancellation that fails to remove storage must remain retryable.
    #[test]
    fn test_failed_unsubscribe_keeps_handle_active() {
        let bus = LocalEventBus::started().expect("bus should start");
        let topic = Topic::<String>::try_new("poisoned-unsubscribe").expect("topic should build");
        let handle = bus
            .subscribe("sub", &topic, |_| ())
            .expect("subscription should register");
        let inner = Arc::clone(&bus.inner);
        assert!(
            thread::spawn(move || {
                let _guard = inner
                    .subscriptions
                    .lock()
                    .expect("subscriptions lock should be available");
                panic!("poison subscriptions lock for cancellation test");
            })
            .join()
            .is_err()
        );
        assert!(handle.cancel().is_err());
        assert!(handle.is_active(), "failed cancellation must remain retryable");
    }

    /// Restart is forbidden until shutdown finalization clears the state.
    #[test]
    fn test_start_rejects_stopping_state() {
        let bus = LocalEventBus::started().expect("bus should start");
        assert!(bus.inner.mark_stopping());
        assert!(matches!(bus.start(), Err(EventBusError::StartFailed { .. })));
    }

    #[test]
    fn test_unbounded_delivery_permit_preserves_counter() {
        let inner = LocalEventBusInner::new(LocalEventBusRuntimeOptions {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: Vec::new(),
            subscriber_interceptors: Vec::new(),
            subscription_handler_pool_size: 1,
            delivery_limits: DeliveryLimits::default(),
        });

        let first = inner
            .try_acquire_delivery_permit()
            .expect("unbounded permit should be available");
        drop(first);
        let second = inner
            .try_acquire_delivery_permit()
            .expect("unbounded permit should remain available");
        drop(second);

        assert_eq!(
            inner.in_flight_delivery_count.load(Ordering::SeqCst),
            0,
            "unbounded permits must not mutate the bounded counter"
        );
    }
}
