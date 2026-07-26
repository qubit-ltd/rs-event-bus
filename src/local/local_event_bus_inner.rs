// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared state for the local event bus.
// qubit-style: allow coverage-cfg
// qubit-style: allow multiple-public-types

#[cfg(coverage)]
mod coverage;

use std::any::{
    Any,
    TypeId,
};
use std::cmp::Reverse;
use std::collections::{
    HashMap,
    VecDeque,
};
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};
use std::sync::{
    Arc,
    Condvar,
    Mutex,
};
use std::time::{
    Duration,
    Instant,
};

use qubit_collections::map::OrderedIndexMap;
use qubit_executor::{
    CancelResult,
    ExecutorService,
    ExecutorServiceBuilderError,
    ScheduledExecutorService,
    SingleThreadScheduledExecutorService,
};
use qubit_thread_pool::FixedThreadPool;

use crate::core::SubscriptionState;
use crate::core::subscribe_options::DeadLetterStrategyAnyFn;
use crate::{
    EventBusError,
    EventBusResult,
    PublishOptions,
    SubscribeOptions,
    TopicKey,
};

use super::erased_subscription::ErasedSubscription;
use super::local_event_bus::{
    PublisherInterceptorAny,
    SubscriberInterceptorAny,
};
use super::ordering_lane_key::OrderingLaneKey;
use super::processing_task::ProcessingTask;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;

#[cfg(coverage)]
pub use coverage::coverage_exercise_local_event_bus_inner_defensive_paths;

type ErrorObserverFn = dyn Fn(&EventBusError) + Send + Sync + 'static;
type TypeErasedDefaults = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;
type TopicSubscriptions =
    OrderedIndexMap<usize, Reverse<i32>, Arc<dyn ErasedSubscription>>;

/// Runtime options used to construct a local event bus.
pub(crate) struct LocalEventBusRuntimeOptions {
    pub(crate) default_publish_options: TypeErasedDefaults,
    pub(crate) default_subscribe_options: TypeErasedDefaults,
    pub(crate) default_dead_letter_strategies: TypeErasedDefaults,
    pub(crate) global_default_dead_letter_strategy:
        Option<Arc<DeadLetterStrategyAnyFn>>,
    pub(crate) global_publisher_interceptors:
        Vec<Arc<dyn PublisherInterceptorAny>>,
    pub(crate) global_subscriber_interceptors:
        Vec<Arc<dyn SubscriberInterceptorAny>>,
    pub(crate) publisher_interceptors: Vec<Arc<dyn PublisherInterceptorEntry>>,
    pub(crate) subscriber_interceptors:
        Vec<Arc<dyn SubscriberInterceptorEntry>>,
    pub(crate) subscription_handler_pool_size: usize,
    pub(crate) subscription_handler_queue_capacity: Option<usize>,
}

/// Ordered subscriber task plus its local queue-capacity reservation.
struct OrderedProcessingEntry {
    task: ProcessingTask,
    reserved_queue_slot: bool,
    delay_started_at: Option<Instant>,
    delay: Option<Duration>,
    subscription_state: Option<Arc<SubscriptionState>>,
}

impl OrderedProcessingEntry {
    /// Creates an ordered processing entry.
    fn new(task: ProcessingTask, reserved_queue_slot: bool) -> Self {
        Self {
            task,
            reserved_queue_slot,
            delay_started_at: None,
            delay: None,
            subscription_state: None,
        }
    }

    /// Creates a delayed ordered processing entry.
    fn delayed(
        task: ProcessingTask,
        reserved_queue_slot: bool,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
    ) -> Self {
        Self {
            task,
            reserved_queue_slot,
            delay_started_at: Some(Instant::now()),
            delay: Some(delay),
            subscription_state: Some(subscription_state),
        }
    }

    /// Returns whether the entry belongs to a cancelled subscription.
    fn is_inactive(&self) -> bool {
        self.subscription_state
            .as_ref()
            .is_some_and(|state| !state.is_active())
    }

    /// Returns the remaining delay before this entry is ready.
    fn remaining_delay(&self) -> Option<(Duration, Arc<SubscriptionState>)> {
        let delay_started_at = self.delay_started_at?;
        let delay = self.delay?;
        let subscription_state = self.subscription_state.as_ref()?;
        delay
            .checked_sub(delay_started_at.elapsed())
            .filter(|remaining| !remaining.is_zero())
            .map(|remaining| (remaining, Arc::clone(subscription_state)))
    }
}

/// Queue of ordered tasks waiting behind the active lane task.
struct OrderedProcessingLane {
    queued: VecDeque<OrderedProcessingEntry>,
}

impl OrderedProcessingLane {
    /// Creates an empty active lane.
    fn new() -> Self {
        Self {
            queued: VecDeque::new(),
        }
    }

    /// Queues a task behind the active task.
    fn push(&mut self, task: ProcessingTask, reserved_queue_slot: bool) {
        self.queued
            .push_back(OrderedProcessingEntry::new(task, reserved_queue_slot));
    }

    /// Queues a delayed task behind the active task.
    fn push_delayed(
        &mut self,
        task: ProcessingTask,
        reserved_queue_slot: bool,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
    ) {
        self.queued.push_back(OrderedProcessingEntry::delayed(
            task,
            reserved_queue_slot,
            delay,
            subscription_state,
        ));
    }

    /// Takes the next queued task.
    fn pop(&mut self) -> Option<OrderedProcessingEntry> {
        self.queued.pop_front()
    }

    /// Returns whether the lane has no queued work.
    fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    /// Releases the local reservation for the next task if one exists.
    fn release_front_queue_slot(&mut self) -> usize {
        let Some(entry) = self.queued.front_mut() else {
            return 0;
        };
        if entry.reserved_queue_slot {
            entry.reserved_queue_slot = false;
            1
        } else {
            0
        }
    }

    /// Releases all local reservations still held by the lane.
    fn release_all_queue_slots(&mut self) -> usize {
        let mut released = 0;
        for entry in &mut self.queued {
            if entry.reserved_queue_slot {
                entry.reserved_queue_slot = false;
                released += 1;
            }
        }
        released
    }
}

/// Work found at the front of an ordered lane.
enum OrderedLaneTask {
    Ready(ProcessingTask),
    Delayed(Duration, Arc<SubscriptionState>),
}

/// Guard that cancels queued lane tasks if an ordered lane runner exits early.
struct OrderedLaneRunnerGuard {
    bus: Arc<LocalEventBusInner>,
    lane_key: Option<OrderingLaneKey>,
}

impl OrderedLaneRunnerGuard {
    /// Creates a lane runner guard.
    fn new(bus: Arc<LocalEventBusInner>, lane_key: OrderingLaneKey) -> Self {
        Self {
            bus,
            lane_key: Some(lane_key),
        }
    }

    /// Marks the lane as drained normally.
    fn disarm(&mut self) {
        self.lane_key = None;
    }
}

impl Drop for OrderedLaneRunnerGuard {
    /// Cancels queued tasks when the lane runner exits before draining them.
    fn drop(&mut self) {
        if let Some(lane_key) = self.lane_key.take() {
            self.bus.cancel_ordered_lane(&lane_key);
        }
    }
}

/// Continuation decision after one ordered lane task finishes.
enum OrderedLaneTurn {
    Drained,
    Rescheduled,
    ContinueInline,
    Cancelled,
}

/// Shared mutable state for [`crate::LocalEventBus`].
pub(crate) struct LocalEventBusInner {
    lifecycle: Mutex<LocalEventBusLifecycle>,
    subscriptions: Mutex<HashMap<TopicKey, TopicSubscriptions>>,
    global_publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorAny>>>,
    global_subscriber_interceptors:
        Mutex<Vec<Arc<dyn SubscriberInterceptorAny>>>,
    publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorEntry>>>,
    subscriber_interceptors: Mutex<Vec<Arc<dyn SubscriberInterceptorEntry>>>,
    error_observers: Mutex<Vec<Arc<ErrorObserverFn>>>,
    ordering_lanes: Mutex<HashMap<OrderingLaneKey, OrderedProcessingLane>>,
    ordered_queued_task_count: AtomicUsize,
    processing_tracker: ProcessingTracker,
    next_subscription_id: AtomicUsize,
    default_publish_options: TypeErasedDefaults,
    default_subscribe_options: TypeErasedDefaults,
    default_dead_letter_strategies: TypeErasedDefaults,
    global_default_dead_letter_strategy: Option<Arc<DeadLetterStrategyAnyFn>>,
    subscription_handler_pool_size: usize,
    subscription_handler_queue_capacity: Option<usize>,
}

impl LocalEventBusInner {
    /// Creates shared local event bus state.
    ///
    /// # Parameters
    /// - `default_subscribe_options`: Typed default subscription options.
    /// - `subscription_handler_pool_size`: Worker count for subscriber
    ///   handlers.
    /// - `subscription_handler_queue_capacity`: Optional queued handler limit.
    ///
    /// # Returns
    /// Shared state initialized in the stopped lifecycle state.
    pub(crate) fn new(options: LocalEventBusRuntimeOptions) -> Self {
        Self {
            lifecycle: Mutex::new(LocalEventBusLifecycle::stopped()),
            subscriptions: Mutex::new(HashMap::new()),
            global_publisher_interceptors: Mutex::new(
                options.global_publisher_interceptors,
            ),
            global_subscriber_interceptors: Mutex::new(
                options.global_subscriber_interceptors,
            ),
            publisher_interceptors: Mutex::new(options.publisher_interceptors),
            subscriber_interceptors: Mutex::new(
                options.subscriber_interceptors,
            ),
            error_observers: Mutex::new(Vec::new()),
            ordering_lanes: Mutex::new(HashMap::new()),
            ordered_queued_task_count: AtomicUsize::new(0),
            processing_tracker: ProcessingTracker::new(),
            next_subscription_id: AtomicUsize::new(1),
            default_publish_options: options.default_publish_options,
            default_subscribe_options: options.default_subscribe_options,
            default_dead_letter_strategies: options
                .default_dead_letter_strategies,
            global_default_dead_letter_strategy: options
                .global_default_dead_letter_strategy,
            subscription_handler_pool_size: options
                .subscription_handler_pool_size,
            subscription_handler_queue_capacity: options
                .subscription_handler_queue_capacity,
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
        if lifecycle.started {
            return Ok(false);
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
        lifecycle.started = true;
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
        if !lifecycle.started {
            return false;
        }
        lifecycle.started = false;
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
    pub(crate) fn take_delay_scheduler(
        &self,
    ) -> Option<SingleThreadScheduledExecutorService> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        lifecycle.delay_scheduler.take()
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
    pub(crate) fn default_subscribe_options<T>(
        &self,
    ) -> Option<SubscribeOptions<T>>
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
    pub(crate) fn global_default_dead_letter_strategy(
        &self,
    ) -> Option<Arc<DeadLetterStrategyAnyFn>> {
        self.global_default_dead_letter_strategy.clone()
    }

    /// Adds a global publisher interceptor.
    ///
    /// # Parameters
    /// - `interceptor`: Interceptor applied to every published payload type.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    #[cfg_attr(not(coverage), allow(dead_code))]
    pub(crate) fn add_global_publisher_interceptor(
        &self,
        interceptor: Arc<dyn PublisherInterceptorAny>,
    ) -> EventBusResult<()> {
        self.global_publisher_interceptors
            .lock()
            .map_err(|_| {
                EventBusError::lock_poisoned("global_publisher_interceptors")
            })?
            .push(interceptor);
        Ok(())
    }

    /// Returns registered global publisher interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn global_publisher_interceptors(
        &self,
    ) -> EventBusResult<Vec<Arc<dyn PublisherInterceptorAny>>> {
        Ok(self
            .global_publisher_interceptors
            .lock()
            .map_err(|_| {
                EventBusError::lock_poisoned("global_publisher_interceptors")
            })?
            .clone())
    }

    /// Adds a publisher interceptor.
    ///
    /// # Parameters
    /// - `interceptor`: Type-erased interceptor entry.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    #[cfg_attr(not(coverage), allow(dead_code))]
    pub(crate) fn add_publisher_interceptor(
        &self,
        interceptor: Arc<dyn PublisherInterceptorEntry>,
    ) -> EventBusResult<()> {
        self.publisher_interceptors
            .lock()
            .map_err(|_| {
                EventBusError::lock_poisoned("publisher_interceptors")
            })?
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
            .map_err(|_| {
                EventBusError::lock_poisoned("publisher_interceptors")
            })?
            .clone())
    }

    /// Adds a type-erased subscriber interceptor entry.
    ///
    /// # Parameters
    /// - `interceptor`: Shared typed interceptor adapter.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    #[cfg_attr(not(coverage), allow(dead_code))]
    pub(crate) fn add_subscriber_interceptor(
        &self,
        interceptor: Arc<dyn SubscriberInterceptorEntry>,
    ) -> EventBusResult<()> {
        self.subscriber_interceptors
            .lock()
            .map_err(|_| {
                EventBusError::lock_poisoned("subscriber_interceptors")
            })?
            .push(interceptor);
        Ok(())
    }

    /// Returns registered subscriber interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn subscriber_interceptors(
        &self,
    ) -> EventBusResult<Vec<Arc<dyn SubscriberInterceptorEntry>>> {
        Ok(self
            .subscriber_interceptors
            .lock()
            .map_err(|_| {
                EventBusError::lock_poisoned("subscriber_interceptors")
            })?
            .clone())
    }

    /// Adds a global subscriber interceptor.
    ///
    /// # Parameters
    /// - `interceptor`: Interceptor applied to every subscriber payload type.
    ///
    /// # Returns
    /// `Ok(())` when the entry is stored.
    #[cfg_attr(not(coverage), allow(dead_code))]
    pub(crate) fn add_global_subscriber_interceptor(
        &self,
        interceptor: Arc<dyn SubscriberInterceptorAny>,
    ) -> EventBusResult<()> {
        self.global_subscriber_interceptors
            .lock()
            .map_err(|_| {
                EventBusError::lock_poisoned("global_subscriber_interceptors")
            })?
            .push(interceptor);
        Ok(())
    }

    /// Returns registered global subscriber interceptors.
    ///
    /// # Returns
    /// Cloned interceptor entries.
    pub(crate) fn global_subscriber_interceptors(
        &self,
    ) -> EventBusResult<Vec<Arc<dyn SubscriberInterceptorAny>>> {
        Ok(self
            .global_subscriber_interceptors
            .lock()
            .map_err(|_| {
                EventBusError::lock_poisoned("global_subscriber_interceptors")
            })?
            .clone())
    }

    /// Adds an error observer.
    ///
    /// # Parameters
    /// - `observer`: Callback notified about internal callback failures.
    ///
    /// # Returns
    /// `Ok(())` when the observer is stored.
    pub(crate) fn add_error_observer(
        &self,
        observer: Arc<ErrorObserverFn>,
    ) -> EventBusResult<()> {
        self.error_observers
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("error_observers"))?
            .push(observer);
        Ok(())
    }

    /// Notifies registered error observers.
    ///
    /// # Parameters
    /// - `error`: Internal failure to observe.
    pub(crate) fn observe_error(&self, error: &EventBusError) {
        let Ok(observers) = self
            .error_observers
            .lock()
            .map(|observers| observers.clone())
        else {
            return;
        };
        for observer in observers {
            let _ =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    observer(error)
                }));
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
        let inserted = subscriptions.entry(topic_key).or_default().try_insert(
            id,
            Reverse(subscription.priority()),
            subscription,
        );
        assert!(inserted.is_ok(), "subscription ID must be unique");
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
    pub(crate) fn unsubscribe(
        &self,
        topic_key: &TopicKey,
        id: usize,
    ) -> EventBusResult<()> {
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
    pub(crate) fn start_processing(
        &self,
        topic_key: &TopicKey,
    ) -> EventBusResult<()> {
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
    pub(crate) fn wait_for_idle(
        &self,
        topic_key: &TopicKey,
    ) -> EventBusResult<()> {
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
    pub(crate) fn wait_for_idle_timeout(
        &self,
        topic_key: &TopicKey,
        timeout: Duration,
    ) -> EventBusResult<bool> {
        self.processing_tracker
            .wait_for_idle_timeout(topic_key, timeout)
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
    pub(crate) fn wait_for_all_idle_timeout(
        &self,
        timeout: Duration,
    ) -> EventBusResult<bool> {
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
    pub(crate) fn submit_processing_task<F>(
        &self,
        task: F,
        allow_stopping: bool,
    ) -> EventBusResult<()>
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
        let delay_scheduler =
            delay_scheduler_for_dispatch(&lifecycle, allow_stopping)?;
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
                subscription_for_delay
                    .unregister_delay_cancellation(registration_id);
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
                    Err(EventBusError::ExecutionRejected { .. }) => {
                        let recovered_task = match task_for_delay.lock() {
                            Ok(mut task) => task.take(),
                            Err(_) => None,
                        };
                        if let Some(task) = recovered_task {
                            task.run();
                        }
                    }
                    Err(error) => bus.observe_error(&error),
                }
            }
            Ok::<(), EventBusError>(())
        });
        let handle = match scheduled {
            Ok(handle) => handle,
            Err(error) => {
                return Err(EventBusError::execution_rejected(
                    error.to_string(),
                ));
            }
        };
        if fired.lock().map(|fired| *fired).unwrap_or(true) {
            return Ok(());
        }
        let task_for_cancel = Arc::clone(&task_slot);
        let handle_for_cancel = Arc::new(Mutex::new(Some(handle)));
        let registration_id =
            subscription_state.register_delay_cancellation(move || {
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
                subscription_state
                    .unregister_delay_cancellation(registration_id);
            } else {
                *registration = Some(registration_id);
            }
        }
        Ok(())
    }

    /// Reserves one local ordered-lane queue slot.
    fn reserve_ordered_queue_slot(
        &self,
        lifecycle: &LocalEventBusLifecycle,
    ) -> EventBusResult<()> {
        let Some(capacity) = self.subscription_handler_queue_capacity else {
            return Ok(());
        };
        let executor_queued = lifecycle
            .executor
            .as_ref()
            .map(FixedThreadPool::queued_count)
            .unwrap_or_default();
        self.ordered_queued_task_count
            .fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |ordered_queued| {
                    if ordered_queued.saturating_add(executor_queued)
                        >= capacity
                    {
                        None
                    } else {
                        Some(ordered_queued + 1)
                    }
                },
            )
            .map(|_| ())
            .map_err(|_| {
                EventBusError::execution_rejected(
                    "subscription handler queue capacity is saturated",
                )
            })
    }

    /// Releases local ordered-lane queue slots.
    fn release_ordered_queue_slots(&self, slots: usize) {
        if slots == 0 {
            return;
        }
        let _ = self.ordered_queued_task_count.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |current| Some(current.saturating_sub(slots)),
        );
    }

    /// Submits a lane runner to a known-live handler executor.
    fn submit_ordered_lane_runner_to_executor(
        self: &Arc<Self>,
        executor: &FixedThreadPool,
        lane_key: OrderingLaneKey,
    ) -> EventBusResult<()> {
        let bus = Arc::clone(self);
        submit_processing_task_to_executor(executor, move || {
            bus.run_ordered_lane(lane_key);
        })
    }

    /// Submits the lane runner for an ordering lane to the handler executor.
    fn submit_ordered_lane_runner(
        self: &Arc<Self>,
        lane_key: OrderingLaneKey,
        allow_stopping: bool,
    ) -> EventBusResult<()> {
        let bus = Arc::clone(self);
        self.submit_processing_task(
            move || {
                bus.run_ordered_lane(lane_key);
            },
            allow_stopping,
        )
    }

    /// Resubmits an ordered lane after its front task delay elapses.
    fn submit_ordered_lane_runner_after_delay(
        self: &Arc<Self>,
        lane_key: OrderingLaneKey,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
    ) -> EventBusResult<()> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        let delay_scheduler = delay_scheduler_for_dispatch(&lifecycle, true)?;
        let bus = Arc::clone(self);
        let registration = Arc::new(Mutex::new(None::<usize>));
        let fired = Arc::new(Mutex::new(false));
        let registration_for_delay = Arc::clone(&registration);
        let fired_for_delay = Arc::clone(&fired);
        let subscription_for_delay = Arc::clone(&subscription_state);
        let lane_key_for_delay = lane_key.clone();
        let scheduled = delay_scheduler.schedule(delay, move || {
            if let Ok(mut fired) = fired_for_delay.lock() {
                *fired = true;
            }
            if let Ok(mut registration) = registration_for_delay.lock()
                && let Some(registration_id) = registration.take()
            {
                subscription_for_delay
                    .unregister_delay_cancellation(registration_id);
            }
            if subscription_for_delay.is_active() {
                match bus.submit_ordered_lane_runner(
                    lane_key_for_delay.clone(),
                    true,
                ) {
                    Ok(()) => {}
                    Err(EventBusError::ExecutionRejected { .. }) => {
                        Arc::clone(&bus)
                            .run_ordered_lane(lane_key_for_delay.clone());
                    }
                    Err(error) => {
                        bus.observe_error(&error);
                        bus.cancel_ordered_lane(&lane_key_for_delay);
                    }
                }
            } else {
                bus.cancel_ordered_lane(&lane_key_for_delay);
            }
            Ok::<(), EventBusError>(())
        });
        let handle = match scheduled {
            Ok(handle) => handle,
            Err(error) => {
                return Err(EventBusError::execution_rejected(
                    error.to_string(),
                ));
            }
        };
        if fired.lock().map(|fired| *fired).unwrap_or(true) {
            return Ok(());
        }
        let bus_for_cancel = Arc::clone(self);
        let lane_key_for_cancel = lane_key.clone();
        let handle_for_cancel = Arc::new(Mutex::new(Some(handle)));
        let registration_id =
            subscription_state.register_delay_cancellation(move || {
                if let Ok(mut handle) = handle_for_cancel.lock()
                    && let Some(handle) = handle.take()
                    && handle.cancel() == CancelResult::Cancelled
                {
                    bus_for_cancel.cancel_ordered_lane(&lane_key_for_cancel);
                }
            });
        if let Some(registration_id) = registration_id
            && let Ok(mut registration) = registration.lock()
        {
            if fired.lock().map(|fired| *fired).unwrap_or(true) {
                subscription_state
                    .unregister_delay_cancellation(registration_id);
            } else {
                *registration = Some(registration_id);
            }
        }
        Ok(())
    }

    /// Pops the next task for an ordered lane.
    fn pop_ordered_lane_task(
        &self,
        lane_key: &OrderingLaneKey,
        guard: &mut OrderedLaneRunnerGuard,
    ) -> Option<OrderedLaneTask> {
        loop {
            let Ok(mut lanes) = self.ordering_lanes.lock() else {
                let error = EventBusError::lock_poisoned("ordering_lanes");
                self.observe_error(&error);
                return None;
            };
            let Some(lane) = lanes.get_mut(lane_key) else {
                guard.disarm();
                return None;
            };

            let Some(front) = lane.queued.front() else {
                lanes.remove(lane_key);
                guard.disarm();
                return None;
            };
            if front.is_inactive() {
                let mut inactive_entry = lane
                    .pop()
                    .expect("front entry should exist after inactive check");
                if inactive_entry.reserved_queue_slot {
                    inactive_entry.reserved_queue_slot = false;
                    self.release_ordered_queue_slots(1);
                }
                drop(inactive_entry);
                continue;
            }
            if let Some((remaining, subscription_state)) =
                front.remaining_delay()
            {
                return Some(OrderedLaneTask::Delayed(
                    remaining,
                    subscription_state,
                ));
            }

            let mut next_entry = lane
                .pop()
                .expect("front entry should exist after readiness check");
            if next_entry.reserved_queue_slot {
                next_entry.reserved_queue_slot = false;
                self.release_ordered_queue_slots(1);
            }
            return Some(OrderedLaneTask::Ready(next_entry.task));
        }
    }

    /// Finishes one ordered lane turn and decides how the lane should continue.
    fn finish_ordered_lane_turn(
        self: &Arc<Self>,
        lane_key: &OrderingLaneKey,
        guard: &mut OrderedLaneRunnerGuard,
    ) -> OrderedLaneTurn {
        {
            let Ok(mut lanes) = self.ordering_lanes.lock() else {
                let error = EventBusError::lock_poisoned("ordering_lanes");
                self.observe_error(&error);
                return OrderedLaneTurn::Cancelled;
            };
            let Some(lane) = lanes.get_mut(lane_key) else {
                guard.disarm();
                return OrderedLaneTurn::Drained;
            };
            if lane.is_empty() {
                lanes.remove(lane_key);
                guard.disarm();
                return OrderedLaneTurn::Drained;
            }
            let released = lane.release_front_queue_slot();
            self.release_ordered_queue_slots(released);
        };
        match self.submit_ordered_lane_runner(lane_key.clone(), true) {
            Ok(()) => {
                guard.disarm();
                OrderedLaneTurn::Rescheduled
            }
            Err(EventBusError::ExecutionRejected { .. }) => {
                OrderedLaneTurn::ContinueInline
            }
            Err(error) => {
                self.observe_error(&error);
                self.cancel_ordered_lane(lane_key);
                guard.disarm();
                OrderedLaneTurn::Cancelled
            }
        }
    }

    /// Submits subscriber processing work through a per-ordering-key lane.
    ///
    /// Tasks in the same lane are submitted to the handler executor one at a
    /// time, preserving publish order for a topic, subscriber, and ordering
    /// key.
    ///
    /// # Parameters
    /// - `lane_key`: Topic, subscriber, and ordering key identifying the lane.
    /// - `task`: Processing task to run or cancel.
    /// - `allow_stopping`: Whether already accepted internal work may continue
    ///   while the bus is stopping.
    ///
    /// # Returns
    /// `Ok(())` when the task is accepted into the ordering lane.
    ///
    /// # Errors
    /// Returns lock-poisoning, stopped-bus, or queue-saturation errors.
    pub(crate) fn submit_ordered_processing_task(
        self: &Arc<Self>,
        lane_key: OrderingLaneKey,
        task: ProcessingTask,
        allow_stopping: bool,
    ) -> EventBusResult<()> {
        let result = {
            let lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
            let executor = executor_for_dispatch(&lifecycle, allow_stopping)?;
            let mut lanes = self
                .ordering_lanes
                .lock()
                .map_err(|_| EventBusError::lock_poisoned("ordering_lanes"))?;
            if let Some(lane) = lanes.get_mut(&lane_key) {
                self.reserve_ordered_queue_slot(&lifecycle)?;
                lane.push(task, true);
                return Ok(());
            }
            let mut lane = OrderedProcessingLane::new();
            lane.push(task, false);
            lanes.insert(lane_key.clone(), lane);
            drop(lanes);
            self.submit_ordered_lane_runner_to_executor(
                executor,
                lane_key.clone(),
            )
        };
        if result.is_err() {
            self.cancel_ordered_lane(&lane_key);
        }
        result
    }

    /// Submits delayed subscriber processing work through a per-ordering-key
    /// lane.
    pub(crate) fn submit_delayed_ordered_processing_task(
        self: &Arc<Self>,
        lane_key: OrderingLaneKey,
        task: ProcessingTask,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
        allow_stopping: bool,
    ) -> EventBusResult<()> {
        let result = {
            let lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
            let executor = executor_for_dispatch(&lifecycle, allow_stopping)?;
            let mut lanes = self
                .ordering_lanes
                .lock()
                .map_err(|_| EventBusError::lock_poisoned("ordering_lanes"))?;
            if let Some(lane) = lanes.get_mut(&lane_key) {
                self.reserve_ordered_queue_slot(&lifecycle)?;
                lane.push_delayed(task, true, delay, subscription_state);
                return Ok(());
            }
            let mut lane = OrderedProcessingLane::new();
            lane.push_delayed(task, false, delay, subscription_state);
            lanes.insert(lane_key.clone(), lane);
            drop(lanes);
            self.submit_ordered_lane_runner_to_executor(
                executor,
                lane_key.clone(),
            )
        };
        if result.is_err() {
            self.cancel_ordered_lane(&lane_key);
        }
        result
    }

    /// Drains one turn of an ordering lane on a handler-pool worker.
    fn run_ordered_lane(self: Arc<Self>, lane_key: OrderingLaneKey) {
        let mut guard =
            OrderedLaneRunnerGuard::new(Arc::clone(&self), lane_key.clone());
        loop {
            let Some(task) = self.pop_ordered_lane_task(&lane_key, &mut guard)
            else {
                return;
            };
            match task {
                OrderedLaneTask::Ready(task) => task.run(),
                OrderedLaneTask::Delayed(delay, subscription_state) => {
                    match self.submit_ordered_lane_runner_after_delay(
                        lane_key.clone(),
                        delay,
                        subscription_state,
                    ) {
                        Ok(()) => guard.disarm(),
                        Err(error) => {
                            self.observe_error(&error);
                            self.cancel_ordered_lane(&lane_key);
                            guard.disarm();
                        }
                    }
                    return;
                }
            }
            match self.finish_ordered_lane_turn(&lane_key, &mut guard) {
                OrderedLaneTurn::ContinueInline => {}
                OrderedLaneTurn::Drained
                | OrderedLaneTurn::Rescheduled
                | OrderedLaneTurn::Cancelled => return,
            }
        }
    }

    /// Removes an ordering lane and drops all queued processing tasks.
    fn cancel_ordered_lane(&self, lane_key: &OrderingLaneKey) {
        let removed_lane = {
            let Ok(mut lanes) = self.ordering_lanes.lock() else {
                let error = EventBusError::lock_poisoned("ordering_lanes");
                self.observe_error(&error);
                return;
            };
            lanes.remove(lane_key)
        };
        if let Some(mut lane) = removed_lane {
            let released = lane.release_all_queue_slots();
            self.release_ordered_queue_slots(released);
        }
    }

    /// Builds the subscription handler executor.
    ///
    /// # Returns
    /// A fixed thread pool configured for subscriber processing.
    ///
    /// # Errors
    /// Returns executor build errors from `rs-thread-pool`.
    fn build_subscription_handler_executor(
        &self,
    ) -> Result<FixedThreadPool, ExecutorServiceBuilderError> {
        let mut builder = FixedThreadPool::builder()
            .pool_size(self.subscription_handler_pool_size)
            .thread_name_prefix("qubit-event-bus-subscriber");
        if let Some(capacity) = self.subscription_handler_queue_capacity {
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
    fn build_delay_scheduler(
        &self,
    ) -> Result<SingleThreadScheduledExecutorService, ExecutorServiceBuilderError>
    {
        SingleThreadScheduledExecutorService::new("qubit-event-bus-delay")
    }
}

/// Returns the executor if the current lifecycle allows dispatch.
fn executor_for_dispatch(
    lifecycle: &LocalEventBusLifecycle,
    allow_stopping: bool,
) -> EventBusResult<&FixedThreadPool> {
    if !lifecycle.started && !allow_stopping {
        return Err(EventBusError::not_started());
    }
    lifecycle
        .executor
        .as_ref()
        .ok_or_else(EventBusError::not_started)
}

/// Returns the delayed-delivery scheduler if the lifecycle allows dispatch.
fn delay_scheduler_for_dispatch(
    lifecycle: &LocalEventBusLifecycle,
    allow_stopping: bool,
) -> EventBusResult<&SingleThreadScheduledExecutorService> {
    if !lifecycle.started && !allow_stopping {
        return Err(EventBusError::not_started());
    }
    lifecycle
        .delay_scheduler
        .as_ref()
        .ok_or_else(EventBusError::not_started)
}

/// Converts an executor build failure into a local event-bus startup failure.
fn start_failed_from_thread_pool_error(
    error: ExecutorServiceBuilderError,
) -> EventBusError {
    EventBusError::start_failed(error.to_string())
}

/// Submits subscriber processing work to the executor.
fn submit_processing_task_to_executor<F>(
    executor: &FixedThreadPool,
    task: F,
) -> EventBusResult<()>
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
struct LocalEventBusLifecycle {
    started: bool,
    executor: Option<FixedThreadPool>,
    delay_scheduler: Option<SingleThreadScheduledExecutorService>,
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
            delay_scheduler: None,
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
            counts = match self.condvar.wait(counts) {
                Ok(counts) => counts,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        Ok(())
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
    fn wait_for_idle_timeout(
        &self,
        topic_key: &TopicKey,
        timeout: Duration,
    ) -> EventBusResult<bool> {
        let started_at = Instant::now();
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while counts.get(topic_key).copied().unwrap_or(0) > 0 {
            let Some(remaining) = remaining_timeout(started_at, timeout) else {
                return Ok(false);
            };
            let (next_counts, timeout_result) =
                match self.condvar.wait_timeout(counts, remaining) {
                    Ok(result) => result,
                    Err(poisoned) => poisoned.into_inner(),
                };
            counts = next_counts;
            if timeout_result.timed_out()
                && counts.get(topic_key).copied().unwrap_or(0) > 0
            {
                return Ok(false);
            }
        }
        Ok(true)
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
            counts = match self.condvar.wait(counts) {
                Ok(counts) => counts,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        Ok(())
    }

    /// Waits until all topics have zero active work or the timeout elapses.
    ///
    /// # Parameters
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once all tracked topics are idle, or `Ok(false)` when the
    /// timeout elapses first.
    fn wait_for_all_idle_timeout(
        &self,
        timeout: Duration,
    ) -> EventBusResult<bool> {
        let started_at = Instant::now();
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while !counts.is_empty() {
            let Some(remaining) = remaining_timeout(started_at, timeout) else {
                return Ok(false);
            };
            let (next_counts, timeout_result) =
                match self.condvar.wait_timeout(counts, remaining) {
                    Ok(result) => result,
                    Err(poisoned) => poisoned.into_inner(),
                };
            counts = next_counts;
            if timeout_result.timed_out() && !counts.is_empty() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Returns whether any topic still has active or queued processing work.
    fn has_active(&self) -> EventBusResult<bool> {
        Ok(!self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?
            .is_empty())
    }
}

/// Returns the remaining time before a timeout elapses.
///
/// # Parameters
/// - `started_at`: Time when the wait began.
/// - `timeout`: Total timeout budget.
///
/// # Returns
/// Remaining duration, or `None` when the timeout has elapsed.
fn remaining_timeout(
    started_at: Instant,
    timeout: Duration,
) -> Option<Duration> {
    timeout.checked_sub(started_at.elapsed())
}
