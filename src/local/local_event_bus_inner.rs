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
// qubit-style: allow coverage-cfg

use std::any::{
    Any,
    TypeId,
};
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

use qubit_thread_pool::{
    ExecutorService,
    FixedThreadPool,
    ThreadPoolBuildError,
};

use crate::{
    EventBusError,
    EventBusResult,
    PublishOptions,
    SubscribeOptions,
    TopicKey,
};

use super::erased_subscription::ErasedSubscription;
use super::ordering_lane_key::OrderingLaneKey;
use super::processing_task::ProcessingTask;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;

pub(crate) type ErrorObserverFn = dyn Fn(&EventBusError) + Send + Sync + 'static;

/// Ordered subscriber task plus its local queue-capacity reservation.
struct OrderedProcessingEntry {
    task: ProcessingTask,
    reserved_queue_slot: bool,
}

impl OrderedProcessingEntry {
    /// Creates an ordered processing entry.
    fn new(task: ProcessingTask, reserved_queue_slot: bool) -> Self {
        Self {
            task,
            reserved_queue_slot,
        }
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
    subscriptions: Mutex<HashMap<TopicKey, Vec<Arc<dyn ErasedSubscription>>>>,
    publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorEntry>>>,
    subscriber_interceptors: Mutex<Vec<Arc<dyn SubscriberInterceptorEntry>>>,
    error_observers: Mutex<Vec<Arc<ErrorObserverFn>>>,
    ordering_lanes: Mutex<HashMap<OrderingLaneKey, OrderedProcessingLane>>,
    ordered_queued_task_count: AtomicUsize,
    processing_tracker: ProcessingTracker,
    next_subscription_id: AtomicUsize,
    default_publish_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    default_dead_letter_strategies: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
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
        default_publish_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
        default_subscribe_options: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
        default_dead_letter_strategies: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
        publisher_interceptors: Vec<Arc<dyn PublisherInterceptorEntry>>,
        subscriber_interceptors: Vec<Arc<dyn SubscriberInterceptorEntry>>,
        subscription_handler_pool_size: usize,
        subscription_handler_queue_capacity: Option<usize>,
    ) -> Self {
        Self {
            lifecycle: Mutex::new(LocalEventBusLifecycle::stopped()),
            subscriptions: Mutex::new(HashMap::new()),
            publisher_interceptors: Mutex::new(publisher_interceptors),
            subscriber_interceptors: Mutex::new(subscriber_interceptors),
            error_observers: Mutex::new(Vec::new()),
            ordering_lanes: Mutex::new(HashMap::new()),
            ordered_queued_task_count: AtomicUsize::new(0),
            processing_tracker: ProcessingTracker::new(),
            next_subscription_id: AtomicUsize::new(1),
            default_publish_options,
            default_subscribe_options,
            default_dead_letter_strategies,
            subscription_handler_pool_size,
            subscription_handler_queue_capacity,
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
        if lifecycle.executor.is_some() {
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
            .map_err(|error| EventBusError::start_failed(error.to_string()))?;
        lifecycle.executor = Some(executor);
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
                strategy
                    .downcast_ref::<Arc<crate::core::subscribe_options::DeadLetterStrategyFn<T>>>()
            })
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
        interceptor: Arc<dyn SubscriberInterceptorEntry>,
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
    ) -> EventBusResult<Vec<Arc<dyn SubscriberInterceptorEntry>>> {
        Ok(self
            .subscriber_interceptors
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("subscriber_interceptors"))?
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
            for entry in entries.iter().filter(|entry| entry.id() == id) {
                entry.deactivate();
            }
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
            for entries in subscriptions.values() {
                for entry in entries {
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
    /// Returns lock-poisoning or executor rejection errors before the task runs.
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

    /// Reserves one local ordered-lane queue slot.
    fn reserve_ordered_queue_slot(&self, lifecycle: &LocalEventBusLifecycle) -> EventBusResult<()> {
        let Some(capacity) = self.subscription_handler_queue_capacity else {
            return Ok(());
        };
        let executor_queued = lifecycle
            .executor
            .as_ref()
            .map(FixedThreadPool::queued_count)
            .unwrap_or_default();
        let mut ordered_queued = self.ordered_queued_task_count.load(Ordering::SeqCst);
        loop {
            if ordered_queued.saturating_add(executor_queued) >= capacity {
                return Err(EventBusError::execution_rejected(
                    "subscription handler queue capacity is saturated",
                ));
            }
            match self.ordered_queued_task_count.compare_exchange(
                ordered_queued,
                ordered_queued + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => ordered_queued = current,
            }
        }
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

    /// Pops the next task for an ordered lane.
    fn pop_ordered_lane_task(
        &self,
        lane_key: &OrderingLaneKey,
        guard: &mut OrderedLaneRunnerGuard,
    ) -> Option<ProcessingTask> {
        let next_entry = {
            let Ok(mut lanes) = self.ordering_lanes.lock() else {
                let error = EventBusError::lock_poisoned("ordering_lanes");
                self.observe_error(&error);
                return None;
            };
            let Some(lane) = lanes.get_mut(lane_key) else {
                guard.disarm();
                return None;
            };
            match lane.pop() {
                Some(entry) => Some(entry),
                None => {
                    lanes.remove(lane_key);
                    guard.disarm();
                    None
                }
            }
        };
        let mut next_entry = next_entry?;
        if next_entry.reserved_queue_slot {
            next_entry.reserved_queue_slot = false;
            self.release_ordered_queue_slots(1);
        }
        Some(next_entry.task)
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
            Err(EventBusError::ExecutionRejected { .. }) => OrderedLaneTurn::ContinueInline,
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
    /// time, preserving publish order for a topic, subscriber, and ordering key.
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
            self.submit_ordered_lane_runner_to_executor(executor, lane_key.clone())
        };
        if result.is_err() {
            self.cancel_ordered_lane(&lane_key);
        }
        result
    }

    /// Drains one turn of an ordering lane on a handler-pool worker.
    fn run_ordered_lane(self: Arc<Self>, lane_key: OrderingLaneKey) {
        let mut guard = OrderedLaneRunnerGuard::new(Arc::clone(&self), lane_key.clone());
        loop {
            let Some(task) = self.pop_ordered_lane_task(&lane_key, &mut guard) else {
                return;
            };
            task.run();
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

/// Submits subscriber processing work to the executor.
fn submit_processing_task_to_executor<F>(executor: &FixedThreadPool, task: F) -> EventBusResult<()>
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
            counts = match self.condvar.wait(counts) {
                Ok(counts) => counts,
                Err(_) => return Err(EventBusError::lock_poisoned("processing_tracker")),
            };
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
            counts = match self.condvar.wait(counts) {
                Ok(counts) => counts,
                Err(_) => return Err(EventBusError::lock_poisoned("processing_tracker")),
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
    fn wait_for_all_idle_timeout(&self, timeout: Duration) -> EventBusResult<bool> {
        let started_at = Instant::now();
        let mut counts = self
            .counts
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("processing_tracker"))?;
        while !counts.is_empty() {
            let Some(remaining) = remaining_timeout(started_at, timeout) else {
                return Ok(false);
            };
            let (next_counts, timeout_result) = match self.condvar.wait_timeout(counts, remaining) {
                Ok(result) => result,
                Err(_) => return Err(EventBusError::lock_poisoned("processing_tracker")),
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
fn remaining_timeout(started_at: Instant, timeout: Duration) -> Option<Duration> {
    timeout.checked_sub(started_at.elapsed())
}

#[cfg(coverage)]
struct CoveragePublisherInterceptor;

#[cfg(coverage)]
impl PublisherInterceptorEntry for CoveragePublisherInterceptor {
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<String>()
    }

    fn intercept(
        &self,
        envelope: Box<dyn Any + Send>,
    ) -> EventBusResult<Option<Box<dyn Any + Send>>> {
        Ok(Some(envelope))
    }
}

#[cfg(coverage)]
struct CoverageSubscriberInterceptor;

#[cfg(coverage)]
impl SubscriberInterceptorEntry for CoverageSubscriberInterceptor {
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<String>()
    }

    fn wrap_handler(
        &self,
        handler: Box<dyn Any + Send + Sync>,
    ) -> EventBusResult<Box<dyn Any + Send + Sync>> {
        Ok(handler)
    }
}

#[cfg(coverage)]
struct CoverageSubscription;

#[cfg(coverage)]
impl ErasedSubscription for CoverageSubscription {
    fn id(&self) -> usize {
        1
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
    ) -> EventBusResult<()> {
        Ok(())
    }
}

#[cfg(coverage)]
fn coverage_noop_task() {}

#[cfg(coverage)]
fn coverage_ignore_error(_error: &EventBusError) {}

/// Exercises internal defensive branches that require poisoned private locks.
///
/// # Returns
/// Errors produced by intentionally poisoning internal locks and task state.
#[cfg(coverage)]
pub fn coverage_exercise_local_event_bus_inner_defensive_paths() -> Vec<EventBusError> {
    fn empty_inner() -> LocalEventBusInner {
        LocalEventBusInner::new(
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Vec::new(),
            Vec::new(),
            1,
            None,
        )
    }

    fn poison_mutex<T>(mutex: &Mutex<T>) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.lock().expect("coverage mutex should lock");
            panic!("coverage poison");
        }));
    }

    fn push_error<T>(errors: &mut Vec<EventBusError>, result: EventBusResult<T>) {
        errors.extend(result.err());
    }

    let mut errors = Vec::new();
    let topic_key = TopicKey::new(
        "coverage-inner-defensive".to_string(),
        TypeId::of::<String>(),
    );

    coverage_noop_task();
    coverage_ignore_error(&EventBusError::handler_failed("coverage"));

    let publisher_interceptor = CoveragePublisherInterceptor;
    assert_eq!(
        publisher_interceptor.payload_type_id(),
        TypeId::of::<String>(),
    );
    let publisher_output = publisher_interceptor
        .intercept(Box::new("payload".to_string()))
        .expect("coverage publisher interceptor should pass payload");
    assert!(publisher_output.is_some());

    let subscriber_interceptor = CoverageSubscriberInterceptor;
    assert_eq!(
        subscriber_interceptor.payload_type_id(),
        TypeId::of::<String>(),
    );
    let subscriber_output = subscriber_interceptor
        .wrap_handler(Box::new("handler".to_string()))
        .expect("coverage subscriber interceptor should pass handler");
    assert!(subscriber_output.downcast::<String>().is_ok());

    let subscription = CoverageSubscription;
    assert_eq!(subscription.id(), 1);
    assert_eq!(subscription.priority(), 0);
    subscription.deactivate();
    subscription
        .dispatch(
            Box::new("payload".to_string()),
            Arc::new(empty_inner()),
            false,
        )
        .expect("coverage subscription dispatch should succeed");

    let invalid_executor_inner = LocalEventBusInner::new(
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Vec::new(),
        Vec::new(),
        0,
        None,
    );
    errors.push(
        invalid_executor_inner
            .mark_started()
            .expect_err("invalid executor should fail startup"),
    );

    let mut one_shot = Some(coverage_noop_task);
    let task = take_subscription_task(&mut one_shot).expect("first task take should succeed");
    task();
    errors.extend(take_subscription_task(&mut one_shot).err());

    let lane_bus = Arc::new(empty_inner());
    let lane_key = OrderingLaneKey::new(topic_key.clone(), "coverage-order", 1);
    let mut lane = OrderedProcessingLane::new();
    assert_eq!(lane.release_front_queue_slot(), 0);
    lane.push(
        ProcessingTask::new(Arc::clone(&lane_bus), topic_key.clone(), coverage_noop_task),
        false,
    );
    assert_eq!(lane.release_front_queue_slot(), 0);
    lane.push(
        ProcessingTask::new(Arc::clone(&lane_bus), topic_key.clone(), coverage_noop_task),
        true,
    );
    assert_eq!(lane.release_all_queue_slots(), 1);
    drop(lane);
    lane_bus.release_ordered_queue_slots(0);

    let cancel_bus = Arc::new(empty_inner());
    let mut cancel_lane = OrderedProcessingLane::new();
    cancel_lane.push(
        ProcessingTask::new(
            Arc::clone(&cancel_bus),
            topic_key.clone(),
            coverage_noop_task,
        ),
        true,
    );
    cancel_bus
        .ordered_queued_task_count
        .store(1, Ordering::SeqCst);
    cancel_bus
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(lane_key.clone(), cancel_lane);
    {
        let _guard = OrderedLaneRunnerGuard::new(Arc::clone(&cancel_bus), lane_key.clone());
    }
    assert!(
        cancel_bus
            .ordering_lanes
            .lock()
            .expect("coverage lanes should lock")
            .get(&lane_key)
            .is_none()
    );
    assert_eq!(
        cancel_bus.ordered_queued_task_count.load(Ordering::SeqCst),
        0,
    );

    let pop_bus = Arc::new(empty_inner());
    let missing_lane_key = OrderingLaneKey::new(topic_key.clone(), "missing-order", 1);
    let mut missing_guard =
        OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), missing_lane_key.clone());
    assert!(
        pop_bus
            .pop_ordered_lane_task(&missing_lane_key, &mut missing_guard)
            .is_none()
    );

    let empty_lane_key = OrderingLaneKey::new(topic_key.clone(), "empty-order", 1);
    pop_bus
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(empty_lane_key.clone(), OrderedProcessingLane::new());
    let mut empty_guard = OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), empty_lane_key.clone());
    assert!(
        pop_bus
            .pop_ordered_lane_task(&empty_lane_key, &mut empty_guard)
            .is_none()
    );

    let reserved_lane_key = OrderingLaneKey::new(topic_key.clone(), "reserved-order", 1);
    let mut reserved_lane = OrderedProcessingLane::new();
    reserved_lane.push(
        ProcessingTask::new(Arc::clone(&pop_bus), topic_key.clone(), coverage_noop_task),
        true,
    );
    pop_bus.ordered_queued_task_count.store(1, Ordering::SeqCst);
    pop_bus
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(reserved_lane_key.clone(), reserved_lane);
    let mut reserved_guard =
        OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), reserved_lane_key.clone());
    pop_bus
        .pop_ordered_lane_task(&reserved_lane_key, &mut reserved_guard)
        .expect("reserved lane should yield a task")
        .run();

    let mut finish_guard =
        OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), missing_lane_key.clone());
    assert!(matches!(
        pop_bus.finish_ordered_lane_turn(&missing_lane_key, &mut finish_guard),
        OrderedLaneTurn::Drained
    ));

    let rejected_order_inner = Arc::new(empty_inner());
    rejected_order_inner
        .mark_started()
        .expect("coverage ordered inner should start");
    {
        let lifecycle = rejected_order_inner
            .lifecycle
            .lock()
            .expect("coverage lifecycle should lock");
        lifecycle
            .executor
            .as_ref()
            .expect("coverage executor should exist")
            .shutdown();
    }
    errors.push(
        rejected_order_inner
            .submit_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "rejected-order", 1),
                ProcessingTask::new(
                    Arc::clone(&rejected_order_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                false,
            )
            .expect_err("shutdown executor should reject ordered runner"),
    );

    let poisoned_lifecycle_order_inner = Arc::new(empty_inner());
    poison_mutex(&poisoned_lifecycle_order_inner.lifecycle);
    errors.push(
        poisoned_lifecycle_order_inner
            .submit_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "poisoned-lifecycle-order", 1),
                ProcessingTask::new(
                    Arc::clone(&poisoned_lifecycle_order_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                false,
            )
            .expect_err("poisoned lifecycle should reject ordered submission"),
    );

    let poisoned_lanes_order_inner = Arc::new(empty_inner());
    poisoned_lanes_order_inner
        .mark_started()
        .expect("coverage poisoned lanes inner should start");
    poison_mutex(&poisoned_lanes_order_inner.ordering_lanes);
    errors.push(
        poisoned_lanes_order_inner
            .submit_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "poisoned-lanes-order", 1),
                ProcessingTask::new(
                    Arc::clone(&poisoned_lanes_order_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                false,
            )
            .expect_err("poisoned lanes should reject ordered submission"),
    );
    if let Some(executor) = poisoned_lanes_order_inner.take_executor() {
        executor.shutdown();
    }

    let no_executor_order_inner = Arc::new(empty_inner());
    let no_executor_lane_key = OrderingLaneKey::new(topic_key.clone(), "no-executor-order", 1);
    let mut no_executor_lane = OrderedProcessingLane::new();
    no_executor_lane.push(
        ProcessingTask::new(
            Arc::clone(&no_executor_order_inner),
            topic_key.clone(),
            coverage_noop_task,
        ),
        false,
    );
    no_executor_order_inner
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(no_executor_lane_key.clone(), no_executor_lane);
    let mut no_executor_guard = OrderedLaneRunnerGuard::new(
        Arc::clone(&no_executor_order_inner),
        no_executor_lane_key.clone(),
    );
    assert!(matches!(
        no_executor_order_inner
            .finish_ordered_lane_turn(&no_executor_lane_key, &mut no_executor_guard,),
        OrderedLaneTurn::Cancelled
    ));

    let draining_inner = empty_inner();
    assert!(draining_inner.mark_started().expect("inner should start"));
    assert!(draining_inner.mark_stopping());
    errors.push(
        draining_inner
            .mark_started()
            .expect_err("draining executor should block restart"),
    );
    assert!(!draining_inner.mark_stopping());
    if let Some(executor) = draining_inner.take_executor() {
        executor.shutdown();
    }

    let lifecycle_inner = empty_inner();
    errors.push(
        lifecycle_inner
            .submit_processing_task(coverage_noop_task, false)
            .expect_err("stopped executor should reject tasks"),
    );
    poison_mutex(&lifecycle_inner.lifecycle);
    assert!(lifecycle_inner.mark_stopped().is_none());
    assert!(!lifecycle_inner.mark_stopping());
    assert!(lifecycle_inner.take_executor().is_none());
    assert!(!lifecycle_inner.is_started());
    errors.push(
        lifecycle_inner
            .mark_started()
            .expect_err("poisoned lifecycle should reject start"),
    );
    errors.push(
        lifecycle_inner
            .submit_processing_task(coverage_noop_task, false)
            .expect_err("poisoned lifecycle should reject task submission"),
    );

    let publisher_inner = empty_inner();
    poison_mutex(&publisher_inner.publisher_interceptors);
    errors.push(
        publisher_inner
            .add_publisher_interceptor(Arc::new(CoveragePublisherInterceptor))
            .expect_err("poisoned publisher interceptors should reject add"),
    );
    push_error(&mut errors, publisher_inner.publisher_interceptors());

    let subscriber_inner = empty_inner();
    poison_mutex(&subscriber_inner.subscriber_interceptors);
    errors.push(
        subscriber_inner
            .add_subscriber_interceptor(Arc::new(CoverageSubscriberInterceptor))
            .expect_err("poisoned subscriber interceptors should reject add"),
    );
    push_error(&mut errors, subscriber_inner.subscriber_interceptors());

    let observer_inner = empty_inner();
    poison_mutex(&observer_inner.error_observers);
    observer_inner.observe_error(&EventBusError::handler_failed("coverage"));
    errors.push(
        observer_inner
            .add_error_observer(Arc::new(coverage_ignore_error))
            .expect_err("poisoned error observers should reject add"),
    );

    let subscriptions_inner = empty_inner();
    subscriptions_inner
        .unsubscribe(&topic_key, 1)
        .expect("missing subscription should be a no-op");
    poison_mutex(&subscriptions_inner.subscriptions);
    errors.push(
        subscriptions_inner
            .add_subscription(topic_key.clone(), Arc::new(CoverageSubscription))
            .expect_err("poisoned subscriptions should reject add"),
    );
    push_error(
        &mut errors,
        subscriptions_inner.subscriptions_for(&topic_key),
    );
    errors.push(
        subscriptions_inner
            .unsubscribe(&topic_key, 1)
            .expect_err("poisoned subscriptions should reject unsubscribe"),
    );
    subscriptions_inner.clear_subscriptions();

    let tracker_inner = empty_inner();
    tracker_inner.finish_processing(&topic_key);
    poison_mutex(&tracker_inner.processing_tracker.counts);
    errors.push(
        tracker_inner
            .start_processing(&topic_key)
            .expect_err("poisoned tracker should reject start"),
    );
    tracker_inner.finish_processing(&topic_key);
    errors.push(
        tracker_inner
            .wait_for_idle(&topic_key)
            .expect_err("poisoned tracker should reject topic wait"),
    );
    errors.push(
        tracker_inner
            .wait_for_all_idle()
            .expect_err("poisoned tracker should reject global wait"),
    );
    errors.push(
        tracker_inner
            .wait_for_all_idle_timeout(Duration::from_millis(1))
            .expect_err("poisoned tracker should reject timeout wait"),
    );
    push_error(&mut errors, tracker_inner.processing_tracker.has_active());

    let tracker = ProcessingTracker::new();
    tracker
        .start(&topic_key)
        .expect("coverage tracker should start");
    assert!(
        !tracker
            .wait_for_all_idle_timeout(Duration::ZERO)
            .expect("coverage tracker timeout should return")
    );
    tracker.finish(&topic_key);

    let poisoned_ordering_inner = Arc::new(empty_inner());
    let poisoned_lane_key = OrderingLaneKey::new(topic_key, "poisoned-order", 1);
    poison_mutex(&poisoned_ordering_inner.ordering_lanes);
    let mut poisoned_guard = OrderedLaneRunnerGuard::new(
        Arc::clone(&poisoned_ordering_inner),
        poisoned_lane_key.clone(),
    );
    assert!(matches!(
        poisoned_ordering_inner.finish_ordered_lane_turn(&poisoned_lane_key, &mut poisoned_guard),
        OrderedLaneTurn::Cancelled
    ));
    poisoned_ordering_inner.cancel_ordered_lane(&poisoned_lane_key);

    errors
}
