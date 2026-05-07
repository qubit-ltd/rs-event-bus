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
use std::collections::HashMap;
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
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;

pub(crate) type ErrorObserverFn = dyn Fn(&EventBusError) + Send + Sync + 'static;

/// Shared mutable state for [`crate::LocalEventBus`].
pub(crate) struct LocalEventBusInner {
    lifecycle: Mutex<LocalEventBusLifecycle>,
    subscriptions: Mutex<HashMap<TopicKey, Vec<Arc<dyn ErasedSubscription>>>>,
    publisher_interceptors: Mutex<Vec<Arc<dyn PublisherInterceptorEntry>>>,
    subscriber_interceptors: Mutex<Vec<Arc<dyn SubscriberInterceptorEntry>>>,
    error_observers: Mutex<Vec<Arc<ErrorObserverFn>>>,
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
        let executor = self
            .build_subscription_handler_executor()
            .map_err(|error| EventBusError::start_failed(error.to_string()))?;
        lifecycle.executor = Some(executor);
        lifecycle.started = true;
        Ok(true)
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
                let task = take_subscription_task(&mut task)?;
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
        .dispatch(Box::new("payload".to_string()), Arc::new(empty_inner()))
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

    let lifecycle_inner = empty_inner();
    errors.push(
        lifecycle_inner
            .submit_processing_task(coverage_noop_task)
            .expect_err("stopped executor should reject tasks"),
    );
    poison_mutex(&lifecycle_inner.lifecycle);
    assert!(lifecycle_inner.mark_stopped().is_none());
    assert!(!lifecycle_inner.is_started());
    errors.push(
        lifecycle_inner
            .mark_started()
            .expect_err("poisoned lifecycle should reject start"),
    );
    errors.push(
        lifecycle_inner
            .submit_processing_task(coverage_noop_task)
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

    errors
}
