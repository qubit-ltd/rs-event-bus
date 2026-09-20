// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Thread-safe in-process event bus.

mod admission;
mod dead_letter;
mod delay;
mod dispatch;
mod interceptor;
mod lifecycle;
mod retry_delivery;
mod subscription_entry;
mod worker_context;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

pub use interceptor::IntoPublisherInterceptorAnyResult;
pub use interceptor::IntoPublisherInterceptorResult;
pub use interceptor::PublisherInterceptor;
pub use interceptor::PublisherInterceptorAny;
pub use interceptor::SubscriberInterceptor;
pub use interceptor::SubscriberInterceptorAny;
pub(super) use interceptor::create_publisher_interceptor_entry;
pub(super) use interceptor::create_subscriber_interceptor_entry;
use qubit_executor::ExecutorService;
use qubit_executor::SingleThreadScheduledExecutorService;
use qubit_thread_pool::FixedThreadPool;

use super::local_event_bus_inner::LocalEventBusInner;
use super::local_event_bus_inner::LocalEventBusRuntimeOptions;
use crate::BatchPublishResult;
use crate::DeliveryFailure;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::IntoEventBusResult;
use crate::PublishOptions;
use crate::PublishReceipt;
use crate::SubscribeOptions;
use crate::Subscription;
use crate::Topic;
use crate::core::delivery_limits::DeliveryLimits;

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;
use retry_delivery::normalize_subscriber_interceptor_result;
use retry_delivery::process_subscription_event;
use retry_delivery::run_with_retry;
use worker_context::SubscriptionWorkerContext;
use worker_context::is_current_subscription_worker_for_bus;
use worker_context::local_event_bus_id;

/// Thread-safe in-process event bus.
///
/// This backend stores subscriptions in memory and dispatches subscriber
/// handlers on background threads. Publishing schedules work and returns after
/// dispatch, while [`wait_for_idle`](Self::wait_for_idle) can be used by tests
/// to wait for all handler work for a topic.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::{LocalEventBus, Topic};
///
/// let bus = LocalEventBus::started().unwrap();
/// let topic = Topic::<String>::try_new("orders.created").unwrap();
/// bus.subscribe("audit", &topic, |_| {}).unwrap();
/// bus.publish(&topic, "order-1001".to_string()).unwrap();
/// bus.wait_for_idle(&topic).unwrap();
/// ```
#[derive(Clone)]
pub struct LocalEventBus {
    pub(crate) inner: Arc<LocalEventBusInner>,
}

/// Keeps the bus in the stopping state until shutdown cleanup has finished.
struct ShutdownCompletionGuard<'a> {
    bus: &'a LocalEventBus,
}

impl Drop for ShutdownCompletionGuard<'_> {
    fn drop(&mut self) {
        if let Some(executor) = self.bus.inner.take_executor() {
            executor.shutdown();
        }
        if let Some(delay_scheduler) = self.bus.inner.take_delay_scheduler() {
            delay_scheduler.shutdown();
        }
        self.bus.inner.clear_subscriptions();
        self.bus.inner.complete_shutdown();
    }
}

impl LocalEventBus {
    /// Creates a stopped local event bus.
    ///
    /// # Returns
    /// A new event bus with no subscriptions.
    pub fn new() -> Self {
        Self::with_runtime_options(LocalEventBusRuntimeOptions {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: Vec::new(),
            subscriber_interceptors: Vec::new(),
            subscription_handler_pool_size: default_subscription_handler_pool_size(),
            delivery_limits: DeliveryLimits::default(),
        })
    }

    /// Creates and starts a local event bus.
    ///
    /// # Returns
    /// A started event bus.
    ///
    /// # Errors
    /// Returns startup errors from the handler executor.
    pub fn started() -> EventBusResult<Self> {
        let bus = Self::new();
        bus.start()?;
        Ok(bus)
    }

    /// Creates a stopped event bus with typed defaults and runtime options.
    ///
    /// # Returns
    /// A stopped event bus.
    pub(crate) fn with_runtime_options(options: LocalEventBusRuntimeOptions) -> Self {
        Self {
            inner: Arc::new(LocalEventBusInner::new(options)),
        }
    }

    /// Starts the event bus.
    ///
    /// # Returns
    /// `Ok(true)` when this call changed the bus from stopped to started.
    ///
    /// # Errors
    /// Returns startup errors from the handler executor.
    pub fn start(&self) -> EventBusResult<bool> {
        self.inner.mark_started()
    }

    /// Shuts down the event bus.
    ///
    /// The method waits for currently scheduled handlers to finish and then
    /// clears all subscriptions.
    ///
    /// # Returns
    /// `true` when this call changed the bus from started to stopped.
    ///
    /// # Panics
    /// Panics when called from one of this bus's subscriber worker threads. A
    /// subscriber worker cannot wait for itself to finish. Use
    /// [`shutdown_nonblocking`](Self::shutdown_nonblocking) or
    /// [`shutdown_with_timeout`](Self::shutdown_with_timeout) from subscriber
    /// handlers.
    pub fn shutdown(&self) -> bool {
        self.assert_not_own_subscription_worker_for_blocking_shutdown();
        if !self.inner.mark_stopping() {
            return false;
        }
        let _completion = ShutdownCompletionGuard { bus: self };
        let _ = self.inner.wait_for_all_idle();
        if let Some(executor) = self.inner.take_executor() {
            executor.shutdown();
            wait_for_executor_termination(&executor);
        }
        if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
            delay_scheduler.shutdown();
            wait_for_delay_scheduler_termination(&delay_scheduler);
        }
        self.inner.clear_subscriptions();
        true
    }

    /// Requests shutdown without waiting for subscriber work to finish.
    ///
    /// The bus stops accepting publish and subscribe operations, asks the
    /// handler executor to shut down, deactivates subscriptions, and
    /// returns immediately. Already running handler code is not
    /// interrupted.
    ///
    /// # Returns
    /// `true` when this call changed the bus from started to stopped.
    pub fn shutdown_nonblocking(&self) -> bool {
        let Some(executor) = self.inner.mark_stopped() else {
            return false;
        };
        let _completion = ShutdownCompletionGuard { bus: self };
        executor.shutdown();
        if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
            delay_scheduler.shutdown();
        }
        self.inner.clear_subscriptions();
        true
    }

    /// Shuts down the event bus with a maximum wait duration.
    ///
    /// The bus stops accepting new publish and subscribe operations
    /// immediately, then waits for scheduled subscriber work and executor
    /// workers to finish. If the timeout elapses, subscriptions are
    /// deactivated before the timeout error is returned.
    ///
    /// # Parameters
    /// - `timeout`: Maximum duration to wait for graceful shutdown.
    ///
    /// # Returns
    /// `Ok(true)` when this call changed the bus from started to stopped and
    /// shutdown completed within the timeout. `Ok(false)` means the bus was
    /// already stopped.
    ///
    /// # Errors
    /// Returns [`EventBusError::ShutdownTimedOut`] if subscriber work or
    /// executor workers do not finish before `timeout`.
    pub fn shutdown_with_timeout(&self, timeout: Duration) -> EventBusResult<bool> {
        let started_at = Instant::now();
        if !self.inner.mark_stopping() {
            return Ok(false);
        }
        let _completion = ShutdownCompletionGuard { bus: self };
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            self.inner.clear_subscriptions();
            if let Some(executor) = self.inner.take_executor() {
                executor.shutdown();
            }
            if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
                delay_scheduler.shutdown();
            }
            return Err(EventBusError::shutdown_timed_out(timeout));
        };
        if !self.inner.wait_for_all_idle_timeout(remaining)? {
            self.inner.clear_subscriptions();
            if let Some(executor) = self.inner.take_executor() {
                executor.shutdown();
            }
            if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
                delay_scheduler.shutdown();
            }
            return Err(EventBusError::shutdown_timed_out(timeout));
        }
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            self.inner.clear_subscriptions();
            if let Some(executor) = self.inner.take_executor() {
                executor.shutdown();
            }
            if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
                delay_scheduler.shutdown();
            }
            return Err(EventBusError::shutdown_timed_out(timeout));
        };
        let Some(executor) = self.inner.take_executor() else {
            if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
                delay_scheduler.shutdown();
            }
            self.inner.clear_subscriptions();
            return Ok(true);
        };
        executor.shutdown();
        if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
            delay_scheduler.shutdown();
            if !wait_for_delay_scheduler_termination_timeout(&delay_scheduler, remaining) {
                self.inner.clear_subscriptions();
                return Err(EventBusError::shutdown_timed_out(timeout));
            }
        }
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            self.inner.clear_subscriptions();
            return Err(EventBusError::shutdown_timed_out(timeout));
        };
        if !wait_for_executor_termination_timeout(&executor, remaining) {
            self.inner.clear_subscriptions();
            return Err(EventBusError::shutdown_timed_out(timeout));
        }
        self.inner.clear_subscriptions();
        Ok(true)
    }

    /// Registers an observer for internal background errors.
    ///
    /// # Parameters
    /// - `observer`: Callback invoked when interceptors, error handlers, or
    ///   dead-letter routing fail.
    ///
    /// # Returns
    /// `Ok(())` when the observer is stored.
    ///
    /// # Errors
    /// Returns a lock-poisoning error if observer state is unavailable.
    pub fn add_error_observer<F>(&self, observer: F) -> EventBusResult<()>
    where
        F: Fn(&EventBusError) + Send + Sync + 'static,
    {
        self.inner.add_error_observer(Arc::new(observer))
    }

    /// Registers an observer for terminal subscriber delivery failures.
    ///
    /// The callback runs once after retries, error handling, and dead-letter
    /// routing have completed for a delivery that remains failed.
    pub fn add_delivery_failure_observer<F>(&self, observer: F) -> EventBusResult<()>
    where
        F: Fn(&DeliveryFailure) + Send + Sync + 'static,
    {
        self.inner.add_delivery_failure_observer(Arc::new(observer))
    }

    /// Waits until all work for a topic is idle.
    ///
    /// # Parameters
    /// - `topic`: Topic to wait for.
    ///
    /// # Returns
    /// `Ok(())` once the topic has no active handler work.
    ///
    /// # Errors
    /// Returns a lock-poisoning error if tracker state is unavailable.
    pub fn wait_for_idle<T>(&self, topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        if is_current_subscription_worker_for_bus(local_event_bus_id(&self.inner)) {
            return Err(EventBusError::would_deadlock("wait_for_idle"));
        }
        self.inner.wait_for_idle(&topic.key())
    }

    /// Waits until all work for a topic is idle or the timeout elapses.
    ///
    /// # Parameters
    /// - `topic`: Topic to wait for.
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once the topic has no active handler work, or `Ok(false)`
    /// when the timeout elapses first.
    ///
    /// # Errors
    /// Returns a lock-poisoning error if tracker state is unavailable.
    pub fn wait_for_idle_timeout<T>(&self, topic: &Topic<T>, timeout: Duration) -> EventBusResult<bool>
    where
        T: 'static,
    {
        if is_current_subscription_worker_for_bus(local_event_bus_id(&self.inner)) {
            return Err(EventBusError::would_deadlock("wait_for_idle_timeout"));
        }
        self.inner.wait_for_idle_timeout(&topic.key(), timeout)
    }

    /// Panics if blocking shutdown is called from this bus's subscriber worker.
    fn assert_not_own_subscription_worker_for_blocking_shutdown(&self) {
        let bus_id = local_event_bus_id(&self.inner);
        if is_current_subscription_worker_for_bus(bus_id) {
            panic!(
                "LocalEventBus::shutdown must not be called from this bus's subscriber worker; use shutdown_nonblocking or shutdown_with_timeout"
            );
        }
    }
}

impl Default for LocalEventBus {
    /// Creates a stopped local event bus.
    fn default() -> Self {
        Self::new()
    }
}

impl crate::EventBus for LocalEventBus {
    type Subscription<T>
        = Subscription<T>
    where
        T: Clone + Send + Sync + 'static;

    /// Starts the local event bus.
    fn start(&self) -> EventBusResult<bool> {
        Self::start(self)
    }

    /// Shuts down the local event bus.
    fn shutdown(&self) -> bool {
        Self::shutdown(self)
    }

    /// Publishes a payload using the local backend.
    fn publish<T>(&self, topic: &Topic<T>, payload: T) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish(self, topic, payload)
    }

    /// Publishes a payload with options using the local backend.
    fn publish_with_options<T>(
        &self,
        topic: &Topic<T>,
        payload: T,
        options: PublishOptions<T>,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_with_options(self, topic, payload, options)
    }

    /// Publishes an envelope using the local backend.
    fn publish_envelope<T>(&self, envelope: EventEnvelope<T>) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_envelope(self, envelope)
    }

    /// Publishes an envelope with options using the local backend.
    fn publish_envelope_with_options<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_envelope_with_options(self, envelope, options)
    }

    /// Publishes a batch using the local backend.
    fn publish_all<T>(&self, envelopes: Vec<EventEnvelope<T>>) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_all(self, envelopes)
    }

    /// Publishes a batch with options using the local backend.
    fn publish_all_with_options<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        Self::publish_all_with_options(self, envelopes, options)
    }

    /// Subscribes a handler using local backend defaults.
    fn subscribe<T, S, F, R>(&self, subscriber_id: S, topic: &Topic<T>, handler: F) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Self::subscribe(self, subscriber_id, topic, handler)
    }

    /// Subscribes a handler with options using the local backend.
    fn subscribe_with_options<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
        options: SubscribeOptions<T>,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Self::subscribe_with_options(self, subscriber_id, topic, handler, options)
    }

    /// Waits until local topic work is idle.
    fn wait_for_idle<T>(&self, topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        Self::wait_for_idle(self, topic)
    }

    /// Waits until local topic work is idle or the timeout elapses.
    fn wait_for_idle_timeout<T>(&self, topic: &Topic<T>, timeout: Duration) -> EventBusResult<bool>
    where
        T: 'static,
    {
        Self::wait_for_idle_timeout(self, topic, timeout)
    }
}

/// Waits for a fixed handler executor to finish after shutdown.
///
/// # Parameters
/// - `executor`: Executor whose graceful shutdown has already been requested.
fn wait_for_executor_termination(executor: &FixedThreadPool) {
    while !executor.is_terminated() {
        thread::sleep(Duration::from_millis(1));
    }
}

/// Waits for a fixed handler executor to finish until the timeout elapses.
///
/// # Parameters
/// - `executor`: Executor whose graceful shutdown has already been requested.
/// - `timeout`: Maximum duration to wait.
///
/// # Returns
/// `true` when the executor terminates before the timeout.
fn wait_for_executor_termination_timeout(executor: &FixedThreadPool, timeout: Duration) -> bool {
    let started_at = Instant::now();
    while !executor.is_terminated() {
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            return false;
        };
        thread::sleep(remaining.min(Duration::from_millis(1)));
    }
    true
}

/// Waits for a delayed task scheduler to finish after shutdown.
///
/// # Parameters
/// - `scheduler`: Scheduler whose graceful shutdown has already been requested.
fn wait_for_delay_scheduler_termination(scheduler: &SingleThreadScheduledExecutorService) {
    while !scheduler.is_terminated() {
        thread::sleep(Duration::from_millis(1));
    }
}

/// Waits for a delayed task scheduler to finish until the timeout elapses.
///
/// # Parameters
/// - `scheduler`: Scheduler whose graceful shutdown has already been requested.
/// - `timeout`: Maximum duration to wait.
///
/// # Returns
/// `true` when the scheduler terminates before the timeout.
fn wait_for_delay_scheduler_termination_timeout(
    scheduler: &SingleThreadScheduledExecutorService,
    timeout: Duration,
) -> bool {
    let started_at = Instant::now();
    while !scheduler.is_terminated() {
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            return false;
        };
        thread::sleep(remaining.min(Duration::from_millis(1)));
    }
    true
}

/// Returns the remaining shutdown timeout.
///
/// # Parameters
/// - `started_at`: Time when the shutdown wait began.
/// - `timeout`: Total timeout budget.
///
/// # Returns
/// Remaining duration, or `None` when the timeout has elapsed.
fn remaining_shutdown_timeout(started_at: Instant, timeout: Duration) -> Option<Duration> {
    timeout.checked_sub(started_at.elapsed())
}

/// Returns the default subscription handler worker count.
///
/// # Returns
/// Available CPU parallelism, or `1` if it cannot be detected.
fn default_subscription_handler_pool_size() -> usize {
    thread::available_parallelism().map(usize::from).unwrap_or(1)
}
