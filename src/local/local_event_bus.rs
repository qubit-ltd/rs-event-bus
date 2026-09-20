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
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::panic;
use std::panic::AssertUnwindSafe;
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
use qubit_retry::Retry;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryConfig;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;
use qubit_thread_pool::FixedThreadPool;

use super::local_event_bus_inner::LocalEventBusInner;
use super::local_event_bus_inner::LocalEventBusRuntimeOptions;
use super::subscriber_interceptor_chain::DownstreamErrorSlot;
use super::subscriber_interceptor_chain::is_recorded_downstream_error;
use crate::AckMode;
use crate::Acknowledgement;
use crate::BatchPublishResult;
use crate::DeadLetterOriginalPayload;
use crate::DeadLetterOutcome;
use crate::DeadLetterPayload;
use crate::DeliveryFailure;
use crate::DispatchStatus;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventBusRetryRule;
use crate::EventEnvelope;
use crate::EventEnvelopeMetadata;
use crate::IntoEventBusResult;
use crate::PublishOptions;
use crate::PublishOutcome;
use crate::PublishReceipt;
use crate::SubscribeOptions;
use crate::Subscription;
use crate::Topic;
use crate::core::SubscriptionState;
use crate::core::delivery_limits::DeliveryLimits;
use crate::core::subscribe_options::DeadLetterStrategyAnyFn;
use crate::core::subscribe_options::DeadLetterStrategyFn;
use crate::core::subscribe_options::normalize_dead_letter_error;

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;
thread_local! {
    static SUBSCRIPTION_WORKER_BUS_IDS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Event delivery state for one handler attempt.
#[derive(Clone)]
struct HandlerDelivery<T: Clone + Send + Sync + 'static> {
    delivered: EventEnvelope<T>,
    acknowledgement: Acknowledgement,
}

impl<T> HandlerDelivery<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a delivered envelope with a fresh acknowledgement.
    ///
    /// # Parameters
    /// - `envelope`: Original event envelope for this attempt.
    ///
    /// # Returns
    /// Delivery state for one handler attempt.
    fn new(envelope: &EventEnvelope<T>) -> Self {
        let acknowledgement = Acknowledgement::new();
        let delivered = envelope.clone().with_acknowledgement(acknowledgement.clone());
        Self {
            delivered,
            acknowledgement,
        }
    }
}

/// Terminal handler failure paired with the final attempt delivery.
struct HandlerRunFailure<T: Clone + Send + Sync + 'static> {
    error: EventBusError,
    delivery: HandlerDelivery<T>,
}

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

/// Returns a stable in-process identifier for one local event bus inner value.
///
/// # Parameters
/// - `inner`: Shared event bus state.
///
/// # Returns
/// Pointer-sized identifier used only for thread-local worker tracking.
fn local_event_bus_id(inner: &Arc<LocalEventBusInner>) -> usize {
    Arc::as_ptr(inner) as usize
}

/// Returns whether the current thread is processing work for the bus.
///
/// # Parameters
/// - `bus_id`: Identifier returned by [`local_event_bus_id`].
///
/// # Returns
/// `true` when the current thread is inside a subscriber task for the same bus.
fn is_current_subscription_worker_for_bus(bus_id: usize) -> bool {
    SUBSCRIPTION_WORKER_BUS_IDS.with(|bus_ids| bus_ids.borrow().contains(&bus_id))
}

/// Thread-local marker for subscriber worker execution.
struct SubscriptionWorkerContext {
    bus_id: usize,
}

impl SubscriptionWorkerContext {
    /// Marks the current thread as processing subscriber work for a bus.
    ///
    /// # Parameters
    /// - `bus_id`: Identifier returned by [`local_event_bus_id`].
    ///
    /// # Returns
    /// Guard that removes the marker on drop.
    fn enter(bus_id: usize) -> Self {
        SUBSCRIPTION_WORKER_BUS_IDS.with(|bus_ids| {
            bus_ids.borrow_mut().push(bus_id);
        });
        Self { bus_id }
    }
}

impl Drop for SubscriptionWorkerContext {
    /// Removes this guard's bus marker from thread-local worker state.
    fn drop(&mut self) {
        SUBSCRIPTION_WORKER_BUS_IDS.with(|bus_ids| {
            let mut bus_ids = bus_ids.borrow_mut();
            if let Some(position) = bus_ids.iter().rposition(|bus_id| *bus_id == self.bus_id) {
                bus_ids.remove(position);
            }
        });
    }
}

/// Processes a subscriber event on a background thread.
///
/// # Parameters
/// - `active`: Shared subscription activity flag.
/// - `handler`: Handler closure.
/// - `options`: Subscriber options.
/// - `subscriber_id`: Subscriber identifier.
/// - `envelope`: Event envelope.
/// - `event_bus`: Event bus used to publish dead letters.
fn process_subscription_event<T>(
    active: Arc<SubscriptionState>,
    handler: Arc<HandlerFn<T>>,
    options: SubscribeOptions<T>,
    subscription_id: usize,
    subscriber_id: String,
    envelope: EventEnvelope<T>,
    event_bus: LocalEventBus,
) where
    T: Clone + Send + Sync + 'static,
{
    if !active.is_active() {
        return;
    }
    match run_handler_with_retry(&handler, &options, envelope) {
        Ok(delivery) => {
            if options.ack_mode() == AckMode::Auto && !delivery.acknowledgement.is_completed() {
                delivery.acknowledgement.ack();
            }
        }
        Err(failure) => {
            let dead_letter = handle_subscription_failure(
                &options,
                &subscriber_id,
                &failure.delivery.delivered,
                &failure.error,
                &failure.delivery.acknowledgement,
                &event_bus,
            );
            let failure = DeliveryFailure::new(
                failure.delivery.delivered.id().to_string(),
                failure.delivery.delivered.topic().name().to_string(),
                subscription_id,
                subscriber_id,
                failure.error,
                failure.delivery.acknowledgement.is_acked(),
                dead_letter,
            );
            event_bus.inner.observe_delivery_failure(&failure);
        }
    }
}

/// Handles a terminal subscriber failure.
///
/// # Parameters
/// - `options`: Subscription options containing error handlers and DLQ policy.
/// - `subscriber_id`: Subscriber identifier.
/// - `delivered`: Delivered event envelope.
/// - `error`: Failure reason.
/// - `acknowledgement`: Shared acknowledgement state.
/// - `event_bus`: Bus used to publish dead-letter events.
fn handle_subscription_failure<T>(
    options: &SubscribeOptions<T>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    acknowledgement: &Acknowledgement,
    event_bus: &LocalEventBus,
) -> DeadLetterOutcome
where
    T: Clone + Send + Sync + 'static,
{
    for error in options.notify_subscribe_error(subscriber_id, delivered, error, acknowledgement) {
        event_bus.inner.observe_error(&error);
    }
    if !acknowledgement.is_completed() {
        acknowledgement.nack();
    }
    if acknowledgement.is_nacked() && !delivered.is_dead_letter() {
        let dead_letter = create_dead_letter_for_failure(options, subscriber_id, delivered, error, event_bus);
        let dead_letter = match dead_letter {
            DeadLetterCreation::NotConfigured => return DeadLetterOutcome::NotConfigured,
            DeadLetterCreation::Dropped => return DeadLetterOutcome::DroppedByStrategy,
            DeadLetterCreation::Failed(error) => return DeadLetterOutcome::Failed(error),
            DeadLetterCreation::Envelope(dead_letter) => dead_letter,
        };
        return match event_bus.publish_dead_letter_envelope(dead_letter.as_dead_letter()) {
            Ok(receipt) if receipt.has_rejections() => {
                let reason = match receipt.outcome() {
                    PublishOutcome::Dispatched(items) => items
                        .iter()
                        .find_map(|item| match item.status() {
                            DispatchStatus::Rejected(error) => Some(error.to_string()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "dead-letter subscriber admission was rejected".to_string()),
                    PublishOutcome::Dropped => "dead-letter was dropped by publisher interceptor".to_string(),
                };
                let observed = EventBusError::dead_letter_failed(reason);
                event_bus.inner.observe_error(&observed);
                DeadLetterOutcome::Rejected(receipt)
            }
            Ok(receipt) => DeadLetterOutcome::Publication(receipt),
            Err(error) => {
                let observed = EventBusError::dead_letter_failed(error.to_string());
                event_bus.inner.observe_error(&observed);
                DeadLetterOutcome::Failed(error)
            }
        };
    }
    DeadLetterOutcome::NotConfigured
}

enum DeadLetterCreation {
    NotConfigured,
    Dropped,
    Envelope(EventEnvelope<DeadLetterPayload>),
    Failed(EventBusError),
}

/// Creates a dead-letter envelope for a failed delivery.
///
/// # Parameters
/// - `options`: Subscription options containing the primary DLQ policy.
/// - `subscriber_id`: Subscriber identifier.
/// - `delivered`: Failed delivered event.
/// - `error`: Failure reason.
/// - `event_bus`: Bus containing optional factory defaults and observers.
///
/// # Returns
/// Dead-letter envelope when a strategy creates one.
fn create_dead_letter_for_failure<T>(
    options: &SubscribeOptions<T>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    event_bus: &LocalEventBus,
) -> DeadLetterCreation
where
    T: Clone + Send + Sync + 'static,
{
    if options.has_dead_letter_strategy() {
        match options.create_dead_letter(subscriber_id, delivered, error) {
            Ok(Some(dead_letter)) => DeadLetterCreation::Envelope(dead_letter),
            Ok(None) => DeadLetterCreation::Dropped,
            Err(error) => {
                event_bus.inner.observe_error(&error);
                DeadLetterCreation::Failed(error)
            }
        }
    } else {
        match create_default_dead_letter_for_failure(options, subscriber_id, delivered, error, event_bus) {
            Ok(Some(dead_letter)) => DeadLetterCreation::Envelope(dead_letter),
            Ok(None) => DeadLetterCreation::NotConfigured,
            Err(error) => DeadLetterCreation::Failed(error),
        }
    }
}

/// Creates a dead-letter envelope with the factory default strategy.
///
/// # Parameters
/// - `options`: Subscription options passed to the strategy.
/// - `subscriber_id`: Subscriber identifier.
/// - `delivered`: Failed delivered event.
/// - `error`: Failure reason.
/// - `event_bus`: Bus containing optional factory defaults and observers.
///
/// # Returns
/// Dead-letter envelope when the default strategy exists and creates one.
fn create_default_dead_letter_for_failure<T>(
    options: &SubscribeOptions<T>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    event_bus: &LocalEventBus,
) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
where
    T: Clone + Send + Sync + 'static,
{
    if let Some(strategy) = event_bus.inner.default_dead_letter_strategy::<T>() {
        return match call_dead_letter_strategy(strategy, subscriber_id, delivered, error, options) {
            Ok(dead_letter) => Ok(dead_letter),
            Err(error) => {
                event_bus.inner.observe_error(&error);
                Err(error)
            }
        };
    }
    let Some(strategy) = event_bus.inner.global_default_dead_letter_strategy() else {
        return Ok(None);
    };
    match call_global_dead_letter_strategy(
        strategy,
        subscriber_id,
        delivered.metadata(),
        Arc::new(delivered.payload().clone()),
        error,
    ) {
        Ok(dead_letter) => Ok(dead_letter),
        Err(error) => {
            event_bus
                .inner
                .observe_error(&EventBusError::dead_letter_failed(error.to_string()));
            Err(error)
        }
    }
}

/// Calls a dead-letter strategy while converting failures into event-bus
/// errors.
///
/// # Parameters
/// - `strategy`: Strategy to invoke.
/// - `subscriber_id`: Subscriber identifier.
/// - `delivered`: Failed delivered event.
/// - `error`: Failure reason.
/// - `options`: Subscription options.
///
/// # Returns
/// Dead-letter envelope produced by the strategy.
fn call_dead_letter_strategy<T>(
    strategy: Arc<DeadLetterStrategyFn<T>>,
    subscriber_id: &str,
    delivered: &EventEnvelope<T>,
    error: &EventBusError,
    options: &SubscribeOptions<T>,
) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
where
    T: Clone + Send + Sync + 'static,
{
    match panic::catch_unwind(AssertUnwindSafe(|| {
        strategy.create_dead_letter(subscriber_id, delivered, error, options)
    })) {
        Ok(Ok(dead_letter)) => Ok(dead_letter),
        Ok(Err(error)) => Err(normalize_dead_letter_error(error)),
        Err(_) => Err(EventBusError::dead_letter_failed(
            "default dead-letter strategy panicked",
        )),
    }
}

/// Calls a type-erased dead-letter strategy while normalizing failures.
///
/// # Parameters
/// - `strategy`: Strategy to invoke.
/// - `subscriber_id`: Subscriber identifier.
/// - `metadata`: Failed event metadata.
/// - `original_payload`: Type-erased cloned original payload.
/// - `error`: Failure reason.
///
/// # Returns
/// Dead-letter envelope produced by the strategy.
fn call_global_dead_letter_strategy(
    strategy: Arc<DeadLetterStrategyAnyFn>,
    subscriber_id: &str,
    metadata: EventEnvelopeMetadata,
    original_payload: DeadLetterOriginalPayload,
    error: &EventBusError,
) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>> {
    match panic::catch_unwind(AssertUnwindSafe(|| {
        strategy.create_dead_letter(subscriber_id, metadata, original_payload, error)
    })) {
        Ok(Ok(dead_letter)) => Ok(dead_letter),
        Ok(Err(error)) => Err(normalize_dead_letter_error(error)),
        Err(_) => Err(EventBusError::dead_letter_failed(
            "global default dead-letter strategy panicked",
        )),
    }
}

/// Runs a handler with retry options.
///
/// # Parameters
/// - `handler`: Subscriber handler.
/// - `options`: Subscriber options.
/// - `envelope`: Original event envelope.
///
/// # Returns
/// Successful attempt delivery, or the final handler error with its delivery.
fn run_handler_with_retry<T>(
    handler: &Arc<HandlerFn<T>>,
    options: &SubscribeOptions<T>,
    envelope: EventEnvelope<T>,
) -> Result<HandlerDelivery<T>, Box<HandlerRunFailure<T>>>
where
    T: Clone + Send + Sync + 'static,
{
    let mut last_delivery = None;
    match run_with_retry(
        options.retry_options(),
        options.retry_rule(),
        options.retry_cancellation_token(),
        || {
            let delivery = HandlerDelivery::new(&envelope);
            last_delivery = Some(delivery.clone());
            call_handler(handler, delivery.delivered.clone())?;
            if delivery.acknowledgement.is_nacked() {
                Err(EventBusError::handler_failed("subscriber nacked the event"))
            } else if options.ack_mode() == AckMode::Manual && !delivery.acknowledgement.is_acked() {
                Err(EventBusError::handler_failed("manual acknowledgement missing"))
            } else {
                Ok(delivery)
            }
        },
    ) {
        Ok(delivery) => Ok(delivery),
        Err(error) => {
            let delivery = match last_delivery {
                Some(delivery) => delivery,
                None => HandlerDelivery::new(&envelope),
            };
            Err(Box::new(HandlerRunFailure { error, delivery }))
        }
    }
}

/// Calls a subscriber handler while converting panics into handler errors.
///
/// # Parameters
/// - `handler`: Subscriber handler or interceptor chain.
/// - `envelope`: Envelope delivered to the handler.
///
/// # Returns
/// Handler result, with panics converted to [`EventBusError::HandlerPanicked`].
fn call_handler<T>(handler: &Arc<HandlerFn<T>>, envelope: EventEnvelope<T>) -> EventBusResult<()>
where
    T: Clone + Send + Sync + 'static,
{
    match panic::catch_unwind(AssertUnwindSafe(|| handler(envelope))) {
        Ok(result) => result,
        Err(_) => Err(EventBusError::handler_panicked()),
    }
}

/// Normalizes the result returned by a subscriber interceptor.
///
/// # Parameters
/// - `result`: Result of invoking the interceptor callback.
/// - `downstream_error`: Shared slot filled when the interceptor called
///   `proceed`.
/// - `panic_message`: Message used when the interceptor callback itself panics.
///
/// # Returns
/// Downstream failures unchanged, or interceptor failures wrapped as
/// [`EventBusError::InterceptorFailed`].
fn normalize_subscriber_interceptor_result(
    result: Result<EventBusResult<()>, Box<dyn Any + Send>>,
    downstream_error: &DownstreamErrorSlot,
    panic_message: &'static str,
) -> EventBusResult<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) if is_recorded_downstream_error(downstream_error, &error) => Err(error),
        Ok(Err(error)) => Err(normalize_subscriber_interceptor_error(error)),
        Err(_) => Err(EventBusError::interceptor_failed("subscribe", panic_message)),
    }
}

/// Converts an interceptor-owned error into the public interceptor failure
/// kind.
///
/// # Parameters
/// - `error`: Error returned directly by an interceptor callback.
///
/// # Returns
/// Existing subscribe interceptor failures are preserved; other errors are
/// wrapped with subscribe interceptor context.
fn normalize_subscriber_interceptor_error(error: EventBusError) -> EventBusError {
    if matches!(
        &error,
        EventBusError::InterceptorFailed { phase, .. } if *phase == "subscribe"
    ) {
        error
    } else {
        EventBusError::interceptor_failed("subscribe", error.to_string())
    }
}

/// Runs a fallible operation with the event-bus retry options.
///
/// # Parameters
/// - `retry_options`: Admission and backoff policy; absent means one direct
///   call.
/// - `retry_rule`: Optional application classification before the default rule.
/// - `cancellation`: Explicit subscriber token; ignored without a retry policy.
/// - `operation`: Operation to call for each attempt.
///
/// # Returns
/// Successful operation value or the final event-bus error. Backoff blocks this
/// thread and preserves the existing delivery/ACK/ordering lifecycle.
fn run_with_retry<T, F>(
    retry_options: Option<&RetryPolicy>,
    retry_rule: Option<&Arc<dyn RetryRule<EventBusError>>>,
    cancellation: Option<&RetryCancellationToken>,
    operation: F,
) -> EventBusResult<T>
where
    F: FnMut() -> EventBusResult<T>,
{
    let Some(retry_options) = retry_options else {
        let mut operation = operation;
        return operation();
    };
    let mut builder = RetryConfig::<EventBusError>::builder().policy((*retry_options).clone());
    if let Some(rule) = retry_rule {
        builder = builder.shared_rule(Arc::clone(rule));
    }
    let retry = builder
        .rule(EventBusRetryRule)
        .build()
        .expect("validated event-bus retry options should build");
    let mut execution = Retry::new(&retry);
    if let Some(token) = cancellation {
        execution = execution.cancellation_token(token.clone());
    }
    match execution.run(operation) {
        // This adapter registers no completion observers.
        Ok(value) => Ok(value.into_value_discarding_diagnostics()),
        Err(error) => Err(EventBusError::from(error)),
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
