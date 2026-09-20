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
use std::any::TypeId;
use std::any::type_name;
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
use qubit_argument::StringArgument;
use qubit_executor::ExecutorService;
use qubit_executor::SingleThreadScheduledExecutorService;
use qubit_retry::Retry;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryConfig;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;
use qubit_thread_pool::FixedThreadPool;
use subscription_entry::TypedSubscriptionEntry;

use super::erased_subscription::DispatchAdmission;
use super::local_event_bus_inner::LocalEventBusInner;
use super::local_event_bus_inner::LocalEventBusRuntimeOptions;
use super::publisher_interceptor_entry::PublisherInterceptorEntry;
use super::subscriber_interceptor_chain::DownstreamErrorSlot;
use super::subscriber_interceptor_chain::SubscriberInterceptorAnyChain;
use super::subscriber_interceptor_chain::SubscriberInterceptorChain;
use super::subscriber_interceptor_chain::create_downstream_error_slot;
use super::subscriber_interceptor_chain::is_recorded_downstream_error;
use super::subscriber_interceptor_entry::SubscriberInterceptorEntry;
use crate::AckMode;
use crate::Acknowledgement;
use crate::BatchPublishItem;
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
use crate::SubscriberDispatchResult;
use crate::Subscription;
use crate::Topic;
use crate::core::SubscriptionState;
use crate::core::delivery_limits::DeliveryLimits;
use crate::core::subscribe_options::DeadLetterStrategyAnyFn;
use crate::core::subscribe_options::DeadLetterStrategyFn;
use crate::core::subscribe_options::normalize_dead_letter_error;

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;
type PublisherInterceptorFn<T> = dyn PublisherInterceptor<T>;
type SubscriberInterceptorFn<T> = dyn SubscriberInterceptor<T>;

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

    /// Publishes a payload to a topic.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    ///
    /// # Returns
    /// Returns a [`PublishReceipt`] describing each matching subscriber.
    ///
    /// Local dispatch is non-transactional across matching subscribers. If
    /// scheduling fails for a later subscriber, earlier subscriber work may
    /// already have been accepted.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish<T>(&self, topic: &Topic<T>, payload: T) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_with_options(topic, payload, PublishOptions::empty())
    }

    /// Publishes a payload to a topic with explicit options.
    ///
    /// # Parameters
    /// - `topic`: Target topic.
    /// - `payload`: Event payload.
    /// - `options`: Publish options merged with factory defaults.
    ///
    /// # Returns
    /// Returns a [`PublishReceipt`] describing each matching subscriber.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish_with_options<T>(
        &self,
        topic: &Topic<T>,
        payload: T,
        options: PublishOptions<T>,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options(EventEnvelope::create(topic.clone(), payload), options)
    }

    /// Publishes an existing envelope.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to dispatch.
    ///
    /// # Returns
    /// Returns a [`PublishReceipt`] describing each matching subscriber.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish_envelope<T>(&self, envelope: EventEnvelope<T>) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_envelope_with_options(envelope, PublishOptions::empty())
    }

    /// Publishes an existing envelope with options.
    ///
    /// # Parameters
    /// - `envelope`: Event envelope to dispatch.
    /// - `options`: Publish options.
    ///
    /// # Returns
    /// `Ok(())` after subscriber work has been scheduled.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] if the bus is stopped.
    pub fn publish_envelope_with_options<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        let options = options.merge_defaults(self.default_publish_options::<T>());
        self.publish_envelope_with_options_internal(envelope, options, false, true)
    }

    /// Publishes an envelope through the local dispatch path.
    fn publish_envelope_with_options_internal<T>(
        &self,
        envelope: EventEnvelope<T>,
        options: PublishOptions<T>,
        allow_stopping: bool,
        require_started: bool,
    ) -> EventBusResult<PublishReceipt>
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Err(error) = self.ensure_started()
            && require_started
        {
            self.observe_errors(options.notify_publish_error(&envelope, &error));
            return Err(error);
        }
        let original_envelope = envelope.clone();
        let envelope = match run_with_retry(options.retry_options(), options.retry_rule(), None, || {
            self.apply_publisher_interceptors(original_envelope.clone())
        }) {
            Ok(Some(envelope)) => envelope,
            Ok(None) => {
                return Ok(PublishReceipt::new(
                    original_envelope.id().to_string(),
                    None,
                    PublishOutcome::Dropped,
                ));
            }
            Err(error) => {
                self.inner.observe_error(&error);
                self.observe_errors(options.notify_publish_error(&original_envelope, &error));
                return Err(error);
            }
        };
        let dispatch_results = self.dispatch_envelope(
            envelope.clone(),
            options.retry_options(),
            options.retry_rule(),
            allow_stopping,
        )?;
        let receipt = PublishReceipt::new(
            original_envelope.id().to_string(),
            Some(envelope.id().to_string()),
            PublishOutcome::Dispatched(dispatch_results),
        );
        if let PublishOutcome::Dispatched(items) = receipt.outcome()
            && let Some(error) = items.iter().find_map(|item| match item.status() {
                DispatchStatus::Rejected(error) => Some(error),
                _ => None,
            })
        {
            self.observe_errors(options.notify_publish_error(&envelope, error));
        }
        Ok(receipt)
    }

    /// Publishes a dead-letter envelope while graceful shutdown is draining.
    fn publish_dead_letter_envelope(
        &self,
        envelope: EventEnvelope<DeadLetterPayload>,
    ) -> EventBusResult<PublishReceipt> {
        let options = PublishOptions::empty().merge_defaults(self.default_publish_options::<DeadLetterPayload>());
        self.publish_envelope_with_options_internal(envelope, options, true, false)
    }

    /// Publishes multiple envelopes by submitting each envelope in input order.
    ///
    /// This method preserves submission order only. Handler execution order is
    /// backend-specific because local handlers can run on multiple worker
    /// threads.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to submit in order.
    ///
    /// # Returns
    /// Summary containing per-envelope successes and failures.
    ///
    /// # Errors
    /// Returns lifecycle or option validation errors before the batch starts.
    pub fn publish_all<T>(&self, envelopes: Vec<EventEnvelope<T>>) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.publish_all_with_options(envelopes, PublishOptions::empty())
    }

    /// Publishes multiple envelopes with explicit publish options.
    ///
    /// # Parameters
    /// - `envelopes`: Envelopes to submit in order.
    /// - `options`: Publish options cloned for each envelope.
    ///
    /// # Returns
    /// Summary containing per-envelope successes and failures.
    ///
    /// # Errors
    /// Returns lifecycle or option validation errors before the batch starts.
    pub fn publish_all_with_options<T>(
        &self,
        envelopes: Vec<EventEnvelope<T>>,
        options: PublishOptions<T>,
    ) -> EventBusResult<BatchPublishResult>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.ensure_started()?;
        let options = options.merge_defaults(self.default_publish_options::<T>());
        let mut items = Vec::with_capacity(envelopes.len());
        for (index, envelope) in envelopes.into_iter().enumerate() {
            let event_id = envelope.id().to_string();
            match self.publish_envelope_with_options_internal(envelope, options.clone(), false, true) {
                Ok(receipt) => items.push(BatchPublishItem::new(index, event_id, Ok(receipt))),
                Err(error) => items.push(BatchPublishItem::new(index, event_id, Err(error))),
            }
        }
        Ok(BatchPublishResult::new(items))
    }

    /// Subscribes a handler using default options.
    ///
    /// # Parameters
    /// - `subscriber_id`: Subscriber identifier.
    /// - `topic`: Topic to subscribe.
    /// - `handler`: Handler invoked for matching events.
    ///
    /// # Returns
    /// Subscription handle.
    ///
    /// # Errors
    /// Returns an error when the bus is stopped or shared state is unavailable.
    pub fn subscribe<T, S, F, R>(
        &self,
        subscriber_id: S,
        topic: &Topic<T>,
        handler: F,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.subscribe_with_options(subscriber_id, topic, handler, SubscribeOptions::empty())
    }

    /// Subscribes a handler using explicit options.
    ///
    /// # Parameters
    /// - `subscriber_id`: Subscriber identifier.
    /// - `topic`: Topic to subscribe.
    /// - `handler`: Handler invoked for matching events.
    /// - `options`: Subscription processing options.
    ///
    /// # Returns
    /// Subscription handle.
    ///
    /// # Errors
    /// Returns an error when the bus is stopped, the subscriber ID is blank, or
    /// shared state is unavailable.
    pub fn subscribe_with_options<T, S, F, R>(
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
        self.ensure_started()?;
        let options = options.merge_defaults(self.default_subscribe_options::<T>());
        let subscriber_id = subscriber_id
            .into()
            .require_non_blank("subscriber_id")
            .map_err(|_| EventBusError::invalid_argument("subscriber_id", "subscriber ID must not be blank"))?;

        let id = self.inner.next_subscription_id();
        let active = Arc::new(SubscriptionState::active());
        let topic_key = topic.key();
        let handler = Arc::new(move |event| handler(event).into_event_bus_result());
        let handler = self.apply_subscriber_interceptors(handler)?;
        let entry = TypedSubscriptionEntry {
            id,
            subscriber_id: subscriber_id.clone(),
            topic: topic.clone(),
            active: Arc::clone(&active),
            handler,
            options: options.clone(),
        };
        self.inner
            .register_subscription_if_started(topic_key.clone(), Arc::new(entry))?;

        Ok(Subscription {
            id,
            subscriber_id,
            topic: topic.clone(),
            topic_key,
            options,
            active,
            bus: Arc::downgrade(&self.inner),
        })
    }

    /// Subscribes a handler to a dead-letter topic.
    ///
    /// Dead-letter payloads are type-erased, so callers can inspect the
    /// original topic, error metadata, and original payload through
    /// [`DeadLetterPayload`].
    ///
    /// # Parameters
    /// - `dead_letter_topic`: Topic carrying dead-letter records.
    /// - `handler`: Handler invoked for dead-letter events.
    /// - `options`: Subscription options merged with factory defaults.
    ///
    /// # Returns
    /// Subscription handle for the dead-letter topic.
    ///
    /// # Errors
    /// Returns an error when the bus is stopped, the generated subscriber ID is
    /// invalid, or shared state is unavailable.
    pub fn add_dead_letter_handler<F, R>(
        &self,
        dead_letter_topic: &Topic<DeadLetterPayload>,
        handler: F,
        options: SubscribeOptions<DeadLetterPayload>,
    ) -> EventBusResult<Subscription<DeadLetterPayload>>
    where
        F: Fn(EventEnvelope<DeadLetterPayload>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.subscribe_with_options(
            format!("dead-letter:{}", dead_letter_topic.name()),
            dead_letter_topic,
            handler,
            options,
        )
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

    /// Returns default publish options for a payload type.
    ///
    /// # Returns
    /// Type-specific default options or empty options.
    fn default_publish_options<T>(&self) -> PublishOptions<T>
    where
        T: 'static,
    {
        self.inner
            .default_publish_options::<T>()
            .unwrap_or_else(PublishOptions::empty)
    }

    /// Returns default subscribe options for a payload type.
    ///
    /// # Returns
    /// Type-specific default options or empty options.
    fn default_subscribe_options<T>(&self) -> SubscribeOptions<T>
    where
        T: 'static,
    {
        self.inner
            .default_subscribe_options::<T>()
            .unwrap_or_else(SubscribeOptions::empty)
    }

    /// Ensures the event bus is started.
    ///
    /// # Returns
    /// `Ok(())` if started.
    ///
    /// # Errors
    /// Returns [`EventBusError::NotStarted`] when the bus is stopped.
    fn ensure_started(&self) -> EventBusResult<()> {
        if self.inner.is_started() {
            Ok(())
        } else {
            Err(EventBusError::not_started())
        }
    }

    /// Observes internal failures produced by user callbacks.
    ///
    /// # Parameters
    /// - `errors`: Callback failures to publish to registered error observers.
    fn observe_errors(&self, errors: Vec<EventBusError>) {
        for error in errors {
            self.inner.observe_error(&error);
        }
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

    /// Applies matching publisher interceptors.
    ///
    /// # Parameters
    /// - `envelope`: Original event envelope.
    ///
    /// # Returns
    /// Modified envelope, or `None` when an interceptor drops the event.
    ///
    /// # Errors
    /// Returns lock or type-erasure errors.
    fn apply_publisher_interceptors<T>(&self, envelope: EventEnvelope<T>) -> EventBusResult<Option<EventEnvelope<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut envelope = envelope;
        for interceptor in self.inner.global_publisher_interceptors()? {
            let metadata = envelope.metadata();
            let metadata = match panic::catch_unwind(AssertUnwindSafe(|| interceptor.on_publish(metadata))) {
                Ok(Ok(Some(metadata))) => metadata,
                Ok(Ok(None)) => return Ok(None),
                Ok(Err(error)) => {
                    return Err(EventBusError::interceptor_failed("publish", error.to_string()));
                }
                Err(_) => {
                    return Err(EventBusError::interceptor_failed(
                        "publish",
                        "global publisher interceptor panicked",
                    ));
                }
            };
            envelope.apply_metadata(metadata);
        }
        let interceptors = self.inner.publisher_interceptors()?;
        let mut current: Option<Box<dyn Any + Send>> = Some(Box::new(envelope));
        for interceptor in interceptors {
            if interceptor.payload_type_id() == TypeId::of::<T>()
                && let Some(boxed) = current.take()
            {
                current = interceptor.intercept(boxed)?;
            }
        }
        current
            .map(|boxed| {
                boxed
                    .downcast::<EventEnvelope<T>>()
                    .map(|envelope| *envelope)
                    .map_err(|_| EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown"))
            })
            .transpose()
    }

    /// Dispatches an envelope to currently registered subscribers.
    ///
    /// # Parameters
    /// - `envelope`: Envelope to dispatch.
    ///
    /// # Returns
    /// `Ok(())` once matching subscriber tasks have been accepted.
    ///
    /// This dispatch loop is best-effort across subscriptions: a later
    /// submission error does not roll back subscriber tasks accepted earlier in
    /// the same publish call.
    ///
    /// # Errors
    /// Returns subscription lookup, type-erasure, or executor submission
    /// errors.
    fn dispatch_envelope<T>(
        &self,
        envelope: EventEnvelope<T>,
        retry_options: Option<&RetryPolicy>,
        retry_rule: Option<&Arc<dyn RetryRule<EventBusError>>>,
        allow_stopping: bool,
    ) -> EventBusResult<Vec<SubscriberDispatchResult>>
    where
        T: Clone + Send + Sync + 'static,
    {
        if !allow_stopping {
            self.ensure_started()?;
        }
        let subscriptions = self.inner.subscriptions_for(&envelope.topic().key())?;
        let mut results = Vec::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            let subscription = Arc::clone(&subscription);
            let status = match run_with_retry(retry_options, retry_rule, None, || {
                subscription.dispatch(Box::new(envelope.clone()), Arc::clone(&self.inner), allow_stopping)
            }) {
                Ok(DispatchAdmission::Accepted) => DispatchStatus::Accepted,
                Ok(DispatchAdmission::Filtered) => DispatchStatus::Filtered,
                Err(error) => DispatchStatus::Rejected(error),
            };
            results.push(SubscriberDispatchResult::new(
                subscription.id(),
                subscription.subscriber_id().to_string(),
                status,
            ));
        }
        Ok(results)
    }

    /// Applies matching subscriber interceptors to a handler.
    ///
    /// # Parameters
    /// - `handler`: Original subscriber handler.
    ///
    /// # Returns
    /// Handler wrapped by registered subscriber interceptors.
    ///
    /// # Errors
    /// Returns lock or type-erasure errors.
    fn apply_subscriber_interceptors<T>(&self, handler: Arc<HandlerFn<T>>) -> EventBusResult<Arc<HandlerFn<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let interceptors = self.inner.subscriber_interceptors()?;
        let mut chain = Box::new(handler) as Box<dyn Any + Send + Sync>;
        for interceptor in interceptors.into_iter().rev() {
            if interceptor.payload_type_id() == TypeId::of::<T>() {
                chain = interceptor.wrap_handler(chain)?;
            }
        }
        let handler = chain
            .downcast::<Arc<HandlerFn<T>>>()
            .map(|handler| *handler)
            .map_err(|_| EventBusError::type_mismatch(type_name::<Arc<HandlerFn<T>>>(), "unknown"))?;
        self.apply_global_subscriber_interceptors(handler)
    }

    /// Applies global subscriber interceptors around a typed handler chain.
    ///
    /// # Parameters
    /// - `handler`: Typed handler chain after typed interceptors are applied.
    ///
    /// # Returns
    /// Handler wrapped by global subscriber interceptors.
    fn apply_global_subscriber_interceptors<T>(&self, handler: Arc<HandlerFn<T>>) -> EventBusResult<Arc<HandlerFn<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut chain = handler;
        for interceptor in self.inner.global_subscriber_interceptors()?.into_iter().rev() {
            let next = Arc::clone(&chain);
            chain = Arc::new(move |event: EventEnvelope<T>| {
                let metadata = event.metadata();
                let next = Arc::clone(&next);
                let event_for_next = event.clone();
                let downstream_error = create_downstream_error_slot();
                let chain = SubscriberInterceptorAnyChain::with_downstream_error(
                    Arc::new(move || next(event_for_next.clone())),
                    Arc::clone(&downstream_error),
                );
                let result = panic::catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(metadata, chain)));
                normalize_subscriber_interceptor_result(
                    result,
                    &downstream_error,
                    "global subscriber interceptor panicked",
                )
            });
        }
        Ok(chain)
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

/// Typed publisher interceptor adapter.
struct TypedPublisherInterceptor<T: Clone + Send + Sync + 'static> {
    interceptor: Arc<PublisherInterceptorFn<T>>,
}

/// Creates a type-erased publisher interceptor entry.
///
/// # Parameters
/// - `interceptor`: Typed publisher interceptor callback.
///
/// # Returns
/// Type-erased entry suitable for local bus storage.
pub(super) fn create_publisher_interceptor_entry<T, I>(interceptor: I) -> Arc<dyn PublisherInterceptorEntry>
where
    T: Clone + Send + Sync + 'static,
    I: PublisherInterceptor<T>,
{
    Arc::new(TypedPublisherInterceptor::<T> {
        interceptor: Arc::new(interceptor),
    })
}

impl<T> PublisherInterceptorEntry for TypedPublisherInterceptor<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the payload [`TypeId`] handled by this interceptor.
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    /// Downcasts and applies the typed interceptor.
    fn intercept(&self, envelope: Box<dyn Any + Send>) -> EventBusResult<Option<Box<dyn Any + Send>>> {
        let envelope = envelope
            .downcast::<EventEnvelope<T>>()
            .map_err(|_| EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown"))?;
        match panic::catch_unwind(AssertUnwindSafe(|| self.interceptor.on_publish(*envelope))) {
            Ok(Ok(envelope)) => Ok(envelope.map(|envelope| Box::new(envelope) as Box<dyn Any + Send>)),
            Ok(Err(error)) => Err(EventBusError::interceptor_failed("publish", error.to_string())),
            Err(_) => Err(EventBusError::interceptor_failed(
                "publish",
                "publisher interceptor panicked",
            )),
        }
    }
}

/// Typed subscriber interceptor adapter.
struct TypedSubscriberInterceptor<T: Clone + Send + Sync + 'static> {
    interceptor: Arc<SubscriberInterceptorFn<T>>,
}

/// Creates a type-erased subscriber interceptor entry.
///
/// # Parameters
/// - `interceptor`: Typed subscriber interceptor callback.
///
/// # Returns
/// Type-erased entry suitable for local bus storage.
pub(super) fn create_subscriber_interceptor_entry<T, I>(interceptor: I) -> Arc<dyn SubscriberInterceptorEntry>
where
    T: Clone + Send + Sync + 'static,
    I: SubscriberInterceptor<T>,
{
    Arc::new(TypedSubscriberInterceptor::<T> {
        interceptor: Arc::new(interceptor),
    })
}

impl<T> SubscriberInterceptorEntry for TypedSubscriberInterceptor<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the payload [`TypeId`] handled by this interceptor.
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    /// Downcasts and wraps the typed handler.
    fn wrap_handler(&self, handler: Box<dyn Any + Send + Sync>) -> EventBusResult<Box<dyn Any + Send + Sync>> {
        let next = handler
            .downcast::<Arc<HandlerFn<T>>>()
            .map_err(|_| EventBusError::type_mismatch(type_name::<Arc<HandlerFn<T>>>(), "unknown"))?;
        let next = *next;
        let interceptor = Arc::clone(&self.interceptor);
        let wrapped: Arc<HandlerFn<T>> = Arc::new(move |event| {
            let downstream_error = create_downstream_error_slot();
            let next_chain =
                SubscriberInterceptorChain::with_downstream_error(Arc::clone(&next), Arc::clone(&downstream_error));
            let result = panic::catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(event, next_chain)));
            normalize_subscriber_interceptor_result(result, &downstream_error, "subscriber interceptor panicked")
        });
        Ok(Box::new(wrapped))
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
