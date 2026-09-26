// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Runtime-neutral asynchronous event-bus facade.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use qubit_clock::MonotonicClock;
use qubit_clock::StdMonotonicClock;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_id::Id;

use super::AsyncSubscription;
use super::DiagnosticObserverHandle;
use super::EventBusFacadeConfig;
use super::PublishMetrics;
use super::PublishMetricsSnapshot;
use super::WaitOutcome;
use super::async_subscription::is_current_bus_poll;
use super::observer_entry::ObserverEntry;
use crate::codec::resolve_codec;
use crate::error::CapabilityError;
use crate::error::LifecycleError;
use crate::error::PublishError;
use crate::error::ShutdownError;
use crate::error::SubscribeError;
use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;
use crate::local::LocalEventBusConfig;
use crate::model::BatchPublishResult;
use crate::model::ProviderId;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::model::SubscribeRequest;
use crate::pipeline::AsyncOrderingLanes;
use crate::pipeline::Diagnostic;
use crate::pipeline::DiagnosticObserver;
use crate::pipeline::PipelineFailure;
use crate::pipeline::PublisherPipeline;
use crate::registry::AsyncEventBusRegistry;
use crate::registry::EventBusConfig;
use crate::spi::AsyncEventBusSpi;
use crate::spi::PayloadModes;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;

/// A cloneable runtime-neutral asynchronous facade over one provider SPI.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::{SubscribeRequest, Topic};
/// use qubit_event_bus::{AsyncEventBus, DeliveryError};
///
/// async fn consume(bus: &AsyncEventBus) -> Result<(), Box<dyn std::error::Error>> {
///     let topic = Topic::<String>::new("orders.created")?;
///     let request = SubscribeRequest::new("audit", topic)?;
///     let mut subscription = bus.subscribe(request).await?;
///     subscription.run(|delivery| async move {
///         audit(delivery.payload()).await?;
///         Ok::<(), DeliveryError>(())
///     }).await?;
///     Ok(())
/// }
///
/// async fn audit(_order: &str) -> Result<(), DeliveryError> { Ok(()) }
/// ```
///
/// The caller supplies a facade created by an async provider and chooses how
/// to drive this function. The event bus does not spawn a runtime task.
#[derive(Clone)]
pub struct AsyncEventBus {
    pub(super) inner: Arc<AsyncEventBusInner>,
}

pub(super) struct AsyncEventBusInner {
    pub(super) spi: Arc<dyn AsyncEventBusSpi>,
    pub(super) provider_id: ProviderId,
    pub(super) publisher: PublisherPipeline,
    pub(super) facade_config: EventBusFacadeConfig,
    pub(super) state: Mutex<BusState>,
    shutdown_active: AtomicBool,
    shutdown_signal: AsyncSignal,
    shutdown_outcome: Mutex<Option<ShutdownOutcome>>,
    pub(super) next_subscription_id: AtomicU64,
    pub(super) controls: Mutex<HashMap<Id, Arc<dyn AsyncShutdownDriver>>>,
    close_errors: Mutex<Vec<Arc<SubscriptionCloseFailure>>>,
    close_error_snapshot: Mutex<Option<Arc<SubscriptionCloseErrors>>>,
    pub(super) observers: Mutex<Vec<Weak<ObserverEntry>>>,
    pub(super) tracker: Arc<AsyncTracker>,
    pub(super) ordering_lanes: AsyncOrderingLanes<()>,
    pub(super) admission: Arc<AsyncAdmission>,
    pub(super) timer: Arc<dyn Timer>,
    publish_metrics: PublishMetrics,
}

/// Bus-wide asynchronous delivery admission. Waiters are woken whenever a
/// delivery releases its permit; cancellation removes the waiter by RAII.
pub(super) struct AsyncAdmission {
    limit: usize,
    state: Mutex<AsyncAdmissionState>,
    next_waiter: AtomicU64,
}

#[derive(Default)]
struct AsyncAdmissionState {
    in_flight: usize,
    waiters: VecDeque<(u64, Waker)>,
}

impl AsyncAdmission {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(AsyncAdmissionState::default()),
            next_waiter: AtomicU64::new(1),
        })
    }

    /// Returns a future that waits for the next bus-wide delivery slot.
    pub(super) fn acquire(self: &Arc<Self>) -> AsyncAdmissionFuture {
        AsyncAdmissionFuture {
            admission: self.clone(),
            waiter_id: self.next_waiter.fetch_add(1, Ordering::Relaxed),
            queued: false,
        }
    }
}

/// Cancellation-safe future for acquiring an async delivery slot.
pub(super) struct AsyncAdmissionFuture {
    admission: Arc<AsyncAdmission>,
    waiter_id: u64,
    queued: bool,
}

impl Future for AsyncAdmissionFuture {
    type Output = AsyncAdmissionPermit;

    fn poll(mut self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        let mut state = this
            .admission
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !this.queued {
            state.waiters.push_back((this.waiter_id, context.waker().clone()));
            this.queued = true;
        } else if let Some((_, waker)) = state.waiters.iter_mut().find(|(id, _)| *id == this.waiter_id)
            && !waker.will_wake(context.waker())
        {
            *waker = context.waker().clone();
        }
        let is_head = state.waiters.front().is_some_and(|(id, _)| *id == this.waiter_id);
        if is_head && state.in_flight < this.admission.limit {
            state.waiters.pop_front();
            state.in_flight += 1;
            this.queued = false;
            return Poll::Ready(AsyncAdmissionPermit {
                admission: this.admission.clone(),
            });
        }
        Poll::Pending
    }
}

impl Drop for AsyncAdmissionFuture {
    fn drop(&mut self) {
        if self.queued {
            let waker = {
                let mut state = self
                    .admission
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let was_head = state.waiters.front().is_some_and(|(id, _)| *id == self.waiter_id);
                state.waiters.retain(|(id, _)| *id != self.waiter_id);
                (was_head && state.in_flight < self.admission.limit)
                    .then(|| state.waiters.front().map(|(_, waker)| waker.clone()))
                    .flatten()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

/// RAII permit covering one received delivery through terminal settlement.
pub(super) struct AsyncAdmissionPermit {
    admission: Arc<AsyncAdmission>,
}

impl Drop for AsyncAdmissionPermit {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self
                .admission
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.in_flight = state.in_flight.saturating_sub(1);
            state
                .waiters
                .front()
                .map(|(_, waker)| waker.clone())
                .into_iter()
                .collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum BusState {
    Running,
    Closing,
    Closed,
}

pub(super) trait AsyncShutdownDriver: Send + Sync {
    fn stop(&self, mode: ShutdownMode);
    fn close_error(&self) -> Option<Arc<SubscriptionCloseFailure>>;
    fn store_close_error(&self, failure: Arc<SubscriptionCloseFailure>) -> Arc<SubscriptionCloseFailure>;
    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> Pin<Box<dyn Future<Output = Result<(), Arc<SubscriptionCloseFailure>>> + Send + 'a>>;
}

#[derive(Default)]
pub(super) struct AsyncSignal {
    wakers: Mutex<HashMap<u64, std::task::Waker>>,
    next_waiter: AtomicU64,
}

impl AsyncSignal {
    pub(super) fn notify(&self) {
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for (_, waker) in wakers {
            waker.wake();
        }
    }
    fn register_waiter(&self, id: u64, waker: &std::task::Waker) {
        self.wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, waker.clone());
    }
    fn unregister(&self, id: u64) {
        self.wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }
}

#[derive(Default)]
pub(super) struct AsyncTracker {
    state: Mutex<TrackerState>,
    signal: AsyncSignal,
}

#[derive(Default)]
struct TrackerState {
    active_runners: usize,
    active_publishes: usize,
    active_subscribes: usize,
    active_closes: usize,
    in_flight: HashMap<Box<str>, usize>,
}

impl AsyncTracker {
    pub(super) fn close_started(self: &Arc<Self>) -> AsyncCloseGuard {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_closes += 1;
        AsyncCloseGuard(self.clone())
    }
    pub(super) fn runner_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_runners += 1;
    }
    pub(super) fn runner_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_runners = state.active_runners.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }
    fn publish_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_publishes += 1;
    }
    fn publish_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_publishes = state.active_publishes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }
    fn subscribe_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_subscribes += 1;
    }
    fn subscribe_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_subscribes = state.active_subscribes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }
    fn close_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_closes = state.active_closes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }
    pub(super) fn track(self: &Arc<Self>, topic: &str) -> AsyncDeliveryGuard {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
            .entry(topic.into())
            .or_default() += 1;
        AsyncDeliveryGuard {
            tracker: self.clone(),
            topic: topic.into(),
        }
    }
    fn is_idle(&self, topic: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
            .get(topic)
            .copied()
            .unwrap_or(0)
            == 0
    }
    fn runners_stopped(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_runners == 0
            && state.active_publishes == 0
            && state.active_subscribes == 0
            && state.active_closes == 0
    }
}

struct AsyncPublishGuard(Arc<AsyncTracker>);
impl Drop for AsyncPublishGuard {
    fn drop(&mut self) {
        self.0.publish_finished();
    }
}

struct AsyncSubscribeGuard(Arc<AsyncTracker>);
impl Drop for AsyncSubscribeGuard {
    fn drop(&mut self) {
        self.0.subscribe_finished();
    }
}

pub(super) struct AsyncRunnerGuard(Arc<AsyncTracker>);
impl AsyncRunnerGuard {
    pub(super) fn enter(tracker: Arc<AsyncTracker>) -> Self {
        tracker.runner_started();
        Self(tracker)
    }
}
impl Drop for AsyncRunnerGuard {
    fn drop(&mut self) {
        self.0.runner_finished();
    }
}

pub(super) struct AsyncCloseGuard(Arc<AsyncTracker>);
impl Drop for AsyncCloseGuard {
    fn drop(&mut self) {
        self.0.close_finished();
    }
}

pub(super) struct AsyncDeliveryGuard {
    tracker: Arc<AsyncTracker>,
    topic: Box<str>,
}
impl Drop for AsyncDeliveryGuard {
    fn drop(&mut self) {
        let mut state = self
            .tracker
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = state.in_flight.get_mut(self.topic.as_ref()) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.in_flight.remove(self.topic.as_ref());
            }
        }
        drop(state);
        self.tracker.signal.notify();
    }
}

impl AsyncEventBus {
    /// Asynchronously creates a facade using the built-in local provider.
    pub async fn local(config: LocalEventBusConfig) -> Result<Self, crate::error::ProviderError> {
        let registry =
            AsyncEventBusRegistry::with_local().map_err(|source| crate::error::ProviderError::Resolution {
                source: Box::new(source),
            })?;
        registry
            .create(&EventBusConfig::default().with_provider_options(config.provider_options()))
            .await
    }

    /// Creates a usable facade around an already-created asynchronous provider
    /// SPI.
    pub fn from_spi(provider_id: ProviderId, spi: Arc<dyn AsyncEventBusSpi>) -> Self {
        Self::with_config(provider_id, spi, EventBusFacadeConfig::default())
    }

    /// Creates a facade using application-supplied codec registrations.
    ///
    /// The codec registry is frozen into the publisher pipeline at
    /// construction; later changes require constructing a new facade.
    pub fn with_config(provider_id: ProviderId, spi: Arc<dyn AsyncEventBusSpi>, config: EventBusFacadeConfig) -> Self {
        Self::with_config_and_timer(provider_id, spi, config, StdMonotonicClock::new().new_timer())
    }

    /// Creates a facade using the supplied runtime-neutral timer for deadlines
    /// and asynchronous retry delays.
    pub fn with_timer(provider_id: ProviderId, spi: Arc<dyn AsyncEventBusSpi>, timer: Arc<dyn Timer>) -> Self {
        Self::with_config_and_timer(provider_id, spi, EventBusFacadeConfig::default(), timer)
    }

    /// Creates a facade with custom codec registrations and timer.
    pub fn with_config_and_timer(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        config: EventBusFacadeConfig,
        timer: Arc<dyn Timer>,
    ) -> Self {
        let admission_limit = config.delivery_admission().max_in_flight();
        Self {
            inner: Arc::new(AsyncEventBusInner {
                spi,
                provider_id: provider_id.clone(),
                publisher: PublisherPipeline::new(provider_id, config.codec_registry().clone()),
                facade_config: config,
                state: Mutex::new(BusState::Running),
                next_subscription_id: AtomicU64::new(1),
                controls: Mutex::new(HashMap::new()),
                close_errors: Mutex::new(Vec::new()),
                close_error_snapshot: Mutex::new(None),
                shutdown_active: AtomicBool::new(false),
                shutdown_signal: AsyncSignal::default(),
                shutdown_outcome: Mutex::new(None),
                observers: Mutex::new(Vec::new()),
                tracker: Arc::new(AsyncTracker::default()),
                ordering_lanes: AsyncOrderingLanes::new(),
                admission: AsyncAdmission::new(admission_limit),
                timer,
                publish_metrics: PublishMetrics::default(),
            }),
        }
    }

    /// Publishes one typed request through interceptors, retry, and provider
    /// SPI.
    pub async fn publish<T: Send + Sync + 'static>(
        &self,
        request: PublishRequest<T>,
    ) -> Result<PublishReceipt, PublishError> {
        // This body begins on the first poll, so a cancelled in-flight future
        // still contributes an attempt without recording a fabricated outcome.
        self.inner.publish_metrics.record_attempt();
        let Some(_publish) = self.inner.begin_publish() else {
            self.inner.publish_metrics.record_error();
            return Err(PublishError::Closed);
        };
        let observers = self.observer_snapshot();
        self.inner
            .publisher
            .publish_async(
                self.inner.spi.as_ref(),
                request,
                self.inner.facade_config.global_publisher_interceptors(),
                &observers,
                self.inner.timer.clone(),
            )
            .await
            .map_err(|failure| {
                self.inner.publish_metrics.record_error();
                publish_pipeline_error(failure)
            })
            .inspect(|receipt| {
                self.inner.publish_metrics.record_receipt(receipt);
            })
    }

    /// Returns the shared publication counters for this facade and its clones.
    #[must_use]
    pub fn publish_metrics(&self) -> PublishMetricsSnapshot {
        self.inner.publish_metrics.snapshot()
    }

    /// Publishes each request in order and retains each independent result.
    pub async fn publish_all<T, I>(&self, requests: I) -> BatchPublishResult
    where
        T: Send + Sync + 'static,
        I: IntoIterator<Item = PublishRequest<T>>,
    {
        let mut results = Vec::new();
        for request in requests {
            results.push(self.publish(request).await);
        }
        BatchPublishResult::new(results)
    }

    /// Creates an asynchronous provider subscription without spawning a task.
    /// Until [`AsyncSubscription::run`] starts, the facade retains ownership
    /// of its provider receiver so [`Self::shutdown`] can close it even if the
    /// returned subscription is never run. Unsupported acknowledgement,
    /// ordering, durability, consumer-group, replay, and codec requirements
    /// return a capability error before provider subscription.
    pub async fn subscribe<T: Send + Sync + 'static>(
        &self,
        request: SubscribeRequest<T>,
    ) -> Result<AsyncSubscription<T>, SubscribeError> {
        let _subscribe = self.inner.begin_subscribe().ok_or(SubscribeError::Closed)?;
        let (subscriber_id, topic, options) = request.into_parts();
        if !options.interceptors().is_empty() || self.inner.facade_config.has_sync_subscriber_interceptors::<T>() {
            return Err(SubscribeError::Configuration(
                crate::error::ConfigurationError::InvalidField {
                    field: "sync_subscriber_interceptor",
                    message: "AsyncEventBus requires async subscriber middleware".into(),
                },
            ));
        }
        let capabilities = self.inner.spi.capabilities();
        let codec = resolve_codec(&topic, self.inner.facade_config.codec_registry());
        crate::pipeline::SubscriberPipeline::validate_ack_capability(options.ack_mode(), capabilities.settlement())?;
        if options.ordering_policy() == crate::model::OrderingPolicy::PerKey
            && !capabilities.ordering().supports_per_key()
        {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "ordering.per_key",
            }));
        }
        if capabilities.payload_modes() == PayloadModes::Encoded && codec.is_none() {
            return Err(SubscribeError::Capability(CapabilityError::CodecRequired));
        }
        crate::pipeline::SubscriberPipeline::validate_subscription_capabilities(&options, capabilities)?;
        let raw_id = self.inner.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let id = Id::new(raw_id);
        let spi_request = SpiSubscriptionRequest::new(
            id,
            TopicAddress::new(topic.name())?,
            subscriber_id.clone(),
            options.consumer_group().cloned(),
            options.durability(),
            options.start_position().clone(),
            options.provider_options().clone(),
            topic.payload_type_id(),
        );
        let subscribe =
            std::panic::catch_unwind(AssertUnwindSafe(|| self.inner.spi.subscribe(spi_request))).map_err(|panic| {
                provider_panic(
                    &self.inner.provider_id,
                    "subscribe",
                    Some(subscriber_id.as_str()),
                    panic,
                )
            })?;
        let receiver = catch_spi_future(
            subscribe,
            &self.inner.provider_id,
            "subscribe",
            Some(subscriber_id.as_str()),
        )
        .await?;
        let (subscription, control) = AsyncSubscription::new(
            self.inner.clone(),
            id,
            subscriber_id.clone(),
            topic,
            codec,
            options,
            receiver,
        );
        let admitted = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *state != BusState::Running {
                false
            } else {
                self.inner
                    .controls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(id, control.clone());
                true
            }
        };
        if !admitted {
            control.stop(ShutdownMode::Immediate);
            let _ = control.shutdown(ShutdownMode::Immediate).await;
            return Err(SubscribeError::Closed);
        }
        Ok(subscription)
    }

    /// Waits until this facade has no delivery it has already received for the
    /// selected topic.
    ///
    /// This does not query the provider's queue and does not establish global
    /// idleness on a remote broker.
    pub async fn wait_for_received_deliveries<T: 'static>(
        &self,
        topic: &crate::model::Topic<T>,
        timeout: Option<Duration>,
    ) -> Result<WaitOutcome, LifecycleError> {
        wait_until(&self.inner.tracker.signal, self.inner.timer.as_ref(), timeout, || {
            self.inner.tracker.is_idle(topic.name())
        })
        .await
    }

    /// Registers a synchronous diagnostic observer until the returned handle is
    /// dropped.
    pub fn observe_diagnostics<F>(&self, observer: F) -> DiagnosticObserverHandle
    where
        F: Fn(&Diagnostic) + Send + Sync + 'static,
    {
        let entry = Arc::new(ObserverEntry {
            active: AtomicBool::new(true),
            callback: Arc::new(observer),
        });
        let mut entries = self
            .inner
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|entry| entry.strong_count() > 0);
        entries.push(Arc::downgrade(&entry));
        DiagnosticObserverHandle::new(entry)
    }

    pub(super) fn observer_snapshot(&self) -> Vec<Arc<DiagnosticObserver>> {
        self.inner.observer_snapshot()
    }

    /// Stops admission, completes active runners, and closes the provider.
    ///
    /// Immediate shutdown requests active runners to stop receiving but waits
    /// for an already-running handler, its settlement, and receiver close
    /// before shutting down the provider. Receivers for subscriptions that
    /// have not started are closed directly by shutdown. It cannot forcibly
    /// cancel user code and may therefore wait indefinitely. Graceful shutdown
    /// applies one deadline to receiver close, runner and publish completion,
    /// and provider shutdown. A timeout leaves the bus closing so the caller
    /// may retry cleanup, including with [`ShutdownMode::Immediate`].
    ///
    /// Directly awaiting shutdown from a handler or middleware Future running
    /// on this bus returns [`LifecycleError::WouldDeadlock`]. This detection is
    /// scoped to each poll of the caller-driven runner Future; tasks the
    /// application independently spawns are outside that scope and must not
    /// await a shutdown that includes their originating handler.
    pub async fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, ShutdownError> {
        let bus_key = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_poll(bus_key) {
            return Err(LifecycleError::WouldDeadlock { operation: "shutdown" }.into());
        }
        let timeout = match mode {
            ShutdownMode::Graceful { timeout } => Some(timeout),
            ShutdownMode::Immediate => None,
        };
        let mut deadline = None;
        loop {
            let is_closed = {
                *self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    == BusState::Closed
            };
            if is_closed {
                if let Some(errors) = self.inner.close_errors_snapshot() {
                    return Err(ShutdownError::SubscriptionClose(errors));
                }
                return Ok(self
                    .inner
                    .shutdown_outcome
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .unwrap_or(ShutdownOutcome::Complete));
            }
            if deadline.is_none() {
                deadline = timeout
                    .map(|timeout| self.inner.timer.after(timeout).map_err(LifecycleError::from))
                    .transpose()?;
            }
            if self
                .inner
                .shutdown_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let _leader = ShutdownLeaderGuard(self.inner.clone());
                *self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = BusState::Closing;
                for control in self
                    .inner
                    .controls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                {
                    control.stop(mode);
                }
                let close = self.close_unstarted_subscriptions(mode);
                if await_until_deadline(close, deadline.as_mut()).await?.is_none() {
                    return Err(ShutdownError::TimedOut {
                        timeout: timeout.expect("a deadline exists for graceful shutdown"),
                    });
                }
                let stopped = wait_until_deadline(&self.inner.tracker.signal, deadline.as_mut(), || {
                    self.inner.tracker.runners_stopped()
                })
                .await?;
                if stopped == WaitOutcome::TimedOut {
                    return Err(ShutdownError::TimedOut {
                        timeout: timeout.expect("a deadline exists for graceful shutdown"),
                    });
                }
                let shutdown = std::panic::catch_unwind(AssertUnwindSafe(|| self.inner.spi.shutdown(mode)))
                    .map_err(|panic| provider_panic(&self.inner.provider_id, "shutdown", None, panic))?;
                let shutdown = catch_spi_future(shutdown, &self.inner.provider_id, "shutdown", None);
                let Some(outcome) = await_until_deadline(shutdown, deadline.as_mut()).await? else {
                    return Err(ShutdownError::TimedOut {
                        timeout: timeout.expect("a deadline exists for graceful shutdown"),
                    });
                };
                let outcome = outcome?;
                *self
                    .inner
                    .shutdown_outcome
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outcome);
                *self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = BusState::Closed;
                if let Some(errors) = self.inner.close_errors_snapshot() {
                    return Err(ShutdownError::SubscriptionClose(errors));
                }
                return Ok(outcome);
            }
            let stopped = wait_until_deadline(&self.inner.shutdown_signal, deadline.as_mut(), || {
                !self.inner.shutdown_active.load(Ordering::Acquire)
            })
            .await?;
            if stopped == WaitOutcome::TimedOut {
                return Err(ShutdownError::TimedOut {
                    timeout: timeout.expect("a deadline exists for graceful shutdown"),
                });
            }
        }
    }

    /// Closes provider receivers that have not yet been transferred to a
    /// runner.
    async fn close_unstarted_subscriptions(&self, mode: ShutdownMode) {
        let controls: Vec<_> = self
            .inner
            .controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(id, control)| (*id, control.clone()))
            .collect();
        for (id, control) in controls {
            if let Err(failure) = control.shutdown(mode).await {
                self.inner.record_close_failure(failure);
                continue;
            }
            self.inner
                .controls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
        }
    }
}

struct ShutdownLeaderGuard(Arc<AsyncEventBusInner>);
impl Drop for ShutdownLeaderGuard {
    fn drop(&mut self) {
        self.0.shutdown_active.store(false, Ordering::Release);
        self.0.shutdown_signal.notify();
    }
}

pub(super) async fn catch_spi_future<F, T>(
    future: F,
    provider: &ProviderId,
    operation: &'static str,
    resource: Option<&str>,
) -> Result<T, crate::error::SpiError>
where
    F: Future<Output = Result<T, crate::error::SpiError>> + Send,
{
    match (CatchSpiFuture {
        future: Box::pin(future),
    })
    .await
    {
        Ok(result) => result,
        Err(panic) => Err(provider_panic(provider, operation, resource, panic)),
    }
}

struct CatchSpiFuture<F: Future> {
    future: Pin<Box<F>>,
}
impl<F: Future> Future for CatchSpiFuture<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;
    fn poll(self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(context))) {
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(panic) => std::task::Poll::Ready(Err(panic)),
        }
    }
}

pub(super) fn provider_panic(
    provider: &ProviderId,
    operation: &'static str,
    resource: Option<&str>,
    panic: Box<dyn std::any::Any + Send>,
) -> crate::error::SpiError {
    let message = panic
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("provider panicked");
    crate::error::SpiError::Operation {
        provider_id: provider.as_str().into(),
        operation,
        resource: resource.map(Into::into),
        kind: "provider_panicked",
        retryable: None,
        source: Box::new(std::io::Error::other(message)),
    }
}

impl AsyncEventBusInner {
    fn begin_publish(self: &Arc<Self>) -> Option<AsyncPublishGuard> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state != BusState::Running {
            return None;
        }
        self.tracker.publish_started();
        drop(state);
        Some(AsyncPublishGuard(self.tracker.clone()))
    }
    fn begin_subscribe(self: &Arc<Self>) -> Option<AsyncSubscribeGuard> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state != BusState::Running {
            return None;
        }
        self.tracker.subscribe_started();
        drop(state);
        Some(AsyncSubscribeGuard(self.tracker.clone()))
    }
    pub(super) fn observer_snapshot(&self) -> Vec<Arc<DiagnosticObserver>> {
        let mut observers = self.observers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        observers.retain(|entry| entry.strong_count() > 0);
        observers
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|entry| entry.active.load(Ordering::Acquire))
            .map(|entry| entry.callback.clone())
            .collect()
    }
    pub(super) fn emit(&self, diagnostic: &Diagnostic) {
        crate::pipeline::emit_diagnostic(&self.observer_snapshot(), diagnostic);
    }

    pub(super) fn record_close_error(
        &self,
        control: &dyn AsyncShutdownDriver,
        subscriber_id: &crate::model::SubscriberId,
        error: crate::error::SpiError,
    ) -> Arc<SubscriptionCloseFailure> {
        if let Some(failure) = control.close_error() {
            return failure.clone();
        }
        let failure = Arc::new(SubscriptionCloseFailure::new(subscriber_id.clone(), error));
        let failure = control.store_close_error(failure);
        self.close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(failure.clone());
        *self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        failure
    }

    pub(super) fn record_close_failure(&self, failure: Arc<SubscriptionCloseFailure>) {
        self.close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(failure);
        *self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    fn close_errors_snapshot(&self) -> Option<Arc<SubscriptionCloseErrors>> {
        let failures = self
            .close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut snapshot = self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(errors) = snapshot.as_ref() {
            return Some(errors.clone());
        }
        if failures.is_empty() {
            return None;
        }
        let errors = Arc::new(SubscriptionCloseErrors::from_failures(failures.clone()));
        *snapshot = Some(errors.clone());
        Some(errors)
    }
}

fn publish_pipeline_error(failure: PipelineFailure) -> PublishError {
    match failure.into_error() {
        crate::error::EventBusError::Configuration(error) => PublishError::Configuration(error),
        crate::error::EventBusError::Capability(error) => PublishError::Capability(error),
        crate::error::EventBusError::Codec(error) => PublishError::Codec(error),
        crate::error::EventBusError::Publish(error) => error,
        other => PublishError::Configuration(crate::error::ConfigurationError::InvalidField {
            field: "publish_pipeline",
            message: other.to_string().into(),
        }),
    }
}

async fn wait_until(
    signal: &AsyncSignal,
    timer: &dyn Timer,
    timeout: Option<Duration>,
    mut ready: impl FnMut() -> bool,
) -> Result<WaitOutcome, LifecycleError> {
    let mut deadline = match timeout {
        Some(duration) => Some(timer.after(duration)?),
        None => None,
    };
    let registration = SignalRegistration::new(signal);
    std::future::poll_fn(|cx| {
        if ready() {
            return std::task::Poll::Ready(Ok(WaitOutcome::Idle));
        }
        if let Some(deadline) = deadline.as_mut() {
            match deadline.as_mut().poll(cx) {
                std::task::Poll::Ready(Ok(())) => {
                    return std::task::Poll::Ready(Ok(WaitOutcome::TimedOut));
                }
                std::task::Poll::Ready(Err(error)) => {
                    return std::task::Poll::Ready(Err(LifecycleError::Timer(error)));
                }
                std::task::Poll::Pending => {}
            }
        }
        registration.register(cx.waker());
        if ready() {
            return std::task::Poll::Ready(Ok(WaitOutcome::Idle));
        }
        std::task::Poll::Pending
    })
    .await
}

/// Awaits a future until it completes or the shared optional deadline expires.
async fn await_until_deadline<F: Future>(
    future: F,
    deadline: Option<&mut TimerFuture>,
) -> Result<Option<F::Output>, LifecycleError> {
    let mut future = Box::pin(future);
    let Some(deadline) = deadline else {
        return Ok(Some(future.await));
    };
    std::future::poll_fn(|cx| {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(cx) {
            return std::task::Poll::Ready(Ok(Some(output)));
        }
        match deadline.as_mut().poll(cx) {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(None)),
            std::task::Poll::Ready(Err(error)) => std::task::Poll::Ready(Err(LifecycleError::Timer(error))),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    })
    .await
}

/// Waits for a signal predicate while polling a caller-owned absolute deadline.
async fn wait_until_deadline(
    signal: &AsyncSignal,
    deadline: Option<&mut TimerFuture>,
    mut ready: impl FnMut() -> bool,
) -> Result<WaitOutcome, LifecycleError> {
    let Some(deadline) = deadline else {
        let registration = SignalRegistration::new(signal);
        return std::future::poll_fn(|cx| {
            if ready() {
                return std::task::Poll::Ready(Ok(WaitOutcome::Idle));
            }
            registration.register(cx.waker());
            if ready() {
                return std::task::Poll::Ready(Ok(WaitOutcome::Idle));
            }
            std::task::Poll::Pending
        })
        .await;
    };
    let registration = SignalRegistration::new(signal);
    std::future::poll_fn(|cx| {
        if ready() {
            return std::task::Poll::Ready(Ok(WaitOutcome::Idle));
        }
        match deadline.as_mut().poll(cx) {
            std::task::Poll::Ready(Ok(())) => {
                return std::task::Poll::Ready(Ok(WaitOutcome::TimedOut));
            }
            std::task::Poll::Ready(Err(error)) => {
                return std::task::Poll::Ready(Err(LifecycleError::Timer(error)));
            }
            std::task::Poll::Pending => {}
        }
        registration.register(cx.waker());
        if ready() {
            return std::task::Poll::Ready(Ok(WaitOutcome::Idle));
        }
        std::task::Poll::Pending
    })
    .await
}

pub(super) struct SignalRegistration<'a> {
    signal: &'a AsyncSignal,
    id: u64,
}
impl SignalRegistration<'_> {
    pub(super) fn new(signal: &AsyncSignal) -> SignalRegistration<'_> {
        SignalRegistration {
            signal,
            id: signal.next_waiter.fetch_add(1, Ordering::Relaxed),
        }
    }
    pub(super) fn register(&self, waker: &std::task::Waker) {
        self.signal.register_waiter(self.id, waker);
    }
}
impl Drop for SignalRegistration<'_> {
    fn drop(&mut self) {
        self.signal.unregister(self.id);
    }
}
