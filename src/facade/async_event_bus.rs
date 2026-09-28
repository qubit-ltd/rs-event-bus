// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Runtime-neutral asynchronous event-bus facade.

mod internal;

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

pub(super) use internal::AsyncDeliveryGuard;
pub(super) use internal::AsyncPublishGuard;
pub(super) use internal::AsyncRunnerGuard;
pub(super) use internal::AsyncSignal;
pub(super) use internal::AsyncSubscribeGuard;
pub(super) use internal::AsyncTracker;
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
use super::ShutdownReport;
use super::WaitOutcome;
use super::async_admission::AsyncAdmission;
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
    pub(super) capabilities: crate::spi::EventBusCapabilities,
    pub(super) publisher: PublisherPipeline,
    pub(super) facade_config: EventBusFacadeConfig,
    pub(super) state: Mutex<BusState>,
    shutdown_active: AtomicBool,
    shutdown_immediate: AtomicBool,
    shutdown_signal: AsyncSignal,
    shutdown_mode_signal: AsyncSignal,
    shutdown_report: Mutex<Option<ShutdownReport>>,
    pub(super) abandoned_deliveries: AtomicU64,
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
    ///
    /// # Errors
    /// Returns the provider's capability call failure. A Rust panic from that
    /// call is reported as a terminal `provider_panicked` SPI error.
    pub fn from_spi(provider_id: ProviderId, spi: Arc<dyn AsyncEventBusSpi>) -> Result<Self, crate::error::SpiError> {
        Self::with_config(provider_id, spi, EventBusFacadeConfig::default())
    }

    /// Creates a facade using application-supplied codec registrations.
    ///
    /// The codec registry is frozen into the publisher pipeline at
    /// construction; later changes require constructing a new facade.
    ///
    /// # Errors
    /// Returns the provider's capability call failure, including a terminal
    /// `provider_panicked` error when the SPI unwinds.
    pub fn with_config(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        config: EventBusFacadeConfig,
    ) -> Result<Self, crate::error::SpiError> {
        Self::with_config_and_timer(provider_id, spi, config, StdMonotonicClock::new().new_timer())
    }

    /// Creates a facade using the supplied runtime-neutral timer for deadlines
    /// and asynchronous retry delays.
    ///
    /// # Errors
    /// Returns the provider's capability call failure, including a terminal
    /// `provider_panicked` error when the SPI unwinds.
    pub fn with_timer(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        timer: Arc<dyn Timer>,
    ) -> Result<Self, crate::error::SpiError> {
        Self::with_config_and_timer(provider_id, spi, EventBusFacadeConfig::default(), timer)
    }

    /// Creates a facade with custom codec registrations and timer.
    ///
    /// # Errors
    /// Returns the provider's capability call failure, including a terminal
    /// `provider_panicked` error when the SPI unwinds.
    pub fn with_config_and_timer(
        provider_id: ProviderId,
        spi: Arc<dyn AsyncEventBusSpi>,
        config: EventBusFacadeConfig,
        timer: Arc<dyn Timer>,
    ) -> Result<Self, crate::error::SpiError> {
        let capabilities =
            crate::spi::panic_boundary::catch_spi_call(provider_id.as_str(), "capabilities", None, || {
                spi.capabilities()
            })?;
        let admission_limit = config.delivery_admission().max_in_flight();
        Ok(Self {
            inner: Arc::new(AsyncEventBusInner {
                spi,
                capabilities,
                provider_id: provider_id.clone(),
                publisher: PublisherPipeline::new(
                    provider_id,
                    config.codec_registry().clone(),
                    capabilities,
                    config.max_encoded_payload_bytes(),
                ),
                facade_config: config,
                state: Mutex::new(BusState::Running),
                next_subscription_id: AtomicU64::new(1),
                controls: Mutex::new(HashMap::new()),
                close_errors: Mutex::new(Vec::new()),
                close_error_snapshot: Mutex::new(None),
                shutdown_active: AtomicBool::new(false),
                shutdown_immediate: AtomicBool::new(false),
                shutdown_signal: AsyncSignal::default(),
                shutdown_mode_signal: AsyncSignal::default(),
                shutdown_report: Mutex::new(None),
                abandoned_deliveries: AtomicU64::new(0),
                observers: Mutex::new(Vec::new()),
                tracker: Arc::new(AsyncTracker::default()),
                ordering_lanes: AsyncOrderingLanes::new(),
                admission: AsyncAdmission::new(admission_limit),
                timer,
                publish_metrics: PublishMetrics::default(),
            }),
        })
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
        let capabilities = self.inner.capabilities;
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
        if options.dead_letter().is_some_and(|policy| {
            policy.admission_policy() == crate::model::DeadLetterAdmissionPolicy::KnownDestination
                && capabilities.publish_visibility() == crate::spi::PublishVisibility::Opaque
        }) {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "dead_letter.known_destination_admission",
            }));
        }
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
        let subscribe = crate::spi::panic_boundary::catch_spi_call(
            self.inner.provider_id.as_str(),
            "subscribe",
            Some(subscriber_id.as_str()),
            || self.inner.spi.subscribe(spi_request),
        )?;
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
        let bus_key = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_poll(bus_key) {
            return Err(LifecycleError::WouldDeadlock {
                operation: "wait_for_received_deliveries",
            });
        }
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
    /// have not started are closed directly by shutdown. Unstarted ephemeral
    /// deliveries may be abandoned and are counted in the returned report.
    /// It cannot forcibly cancel user code and may therefore wait indefinitely.
    /// Graceful shutdown applies this caller's deadline to receiver close,
    /// runner and publish completion, and provider shutdown. A timeout leaves
    /// the bus closing so the caller may retry cleanup, including with
    /// [`ShutdownMode::Immediate`].
    ///
    /// Directly awaiting shutdown from a handler or middleware Future running
    /// on this bus returns [`LifecycleError::WouldDeadlock`]. This detection is
    /// scoped to each poll of the caller-driven runner Future; tasks the
    /// application independently spawns are outside that scope and must not
    /// await a shutdown that includes their originating handler.
    ///
    /// # Returns
    /// The cached provider outcome and facade-known abandoned-delivery count.
    pub async fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownReport, ShutdownError> {
        let bus_key = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_poll(bus_key) {
            return Err(LifecycleError::WouldDeadlock { operation: "shutdown" }.into());
        }
        if mode == ShutdownMode::Immediate {
            self.inner.shutdown_immediate.store(true, Ordering::Release);
            self.inner.shutdown_mode_signal.notify();
            let controls: Vec<_> = self
                .inner
                .controls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .cloned()
                .collect();
            for control in controls {
                control.stop(ShutdownMode::Immediate);
            }
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
                    .shutdown_report
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .unwrap_or(ShutdownReport::new(ShutdownOutcome::Complete, 0, false)));
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
                let requested_mode = self.requested_shutdown_mode(mode);
                let controls: Vec<_> = self
                    .inner
                    .controls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                    .cloned()
                    .collect();
                for control in controls {
                    control.stop(requested_mode);
                }
                let close = self.close_unstarted_subscriptions(requested_mode);
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
                let outcome = loop {
                    let requested_mode = self.requested_shutdown_mode(mode);
                    let shutdown = crate::spi::panic_boundary::catch_spi_call(
                        self.inner.provider_id.as_str(),
                        "shutdown",
                        None,
                        || self.inner.spi.shutdown(requested_mode),
                    )?;
                    let shutdown = catch_spi_future(shutdown, &self.inner.provider_id, "shutdown", None);
                    if requested_mode == ShutdownMode::Immediate {
                        let Some(outcome) = await_until_deadline(shutdown, deadline.as_mut()).await? else {
                            return Err(ShutdownError::TimedOut {
                                timeout: timeout.expect("a deadline exists for graceful shutdown"),
                            });
                        };
                        break outcome?;
                    }
                    match await_shutdown_or_immediate(
                        shutdown,
                        deadline.as_mut(),
                        &self.inner.shutdown_mode_signal,
                        &self.inner.shutdown_immediate,
                    )
                    .await?
                    {
                        ShutdownWait::Complete(result) => break result?,
                        ShutdownWait::TimedOut => {
                            return Err(ShutdownError::TimedOut {
                                timeout: timeout.expect("a deadline exists for graceful shutdown"),
                            });
                        }
                        ShutdownWait::ImmediateRequested => continue,
                    }
                };
                let report = ShutdownReport::new(
                    outcome,
                    self.inner.abandoned_deliveries.load(Ordering::Acquire),
                    self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral
                        || outcome == ShutdownOutcome::TimedOut,
                );
                *self
                    .inner
                    .shutdown_report
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(report);
                *self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = BusState::Closed;
                if let Some(errors) = self.inner.close_errors_snapshot() {
                    return Err(ShutdownError::SubscriptionClose(errors));
                }
                return Ok(report);
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

    /// Returns the strongest shutdown mode requested by any caller so far.
    fn requested_shutdown_mode(&self, requested: ShutdownMode) -> ShutdownMode {
        if requested == ShutdownMode::Immediate || self.inner.shutdown_immediate.load(Ordering::Acquire) {
            ShutdownMode::Immediate
        } else {
            requested
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
        Err(panic) => Err(crate::spi::panic_boundary::provider_panic(
            provider.as_str(),
            operation,
            resource,
            panic,
        )),
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

impl AsyncEventBusInner {
    fn begin_publish(self: &Arc<Self>) -> Option<AsyncPublishGuard> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state != BusState::Running {
            return None;
        }
        self.tracker.publish_started();
        drop(state);
        Some(AsyncPublishGuard::after_start(self.tracker.clone()))
    }
    fn begin_subscribe(self: &Arc<Self>) -> Option<AsyncSubscribeGuard> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state != BusState::Running {
            return None;
        }
        self.tracker.subscribe_started();
        drop(state);
        Some(AsyncSubscribeGuard::after_start(self.tracker.clone()))
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

enum ShutdownWait<T> {
    Complete(T),
    TimedOut,
    ImmediateRequested,
}

/// Awaits provider shutdown until completion, the caller deadline, or a mode
/// escalation.
async fn await_shutdown_or_immediate<F: Future>(
    future: F,
    deadline: Option<&mut TimerFuture>,
    signal: &AsyncSignal,
    immediate: &AtomicBool,
) -> Result<ShutdownWait<F::Output>, LifecycleError> {
    let mut future = Box::pin(future);
    let mut deadline = deadline;
    let registration = SignalRegistration::new(signal);
    std::future::poll_fn(|cx| {
        if immediate.load(Ordering::Acquire) {
            return Poll::Ready(Ok(ShutdownWait::ImmediateRequested));
        }
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(ShutdownWait::Complete(output)));
        }
        if let Some(deadline) = deadline.as_deref_mut() {
            match deadline.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => return Poll::Ready(Ok(ShutdownWait::TimedOut)),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(LifecycleError::Timer(error))),
                Poll::Pending => {}
            }
        }
        registration.register(cx.waker());
        if immediate.load(Ordering::Acquire) {
            return Poll::Ready(Ok(ShutdownWait::ImmediateRequested));
        }
        Poll::Pending
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
            id: signal.next_waiter_id(),
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
