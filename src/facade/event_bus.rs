// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Synchronous type-safe event-bus facade over an object-safe provider SPI.

use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_id::Id;
use qubit_retry::AttemptFailure;
use qubit_retry::Retry;
use qubit_retry::RetryCallbackKind;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryErrorReason;
use qubit_retry::RetryFallback;

use super::DiagnosticObserverHandle;
use super::observer_entry::ObserverEntry;
use crate::error::CapabilityError;
use crate::error::ConfigurationError;
use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;
use crate::error::EventBusError;
use crate::error::LifecycleError;
use crate::error::ProviderError;
use crate::error::PublishError;
use crate::error::ShutdownError;
use crate::error::SpiError;
use crate::error::SubscribeError;
use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;
use crate::facade::BusContextGuard;
use crate::facade::DeliveryTrackerGuard;
use crate::facade::EventBusFacadeConfig;
use crate::facade::LifecycleState;
use crate::facade::LifecycleTracker;
use crate::facade::PublishMetrics;
use crate::facade::PublishMetricsSnapshot;
use crate::facade::Subscription;
use crate::facade::SubscriptionControl;
use crate::facade::WaitOutcome;
use crate::facade::is_current_bus_context;
use crate::facade::receive_poll_interval;
use crate::facade::shutdown_coordinator::ShutdownCoordinator;
use crate::facade::sync_delivery_scheduler::SyncDeliveryScheduler;
use crate::local::LocalEventBusConfig;
use crate::model::BatchPublishResult;
use crate::model::Delivery;
use crate::model::DeliveryContext;
use crate::model::EventEnvelope;
use crate::model::EventId;
use crate::model::FailureDirective;
use crate::model::ProviderId;
use crate::model::PublishReceipt;
use crate::model::PublishRequest;
use crate::model::SubscribeRequest;
use crate::model::SubscriberId;
use crate::model::Topic;
use crate::pipeline::DeliveryFailureAction;
use crate::pipeline::DeliveryOutcome;
use crate::pipeline::Diagnostic;
use crate::pipeline::DiagnosticObserver;
use crate::pipeline::OrderingLaneKey;
use crate::pipeline::PublisherPipeline;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::dead_letter_envelope;
use crate::pipeline::emit_diagnostic;
use crate::registry::EventBusConfig;
use crate::registry::EventBusRegistry;
use crate::spi::DeliveryDisposition;
use crate::spi::EventBusSpi;
use crate::spi::InboundMessage;
use crate::spi::PayloadModes;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementToken;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

/// Converts a synchronous subscriber handler's return value into a delivery
/// result.
pub trait IntoHandlerResult {
    /// Turns an accepted return value into success or a source-preserving
    /// handler error.
    fn into_handler_result(self) -> Result<(), DeliveryError>;
}

impl IntoHandlerResult for () {
    /// Treats a handler returning unit as successful completion.
    fn into_handler_result(self) -> Result<(), DeliveryError> {
        Ok(())
    }
}

impl IntoHandlerResult for Result<(), DeliveryError> {
    /// Preserves a handler error as the source of a delivery failure.
    fn into_handler_result(self) -> Result<(), DeliveryError> {
        self.map_err(|source| DeliveryError::Handler {
            source: Box::new(source),
        })
    }
}

/// A cloneable synchronous facade with one provider coordinator per
/// subscription and a shared bounded handler pool.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::model::{PublishRequest, SubscribeRequest, Topic};
/// use qubit_event_bus::{DeliveryError, EventBus};
///
/// let bus = EventBus::local(LocalEventBusConfig::default())?;
/// let topic = Topic::<String>::new("orders.created")?;
/// let request = SubscribeRequest::new("audit", topic.clone())?;
/// let subscription = bus.subscribe(request, |_| Ok::<(), DeliveryError>(()))?;
/// let receipt = bus.publish(PublishRequest::new(topic.clone(), "order-1".to_owned())?)?;
/// assert_eq!(receipt.provider_id().as_str(), "local");
/// bus.wait_for_idle(&topic, None)?;
/// subscription.cancel()?;
/// bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct EventBus {
    pub(super) inner: Arc<EventBusInner>,
}

pub(super) struct EventBusInner {
    spi: Arc<dyn EventBusSpi>,
    provider_id: ProviderId,
    publisher: PublisherPipeline,
    facade_config: EventBusFacadeConfig,
    lifecycle: Mutex<LifecycleState>,
    operations: OperationGate,
    tracker: LifecycleTracker,
    subscriptions: Mutex<HashMap<Id, Arc<SubscriptionControl>>>,
    close_errors: Mutex<Vec<Arc<SubscriptionCloseFailure>>>,
    close_error_snapshot: Mutex<Option<Arc<SubscriptionCloseErrors>>>,
    next_subscription_id: AtomicU64,
    observers: Mutex<Vec<Weak<ObserverEntry>>>,
    shutdown_gate: Mutex<ShutdownState>,
    shutdown_coordinator: ShutdownCoordinator,
    scheduler: Arc<SyncDeliveryScheduler>,
    publish_metrics: PublishMetrics,
}

struct ShutdownState {
    outcome: Option<ShutdownOutcome>,
}

/// Linearizes new public publish/subscribe calls against provider shutdown.
#[derive(Default)]
struct OperationGate {
    state: Mutex<OperationGateState>,
    changed: Condvar,
}

/// Counts calls that entered the facade while it was still accepting
/// operations.
#[derive(Default)]
struct OperationGateState {
    closing: bool,
    active: usize,
}

/// Releases one operation admission when its complete SPI call sequence
/// returns.
struct OperationPermit<'a> {
    gate: &'a OperationGate,
}

impl OperationGate {
    /// Admits a facade operation unless shutdown has closed admission.
    fn enter(&self) -> Option<OperationPermit<'_>> {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closing {
            return None;
        }
        state.active += 1;
        Some(OperationPermit { gate: self })
    }

    /// Closes operation admission without waiting for existing calls.
    fn close_admission(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closing = true;
        self.changed.notify_all();
    }

    /// Waits for every previously admitted SPI call to finish.
    fn wait_for_idle(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.active != 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl Drop for OperationPermit<'_> {
    /// Releases one in-progress operation and wakes shutdown when admission
    /// drains.
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.gate.changed.notify_all();
        }
    }
}

enum CoordinatorMessage {
    Settlement {
        token: Option<SettlementToken>,
        disposition: DeliveryDisposition,
        event_id: EventId,
        topic: Box<str>,
        subscription_id: Id,
        subscriber_id: SubscriberId,
        settled: Option<mpsc::SyncSender<()>>,
    },
    TaskFinished(usize),
}

#[derive(Clone)]
struct OwnerSettlementRouter {
    sender: mpsc::Sender<CoordinatorMessage>,
}

impl OwnerSettlementRouter {
    fn settle(
        &self,
        token: Option<SettlementToken>,
        disposition: DeliveryDisposition,
        event_id: EventId,
        topic: &str,
        subscription_id: Id,
        subscriber_id: &SubscriberId,
    ) {
        let (settled, wait) = mpsc::sync_channel(0);
        if self
            .sender
            .send(CoordinatorMessage::Settlement {
                token,
                disposition,
                event_id,
                topic: topic.into(),
                subscription_id,
                subscriber_id: subscriber_id.clone(),
                settled: Some(settled),
            })
            .is_ok()
        {
            let _ = wait.recv();
        }
    }
}

impl EventBus {
    /// Creates a synchronous facade using the built-in local provider.
    ///
    /// This convenience path uses the same provider registry and SPI
    /// validation as explicit provider selection. The supplied configuration
    /// controls local transport queues; facade behavior retains its defaults.
    ///
    /// # Errors
    /// Returns an error if the local provider cannot be registered or created,
    /// or if the supplied local configuration is invalid.
    pub fn local(config: LocalEventBusConfig) -> Result<Self, ProviderError> {
        let registry = EventBusRegistry::with_local().map_err(|source| ProviderError::Resolution {
            source: Box::new(source),
        })?;
        let config = EventBusConfig::default().with_provider_options(config.provider_options());
        registry.create(&config)
    }

    /// Creates a running facade around an already-created provider SPI.
    ///
    /// The facade takes shared ownership of the provider. Each subscription has
    /// one managed coordinator that owns its SPI receiver and polls blocking
    /// receives at a finite interval; handler callbacks run on the facade-wide
    /// bounded pool. Use a registry when provider selection or creation
    /// fallback is required.
    pub fn from_spi(provider_id: ProviderId, spi: Arc<dyn EventBusSpi>) -> Self {
        Self::with_config(provider_id, spi, EventBusFacadeConfig::default())
    }

    /// Creates a facade using application-supplied codec registrations.
    ///
    /// The codec registry is frozen into the publisher pipeline at
    /// construction; later changes require constructing a new facade.
    pub fn with_config(provider_id: ProviderId, spi: Arc<dyn EventBusSpi>, config: EventBusFacadeConfig) -> Self {
        let scheduler = SyncDeliveryScheduler::new(config.sync_delivery_scheduler());
        Self {
            inner: Arc::new(EventBusInner {
                spi,
                provider_id: provider_id.clone(),
                publisher: PublisherPipeline::new(provider_id, config.codec_registry().clone()),
                facade_config: config,
                lifecycle: Mutex::new(LifecycleState::Running),
                operations: OperationGate::default(),
                tracker: LifecycleTracker::new(),
                subscriptions: Mutex::new(HashMap::new()),
                close_errors: Mutex::new(Vec::new()),
                close_error_snapshot: Mutex::new(None),
                next_subscription_id: AtomicU64::new(1),
                observers: Mutex::new(Vec::new()),
                shutdown_gate: Mutex::new(ShutdownState { outcome: None }),
                shutdown_coordinator: ShutdownCoordinator::new(),
                scheduler,
                publish_metrics: PublishMetrics::default(),
            }),
        }
    }

    /// Publishes one typed request through publisher interceptors, retry, and
    /// SPI.
    ///
    /// # Errors
    /// Returns the structured publication failure from request validation,
    /// capability/codec checks, SPI, retry, or a closed facade.
    pub fn publish<T: Send + Sync + 'static>(
        &self,
        request: PublishRequest<T>,
    ) -> Result<PublishReceipt, PublishError> {
        self.inner.publish_metrics.record_attempt();
        let Some(_operation) = self.inner.operations.enter() else {
            self.inner.publish_metrics.record_error();
            return Err(PublishError::Closed);
        };
        let bus_identity = Arc::as_ptr(&self.inner) as usize;
        let _call_context = BusContextGuard::enter(bus_identity);
        let observers = self.inner.observer_snapshot();
        self.inner
            .publisher
            .publish(
                self.inner.spi.as_ref(),
                request,
                self.inner.facade_config.global_publisher_interceptors(),
                &observers,
            )
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

    /// Publishes requests independently in input order and retains each result.
    ///
    /// Later requests are still attempted after an earlier request fails.
    pub fn publish_all<T, I>(&self, requests: I) -> BatchPublishResult
    where
        T: Send + Sync + 'static,
        I: IntoIterator<Item = PublishRequest<T>>,
    {
        let items = requests.into_iter().map(|request| self.publish(request)).collect();
        BatchPublishResult::new(items)
    }

    /// Creates a provider subscription and starts its SPI coordinator.
    ///
    /// The handler is invoked outside facade locks. The returned handle does
    /// not cancel the worker when dropped; call `cancel` or shut down the
    /// bus.
    ///
    /// # Errors
    /// Returns `Closed` after shutdown begins, `Capability` for unsupported
    /// manual acknowledgement or per-key ordering, `Configuration` for
    /// runtime-model mismatches, or the provider subscription error.
    pub fn subscribe<T, H, R>(&self, request: SubscribeRequest<T>, handler: H) -> Result<Subscription, SubscribeError>
    where
        T: Send + Sync + 'static,
        H: Fn(Delivery<T>) -> R + Send + Sync + 'static,
        R: IntoHandlerResult + 'static,
    {
        let _operation = self.inner.operations.enter().ok_or(SubscribeError::Closed)?;
        let bus_identity = Arc::as_ptr(&self.inner) as usize;
        let _call_context = BusContextGuard::enter(bus_identity);
        let (subscriber_id, topic, options) = request.into_parts();
        if !options.async_interceptors().is_empty() || self.inner.facade_config.has_async_subscriber_interceptors::<T>()
        {
            return Err(SubscribeError::Configuration(ConfigurationError::InvalidField {
                field: "async_subscriber_interceptor",
                message: "synchronous EventBus requires synchronous subscriber middleware".into(),
            }));
        }
        let capabilities = self.inner.spi.capabilities();
        SubscriberPipeline::validate_ack_capability(options.ack_mode(), capabilities.settlement())?;
        if options.ordering_policy() == crate::model::OrderingPolicy::PerKey
            && !capabilities.ordering().supports_per_key()
        {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "ordering.per_key",
            }));
        }
        if capabilities.payload_modes() == PayloadModes::Encoded && topic.codec().is_none() {
            return Err(SubscribeError::Capability(CapabilityError::CodecRequired));
        }
        let id = self.next_subscription_id()?;
        let address = TopicAddress::new(topic.name())?;
        let spi_request = SpiSubscriptionRequest::new(
            id,
            address,
            subscriber_id.clone(),
            options.consumer_group().cloned(),
            options.durability(),
            options.start_position().clone(),
            options.provider_options().clone(),
            topic.payload_type_id(),
        );
        let spi_subscription = self.inner.spi.subscribe(spi_request)?;
        let spi_subscription_slot = Arc::new(Mutex::new(Some(spi_subscription)));
        if let Err(error) = self.inner.scheduler.start() {
            let spi_subscription = spi_subscription_slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            return Err(cleanup_failed_worker_spawn(
                &self.inner,
                &subscriber_id,
                spi_subscription,
                error,
            ));
        }
        let control = SubscriptionControl::new(id, subscriber_id.clone());
        {
            let _lifecycle = self.lock_lifecycle();
            self.inner.tracker.worker_started();
            self.inner
                .subscriptions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id, control.clone());
        }
        let inner = self.inner.clone();
        let handler = Arc::new(move |delivery: Delivery<T>| handler(delivery).into_handler_result());
        let thread_control = control.clone();
        let thread_topic = topic.clone();
        let thread_options = options.clone();
        let thread_subscriber_id = subscriber_id.clone();
        let thread_spi_subscription_slot = spi_subscription_slot.clone();
        let worker = thread::Builder::new()
            .name(format!("event-bus-subscription-{}", id.value()))
            .spawn(move || {
                let spi_subscription = thread_spi_subscription_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .expect("subscription worker owns an initialized SPI subscription");
                run_subscription_worker(
                    inner,
                    bus_identity,
                    thread_control,
                    spi_subscription,
                    thread_topic,
                    thread_subscriber_id,
                    thread_options,
                    handler,
                );
            });
        match worker {
            Ok(worker) => {
                control.set_worker(worker);
                Ok(Subscription::new(control, bus_identity, self.inner.scheduler.clone()))
            }
            Err(error) => {
                self.inner.tracker.worker_finished();
                self.inner
                    .subscriptions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&id);
                let spi_subscription = spi_subscription_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                Err(cleanup_failed_worker_spawn(
                    &self.inner,
                    &subscriber_id,
                    spi_subscription,
                    error,
                ))
            }
        }
    }

    /// Waits until the provider reports that `topic` has no queued or unsettled
    /// messages.
    ///
    /// This is local to this facade and does not establish that a remote broker
    /// or other consumers are globally idle. A call from one of this bus's
    /// synchronous callbacks or workers returns `WouldDeadlock` rather than
    /// waiting for work that depends on the current call to finish.
    ///
    /// # Errors
    /// Returns `WouldDeadlock` when called within a synchronous callback or
    /// worker owned by this bus, `IdleWaitUnsupported` when the provider has no
    /// topic-idle reporting capability, or the original SPI error.
    pub fn wait_for_idle<T: 'static>(
        &self,
        topic: &Topic<T>,
        timeout: Option<Duration>,
    ) -> Result<WaitOutcome, LifecycleError> {
        let identity = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_context(identity) {
            return Err(LifecycleError::WouldDeadlock {
                operation: "wait_for_idle",
            });
        }
        let address = TopicAddress::new(topic.name()).expect("typed topic names are valid SPI addresses");
        self.inner
            .spi
            .wait_for_topic_idle(&address, timeout)?
            .map(|idle| if idle { WaitOutcome::Idle } else { WaitOutcome::TimedOut })
            .ok_or(LifecycleError::IdleWaitUnsupported)
    }

    /// Waits until this facade has completed work already received for `topic`.
    ///
    /// This is local to this facade and does not establish that a remote broker
    /// or other consumers are globally idle.
    pub fn wait_for_received_deliveries<T: 'static>(
        &self,
        topic: &Topic<T>,
        timeout: Option<Duration>,
    ) -> Result<WaitOutcome, LifecycleError> {
        let identity = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_context(identity) {
            return Err(LifecycleError::WouldDeadlock {
                operation: "wait_for_received_deliveries",
            });
        }
        Ok(self.inner.tracker.wait_for_idle(topic.name(), timeout))
    }

    /// Registers an observer until the returned handle is dropped.
    ///
    /// Observers run synchronously in registration order and outside facade
    /// locks. Panics are contained and do not recursively emit diagnostics.
    pub fn observe_diagnostics<F>(&self, observer: F) -> DiagnosticObserverHandle
    where
        F: Fn(&Diagnostic) + Send + Sync + 'static,
    {
        let entry = Arc::new(ObserverEntry {
            active: std::sync::atomic::AtomicBool::new(true),
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

    /// Stops operation admission, completes active deliveries, and shuts down
    /// the provider.
    ///
    /// The facade first rejects new publish/subscribe calls and waits for calls
    /// admitted earlier to finish their provider SPI operations. A subscription
    /// admitted before shutdown is included in the subsequent close phase. Both
    /// modes then stop workers from receiving additional messages. Graceful
    /// shutdown drains admitted queued and active deliveries; a message already
    /// received but not admitted by the shared scheduler is returned with
    /// `Retry`. Immediate shutdown returns admitted queued deliveries with
    /// `Retry`, allows active handlers and settlements to finish, and then
    /// closes subscriptions. Graceful shutdown applies its timeout to the
    /// caller's wait for the entire close sequence. If the deadline expires,
    /// this method returns `TimedOut` while one background coordinator keeps
    /// closing the bus; new operations remain rejected. Call shutdown again to
    /// wait for the result, or use `Immediate` to strengthen an active attempt.
    /// The coordinator cannot forcibly stop a blocked synchronous SPI call or
    /// user handler, so it can remain alive until that code returns.
    /// Calling either mode from a synchronous callback or worker owned by this
    /// bus returns `WouldDeadlock` instead of waiting for the current
    /// operation permit.
    ///
    /// # Errors
    /// Returns `WouldDeadlock` for a call from a bus callback or worker,
    /// `TimedOut` when the full graceful close has not completed by its
    /// deadline, a coordinator thread could not start, and provider/close
    /// failures without suppressing their source errors.
    pub fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, ShutdownError> {
        let identity = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_context(identity) {
            return Err(LifecycleError::WouldDeadlock { operation: "shutdown" }.into());
        }
        let timeout = match mode {
            ShutdownMode::Graceful { timeout } => Some(timeout),
            ShutdownMode::Immediate => None,
        };
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        {
            let mut state = self.lock_lifecycle();
            if *state == LifecycleState::Closed {
                if let Some(errors) = self.inner.close_errors_snapshot() {
                    return Err(ShutdownError::SubscriptionClose(errors));
                }
                return Ok(self
                    .inner
                    .shutdown_gate
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .outcome
                    .unwrap_or(ShutdownOutcome::Complete));
            }
            *state = LifecycleState::Closing;
        }
        self.inner.operations.close_admission();

        loop {
            let (start, generation) = self.inner.shutdown_coordinator.begin(mode);
            self.inner
                .scheduler
                .stop_admission(matches!(mode, ShutdownMode::Immediate));
            for control in &self.inner.subscription_snapshot() {
                control.request_cancel();
            }
            if start {
                let inner = self.inner.clone();
                let spawn = thread::Builder::new()
                    .name("event-bus-shutdown".to_owned())
                    .spawn(move || inner.run_shutdown(generation));
                if let Err(error) = spawn {
                    self.inner.shutdown_coordinator.abort_start(generation);
                    return Err(ShutdownError::CoordinatorStart(error));
                }
            }
            let (timed_out, result) = self.inner.shutdown_coordinator.wait(generation, deadline);
            if timed_out {
                return Err(ShutdownError::TimedOut {
                    timeout: timeout.expect("only graceful shutdown has a deadline"),
                });
            }
            let Some(result) = result else {
                continue;
            };
            let outcome = result.map_err(clone_spi_error)?;
            if let Some(errors) = self.inner.close_errors_snapshot() {
                return Err(ShutdownError::SubscriptionClose(errors));
            }
            return Ok(outcome);
        }
    }

    /// Locks the lifecycle state while recovering from internal poison.
    fn lock_lifecycle(&self) -> std::sync::MutexGuard<'_, LifecycleState> {
        self.inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Allocates one monotonically increasing, bus-local subscription ID.
    fn next_subscription_id(&self) -> Result<Id, SubscribeError> {
        self.inner
            .next_subscription_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| current.checked_add(1))
            .map(Id::new)
            .map_err(|_| {
                SubscribeError::Configuration(ConfigurationError::InvalidField {
                    field: "subscription_id",
                    message: "bus-local subscription ID space is exhausted".into(),
                })
            })
    }
}

impl EventBusInner {
    /// Returns an immutable snapshot of currently active observer callbacks.
    fn observer_snapshot(&self) -> Vec<Arc<DiagnosticObserver>> {
        let mut observers = self.observers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        observers.retain(|entry| entry.strong_count() > 0);
        observers
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|entry| entry.active.load(Ordering::Acquire))
            .map(|entry| entry.callback.clone())
            .collect()
    }

    /// Emits a diagnostic with observer panic isolation and no registry lock
    /// held.
    fn emit(&self, diagnostic: Diagnostic) {
        let observers = self.observer_snapshot();
        emit_diagnostic(&observers, &diagnostic);
    }

    /// Emits a non-fatal internal failure through the common observer path.
    fn emit_internal(&self, origin: &str, message: String) {
        self.emit(Diagnostic::InternalFailure {
            origin: origin.into(),
            message: message.into(),
        });
    }

    /// Returns a snapshot of active subscription worker controls.
    fn subscription_snapshot(&self) -> Vec<Arc<SubscriptionControl>> {
        self.subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    /// Returns a stable aggregate snapshot of every worker close failure so
    /// far.
    fn close_errors_snapshot(&self) -> Option<Arc<SubscriptionCloseErrors>> {
        let mut snapshot = self
            .close_error_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(errors) = snapshot.as_ref() {
            return Some(errors.clone());
        }
        let failures = self
            .close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failures.is_empty() {
            return None;
        }
        let errors = Arc::new(SubscriptionCloseErrors::from_failures(failures.clone()));
        *snapshot = Some(errors.clone());
        Some(errors)
    }

    /// Joins one worker after the caller has established it has finished.
    fn join_control(&self, control: &Arc<SubscriptionControl>) -> Result<(), SpiError> {
        let worker = control
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            worker.join().map_err(|_| SpiError::Operation {
                provider_id: self.provider_id.as_str().into(),
                operation: "subscription_worker",
                resource: Some(control.subscriber_id.as_str().into()),
                kind: "worker_panicked",
                retryable: None,
                source: Box::new(std::io::Error::other("subscription worker panicked")),
            })?;
        }
        Ok(())
    }

    /// Completes one shutdown attempt on the dedicated coordinator thread.
    fn run_shutdown(self: Arc<Self>, generation: u64) {
        let bus_identity = Arc::as_ptr(&self) as usize;
        let _context = BusContextGuard::enter(bus_identity);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.perform_shutdown(generation)))
            .unwrap_or_else(|panic| {
                Err(SpiError::Operation {
                    provider_id: self.provider_id.as_str().into(),
                    operation: "shutdown_coordinator",
                    resource: None,
                    kind: "coordinator_panicked",
                    retryable: None,
                    source: Box::new(std::io::Error::other(panic_message(panic.as_ref()))),
                })
            });
        let failure_message = result.as_ref().err().map(ToString::to_string);
        self.shutdown_coordinator.finish(generation, result);
        if let Some(message) = failure_message {
            self.emit_internal("shutdown_coordinator", message);
        }
    }

    /// Waits for admitted calls and workers, then closes provider resources.
    fn perform_shutdown(&self, generation: u64) -> Result<ShutdownOutcome, SpiError> {
        self.operations.wait_for_idle();
        let controls = self.subscription_snapshot();
        self.scheduler.stop_admission(matches!(
            self.shutdown_coordinator.mode(generation),
            ShutdownMode::Immediate
        ));
        for control in &controls {
            control.request_cancel();
        }
        self.tracker.wait_for_workers(None);
        for control in &controls {
            self.join_control(control)?;
        }
        self.scheduler.join();
        let mode = self.shutdown_coordinator.mode(generation);
        let outcome = self.shutdown_provider_once(mode)?;
        if self.tracker.workers_are_idle() {
            *self.lifecycle.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = LifecycleState::Closed;
        }
        Ok(outcome)
    }

    /// Shuts down the provider once and caches its successful outcome.
    fn shutdown_provider_once(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        let mut state = self
            .shutdown_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(outcome) = state.outcome {
            return Ok(outcome);
        }
        let outcome = self.spi.shutdown(mode)?;
        state.outcome = Some(outcome);
        Ok(outcome)
    }
}

/// Recreates a shared SPI error while retaining the original error in its
/// source chain for each concurrent shutdown caller.
fn clone_spi_error(error: Arc<SpiError>) -> SpiError {
    match error.as_ref() {
        SpiError::Operation {
            provider_id,
            operation,
            resource,
            kind,
            retryable,
            ..
        } => SpiError::Operation {
            provider_id: provider_id.clone(),
            operation,
            resource: resource.clone(),
            kind,
            retryable: *retryable,
            source: Box::new(error),
        },
        SpiError::InvalidSettlementToken {
            provider_id,
            operation,
            resource,
            reason,
            retryable,
            ..
        } => SpiError::InvalidSettlementToken {
            provider_id: provider_id.clone(),
            operation,
            resource: resource.clone(),
            reason,
            retryable: *retryable,
            source: Box::new(error),
        },
    }
}

/// Closes a provider receiver when its facade worker could not be spawned.
pub(super) fn cleanup_failed_worker_spawn(
    inner: &EventBusInner,
    subscriber_id: &SubscriberId,
    spi_subscription: Option<Box<dyn crate::spi::EventSubscriptionSpi>>,
    spawn_error: std::io::Error,
) -> SubscribeError {
    if let Some(mut spi_subscription) = spi_subscription
        && let Err(close_error) = close_spi_subscription(inner, subscriber_id, &mut *spi_subscription)
    {
        inner.emit_internal("subscription_spawn_close", close_error.to_string());
        inner
            .close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::new(SubscriptionCloseFailure::new(
                subscriber_id.clone(),
                close_error,
            )));
    }
    SubscribeError::Spi(SpiError::Operation {
        provider_id: inner.provider_id.as_str().into(),
        operation: "spawn_subscription_worker",
        resource: Some(subscriber_id.as_str().into()),
        kind: "worker_spawn_failed",
        retryable: None,
        source: Box::new(spawn_error),
    })
}

/// Closes a subscription while converting provider panics into source errors.
fn close_spi_subscription(
    inner: &EventBusInner,
    subscriber_id: &SubscriberId,
    spi_subscription: &mut dyn crate::spi::EventSubscriptionSpi,
) -> Result<(), SpiError> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| spi_subscription.close())) {
        Ok(result) => result,
        Err(payload) => Err(SpiError::Operation {
            provider_id: inner.provider_id.as_str().into(),
            operation: "close_subscription",
            resource: Some(subscriber_id.as_str().into()),
            kind: "provider_panicked",
            retryable: None,
            source: Box::new(std::io::Error::other(panic_message(payload.as_ref()))),
        }),
    }
}

/// Converts a publisher pipeline failure into its operation-level error type.
fn publish_pipeline_error(failure: crate::pipeline::PipelineFailure) -> PublishError {
    match failure.into_error() {
        EventBusError::Configuration(error) => PublishError::Configuration(error),
        EventBusError::Capability(error) => PublishError::Capability(error),
        EventBusError::Codec(error) => PublishError::Codec(error),
        EventBusError::Publish(error) => error,
        other => PublishError::Configuration(ConfigurationError::InvalidField {
            field: "publish_pipeline",
            message: other.to_string().into(),
        }),
    }
}

/// Runs one subscription receiver and its entire typed processing lifecycle.
// The worker takes each owned subscription resource exactly once; a wrapper
// struct would only restate this one-shot call boundary.
#[allow(clippy::too_many_arguments)]
fn run_subscription_worker<T>(
    inner: Arc<EventBusInner>,
    bus_identity: usize,
    control: Arc<SubscriptionControl>,
    mut spi_subscription: Box<dyn crate::spi::EventSubscriptionSpi>,
    topic: Topic<T>,
    subscriber_id: SubscriberId,
    options: crate::model::SubscribeOptions<T>,
    handler: Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
) where
    T: Send + Sync + 'static,
{
    let _worker_context = BusContextGuard::enter(bus_identity);
    let worker_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (message_tx, message_rx) = mpsc::channel();
        let mut pending: Option<(usize, InboundMessage, DeliveryTrackerGuard<'_>)> = None;
        let mut pending_settlements = VecDeque::new();
        let mut active: HashMap<usize, DeliveryTrackerGuard<'_>> = HashMap::new();
        let mut next_task = 1usize;
        let mut receive_closed = false;
        loop {
            while let Ok(message) = message_rx.try_recv() {
                match message {
                    settlement @ CoordinatorMessage::Settlement { .. } => {
                        pending_settlements.push_back(settlement);
                    }
                    CoordinatorMessage::TaskFinished(task_id) => {
                        active.remove(&task_id);
                    }
                }
            }

            let stopping = control.is_cancelled();
            if stopping {
                receive_closed = true;
                if let Some((_, message, _guard)) = pending.take() {
                    requeue_unstarted_message(&inner, &mut pending_settlements, control.id, &subscriber_id, message);
                }
            }

            // Keep failed settlements, including their non-cloneable provider
            // token, until the provider accepts the same idempotent disposition.
            // Retry only once per owner-loop iteration so receive can drain a
            // full bounded local queue that may be blocking a Requeue.
            if let Some(settlement) = pending_settlements.pop_front()
                && !apply_queued_settlement(&inner, &mut *spi_subscription, &settlement)
            {
                if stopping {
                    release_settlement_waiter(&settlement);
                } else {
                    pending_settlements.push_back(settlement);
                }
            }

            if let Some((task_id, message, guard)) = pending.take() {
                let ordering_key = if options.ordering_policy() == crate::model::OrderingPolicy::PerKey {
                    Some(OrderingLaneKey::new(
                        topic.name(),
                        message.ordering_key().map(crate::spi::OrderingKey::as_str),
                        control.id,
                    ))
                } else {
                    None
                };
                if let Some(reservation) = inner.scheduler.try_reserve(control.id, ordering_key) {
                    let task_subscription_id = control.id;
                    let task_inner = inner.clone();
                    let task_topic = topic.clone();
                    let task_options = options.clone();
                    let task_handler = handler.clone();
                    let task_subscriber_id = subscriber_id.clone();
                    let task_sender = message_tx.clone();
                    let task_id_for_job = task_id;
                    reservation.submit(move |cancelled| {
                        let _task_context = BusContextGuard::enter(bus_identity);
                        let task_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            if cancelled {
                                requeue_unstarted_message_via_owner(
                                    &task_inner,
                                    &OwnerSettlementRouter {
                                        sender: task_sender.clone(),
                                    },
                                    task_subscription_id,
                                    &task_subscriber_id,
                                    message,
                                );
                            } else {
                                process_inbound(
                                    &task_inner,
                                    &OwnerSettlementRouter {
                                        sender: task_sender.clone(),
                                    },
                                    task_subscription_id,
                                    &task_subscriber_id,
                                    &task_topic,
                                    &task_options,
                                    &task_handler,
                                    message,
                                );
                            }
                        }));
                        if let Err(payload) = task_result {
                            task_inner.emit_internal("delivery_worker", panic_message(payload.as_ref()).into());
                        }
                        let _ = task_sender.send(CoordinatorMessage::TaskFinished(task_id_for_job));
                    });
                    active.insert(task_id, guard);
                } else {
                    pending = Some((task_id, message, guard));
                }
            }

            if receive_closed && active.is_empty() && pending.is_none() && pending_settlements.is_empty() {
                break;
            }

            if pending.is_none() && !receive_closed {
                match spi_subscription.receive(if receive_closed {
                    Duration::ZERO
                } else {
                    receive_poll_interval()
                }) {
                    Ok(ReceiveOutcome::TimedOut) => {}
                    Ok(ReceiveOutcome::Closed) => receive_closed = true,
                    Ok(ReceiveOutcome::Gap(gap)) => inner.emit(Diagnostic::ReceiveGap {
                        subscription_id: control.id,
                        subscriber_id: subscriber_id.clone(),
                        topic: topic.name().into(),
                        gap,
                    }),
                    Ok(ReceiveOutcome::Message(message)) => {
                        if control.is_cancelled() || receive_closed {
                            requeue_unstarted_message(
                                &inner,
                                &mut pending_settlements,
                                control.id,
                                &subscriber_id,
                                message,
                            );
                            receive_closed = true;
                        } else {
                            let task_id = next_task;
                            next_task = next_task.wrapping_add(1).max(1);
                            let guard = inner.tracker.track_delivery(topic.name());
                            pending = Some((task_id, message, guard));
                        }
                    }
                    Err(error) => {
                        inner.emit_internal("receive", error.to_string());
                        receive_closed = true;
                    }
                }
            } else if !active.is_empty() || pending.is_some() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }));
    if let Err(payload) = worker_result {
        inner.emit_internal("subscription_worker", panic_message(payload.as_ref()).into());
    }
    if let Err(error) = close_spi_subscription(&inner, &subscriber_id, &mut *spi_subscription) {
        inner.emit_internal("subscription_close", error.to_string());
        let failure = Arc::new(SubscriptionCloseFailure::new(subscriber_id.clone(), error));
        control.record_close_error(failure.clone());
        inner
            .close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(failure);
    }
    inner.scheduler.finish_subscription(control.id);
    inner.tracker.worker_finished();
    inner
        .subscriptions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&control.id);
    let mut lifecycle = inner
        .lifecycle
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if *lifecycle == LifecycleState::Closing
        && inner.tracker.workers_are_idle()
        && inner
            .shutdown_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outcome
            .is_some()
    {
        *lifecycle = LifecycleState::Closed;
    }
    drop(lifecycle);
    control.mark_finished();
}

/// Applies an SPI settlement requested by a handler worker on its owner thread.
// Settlement metadata deliberately remains explicit so ownership and provider
// routing are auditable at the SPI boundary.
#[allow(clippy::too_many_arguments)]
fn apply_owner_settlement(
    inner: &EventBusInner,
    spi_subscription: &mut dyn crate::spi::EventSubscriptionSpi,
    token: &SettlementToken,
    disposition: DeliveryDisposition,
    event_id: EventId,
    topic: &str,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
) -> bool {
    if !token.belongs_to(subscription_id) {
        inner.emit_internal(
            "settlement",
            "provider settlement token belongs to another subscription".into(),
        );
        return true;
    }
    match spi_subscription.settle(token, disposition) {
        Ok(()) => true,
        Err(error) => {
            inner.emit(Diagnostic::SettlementFailed {
                event_id,
                topic: topic.into(),
                subscription_id,
                subscriber_id: subscriber_id.clone(),
                disposition,
                error: error.to_string().into(),
            });
            false
        }
    }
}

fn apply_queued_settlement(
    inner: &EventBusInner,
    spi_subscription: &mut dyn crate::spi::EventSubscriptionSpi,
    message: &CoordinatorMessage,
) -> bool {
    let CoordinatorMessage::Settlement {
        token,
        disposition,
        event_id,
        topic,
        subscription_id,
        subscriber_id,
        settled,
    } = message
    else {
        return true;
    };
    let complete = token.as_ref().is_none_or(|token| {
        apply_owner_settlement(
            inner,
            spi_subscription,
            token,
            *disposition,
            event_id.clone(),
            topic,
            *subscription_id,
            subscriber_id,
        )
    });
    if complete && let Some(settled) = settled {
        let _ = settled.send(());
    }
    complete
}

/// Unblocks a facade worker after its final best-effort settlement attempt
/// fails during cancellation; receiver close then owns unresolved delivery
/// recovery according to the provider's SPI contract.
fn release_settlement_waiter(message: &CoordinatorMessage) {
    if let CoordinatorMessage::Settlement {
        settled: Some(settled), ..
    } = message
    {
        let _ = settled.send(());
    }
}

/// Requeues a delivery canceled before its handler starts through the SPI
/// owner.
fn requeue_unstarted_message_via_owner(
    inner: &EventBusInner,
    router: &OwnerSettlementRouter,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    message: InboundMessage,
) {
    let (address, event_id, _, _, _, _, token, _) = message.into_parts();
    let capability = inner.spi.capabilities().settlement();
    if let Some(token) = token {
        if !token.belongs_to(subscription_id) {
            inner.emit_internal(
                "settlement",
                "provider settlement token belongs to another subscription".into(),
            );
            return;
        }
        if capability == crate::spi::SettlementCapabilities::AcceptRetryReject {
            router.settle(
                Some(token),
                DeliveryDisposition::Retry,
                event_id,
                address.as_str(),
                subscription_id,
                subscriber_id,
            );
            return;
        }
    }
    inner.emit(Diagnostic::SettlementUnavailable {
        event_id,
        topic: address.as_str().into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        requested: DeliveryDisposition::Retry,
    });
}

/// Retains a message received at the cancellation boundary for provider
/// requeue without invoking the subscriber handler.
fn requeue_unstarted_message(
    inner: &Arc<EventBusInner>,
    pending_settlements: &mut VecDeque<CoordinatorMessage>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    message: InboundMessage,
) {
    let (address, event_id, _, _, _, _, token, _) = message.into_parts();
    let capability = inner.spi.capabilities().settlement();
    if let Some(token) = token {
        if !token.belongs_to(subscription_id) {
            inner.emit_internal(
                "settlement",
                "provider settlement token belongs to another subscription".into(),
            );
            return;
        }
        if capability == crate::spi::SettlementCapabilities::AcceptRetryReject {
            let (settled, wait) = mpsc::sync_channel(0);
            drop(wait);
            pending_settlements.push_back(CoordinatorMessage::Settlement {
                token: Some(token),
                disposition: DeliveryDisposition::Retry,
                event_id,
                topic: address.as_str().into(),
                subscription_id,
                subscriber_id: subscriber_id.clone(),
                settled: Some(settled),
            });
            return;
        }
    }
    inner.emit(Diagnostic::SettlementUnavailable {
        event_id,
        topic: address.as_str().into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        requested: DeliveryDisposition::Retry,
    });
}

/// Processes one provider message, including middleware, retry and settlement.
// These independent borrowed pipeline inputs preserve their source lifetimes;
// combining them would add a one-use context type without reducing ownership.
#[allow(clippy::too_many_arguments)]
fn process_inbound<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    topic: &Topic<T>,
    options: &crate::model::SubscribeOptions<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    message: InboundMessage,
) where
    T: Send + Sync + 'static,
{
    let (address, event_id, timestamp, headers, ordering_key, payload, mut settlement, provider_metadata) =
        message.into_parts();
    let fallback_event_id = event_id.clone();
    let fallback_topic = address.as_str().to_owned();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        process_inbound_parts(
            inner,
            settler,
            subscription_id,
            subscriber_id,
            topic,
            options,
            handler,
            address,
            event_id,
            timestamp,
            headers,
            ordering_key,
            payload,
            &mut settlement,
            provider_metadata,
        );
    }));
    if let Err(payload) = result {
        inner.emit_internal("delivery_worker", panic_message(payload.as_ref()).into());
        if let Some(token) = settlement.take() {
            if token.belongs_to(subscription_id)
                && inner.spi.capabilities().settlement() == crate::spi::SettlementCapabilities::AcceptRetryReject
            {
                settler.settle(
                    Some(token),
                    DeliveryDisposition::Retry,
                    fallback_event_id,
                    &fallback_topic,
                    subscription_id,
                    subscriber_id,
                );
            } else {
                inner.emit(Diagnostic::SettlementUnavailable {
                    event_id: fallback_event_id,
                    topic: fallback_topic.into(),
                    subscription_id,
                    subscriber_id: subscriber_id.clone(),
                    requested: DeliveryDisposition::Retry,
                });
            }
        }
    }
}

/// Processes message parts while keeping the settlement token available to the
/// caller's panic recovery.
// This helper keeps transport fields separate so the settlement token remains
// borrowed by the outer panic-recovery scope.
#[allow(clippy::too_many_arguments)]
fn process_inbound_parts<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    topic: &Topic<T>,
    options: &crate::model::SubscribeOptions<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    address: TopicAddress,
    event_id: EventId,
    timestamp: std::time::SystemTime,
    headers: crate::model::Headers,
    ordering_key: Option<crate::spi::OrderingKey>,
    payload: TransportPayload,
    settlement: &mut Option<SettlementToken>,
    provider_metadata: crate::model::ProviderMessageMetadata,
) where
    T: Send + Sync + 'static,
{
    let payload = match decode_payload(topic, payload) {
        Ok(payload) => payload,
        Err(error) => {
            settle_rejected(
                inner,
                settler,
                settlement.take(),
                subscription_id,
                subscriber_id,
                event_id,
                address.as_str(),
                error,
            );
            return;
        }
    };
    let mut event = EventEnvelope::with_id_and_shared_payload(topic.clone(), payload, event_id);
    event.timestamp = timestamp;
    event.headers = headers;
    event.ordering_key = ordering_key.map(|value| value.as_str().into());
    let event = Arc::new(event);
    if let Some(filter) = options.filter() {
        let accepted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| filter(&event)));
        match accepted {
            Ok(false) => {
                settle_token(
                    inner,
                    settler,
                    settlement.take(),
                    DeliveryDisposition::Accept,
                    &event,
                    subscription_id,
                    subscriber_id,
                );
                return;
            }
            Ok(true) => {}
            Err(_) => {
                let error = DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("subscriber filter panicked")),
                };
                let directive = notify_error_handlers(inner, options, &event, &error);
                finish_failed_delivery(
                    inner,
                    settler,
                    settlement.take(),
                    event,
                    subscription_id,
                    subscriber_id,
                    options,
                    error,
                    1,
                    directive,
                );
                return;
            }
        }
    }

    let context = DeliveryContext::new(inner.provider_id.clone(), subscription_id, subscriber_id.clone())
        .with_provider_metadata(provider_metadata)
        .with_settlement(settlement.is_some());
    let context = if event.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE) {
        context.as_dead_letter()
    } else {
        context
    };
    let delivery = Delivery::new(event.clone(), context);
    let global_interceptors = inner.facade_config.subscriber_interceptors::<T>();
    let outcome = run_delivery_with_retry(inner, options, delivery, handler, &global_interceptors);
    match outcome {
        Ok(()) => settle_token(
            inner,
            settler,
            settlement.take(),
            DeliveryDisposition::Accept,
            &event,
            subscription_id,
            subscriber_id,
        ),
        Err((error, attempts, directive)) => finish_failed_delivery(
            inner,
            settler,
            settlement.take(),
            event,
            subscription_id,
            subscriber_id,
            options,
            error,
            attempts,
            directive,
        ),
    }
}

/// Creates a typed event from a provider message using native downcast or topic
/// codec.
fn decode_payload<T: Send + Sync + 'static>(
    topic: &Topic<T>,
    payload: TransportPayload,
) -> Result<Arc<T>, DeliveryError> {
    match payload {
        TransportPayload::Native(value) => {
            Arc::downcast::<T>(value).map_err(|_| decode_error("native payload type does not match subscribed topic"))
        }
        TransportPayload::Encoded(encoded) => {
            let codec = topic
                .codec()
                .ok_or_else(|| decode_error("encoded payload has no topic codec"))?;
            codec.decode(encoded.bytes()).map(Arc::new).map_err(Into::into)
        }
    }
}

/// Creates a source-preserving codec error for invalid transport payload
/// representation.
fn decode_error(message: &'static str) -> DeliveryError {
    crate::error::CodecError::Decode {
        source: Box::new(std::io::Error::other(message)),
    }
    .into()
}

/// Applies configured retry to middleware and one handler attempt.
fn run_delivery_with_retry<T>(
    inner: &EventBusInner,
    options: &crate::model::SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    global_interceptors: &[Arc<crate::model::SubscriberInterceptor<T>>],
) -> Result<(), (DeliveryError, u32, FailureDirective)>
where
    T: Send + Sync + 'static,
{
    let Some(policy) = options.retry_policy() else {
        let event = delivery.event_arc();
        return match run_delivery_attempt(options, delivery, handler, 1, global_interceptors) {
            DeliveryOutcome::Success => Ok(()),
            DeliveryOutcome::Failure(error) => {
                let directive = notify_error_handlers(inner, options, &event, &error);
                let directive = if directive == FailureDirective::Retry {
                    FailureDirective::Discard
                } else {
                    directive
                };
                Err((error, 1, directive))
            }
        };
    };
    let terminal_directive = Arc::new(Mutex::new(None::<FailureDirective>));
    let directive_for_rule = terminal_directive.clone();
    let user_rule = options.retry_rule().cloned();
    let builder = RetryConfig::<DeliveryAttemptError>::builder()
        .policy(policy.clone())
        .fallback(RetryFallback::Abort)
        .rule(
            move |failure: &AttemptFailure<DeliveryAttemptError>, context: &RetryContext| {
                let directive = directive_for_rule
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .unwrap_or(FailureDirective::Discard);
                if directive != FailureDirective::Retry {
                    return RetryDecision::Abort;
                }
                match user_rule.as_ref().map(|rule| rule.decide(failure, context)) {
                    Some(decision) if decision != RetryDecision::UseDefault => decision,
                    _ => match failure.as_error().and_then(DeliveryAttemptError::retryable) {
                        Some(true) => RetryDecision::Retry,
                        Some(false) => RetryDecision::Abort,
                        None => RetryDecision::UseDefault,
                    },
                }
            },
        );
    let config = builder.build().map_err(|error| {
        (
            DeliveryError::Handler {
                source: Box::new(error),
            },
            0,
            FailureDirective::Discard,
        )
    })?;
    let mut retry = Retry::new(&config);
    if let Some(token) = options.retry_cancellation_token() {
        retry = retry.cancellation_token(token.clone());
    }
    let attempts = std::sync::atomic::AtomicU32::new(0);
    let event = delivery.event_arc();
    match retry.run(|| {
        let attempt = attempts.fetch_add(1, Ordering::AcqRel) + 1;
        match run_delivery_attempt(
            options,
            delivery.next_attempt(attempt),
            handler,
            attempt,
            global_interceptors,
        ) {
            DeliveryOutcome::Success => Ok(()),
            DeliveryOutcome::Failure(error) => {
                let directive = notify_error_handlers(inner, options, &event, &error);
                *terminal_directive
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(directive);
                Err(DeliveryAttemptError::new(
                    "delivery",
                    Some(directive == FailureDirective::Retry),
                    error,
                ))
            }
        }
    }) {
        Ok(_) => Ok(()),
        Err(error) => {
            let count = attempts.load(Ordering::Acquire);
            let retry_rule_panicked = matches!(
                error.reason(),
                RetryErrorReason::CallbackFailed { callback }
                    if callback.callback() == RetryCallbackKind::Rule
            );
            let directive = terminal_directive
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .unwrap_or(FailureDirective::Discard);
            let directive = if retry_rule_panicked {
                inner.emit_internal("retry_rule", error.to_string());
                FailureDirective::Requeue
            } else if directive == FailureDirective::Retry {
                FailureDirective::Discard
            } else {
                directive
            };
            Err((SubscriberPipeline::retry_error(error), count, directive))
        }
    }
}

/// Runs each terminal action callback in registration order and chooses a safe
/// directive.
fn notify_error_handlers<T>(
    inner: &EventBusInner,
    options: &crate::model::SubscribeOptions<T>,
    event: &EventEnvelope<T>,
    error: &DeliveryError,
) -> FailureDirective {
    if options.error_handlers().is_empty() {
        return if options.retry_policy().is_some() {
            FailureDirective::Retry
        } else {
            FailureDirective::Discard
        };
    }
    let mut requested_retry = false;
    let mut abort_directive = None;
    for handler in options.error_handlers() {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(event, error))) {
            Ok(FailureDirective::Retry) => requested_retry = true,
            Ok(directive) => {
                abort_directive.get_or_insert(directive);
            }
            Err(payload) => {
                inner.emit_internal("subscriber_error_handler", panic_message(payload.as_ref()).into());
                abort_directive.get_or_insert(FailureDirective::Discard);
            }
        }
    }
    abort_directive.unwrap_or(if requested_retry {
        FailureDirective::Retry
    } else {
        FailureDirective::Discard
    })
}

/// Invokes one handler attempt through global and typed synchronous middleware.
fn run_delivery_attempt<T>(
    options: &crate::model::SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: &Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>,
    _attempt: u32,
    global_interceptors: &[Arc<crate::model::SubscriberInterceptor<T>>],
) -> DeliveryOutcome
where
    T: Send + Sync + 'static,
{
    let handler = handler.clone();
    SubscriberPipeline::attempt_sync(
        options.ack_mode(),
        delivery,
        global_interceptors,
        options.interceptors(),
        move |delivery| handler(delivery),
    )
}

/// Runs terminal subscriber handlers, optional dead-letter publication and
/// settlement.
// Failure settlement requires the original event, token, policy, and attempt
// outcome together; keep these explicit at this private terminal boundary.
#[allow(clippy::too_many_arguments)]
fn finish_failed_delivery<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    token: Option<SettlementToken>,
    event: Arc<EventEnvelope<T>>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    options: &crate::model::SubscribeOptions<T>,
    error: DeliveryError,
    attempts: u32,
    directive: FailureDirective,
) where
    T: Send + Sync + 'static,
{
    let action = SubscriberPipeline::failure_action(directive);
    let capabilities = inner.spi.capabilities().settlement();
    let mut requested_disposition = match action {
        DeliveryFailureAction::Requeue => DeliveryDisposition::Retry,
        DeliveryFailureAction::RetryLocally | DeliveryFailureAction::DeadLetter | DeliveryFailureAction::Discard => {
            DeliveryDisposition::Reject
        }
    };
    let mut disposition = if action == DeliveryFailureAction::DeadLetter {
        None
    } else {
        SubscriberPipeline::failure_disposition(action, capabilities)
    };
    let is_dead_letter = event.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE);
    if action == DeliveryFailureAction::DeadLetter && !is_dead_letter {
        if let Some(policy) = options.dead_letter() {
            let crate::model::DeadLetterPolicy::Topic(dead_letter_topic) = policy;
            let context = DeliveryContext::new(inner.provider_id.clone(), subscription_id, subscriber_id.clone());
            let context =
                if event.header(crate::model::DEAD_LETTER_HEADER) == Some(crate::model::DEAD_LETTER_HEADER_VALUE) {
                    context.as_dead_letter()
                } else {
                    context
                };
            let delivery = Delivery::new(event.clone(), context);
            match dead_letter_envelope(&delivery, &error, dead_letter_topic) {
                Ok(Some(envelope)) => {
                    let request = PublishRequest::from_envelope(envelope);
                    match publish_internal(inner, request) {
                        Ok(_) => {
                            disposition = SubscriberPipeline::failure_disposition(
                                DeliveryFailureAction::DeadLetter,
                                capabilities,
                            );
                        }
                        Err(publish_error) => {
                            inner.emit_internal("dead_letter_publish", publish_error.to_string());
                            requested_disposition = DeliveryDisposition::Retry;
                            if token.as_ref().is_some_and(|token| token.belongs_to(subscription_id)) {
                                disposition = SubscriberPipeline::failure_disposition(
                                    DeliveryFailureAction::Requeue,
                                    capabilities,
                                );
                            }
                        }
                    }
                }
                Ok(None) => {
                    inner.emit_internal("dead_letter_build", "dead-letter event was not created".into());
                    requested_disposition = DeliveryDisposition::Retry;
                    if token.as_ref().is_some_and(|token| token.belongs_to(subscription_id)) {
                        disposition =
                            SubscriberPipeline::failure_disposition(DeliveryFailureAction::Requeue, capabilities);
                    }
                }
                Err(build_error) => {
                    inner.emit_internal("dead_letter_build", build_error.to_string());
                    requested_disposition = DeliveryDisposition::Retry;
                    if token.as_ref().is_some_and(|token| token.belongs_to(subscription_id)) {
                        disposition =
                            SubscriberPipeline::failure_disposition(DeliveryFailureAction::Requeue, capabilities);
                    }
                }
            }
        } else {
            inner.emit_internal(
                "dead_letter_policy",
                "dead-letter directive has no configured topic".into(),
            );
            requested_disposition = DeliveryDisposition::Retry;
            if token.as_ref().is_some_and(|token| token.belongs_to(subscription_id)) {
                disposition = SubscriberPipeline::failure_disposition(DeliveryFailureAction::Requeue, capabilities);
            }
        }
    } else if action == DeliveryFailureAction::DeadLetter {
        disposition = SubscriberPipeline::failure_disposition(DeliveryFailureAction::DeadLetter, capabilities);
    }
    if action == DeliveryFailureAction::RetryLocally && disposition.is_none() {
        requested_disposition = DeliveryDisposition::Reject;
        disposition = SubscriberPipeline::failure_disposition(
            DeliveryFailureAction::Discard,
            inner.spi.capabilities().settlement(),
        );
    }
    if let Some(disposition) = disposition {
        settle_token(
            inner,
            settler,
            token,
            disposition,
            &event,
            subscription_id,
            subscriber_id,
        );
    } else if token.as_ref().is_some_and(|token| !token.belongs_to(subscription_id)) {
        inner.emit_internal(
            "settlement",
            "provider settlement token belongs to another subscription".into(),
        );
    } else {
        inner.emit(Diagnostic::SettlementUnavailable {
            event_id: event.id().clone(),
            topic: event.topic().name().into(),
            subscription_id,
            subscriber_id: subscriber_id.clone(),
            requested: requested_disposition,
        });
    }
    inner.emit(Diagnostic::DeliveryFailed {
        event_id: event.id().clone(),
        topic: event.topic().name().into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        attempts,
        error: error.to_string().into(),
    });
}

/// Settles one provider-issued token and emits a structured failure on error.
fn settle_token<T>(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    token: Option<SettlementToken>,
    disposition: DeliveryDisposition,
    event: &EventEnvelope<T>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
) {
    let Some(token) = token else {
        return;
    };
    if !token.belongs_to(subscription_id) {
        inner.emit_internal(
            "settlement",
            "provider settlement token belongs to another subscription".into(),
        );
        return;
    }
    settler.settle(
        Some(token),
        disposition,
        event.id().clone(),
        event.topic().name(),
        subscription_id,
        subscriber_id,
    );
}

/// Rejects one undecodable provider message, preserving settlement ownership.
// Rejected-message fields retain distinct source/settlement semantics and are
// intentionally explicit at this one-shot SPI settlement boundary.
#[allow(clippy::too_many_arguments)]
fn settle_rejected(
    inner: &Arc<EventBusInner>,
    settler: &OwnerSettlementRouter,
    token: Option<SettlementToken>,
    subscription_id: Id,
    subscriber_id: &SubscriberId,
    event_id: crate::model::EventId,
    topic: &str,
    error: DeliveryError,
) {
    if let Some(token) = token {
        if token.belongs_to(subscription_id) {
            if inner.spi.capabilities().settlement() == crate::spi::SettlementCapabilities::AcceptRetryReject {
                settler.settle(
                    Some(token),
                    DeliveryDisposition::Reject,
                    event_id.clone(),
                    topic,
                    subscription_id,
                    subscriber_id,
                );
            } else {
                inner.emit(Diagnostic::SettlementUnavailable {
                    event_id: event_id.clone(),
                    topic: topic.into(),
                    subscription_id,
                    subscriber_id: subscriber_id.clone(),
                    requested: DeliveryDisposition::Reject,
                });
            }
        } else {
            inner.emit_internal(
                "settlement",
                "provider settlement token belongs to another subscription".into(),
            );
        }
    }
    inner.emit(Diagnostic::DeliveryFailed {
        event_id,
        topic: topic.into(),
        subscription_id,
        subscriber_id: subscriber_id.clone(),
        attempts: 0,
        error: error.to_string().into(),
    });
}

/// Publishes an internally constructed record during graceful shutdown drain.
fn publish_internal<T: Send + Sync + 'static>(
    inner: &EventBusInner,
    request: PublishRequest<T>,
) -> Result<PublishReceipt, PublishError> {
    let observers = inner.observer_snapshot();
    inner
        .publisher
        .publish(inner.spi.as_ref(), request, &[], &observers)
        .map_err(publish_pipeline_error)
}

/// Formats panic payloads without exposing arbitrary panic internals.
fn panic_message(payload: &(dyn Any + Send)) -> &'static str {
    if payload.is::<&'static str>() || payload.is::<String>() {
        "user or provider callback panicked"
    } else {
        "callback panicked with a non-string payload"
    }
}

#[cfg(test)]
#[path = "../../tests/support/spawn_failure_tests.rs"]
mod spawn_failure_tests;

/// Converts one panicking provider operation into a source-preserving SPI
/// error.
#[allow(dead_code)]
fn provider_panic(provider_id: &ProviderId, operation: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: provider_id.as_str().into(),
        operation,
        resource: None,
        kind: "spi_panicked",
        retryable: None,
        source: Box::new(std::io::Error::other("provider SPI panicked")),
    }
}
