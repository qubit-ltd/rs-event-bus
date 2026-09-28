// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Caller-driven asynchronous subscription runner.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use qubit_id::Id;
use qubit_retry::AsyncRetry;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFallback;
use qubit_retry::RetryPolicy;

use self::internal::DeliveryTaskContext;
use self::internal::OwnedDeliveryTask;
use self::internal::PendingDelivery;
use self::internal::SessionSignals;
use self::internal::discard_unstarted_tasks;
use super::async_admission::AsyncAdmissionFuture;
use super::async_admission::AsyncAdmissionPermit;
use super::async_event_bus::AsyncEventBusInner;
use super::async_event_bus::AsyncRunnerGuard;
use super::async_event_bus::AsyncShutdownDriver;
use super::async_event_bus::AsyncSignal;
use super::async_event_bus::BusState;
use super::async_event_bus::SignalRegistration;
use super::async_event_bus::catch_spi_future;
use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;
use crate::error::ReceiveError;
use crate::error::SubscriptionCloseErrors;
use crate::model::DeadLetterAdmissionPolicy;
use crate::model::DeadLetterEvent;
use crate::model::Delivery;
use crate::model::EventEnvelope;
use crate::model::FailureDirective;
use crate::model::PublishReceipt;
use crate::model::SubscribeOptions;
use crate::model::SubscriberId;
use crate::model::Topic;
use crate::pipeline::DeadLetterForwardError;
use crate::pipeline::DeliveryOutcome;
use crate::pipeline::Diagnostic;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::dead_letter_retry_config;
use crate::pipeline::is_retry_rule_failure;
use crate::pipeline::terminal_directive as choose_terminal_directive;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::DeliveryDisposition;
use crate::spi::InboundMessage;
use crate::spi::ReceiveOutcome;
use crate::spi::ShutdownMode;
use crate::spi::SpiFuture;

mod internal;

pub(super) use internal::is_current_bus_poll;

type AsyncHandler<T> = dyn Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync;
type SharedAsyncHandler<T> = Arc<AsyncHandler<T>>;
type RetryFailure = (Box<DeliveryError>, u32, FailureDirective);

/// Runtime state for one typed subscription, including the receiver and all
/// delivery futures that have been accepted by the facade.
struct AsyncSession<T: 'static> {
    inner: Arc<AsyncEventBusInner>,
    id: Id,
    subscriber_id: SubscriberId,
    topic: Topic<T>,
    codec: Option<Arc<dyn crate::codec::EventCodec<T>>>,
    options: SubscribeOptions<T>,
    receiver: Option<Box<dyn AsyncEventSubscriptionSpi>>,
    receiver_closed: bool,
    signals: Arc<SessionSignals>,
    pending: Option<PendingDelivery<T>>,
    waiting_admission: Option<PendingDelivery<T>>,
    tasks: Vec<OwnedDeliveryTask<T>>,
    completed: VecDeque<PendingDelivery<T>>,
    defer_settlement: bool,
    handler: Option<SharedAsyncHandler<T>>,
    admission_waiter: Option<AsyncAdmissionFuture>,
}

enum AsyncRunnerEvent<T: 'static> {
    Delivery(PendingDelivery<T>),
    Receive(Result<ReceiveOutcome, crate::error::SpiError>),
    Stopped,
}

enum AdmissionWaitEvent<T: 'static> {
    Permit(AsyncAdmissionPermit),
    Delivery(Box<PendingDelivery<T>>),
    ImmediateStop,
}

struct SessionSlot<T: 'static> {
    session: Option<AsyncSession<T>>,
    active: bool,
    disposed: bool,
}

pub(super) struct AsyncSubscriptionControl<T: 'static> {
    signals: Arc<SessionSignals>,
    slot: Mutex<SessionSlot<T>>,
    available: AsyncSignal,
    close_error: Mutex<Option<Arc<crate::error::SubscriptionCloseFailure>>>,
    bus: Weak<AsyncEventBusInner>,
    id: Id,
}

impl<T: Send + Sync + 'static> AsyncSubscriptionControl<T> {
    fn new(session: AsyncSession<T>, signals: Arc<SessionSignals>) -> Arc<Self> {
        let bus = Arc::downgrade(&session.inner);
        let id = session.id;
        Arc::new(Self {
            signals,
            slot: Mutex::new(SessionSlot {
                session: Some(session),
                active: false,
                disposed: false,
            }),
            available: AsyncSignal::default(),
            close_error: Mutex::new(None),
            bus,
            id,
        })
    }

    async fn lease(&self) -> Option<SessionLease<'_, T>> {
        let registration = SignalRegistration::new(&self.available);
        let session = std::future::poll_fn(|cx| {
            let mut slot = self.slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if slot.disposed {
                return std::task::Poll::Ready(None);
            }
            if !slot.active
                && let Some(session) = slot.session.take()
            {
                slot.active = true;
                return std::task::Poll::Ready(Some(session));
            }
            registration.register(cx.waker());
            std::task::Poll::Pending
        })
        .await;
        session.map(|session| SessionLease {
            control: self,
            session: Some(session),
        })
    }
}

impl<T: 'static> AsyncSubscriptionControl<T> {
    /// Stops the subscription and relinquishes its receiver on handle drop.
    fn dispose(&self) {
        self.signals.stop(ShutdownMode::Immediate);
        let session = {
            let mut slot = self.slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.disposed = true;
            if slot.active { None } else { slot.session.take() }
        };
        if let Some(bus) = self.bus.upgrade() {
            bus.controls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.id);
        }
        drop(session);
        self.available.notify();
    }
}

impl<T: Send + Sync + 'static> AsyncShutdownDriver for AsyncSubscriptionControl<T> {
    fn stop(&self, mode: ShutdownMode) {
        self.signals.stop(mode);
    }

    fn close_error(&self) -> Option<Arc<crate::error::SubscriptionCloseFailure>> {
        self.close_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn store_close_error(
        &self,
        failure: Arc<crate::error::SubscriptionCloseFailure>,
    ) -> Arc<crate::error::SubscriptionCloseFailure> {
        let mut stored = self
            .close_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stored.get_or_insert(failure).clone()
    }

    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> Pin<Box<dyn Future<Output = Result<(), Arc<crate::error::SubscriptionCloseFailure>>> + Send + 'a>> {
        Box::pin(async move {
            self.signals.stop(mode);
            let Some(mut lease) = self.lease().await else {
                return Ok(());
            };
            let Some(session) = lease.session.as_mut() else {
                return Ok(());
            };
            if let Some(handler) = session.handler.clone()
                && let Err(error) = session.run_loop(handler).await
            {
                session.inner.emit(&Diagnostic::InternalFailure {
                    origin: "shutdown_resume".into(),
                    message: error.to_string().into(),
                });
            }
            session.close_inner(self).await
        })
    }
}

struct SessionLease<'a, T: 'static> {
    control: &'a AsyncSubscriptionControl<T>,
    session: Option<AsyncSession<T>>,
}

impl<T: 'static> Drop for SessionLease<'_, T> {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let mut slot = self
                .control
                .slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if slot.disposed {
                drop(slot);
                drop(session);
                self.control.available.notify();
                return;
            }
            slot.session = Some(session);
            slot.active = false;
            drop(slot);
            self.control.available.notify();
        }
    }
}

/// A typed subscription whose receive loop is driven by the caller's executor.
///
/// Dropping this handle cannot await provider cleanup. An unstarted receiver
/// is released locally; call [`Self::close`] for deterministic asynchronous
/// cleanup and close errors. An ephemeral provider may discard unsettled work
/// on receiver drop. A durable provider must follow its durable recovery
/// contract and must not silently acknowledge unsettled deliveries.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::{AsyncSubscription, DeliveryError};
///
/// async fn run(subscription: &mut AsyncSubscription<String>) -> Result<(), Box<dyn std::error::Error>> {
///     subscription.run(|delivery| async move {
///         consume(delivery.payload()).await?;
///         Ok::<(), DeliveryError>(())
///     }).await?;
///     Ok(())
/// }
///
/// async fn consume(_value: &str) -> Result<(), DeliveryError> { Ok(()) }
/// ```
pub struct AsyncSubscription<T: 'static> {
    id: Id,
    subscriber_id: SubscriberId,
    control: Arc<AsyncSubscriptionControl<T>>,
}

const SETTLEMENT_RETRY_BASE_DELAY: Duration = Duration::from_millis(10);
const SETTLEMENT_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

impl<T: Send + Sync + 'static> AsyncSession<T> {
    // Keep session construction explicit: each argument maps to owned runtime
    // state.
    #[allow(clippy::too_many_arguments)]
    fn new(
        inner: Arc<AsyncEventBusInner>,
        id: Id,
        subscriber_id: SubscriberId,
        topic: Topic<T>,
        codec: Option<Arc<dyn crate::codec::EventCodec<T>>>,
        options: SubscribeOptions<T>,
        receiver: Box<dyn AsyncEventSubscriptionSpi>,
        signals: Arc<SessionSignals>,
    ) -> Self {
        Self {
            inner,
            id,
            subscriber_id,
            topic,
            codec,
            options,
            receiver: Some(receiver),
            receiver_closed: false,
            signals,
            pending: None,
            waiting_admission: None,
            tasks: Vec::new(),
            completed: VecDeque::new(),
            defer_settlement: false,
            handler: None,
            admission_waiter: None,
        }
    }

    /// Runs this subscription until its provider closes or bus shutdown
    /// requests stop.
    ///
    /// This method does not spawn a task. Dropping the returned future cancels
    /// the current receive wait but pauses, rather than cancels, already-owned
    /// delivery futures and their admission permits. Calling `run` again
    /// resumes those futures; the new handler is used only for subsequently
    /// received messages. Bus shutdown can also take over and drain a paused
    /// session. The SPI contract requires a cancelled receive future to
    /// preserve any message already received from the provider.
    /// The runner polls middleware and handler futures inside a bus-scoped
    /// context so direct shutdown awaits on this bus can be rejected; context
    /// does not propagate to application-spawned child tasks.
    async fn run<H, F>(&mut self, handler: H, control: &AsyncSubscriptionControl<T>) -> Result<(), ReceiveError>
    where
        H: Fn(Delivery<T>) -> F + Send + Sync + 'static,
        F: Future<Output = Result<(), DeliveryError>> + Send + 'static,
    {
        let _runner = AsyncRunnerGuard::enter(self.inner.tracker.clone());
        let handler: SharedAsyncHandler<T> = Arc::new(move |delivery| Box::pin(handler(delivery)));
        self.handler = Some(handler.clone());
        let result = self.run_loop(handler).await;
        if let Err(failure) = self.close_inner(control).await {
            let error = failure.error();
            return Err(ReceiveError::Spi(crate::error::SpiError::Operation {
                provider_id: error.provider_id().into(),
                operation: error.operation(),
                resource: error.resource().map(Into::into),
                kind: error.kind(),
                retryable: error.retryable(),
                source: Box::new(failure),
            }));
        }
        result
    }

    /// Stops this receiver and asynchronously releases provider resources.
    async fn close(&mut self, control: &AsyncSubscriptionControl<T>) -> Result<(), crate::error::LifecycleError> {
        if *self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == BusState::Closed
        {
            return Ok(());
        }
        self.signals.stop(ShutdownMode::Immediate);
        if let Some(handler) = self.handler.clone()
            && let Err(error) = self.run_loop(handler).await
        {
            self.inner.emit(&Diagnostic::InternalFailure {
                origin: "close_resume".into(),
                message: error.to_string().into(),
            });
        }
        match self.close_inner(control).await {
            Ok(()) => Ok(()),
            Err(failure) => Err(crate::error::LifecycleError::SubscriptionClose(Arc::new(
                SubscriptionCloseErrors::from_failures(vec![failure]),
            ))),
        }
    }

    async fn close_inner(
        &mut self,
        control: &AsyncSubscriptionControl<T>,
    ) -> Result<(), Arc<crate::error::SubscriptionCloseFailure>> {
        self.signals.stop(ShutdownMode::Immediate);
        self.admission_waiter.take();
        let _close = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (*state == BusState::Running).then(|| self.inner.tracker.close_started())
        };
        let close_result = {
            if let Some(receiver) = self.receiver.as_mut() {
                let close = crate::spi::panic_boundary::catch_spi_call(
                    self.inner.provider_id.as_str(),
                    "close",
                    Some(self.subscriber_id.as_str()),
                    || receiver.close(),
                );
                match close {
                    Ok(future) => {
                        catch_spi_future(
                            future,
                            &self.inner.provider_id,
                            "close",
                            Some(self.subscriber_id.as_str()),
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            } else {
                Ok(())
            }
        };
        if let Err(error) = close_result {
            let failure = self.inner.record_close_error(control, &self.subscriber_id, error);
            return Err(failure);
        }
        self.receiver.take();
        self.inner
            .controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
        Ok(())
    }

    async fn run_loop(&mut self, handler: SharedAsyncHandler<T>) -> Result<(), ReceiveError> {
        loop {
            if self.pending.as_ref().is_some_and(|pending| pending.admission.is_none())
                && let Some(completed) = self.completed.pop_front()
            {
                self.waiting_admission = self.pending.take();
                self.pending = Some(completed);
            }
            if self.pending.is_none() {
                self.pending = self.waiting_admission.take();
            }
            if self.pending.is_some() {
                if self.pending.as_ref().is_some_and(|pending| pending.admission.is_none()) {
                    let mut admission = self
                        .admission_waiter
                        .take()
                        .unwrap_or_else(|| self.inner.admission.acquire());
                    let registration = SignalRegistration::new(self.signals.signal());
                    let tasks = &mut self.tasks;
                    let abandoned = &self.inner.abandoned_deliveries;
                    let ephemeral = self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral;
                    let event = std::future::poll_fn(|cx| {
                        if self.signals.is_stopped() && !self.signals.stopping_gracefully() {
                            discard_unstarted_tasks(tasks, abandoned, ephemeral);
                            if tasks.is_empty() {
                                return Poll::Ready(AdmissionWaitEvent::ImmediateStop);
                            }
                        }
                        for index in 0..tasks.len() {
                            if let Poll::Ready(delivery) = tasks[index].future.as_mut().poll(cx) {
                                drop(tasks.swap_remove(index));
                                return Poll::Ready(AdmissionWaitEvent::Delivery(Box::new(delivery)));
                            }
                        }
                        if self.signals.is_stopped() && !self.signals.stopping_gracefully() {
                            return Poll::Ready(AdmissionWaitEvent::ImmediateStop);
                        }
                        registration.register(cx.waker());
                        if self.signals.is_stopped() && !self.signals.stopping_gracefully() {
                            return Poll::Ready(AdmissionWaitEvent::ImmediateStop);
                        }
                        match Pin::new(&mut admission).poll(cx) {
                            Poll::Ready(permit) => Poll::Ready(AdmissionWaitEvent::Permit(permit)),
                            Poll::Pending => Poll::Pending,
                        }
                    })
                    .await;
                    drop(registration);
                    match event {
                        AdmissionWaitEvent::Permit(permit) => {
                            if let Some(pending) = self.pending.as_mut() {
                                pending.admission = Some(permit);
                            }
                        }
                        AdmissionWaitEvent::Delivery(delivery) => {
                            self.completed.push_back(*delivery);
                            self.admission_waiter = Some(admission);
                            continue;
                        }
                        AdmissionWaitEvent::ImmediateStop => {
                            self.abandon_pending();
                            continue;
                        }
                    }
                }
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.settlement_intent.is_none())
                {
                    if self.signals.is_stopped() && !self.signals.stopping_gracefully() {
                        self.abandon_pending();
                        continue;
                    }
                    self.start_pending_task(handler.clone());
                    continue;
                }
                let disposition = self.pending.as_ref().and_then(|pending| pending.settlement_intent);
                if let Some(disposition) = disposition {
                    let event = self
                        .pending
                        .as_ref()
                        .and_then(|pending| pending.event.as_deref())
                        .cloned();
                    self.settle_pending(disposition, event.as_ref()).await;
                }
                // A permanent provider settlement failure must not keep
                // shutdown alive forever. Keep retrying while running; once
                // stopped, release the receiver so its close contract can
                // decide the fate of provider-owned in-flight state.
                if self.signals.is_stopped() {
                    self.abandon_pending();
                    continue;
                }
                if let Some(pending) = self
                    .pending
                    .as_ref()
                    .filter(|pending| pending.settlement_intent.is_some())
                {
                    let exponent = pending.settlement_failures.saturating_sub(1).min(7);
                    let delay = SETTLEMENT_RETRY_BASE_DELAY
                        .saturating_mul(1_u32 << exponent)
                        .min(SETTLEMENT_RETRY_MAX_DELAY);
                    let timer = self.inner.timer.after(delay)?;
                    if await_or_stop(timer, &self.signals).await.is_none() {
                        return Ok(());
                    }
                }
                continue;
            }
            if let Some(completed) = self.completed.pop_front() {
                self.pending = Some(completed);
                continue;
            }
            if self.receiver_closed {
                self.signals.stop(ShutdownMode::Immediate);
            }
            if self.signals.is_stopped() {
                if !self.signals.stopping_gracefully() {
                    self.discard_unstarted_tasks();
                }
                if let Some(completed) = self.completed.pop_front() {
                    self.pending = Some(completed);
                    continue;
                }
                if self.tasks.is_empty() {
                    return match self.signals.take_terminal_error() {
                        Some(error) => Err(error),
                        None => Ok(()),
                    };
                }
                let completed = std::future::poll_fn(|cx| {
                    for index in 0..self.tasks.len() {
                        if let Poll::Ready(delivery) = self.tasks[index].future.as_mut().poll(cx) {
                            drop(self.tasks.swap_remove(index));
                            return Poll::Ready(delivery);
                        }
                    }
                    Poll::Pending
                })
                .await;
                self.completed.push_back(completed);
                continue;
            }
            let provider_id = self.inner.provider_id.clone();
            let resource = self.subscriber_id.as_str().to_owned();
            let receiver = self.receiver.as_mut().ok_or(ReceiveError::Closed)?;
            let receive =
                crate::spi::panic_boundary::catch_spi_call(provider_id.as_str(), "receive", Some(&resource), || {
                    receiver.receive(Duration::MAX)
                });
            let mut receive = Box::pin(async move {
                match receive {
                    Ok(future) => catch_spi_future(future, &provider_id, "receive", Some(&resource)).await,
                    Err(error) => Err(error),
                }
            });
            let registration = SignalRegistration::new(self.signals.signal());
            let tasks = &mut self.tasks;
            let abandoned = &self.inner.abandoned_deliveries;
            let ephemeral = self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral;
            let event = std::future::poll_fn(|cx| {
                if self.signals.is_stopped() && !self.signals.stopping_gracefully() {
                    discard_unstarted_tasks(tasks, abandoned, ephemeral);
                    if tasks.is_empty() {
                        return Poll::Ready(AsyncRunnerEvent::Stopped);
                    }
                }
                for index in 0..tasks.len() {
                    if let Poll::Ready(delivery) = tasks[index].future.as_mut().poll(cx) {
                        drop(tasks.swap_remove(index));
                        return Poll::Ready(AsyncRunnerEvent::Delivery(delivery));
                    }
                }
                if self.signals.is_stopped() {
                    return Poll::Ready(AsyncRunnerEvent::Stopped);
                }
                registration.register(cx.waker());
                if self.signals.is_stopped() {
                    return Poll::Ready(AsyncRunnerEvent::Stopped);
                }
                match receive.as_mut().poll(cx) {
                    Poll::Ready(result) => Poll::Ready(AsyncRunnerEvent::Receive(result)),
                    Poll::Pending => Poll::Pending,
                }
            })
            .await;
            drop(receive);
            drop(registration);
            let outcome = match event {
                AsyncRunnerEvent::Delivery(delivery) => {
                    self.completed.push_back(delivery);
                    continue;
                }
                AsyncRunnerEvent::Stopped => continue,
                AsyncRunnerEvent::Receive(result) => result?,
            };
            match outcome {
                ReceiveOutcome::Message(message) => {
                    self.prepare_message(message);
                }
                ReceiveOutcome::Gap(gap) => self.inner.emit(&Diagnostic::ReceiveGap {
                    subscription_id: self.id,
                    subscriber_id: self.subscriber_id.clone(),
                    topic: self.topic.name().into(),
                    gap,
                }),
                ReceiveOutcome::TimedOut => {}
                ReceiveOutcome::Closed => {
                    self.receiver_closed = true;
                    self.signals.stop(ShutdownMode::Immediate);
                }
            }
        }
    }

    /// Records one facade-owned delivery abandoned by an ephemeral provider.
    fn record_abandoned_delivery(&self) {
        if self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral {
            self.inner.abandoned_deliveries.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Drops the current unstarted or unsettled delivery during Immediate stop.
    fn abandon_pending(&mut self) {
        if self.pending.take().is_some() {
            self.record_abandoned_delivery();
        }
    }

    /// Drops queued handler futures that Immediate stop forbids from starting.
    fn discard_unstarted_tasks(&mut self) {
        let ephemeral = self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral;
        discard_unstarted_tasks(&mut self.tasks, &self.inner.abandoned_deliveries, ephemeral);
    }

    fn start_pending_task(&mut self, handler: SharedAsyncHandler<T>) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let started = Arc::new(AtomicBool::new(false));
        let mut task = DeliveryTaskContext {
            inner: self.inner.clone(),
            id: self.id,
            subscriber_id: self.subscriber_id.clone(),
            topic: self.topic.clone(),
            options: self.options.clone(),
            signals: self.signals.clone(),
            pending: Some(pending),
            started: Some(started.clone()),
        };
        self.tasks.push(OwnedDeliveryTask {
            started,
            future: Box::pin(async move {
                task.process_pending(handler).await;
                task.pending
                    .take()
                    .expect("processing task retains delivery until completion")
            }),
        });
    }

    fn prepare_message(&mut self, message: InboundMessage) {
        let tracking = self.inner.tracker.track(self.topic.name());
        let (address, event_id, timestamp, headers, ordering_key, transport_payload, token, provider_metadata) =
            message.into_parts();
        let payload = match crate::codec::decode_payload(self.codec.as_ref(), transport_payload).map_err(Into::into) {
            Ok(payload) => payload,
            Err(error) => {
                self.pending = Some(PendingDelivery {
                    _tracking: tracking,
                    event_id,
                    event: None,
                    token,
                    metadata: provider_metadata,
                    decode_error: Some(error),
                    settlement_intent: None,
                    settlement_failures: 0,
                    failure_diagnostic: None,
                    admission: None,
                    lane: None,
                });
                let _ = address;
                return;
            }
        };
        let mut event = EventEnvelope::with_id_and_shared_payload(self.topic.clone(), payload, event_id);
        event.timestamp = timestamp;
        event.headers = headers;
        event.ordering_key = ordering_key.map(|key| key.as_str().into());
        let event = Arc::new(event);
        let event_id = event.id().clone();
        self.pending = Some(PendingDelivery {
            _tracking: tracking,
            event_id,
            event: Some(event),
            token,
            metadata: provider_metadata,
            decode_error: None,
            settlement_intent: None,
            settlement_failures: 0,
            failure_diagnostic: None,
            admission: None,
            lane: None,
        });
    }

    async fn settle_pending(&mut self, disposition: DeliveryDisposition, event: Option<&EventEnvelope<T>>) {
        let Some(pending) = self.pending.as_mut() else {
            return;
        };
        pending.settlement_intent = Some(disposition);
        if self.defer_settlement {
            return;
        }
        let failure_diagnostic = pending.failure_diagnostic.clone();
        let event_id = event
            .map(|event| event.id().clone())
            .unwrap_or_else(|| pending.event_id.clone());
        let topic = event
            .map(|event| event.topic().name().into())
            .unwrap_or_else(|| self.topic.name().into());
        let Some(token) = pending.token.as_ref() else {
            self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
            self.pending.take();
            return;
        };
        if !token.belongs_to(self.id) {
            self.inner.emit(&Diagnostic::InternalFailure {
                origin: "settlement".into(),
                message: "provider settlement token belongs to another subscription".into(),
            });
            self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
            self.pending.take();
            return;
        }
        let capability = self.inner.capabilities.settlement();
        let supported = match disposition {
            DeliveryDisposition::Accept => capability != crate::spi::SettlementCapabilities::None,
            DeliveryDisposition::Retry | DeliveryDisposition::Reject => {
                capability == crate::spi::SettlementCapabilities::AcceptRetryReject
            }
        };
        if !supported {
            self.inner.emit(&Diagnostic::SettlementUnavailable {
                event_id: event_id.clone(),
                topic: topic.clone(),
                subscription_id: self.id,
                subscriber_id: self.subscriber_id.clone(),
                requested: disposition,
            });
            self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
            self.pending.take();
            return;
        }
        let result = if let Some(receiver) = self.receiver.as_mut() {
            match crate::spi::panic_boundary::catch_spi_call(
                self.inner.provider_id.as_str(),
                "settle",
                Some(self.subscriber_id.as_str()),
                || receiver.settle(token, disposition),
            ) {
                Ok(future) => {
                    catch_spi_future(
                        future,
                        &self.inner.provider_id,
                        "settle",
                        Some(self.subscriber_id.as_str()),
                    )
                    .await
                }
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        };
        match result {
            Ok(()) => {
                self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
                self.pending.take();
            }
            Err(error) => {
                if error.kind() == "provider_panicked" {
                    self.inner.emit(&Diagnostic::SettlementUnavailable {
                        event_id: event_id.clone(),
                        topic: topic.clone(),
                        subscription_id: self.id,
                        subscriber_id: self.subscriber_id.clone(),
                        requested: disposition,
                    });
                    self.inner.emit(&Diagnostic::SettlementFailed {
                        event_id: event_id.clone(),
                        topic: topic.clone(),
                        subscription_id: self.id,
                        subscriber_id: self.subscriber_id.clone(),
                        disposition,
                        error: error.to_string().into(),
                    });
                    self.emit_failure_diagnostic(failure_diagnostic, event_id, topic);
                    self.pending.take();
                    return;
                }
                if let Some(pending) = self.pending.as_mut() {
                    pending.settlement_failures = pending.settlement_failures.saturating_add(1);
                }
                self.inner.emit(&Diagnostic::SettlementFailed {
                    event_id: event_id.clone(),
                    topic: topic.clone(),
                    subscription_id: self.id,
                    subscriber_id: self.subscriber_id.clone(),
                    disposition,
                    error: error.to_string().into(),
                });
            }
        }
    }

    fn emit_failure_diagnostic(
        &self,
        failure: Option<(u32, Box<str>)>,
        event_id: crate::model::EventId,
        topic: Box<str>,
    ) {
        if let Some((attempts, error)) = failure {
            self.inner.emit(&Diagnostic::DeliveryFailed {
                event_id,
                topic,
                subscription_id: self.id,
                subscriber_id: self.subscriber_id.clone(),
                attempts,
                error,
            });
        }
    }
}

async fn publish_dead_letter_async<T: Send + Sync + 'static>(
    inner: &Arc<AsyncEventBusInner>,
    envelope: &EventEnvelope<DeadLetterEvent<T>>,
    retry_policy: Option<&RetryPolicy>,
    cancellation: Option<&RetryCancellationToken>,
    admission_policy: DeadLetterAdmissionPolicy,
) -> Result<PublishReceipt, String> {
    async fn attempt<T: Send + Sync + 'static>(
        inner: &Arc<AsyncEventBusInner>,
        envelope: EventEnvelope<DeadLetterEvent<T>>,
        admission_policy: DeadLetterAdmissionPolicy,
    ) -> Result<PublishReceipt, DeadLetterForwardError> {
        let receipt = inner
            .publisher
            .publish_async(
                inner.spi.as_ref(),
                crate::model::PublishRequest::from_envelope(envelope),
                &[],
                &inner.observer_snapshot(),
                inner.timer.clone(),
            )
            .await
            .map_err(|failure| DeadLetterForwardError::Pipeline(failure.to_string().into()))?;
        if crate::pipeline::dead_letter_was_accepted(&receipt, inner.capabilities, admission_policy) {
            Ok(receipt)
        } else {
            Err(DeadLetterForwardError::NotAdmitted(receipt.admission_outcome()))
        }
    }

    let Some(policy) = retry_policy else {
        return attempt(inner, envelope.clone(), admission_policy)
            .await
            .map_err(|error| error.to_string());
    };
    let config = dead_letter_retry_config(policy).map_err(|error| error.to_string())?;
    let mut retry = AsyncRetry::new(&config).timer(inner.timer.clone());
    if let Some(cancellation) = cancellation {
        retry = retry.cancellation_token(cancellation.clone());
    }
    retry
        .run(|| attempt(inner, envelope.clone(), admission_policy))
        .await
        .map(|success| success.value().clone())
        .map_err(|error| error.to_string())
}

async fn run_with_retry<T: Send + Sync + 'static>(
    options: SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: SharedAsyncHandler<T>,
    inner: &Arc<AsyncEventBusInner>,
    global_interceptors: Vec<Arc<crate::model::AsyncSubscriberInterceptor<T>>>,
) -> Result<u32, RetryFailure> {
    let Some(policy) = options.retry_policy().cloned() else {
        let outcome = run_one_attempt(&options, delivery.clone(), handler, 1, &global_interceptors).await;
        return match outcome {
            DeliveryOutcome::Success => Ok(1),
            DeliveryOutcome::Failure(error) => {
                let directive = notify_failure(&options, delivery.event(), &error, false, inner);
                Err((Box::new(error), 1, directive))
            }
        };
    };
    let directive = Arc::new(Mutex::new(None::<FailureDirective>));
    let rule_directive = directive.clone();
    let user_rule = options.retry_rule().cloned();
    let config = RetryConfig::<DeliveryAttemptError>::builder()
        .policy(policy)
        .fallback(RetryFallback::Abort)
        .rule(
            move |failure: &AttemptFailure<DeliveryAttemptError>, context: &RetryContext| {
                let requested = rule_directive
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .unwrap_or(FailureDirective::Discard);
                if requested != FailureDirective::Retry {
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
        )
        .build()
        .map_err(|error| {
            (
                Box::new(DeliveryError::Handler {
                    source: Box::new(error),
                }),
                0,
                FailureDirective::Discard,
            )
        })?;
    let mut retry = AsyncRetry::new(&config).timer(inner.timer.clone());
    if let Some(cancellation) = options.retry_cancellation_token() {
        retry = retry.cancellation_token(cancellation.clone());
    }
    let attempts = Arc::new(AtomicU32::new(0));
    let operation = || {
        let options = options.clone();
        let delivery = delivery.clone();
        let handler = handler.clone();
        let directive = directive.clone();
        let attempts = attempts.clone();
        let inner = inner.clone();
        let global_interceptors = global_interceptors.clone();
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::AcqRel) + 1;
            match run_one_attempt(&options, delivery.clone(), handler, attempt, &global_interceptors).await {
                DeliveryOutcome::Success => Ok(()),
                DeliveryOutcome::Failure(error) => {
                    let requested = notify_failure(&options, delivery.event(), &error, true, &inner);
                    *directive.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(requested);
                    Err(DeliveryAttemptError::new(
                        "delivery",
                        Some(requested == FailureDirective::Retry),
                        error,
                    ))
                }
            }
        })
    };
    match retry.run(operation).await {
        Ok(_) => Ok(attempts.load(Ordering::Acquire)),
        Err(error) => {
            let count = attempts.load(Ordering::Acquire);
            let requested = directive
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .unwrap_or(FailureDirective::Discard);
            if is_retry_rule_failure(error.reason()) {
                inner.emit(&Diagnostic::InternalFailure {
                    origin: "retry_rule".into(),
                    message: error.to_string().into(),
                });
            }
            let terminal = choose_terminal_directive(error.reason(), requested);
            Err((Box::new(DeliveryError::Retry(Box::new(error))), count, terminal))
        }
    }
}

async fn run_one_attempt<T: Send + Sync + 'static>(
    options: &SubscribeOptions<T>,
    delivery: Delivery<T>,
    handler: SharedAsyncHandler<T>,
    attempt: u32,
    global_interceptors: &[Arc<crate::model::AsyncSubscriberInterceptor<T>>],
) -> DeliveryOutcome {
    let interceptors = options.async_interceptors().to_vec();
    SubscriberPipeline::attempt_async(
        options.ack_mode(),
        delivery.next_attempt(attempt),
        global_interceptors,
        &interceptors,
        move |delivery| handler(delivery),
    )
    .await
}

fn notify_failure<T: Send + Sync + 'static>(
    options: &SubscribeOptions<T>,
    event: &EventEnvelope<T>,
    error: &DeliveryError,
    retry_enabled: bool,
    inner: &AsyncEventBusInner,
) -> FailureDirective {
    if options.error_handlers().is_empty() {
        return if retry_enabled {
            FailureDirective::Retry
        } else {
            FailureDirective::Discard
        };
    }
    let mut directives = Vec::with_capacity(options.error_handlers().len());
    for callback in options.error_handlers() {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(event, error))) {
            Ok(directive) => directives.push(Ok(directive)),
            Err(_) => {
                inner.emit(&Diagnostic::InternalFailure {
                    origin: "subscriber_error_handler".into(),
                    message: "subscriber error handler panicked".into(),
                });
                directives.push(Err(()));
            }
        }
    }
    crate::pipeline::choose_failure_directive(retry_enabled, directives)
}

impl<T: Send + Sync + 'static> AsyncSubscription<T> {
    pub(super) fn new(
        inner: Arc<AsyncEventBusInner>,
        id: Id,
        subscriber_id: SubscriberId,
        topic: Topic<T>,
        codec: Option<Arc<dyn crate::codec::EventCodec<T>>>,
        options: SubscribeOptions<T>,
        receiver: Box<dyn AsyncEventSubscriptionSpi>,
    ) -> (Self, Arc<AsyncSubscriptionControl<T>>) {
        let signals = SessionSignals::new();
        let session = AsyncSession::new(
            inner,
            id,
            subscriber_id.clone(),
            topic,
            codec,
            options,
            receiver,
            signals.clone(),
        );
        let control = AsyncSubscriptionControl::new(session, signals);
        (
            Self {
                id,
                subscriber_id,
                control: control.clone(),
            },
            control,
        )
    }

    /// Returns this subscription's bus-local object ID.
    #[must_use = "the subscription ID is useful for diagnostics and settlement context"]
    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }

    /// Returns the caller-supplied logical subscriber identity.
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }

    /// Runs this subscription until its receiver closes or shutdown stops it.
    ///
    /// The bus-wide `max_in_flight` admission setting bounds the delivery
    /// futures owned by this and other subscriptions. Different ordering keys
    /// may run concurrently; messages with the same key retain receive order.
    /// Dropping this future pauses the session. A later call resumes existing
    /// handler futures before using its handler for new messages.
    pub async fn run<H, F>(&mut self, handler: H) -> Result<(), ReceiveError>
    where
        H: Fn(Delivery<T>) -> F + Send + Sync + 'static,
        F: Future<Output = Result<(), DeliveryError>> + Send + 'static,
    {
        let mut lease = self.control.lease().await.ok_or(ReceiveError::Closed)?;
        lease
            .session
            .as_mut()
            .expect("session lease owns its session")
            .run(handler, &self.control)
            .await
    }

    /// Stops this session and closes its provider receiver.
    pub async fn close(&mut self) -> Result<(), crate::error::LifecycleError> {
        if self.control.bus.upgrade().is_none_or(|inner| {
            *inner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner) != BusState::Running
        }) {
            return Ok(());
        }
        self.control.signals.stop(ShutdownMode::Immediate);
        let mut lease = self.control.lease().await.ok_or(crate::error::LifecycleError::Closed)?;
        lease
            .session
            .as_mut()
            .expect("session lease owns its session")
            .close(&self.control)
            .await
    }
}

impl<T: 'static> Drop for AsyncSubscription<T> {
    fn drop(&mut self) {
        self.control.dispose();
    }
}

async fn await_or_stop<F>(future: F, control: &SessionSignals) -> Option<F::Output>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let registration = SignalRegistration::new(control.signal());
    std::future::poll_fn(|cx| {
        if control.is_stopped() {
            return std::task::Poll::Ready(None);
        }
        registration.register(cx.waker());
        if control.is_stopped() {
            return std::task::Poll::Ready(None);
        }
        match future.as_mut().poll(cx) {
            std::task::Poll::Ready(value) => std::task::Poll::Ready(Some(value)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    })
    .await
}
