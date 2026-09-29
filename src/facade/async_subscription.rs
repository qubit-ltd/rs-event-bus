// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Caller-driven asynchronous subscription runner.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use qubit_id::Id;

use self::internal::AsyncSession;
pub(in crate::facade) use self::internal::AsyncSubscriptionControl;
use self::internal::SessionSignals;
use super::async_event_bus::AsyncEventBusInner;
use super::async_event_bus::AsyncRunnerGuard;
use super::async_event_bus::AsyncShutdownDriver;
use super::async_event_bus::BusState;
use super::async_event_bus::SignalRegistration;
use crate::error::DeliveryError;
use crate::error::ReceiveError;
use crate::error::SubscriptionCloseErrors;
use crate::model::Delivery;
use crate::model::FailureDirective;
use crate::model::SubscribeOptions;
use crate::model::SubscriberId;
use crate::model::Topic;
use crate::pipeline::terminal_directive as choose_terminal_directive;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::ShutdownMode;
use crate::spi::SpiFuture;

mod internal;
// Publishes asynchronous dead-letter records through the facade pipeline.
mod dead_letter;
// Runs retry attempts and reports terminal delivery failures.
mod delivery_task;

pub(super) use internal::is_current_bus_poll;

/// Boxed subscriber handler that returns a runtime-neutral future.
///
/// # Type Parameters
/// - `T`: payload type accepted by the handler.
type AsyncHandler<T> = dyn Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync;
/// Shared owner of a subscriber handler callback.
///
/// # Type Parameters
/// - `T`: payload type accepted by the handler.
type SharedAsyncHandler<T> = Arc<AsyncHandler<T>>;
/// Delivery failure, number of attempts, and selected terminal directive.
type RetryFailure = (Box<DeliveryError>, u32, FailureDirective);

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
/// use qubit_event_bus::AsyncEventBus;
/// use qubit_event_bus::AsyncSubscription;
/// use qubit_event_bus::local::LocalEventBusConfig;
///
/// use qubit_event_bus::model::SubscribeRequest;
/// use qubit_event_bus::model::Topic;
///
/// async fn close_unstarted() -> Result<(), Box<dyn std::error::Error>> {
///     let bus = AsyncEventBus::local(LocalEventBusConfig::default()).await?;
///     let topic = Topic::<String>::new("orders.created")?;
///     let request = SubscribeRequest::new("audit", topic)?;
///     let mut subscription = bus.subscribe(request).await?;
///     subscription.close().await?;
///     bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await?;
///     Ok(())
/// }
/// ```
///
/// # Type Parameters
/// - `T`: payload type received by this subscription.
pub struct AsyncSubscription<T: 'static> {
    /// Bus-local object ID used in diagnostics and provider tokens.
    id: Id,
    /// Caller-supplied logical subscriber name.
    subscriber_id: SubscriberId,
    /// Shared session lease, stop signal, and close-error state.
    control: Arc<AsyncSubscriptionControl<T>>,
}

/// Initial delay between receiver settlement retries.
const SETTLEMENT_RETRY_BASE_DELAY: Duration = Duration::from_millis(10);
/// Maximum delay between receiver settlement retries.
const SETTLEMENT_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

impl<T: Send + Sync + 'static> AsyncSubscription<T> {
    /// Constructs a public handle and its bus-owned shutdown control.
    ///
    /// # Type Parameters
    /// - `T`: payload type received by the subscription.
    ///
    /// # Parameters
    /// - `inner`: shared bus state.
    /// - `id`: bus-local subscription identity.
    /// - `subscriber_id`: logical subscriber identity.
    /// - `topic`: typed event source.
    /// - `codec`: codec selected for this topic, if any.
    /// - `options`: subscription processing policies.
    /// - `receiver`: provider-owned subscription receiver.
    ///
    /// # Returns
    /// The public subscription handle and its shared shutdown control.
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
    ///
    /// # Returns
    /// The ID used in diagnostics and provider settlement context.
    #[must_use = "the subscription ID is useful for diagnostics and settlement context"]
    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }

    /// Returns the caller-supplied logical subscriber identity.
    ///
    /// # Returns
    /// The validated logical subscriber name.
    #[must_use = "Use the returned subscriber id."]
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
    ///
    /// # Type Parameters
    /// - `H`: handler factory callable type.
    /// - `F`: future returned by the handler.
    ///
    /// # Parameters
    /// - `handler`: callback run for each newly received delivery.
    ///
    /// # Returns
    /// `Ok(())` when the provider closes or shutdown stops the runner.
    ///
    /// # Errors
    /// Returns provider receive, timer, or receiver close failures.
    ///
    /// # Panics
    /// Panics if the session lease invariant is violated internally.
    pub async fn run<H, F>(&mut self, handler: H) -> Result<(), ReceiveError>
    where
        H: Fn(Delivery<T>) -> F + Send + Sync + 'static,
        F: Future<Output = Result<(), DeliveryError>> + Send + 'static,
    {
        if let Some(reason) = self.terminal_failure() {
            return Err(ReceiveError::Stopped(reason));
        }
        let mut lease = self.control.lease().await.ok_or(ReceiveError::Closed)?;
        lease
            .session
            .as_mut()
            .expect("session lease owns its session")
            .run(handler, &self.control)
            .await
    }

    /// Returns the first terminal receive cause, or None before a failure.
    /// The Arc is retained across runner cancellation and receiver close.
    #[must_use]
    pub fn terminal_failure(&self) -> Option<Arc<crate::model::SubscriptionStopReason>> {
        self.control.signals.terminal_failure()
    }

    /// Stops this session and closes its provider receiver.
    ///
    /// # Returns
    /// `Ok(())` after receiver cleanup completes.
    ///
    /// # Errors
    /// Returns a lifecycle error when provider receiver close fails.
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
    /// Signals stop and relinquishes an unstarted session synchronously.
    fn drop(&mut self) {
        self.control.dispose();
    }
}

/// Awaits an operation until it completes or the subscription stops.
///
/// # Type Parameters
/// - `F`: future type being awaited.
///
/// # Parameters
/// - `future`: timer or asynchronous operation.
/// - `control`: session stop signals.
///
/// # Returns
/// `Some` with the future output when complete, or `None` after stop.
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
