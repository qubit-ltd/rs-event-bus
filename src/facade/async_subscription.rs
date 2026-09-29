// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

// qubit-style: allow multiple-public-types

//! Caller-driven asynchronous subscription runner.

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

pub(super) use internal::is_current_bus_poll;
use qubit_id::Id;
use qubit_retry::AsyncRetry;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryConfig;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryFallback;
use qubit_retry::RetryPolicy;

use self::internal::AsyncSession;
use self::internal::AsyncSubscriptionControl;
use self::internal::SessionSignals;
use super::async_admission::AsyncAdmissionPermit;
use super::async_event_bus::AsyncEventBusInner;
use super::async_event_bus::AsyncRunnerGuard;
use super::async_event_bus::AsyncShutdownDriver;
use super::async_event_bus::AsyncSignal;
use super::async_event_bus::BusState;
use super::async_event_bus::SignalRegistration;
use crate::error::DeliveryAttemptError;
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

mod dead_letter;
mod delivery_task;
mod internal;
mod waiting;

/// Boxed subscriber handler that returns a runtime-neutral future.
type AsyncHandler<T> = dyn Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync;
/// Shared owner of a subscriber handler callback.
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
