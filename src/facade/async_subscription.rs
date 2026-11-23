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
use std::sync::PoisonError;

use qubit_id::Id;

use self::internal::AsyncSession;
pub(in crate::facade) use self::internal::AsyncSubscriptionControl;
use self::internal::RunStateGuard;
use self::internal::SessionSignals;
use self::internal::is_current_subscription_poll;
use super::async_event_bus::AsyncEventBusInner;
use super::async_event_bus::AsyncRunnerGuard;
use super::async_event_bus::AsyncShutdownDriver;
use super::async_event_bus::BusState;
use super::async_event_bus::SignalRegistration;
use crate::codec::EventCodec;
use crate::error::DeliveryError;
use crate::error::LifecycleError;
use crate::error::ReceiveError;
use crate::error::SubscriptionCloseErrors;
use crate::facade::SubscriptionDeliveryMetricsSnapshot;
use crate::model::Delivery;
use crate::model::FailureDirective;
use crate::model::SubscribeOptions;
use crate::model::SubscriberId;
use crate::model::SubscriptionStopReason;
use crate::model::Topic;
use crate::pipeline::terminal_directive as choose_terminal_directive;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::ShutdownMode;

mod internal;
// Publishes asynchronous dead-letter records through the facade pipeline.
mod dead_letter;
// Runs retry attempts and reports terminal delivery failures.
mod delivery_task;

pub(super) use internal::BusContextFuture;
pub(in crate::facade::async_subscription) use internal::async_handler_owner::AsyncHandlerOwner;
use internal::async_handler_owner::SharedAsyncHandler;
pub(super) use internal::is_current_bus_poll;
pub(super) use internal::with_bus_scope;
pub(super) use internal::with_subscription_scope;

/// Delivery failure, number of attempts, and selected terminal directive.
type RetryFailure = (Box<DeliveryError>, u32, FailureDirective);

/// A typed subscription whose receive loop is driven by the caller's executor.
///
/// Dropping this handle synchronously disposes its control and releases any
/// unstarted or paused session it owns. Destructors for that session's
/// resources run inside the subscription's bus scope, so a nested shutdown
/// on the same bus returns [`LifecycleError::WouldDeadlock`] before changing
/// lifecycle state. This synchronous disposal cannot await provider receiver
/// close; call [`Self::close`] when asynchronous provider cleanup and its
/// errors must be observed.
/// Disposal removes the control from the bus, so deterministic receiver close
/// and error reporting require calling [`Self::close`] before dropping the
/// handle. An ephemeral provider may discard unsettled work on receiver drop.
/// A durable provider must follow its durable recovery contract and must not
/// silently acknowledge unsettled deliveries.
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
#[must_use]
pub struct AsyncSubscription<T: 'static> {
    /// Bus-local object ID used in diagnostics and provider tokens.
    id: Id,
    /// Caller-supplied logical subscriber name.
    subscriber_id: SubscriberId,
    /// Shared session lease, stop signal, and close-error state.
    control: Arc<AsyncSubscriptionControl<T>>,
}

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
    #[must_use = "the subscription handle and shutdown control must be retained"]
    pub(super) fn new(
        inner: Arc<AsyncEventBusInner>,
        id: Id,
        subscriber_id: SubscriberId,
        topic: Topic<T>,
        codec: Option<Arc<dyn EventCodec<T>>>,
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

    /// Returns this subscription's live gauges and counters retained after
    /// close.
    ///
    /// # Returns
    /// Subscription identity, live gauges, and cumulative lifecycle counters.
    pub fn delivery_metrics(&self) -> SubscriptionDeliveryMetricsSnapshot {
        SubscriptionDeliveryMetricsSnapshot {
            subscription_id: self.id,
            subscriber_id: self.subscriber_id.clone(),
            metrics: self.control.delivery_metrics(),
        }
    }

    /// Returns this subscription's bus-local object ID.
    ///
    /// # Returns
    /// The ID used in diagnostics and provider settlement context.
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

    /// Returns the first terminal receive cause, or None before a failure.
    /// The Arc is retained across runner cancellation and receiver close.
    ///
    /// # Returns
    /// `Some` with the immutable first cause after a terminal receive failure;
    /// `None` when no terminal failure has been recorded.
    #[inline]
    pub fn terminal_failure(&self) -> Option<Arc<SubscriptionStopReason>> {
        self.control.signals.terminal_failure()
    }

    /// Returns a snapshot of this subscription runner's lifecycle state.
    ///
    /// The state is instantaneous and can change immediately after it is read.
    /// It describes whether the caller-driven runner is active; it does not
    /// report whether the provider queue is empty.
    #[must_use]
    #[inline]
    pub fn run_state(&self) -> crate::facade::AsyncSubscriptionRunState {
        self.control.run_state()
    }

    /// Runs this subscription until its receiver closes or shutdown stops it.
    ///
    /// The bus-wide delivery scheduling configuration independently bounds
    /// running handlers, owned deliveries, and registered subscriptions.
    /// With [`crate::model::OrderingPolicy::PerKey`], deliveries with the same
    /// ordering key in this subscription retain receive order through
    /// settlement completion. The default
    /// [`crate::model::OrderingPolicy::Unordered`] does not provide this
    /// serialization.
    /// Dropping this future pauses the session. A later call resumes existing
    /// handler futures before using its handler for new messages. Same-bus
    /// shutdown reentry protection also covers destruction of suspended
    /// receive and settlement futures during cancellation.
    ///
    /// # Type Parameters
    /// - `H`: callback type invoked for each newly received delivery.
    /// - `F`: future returned by the handler.
    ///
    /// # Parameters
    /// - `handler`: callback run for each newly received delivery.
    ///
    /// # Returns
    /// `Ok(())` when the provider closes after all facade-owned deliveries
    /// have completed, or shutdown stops the runner. Natural provider closure
    /// stops further receives and drains already received handler and
    /// settlement work. Explicit immediate shutdown retains its cancellation
    /// behavior.
    ///
    /// # Errors
    /// Returns `ReceiveError::Closed` when the session is already closed,
    /// the retained first terminal cause as `ReceiveError::Stopped`, or a
    /// receiver close failure when no earlier cause exists. Calling `run`
    /// recursively from this subscription's handler, receiver-close callback,
    /// protected cleanup destructor, or stop notification wake returns
    /// `ReceiveError::WouldDeadlock` immediately. To run it again, return from
    /// the current callback and start it from an independent caller.
    /// Synchronous waiter wakes caused by returning this runner's session lease
    /// are also covered by the same-bus shutdown reentrancy check.
    /// A failed dead-letter forward returns
    /// [`ReceiveError::DeadLetterForwardFailed`]. The original delivery remains
    /// unsettled, and the error chain contains the forwarding failure for
    /// diagnosis. After the runner stops, subscribe again and let the provider
    /// recover the unsettled delivery according to its recovery contract. See
    /// the dead-letter guidance in the [English user
    /// guide](https://github.com/qubit-ltd/rs-event-bus/blob/c2f37352aa49ae5d02a53bae8a50c5ef73f0ded8/doc/user_guide.md#keep-an-event-that-fails-for-good)
    /// and [Chinese user
    /// guide](https://github.com/qubit-ltd/rs-event-bus/blob/c2f37352aa49ae5d02a53bae8a50c5ef73f0ded8/doc/user_guide.zh_CN.md#%E4%BF%9D%E5%AD%98%E6%9C%80%E7%BB%88%E5%A4%84%E7%90%86%E5%A4%B1%E8%B4%A5%E7%9A%84%E4%BA%8B%E4%BB%B6).
    ///
    /// # Panics
    /// Panics if the session lease invariant is violated internally. If SPI
    /// close succeeds, the handle becomes `Stopped` after local deregistration
    /// and handler cleanup have been attempted, before a receiver or handler
    /// destructor panic is propagated.
    /// Replacing a previously retained handler uses the same-bus subscription
    /// context; its destructor panic is contained and reported as an internal
    /// failure so the new run can proceed.
    pub async fn run<H, F>(&self, handler: H) -> Result<(), ReceiveError>
    where
        H: Fn(Delivery<T>) -> F + Send + Sync + 'static,
        F: Future<Output = Result<(), DeliveryError>> + Send + 'static,
    {
        let bus_key = self.control.bus.as_ptr() as usize;
        if is_current_subscription_poll(bus_key, self.control.id) {
            return Err(ReceiveError::WouldDeadlock);
        }
        let mut lease = self.control.lease().await.ok_or(ReceiveError::Closed)?;
        if !self.control.enter_run() {
            return Err(self
                .control
                .signals
                .terminal_failure()
                .map_or(ReceiveError::Closed, ReceiveError::Stopped));
        }
        let _run_state_guard = RunStateGuard::new(&self.control);
        let result = lease
            .session
            .as_mut()
            .expect("session lease owns its session")
            .run(handler, &self.control)
            .await;
        self.control.mark_stopped();
        result
    }

    /// Stops this session and closes its provider receiver.
    ///
    /// # Returns
    /// `Ok(())` after receiver cleanup completes. Calling `close` again while
    /// the bus is still running returns `Ok(())` without closing the provider
    /// receiver a second time, but only after a prior close succeeded. A failed
    /// provider close retains the receiver so a later call can retry it.
    ///
    /// # Errors
    /// Returns `LifecycleError::Closed` when the bus has begun shutdown. This
    /// error does not indicate that cleanup is complete; continue driving
    /// `AsyncEventBus::shutdown` and inspect its result. Other lifecycle errors
    /// report provider receiver close failures. A repeated close is idempotent
    /// only after success. A failed close may be retried while the bus remains
    /// running; historical errors can still be included in the bus shutdown
    /// result.
    /// Calling shutdown for this bus from the receiver close SPI call returns
    /// [`LifecycleError::WouldDeadlock`]. Request shutdown from an application
    /// lifecycle task after the close callback returns.
    /// Synchronous destruction of session-owned deliveries, receiver, and
    /// handler after a successful close also runs in this bus context; shutdown
    /// requested from those destructors returns `WouldDeadlock`.
    ///
    /// # Panics
    /// Panics if the session lease invariant is violated internally. After a
    /// successful provider close, a panic while dropping the receiver object
    /// is propagated only after local deregistration and handler cleanup have
    /// completed. A handler destructor panic is also propagated after local
    /// deregistration; if both destructors panic, the receiver panic is
    /// propagated and the handler panic payload is safely disposed.
    pub async fn close(&mut self) -> Result<(), LifecycleError> {
        self.control.mark_stopped();
        if self
            .control
            .bus
            .upgrade()
            .is_none_or(|inner| *inner.state.lock().unwrap_or_else(PoisonError::into_inner) != BusState::Running)
        {
            return Err(LifecycleError::Closed);
        }
        self.control.signals.stop(ShutdownMode::Immediate);
        let mut lease = self.control.lease().await.ok_or(LifecycleError::Closed)?;
        lease
            .session
            .as_mut()
            .expect("session lease owns its session")
            .close(&self.control)
            .await
    }
}

impl<T: 'static> Drop for AsyncSubscription<T> {
    /// Disposes the control synchronously and removes it from bus shutdown
    /// tracking.
    ///
    /// This releases unstarted or paused session ownership locally, with
    /// same-bus lifecycle reentrancy checks active during resource
    /// destruction. Provider receiver close remains asynchronous and is not
    /// awaited here; use [`Self::close`] when that cleanup must complete.
    fn drop(&mut self) {
        self.control.dispose();
    }
}

#[cfg(test)]
mod session_lease_registration_tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Waker;

    use crate::facade::AsyncEventBus;
    use crate::local::LocalEventBusConfig;
    use crate::model::SubscribeRequest;
    use crate::model::Topic;
    use crate::spi::ShutdownMode;

    #[derive(Default)]
    struct WakerCounts {
        clones: AtomicUsize,
        drops: AtomicUsize,
    }

    unsafe fn clone_counted(data: *const ()) -> RawWaker {
        // SAFETY: Every raw pointer in this vtable owns an Arc<WakerCounts>.
        let counts = unsafe { &*data.cast::<WakerCounts>() };
        counts.clones.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The returned RawWaker takes ownership of this incremented Arc.
        unsafe { Arc::<WakerCounts>::increment_strong_count(data.cast()) };
        RawWaker::new(data, &COUNTED_VTABLE)
    }

    unsafe fn wake_counted(data: *const ()) {
        // SAFETY: wake consumes its owned Arc reference.
        drop(unsafe { Arc::<WakerCounts>::from_raw(data.cast()) });
    }

    unsafe fn wake_counted_by_ref(_: *const ()) {}

    unsafe fn drop_counted(data: *const ()) {
        // SAFETY: drop consumes its owned Arc reference.
        let counts = unsafe { Arc::<WakerCounts>::from_raw(data.cast()) };
        counts.drops.fetch_add(1, Ordering::Relaxed);
    }

    static COUNTED_VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_counted, wake_counted, wake_counted_by_ref, drop_counted);

    fn counted_waker(counts: &Arc<WakerCounts>) -> Waker {
        let data = Arc::into_raw(Arc::clone(counts)).cast();
        // SAFETY: COUNTED_VTABLE maintains one Arc reference per RawWaker.
        unsafe { Waker::from_raw(RawWaker::new(data, &COUNTED_VTABLE)) }
    }

    #[test]
    fn test_session_acquisition_drops_its_signal_registration_waker() {
        let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::default())).expect("local bus");
        let topic = Topic::<u32>::new("lease-registration-drop").expect("topic");
        let subscription =
            block_on(bus.subscribe(SubscribeRequest::new("lease-registration", topic).expect("request")))
                .expect("subscription");
        let counts = Arc::new(WakerCounts::default());
        let waker = counted_waker(&counts);
        let mut context = Context::from_waker(&waker);
        let lease = {
            let mut lease_future = Box::pin(subscription.control.lease());
            match Future::poll(Pin::as_mut(&mut lease_future), &mut context) {
                Poll::Ready(Some(lease)) => lease,
                Poll::Ready(None) => panic!("live subscription should yield its session"),
                Poll::Pending => panic!("uncontended session lease should be immediately available"),
            }
        };

        assert_eq!(1, counts.clones.load(Ordering::Relaxed));
        assert_eq!(1, counts.drops.load(Ordering::Relaxed));
        drop(lease);
        let mut subscription = subscription;
        block_on(subscription.close()).expect("close session");
        let _report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown bus");
        drop(waker);
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        loop {
            if let Poll::Ready(output) = Future::poll(Pin::as_mut(&mut future), &mut context) {
                return output;
            }
        }
    }
}
