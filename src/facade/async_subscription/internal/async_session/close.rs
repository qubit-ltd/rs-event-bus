// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async subscription session close and delivery abandonment.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;

use super::super::AsyncSubscriptionControl;
use crate::Diagnostic;
use crate::error::LifecycleError;
use crate::error::ReceiveError;
use crate::error::SubscriptionCloseFailure;
use crate::facade::async_event_bus::catch_cleanup_spi_future;
use crate::facade::async_subscription::AsyncSession;
use crate::facade::async_subscription::BusContextFuture;
use crate::facade::async_subscription::BusState;
use crate::facade::async_subscription::SubscriptionCloseErrors;
use crate::facade::async_subscription::with_subscription_scope;
use crate::internal::panic_payload::dispose_panic_payload;
use crate::pipeline::format_diagnostic_error;
use crate::spi::DurabilityCapability;
use crate::spi::ShutdownMode;
use crate::spi::panic_boundary::catch_spi_call;

impl<T: Send + Sync + 'static> AsyncSession<T> {
    /// Reports an error left by a resumed runner and safely releases it.
    ///
    /// # Parameters
    /// - `origin`: diagnostic origin associated with the recovery path.
    /// - `error`: one-shot receive error returned by the resumed runner.
    pub(in crate::facade) fn report_resume_error(&self, origin: &'static str, error: ReceiveError) {
        let bus_key = Arc::as_ptr(&self.inner) as usize;
        with_subscription_scope(bus_key, self.id, || {
            self.inner.emit(&Diagnostic::InternalFailure {
                origin: origin.into(),
                message: format_diagnostic_error(&error, "resume error formatting panicked").into(),
            });
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(error))) {
                dispose_panic_payload(payload);
            }
        });
    }

    /// Stops this receiver and asynchronously releases provider resources.
    ///
    /// # Parameters
    /// - `control`: coordinator retaining close failures for bus shutdown.
    ///
    /// # Returns
    /// `Ok(())` after close succeeds or when the bus already closed.
    ///
    /// # Errors
    /// Returns a lifecycle error containing the provider receiver close
    /// failure.
    pub(in crate::facade) async fn close(
        &mut self,
        control: &AsyncSubscriptionControl<T>,
    ) -> Result<(), LifecycleError> {
        if *self.inner.state.lock().unwrap_or_else(PoisonError::into_inner) == BusState::Closed {
            return Ok(());
        }
        self.signals.stop(ShutdownMode::Immediate);
        if let Some(handler) = self.handler.clone()
            && let Err(error) = self.run_loop(handler).await
            && !matches!(error, ReceiveError::Stopped(_))
        {
            self.report_resume_error("close_resume", error);
        }
        match self.close_inner(control).await {
            Ok(()) => Ok(()),
            Err(failure) => Err(LifecycleError::SubscriptionClose(Arc::new(
                SubscriptionCloseErrors::from_failures(vec![failure]),
            ))),
        }
    }

    /// Closes the provider receiver and unregisters it after successful close.
    ///
    /// On success, any remaining facade-owned delivery is abandoned; an
    /// ephemeral provider's shutdown report counts that delivery. If provider
    /// close fails, the pending delivery remains available for a close retry.
    ///
    /// # Parameters
    /// - `control`: owner that stores the canonical close failure.
    ///
    /// # Returns
    /// Success or the canonical provider close failure.
    ///
    /// # Errors
    /// Returns the canonical failure recorded for this receiver's close.
    pub(in crate::facade) async fn close_inner(
        &mut self,
        control: &AsyncSubscriptionControl<T>,
    ) -> Result<(), Arc<SubscriptionCloseFailure>> {
        self.signals.stop(ShutdownMode::Immediate);
        self.inner.scheduler.stop_subscription(self.id);
        self.inner.notify_scheduler();
        let _close = {
            let state = self.inner.state.lock().unwrap_or_else(PoisonError::into_inner);
            (*state == BusState::Running).then(|| self.inner.tracker.close_started())
        };
        let bus_key = Arc::as_ptr(&self.inner) as usize;
        let subscription_id = self.id;
        let close_result = if let Some(receiver) = self.receiver.as_mut() {
            BusContextFuture::new_for_subscription(bus_key, subscription_id, async {
                let close = catch_spi_call(
                    self.inner.provider_id.as_str(),
                    "close",
                    Some(self.subscriber_id.as_str()),
                    || receiver.close(),
                )?;
                catch_cleanup_spi_future(
                    close,
                    &self.inner.provider_id,
                    "close",
                    Some(self.subscriber_id.as_str()),
                )
                .await
            })
            .await
        } else {
            Ok(())
        };
        if let Err(error) = close_result {
            let failure = with_subscription_scope(bus_key, subscription_id, || {
                self.inner.record_close_error(control, &self.subscriber_id, error)
            });
            return Err(failure);
        }
        let receiver_drop = with_subscription_scope(bus_key, subscription_id, || {
            self.abandon_queued_and_completed();
            catch_unwind(AssertUnwindSafe(|| drop(self.receiver.take())))
        });
        let _ = self.inner.scheduler.unregister(self.id);
        self.inner.notify_scheduler();
        {
            self.inner
                .controls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&self.id);
        }
        let handler_drop = with_subscription_scope(bus_key, subscription_id, || {
            catch_unwind(AssertUnwindSafe(|| drop(self.handler.take())))
        });
        control.mark_stopped();
        if let Err(primary) = receiver_drop {
            if let Err(secondary) = handler_drop {
                with_subscription_scope(bus_key, subscription_id, || dispose_panic_payload(secondary));
            }
            resume_unwind(primary);
        }
        if let Err(payload) = handler_drop {
            resume_unwind(payload);
        }
        Ok(())
    }

    /// Records one facade-owned delivery abandoned by an ephemeral provider.
    ///
    /// This updates the bus shutdown report count only when provider durability
    /// does not promise recovery.
    pub(in crate::facade) fn record_abandoned_delivery(&self) {
        if self.inner.capabilities.durability() == DurabilityCapability::Ephemeral {
            self.inner.abandoned_deliveries.fetch_add(1, Ordering::AcqRel);
            self.metrics.record_abandoned_ephemeral();
        }
    }

    /// Counts and releases unresolved buffered and completed deliveries.
    pub(super) fn abandon_queued_and_completed(&mut self) {
        let mut drop_panicked = false;
        while let Some(mut delivery) = self.buffered.pop_front() {
            self.record_abandoned_delivery();
            self.emit_failure_diagnostic(
                delivery.failure_diagnostic.take(),
                delivery.event_id.clone(),
                self.topic.name().into(),
            );
            drop_panicked |= self.drop_abandoned_delivery(delivery);
        }
        while let Some(mut delivery) = self.completed.pop_front() {
            if !delivery.settlement.confirmed {
                self.record_abandoned_delivery();
            }
            self.emit_failure_diagnostic(
                delivery.failure_diagnostic.take(),
                delivery.event_id.clone(),
                self.topic.name().into(),
            );
            drop_panicked |= self.drop_abandoned_delivery(delivery);
        }
        while let Some(mut delivery) = self.completed_during_settlement.pop_front() {
            if !delivery.settlement.confirmed {
                self.record_abandoned_delivery();
            }
            self.emit_failure_diagnostic(
                delivery.failure_diagnostic.take(),
                delivery.event_id.clone(),
                self.topic.name().into(),
            );
            drop_panicked |= self.drop_abandoned_delivery(delivery);
        }
        if drop_panicked {
            self.inner.emit(&Diagnostic::InternalFailure {
                origin: "abandoned_delivery_drop".into(),
                message: "abandoned delivery drop panicked".into(),
            });
        }
    }

    /// Drops one abandoned delivery without interrupting the remaining cleanup.
    ///
    /// # Parameters
    /// - `delivery`: Removed delivery whose payload is being released.
    pub(super) fn drop_abandoned_delivery(
        &self,
        delivery: crate::facade::async_subscription::internal::pending_delivery::PendingDelivery<T>,
    ) -> bool {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(delivery))) {
            dispose_panic_payload(payload);
            true
        } else {
            false
        }
    }

    /// Drops queued handler futures that Immediate stop forbids from starting.
    pub(in crate::facade) fn discard_unstarted_tasks(&mut self) {
        let ephemeral = self.inner.capabilities.durability() == DurabilityCapability::Ephemeral;
        let abandoned = &self.inner.abandoned_deliveries;
        let metrics = &self.metrics;
        self.tasks.retain(|task| {
            let started = task.started.load(Ordering::Acquire);
            if !started && ephemeral {
                abandoned.fetch_add(1, Ordering::AcqRel);
                metrics.record_abandoned_ephemeral();
            }
            started
        });
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use crate::facade::AsyncEventBus;
    use crate::facade::async_subscription::SharedAsyncHandler;
    use crate::facade::async_subscription::internal::owned_delivery_lease::OwnedDeliveryLease;
    use crate::local::LocalEventBusConfig;
    use crate::model::EventId;
    use crate::model::SubscribeRequest;
    use crate::model::Topic;
    use crate::spi::InboundMessage;
    use crate::spi::ShutdownMode;
    use crate::spi::TransportPayload;

    struct Payload {
        panic_on_drop: AtomicBool,
    }

    impl Drop for Payload {
        fn drop(&mut self) {
            if self.panic_on_drop.swap(false, Ordering::AcqRel) {
                panic!("abandoned payload probe");
            }
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        loop {
            if let Poll::Ready(result) = Future::poll(Pin::as_mut(&mut future), &mut context) {
                return result;
            }
        }
    }

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        future.poll(&mut context)
    }

    fn message(event_id: &str, panic_on_drop: bool) -> InboundMessage {
        InboundMessage::new(
            crate::spi::TopicAddress::new("close.test").expect("topic address"),
            EventId::new(event_id).expect("event id"),
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
            TransportPayload::Native(Arc::new(Payload {
                panic_on_drop: AtomicBool::new(panic_on_drop),
            })),
            None,
            Default::default(),
        )
    }

    fn lease_receive<T: Send + Sync + 'static>(
        session: &mut crate::facade::async_subscription::AsyncSession<T>,
        event_id: &str,
        panic_on_drop: bool,
    ) {
        session.inner.scheduler.request_receive(session.id);
        let id = session
            .inner
            .scheduler
            .take_receive_reservation(session.id)
            .expect("receive reservation");
        session.prepare_message(
            message(event_id, panic_on_drop),
            OwnedDeliveryLease::new(id, &session.inner),
        );
    }

    fn setup() -> (AsyncEventBus, crate::facade::AsyncSubscription<Payload>) {
        let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::default())).expect("local bus");
        let subscription = block_on(bus.subscribe(
            SubscribeRequest::new("close-probe", Topic::<Payload>::new("close.test").expect("topic")).expect("request"),
        ))
        .expect("subscription");
        (bus, subscription)
    }

    #[test]
    fn test_discard_unstarted_tasks_counts_removed_delivery_before_drop_panic() {
        let (bus, subscription) = setup();
        let mut lease = block_on(subscription.control.lease()).expect("session lease");
        let session = lease.session.as_mut().expect("leased session");
        session.inner.scheduler.set_dispatch_active(session.id, true);
        lease_receive(session, "ordinary", false);
        let id = session.inner.scheduler.take_ready(session.id).expect("dispatch grant");
        let ordinary = session.buffered.pop_front().expect("ordinary delivery");
        assert_eq!(ordinary.lease.id, id);
        lease_receive(session, "panicking", true);
        let id = session.inner.scheduler.take_ready(session.id).expect("dispatch grant");
        let panicking = session.buffered.pop_front().expect("panicking delivery");
        assert_eq!(panicking.lease.id, id);
        let handler: SharedAsyncHandler<Payload> = Arc::new(|_| Box::pin(std::future::pending()));
        session.start_pending_task(ordinary, handler.clone());
        session.start_pending_task(panicking, handler);
        assert!(poll_once(session.tasks[0].future.as_mut()).is_pending());
        assert!(session.tasks[0].started.load(Ordering::Acquire));
        session.signals.stop(ShutdownMode::Immediate);

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            session.discard_unstarted_tasks();
        }))
        .expect_err("the unstarted task owns a panicking payload");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"abandoned payload probe"));
        assert_eq!(session.tasks.len(), 1);
        assert!(session.tasks[0].started.load(Ordering::Acquire));
        assert_eq!(session.inner.abandoned_deliveries.load(Ordering::Acquire), 1);
        assert_eq!(subscription.delivery_metrics().metrics.abandoned_ephemeral, 1);

        session.discard_unstarted_tasks();
        assert_eq!(session.tasks.len(), 1);
        assert_eq!(session.inner.abandoned_deliveries.load(Ordering::Acquire), 1);
        drop(lease);
        drop(subscription);
        let report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
        assert_eq!(report.known_abandoned_deliveries, 2);
    }

    #[test]
    fn test_terminal_settlement_cleans_completed_deliveries_after_drop_panic() {
        let (bus, subscription) = setup();
        let mut lease = block_on(subscription.control.lease()).expect("session lease");
        let session = lease.session.as_mut().expect("leased session");
        session.inner.scheduler.set_dispatch_active(session.id, true);
        lease_receive(session, "panicking-queued", true);
        let panicking_id = session
            .inner
            .scheduler
            .take_ready(session.id)
            .expect("first dispatch grant");
        session.inner.scheduler.handler_finished(panicking_id);
        lease_receive(session, "ordinary-completed", false);
        let ordinary_id = session
            .inner
            .scheduler
            .take_ready(session.id)
            .expect("second dispatch grant");
        session.inner.scheduler.handler_finished(ordinary_id);
        // Put both deliveries in the queues processed by close, preserving a
        // panicking front item and an ordinary retained item behind it.
        let first = session.buffered.pop_front().expect("first delivery");
        let second = session.buffered.pop_front().expect("second delivery");
        assert_eq!(first.lease.id, panicking_id);
        assert_eq!(second.lease.id, ordinary_id);
        session.completed.push_back(first);
        session.completed_during_settlement.push_back(second);

        let close_result = block_on(session.close_inner(&subscription.control));
        assert!(
            close_result.is_ok(),
            "receiver close continues after delivery drop panic"
        );
        assert!(session.completed.is_empty());
        assert!(session.completed_during_settlement.is_empty());
        assert_eq!(session.inner.abandoned_deliveries.load(Ordering::Acquire), 2);
        assert_eq!(subscription.delivery_metrics().metrics.abandoned_ephemeral, 2);

        session.abandon_queued_and_completed();
        assert!(session.buffered.is_empty());
        assert!(session.completed.is_empty());
        assert!(session.completed_during_settlement.is_empty());
        assert_eq!(session.inner.abandoned_deliveries.load(Ordering::Acquire), 2);
        assert_eq!(subscription.delivery_metrics().metrics.abandoned_ephemeral, 2);
        drop(lease);
        drop(subscription);
        let report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
        assert_eq!(report.known_abandoned_deliveries, 2);
    }
}
