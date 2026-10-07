// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Caller-driven receiver loop with independent handler and settlement
//! progress.

use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::io::Error;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use super::runner_event::RunnerEvent;
use crate::DeliveryError;
use crate::Diagnostic;
use crate::ReceiveError;
use crate::SpiError;
use crate::facade::async_event_bus::AsyncEventBusInner;
use crate::facade::async_event_bus::catch_spi_future;
use crate::facade::async_subscription::AsyncRunnerGuard;
use crate::facade::async_subscription::SharedAsyncHandler;
use crate::facade::async_subscription::SignalRegistration;
use crate::facade::async_subscription::internal::AsyncSession;
use crate::facade::async_subscription::internal::AsyncSubscriptionControl;
use crate::facade::async_subscription::internal::OwnedDeliveryTask;
use crate::facade::async_subscription::internal::PendingDelivery;
use crate::facade::async_subscription::internal::owned_delivery_lease::OwnedDeliveryLease;
use crate::model::Delivery;
use crate::model::SubscriptionStopReason;
use crate::spi::ReceiveOutcome;
use crate::spi::ShutdownMode;
use crate::spi::panic_boundary::catch_spi_call;

/// Polls each started task once, rotating pending tasks and releasing H on
/// completion.
///
/// # Type Parameters
/// - `T`: Payload owned by the retained delivery futures.
///
/// # Parameters
/// - `tasks`: Running tasks to poll without changing the receiver owner.
/// - `completed`: Queue receiving completed tasks with their tokens and lanes.
/// - `inner`: Shared scheduler and wake router.
/// - `cx`: Executor context for task and timer registrations.
///
/// # Returns
/// True if at least one task completed. Polling invokes application futures and
/// routes notifications only after each scheduler transition unlocks.
pub(super) fn poll_tasks<T: Send + Sync + 'static>(
    tasks: &mut Vec<OwnedDeliveryTask<T>>,
    completed: &mut VecDeque<PendingDelivery<T>>,
    inner: &AsyncEventBusInner,
    cx: &mut Context<'_>,
) -> bool {
    let mut progress = false;
    let mut index = 0;
    while index < tasks.len() {
        if let Poll::Ready(delivery) = tasks[index].future.as_mut().poll(cx) {
            inner.scheduler.handler_finished(delivery.lease.id);
            drop(tasks.remove(index));
            completed.push_back(delivery);
            progress = true;
        } else {
            index += 1;
        }
    }
    if tasks.len() > 1 {
        tasks.rotate_left(1);
    }
    inner.notify_scheduler();
    progress
}

/// Registers every outstanding retry timer and reports any eligible settlement.
///
/// # Type Parameters
/// - `T`: Payload retained by the completed deliveries.
///
/// # Parameters
/// - `completed`: Deliveries retaining retry deadlines and timer registrations.
/// - `inner`: Bus supplying the shared timer.
/// - `cx`: Executor context receiving timer wakeups.
///
/// # Returns
/// True when settlement or deferred error handling can progress. Clock failures
/// remain in session accounting for the receiver owner to publish terminally.
fn poll_deadlines<T: 'static>(
    completed: &mut VecDeque<PendingDelivery<T>>,
    inner: &AsyncEventBusInner,
    cx: &mut Context<'_>,
) -> bool {
    for delivery in completed {
        let progress = &mut delivery.settlement;
        if progress.cancelled || progress.started_at.is_none() {
            return true;
        }
        match progress.elapsed(inner.timer.as_ref()) {
            Err(_) => return true,
            Ok(elapsed) if elapsed >= progress.due => return true,
            Ok(elapsed) => {
                if progress.timer.is_none() {
                    match inner.timer.after(progress.due.saturating_sub(elapsed)) {
                        Ok(timer) => progress.timer = Some(timer),
                        Err(error) => {
                            progress.infrastructure_error = Some(error);
                            return true;
                        }
                    }
                }
                if let Some(timer) = progress.timer.as_mut()
                    && let Poll::Ready(result) = timer.as_mut().poll(cx)
                {
                    progress.timer = None;
                    if let Err(error) = result {
                        progress.infrastructure_error = Some(error);
                    }
                    return true;
                }
            }
        }
    }
    false
}

impl<T: Send + Sync + 'static> AsyncSession<T> {
    /// Runs to completion while keeping tasks and retry state in the leased
    /// session.
    ///
    /// # Type Parameters
    /// - `T`: Payload carried by this subscription session.
    /// - `H`: Factory used only for newly granted handler work.
    /// - `F`: Application future retained across caller pauses.
    ///
    /// # Parameters
    /// - `handler`: Callback to invoke after a scheduler execution grant.
    /// - `control`: Session coordinator retaining independent close errors.
    ///
    /// # Returns
    /// Success after work and receiver close complete.
    ///
    /// # Errors
    /// Returns the first terminal cause, or a close failure if no earlier cause
    /// exists. Dropping this future pauses owned work and cancels only local
    /// SPI waits.
    pub(in crate::facade) async fn run<H, F>(
        &mut self,
        handler: H,
        control: &AsyncSubscriptionControl<T>,
    ) -> Result<(), ReceiveError>
    where
        H: Fn(Delivery<T>) -> F + Send + Sync + 'static,
        F: Future<Output = Result<(), DeliveryError>> + Send + 'static,
    {
        let _runner = AsyncRunnerGuard::enter(self.inner.tracker.clone());
        let handler: SharedAsyncHandler<T> = Arc::new(move |delivery| Box::pin(handler(delivery)));
        self.handler = Some(handler.clone());
        let result = self.run_loop(handler).await;
        let closed = self.close_inner(control).await;
        result?;
        if let Err(failure) = closed {
            let error = failure.error();
            return Err(ReceiveError::Spi(SpiError::Operation {
                provider_id: error.provider_id().into(),
                operation: error.operation(),
                resource: error.resource().map(Into::into),
                kind: error.kind(),
                retryable: error.retryable(),
                source: Box::new(failure),
            }));
        }
        Ok(())
    }

    /// Drives bounded polls; a receiver operation borrows only the receiver
    /// field.
    ///
    /// # Type Parameters
    /// - `T`: Payload carried by this subscription session.
    ///
    /// # Parameters
    /// - `handler`: Callback retained for work granted during this run.
    ///
    /// # Returns
    /// Success once stop has drained the owned work set.
    ///
    /// # Errors
    /// Returns a retained terminal processing cause or `ReceiveError::Closed`
    /// if an active receiver owner is unexpectedly absent. Handler and SPI
    /// futures are polled on this caller; no background task is created.
    pub(in crate::facade) async fn run_loop(
        &mut self,
        handler: SharedAsyncHandler<T>,
    ) -> Result<(), ReceiveError> {
        self.inner.scheduler.set_dispatch_active(self.id, true);
        self.inner.notify_scheduler();
        let mut turns = 0;
        loop {
            self.completed.append(&mut self.completed_during_settlement);
            turns += 1;
            if turns == 64 {
                turns = 0;
                let mut yielded = false;
                poll_fn(|cx| {
                    if yielded {
                        Poll::Ready(())
                    } else {
                        yielded = true;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
            }
            if self.signals.is_stopped() && !self.signals.stopping_gracefully() {
                let count = self.buffered.len();
                self.buffered.clear();
                for _ in 0..count {
                    self.record_abandoned_delivery();
                }
                self.discard_unstarted_tasks();
                self.inner.scheduler.stop_subscription(self.id);
                if self.signals.terminal_failure().is_some() {
                    self.abandon_queued_and_completed();
                }
            }
            if self.signals.is_stopped() {
                self.inner.scheduler.cancel_receive(self.id);
            }
            if self.settle_ready().await {
                continue;
            }
            let registration = SignalRegistration::new(self.signals.signal());
            let buffered_empty = self.buffered.is_empty();
            let event = poll_fn(|cx| {
                registration.register(cx.waker());
                if poll_tasks(&mut self.tasks, &mut self.completed, &self.inner, cx) {
                    return Poll::Ready(RunnerEvent::Changed);
                }
                if self.signals.is_stopped()
                    && !self.signals.stopping_gracefully()
                    && !buffered_empty
                {
                    return Poll::Ready(RunnerEvent::Changed);
                }
                if (!self.signals.is_stopped() || self.signals.stopping_gracefully())
                    && let Some(id) = self.inner.scheduler.take_ready(self.id)
                {
                    self.inner.notify_scheduler();
                    return Poll::Ready(RunnerEvent::Ready(id));
                }
                if (!self.signals.is_stopped() || self.signals.stopping_gracefully())
                    && let Some(id) = self.inner.scheduler.take_settlement_ready(self.id)
                {
                    self.inner.notify_scheduler();
                    return Poll::Ready(RunnerEvent::Settle(id));
                }
                if poll_deadlines(&mut self.completed, &self.inner, cx) {
                    return Poll::Ready(RunnerEvent::Changed);
                }
                if self.signals.is_stopped() {
                    if self.tasks.is_empty() && self.completed.is_empty() && buffered_empty {
                        return Poll::Ready(RunnerEvent::Finished);
                    }
                    return Poll::Pending;
                }
                self.inner.scheduler.request_receive(self.id);
                let reservation = self.inner.scheduler.take_receive_reservation(self.id);
                self.inner.notify_scheduler();
                if let Some(id) = reservation {
                    return Poll::Ready(RunnerEvent::Reserve(id));
                }
                if self.inner.scheduler.lease_ids_exhausted() {
                    let _ = self.signals.fail_receive(SubscriptionStopReason::Provider {
                        error: Arc::new(SpiError::Operation {
                            provider_id: self.inner.provider_id.as_str().into(),
                            operation: "receive",
                            resource: None,
                            kind: "lease_ids_exhausted",
                            retryable: Some(false),
                            source: Box::new(Error::other(
                                "delivery lease identity space exhausted",
                            )),
                        }),
                    });
                    return Poll::Ready(RunnerEvent::Changed);
                }
                Poll::Pending
            })
            .await;
            drop(registration);
            match event {
                RunnerEvent::Changed => continue,
                RunnerEvent::Finished => {
                    return self.signals.take_terminal_error().map_or(Ok(()), Err);
                }
                RunnerEvent::Ready(id) => {
                    self.dispatch(id, handler.clone());
                }
                RunnerEvent::Settle(id) => {
                    self.dispatch_settlement(id);
                }
                RunnerEvent::Reserve(id) => {
                    let lease = OwnedDeliveryLease::new(id, &self.inner);
                    let provider_id = self.inner.provider_id.clone();
                    let resource = self.subscriber_id.as_str().to_owned();
                    let receiver = self.receiver.as_mut().ok_or(ReceiveError::Closed)?;
                    let receive =
                        catch_spi_call(provider_id.as_str(), "receive", Some(&resource), || {
                            receiver.receive(Duration::MAX)
                        });
                    let mut receive = Box::pin(async {
                        match receive {
                            Ok(future) => {
                                catch_spi_future(future, &provider_id, "receive", Some(&resource))
                                    .await
                            }
                            Err(error) => Err(error),
                        }
                    });
                    let registration = SignalRegistration::new(self.signals.signal());
                    let mut grant = None;
                    let mut settlement_grant = None;
                    let outcome = poll_fn(|cx| {
                        registration.register(cx.waker());
                        if poll_tasks(&mut self.tasks, &mut self.completed, &self.inner, cx) {
                            return Poll::Ready(None);
                        }
                        if self.signals.is_stopped()
                            || poll_deadlines(&mut self.completed, &self.inner, cx)
                        {
                            return Poll::Ready(None);
                        }
                        grant = self.inner.scheduler.take_ready(self.id);
                        if grant.is_none() {
                            settlement_grant = self.inner.scheduler.take_settlement_ready(self.id);
                        }
                        self.inner.notify_scheduler();
                        if grant.is_some() || settlement_grant.is_some() {
                            return Poll::Ready(None);
                        }
                        receive.as_mut().poll(cx).map(Some)
                    })
                    .await;
                    drop(receive);
                    drop(registration);
                    if let Some(grant) = grant {
                        self.dispatch(grant, handler.clone());
                    }
                    if let Some(grant) = settlement_grant {
                        self.dispatch_settlement(grant);
                    }
                    match outcome {
                        Some(Ok(ReceiveOutcome::Message(message))) => {
                            self.prepare_message(message, lease)
                        }
                        Some(Ok(ReceiveOutcome::Gap(gap))) => {
                            self.inner.emit(&Diagnostic::ReceiveGap {
                                subscription_id: self.id,
                                subscriber_id: self.subscriber_id.clone(),
                                topic: self.topic.name().into(),
                                gap: gap.clone(),
                            });
                            if self.options.gap_policy() == crate::model::GapPolicy::Stop {
                                let _ = self.signals.fail_receive(SubscriptionStopReason::Gap {
                                    gap: Arc::new(gap),
                                });
                            }
                        }
                        Some(Ok(ReceiveOutcome::Closed)) => {
                            self.receiver_closed = true;
                            self.signals.stop(ShutdownMode::Immediate);
                        }
                        Some(Err(error)) => {
                            let message = error.to_string();
                            if self.signals.fail_receive(SubscriptionStopReason::Provider {
                                error: Arc::new(error),
                            }) {
                                self.inner.emit(&Diagnostic::InternalFailure {
                                    origin: "receive".into(),
                                    message: message.into(),
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// Converts a consumed scheduler grant into its one owned task.
    ///
    /// # Parameters
    /// - `id`: Lease whose lane and handler slot have already been granted.
    /// - `handler`: Factory captured without invoking user code until task
    ///   polling.
    fn dispatch(&mut self, id: u64, handler: SharedAsyncHandler<T>) {
        if let Some(index) = self
            .buffered
            .iter()
            .position(|pending| pending.lease.id == id)
        {
            let pending = self
                .buffered
                .remove(index)
                .expect("grant identifies queued delivery");
            self.start_pending_task(pending, handler);
        }
    }
    /// Transfers an ordered no-handler grant into the receiver's settlement
    /// queue.
    ///
    /// # Parameters
    /// - `id`: Lease whose ordering lane was granted by the shared scheduler.
    fn dispatch_settlement(&mut self, id: u64) {
        if let Some(index) = self
            .buffered
            .iter()
            .position(|pending| pending.lease.id == id)
        {
            let pending = self
                .buffered
                .remove(index)
                .expect("settlement grant identifies queued delivery");
            self.completed.push_back(pending);
        }
    }
}
