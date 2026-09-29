// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async session receive loop and caller-driven runner.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use super::super::AsyncSubscriptionControl;
use crate::DeliveryError;
use crate::Diagnostic;
use crate::ReceiveError;
use crate::facade::async_event_bus::catch_spi_future;
use crate::facade::async_subscription::AsyncRunnerGuard;
use crate::facade::async_subscription::AsyncSession;
use crate::facade::async_subscription::SETTLEMENT_RETRY_BASE_DELAY;
use crate::facade::async_subscription::SETTLEMENT_RETRY_MAX_DELAY;
use crate::facade::async_subscription::SharedAsyncHandler;
use crate::facade::async_subscription::SignalRegistration;
use crate::facade::async_subscription::await_or_stop;
use crate::facade::async_subscription::internal::admission_wait_event::AdmissionWaitEvent;
use crate::facade::async_subscription::internal::async_runner_event::AsyncRunnerEvent;
use crate::facade::async_subscription::internal::owned_delivery_task::discard_unstarted_tasks;
use crate::model::Delivery;
use crate::spi::ReceiveOutcome;
use crate::spi::ShutdownMode;

impl<T: Send + Sync + 'static> AsyncSession<T> {
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
    ///
    /// The runner polls middleware and handler futures inside a bus-scoped
    /// context so direct shutdown awaits on this bus can be rejected; context
    /// does not propagate to application-spawned child tasks.
    ///
    /// # Type Parameters
    /// - `H`: handler factory callable type.
    /// - `F`: future returned for each delivery.
    ///
    /// # Parameters
    /// - `handler`: callback run for each admitted delivery.
    /// - `control`: coordinator that stores receiver close failures.
    ///
    /// # Returns
    /// `Ok(())` when receive ends or stop completes.
    ///
    /// # Errors
    /// Returns provider receive, timer, or terminal delivery processing errors.
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

    /// Selects between receive, completion, admission, settlement, and stop.
    ///
    /// # Parameters
    /// - `handler`: callback used to process newly received deliveries.
    ///
    /// # Returns
    /// `Ok(())` when the session stops cleanly.
    ///
    /// # Errors
    /// Returns receive, timer, or terminal delivery failures.
    pub(in crate::facade) async fn run_loop(&mut self, handler: SharedAsyncHandler<T>) -> Result<(), ReceiveError> {
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
}
