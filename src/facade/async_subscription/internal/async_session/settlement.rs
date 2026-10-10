// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Serial receiver settlement while other started handlers remain pollable.

use std::future::poll_fn;
use std::io::Error;
use std::sync::Arc;
use std::task::Poll;

use qubit_clock::TimeError;

use crate::Diagnostic;
use crate::SpiError;
use crate::facade::async_event_bus::catch_spi_future;
use crate::facade::async_subscription::internal::AsyncSession;
use crate::facade::async_subscription::internal::settlement_attempt_guard::SettlementAttemptGuard;
use crate::facade::internal::SettlementRetryDecision;
use crate::model::SettlementTermination;
use crate::model::SubscriptionStopReason;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementCapabilities;
use crate::spi::panic_boundary::catch_spi_call;

impl<T: Send + Sync + 'static> AsyncSession<T> {
    /// Executes one due immutable settlement intent, preserving state on
    /// cancellation.
    ///
    /// # Returns
    /// True after a due attempt or terminal transition; false while all
    /// deadlines remain pending. Provider and time errors are stored as the
    /// first terminal cause before callbacks. Dropping the future retains
    /// interrupted accounting. The single receiver remains borrowed
    /// exclusively while started tasks are polled.
    ///
    /// # Panics
    /// Panics if retry state marks an attempt cancelled or exhausted without
    /// retaining the preceding error required to classify that attempt.
    #[must_use]
    pub(super) async fn settle_ready(&mut self) -> bool {
        let mut selected = None;
        for (index, delivery) in self.completed.iter_mut().enumerate() {
            let progress = &mut delivery.settlement;
            if progress.infrastructure_error.is_some()
                || progress.cancelled
                || self.signals.is_stopped()
                || progress.elapsed(self.inner.timer.as_ref()).is_err()
                || progress
                    .elapsed(self.inner.timer.as_ref())
                    .is_ok_and(|elapsed| elapsed >= progress.due)
            {
                selected = Some(index);
                break;
            }
        }
        let Some(index) = selected else {
            return false;
        };
        if self.signals.terminal_failure().is_some() {
            self.abandon_completed(index);
            return true;
        }
        let pending = &mut self.completed[index];
        let Some(disposition) = pending.settlement_intent else {
            self.abandon_completed(index);
            return true;
        };
        if self.signals.is_stopped() && pending.settlement.retry.attempts() > 0 {
            self.abandon_completed(index);
            return true;
        }
        let Some(token) = pending.token.as_ref() else {
            if disposition == DeliveryDisposition::Accept {
                self.finish_completed(index);
            } else {
                let pending = &self.completed[index];
                self.inner.emit(&Diagnostic::SettlementUnavailable {
                    event_id: pending.event_id.clone(),
                    topic: self.topic.name().into(),
                    subscription_id: self.id,
                    subscriber_id: self.subscriber_id.clone(),
                    requested: disposition,
                });
                self.abandon_completed(index);
            }
            return true;
        };
        if !token.belongs_to(self.id) {
            let error = Arc::new(SpiError::InvalidSettlementToken {
                provider_id: self.inner.provider_id.as_str().into(),
                operation: "settle",
                resource: Some(self.subscriber_id.as_str().into()),
                reason: "foreign_owner",
                retryable: Some(false),
                source: Box::new(Error::other(
                    "provider settlement token belongs to another subscription",
                )),
            });
            self.stop_settlement(
                index,
                error,
                SettlementTermination::InvalidToken,
                Some(Diagnostic::InternalFailure {
                    origin: "settlement".into(),
                    message: "provider settlement token belongs to another subscription".into(),
                }),
            );
            return true;
        }
        let capability = self.inner.capabilities.settlement();
        let supported = match disposition {
            DeliveryDisposition::Accept => capability != SettlementCapabilities::None,
            DeliveryDisposition::Retry | DeliveryDisposition::Reject => {
                capability == SettlementCapabilities::AcceptRetryReject
            }
        };
        if !supported {
            self.inner.emit(&Diagnostic::SettlementUnavailable {
                event_id: pending.event_id.clone(),
                topic: self.topic.name().into(),
                subscription_id: self.id,
                subscriber_id: self.subscriber_id.clone(),
                requested: disposition,
            });
            self.finish_completed(index);
            return true;
        }
        if let Some(error) = pending.settlement.infrastructure_error.take() {
            self.stop_infrastructure(index, error, None);
            return true;
        }
        let elapsed = match pending.settlement.elapsed(self.inner.timer.as_ref()) {
            Ok(elapsed) => elapsed,
            Err(error) => {
                self.stop_infrastructure(index, error, None);
                return true;
            }
        };
        if pending.settlement.cancelled {
            pending.settlement.cancelled = false;
            let error = pending
                .settlement
                .last_error
                .as_ref()
                .expect("cancelled attempts preserve context")
                .clone();
            if let SettlementRetryDecision::Stop(termination) = pending.settlement.retry.after_error(&error, elapsed) {
                self.stop_settlement(index, error, termination, None);
                return true;
            }
        }
        if elapsed < pending.settlement.due {
            if pending.settlement.timer.is_none() {
                match self.inner.timer.after(pending.settlement.due.saturating_sub(elapsed)) {
                    Ok(timer) => pending.settlement.timer = Some(timer),
                    Err(error) => self.stop_infrastructure(index, error, None),
                }
            }
            return false;
        }
        pending.settlement.timer = None;
        if pending.settlement.started_at.is_none() {
            pending.settlement.started_at = Some(self.inner.timer.clock().now());
        }
        let attempt = match pending.settlement.retry.admit_attempt(elapsed) {
            Ok(attempt) => attempt,
            Err(termination) => {
                let error = pending
                    .settlement
                    .last_error
                    .as_ref()
                    .expect("exhausted budget retains the prior failure")
                    .clone();
                self.stop_settlement(index, error, termination, None);
                return true;
            }
        };
        let result = self.perform_settlement_attempt(index, disposition, attempt).await;
        self.handle_settlement_result(index, disposition, attempt, result);
        true
    }

    /// Calls the provider while continuing to poll already-started handlers.
    ///
    /// The attempt guard retains interrupted accounting until the SPI future
    /// completes, and completed deliveries are buffered until the token borrow
    /// ends.
    ///
    /// # Parameters
    /// - `index`: Selected delivery whose provider token is being settled.
    /// - `disposition`: Provider action requested by the completed handler.
    /// - `attempt`: One-based attempt number recorded before the SPI call.
    ///
    /// # Returns
    /// `Ok(())` after the provider confirms settlement.
    ///
    /// # Errors
    /// Returns the source-preserving SPI error from invocation or polling.
    ///
    /// # Panics
    /// Panics if the selected delivery no longer has its validated provider
    /// token or the session no longer owns its receiver.
    async fn perform_settlement_attempt(
        &mut self,
        index: usize,
        disposition: DeliveryDisposition,
        attempt: u32,
    ) -> Result<(), SpiError> {
        let pending = &mut self.completed[index];
        let token = pending.token.as_ref().expect("validated settlement token");
        let result = {
            let receiver = self.receiver.as_mut().expect("settlement owner retains receiver");
            let mut guard = SettlementAttemptGuard {
                progress: &mut pending.settlement,
                timer: self.inner.timer.clone(),
                provider_id: self.inner.provider_id.as_str().into(),
                armed: true,
            };
            self.metrics.record_settlement_attempt(attempt);
            let settle = catch_spi_call(
                self.inner.provider_id.as_str(),
                "settle",
                Some(self.subscriber_id.as_str()),
                || receiver.settle(token, disposition),
            );
            let result = match settle {
                Ok(future) => {
                    let mut future = Box::pin(catch_spi_future(
                        future,
                        &self.inner.provider_id,
                        "settle",
                        Some(self.subscriber_id.as_str()),
                    ));
                    // Preserve the selected token borrow while collecting task completions.

                    poll_fn(|cx| {
                        let mut cursor = 0;
                        while cursor < self.tasks.len() {
                            if let Poll::Ready(delivery) = self.tasks[cursor].future.as_mut().poll(cx) {
                                self.inner.scheduler.handler_finished(delivery.lease.id);
                                drop(self.tasks.remove(cursor));
                                self.completed_during_settlement.push_back(delivery);
                            } else {
                                cursor += 1;
                            }
                        }
                        if self.tasks.len() > 1 {
                            self.tasks.rotate_left(1);
                        }
                        self.inner.notify_scheduler();
                        future.as_mut().poll(cx)
                    })
                    .await
                }
                Err(error) => Err(error),
            };
            guard.armed = false;
            drop(guard);
            result
        };
        self.completed.append(&mut self.completed_during_settlement);
        result
    }

    /// Applies the successful result or retry/terminal policy for one attempt.
    ///
    /// # Parameters
    /// - `index`: Selected delivery whose attempt just completed.
    /// - `disposition`: Provider action requested for the delivery.
    /// - `attempt`: One-based attempt number attached to failure diagnostics.
    /// - `result`: Provider confirmation or its source-preserving SPI failure.
    ///
    /// A confirmed result marks ownership settled before completion accounting.
    /// A failed result is retained before elapsed-time and retry policy checks;
    /// terminal errors stop the subscription, while retryable errors schedule
    /// the next attempt and emit the failed-attempt diagnostic.
    fn handle_settlement_result(
        &mut self,
        index: usize,
        disposition: DeliveryDisposition,
        attempt: u32,
        result: Result<(), SpiError>,
    ) {
        match result {
            Ok(()) => {
                self.completed[index].settlement.confirmed = true;
                self.finish_completed(index);
            }
            Err(error) => {
                let error = Arc::new(error);
                let pending = &mut self.completed[index];
                let failure = Diagnostic::SettlementFailed {
                    event_id: pending.event_id.clone(),
                    topic: self.topic.name().into(),
                    subscription_id: self.id,
                    subscriber_id: self.subscriber_id.clone(),
                    disposition,
                    attempt,
                    error: error.clone(),
                };
                pending.settlement.last_error = Some(error.clone());
                let elapsed = match pending.settlement.elapsed(self.inner.timer.as_ref()) {
                    Ok(elapsed) => elapsed,
                    Err(error) => {
                        self.stop_infrastructure(index, error, Some(failure));
                        return;
                    }
                };
                match pending.settlement.retry.after_error(&error, elapsed) {
                    SettlementRetryDecision::Stop(termination) => {
                        self.stop_settlement(index, error, termination, Some(failure))
                    }
                    SettlementRetryDecision::RetryAfter(delay) => {
                        pending.settlement.due = elapsed.saturating_add(delay);
                        match self.inner.timer.after(delay) {
                            Ok(timer) => {
                                pending.settlement.timer = Some(timer);
                                self.inner.emit(&failure);
                            }
                            Err(error) => self.stop_infrastructure(index, error, Some(failure)),
                        }
                    }
                }
            }
        }
    }

    /// Records the first structured terminal cause independently of close
    /// failures.
    ///
    /// # Parameters
    /// - `index`: Selected delivery in the completed queue.
    /// - `error`: Original provider or infrastructure source retained by Arc.
    /// - `termination`: Policy classification for this terminal settlement.
    /// - `preceding`: Optional failed-attempt diagnostic emitted before
    ///   Stopped, but only after the immutable terminal cause has been stored.
    ///
    /// This stops dispatch before observers run, records terminal metrics,
    /// emits at most one SettlementStopped, and releases the actual
    /// delivery owner.
    ///
    /// # Panics
    /// Panics if the selected delivery has no immutable settlement intent.
    fn stop_settlement(
        &mut self,
        index: usize,
        error: Arc<SpiError>,
        termination: SettlementTermination,
        preceding: Option<Diagnostic>,
    ) {
        let pending = &self.completed[index];
        let disposition = pending.settlement_intent.expect("settlement intent is immutable");
        let attempts = pending.settlement.retry.attempts();
        let first = self.signals.fail_receive(SubscriptionStopReason::Settlement {
            event_id: pending.event_id.clone(),
            disposition,
            attempts,
            termination,
            error: error.clone(),
        });
        self.inner.scheduler.stop_subscription(self.id);
        self.metrics.record_terminal_failure();
        if let Some(diagnostic) = preceding {
            self.inner.emit(&diagnostic);
        }
        if pending.settlement.started_at.is_some() {
            match pending.settlement.elapsed(self.inner.timer.as_ref()) {
                Ok(elapsed) => self.metrics.record_settlement_elapsed(elapsed),
                Err(clock_error) => self.inner.emit(&Diagnostic::InternalFailure {
                    origin: "settlement_clock".into(),
                    message: clock_error.to_string().into(),
                }),
            }
        }
        if first {
            self.inner.emit(&Diagnostic::SettlementStopped {
                event_id: pending.event_id.clone(),
                topic: self.topic.name().into(),
                subscription_id: self.id,
                subscriber_id: self.subscriber_id.clone(),
                disposition,
                attempts,
                termination,
                error,
            });
        }
        self.inner.notify_scheduler();
        self.abandon_completed(index);
    }

    /// Converts clock failures to a terminal source-preserving infrastructure
    /// error.
    ///
    /// # Parameters
    /// - `index`: Delivery whose settlement clock failed.
    /// - `source`: Original domain/order/registration/poll failure.
    /// - `preceding`: Failed-attempt diagnostic deferred until after the stop
    ///   gate.
    ///
    /// The resulting SpiError retains `source`; callbacks run after
    /// publication.
    fn stop_infrastructure(&mut self, index: usize, source: TimeError, preceding: Option<Diagnostic>) {
        let error = Arc::new(SpiError::Operation {
            provider_id: self.inner.provider_id.as_str().into(),
            operation: "settle",
            resource: Some(self.subscriber_id.as_str().into()),
            kind: "settlement_clock_failure",
            retryable: Some(false),
            source: Box::new(source),
        });
        self.stop_settlement(index, error, SettlementTermination::InfrastructureFailure, preceding);
    }

    /// Releases successful settlement ownership and deferred handler
    /// diagnostics.
    ///
    /// # Parameters
    /// - `index`: Delivery whose processing or SPI settlement completed.
    ///
    /// A duration error stops lifecycle accounting without undoing a confirmed
    /// SPI response or counting that message as an unresolved abandonment.
    ///
    /// # Panics
    /// Panics if the selected delivery is absent from the completed queue.
    fn finish_completed(&mut self, index: usize) {
        if self.completed[index].settlement.started_at.is_some() {
            match self.completed[index].settlement.elapsed(self.inner.timer.as_ref()) {
                Ok(elapsed) => self.metrics.record_settlement_elapsed(elapsed),
                Err(error) => {
                    self.stop_infrastructure(index, error, None);
                    return;
                }
            }
        }
        self.metrics.record_completed();
        let pending = self.completed.remove(index).expect("selected owned delivery");
        self.emit_failure_diagnostic(
            pending.failure_diagnostic.clone(),
            pending.event_id.clone(),
            self.topic.name().into(),
        );
    }

    /// Releases facade ownership without acknowledging provider-owned durable
    /// state.
    ///
    /// # Parameters
    /// - `index`: Delivery removed from the completed queue.
    ///
    /// Only unresolved ephemeral work is counted as abandoned; confirmed
    /// provider settlement remains a known success even if lifecycle
    /// bookkeeping failed.
    fn abandon_completed(&mut self, index: usize) {
        if let Some(delivery) = self.completed.remove(index)
            && !delivery.settlement.confirmed
        {
            self.record_abandoned_delivery();
        }
    }
}
