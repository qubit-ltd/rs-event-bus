// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus lifecycle operations.

use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use qubit_clock::TimerFuture;
use qubit_id::Id;

use crate::AsyncEventBus;
use crate::LifecycleError;
use crate::ShutdownError;
use crate::ShutdownReport;
use crate::WaitOutcome;
use crate::facade::async_event_bus::BusState;
use crate::facade::async_event_bus::ShutdownLeaderGuard;
use crate::facade::async_event_bus::ShutdownWait;
use crate::facade::async_event_bus::catch_spi_future;
use crate::facade::async_event_bus::waiting::await_shutdown_or_immediate;
use crate::facade::async_event_bus::waiting::await_until_deadline;
use crate::facade::async_event_bus::waiting::wait_until;
use crate::facade::async_event_bus::waiting::wait_until_deadline;
use crate::facade::async_subscription::is_current_bus_poll;
use crate::model::Topic;
use crate::spi::DurabilityCapability;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::panic_boundary::catch_spi_call;

/// Future that closes one receiver and returns its control-table identity.
type ReceiverCloseFuture<'a> =
    Pin<Box<dyn Future<Output = (Id, Result<(), Arc<crate::error::SubscriptionCloseFailure>>)> + Send + 'a>>;

impl AsyncEventBus {
    /// Returns the cached provider shutdown report, if one has been published.
    ///
    /// The report becomes available after provider shutdown completes, even
    /// when receiver cleanup also produced an error. A caller timeout, failed
    /// SPI shutdown, or cancelled shutdown future does not publish a report
    /// unless cleanup later completes. This method only reads the cached
    /// snapshot and never starts or advances cleanup. It may briefly wait for
    /// the internal shutdown-report mutex.
    ///
    /// # Returns
    /// `Some(report)` after provider shutdown publishes its outcome, including
    /// when receiver cleanup also returned a close error. `None` means no
    /// report had been published at the time of this read; it is not itself a
    /// shutdown result. A later cleanup attempt can publish a report.
    pub fn shutdown_report(&self) -> Option<ShutdownReport> {
        *self
            .inner
            .shutdown_report
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits until this facade has no delivery it has already received for the
    /// selected topic.
    ///
    /// This does not query the provider's queue and does not establish global
    /// idleness on a remote broker.
    /// A callback polled for this bus cannot wait for this operation: it
    /// receives [`LifecycleError::WouldDeadlock`] even when no matching
    /// delivery is active, because the guard is scoped to the whole bus poll.
    ///
    /// # Type Parameters
    /// - `T`: Topic payload type.
    ///
    /// # Parameters
    /// - `topic`: Topic whose facade-received deliveries are tracked.
    /// - `timeout`: Optional maximum wait; `None` waits without a deadline.
    ///
    /// # Returns
    /// `Idle` when no matching delivery is active, or `TimedOut` when the
    /// timeout expires. A zero timeout checks once without registering a
    /// timer; an already idle topic returns immediately for any timeout.
    ///
    /// # Errors
    /// Returns a lifecycle error if a timer fails when a wait is needed, or
    /// waiting would deadlock.
    pub async fn wait_for_received_deliveries<T: 'static>(
        &self,
        topic: &Topic<T>,
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

    /// Stops admission, completes active runners, and closes the provider.
    ///
    /// Immediate shutdown requests active runners to stop receiving but waits
    /// for an already-running handler, its settlement, and receiver close
    /// before shutting down the provider. Shutdown closes every registered
    /// subscription receiver, including receivers whose `run` method was
    /// never started. The report's known-abandoned count is cumulative across
    /// facade-held unresolved ephemeral deliveries; it can include deliveries
    /// whose handler ran but whose settlement was not confirmed, as well as
    /// work discarded by earlier subscription termination or close operations.
    /// It does not imply whether a handler produced a business side effect.
    /// It cannot forcibly cancel user code and may therefore wait indefinitely.
    /// Graceful shutdown applies this call's timeout budget to receiver close,
    /// runner and publish completion, and provider shutdown. If it times out,
    /// the bus remains closing and stops admitting work. Call `shutdown`
    /// again to continue cleanup; each call with [`ShutdownMode::Graceful`]
    /// gets its own timeout budget. [`ShutdownMode::Immediate`] has no
    /// internal deadline and may wait indefinitely for a running handler or
    /// provider operation, which it cannot forcibly cancel.
    ///
    /// This future drives cleanup while it is polled. Dropping it stops the
    /// current attempt; to continue cleanup, the caller must call `shutdown`
    /// again and keep polling the returned future. The async bus does not
    /// continue shutdown in a background task.
    /// If shutdown returns a receiver-close error after provider cleanup has
    /// completed, [`Self::shutdown_report`] still exposes the final provider
    /// outcome and facade abandonment count.
    ///
    /// A pending future must keep being polled to drive this attempt. Dropping
    /// it cancels that attempt, and a later call can retry cleanup that has
    /// not completed. Once this call returns, its future is complete; it cannot
    /// be polled again to resume work. Another still-pending shutdown call can
    /// become the next cleanup driver; otherwise, start and poll a new call to
    /// retry incomplete cleanup. A returned receiver-close error can accompany
    /// completed provider cleanup, in which case the cached report is
    /// available through [`Self::shutdown_report`].
    ///
    /// Directly awaiting shutdown from a handler, middleware, or publisher
    /// callback running on this bus returns [`LifecycleError::WouldDeadlock`].
    /// This detection is scoped to each poll of caller-driven runner and
    /// publisher futures. Request shutdown from an application lifecycle task
    /// after the callback returns. Independently spawned tasks are outside
    /// that scope; this check does not detect arbitrary dependency cycles.
    ///
    /// # Parameters
    /// - `mode`: Shutdown policy and, for graceful mode, its deadline.
    ///
    /// Graceful shutdown first stops admission and fixes its absolute deadline,
    /// then signals active runners to stop. Synchronous stop notifications
    /// cannot be interrupted, but their time counts against the budget. The
    /// deadline is checked before starting the registered receiver
    /// close batch. If receivers remain when a zero-length budget is exhausted,
    /// this call returns [`ShutdownError::TimedOut`] without starting those
    /// close operations. An active runner that the application continues to
    /// poll can independently finish receiving and close its own receiver
    /// after this call returns. An empty receiver batch can proceed to later
    /// cleanup stages, including provider shutdown, even with a zero-length
    /// budget; those stages can still time out if they need to wait.
    ///
    /// # Returns
    /// The cached provider outcome, the cumulative number of facade-held
    /// unresolved ephemeral deliveries that the facade can verify were
    /// abandoned, and whether the provider may have abandoned additional work
    /// still queued in its own mailbox. This count can include deliveries
    /// whose handler ran but whose settlement was not confirmed, and may
    /// include abandonment recorded before this shutdown call. It cannot
    /// establish whether a handler produced a business side effect. The
    /// facade cannot count provider-owned queued deliveries.
    ///
    /// # Errors
    /// Returns [`ShutdownError::TimedOut`] when this graceful call's timeout
    /// expires. The bus remains closing, and a later call can continue
    /// cleanup with a new timeout budget. A graceful timer registration error
    /// also leaves the bus closing, so new publishes and subscriptions are
    /// rejected while a later call retries shutdown. Other errors report a
    /// timer failure, provider shutdown failure, subscription receiver
    /// close failure, or a lifecycle error such as attempting to wait from
    /// a callback running on this bus. An error does not mean that cleanup
    /// will continue in the background. While this future is pending, keep
    /// polling it to drive cleanup. After it returns, a later call can
    /// retry cleanup that remains incomplete. A cancelled future publishes
    /// no report unless a later attempt completes provider cleanup; use
    /// [`Self::shutdown_report`] to inspect the cached result without
    /// starting another attempt. If deadline calculation or timer registration
    /// fails while another call completes shutdown, the completed shutdown
    /// result (including a recorded receiver-close error) takes precedence.
    /// Otherwise the timer error is returned and the closing bus can be
    /// retried.
    ///
    /// # Panics
    /// An internal invariant violation while handling a graceful deadline
    /// triggers a panic.
    pub async fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownReport, ShutdownError> {
        let bus_key = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_poll(bus_key) {
            return Err(LifecycleError::WouldDeadlock { operation: "shutdown" }.into());
        }
        if mode == ShutdownMode::Immediate {
            self.inner.shutdown_immediate.store(true, Ordering::Release);
            self.inner.shutdown_mode_signal.notify();
        }
        let (started_closing, already_closed) = {
            let mut state = self.inner.state.lock().unwrap_or_else(PoisonError::into_inner);
            if *state == BusState::Running {
                *state = BusState::Closing;
                (true, false)
            } else {
                (false, *state == BusState::Closed)
            }
        };
        let timeout = match mode {
            ShutdownMode::Graceful { timeout } => Some(timeout),
            ShutdownMode::Immediate => None,
        };
        let mut pending_deadline = timeout
            .filter(|_| !already_closed)
            .map(|duration| self.inner.timer.clock().deadline_after(duration));
        let notified_controls = started_closing || mode == ShutdownMode::Immediate;
        if notified_controls {
            self.stop_subscriptions(self.requested_shutdown_mode(mode));
        }
        let mut deadline = None;
        loop {
            let is_closed = { *self.inner.state.lock().unwrap_or_else(PoisonError::into_inner) == BusState::Closed };
            if is_closed {
                if let Some(errors) = self.inner.close_errors_snapshot() {
                    return Err(ShutdownError::SubscriptionClose(errors));
                }
                return Ok(self
                    .inner
                    .shutdown_report
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .unwrap_or(ShutdownReport::new(ShutdownOutcome::Complete, 0, false)));
            }
            if let Some(absolute_deadline) = pending_deadline.take() {
                match absolute_deadline.and_then(|instant| self.inner.timer.at(instant)) {
                    Ok(timer) => deadline = Some(timer),
                    Err(error) => {
                        let closed =
                            *self.inner.state.lock().unwrap_or_else(PoisonError::into_inner) == BusState::Closed;
                        if closed {
                            continue;
                        }
                        return Err(LifecycleError::from(error).into());
                    }
                }
            }
            if self
                .inner
                .shutdown_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let _leader = ShutdownLeaderGuard::new(self.inner.clone());
                let already_closed =
                    { *self.inner.state.lock().unwrap_or_else(PoisonError::into_inner) == BusState::Closed };
                if already_closed {
                    continue;
                }
                let requested_mode = self.requested_shutdown_mode(mode);
                if !notified_controls {
                    self.stop_subscriptions(requested_mode);
                }
                return self
                    .shutdown_as_leader(mode, requested_mode, timeout, &mut deadline)
                    .await;
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

    /// Completes ordered cleanup after this caller becomes the shutdown leader.
    ///
    /// # Parameters
    /// - `mode`: shutdown policy selected by this caller.
    /// - `requested_mode`: strongest mode observed before receiver cleanup.
    /// - `timeout`: optional graceful-shutdown duration used in timeout errors.
    /// - `deadline`: shared absolute deadline for all graceful cleanup stages.
    ///
    /// # Returns
    /// The provider outcome and facade-known abandoned-delivery count.
    ///
    /// # Errors
    /// Returns a timeout, timer, provider, or receiver-close error without
    /// marking the bus closed before provider shutdown completes.
    async fn shutdown_as_leader(
        &self,
        mode: ShutdownMode,
        requested_mode: ShutdownMode,
        timeout: Option<Duration>,
        deadline: &mut Option<TimerFuture>,
    ) -> Result<ShutdownReport, ShutdownError> {
        if !self
            .close_subscription_receivers(requested_mode, deadline.as_mut())
            .await?
        {
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

        let outcome = self.shutdown_provider(mode, timeout, deadline).await?;
        let remaining_controls: Vec<_> = self
            .inner
            .controls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for control in remaining_controls {
            control.dispose();
        }
        let report = ShutdownReport::new(
            outcome,
            self.inner.abandoned_deliveries.load(Ordering::Acquire),
            self.inner.capabilities.durability() == DurabilityCapability::Ephemeral
                || outcome == ShutdownOutcome::TimedOut,
        );
        *self
            .inner
            .shutdown_report
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(report);
        *self.inner.state.lock().unwrap_or_else(PoisonError::into_inner) = BusState::Closed;
        if let Some(errors) = self.inner.close_errors_snapshot() {
            return Err(ShutdownError::SubscriptionClose(errors));
        }
        Ok(report)
    }

    /// Shuts down the provider, restarting with immediate mode when requested.
    ///
    /// # Parameters
    /// - `mode`: shutdown policy selected by this caller.
    /// - `timeout`: optional graceful-shutdown duration used in timeout errors.
    /// - `deadline`: shared absolute deadline for provider shutdown.
    ///
    /// # Returns
    /// The outcome reported by the provider.
    ///
    /// # Errors
    /// Returns timer or provider shutdown failures, or a graceful timeout.
    async fn shutdown_provider(
        &self,
        mode: ShutdownMode,
        timeout: Option<Duration>,
        deadline: &mut Option<TimerFuture>,
    ) -> Result<ShutdownOutcome, ShutdownError> {
        loop {
            let requested_mode = self.requested_shutdown_mode(mode);
            let shutdown = catch_spi_call(self.inner.provider_id.as_str(), "shutdown", None, || {
                self.inner.spi.shutdown(requested_mode)
            })?;
            let shutdown = catch_spi_future(shutdown, &self.inner.provider_id, "shutdown", None);
            if requested_mode == ShutdownMode::Immediate {
                let Some(outcome) = await_until_deadline(shutdown, deadline.as_mut()).await? else {
                    return Err(ShutdownError::TimedOut {
                        timeout: timeout.expect("a deadline exists for graceful shutdown"),
                    });
                };
                return Ok(outcome?);
            }
            match await_shutdown_or_immediate(
                shutdown,
                deadline.as_mut(),
                &self.inner.shutdown_mode_signal,
                &self.inner.shutdown_immediate,
            )
            .await?
            {
                ShutdownWait::Complete(result) => return Ok(result?),
                ShutdownWait::TimedOut => {
                    return Err(ShutdownError::TimedOut {
                        timeout: timeout.expect("a deadline exists for graceful shutdown"),
                    });
                }
                ShutdownWait::ImmediateRequested => {}
            }
        }
    }

    /// Returns the strongest shutdown mode requested by any caller so far.
    ///
    /// # Parameters
    /// - `requested`: mode requested by the current caller.
    ///
    /// # Returns
    /// Immediate mode if any caller requested it, otherwise `requested`.
    #[inline]
    fn requested_shutdown_mode(&self, requested: ShutdownMode) -> ShutdownMode {
        if requested == ShutdownMode::Immediate || self.inner.shutdown_immediate.load(Ordering::Acquire) {
            ShutdownMode::Immediate
        } else {
            requested
        }
    }

    /// Sends the strongest requested stop mode to a snapshot of subscriptions.
    ///
    /// # Parameters
    /// - `mode`: shutdown mode applied to each registered subscription.
    fn stop_subscriptions(&self, mode: ShutdownMode) {
        let controls: Vec<_> = self
            .inner
            .controls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for control in controls {
            control.stop(mode);
        }
    }

    /// Closes all registered subscription receivers and records close results.
    /// The same shutdown deadline is checked before the batch and after each
    /// receiver poll so an expired budget does not start another close.
    ///
    /// # Parameters
    /// - `mode`: receiver shutdown policy.
    /// - `deadline`: shared shutdown deadline, if this call is graceful.
    ///
    /// # Returns
    /// `true` when every receiver close future completed before the deadline.
    ///
    /// # Errors
    /// Returns a timer error if polling the shared deadline fails.
    async fn close_subscription_receivers(
        &self,
        mode: ShutdownMode,
        deadline: Option<&mut TimerFuture>,
    ) -> Result<bool, LifecycleError> {
        let controls: Vec<_> = self
            .inner
            .controls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(id, control)| (*id, control.clone()))
            .collect();
        let mut closes: Vec<Option<ReceiverCloseFuture<'_>>> = controls
            .into_iter()
            .map(|(id, control)| Some(Box::pin(async move { (id, control.shutdown(mode).await) }) as _))
            .collect();
        if closes.is_empty() {
            return Ok(true);
        }

        let mut deadline = deadline;
        poll_fn(|context| {
            if let Some(deadline) = deadline.as_deref_mut() {
                match deadline.as_mut().poll(context) {
                    Poll::Ready(Ok(())) => return Poll::Ready(Ok(false)),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(LifecycleError::Timer(error))),
                    Poll::Pending => {}
                }
            }
            let mut pending = false;
            for close in &mut closes {
                let Some(future) = close.as_mut() else {
                    continue;
                };
                match future.as_mut().poll(context) {
                    Poll::Pending => pending = true,
                    Poll::Ready((id, result)) => {
                        close.take();
                        if result.is_ok() {
                            self.inner
                                .controls
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .remove(&id);
                        }
                    }
                }
                if let Some(deadline) = deadline.as_deref_mut() {
                    match deadline.as_mut().poll(context) {
                        Poll::Ready(Ok(())) => return Poll::Ready(Ok(false)),
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(LifecycleError::Timer(error))),
                        Poll::Pending => {}
                    }
                }
            }
            if pending { Poll::Pending } else { Poll::Ready(Ok(true)) }
        })
        .await
    }
}
