// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus lifecycle operations.

use crate::AsyncEventBus;
use crate::LifecycleError;
use crate::ShutdownError;
use crate::ShutdownReport;
use crate::WaitOutcome;
use crate::facade::async_event_bus::Arc;
use crate::facade::async_event_bus::BusState;
use crate::facade::async_event_bus::Duration;
use crate::facade::async_event_bus::Ordering;
use crate::facade::async_event_bus::ShutdownLeaderGuard;
use crate::facade::async_event_bus::ShutdownWait;
use crate::facade::async_event_bus::catch_spi_future;
use crate::facade::async_event_bus::waiting::await_shutdown_or_immediate;
use crate::facade::async_event_bus::waiting::await_until_deadline;
use crate::facade::async_event_bus::waiting::wait_until;
use crate::facade::async_event_bus::waiting::wait_until_deadline;
use crate::facade::async_subscription::is_current_bus_poll;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

impl AsyncEventBus {
    /// Waits until this facade has no delivery it has already received for the
    /// selected topic.
    ///
    /// This does not query the provider's queue and does not establish global
    /// idleness on a remote broker.
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
    /// timeout expires.
    ///
    /// # Errors
    /// Returns a lifecycle error if the timer fails or waiting would deadlock.
    pub async fn wait_for_received_deliveries<T: 'static>(
        &self,
        topic: &crate::model::Topic<T>,
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
    /// before shutting down the provider. Receivers for subscriptions that
    /// have not started are closed directly by shutdown. Unstarted ephemeral
    /// deliveries may be abandoned and are counted in the returned report.
    /// It cannot forcibly cancel user code and may therefore wait indefinitely.
    /// Graceful shutdown applies this caller's deadline to receiver close,
    /// runner and publish completion, and provider shutdown. A timeout leaves
    /// the bus closing so the caller may retry cleanup, including with
    /// [`ShutdownMode::Immediate`].
    ///
    /// Directly awaiting shutdown from a handler or middleware Future running
    /// on this bus returns [`LifecycleError::WouldDeadlock`]. This detection is
    /// scoped to each poll of the caller-driven runner Future; tasks the
    /// application independently spawns are outside that scope and must not
    /// await a shutdown that includes their originating handler.
    ///
    /// # Parameters
    /// - `mode`: Shutdown policy and, for graceful mode, its deadline.
    ///
    /// # Returns
    /// The cached provider outcome and facade-known abandoned-delivery count.
    ///
    /// # Errors
    /// Returns an error if cleanup, provider shutdown, the timer, or a
    /// subscription receiver close fails.
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
            let controls: Vec<_> = self
                .inner
                .controls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .cloned()
                .collect();
            for control in controls {
                control.stop(ShutdownMode::Immediate);
            }
        }
        let timeout = match mode {
            ShutdownMode::Graceful { timeout } => Some(timeout),
            ShutdownMode::Immediate => None,
        };
        let mut deadline = None;
        loop {
            let is_closed = {
                *self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    == BusState::Closed
            };
            if is_closed {
                if let Some(errors) = self.inner.close_errors_snapshot() {
                    return Err(ShutdownError::SubscriptionClose(errors));
                }
                return Ok(self
                    .inner
                    .shutdown_report
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .unwrap_or(ShutdownReport::new(ShutdownOutcome::Complete, 0, false)));
            }
            if deadline.is_none() {
                deadline = timeout
                    .map(|timeout| self.inner.timer.after(timeout).map_err(LifecycleError::from))
                    .transpose()?;
            }
            if self
                .inner
                .shutdown_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let _leader = ShutdownLeaderGuard(self.inner.clone());
                *self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = BusState::Closing;
                let requested_mode = self.requested_shutdown_mode(mode);
                let controls: Vec<_> = self
                    .inner
                    .controls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                    .cloned()
                    .collect();
                for control in controls {
                    control.stop(requested_mode);
                }
                let close = self.close_unstarted_subscriptions(requested_mode);
                if await_until_deadline(close, deadline.as_mut()).await?.is_none() {
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
                let outcome = loop {
                    let requested_mode = self.requested_shutdown_mode(mode);
                    let shutdown = crate::spi::panic_boundary::catch_spi_call(
                        self.inner.provider_id.as_str(),
                        "shutdown",
                        None,
                        || self.inner.spi.shutdown(requested_mode),
                    )?;
                    let shutdown = catch_spi_future(shutdown, &self.inner.provider_id, "shutdown", None);
                    if requested_mode == ShutdownMode::Immediate {
                        let Some(outcome) = await_until_deadline(shutdown, deadline.as_mut()).await? else {
                            return Err(ShutdownError::TimedOut {
                                timeout: timeout.expect("a deadline exists for graceful shutdown"),
                            });
                        };
                        break outcome?;
                    }
                    match await_shutdown_or_immediate(
                        shutdown,
                        deadline.as_mut(),
                        &self.inner.shutdown_mode_signal,
                        &self.inner.shutdown_immediate,
                    )
                    .await?
                    {
                        ShutdownWait::Complete(result) => break result?,
                        ShutdownWait::TimedOut => {
                            return Err(ShutdownError::TimedOut {
                                timeout: timeout.expect("a deadline exists for graceful shutdown"),
                            });
                        }
                        ShutdownWait::ImmediateRequested => continue,
                    }
                };
                let report = ShutdownReport::new(
                    outcome,
                    self.inner.abandoned_deliveries.load(Ordering::Acquire),
                    self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral
                        || outcome == ShutdownOutcome::TimedOut,
                );
                *self
                    .inner
                    .shutdown_report
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(report);
                *self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = BusState::Closed;
                if let Some(errors) = self.inner.close_errors_snapshot() {
                    return Err(ShutdownError::SubscriptionClose(errors));
                }
                return Ok(report);
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

    /// Returns the strongest shutdown mode requested by any caller so far.
    /// Computes the strongest shutdown mode requested so far.
    ///
    /// # Parameters
    /// - `requested`: mode requested by the current caller.
    ///
    /// # Returns
    /// Immediate mode if any caller requested it, otherwise `requested`.
    fn requested_shutdown_mode(&self, requested: ShutdownMode) -> ShutdownMode {
        if requested == ShutdownMode::Immediate || self.inner.shutdown_immediate.load(Ordering::Acquire) {
            ShutdownMode::Immediate
        } else {
            requested
        }
    }

    /// Closes provider receivers not yet transferred to a runner and records
    /// any close failures.
    ///
    /// # Parameters
    /// - `mode`: receiver shutdown policy.
    async fn close_unstarted_subscriptions(&self, mode: ShutdownMode) {
        let controls: Vec<_> = self
            .inner
            .controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(id, control)| (*id, control.clone()))
            .collect();
        for (id, control) in controls {
            if let Err(failure) = control.shutdown(mode).await {
                self.inner.record_close_failure(failure);
                continue;
            }
            self.inner
                .controls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
        }
    }
}
