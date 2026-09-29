// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus lifecycle operations.

use std::thread;

use crate::EventBus;
use crate::LifecycleError;
use crate::ShutdownError;
use crate::ShutdownReport;
use crate::WaitOutcome;
use crate::facade::event_bus::Arc;
use crate::facade::event_bus::Duration;
use crate::facade::event_bus::Instant;
use crate::facade::event_bus::internal::clone_spi_error;
use crate::facade::internal::LifecycleState;
use crate::facade::internal::is_current_bus_context;
use crate::model::Topic;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::TopicAddress;

impl EventBus {
    /// Waits until the provider reports that `topic` has no queued or unsettled
    /// messages.
    ///
    /// This is local to this facade and does not establish that a remote broker
    /// or other consumers are globally idle. A call from one of this bus's
    /// synchronous callbacks or workers returns `WouldDeadlock` rather than
    /// waiting for work that depends on the current call to finish.
    ///
    /// # Errors
    /// Returns `WouldDeadlock` when called within a synchronous callback or
    /// worker owned by this bus, `IdleWaitUnsupported` when the provider has no
    /// topic-idle reporting capability, or the original SPI error.
    pub fn wait_for_idle<T: 'static>(
        &self,
        topic: &Topic<T>,
        timeout: Option<Duration>,
    ) -> Result<WaitOutcome, LifecycleError> {
        let identity = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_context(identity) {
            return Err(LifecycleError::WouldDeadlock {
                operation: "wait_for_idle",
            });
        }
        let address = TopicAddress::new(topic.name()).expect("typed topic names are valid SPI addresses");
        crate::spi::panic_boundary::catch_spi_call(
            self.inner.provider_id.as_str(),
            "wait_for_topic_idle",
            Some(address.as_str()),
            || self.inner.spi.wait_for_topic_idle(&address, timeout),
        )??
        .map(|idle| if idle { WaitOutcome::Idle } else { WaitOutcome::TimedOut })
        .ok_or(LifecycleError::IdleWaitUnsupported)
    }

    /// Waits until this facade has completed work already received for `topic`.
    ///
    /// This is local to this facade and does not establish that a remote broker
    /// or other consumers are globally idle.
    pub fn wait_for_received_deliveries<T: 'static>(
        &self,
        topic: &Topic<T>,
        timeout: Option<Duration>,
    ) -> Result<WaitOutcome, LifecycleError> {
        let identity = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_context(identity) {
            return Err(LifecycleError::WouldDeadlock {
                operation: "wait_for_received_deliveries",
            });
        }
        Ok(self.inner.tracker.wait_for_idle(topic.name(), timeout))
    }

    /// Stops operation admission, completes active deliveries, and shuts down
    /// the provider.
    ///
    /// The facade first rejects new publish/subscribe calls and waits for calls
    /// admitted earlier to finish their provider SPI operations. A subscription
    /// admitted before shutdown is included in the subsequent close phase. Both
    /// modes then stop workers from receiving additional messages. Graceful
    /// shutdown drains admitted queued and active deliveries; a message already
    /// received but not admitted by the shared scheduler is returned with
    /// `Retry`. Immediate shutdown returns admitted queued deliveries with
    /// `Retry`, allows active handlers and settlements to finish, and then
    /// closes subscriptions. Graceful shutdown applies its timeout to the
    /// caller's wait for the entire close sequence. If the deadline expires,
    /// this method returns `ShutdownError::TimedOut` while one background
    /// coordinator keeps closing the bus; new operations remain rejected. Call
    /// shutdown again to wait for the result, or use `Immediate` to strengthen
    /// an active attempt. Success returns a `ShutdownReport`; its provider
    /// outcome does not imply business handler success.
    /// The coordinator cannot forcibly stop a blocked synchronous SPI call or
    /// user handler, so it can remain alive until that code returns.
    /// Calling either mode from a synchronous callback or worker owned by this
    /// bus returns `WouldDeadlock` instead of waiting for the current
    /// operation permit.
    ///
    /// # Errors
    /// Returns `WouldDeadlock` for a call from a bus callback or worker,
    /// `TimedOut` when the full graceful close has not completed by its
    /// deadline, a coordinator thread could not start, and provider/close
    /// failures without suppressing their source errors.
    ///
    /// # Returns
    /// The cached provider outcome and facade-known abandoned-delivery count.
    pub fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownReport, ShutdownError> {
        let identity = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_context(identity) {
            return Err(LifecycleError::WouldDeadlock { operation: "shutdown" }.into());
        }
        let timeout = match mode {
            ShutdownMode::Graceful { timeout } => Some(timeout),
            ShutdownMode::Immediate => None,
        };
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        {
            let mut state = self.lock_lifecycle();
            if *state == LifecycleState::Closed {
                if let Some(errors) = self.inner.close_errors_snapshot() {
                    return Err(ShutdownError::SubscriptionClose(errors));
                }
                return Ok(self
                    .inner
                    .shutdown_gate
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .report
                    .unwrap_or(ShutdownReport::new(ShutdownOutcome::Complete, 0, false)));
            }
            *state = LifecycleState::Closing;
        }
        self.inner.operations.close_admission();

        loop {
            let (start, generation) = self.inner.shutdown_coordinator.begin(mode);
            self.inner
                .scheduler
                .stop_admission(matches!(mode, ShutdownMode::Immediate));
            for control in &self.inner.subscription_snapshot() {
                control.request_cancel();
            }
            if start {
                let inner = self.inner.clone();
                let spawn = thread::Builder::new()
                    .name("event-bus-shutdown".to_owned())
                    .spawn(move || inner.run_shutdown(generation));
                if let Err(error) = spawn {
                    self.inner.shutdown_coordinator.abort_start(generation);
                    return Err(ShutdownError::CoordinatorStart(error));
                }
            }
            let (timed_out, result) = self.inner.shutdown_coordinator.wait(generation, deadline);
            if timed_out {
                return Err(ShutdownError::TimedOut {
                    timeout: timeout.expect("only graceful shutdown has a deadline"),
                });
            }
            let Some(result) = result else {
                continue;
            };
            result.map_err(clone_spi_error)?;
            if let Some(errors) = self.inner.close_errors_snapshot() {
                return Err(ShutdownError::SubscriptionClose(errors));
            }
            let report = self
                .inner
                .shutdown_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .report
                .unwrap_or(ShutdownReport::new(ShutdownOutcome::Complete, 0, false));
            return Ok(report);
        }
    }

    /// Locks the lifecycle state while recovering from internal poison.
    pub(in crate::facade) fn lock_lifecycle(&self) -> std::sync::MutexGuard<'_, LifecycleState> {
        self.inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
