// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus lifecycle operations.

use std::io::Error as IoError;
use std::io::Result as IoResult;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::thread;
use std::time::Duration;

use crate::EventBus;
use crate::EventBusShutdown;
use crate::LifecycleError;
use crate::ShutdownError;
use crate::ShutdownReport;
use crate::WaitOutcome;
use crate::facade::SubscriptionControl;
use crate::facade::event_bus::EventBusInner;
use crate::facade::internal::LifecycleState;
use crate::facade::internal::is_current_bus_context;
use crate::model::Topic;
use crate::spi::ShutdownMode;
use crate::spi::TopicAddress;
use crate::spi::panic_boundary::catch_spi_call;

impl EventBus {
    /// Waits until the provider reports that `topic` has no queued or unsettled
    /// messages.
    ///
    /// This is local to this facade and does not establish that a remote broker
    /// or other consumers are globally idle. A call from one of this bus's
    /// synchronous callbacks or workers returns `WouldDeadlock` rather than
    /// waiting for work that depends on the current call to finish.
    ///
    /// # Parameters
    /// - `topic`: typed destination whose provider queue is checked.
    /// - `timeout`: optional maximum wait duration.
    ///
    /// # Returns
    /// `Idle` when the provider reports no work, or `TimedOut` when the wait
    /// expires.
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
        catch_spi_call(
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
    ///
    /// # Parameters
    /// - `topic`: typed destination whose facade-received work is checked.
    /// - `timeout`: optional maximum wait duration.
    ///
    /// # Returns
    /// `Idle` when no matching delivery remains, or `TimedOut` when the wait
    /// expires.
    ///
    /// # Errors
    /// Returns `WouldDeadlock` when called within a synchronous callback or
    /// worker owned by this bus.
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
    /// # Parameters
    /// - `mode`: graceful or immediate shutdown policy.
    ///
    /// # Returns
    /// The cached provider outcome and facade-known abandoned-delivery count.
    ///
    /// # Errors
    /// Returns `WouldDeadlock` for a call from a bus callback or worker,
    /// `TimedOut` when the full graceful close has not completed by its
    /// deadline, a coordinator thread could not start, and provider/close
    /// failures without suppressing their source errors.
    pub fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownReport, ShutdownError> {
        let identity = Arc::as_ptr(&self.inner) as usize;
        if is_current_bus_context(identity) {
            return Err(LifecycleError::WouldDeadlock { operation: "shutdown" }.into());
        }
        let timeout = match mode {
            ShutdownMode::Graceful { timeout } => Some(timeout),
            ShutdownMode::Immediate => None,
        };
        self.request_shutdown(mode)?.wait(timeout)
    }

    /// Requests shutdown without waiting for handlers, workers or the provider.
    /// Closes admission and strengthens the current attempt when Immediate is
    /// requested. Graceful's timeout is passed to the provider; ticket timeouts
    /// apply only to the observer. Safe within this bus's callbacks and
    /// workers. Returns a ticket bound to the exact attempt or a ready
    /// cached ticket if already closed. A thread start failure is published
    /// to joined tickets before returning `CoordinatorStart`; a subsequent
    /// request may retry.
    ///
    /// # Parameters
    /// - `mode`: graceful or immediate shutdown policy.
    ///
    /// # Returns
    /// A ticket bound to the shutdown attempt, or a ready ticket if already
    /// closed.
    ///
    /// # Errors
    /// Returns `CoordinatorStart` when the shutdown coordinator thread cannot
    /// start.
    pub fn request_shutdown(&self, mode: ShutdownMode) -> Result<EventBusShutdown, ShutdownError> {
        self.request_shutdown_with_spawner(mode, |inner, generation| {
            thread::Builder::new()
                .name("event-bus-shutdown".to_owned())
                .spawn(move || inner.run_shutdown(generation))
                .map(|_| ())
        })
    }

    /// Requests a generation using the supplied coordinator thread launcher.
    /// The launcher runs after admission closes and lifecycle locks are
    /// released; start errors are published to existing generation
    /// observers before return.
    ///
    /// # Type Parameters
    /// - `F`: one-shot launcher receiving the bus state and shutdown
    ///   generation.
    ///
    /// # Parameters
    /// - `mode`: shutdown policy selected for this generation.
    /// - `spawn`: launcher for the coordinator thread.
    ///
    /// # Returns
    /// A ticket bound to this shutdown generation, or a ready ticket if closed.
    ///
    /// # Errors
    /// Returns `CoordinatorStart` if the launcher cannot start the coordinator.
    fn request_shutdown_with_spawner<F>(&self, mode: ShutdownMode, spawn: F) -> Result<EventBusShutdown, ShutdownError>
    where
        F: FnOnce(Arc<EventBusInner>, u64) -> IoResult<()>,
    {
        let (start, generation) = {
            let mut state = self.lock_lifecycle();
            if *state == LifecycleState::Closed {
                return Ok(EventBusShutdown {
                    inner: self.inner.clone(),
                    generation: None,
                });
            }
            *state = LifecycleState::Closing;
            self.inner.operations.close_admission();
            self.inner.shutdown_coordinator.begin(mode)
        };
        let ticket = EventBusShutdown {
            inner: self.inner.clone(),
            generation: Some(generation),
        };
        self.inner.scheduler.request_stop(matches!(
            self.inner.shutdown_coordinator.mode(generation),
            ShutdownMode::Immediate
        ));
        self.inner.signal_subscriptions(SubscriptionControl::request_cancel);
        if start && let Err(error) = spawn(self.inner.clone(), generation) {
            let returned = error.raw_os_error().map_or_else(
                || IoError::new(error.kind(), error.to_string()),
                IoError::from_raw_os_error,
            );
            self.inner.shutdown_coordinator.abort_start(generation, error);
            return Err(ShutdownError::CoordinatorStart(returned));
        }
        Ok(ticket)
    }

    /// Locks the lifecycle state while recovering from internal poison.
    ///
    /// # Returns
    /// The lifecycle mutex guard for this bus.
    pub(in crate::facade) fn lock_lifecycle(&self) -> MutexGuard<'_, LifecycleState> {
        self.inner.lifecycle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io::Error;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;

    use crate::EventBus;
    use crate::ShutdownError;
    use crate::local::LocalEventBusConfig;
    use crate::spi::ShutdownMode;
    #[test]
    fn test_failed_start_ticket_remains_ready_after_successful_retry() {
        let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
        let joined = Mutex::new(None);
        let error = bus
            .request_shutdown_with_spawner(ShutdownMode::Immediate, |_, _| {
                *joined.lock().expect("ticket slot") =
                    Some(bus.request_shutdown(ShutdownMode::Immediate).expect("join ticket"));
                Err(Error::other("injected coordinator start failure"))
            })
            .err()
            .expect("start failure");
        assert!(matches!(error, ShutdownError::CoordinatorStart(_)));
        let old = joined.into_inner().expect("ticket slot").expect("joined ticket");
        let retry = bus.request_shutdown(ShutdownMode::Immediate).expect("retry starts");
        let retry_report = retry.wait(Some(Duration::from_secs(5))).expect("retry completes");
        assert_eq!(retry_report.outcome, crate::spi::ShutdownOutcome::Complete);
        assert!(matches!(
            old.wait(Some(Duration::ZERO)),
            Err(ShutdownError::CoordinatorStart(_))
        ));
        let mut future = Box::pin(old.wait_async());
        assert!(matches!(
            future.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(ShutdownError::CoordinatorStart(_)))
        ));
    }
}
