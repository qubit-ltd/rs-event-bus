// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Local event-bus lifecycle and runtime observation.

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_executor::ExecutorService;
use qubit_executor::SingleThreadScheduledExecutorService;
use qubit_thread_pool::FixedThreadPool;

use super::LocalEventBus;
use super::worker_context::is_current_subscription_worker_for_bus;
use super::worker_context::local_event_bus_id;
use crate::DeliveryFailure;
use crate::EventBusError;
use crate::EventBusResult;
use crate::Topic;

/// Keeps the bus in the stopping state until shutdown cleanup has finished.
struct ShutdownCompletionGuard<'a> {
    bus: &'a LocalEventBus,
}

impl Drop for ShutdownCompletionGuard<'_> {
    fn drop(&mut self) {
        if let Some(executor) = self.bus.inner.take_executor() {
            executor.shutdown();
        }
        if let Some(delay_scheduler) = self.bus.inner.take_delay_scheduler() {
            delay_scheduler.shutdown();
        }
        self.bus.inner.clear_subscriptions();
        self.bus.inner.complete_shutdown();
    }
}

impl LocalEventBus {
    /// Starts the event bus.
    ///
    /// # Returns
    /// `Ok(true)` when this call changed the bus from stopped to started.
    ///
    /// # Errors
    /// Returns startup errors from the handler executor.
    pub fn start(&self) -> EventBusResult<bool> {
        self.inner.mark_started()
    }

    /// Shuts down the event bus.
    ///
    /// The method waits for currently scheduled handlers to finish and then
    /// clears all subscriptions.
    ///
    /// # Returns
    /// `true` when this call changed the bus from started to stopped.
    ///
    /// # Panics
    /// Panics when called from one of this bus's subscriber worker threads. A
    /// subscriber worker cannot wait for itself to finish. Use
    /// [`shutdown_nonblocking`](Self::shutdown_nonblocking) from subscriber
    /// handlers.
    pub fn shutdown(&self) -> bool {
        self.assert_not_own_subscription_worker_for_blocking_shutdown();
        if !self.inner.mark_stopping() {
            return false;
        }
        let _completion = ShutdownCompletionGuard { bus: self };
        let _ = self.inner.wait_for_all_idle();
        if let Some(executor) = self.inner.take_executor() {
            executor.shutdown();
            wait_for_executor_termination(&executor);
        }
        if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
            delay_scheduler.shutdown();
            wait_for_delay_scheduler_termination(&delay_scheduler);
        }
        self.inner.clear_subscriptions();
        true
    }

    /// Requests shutdown without waiting for subscriber work to finish.
    ///
    /// The bus stops accepting publish and subscribe operations, asks the
    /// handler executor to shut down, deactivates subscriptions, and
    /// returns immediately. Already running handler code is not
    /// interrupted.
    ///
    /// # Returns
    /// `true` when this call changed the bus from started to stopped.
    pub fn shutdown_nonblocking(&self) -> bool {
        let Some(executor) = self.inner.mark_stopped() else {
            return false;
        };
        let _completion = ShutdownCompletionGuard { bus: self };
        executor.shutdown();
        if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
            delay_scheduler.shutdown();
        }
        self.inner.clear_subscriptions();
        true
    }

    /// Shuts down the event bus with a maximum wait duration.
    ///
    /// The bus stops accepting new publish and subscribe operations
    /// immediately, then waits for scheduled subscriber work and executor
    /// workers to finish. If the timeout elapses, subscriptions are
    /// deactivated before the timeout error is returned.
    ///
    /// When called from this bus's current subscriber worker, the current
    /// delivery remains active until the handler returns. The call therefore
    /// waits until `timeout` and returns
    /// [`ShutdownTimedOut`](EventBusError::ShutdownTimedOut); use
    /// [`shutdown_nonblocking`](Self::shutdown_nonblocking) to request
    /// shutdown from a handler without waiting.
    ///
    /// # Parameters
    /// - `timeout`: Maximum duration to wait for graceful shutdown.
    ///
    /// # Returns
    /// `Ok(true)` when this call changed the bus from started to stopped and
    /// shutdown completed within the timeout. `Ok(false)` means the bus was
    /// already stopped.
    ///
    /// # Errors
    /// Returns [`EventBusError::ShutdownTimedOut`] if subscriber work or
    /// executor workers do not finish before `timeout`.
    pub fn shutdown_with_timeout(&self, timeout: Duration) -> EventBusResult<bool> {
        let started_at = Instant::now();
        if !self.inner.mark_stopping() {
            return Ok(false);
        }
        let _completion = ShutdownCompletionGuard { bus: self };
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            self.inner.clear_subscriptions();
            if let Some(executor) = self.inner.take_executor() {
                executor.shutdown();
            }
            if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
                delay_scheduler.shutdown();
            }
            return Err(EventBusError::shutdown_timed_out(timeout));
        };
        if !self.inner.wait_for_all_idle_timeout(remaining)? {
            self.inner.clear_subscriptions();
            if let Some(executor) = self.inner.take_executor() {
                executor.shutdown();
            }
            if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
                delay_scheduler.shutdown();
            }
            return Err(EventBusError::shutdown_timed_out(timeout));
        }
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            self.inner.clear_subscriptions();
            if let Some(executor) = self.inner.take_executor() {
                executor.shutdown();
            }
            if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
                delay_scheduler.shutdown();
            }
            return Err(EventBusError::shutdown_timed_out(timeout));
        };
        let Some(executor) = self.inner.take_executor() else {
            if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
                delay_scheduler.shutdown();
            }
            self.inner.clear_subscriptions();
            return Ok(true);
        };
        executor.shutdown();
        if let Some(delay_scheduler) = self.inner.take_delay_scheduler() {
            delay_scheduler.shutdown();
            if !wait_for_delay_scheduler_termination_timeout(&delay_scheduler, remaining) {
                self.inner.clear_subscriptions();
                return Err(EventBusError::shutdown_timed_out(timeout));
            }
        }
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            self.inner.clear_subscriptions();
            return Err(EventBusError::shutdown_timed_out(timeout));
        };
        if !wait_for_executor_termination_timeout(&executor, remaining) {
            self.inner.clear_subscriptions();
            return Err(EventBusError::shutdown_timed_out(timeout));
        }
        self.inner.clear_subscriptions();
        Ok(true)
    }

    /// Registers an observer for internal background errors.
    ///
    /// # Parameters
    /// - `observer`: Callback invoked when interceptors, error handlers, or
    ///   dead-letter routing fail.
    ///
    /// # Returns
    /// `Ok(())` when the observer is stored.
    ///
    /// # Errors
    /// Returns a lock-poisoning error if observer state is unavailable.
    pub fn add_error_observer<F>(&self, observer: F) -> EventBusResult<()>
    where
        F: Fn(&EventBusError) + Send + Sync + 'static,
    {
        self.inner.add_error_observer(Arc::new(observer))
    }

    /// Registers an observer for terminal subscriber delivery failures.
    ///
    /// The callback runs once after retries, error handling, and dead-letter
    /// routing have completed for a delivery that remains failed.
    pub fn add_delivery_failure_observer<F>(&self, observer: F) -> EventBusResult<()>
    where
        F: Fn(&DeliveryFailure) + Send + Sync + 'static,
    {
        self.inner.add_delivery_failure_observer(Arc::new(observer))
    }

    /// Waits until all work for a topic is idle.
    ///
    /// # Parameters
    /// - `topic`: Topic to wait for.
    ///
    /// # Returns
    /// `Ok(())` once the topic has no active handler work.
    ///
    /// # Errors
    /// Returns a lock-poisoning error if tracker state is unavailable.
    pub fn wait_for_idle<T>(&self, topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        if is_current_subscription_worker_for_bus(local_event_bus_id(&self.inner)) {
            return Err(EventBusError::would_deadlock("wait_for_idle"));
        }
        self.inner.wait_for_idle(&topic.key())
    }

    /// Waits until all work for a topic is idle or the timeout elapses.
    ///
    /// # Parameters
    /// - `topic`: Topic to wait for.
    /// - `timeout`: Maximum duration to wait.
    ///
    /// # Returns
    /// `Ok(true)` once the topic has no active handler work, or `Ok(false)`
    /// when the timeout elapses first.
    ///
    /// # Errors
    /// Returns a lock-poisoning error if tracker state is unavailable.
    pub fn wait_for_idle_timeout<T>(&self, topic: &Topic<T>, timeout: Duration) -> EventBusResult<bool>
    where
        T: 'static,
    {
        if is_current_subscription_worker_for_bus(local_event_bus_id(&self.inner)) {
            return Err(EventBusError::would_deadlock("wait_for_idle_timeout"));
        }
        self.inner.wait_for_idle_timeout(&topic.key(), timeout)
    }

    /// Panics if blocking shutdown is called from this bus's subscriber worker.
    fn assert_not_own_subscription_worker_for_blocking_shutdown(&self) {
        let bus_id = local_event_bus_id(&self.inner);
        if is_current_subscription_worker_for_bus(bus_id) {
            panic!(
                "LocalEventBus::shutdown must not be called from this bus's subscriber worker; use shutdown_nonblocking"
            );
        }
    }
}

/// Waits for a fixed handler executor to finish after shutdown.
fn wait_for_executor_termination(executor: &FixedThreadPool) {
    while !executor.is_terminated() {
        thread::sleep(Duration::from_millis(1));
    }
}

/// Waits for a fixed handler executor to finish until the timeout elapses.
fn wait_for_executor_termination_timeout(executor: &FixedThreadPool, timeout: Duration) -> bool {
    let started_at = Instant::now();
    while !executor.is_terminated() {
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            return false;
        };
        thread::sleep(remaining.min(Duration::from_millis(1)));
    }
    true
}

/// Waits for a delayed task scheduler to finish after shutdown.
fn wait_for_delay_scheduler_termination(scheduler: &SingleThreadScheduledExecutorService) {
    while !scheduler.is_terminated() {
        thread::sleep(Duration::from_millis(1));
    }
}

/// Waits for a delayed task scheduler to finish until the timeout elapses.
fn wait_for_delay_scheduler_termination_timeout(
    scheduler: &SingleThreadScheduledExecutorService,
    timeout: Duration,
) -> bool {
    let started_at = Instant::now();
    while !scheduler.is_terminated() {
        let Some(remaining) = remaining_shutdown_timeout(started_at, timeout) else {
            return false;
        };
        thread::sleep(remaining.min(Duration::from_millis(1)));
    }
    true
}

/// Returns the remaining shutdown timeout.
fn remaining_shutdown_timeout(started_at: Instant, timeout: Duration) -> Option<Duration> {
    timeout.checked_sub(started_at.elapsed())
}
