// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Lifecycle state and runtime resource ownership for the local event bus.

use qubit_executor::ExecutorServiceBuilderError;
use qubit_executor::SingleThreadScheduledExecutorService;
use qubit_thread_pool::FixedThreadPool;

use super::LocalEventBusInner;
use crate::EventBusError;
use crate::EventBusResult;

/// Lifecycle state protected by the local event bus lifecycle lock.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum LifecycleState {
    Stopped,
    Started,
    Stopping,
}

/// Lifecycle state and runtime resources owned by the local event bus.
pub(super) struct LocalEventBusLifecycle {
    pub(super) state: LifecycleState,
    pub(super) executor: Option<FixedThreadPool>,
    pub(super) delay_scheduler: Option<SingleThreadScheduledExecutorService>,
}

impl LocalEventBusLifecycle {
    /// Creates a stopped lifecycle without runtime resources.
    pub(super) fn stopped() -> Self {
        Self {
            state: LifecycleState::Stopped,
            executor: None,
            delay_scheduler: None,
        }
    }
}

impl LocalEventBusInner {
    /// Marks the bus as started.
    ///
    /// # Returns
    /// `true` when this call changed state from stopped to started.
    pub(crate) fn mark_started(&self) -> EventBusResult<bool> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        match lifecycle.state {
            LifecycleState::Started => return Ok(false),
            LifecycleState::Stopping => {
                return Err(EventBusError::start_failed("previous shutdown is still in progress"));
            }
            LifecycleState::Stopped => {}
        }
        if lifecycle.executor.is_some() || lifecycle.delay_scheduler.is_some() {
            return Err(EventBusError::start_failed(
                "previous shutdown is still draining subscriber work",
            ));
        }
        if self.processing_tracker.has_active()? {
            return Err(EventBusError::start_failed(
                "previous shutdown still has active subscriber work",
            ));
        }
        let executor = self
            .build_subscription_handler_executor()
            .map_err(start_failed_from_thread_pool_error)?;
        let delay_scheduler = self
            .build_delay_scheduler()
            .map_err(start_failed_from_thread_pool_error)?;
        lifecycle.executor = Some(executor);
        lifecycle.delay_scheduler = Some(delay_scheduler);
        lifecycle.state = LifecycleState::Started;
        Ok(true)
    }

    /// Marks the bus as stopping while keeping its handler executor alive.
    ///
    /// # Returns
    /// `true` when this call changed state from started to stopping.
    pub(crate) fn mark_stopping(&self) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if lifecycle.state != LifecycleState::Started {
            return false;
        }
        lifecycle.state = LifecycleState::Stopping;
        true
    }

    /// Marks the bus as stopped and removes its handler executor.
    ///
    /// # Returns
    /// Handler executor when this call changed state from started to stopped.
    pub(crate) fn mark_stopped(&self) -> Option<FixedThreadPool> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        if lifecycle.state != LifecycleState::Started {
            return None;
        }
        lifecycle.state = LifecycleState::Stopping;
        lifecycle.executor.take()
    }

    /// Removes the handler executor after the bus has entered stopping state.
    ///
    /// # Returns
    /// Handler executor if one is still owned by the bus.
    pub(crate) fn take_executor(&self) -> Option<FixedThreadPool> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        lifecycle.executor.take()
    }

    /// Removes the delayed-delivery scheduler after the bus has entered
    /// stopping state.
    ///
    /// # Returns
    /// Delayed-delivery scheduler if one is still owned by the bus.
    pub(crate) fn take_delay_scheduler(&self) -> Option<SingleThreadScheduledExecutorService> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        lifecycle.delay_scheduler.take()
    }

    /// Completes a shutdown after subscriptions and runtime resources are
    /// cleared.
    pub(crate) fn complete_shutdown(&self) {
        if let Ok(mut lifecycle) = self.lifecycle.lock() {
            lifecycle.state = LifecycleState::Stopped;
        }
    }

    /// Returns whether the bus is currently started.
    ///
    /// # Returns
    /// `true` if publishing and subscribing are allowed.
    pub(crate) fn is_started(&self) -> bool {
        self.lifecycle
            .lock()
            .map(|lifecycle| lifecycle.state == LifecycleState::Started)
            .unwrap_or(false)
    }

    /// Builds the subscription handler executor.
    ///
    /// # Returns
    /// A fixed thread pool configured for subscriber processing.
    ///
    /// # Errors
    /// Returns executor build errors from `rs-thread-pool`.
    fn build_subscription_handler_executor(&self) -> Result<FixedThreadPool, ExecutorServiceBuilderError> {
        let mut builder = FixedThreadPool::builder()
            .pool_size(self.subscription_handler_pool_size)
            .thread_name_prefix("qubit-event-bus-subscriber");
        if let Some(capacity) = self.delivery_limits.handler_queue_capacity() {
            builder = builder.queue_capacity(capacity);
        }
        builder.build()
    }

    /// Builds the delayed-delivery scheduled executor service.
    ///
    /// # Returns
    /// A scheduled executor service used to wait for delayed deliveries.
    ///
    /// # Errors
    /// Returns executor build errors from `rs-executor`.
    fn build_delay_scheduler(&self) -> Result<SingleThreadScheduledExecutorService, ExecutorServiceBuilderError> {
        SingleThreadScheduledExecutorService::new("qubit-event-bus-delay")
    }
}

/// Returns the executor if the current lifecycle allows dispatch.
pub(super) fn executor_for_dispatch(
    lifecycle: &LocalEventBusLifecycle,
    allow_stopping: bool,
) -> EventBusResult<&FixedThreadPool> {
    if lifecycle.state != LifecycleState::Started && !(allow_stopping && lifecycle.state == LifecycleState::Stopping) {
        return Err(EventBusError::not_started());
    }
    lifecycle.executor.as_ref().ok_or_else(EventBusError::not_started)
}

/// Returns the delayed-delivery scheduler if the lifecycle allows dispatch.
pub(super) fn delay_scheduler_for_dispatch(
    lifecycle: &LocalEventBusLifecycle,
    allow_stopping: bool,
) -> EventBusResult<&SingleThreadScheduledExecutorService> {
    if lifecycle.state != LifecycleState::Started && !(allow_stopping && lifecycle.state == LifecycleState::Stopping) {
        return Err(EventBusError::not_started());
    }
    lifecycle
        .delay_scheduler
        .as_ref()
        .ok_or_else(EventBusError::not_started)
}

/// Converts an executor build failure into a local event-bus startup failure.
fn start_failed_from_thread_pool_error(error: ExecutorServiceBuilderError) -> EventBusError {
    EventBusError::start_failed(error.to_string())
}
