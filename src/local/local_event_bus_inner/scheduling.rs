// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscriber task submission and delayed-delivery scheduling.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use qubit_executor::CancelResult;
use qubit_executor::ExecutorService;
use qubit_executor::ScheduledExecutorService;
use qubit_thread_pool::FixedThreadPool;

use super::LocalEventBusInner;
use super::lifecycle::delay_scheduler_for_dispatch;
use super::lifecycle::executor_for_dispatch;
use crate::EventBusError;
use crate::EventBusResult;
use crate::core::SubscriptionState;
use crate::local::processing_task::ProcessingTask;

impl LocalEventBusInner {
    /// Submits subscriber processing work to the handler pool.
    ///
    /// # Parameters
    /// - `task`: One-shot task that owns one subscriber delivery.
    /// - `allow_stopping`: Whether the stopping state may still accept the
    ///   task.
    ///
    /// # Returns
    /// `Ok(())` when the pool accepts the task.
    ///
    /// # Errors
    /// Returns lock-poisoning or executor rejection errors before the task
    /// runs.
    pub(crate) fn submit_processing_task<F>(&self, task: F, allow_stopping: bool) -> EventBusResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        let executor = executor_for_dispatch(&lifecycle, allow_stopping)?;
        submit_processing_task_to_executor(executor, task)
    }

    /// Schedules delayed subscriber processing without occupying a handler
    /// worker.
    ///
    /// # Parameters
    /// - `task`: Subscriber processing task accepted by dispatch.
    /// - `delay`: Delay before the task can enter the handler pool.
    /// - `subscription_state`: State used to wake the delay when the
    ///   subscription is cancelled.
    /// - `allow_stopping`: Whether the stopping state may still accept the
    ///   task.
    ///
    /// # Returns
    /// `Ok(())` after the delay wait has been scheduled.
    ///
    /// # Errors
    /// Returns executor admission errors if the bus is not accepting dispatch
    /// or the delayed-delivery executor rejects the delay waiter.
    pub(crate) fn submit_delayed_processing_task(
        self: &Arc<Self>,
        task: ProcessingTask,
        delay: Duration,
        subscription_state: Arc<SubscriptionState>,
        allow_stopping: bool,
    ) -> EventBusResult<()> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| EventBusError::lock_poisoned("lifecycle"))?;
        let delay_scheduler = delay_scheduler_for_dispatch(&lifecycle, allow_stopping)?;
        let bus = Arc::clone(self);
        let task_slot = Arc::new(Mutex::new(Some(task)));
        let registration = Arc::new(Mutex::new(None::<usize>));
        let fired = Arc::new(Mutex::new(false));
        let task_for_delay = Arc::clone(&task_slot);
        let registration_for_delay = Arc::clone(&registration);
        let fired_for_delay = Arc::clone(&fired);
        let subscription_for_delay = Arc::clone(&subscription_state);
        let scheduled = delay_scheduler.schedule(delay, move || {
            if let Ok(mut fired) = fired_for_delay.lock() {
                *fired = true;
            }
            if let Ok(mut registration) = registration_for_delay.lock()
                && let Some(registration_id) = registration.take()
            {
                subscription_for_delay.unregister_delay_cancellation(registration_id);
            }
            if subscription_for_delay.is_active() {
                let task_for_executor = Arc::clone(&task_for_delay);
                let result = bus.submit_processing_task(
                    move || {
                        if let Ok(mut task) = task_for_executor.lock()
                            && let Some(task) = task.take()
                        {
                            task.run();
                        }
                    },
                    true,
                );
                match result {
                    Ok(()) => {}
                    Err(error) => {
                        let recovered_task = match task_for_delay.lock() {
                            Ok(mut task) => task.take(),
                            Err(_) => None,
                        };
                        if let Some(task) = recovered_task {
                            task.reject(&error);
                        }
                    }
                }
            }
            Ok::<(), EventBusError>(())
        });
        let handle = match scheduled {
            Ok(handle) => handle,
            Err(error) => {
                return Err(EventBusError::execution_rejected(error.to_string()));
            }
        };
        if fired.lock().map(|fired| *fired).unwrap_or(true) {
            return Ok(());
        }
        let task_for_cancel = Arc::clone(&task_slot);
        let handle_for_cancel = Arc::new(Mutex::new(Some(handle)));
        let registration_id = subscription_state.register_delay_cancellation(move || {
            if let Ok(mut handle) = handle_for_cancel.lock()
                && let Some(handle) = handle.take()
                && handle.cancel() == CancelResult::Cancelled
                && let Ok(mut task) = task_for_cancel.lock()
            {
                let _ = task.take();
            }
        });
        if let Some(registration_id) = registration_id
            && let Ok(mut registration) = registration.lock()
        {
            if fired.lock().map(|fired| *fired).unwrap_or(true) {
                subscription_state.unregister_delay_cancellation(registration_id);
            } else {
                *registration = Some(registration_id);
            }
        }
        Ok(())
    }
}

/// Submits subscriber processing work to the executor.
pub(super) fn submit_processing_task_to_executor<F>(executor: &FixedThreadPool, task: F) -> EventBusResult<()>
where
    F: FnOnce() + Send + 'static,
{
    let mut task = Some(task);
    executor
        .submit_callable(move || {
            let task = take_subscription_task(&mut task)?;
            task();
            Ok::<(), EventBusError>(())
        })
        .map(|_handle| ())
        .map_err(|error| EventBusError::execution_rejected(error.to_string()))
}

/// Takes a one-shot subscription task from executor state.
///
/// # Parameters
/// - `task`: Mutable one-shot task slot.
///
/// # Returns
/// Task to invoke exactly once.
///
/// # Errors
/// Returns [`EventBusError::HandlerFailed`] when the executor invokes the same
/// callable more than once.
fn take_subscription_task<F>(task: &mut Option<F>) -> EventBusResult<F>
where
    F: FnOnce() + Send + 'static,
{
    match task.take() {
        Some(task) => Ok(task),
        None => Err(EventBusError::handler_failed(
            "subscription task was invoked more than once",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::take_subscription_task;
    use crate::EventBusError;

    #[test]
    fn test_subscription_task_can_only_be_taken_once() {
        let mut task = Some(|| {});

        assert!(take_subscription_task(&mut task).is_ok());
        assert!(matches!(
            take_subscription_task(&mut task),
            Err(EventBusError::HandlerFailed { .. })
        ));
    }
}
