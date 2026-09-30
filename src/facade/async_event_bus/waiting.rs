// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral async deadline and signal waits.

use std::future::Future;
use std::future::poll_fn;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use qubit_clock::Timer;
use qubit_clock::TimerFuture;

use crate::LifecycleError;
use crate::WaitOutcome;
use crate::facade::async_event_bus::AsyncSignal;
use crate::facade::async_event_bus::ShutdownWait;
use crate::facade::async_event_bus::SignalRegistration;

/// Waits for a predicate or an optional relative timeout.
///
/// # Parameters
/// - `signal`: signal notifying changes that may satisfy `ready`.
/// - `timer`: clock used to construct an optional timeout future.
/// - `timeout`: maximum wait duration, or `None` to wait indefinitely.
/// - `ready`: predicate checked before and after waiter registration.
///
/// # Returns
/// `Idle` when `ready` succeeds, otherwise `TimedOut` when the deadline ends.
///
/// # Errors
/// Returns a lifecycle error if creating or polling the timer fails.
#[must_use = "the wait result must be handled"]
pub(in crate::facade) async fn wait_until(
    signal: &AsyncSignal,
    timer: &dyn Timer,
    timeout: Option<Duration>,
    mut ready: impl FnMut() -> bool,
) -> Result<WaitOutcome, LifecycleError> {
    let mut deadline = match timeout {
        Some(duration) => Some(timer.after(duration)?),
        None => None,
    };
    let registration = SignalRegistration::new(signal);
    poll_fn(|cx| {
        if ready() {
            return Poll::Ready(Ok(WaitOutcome::Idle));
        }
        if let Some(deadline) = deadline.as_mut() {
            match deadline.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    return Poll::Ready(Ok(WaitOutcome::TimedOut));
                }
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(LifecycleError::Timer(error)));
                }
                Poll::Pending => {}
            }
        }
        registration.register(cx.waker());
        if ready() {
            return Poll::Ready(Ok(WaitOutcome::Idle));
        }
        Poll::Pending
    })
    .await
}

/// Awaits provider shutdown until completion, deadline, or mode escalation.
///
/// # Type Parameters
/// - `F`: provider shutdown future type.
///
/// # Parameters
/// - `future`: provider shutdown operation.
/// - `deadline`: optional absolute caller deadline.
/// - `signal`: notification for shutdown mode escalation.
/// - `immediate`: flag set when immediate shutdown is requested.
///
/// # Returns
/// Whether shutdown completed, timed out, or should restart in immediate mode.
///
/// # Errors
/// Returns a lifecycle error if the timer fails.
#[must_use = "the shutdown wait must be driven and handled"]
pub(in crate::facade) async fn await_shutdown_or_immediate<F: Future>(
    future: F,
    deadline: Option<&mut TimerFuture>,
    signal: &AsyncSignal,
    immediate: &AtomicBool,
) -> Result<ShutdownWait<F::Output>, LifecycleError> {
    let mut future = Box::pin(future);
    let mut deadline = deadline;
    let registration = SignalRegistration::new(signal);
    poll_fn(|cx| {
        if immediate.load(Ordering::Acquire) {
            return Poll::Ready(Ok(ShutdownWait::ImmediateRequested));
        }
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(ShutdownWait::Complete(output)));
        }
        if let Some(deadline) = deadline.as_deref_mut() {
            match deadline.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => return Poll::Ready(Ok(ShutdownWait::TimedOut)),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(LifecycleError::Timer(error))),
                Poll::Pending => {}
            }
        }
        registration.register(cx.waker());
        if immediate.load(Ordering::Acquire) {
            return Poll::Ready(Ok(ShutdownWait::ImmediateRequested));
        }
        Poll::Pending
    })
    .await
}

/// Awaits a future until it completes or the shared optional deadline expires.
///
/// # Type Parameters
/// - `F`: future type being awaited.
///
/// # Parameters
/// - `future`: operation to await.
/// - `deadline`: optional absolute deadline shared with the shutdown caller.
///
/// # Returns
/// `Some(output)` on completion or `None` when the deadline expires.
///
/// # Errors
/// Returns a lifecycle error if polling the deadline timer fails.
#[must_use = "the deadline result must be handled"]
pub(in crate::facade) async fn await_until_deadline<F: Future>(
    future: F,
    deadline: Option<&mut TimerFuture>,
) -> Result<Option<F::Output>, LifecycleError> {
    let mut future = Box::pin(future);
    let Some(deadline) = deadline else {
        return Ok(Some(future.await));
    };
    poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(Some(output)));
        }
        match deadline.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(None)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(LifecycleError::Timer(error))),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

/// Waits for a predicate while polling a caller-owned absolute deadline.
///
/// # Parameters
/// - `signal`: signal notifying changes that may satisfy `ready`.
/// - `deadline`: optional absolute deadline owned by the caller.
/// - `ready`: predicate checked before and after waiter registration.
///
/// # Returns
/// `Idle` when `ready` succeeds, otherwise `TimedOut` when the deadline ends.
///
/// # Errors
/// Returns a lifecycle error if the timer fails.
#[must_use = "the deadline wait result must be handled"]
pub(in crate::facade) async fn wait_until_deadline(
    signal: &AsyncSignal,
    deadline: Option<&mut TimerFuture>,
    mut ready: impl FnMut() -> bool,
) -> Result<WaitOutcome, LifecycleError> {
    let Some(deadline) = deadline else {
        let registration = SignalRegistration::new(signal);
        return poll_fn(|cx| {
            if ready() {
                return Poll::Ready(Ok(WaitOutcome::Idle));
            }
            registration.register(cx.waker());
            if ready() {
                return Poll::Ready(Ok(WaitOutcome::Idle));
            }
            Poll::Pending
        })
        .await;
    };
    let registration = SignalRegistration::new(signal);
    poll_fn(|cx| {
        if ready() {
            return Poll::Ready(Ok(WaitOutcome::Idle));
        }
        match deadline.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => {
                return Poll::Ready(Ok(WaitOutcome::TimedOut));
            }
            Poll::Ready(Err(error)) => {
                return Poll::Ready(Err(LifecycleError::Timer(error)));
            }
            Poll::Pending => {}
        }
        registration.register(cx.waker());
        if ready() {
            return Poll::Ready(Ok(WaitOutcome::Idle));
        }
        Poll::Pending
    })
    .await
}
