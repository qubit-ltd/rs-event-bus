// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-worker bounded notification publication.

use std::io;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::TrySendError as ChannelTrySendError;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use super::internal::WorkerCompletionGuard;
use super::internal::WorkerExit;
use super::internal::WorkerState;
use super::notification_config::DEFAULT_QUEUE_CAPACITY;
use super::notification_outcome::NotificationOutcome;
use super::notification_stats::NotificationStats;
use super::notification_stats_snapshot::NotificationStatsSnapshot;
use super::try_publish_error::TryPublishError;
use crate::EventBus;
use crate::model::PublishRequest;
use crate::model::Topic;

/// A bounded queue that calls a synchronous event bus from one worker thread.
///
/// Queue admission never waits for the worker or provider. `close` stops new
/// admission, drains accepted notifications, and blocks until the worker exits.
/// The worker invokes the observer synchronously; observer panics are contained
/// and counted. Dropping this handle closes its sender without waiting. It does
/// not shut down the injected event bus.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::NotificationPublisher;
/// use qubit_event_bus::model::Topic;
///
/// let bus = EventBus::local(Default::default())?;
/// let publisher = NotificationPublisher::new(
///     bus,
///     Topic::<String>::new("notifications")?,
///     NotificationPublisher::<String>::default_capacity(),
///     |_| {},
/// )?;
/// publisher.try_publish("task changed".to_owned())?;
/// publisher.close()?;
/// # Ok(())
/// # }
/// ```
#[must_use]
pub struct NotificationPublisher<T: Send + Sync + 'static> {
    /// Admission sender; `None` means close has stopped new queue entries.
    sender: Mutex<Option<SyncSender<T>>>,
    /// Worker handle, taken exactly once by the first closer that joins it.
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    /// Worker identity retained independently of the join handle.
    worker_thread_id: thread::ThreadId,
    /// Worker terminal state and condition variable shared with close callers.
    state: Arc<(Mutex<WorkerState>, Condvar)>,
    /// Atomic counters exposed through [`Self::stats`].
    stats: Arc<NotificationStats>,
}

impl<T: Send + Sync + 'static> NotificationPublisher<T> {
    /// Starts a named worker with a bounded FIFO queue.
    ///
    /// `observer` runs on the worker thread after each request result and
    /// should return promptly. The observer receives provider admission
    /// outcomes, not handler completion. Returns an I/O error if the worker
    /// thread cannot be started.
    ///
    /// # Type Parameters
    /// - `F`: observer callback type.
    ///
    /// # Parameters
    /// - `bus`: event bus used by the worker to publish queued payloads.
    /// - `topic`: topic assigned to every queued payload.
    /// - `capacity`: maximum number of queued payloads awaiting publication.
    /// - `observer`: callback invoked on the worker after each publish result.
    ///
    /// # Returns
    /// A publisher handle that performs nonblocking queue admission.
    ///
    /// # Errors
    /// Returns an I/O error if the worker thread cannot be started.
    pub fn new<F>(bus: EventBus, topic: Topic<T>, capacity: NonZeroUsize, observer: F) -> io::Result<Self>
    where
        F: Fn(NotificationOutcome) + Send + Sync + 'static,
    {
        let (sender, receiver) = sync_channel(capacity.get());
        let stats = Arc::new(NotificationStats::default());
        let worker_stats = stats.clone();
        let state = Arc::new((Mutex::new(WorkerState { exit: None }), Condvar::new()));
        let worker_state = state.clone();
        let observer = Arc::new(observer);
        let worker = thread::Builder::new()
            .name("event-notification-publisher".into())
            .spawn(move || {
                let mut completion = WorkerCompletionGuard::new(worker_state, worker_stats.clone());
                // The move closure owns every user-controlled resource so both
                // normal cleanup and unwind cleanup remain inside this boundary.
                let result = catch_unwind(AssertUnwindSafe(move || {
                    while let Ok(payload) = receiver.recv() {
                        let outcome = match PublishRequest::new(topic.clone(), payload) {
                            Ok(request) => match bus.publish(request) {
                                Ok(receipt) => {
                                    NotificationStats::increment(&worker_stats.published);
                                    NotificationOutcome::Published(receipt)
                                }
                                Err(error) => {
                                    NotificationStats::increment(&worker_stats.publish_errors);
                                    NotificationOutcome::PublishFailed(error)
                                }
                            },
                            Err(error) => {
                                NotificationStats::increment(&worker_stats.request_errors);
                                NotificationOutcome::RequestFailed(error)
                            }
                        };
                        if catch_unwind(AssertUnwindSafe(|| observer(outcome))).is_err() {
                            NotificationStats::increment(&worker_stats.observer_panicked);
                        }
                    }
                    drop(receiver);
                    drop(topic);
                    drop(bus);
                    drop(observer);
                }));
                if result.is_ok() {
                    completion.mark_drained();
                }
            })?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker_thread_id: worker.thread().id(),
            worker: Mutex::new(Some(worker)),
            state,
            stats,
        })
    }

    /// Returns the default queue capacity used by applications that select it.
    ///
    /// # Returns
    /// The nonzero default queue capacity.
    ///
    /// # Panics
    /// Panics if the crate's configured default queue capacity is zero.
    #[must_use]
    #[inline]
    pub const fn default_capacity() -> NonZeroUsize {
        match NonZeroUsize::new(DEFAULT_QUEUE_CAPACITY) {
            Some(capacity) => capacity,
            None => unreachable!(),
        }
    }

    /// Returns a monotonic snapshot of queue and worker outcomes.
    ///
    /// # Returns
    /// A best-effort snapshot whose counters are loaded independently.
    #[must_use]
    #[inline]
    pub fn stats(&self) -> NotificationStatsSnapshot {
        self.stats.snapshot()
    }

    /// Attempts to queue one payload without waiting for provider work.
    ///
    /// Returns `Full(payload)` when all configured queue slots are occupied or
    /// `Closed(payload)` after close has stopped admission.
    ///
    /// # Parameters
    /// - `payload`: event payload to enqueue without blocking.
    ///
    /// # Returns
    /// `Ok(())` when queued, or an error containing the original payload when
    /// admission fails.
    ///
    /// # Errors
    /// Returns `Full` when the bounded queue has no free slot and `Closed`
    /// when admission has stopped or the worker has disconnected.
    pub fn try_publish(&self, payload: T) -> Result<(), TryPublishError<T>> {
        let sender = self.sender.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(sender) = sender.as_ref() else {
            NotificationStats::increment(&self.stats.queue_closed);
            return Err(TryPublishError::Closed(payload));
        };
        match sender.try_send(payload) {
            Ok(()) => {
                NotificationStats::increment(&self.stats.enqueued);
                Ok(())
            }
            Err(ChannelTrySendError::Full(payload)) => {
                NotificationStats::increment(&self.stats.queue_full);
                Err(TryPublishError::Full(payload))
            }
            Err(ChannelTrySendError::Disconnected(payload)) => {
                NotificationStats::increment(&self.stats.queue_closed);
                Err(TryPublishError::Closed(payload))
            }
        }
    }

    /// Stops admission, drains the queue, and waits for the worker to finish.
    ///
    /// Multiple callers may close concurrently; each waits for the same worker
    /// completion and only one caller joins its thread handle. Returns an I/O
    /// error if called from the worker thread or if the worker panicked outside
    /// contained observer panics.
    ///
    /// # Returns
    /// `Ok(())` after all queued payloads are processed and the worker exits.
    ///
    /// # Errors
    /// Returns an error when called from the worker thread or when the worker
    /// panicked outside contained observer panics.
    pub fn close(&self) -> io::Result<()> {
        self.close_inner(None)
    }

    /// Stops admission and waits up to `timeout` for accepted notifications to
    /// drain and the worker thread to exit.
    ///
    /// On timeout, the worker continues processing the accepted queue and a
    /// later close call can wait for completion. New calls to `try_publish`
    /// return `Closed` after this method begins. A synchronous provider call
    /// cannot be forcibly interrupted.
    ///
    /// # Parameters
    /// - `timeout`: maximum time allowed for draining and worker shutdown.
    ///
    /// # Returns
    /// `Ok(())` after the worker exits and is joined.
    ///
    /// # Errors
    /// Returns `TimedOut` when the worker has not exited before the deadline,
    /// `Other` when called from the worker thread or when the worker panics.
    pub fn close_with_timeout(&self, timeout: Duration) -> io::Result<()> {
        self.close_inner(Some(timeout))
    }

    /// Closes admission and waits for worker completion under the requested
    /// deadline.
    ///
    /// # Parameters
    /// - `timeout`: maximum wait, or `None` to wait without a deadline.
    ///
    /// # Returns
    /// `Ok(())` after the worker exits and is joined.
    ///
    /// # Errors
    /// Returns an I/O error when called from the worker thread, when the
    /// deadline expires, or when the worker panics.
    fn close_inner(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.reject_worker_thread()?;
        let started = Instant::now();
        let sender = self.sender.lock().unwrap_or_else(PoisonError::into_inner).take();
        drop(sender);
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(PoisonError::into_inner);
        while state.exit.is_none() {
            state = match timeout {
                None => changed.wait(state).unwrap_or_else(PoisonError::into_inner),
                Some(limit) => {
                    let remaining = remaining_timeout(limit, started)?;
                    let (next_state, _) = changed
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(PoisonError::into_inner);
                    next_state
                }
            };
        }
        let exit = state.exit;
        drop(state);
        self.finish_join(timeout, started)?;
        match exit {
            Some(WorkerExit::Drained) => Ok(()),
            Some(WorkerExit::Panicked) | None => Err(io::Error::other("notification publisher worker panicked")),
        }
    }

    /// Rejects a blocking close request made by the worker it would join.
    ///
    /// # Returns
    /// `Ok(())` when the caller is not the worker thread.
    ///
    /// # Errors
    /// Returns `Other` when called from the worker thread.
    fn reject_worker_thread(&self) -> io::Result<()> {
        if self.worker_thread_id == thread::current().id() {
            return Err(io::Error::other(
                "notification publisher cannot close from its worker thread",
            ));
        }
        Ok(())
    }

    /// Joins the worker only after the operating system reports it exited.
    ///
    /// # Parameters
    /// - `timeout`: maximum total wait, or `None` to wait without a deadline.
    /// - `started`: instant at which the enclosing close operation began.
    ///
    /// # Returns
    /// `Ok(())` after joining the worker.
    ///
    /// # Errors
    /// Returns `TimedOut` when the deadline expires or `Other` when the worker
    /// panicked.
    fn finish_join(&self, timeout: Option<Duration>, started: Instant) -> io::Result<()> {
        loop {
            let worker_finished = self
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .is_none_or(thread::JoinHandle::is_finished);
            if worker_finished {
                break;
            }
            match timeout {
                None => thread::sleep(Duration::from_millis(1)),
                Some(limit) => thread::sleep(remaining_timeout(limit, started)?.min(Duration::from_millis(1))),
            }
        }
        if let Some(worker) = self.worker.lock().unwrap_or_else(PoisonError::into_inner).take() {
            worker
                .join()
                .map_err(|_| io::Error::other("notification publisher worker panicked"))?;
        }
        Ok(())
    }
}

/// Returns the remaining close time, or a timeout error after the deadline.
///
/// # Parameters
/// - `limit`: total time allowed for the close operation.
/// - `started`: instant at which that operation began.
///
/// # Returns
/// The unelapsed portion of `limit`.
///
/// # Errors
/// Returns `TimedOut` when the deadline has elapsed.
fn remaining_timeout(limit: Duration, started: Instant) -> io::Result<Duration> {
    limit
        .checked_sub(started.elapsed())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "notification publisher close timed out"))
}

impl<T: Send + Sync + 'static> Drop for NotificationPublisher<T> {
    /// Closes queue admission without waiting for worker completion.
    fn drop(&mut self) {
        self.sender.get_mut().unwrap_or_else(PoisonError::into_inner).take();
    }
}
