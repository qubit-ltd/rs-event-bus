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
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::TrySendError as ChannelTrySendError;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;
use std::time::Instant;

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
pub struct NotificationPublisher<T: Send + Sync + 'static> {
    sender: Mutex<Option<SyncSender<T>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    state: Arc<(Mutex<WorkerState>, Condvar)>,
    stats: Arc<NotificationStats>,
}

impl<T: Send + Sync + 'static> NotificationPublisher<T> {
    /// Starts a named worker with a bounded FIFO queue.
    ///
    /// `observer` runs on the worker thread after each request result and
    /// should return promptly. The observer receives provider admission
    /// outcomes, not handler completion. Returns an I/O error if the worker
    /// thread cannot be started.
    pub fn new<F>(bus: EventBus, topic: Topic<T>, capacity: NonZeroUsize, observer: F) -> io::Result<Self>
    where
        F: Fn(NotificationOutcome) + Send + Sync + 'static,
    {
        let (sender, receiver) = sync_channel(capacity.get());
        let stats = Arc::new(NotificationStats::default());
        let worker_stats = stats.clone();
        let state = Arc::new((Mutex::new(WorkerState { finished: false }), Condvar::new()));
        let worker_state = state.clone();
        let observer = Arc::new(observer);
        let worker = thread::Builder::new()
            .name("event-notification-publisher".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
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
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(outcome))).is_err() {
                            NotificationStats::increment(&worker_stats.observer_panicked);
                        }
                    }
                }));
                if result.is_err() {
                    NotificationStats::increment(&worker_stats.worker_panicked);
                }
                drop(receiver);
                drop(topic);
                drop(bus);
                drop(observer);
                drop(worker_stats);
                let (lock, changed) = &*worker_state;
                lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner).finished = true;
                changed.notify_all();
            })?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
            state,
            stats,
        })
    }

    /// Returns the default queue capacity used by applications that select it.
    #[must_use]
    pub const fn default_capacity() -> NonZeroUsize {
        match NonZeroUsize::new(DEFAULT_QUEUE_CAPACITY) {
            Some(capacity) => capacity,
            None => unreachable!(),
        }
    }

    /// Attempts to queue one payload without waiting for provider work.
    ///
    /// Returns `Full(payload)` when all configured queue slots are occupied or
    /// `Closed(payload)` after close has stopped admission.
    pub fn try_publish(&self, payload: T) -> Result<(), TryPublishError<T>> {
        let sender = self.sender.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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

    /// Returns a monotonic snapshot of queue and worker outcomes.
    #[must_use]
    pub fn stats(&self) -> NotificationStatsSnapshot {
        self.stats.snapshot()
    }

    /// Stops admission, drains the queue, and waits for the worker to finish.
    ///
    /// Multiple callers may close concurrently; each waits for the same worker
    /// completion and only one caller joins its thread handle. Returns an I/O
    /// error if called from the worker thread or if the worker panicked outside
    /// contained observer panics.
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
    /// # Errors
    /// Returns `TimedOut` when the worker has not exited before the deadline,
    /// `Other` when called from the worker thread or when the worker panics.
    pub fn close_with_timeout(&self, timeout: Duration) -> io::Result<()> {
        self.close_inner(Some(timeout))
    }

    /// Closes admission and waits for worker completion under the requested
    /// deadline.
    fn close_inner(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.reject_worker_thread()?;
        let started = Instant::now();
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.finished {
            state = match timeout {
                None => changed.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner),
                Some(limit) => {
                    let remaining = remaining_timeout(limit, started)?;
                    changed
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .0
                }
            };
        }
        drop(state);
        self.finish_join(timeout, started)
    }

    /// Rejects a blocking close request made by the worker it would join.
    fn reject_worker_thread(&self) -> io::Result<()> {
        let called_from_worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|worker| worker.thread().id() == thread::current().id());
        if called_from_worker {
            return Err(io::Error::other(
                "notification publisher cannot close from its worker thread",
            ));
        }
        Ok(())
    }

    /// Joins the worker only after the operating system reports it exited.
    fn finish_join(&self, timeout: Option<Duration>, started: Instant) -> io::Result<()> {
        loop {
            let worker_finished = self
                .worker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            worker
                .join()
                .map_err(|_| io::Error::other("notification publisher worker panicked"))?;
        }
        if self.stats.worker_panicked.load(std::sync::atomic::Ordering::Acquire) > 0 {
            return Err(io::Error::other("notification publisher worker panicked"));
        }
        Ok(())
    }
}

/// Returns the remaining close time, or a timeout error after the deadline.
fn remaining_timeout(limit: Duration, started: Instant) -> io::Result<Duration> {
    limit
        .checked_sub(started.elapsed())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "notification publisher close timed out"))
}

impl<T: Send + Sync + 'static> Drop for NotificationPublisher<T> {
    fn drop(&mut self) {
        self.sender
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}
