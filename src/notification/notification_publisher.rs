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

use super::notification_config::DEFAULT_QUEUE_CAPACITY;
use super::notification_outcome::NotificationOutcome;
use super::notification_stats::NotificationStats;
use super::notification_stats_snapshot::NotificationStatsSnapshot;
use super::try_publish_error::TryPublishError;
use crate::EventBus;
use crate::model::PublishRequest;
use crate::model::Topic;

struct WorkerState {
    finished: bool,
}

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
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.finished {
            state = changed.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(state);
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

impl<T: Send + Sync + 'static> Drop for NotificationPublisher<T> {
    fn drop(&mut self) {
        self.sender
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}
