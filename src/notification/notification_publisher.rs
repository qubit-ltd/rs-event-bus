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

use super::internal::QueuedNotification;
use super::internal::WeightBudget;
use super::internal::WorkerCompletionGuard;
use super::internal::WorkerExit;
use super::internal::WorkerState;
use super::notification_admission_error::NotificationAdmissionError;
use super::notification_config::DEFAULT_QUEUE_CAPACITY;
use super::notification_outcome::NotificationOutcome;
use super::notification_rejection_reason::NotificationRejectionReason;
use super::notification_stats::NotificationStats;
use super::notification_stats_snapshot::NotificationStatsSnapshot;
use super::try_publish_error::TryPublishError;
use crate::EventBus;
use crate::internal::panic_payload::dispose_panic_payload;
use crate::model::PublishOptions;
use crate::model::PublishRequest;
use crate::model::Topic;

/// A bounded queue that calls a synchronous event bus from one worker thread.
///
/// Queue admission never waits for queue capacity, the worker, or provider
/// work. It does acquire an internal mutex and can wait under mutex contention.
/// `close` stops new admission, drains accepted notifications, and blocks until
/// the worker exits.
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
///
/// # Type Parameters
/// - `T`: notification payload type sent to the event bus.
#[must_use]
pub struct NotificationPublisher<T: Send + Sync + 'static> {
    /// Admission sender; `None` means close has stopped new queue entries.
    sender: Mutex<Option<SyncSender<QueuedNotification<T>>>>,
    /// Worker handle, taken exactly once by the first closer that joins it.
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    /// Worker identity retained independently of the join handle.
    worker_thread_id: thread::ThreadId,
    /// Worker terminal state and condition variable shared with close callers.
    state: Arc<(Mutex<WorkerState>, Condvar)>,
    /// Atomic counters exposed through [`Self::stats`].
    stats: Arc<NotificationStats>,
    /// Optional cumulative declared-weight limit for this publisher.
    weight_budget: Option<Arc<WeightBudget>>,
}

/// Maps a detailed rejection onto the legacy two-variant error contract.
///
/// # Parameters
/// - `error`: detailed admission rejection with its original payload.
///
/// # Returns
/// `Closed` when admission has stopped, otherwise the legacy `Full` variant.
///
/// # Type Parameters
/// - `T`: payload type carried by the admission error.
fn map_admission_error<T>(error: NotificationAdmissionError<T>) -> TryPublishError<T> {
    let reason = error.reason();
    let payload = error.into_payload();
    match reason {
        NotificationRejectionReason::Closed => TryPublishError::Closed(payload),
        _ => TryPublishError::Full(payload),
    }
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
    /// A publisher handle that admits queue entries without waiting for queue
    /// capacity or provider work. Admission can wait for an internal mutex.
    ///
    /// # Errors
    /// Returns an I/O error if the worker thread cannot be started.
    pub fn new<F>(bus: EventBus, topic: Topic<T>, capacity: NonZeroUsize, observer: F) -> io::Result<Self>
    where
        F: Fn(NotificationOutcome) + Send + Sync + 'static,
    {
        Self::with_publish_options(bus, topic, capacity, PublishOptions::default(), observer)
    }

    /// Starts a bounded notification publisher using options for each request.
    ///
    /// The worker creates each request and applies a clone of `options` before
    /// calling the event bus. Option callbacks, including a native payload
    /// weight callback, therefore run on the background publication path. The
    /// notification queue remains bounded by item count; a declared weight is
    /// used by providers that enforce an outstanding-weight budget.
    ///
    /// # Type Parameters
    /// - `F`: observer callback type.
    ///
    /// # Parameters
    /// - `bus`: event bus used by the worker to publish queued payloads.
    /// - `topic`: topic assigned to every queued payload.
    /// - `capacity`: maximum number of queued payloads awaiting publication.
    /// - `options`: options cloned onto each request published by the worker.
    /// - `observer`: callback invoked on the worker after each publish result.
    ///
    /// # Returns
    /// A publisher handle that admits queue entries without waiting for queue
    /// capacity or provider work. Admission can wait for an internal mutex.
    ///
    /// # Errors
    /// Returns an I/O error if the worker thread cannot be started.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::num::NonZeroUsize;
    /// use qubit_event_bus::EventBus;
    /// use qubit_event_bus::NotificationPublisher;
    /// use qubit_event_bus::local::LocalEventBusConfig;
    /// use qubit_event_bus::model::PublishOptions;
    /// use qubit_event_bus::model::Topic;
    ///
    /// let bus = EventBus::local(
    ///     LocalEventBusConfig::new()
    ///         .max_total_outstanding_weight_bytes(NonZeroUsize::new(1024).unwrap()),
    /// )?;
    /// let options = PublishOptions::<String>::builder()
    ///     .native_payload_weight(|payload| {
    ///         NonZeroUsize::new(payload.len().max(1)).expect("positive declared weight")
    ///     })
    ///     .build();
    /// let publisher = NotificationPublisher::with_publish_options(
    ///     bus.clone(),
    ///     Topic::<String>::new("notifications")?,
    ///     NonZeroUsize::new(16).unwrap(),
    ///     options,
    ///     |_| {},
    /// )?;
    /// publisher.try_publish("task changed".to_owned())?;
    /// publisher.close()?;
    /// bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_publish_options<F>(
        bus: EventBus,
        topic: Topic<T>,
        capacity: NonZeroUsize,
        options: PublishOptions<T>,
        observer: F,
    ) -> io::Result<Self>
    where
        F: Fn(NotificationOutcome) + Send + Sync + 'static,
    {
        Self::with_optional_weight_budget(bus, topic, capacity, options, observer, None)
    }

    /// Creates a publisher with an optional cumulative accepted-weight limit.
    ///
    /// # Type Parameters
    /// - `F`: callback invoked for each publication outcome.
    ///
    /// # Parameters
    /// - `bus`: event bus used to publish accepted payloads.
    /// - `topic`: destination topic.
    /// - `capacity`: maximum number of entries waiting in the queue.
    /// - `options`: options applied to each publish request.
    /// - `observer`: callback invoked after each publication attempt.
    /// - `weight_budget`: optional cumulative budget for weighted entries.
    ///
    /// # Errors
    /// Returns an error if the worker thread cannot be started.
    pub(super) fn with_optional_weight_budget<F>(
        bus: EventBus,
        topic: Topic<T>,
        capacity: NonZeroUsize,
        options: PublishOptions<T>,
        observer: F,
        weight_budget: Option<Arc<WeightBudget>>,
    ) -> io::Result<Self>
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
                    while let Ok(entry) = receiver.recv() {
                        let QueuedNotification { payload, reservation } = entry;
                        let outcome = match PublishRequest::new(topic.clone(), payload) {
                            Ok(request) => match bus.publish(request.with_options(options.clone())) {
                                Ok(receipt) => {
                                    NotificationStats::increment(&worker_stats.published);
                                    worker_stats.record_admission(receipt.admission_outcome());
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
                        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| observer(outcome))) {
                            NotificationStats::increment(&worker_stats.observer_panicked);
                            dispose_panic_payload(payload);
                        }
                        drop(reservation);
                    }
                    drop(receiver);
                    drop(topic);
                    drop(options);
                    drop(bus);
                    drop(observer);
                }));
                match result {
                    Ok(()) => completion.mark_drained(),
                    Err(payload) => dispose_panic_payload(payload),
                }
            })?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker_thread_id: worker.thread().id(),
            worker: Mutex::new(Some(worker)),
            state,
            stats,
            weight_budget,
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

    /// Attempts to queue one payload without waiting for queue capacity or
    /// provider work. The call acquires an internal mutex and can wait when
    /// another thread holds that mutex.
    ///
    /// Returns `Full(payload)` when all configured queue slots are occupied or
    /// `Closed(payload)` after close has stopped admission.
    ///
    /// # Parameters
    /// - `payload`: event payload to enqueue without waiting for queue capacity
    ///   or provider work; mutex contention can make the call wait.
    ///
    /// # Returns
    /// `Ok(())` when queued, or an error containing the original payload when
    /// admission fails.
    ///
    /// # Errors
    /// Returns `Full` when the bounded queue has no free slot and `Closed`
    /// when admission has stopped or the worker has disconnected.
    pub fn try_publish(&self, payload: T) -> Result<(), TryPublishError<T>> {
        self.try_publish_detailed(payload).map_err(map_admission_error)
    }

    /// Attempts to queue one payload and returns the specific admission reason
    /// when the bounded queue rejects it.
    ///
    /// # Parameters
    /// - `payload`: event payload to enqueue.
    ///
    /// # Returns
    /// `Ok(())` when queued, or an error owning the original payload and the
    /// observed rejection reason.
    pub fn try_publish_detailed(&self, payload: T) -> Result<(), NotificationAdmissionError<T>> {
        self.try_publish_entry(payload, None)
    }

    /// Attempts to queue a payload with a declared weight through the shared
    /// worker implementation.
    ///
    /// # Parameters
    /// - `payload`: event payload to enqueue.
    /// - `weight`: positive caller-declared accounting weight.
    ///
    /// # Returns
    /// `Ok(())` when queued, or the original payload in the error when
    /// rejected.
    pub(super) fn try_publish_weighted(&self, payload: T, weight: NonZeroUsize) -> Result<(), TryPublishError<T>> {
        self.try_publish_weighted_detailed(payload, weight)
            .map_err(map_admission_error)
    }

    /// Attempts weighted admission and returns the specific rejection reason.
    ///
    /// # Parameters
    /// - `payload`: event payload to enqueue.
    /// - `weight`: positive caller-declared accounting weight.
    ///
    /// # Returns
    /// `Ok(())` when queued, or an error owning the original payload and the
    /// observed rejection reason.
    pub(super) fn try_publish_weighted_detailed(
        &self,
        payload: T,
        weight: NonZeroUsize,
    ) -> Result<(), NotificationAdmissionError<T>> {
        self.try_publish_entry(payload, Some(weight))
    }

    /// Admits either the legacy unweighted entry or a weighted entry.
    ///
    /// # Parameters
    /// - `payload`: event payload to enqueue.
    /// - `weight`: optional caller-declared weight; `None` selects legacy
    ///   admission.
    ///
    /// # Returns
    /// `Ok(())` when queued, or the original payload in the error when
    /// rejected.
    fn try_publish_entry(&self, payload: T, weight: Option<NonZeroUsize>) -> Result<(), NotificationAdmissionError<T>> {
        let sender_guard = self.sender.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(sender) = sender_guard.as_ref() else {
            NotificationStats::increment(&self.stats.queue_closed);
            return Err(NotificationAdmissionError::new(
                NotificationRejectionReason::Closed,
                payload,
            ));
        };
        let reservation = match weight {
            Some(weight) => match self.weight_budget.as_ref().and_then(|budget| budget.reserve(weight)) {
                Some(reservation) => Some(reservation),
                None => {
                    let budget = self.weight_budget.as_ref().expect("weighted entries have a budget");
                    let reason = if budget.exceeds_limit(weight) {
                        NotificationStats::increment(&self.stats.oversized_weight_rejections);
                        NotificationRejectionReason::OversizedWeight
                    } else {
                        NotificationStats::increment(&self.stats.weight_capacity_rejections);
                        NotificationRejectionReason::WeightCapacity
                    };
                    NotificationStats::increment(&self.stats.queue_full);
                    return Err(NotificationAdmissionError::new(reason, payload));
                }
            },
            None => None,
        };
        match sender.try_send(QueuedNotification { payload, reservation }) {
            Ok(()) => {
                NotificationStats::increment(&self.stats.enqueued);
                Ok(())
            }
            Err(ChannelTrySendError::Full(entry)) => {
                NotificationStats::increment(&self.stats.queue_capacity_rejections);
                NotificationStats::increment(&self.stats.queue_full);
                drop(sender_guard);
                Err(NotificationAdmissionError::new(
                    NotificationRejectionReason::QueueCapacity,
                    entry.payload,
                ))
            }
            Err(ChannelTrySendError::Disconnected(entry)) => {
                NotificationStats::increment(&self.stats.queue_closed);
                drop(sender_guard);
                Err(NotificationAdmissionError::new(
                    NotificationRejectionReason::Closed,
                    entry.payload,
                ))
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
    /// The timeout budget starts when this method is called and covers the
    /// drain and worker-join waits, but acquiring internal mutexes is
    /// synchronous and cannot be interrupted. Therefore it is not a strict
    /// wall-clock upper bound for the whole method. On timeout,
    /// the worker continues processing the accepted queue and a later close
    /// call can wait for completion. Once this method acquires the admission
    /// mutex and removes the sender, new calls to `try_publish` return
    /// `Closed`. Concurrent publish attempts may still be admitted before that
    /// transition. A synchronous provider call cannot be forcibly interrupted.
    ///
    /// # Parameters
    /// - `timeout`: budget for draining and worker shutdown waits.
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
                    #[cfg(test)]
                    crate::internal::test_wait_probe::announce("notification_close");
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
