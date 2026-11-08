// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-worker notification publication with a cumulative declared-weight
//! limit.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use super::internal::WeightBudget;
use super::notification_admission_error::NotificationAdmissionError;
use super::notification_outcome::NotificationOutcome;
use super::notification_publisher::NotificationPublisher;
use super::notification_stats_snapshot::NotificationStatsSnapshot;
use super::try_publish_error::TryPublishError;
use crate::EventBus;
use crate::model::PublishOptions;
use crate::model::Topic;

/// A bounded notification publisher limited by both queued item count and
/// cumulative declared weight still being published or observed.
///
/// The declared weight is supplied by the caller and represents application
/// accounting, not a measurement or bound on process memory. Queue capacity
/// still limits only items waiting in the queue; the outstanding-weight limit
/// also counts the item currently published or observed by the worker. A
/// [`TryPublishError::Full`] therefore means either limit rejected admission.
///
/// # Type Parameters
/// - `T`: thread-safe payload type published to the event bus.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::num::NonZeroUsize;
///
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::WeightedNotificationPublisher;
/// use qubit_event_bus::model::Topic;
///
/// let bus = EventBus::local(Default::default())?;
/// let publisher = WeightedNotificationPublisher::new(
///     bus,
///     Topic::<String>::new("notifications")?,
///     NonZeroUsize::new(16).unwrap(),
///     NonZeroUsize::new(1024).unwrap(),
///     |_| {},
/// )?;
/// publisher.try_publish("task changed".to_owned(), NonZeroUsize::new(32).unwrap())?;
/// publisher.close()?;
/// # Ok(())
/// # }
/// ```
#[must_use]
pub struct WeightedNotificationPublisher<T: Send + Sync + 'static> {
    /// Shared single-worker queue and lifecycle implementation.
    publisher: NotificationPublisher<T>,
}

impl<T: Send + Sync + 'static> WeightedNotificationPublisher<T> {
    /// Starts a weighted publisher using default publish options.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe callback invoked for each publication outcome.
    ///
    /// # Parameters
    /// - `bus`: event bus used to publish accepted payloads.
    /// - `topic`: destination topic.
    /// - `capacity`: maximum number of entries waiting in the queue.
    /// - `max_outstanding_weight`: cumulative weight limit including in-flight
    ///   work.
    /// - `observer`: callback invoked after each publication attempt.
    ///
    /// # Errors
    /// Returns an error if the worker thread cannot be started.
    pub fn new<F>(
        bus: EventBus,
        topic: Topic<T>,
        capacity: NonZeroUsize,
        max_outstanding_weight: NonZeroUsize,
        observer: F,
    ) -> io::Result<Self>
    where
        F: Fn(NotificationOutcome) + Send + Sync + 'static,
    {
        Self::with_publish_options(
            bus,
            topic,
            capacity,
            max_outstanding_weight,
            PublishOptions::default(),
            observer,
        )
    }

    /// Starts a weighted publisher using the given options for each publish.
    ///
    /// # Type Parameters
    /// - `F`: thread-safe callback invoked for each publication outcome.
    ///
    /// # Parameters
    /// - `bus`: event bus used to publish accepted payloads.
    /// - `topic`: destination topic.
    /// - `capacity`: maximum number of entries waiting in the queue.
    /// - `max_outstanding_weight`: cumulative weight limit including in-flight
    ///   work.
    /// - `options`: options applied to each publish request.
    /// - `observer`: callback invoked after each publication attempt.
    ///
    /// # Errors
    /// Returns an error if the worker thread cannot be started.
    pub fn with_publish_options<F>(
        bus: EventBus,
        topic: Topic<T>,
        capacity: NonZeroUsize,
        max_outstanding_weight: NonZeroUsize,
        options: PublishOptions<T>,
        observer: F,
    ) -> io::Result<Self>
    where
        F: Fn(NotificationOutcome) + Send + Sync + 'static,
    {
        let budget = Arc::new(WeightBudget::new(max_outstanding_weight));
        let publisher =
            NotificationPublisher::with_optional_weight_budget(bus, topic, capacity, options, observer, Some(budget))?;
        Ok(Self { publisher })
    }

    /// Tries to enqueue a payload with its caller-declared positive weight.
    ///
    /// # Parameters
    /// - `payload`: event payload to enqueue.
    /// - `weight`: positive caller-declared accounting weight.
    ///
    /// # Returns
    /// `Ok(())` when queued, or the original payload in the error when
    /// rejected.
    pub fn try_publish(&self, payload: T, weight: NonZeroUsize) -> Result<(), TryPublishError<T>> {
        self.publisher.try_publish_weighted(payload, weight)
    }

    /// Tries to queue a payload and returns the specific admission rejection.
    ///
    /// # Parameters
    /// - `payload`: event payload to enqueue.
    /// - `weight`: positive caller-declared accounting weight.
    ///
    /// # Returns
    /// `Ok(())` when queued, or an error owning the original payload and the
    /// observed rejection reason.
    pub fn try_publish_detailed(&self, payload: T, weight: NonZeroUsize) -> Result<(), NotificationAdmissionError<T>> {
        self.publisher.try_publish_weighted_detailed(payload, weight)
    }

    /// Returns a best-effort snapshot of queue, rejection-reason, and worker
    /// counters.
    ///
    /// `queue_full` includes queue-capacity, cumulative-weight, and
    /// oversized-item rejections. Reason counters are cumulative;
    /// concurrent snapshots are not guaranteed to sum consistently, and the
    /// declared weights are not memory measurements.
    #[must_use]
    pub fn stats(&self) -> NotificationStatsSnapshot {
        self.publisher.stats()
    }

    /// Stops admission, drains accepted notifications, and joins the worker.
    ///
    /// # Errors
    /// Returns an error if called from the worker thread or if the worker
    /// panicked.
    pub fn close(&self) -> io::Result<()> {
        self.publisher.close()
    }

    /// Stops admission and waits up to the given duration for the worker.
    ///
    /// This delegates to [`NotificationPublisher::close_with_timeout`]. The
    /// timeout covers draining accepted notifications and waiting for the
    /// worker to exit, but synchronous internal mutex acquisition cannot be
    /// interrupted and is not bounded by the timeout. A timed-out worker keeps
    /// running and may publish notifications that were already accepted.
    /// Once the call acquires the required locks and stops admission, new
    /// publish attempts return [`TryPublishError::Closed`]. A later call can
    /// wait for the same worker again. This method does not close the injected
    /// event bus. Calling it from the worker or its observer returns an error.
    ///
    /// # Parameters
    /// - `timeout`: budget for queue draining and worker completion waits.
    ///
    /// # Errors
    /// Returns `TimedOut` if the worker does not finish within the covered
    /// waits, `Other` if called from the worker thread, or an error if the
    /// worker panicked.
    pub fn close_with_timeout(&self, timeout: Duration) -> io::Result<()> {
        self.publisher.close_with_timeout(timeout)
    }
}
