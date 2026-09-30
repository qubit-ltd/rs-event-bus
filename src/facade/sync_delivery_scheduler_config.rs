// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Capacity and worker limits for synchronous facade delivery scheduling.

use std::num::NonZeroUsize;

use crate::error::ConfigurationError;

/// Bounds synchronous handler scheduling for one event-bus facade.
///
/// `max_in_flight` includes active and queued deliveries. A queue capacity of
/// zero permits a delivery only when an eligible worker can accept it directly.
/// Subscription receive threads have a separate positive limit.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use qubit_event_bus::facade::SyncDeliverySchedulerConfig;
///
/// let config = SyncDeliverySchedulerConfig::new(8, 4)
///     .unwrap()
///     .with_max_subscription_workers(NonZeroUsize::new(16).unwrap());
/// assert_eq!(config.max_in_flight(), 8);
/// assert_eq!(config.handler_queue_capacity(), 4);
/// assert_eq!(config.max_subscription_workers().get(), 16);
/// ```
#[must_use = "scheduler limits must be applied to an event bus"]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncDeliverySchedulerConfig {
    /// Maximum number of queued and active deliveries.
    max_in_flight: usize,
    /// Maximum number of deliveries waiting for an idle handler worker.
    handler_queue_capacity: usize,
    /// Maximum number of active or starting subscription receive threads.
    max_subscription_workers: NonZeroUsize,
}

impl SyncDeliverySchedulerConfig {
    /// Creates scheduler limits; `max_in_flight` must be greater than zero.
    /// A zero queue capacity permits direct handoff only to an idle worker.
    ///
    /// # Parameters
    /// - `max_in_flight`: positive bound for active and queued deliveries.
    /// - `handler_queue_capacity`: bound for tasks waiting on a handler worker;
    ///   zero allows direct handoff only.
    ///
    /// # Returns
    /// A scheduler policy using the default subscription-worker limit.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] for `max_in_flight` when
    /// its value is zero.
    pub fn new(max_in_flight: usize, handler_queue_capacity: usize) -> Result<Self, ConfigurationError> {
        if max_in_flight == 0 {
            return Err(ConfigurationError::InvalidField {
                field: "max_in_flight",
                message: "must be greater than zero".into(),
            });
        }
        Ok(Self {
            max_in_flight,
            handler_queue_capacity,
            max_subscription_workers: NonZeroUsize::new(256).expect("positive default worker limit"),
        })
    }

    /// Returns the maximum number of active or starting subscription receive
    /// threads.
    ///
    /// # Returns
    /// The positive worker-thread limit.
    #[must_use]
    #[inline]
    pub const fn max_subscription_workers(self) -> NonZeroUsize {
        self.max_subscription_workers
    }

    /// Returns the maximum number of admitted deliveries, including queued
    /// work.
    ///
    /// # Returns
    /// The combined bound for queued and active delivery tasks.
    #[must_use]
    #[inline]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Returns the maximum number of admitted tasks waiting for a handler
    /// worker. Zero allows only immediate handoff to an idle eligible worker.
    ///
    /// # Returns
    /// The queue capacity, which may be zero.
    #[must_use]
    #[inline]
    pub fn handler_queue_capacity(&self) -> usize {
        self.handler_queue_capacity
    }

    /// Sets the maximum number of active or starting subscription receive
    /// threads.
    ///
    /// # Parameters
    /// - `limit`: positive receive-thread limit.
    ///
    /// # Returns
    /// This scheduler policy with the new subscription-worker bound.
    #[inline]
    pub const fn with_max_subscription_workers(mut self, limit: NonZeroUsize) -> Self {
        self.max_subscription_workers = limit;
        self
    }
}

impl Default for SyncDeliverySchedulerConfig {
    /// Creates defaults for four in-flight deliveries, a 32-task handler queue,
    /// and 256 subscription receive threads.
    fn default() -> Self {
        Self {
            max_in_flight: 4,
            handler_queue_capacity: 32,
            max_subscription_workers: NonZeroUsize::new(256).expect("positive default worker limit"),
        }
    }
}
