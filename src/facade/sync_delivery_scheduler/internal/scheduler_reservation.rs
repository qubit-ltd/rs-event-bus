// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Reservation of global admission and one scheduler queue position.

use std::sync::Arc;

use qubit_id::Id;

use super::super::OrderingLaneKey;
use super::super::SyncDeliveryScheduler;
use super::scheduled_job::ScheduledJob;
use crate::pipeline::AdmissionPermit;

/// Reserves both global admission and one bounded queue slot before a job is
/// built.
pub(in crate::facade) struct SchedulerReservation {
    /// Dispatcher whose queue capacity was reserved.
    pub(in crate::facade::sync_delivery_scheduler) scheduler: Arc<SyncDeliveryScheduler>,
    /// Subscription coordinator that owns the future job.
    pub(in crate::facade::sync_delivery_scheduler) subscription_id: Id,
    /// Optional lane reserved for this job.
    pub(in crate::facade::sync_delivery_scheduler) ordering_key: Option<OrderingLaneKey>,
    /// Global admission permit retained until job completion.
    pub(in crate::facade::sync_delivery_scheduler) permit: Option<AdmissionPermit>,
    /// Whether the reservation has been transferred into dispatcher state.
    pub(in crate::facade::sync_delivery_scheduler) committed: bool,
}

impl SchedulerReservation {
    /// Commits a reserved delivery to the shared dispatcher.
    ///
    /// # Type Parameters
    /// - `F`: one-shot handler closure type.
    ///
    /// # Parameters
    /// - `run`: closure invoked with `true` if cancellation prevents it from
    ///   starting.
    pub(in crate::facade) fn submit<F>(mut self, run: F)
    where
        F: FnOnce(bool) + Send + 'static,
    {
        let mut state = self
            .scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reserved_queue = state.reserved_queue.saturating_sub(1);
        if state.stopping_immediate || state.cancelled_subscriptions.contains(&self.subscription_id) {
            state.cancelled_jobs.push_back(ScheduledJob {
                subscription_id: self.subscription_id,
                ordering_key: None,
                run: Box::new(run),
                permit: self.permit.take(),
            });
            self.committed = true;
            self.scheduler.changed.notify_all();
            return;
        }
        let job = ScheduledJob {
            subscription_id: self.subscription_id,
            ordering_key: self.ordering_key.clone(),
            run: Box::new(run),
            permit: self.permit.take(),
        };
        let subscription_id = job.subscription_id;
        let queue_is_empty = state
            .queues
            .get(&subscription_id)
            .is_none_or(std::collections::VecDeque::is_empty);
        if queue_is_empty {
            state.round_robin.push_back(job.subscription_id);
        }
        state.queues.entry(subscription_id).or_default().push_back(job);
        state.queued += 1;
        self.committed = true;
        self.scheduler.changed.notify_all();
    }
}

impl Drop for SchedulerReservation {
    /// Releases the queue slot when it was never committed to dispatcher state.
    fn drop(&mut self) {
        if !self.committed {
            let mut state = self
                .scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.reserved_queue = state.reserved_queue.saturating_sub(1);
            self.scheduler.changed.notify_all();
        }
    }
}
