// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Reservation of global admission and one scheduler queue position.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::PoisonError;

use qubit_id::Id;

use super::super::OrderingLaneKey;
use super::super::SyncDeliveryScheduler;
use super::scheduled_job::ScheduledJob;
use crate::pipeline::AdmissionPermit;

/// Holds global admission and a bounded scheduler queue slot for a future job.
///
/// A reservation starts uncommitted. Submitting it transfers the admission
/// permit and queue position into dispatcher state; dropping it first returns
/// the reserved queue capacity. The permit then remains with the scheduled job
/// until its handler or cancellation callback finishes.
pub(in crate::facade) struct SchedulerReservation {
    /// Shared dispatcher whose queue capacity and state this reservation uses.
    pub(in crate::facade::sync_delivery_scheduler) scheduler: Arc<SyncDeliveryScheduler>,
    /// Subscription that owns the job and scopes subscription cancellation.
    pub(in crate::facade::sync_delivery_scheduler) subscription_id: Id,
    /// Optional ordering lane that serializes this job with matching
    /// deliveries.
    pub(in crate::facade::sync_delivery_scheduler) ordering_key: Option<OrderingLaneKey>,
    /// Global queued-or-active admission permit transferred to the scheduled
    /// job.
    pub(in crate::facade::sync_delivery_scheduler) permit: Option<AdmissionPermit>,
    /// Prevents `Drop` from releasing queue capacity after submission transfers
    /// it.
    pub(in crate::facade::sync_delivery_scheduler) committed: bool,
}

impl SchedulerReservation {
    /// Transfers this reservation and delivery callback into the dispatcher.
    ///
    /// The reservation's queue slot is committed under the dispatcher lock. A
    /// live subscription adds the job to its fair queue; a canceled
    /// subscription or immediate stop instead schedules its cancellation
    /// callback. In either case the callback runs on a dispatcher worker
    /// and its admission permit remains held until the callback returns.
    ///
    /// # Type Parameters
    /// - `F`: one-shot handler closure type.
    ///
    /// # Parameters
    /// - `run`: one-shot delivery callback; it receives `true` if cancellation
    ///   prevents delivery from starting and `false` when the handler may run.
    pub(in crate::facade) fn submit<F>(mut self, run: F)
    where
        F: FnOnce(bool) + Send + 'static,
    {
        let mut state = self.scheduler.state.lock().unwrap_or_else(PoisonError::into_inner);
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
        let queue_is_empty = state.queues.get(&subscription_id).is_none_or(VecDeque::is_empty);
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
            let mut state = self.scheduler.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.reserved_queue = state.reserved_queue.saturating_sub(1);
            self.scheduler.changed.notify_all();
        }
    }
}
