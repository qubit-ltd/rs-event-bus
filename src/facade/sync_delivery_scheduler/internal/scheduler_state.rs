// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable queues, fairness, and shutdown state for the dispatcher.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use qubit_id::Id;

use super::super::OrderingLaneKey;
use super::scheduled_job::ScheduledJob;

/// Queue, fair-scheduling, and lifecycle state protected by the dispatcher
/// lock.
#[derive(Default)]
pub(in crate::facade::sync_delivery_scheduler) struct SchedulerState {
    /// Pending jobs indexed by subscription.
    pub(in crate::facade::sync_delivery_scheduler) queues: HashMap<Id, VecDeque<ScheduledJob>>,
    /// Jobs canceled before handler execution and awaiting a worker.
    pub(in crate::facade::sync_delivery_scheduler) cancelled_jobs: VecDeque<ScheduledJob>,
    /// Subscription IDs ordered by round-robin queue access.
    pub(in crate::facade::sync_delivery_scheduler) round_robin: VecDeque<Id>,
    /// Per-key lanes currently owned by active handler jobs.
    pub(in crate::facade::sync_delivery_scheduler) active_keys: HashSet<OrderingLaneKey>,
    /// Subscriptions that no longer admit work.
    pub(in crate::facade::sync_delivery_scheduler) cancelled_subscriptions: HashSet<Id>,
    /// Number of committed jobs waiting for workers.
    pub(in crate::facade::sync_delivery_scheduler) queued: usize,
    /// Number of queue positions reserved but not yet committed.
    pub(in crate::facade::sync_delivery_scheduler) reserved_queue: usize,
    /// Number of workers waiting for an eligible job.
    pub(in crate::facade::sync_delivery_scheduler) idle_workers: usize,
    /// Whether the fixed worker set has been started.
    pub(in crate::facade::sync_delivery_scheduler) started: bool,
    /// Whether new reservations are accepted.
    pub(in crate::facade::sync_delivery_scheduler) accepting: bool,
    /// Whether queued jobs should be returned instead of started.
    pub(in crate::facade::sync_delivery_scheduler) stopping_immediate: bool,
    /// Whether all workers have exited.
    pub(in crate::facade::sync_delivery_scheduler) stopped: bool,
}
