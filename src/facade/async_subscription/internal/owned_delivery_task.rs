// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Handler tasks that return delivery ownership to the session owner.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::PendingDelivery;

/// A handler/retry task that does not own the provider receiver.
pub(in crate::facade) struct OwnedDeliveryTask<T: 'static> {
    /// Produces the same pending delivery after handler work completes.
    pub(in crate::facade) future: Pin<Box<dyn Future<Output = PendingDelivery<T>> + Send>>,
    /// Marks whether user code started before an immediate stop.
    pub(in crate::facade) started: Arc<AtomicBool>,
}

/// Drops queued handlers that have not started, counting ephemeral deliveries.
///
/// # Type Parameters
/// - `T`: Payload retained by each delivery task.
///
/// # Parameters
///
/// - `tasks`: Queued handler tasks to filter in place.
/// - `abandoned`: Counter incremented for discarded ephemeral deliveries.
/// - `ephemeral`: Whether discarded deliveries should contribute to the
///   counter.
///
/// # Side Effects
///
/// Removes unstarted tasks from `tasks` and may increment `abandoned`.
pub(in crate::facade) fn discard_unstarted_tasks<T: 'static>(
    tasks: &mut Vec<OwnedDeliveryTask<T>>,
    abandoned: &AtomicU64,
    ephemeral: bool,
) {
    let unstarted = tasks
        .iter()
        .filter(|task| !task.started.load(Ordering::Acquire))
        .count() as u64;
    tasks.retain(|task| task.started.load(Ordering::Acquire));
    if ephemeral && unstarted > 0 {
        abandoned.fetch_add(unstarted, Ordering::AcqRel);
    }
}
