// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded receive demand and lane queues of one registration.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use super::owned_delivery_phase::OwnedDeliveryPhase;
use super::owned_delivery_record::OwnedDeliveryRecord;
use crate::pipeline::OrderingLaneKey;

/// Per-subscription scheduling metadata bounded by its owned limit.
#[derive(Default)]
pub(super) struct SubscriptionScheduleState {
    /// Credits reserved, queued, running, or settling for this subscription.
    pub(super) owned: usize,
    /// Whether a runner currently permits new handler or owner-settlement lane
    /// grants.
    pub(super) dispatch_active: bool,
    /// Permanent stop fence for this registration.
    pub(super) stopped: bool,
    /// At most one outstanding receive demand in the global waiter rotation.
    pub(super) receive_waiting: bool,
    /// The sole reserved receive lease, including while the adapter holds it.
    pub(super) receive_reservation: Option<u64>,
    /// Whether the adapter already claimed the reserved receive lease.
    pub(super) receive_taken: bool,
    /// FIFO deliveries for each lane, with lanes in round-robin order.
    pub(super) lanes: VecDeque<(Option<OrderingLaneKey>, VecDeque<u64>)>,
    /// Ordered lanes held by running or settling deliveries.
    pub(super) locked_lanes: HashSet<OrderingLaneKey>,
}

impl SubscriptionScheduleState {
    /// Returns the next unlocked lane index; inactive owners cannot dispatch.
    ///
    /// # Parameters
    /// - `owned`: live lease metadata used to identify each FIFO head's grant
    ///   kind.
    /// - `handler_available`: whether a handler head can consume execution
    ///   capacity.
    ///
    /// # Returns
    /// `Some(index)` identifies the first unlocked lane whose head can consume
    /// a grant. `None` means stopped, inactive, empty, locked, or only
    /// handler heads without capacity.
    #[must_use]
    pub(super) fn eligible_lane(
        &self,
        owned: &HashMap<u64, OwnedDeliveryRecord>,
        handler_available: bool,
    ) -> Option<usize> {
        if self.stopped || !self.dispatch_active {
            return None;
        }
        self.lanes.iter().position(|(lane, queue)| {
            lane.as_ref().is_none_or(|key| !self.locked_lanes.contains(key))
                && queue.front().and_then(|lease| owned.get(lease)).is_some_and(|record| {
                    record.phase == OwnedDeliveryPhase::QueuedSettlement
                        || (handler_available && record.phase == OwnedDeliveryPhase::Queued)
                })
        })
    }

    /// Adds a lease to an ordered FIFO or to a fresh independent lane.
    ///
    /// # Parameters
    /// - `lease`: received lease to append.
    /// - `lane`: ordering identity to share, or `None` to create an independent
    ///   lane.
    pub(super) fn enqueue(&mut self, lease: u64, lane: Option<OrderingLaneKey>) {
        if lane.is_some()
            && let Some((_, queue)) = self.lanes.iter_mut().find(|(key, _)| *key == lane)
        {
            queue.push_back(lease);
            return;
        }
        self.lanes.push_back((lane, VecDeque::from([lease])));
    }

    /// Consumes one unlocked FIFO head and rotates the remaining lane.
    ///
    /// # Parameters
    /// - `index`: eligible lane index selected under the same scheduler state
    ///   lock.
    ///
    /// # Returns
    /// `Some(lease)` is the selected FIFO head; its ordered lane is locked.
    /// `None` means the supplied lane index is absent or empty.
    pub(super) fn take_ready(&mut self, index: usize) -> Option<u64> {
        let (lane, mut queue) = self.lanes.remove(index)?;
        let lease = queue.pop_front()?;
        if let Some(key) = lane.as_ref() {
            self.locked_lanes.insert(key.clone());
        }
        if !queue.is_empty() {
            self.lanes.push_back((lane, queue));
        }
        Some(lease)
    }

    /// Removes a cancelled queued lease and immediately reclaims empty lanes.
    ///
    /// # Parameters
    /// - `lease`: queued lease to remove; absent IDs leave the lane queues
    ///   unchanged.
    pub(super) fn remove_queued(&mut self, lease: u64) {
        self.lanes.retain_mut(|(_, queue)| {
            queue.retain(|candidate| *candidate != lease);
            !queue.is_empty()
        });
    }
}
