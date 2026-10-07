// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure scheduler state accessed under the core's single transition mutex.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use qubit_id::Id;

use super::delivery_snapshot_input::DeliverySnapshotInput;
use super::owned_delivery_phase::OwnedDeliveryPhase;
use super::owned_delivery_record::OwnedDeliveryRecord;
use super::subscription_schedule_state::SubscriptionScheduleState;
use crate::facade::DeliveryMetricsSnapshot;
use crate::facade::DeliverySchedulingConfig;

/// Globally bounded lease metadata and the two subscription rotations.
pub(super) struct DeliverySchedulerState {
    /// Validated capacity bounds.
    pub(super) config: DeliverySchedulingConfig,
    /// All credits, including unclaimed receive reservations.
    pub(super) owned: HashMap<u64, OwnedDeliveryRecord>,
    /// Number of leases in the Running phase.
    pub(super) running: usize,
    /// Live registrations, including stopped owners still draining.
    pub(super) subscriptions: HashMap<Id, SubscriptionScheduleState>,
    /// Eligible subscriptions in dispatch round-robin order.
    pub(super) ready: VecDeque<Id>,
    /// Pending receive demands in fair admission order.
    pub(super) receive_waiters: VecDeque<Id>,
    /// Coalesced wake targets; never more than registered subscriptions.
    pub(super) notifications: HashSet<Id>,
    /// Next never-reused lease identity; None denotes permanent exhaustion.
    pub(super) next_lease: Option<u64>,
}

impl DeliverySchedulerState {
    /// Initializes empty state; allocations grow only with admitted work.
    ///
    /// # Parameters
    /// - `config`: validated bounds for live scheduling metadata.
    ///
    /// # Returns
    /// Empty scheduling state with the first lease identity ready for
    /// allocation.
    #[must_use]
    pub(super) fn new(config: DeliverySchedulingConfig) -> Self {
        Self {
            config,
            owned: HashMap::new(),
            running: 0,
            subscriptions: HashMap::new(),
            ready: VecDeque::new(),
            receive_waiters: VecDeque::new(),
            notifications: HashSet::new(),
            next_lease: Some(1),
        }
    }

    /// Captures current gauges and lease starts without calling a clock.
    ///
    /// # Parameters
    /// - `subscription_id`: one registration to select, or `None` for all
    ///   registrations.
    ///
    /// # Returns
    /// Gauges and at most one timestamp per selected owned record from this
    /// locked state. The caller must release the state lock before sampling
    /// the instant used for age.
    #[must_use]
    pub(super) fn snapshot_input(&self, subscription_id: Option<Id>) -> DeliverySnapshotInput {
        let gauges = self.snapshot_gauges(subscription_id);
        let owned_starts = self
            .owned
            .values()
            .filter(|record| subscription_id.is_none_or(|id| record.subscription_id == id))
            .filter_map(|record| record.created_at)
            .collect();
        DeliverySnapshotInput::new(gauges, owned_starts)
    }

    /// Scans live phase and ordered-lane metadata without reading or comparing
    /// time.
    ///
    /// # Parameters
    /// - `subscription_id`: one registration to select, or `None` for all
    ///   registrations.
    ///
    /// # Returns
    /// Accurate live gauges with zero cumulative counters and no owned age.
    #[must_use = "scheduler gauges are the current delivery snapshot"]
    #[inline]
    pub(super) fn snapshot_gauges(&self, subscription_id: Option<Id>) -> DeliveryMetricsSnapshot {
        let mut snapshot = DeliveryMetricsSnapshot::default();
        for record in self.owned.values() {
            if subscription_id.is_some_and(|id| record.subscription_id != id) {
                continue;
            }
            let gauge = match record.phase {
                OwnedDeliveryPhase::ReservedReceive => &mut snapshot.reserved_receives,
                OwnedDeliveryPhase::Queued
                | OwnedDeliveryPhase::QueuedSettlement
                | OwnedDeliveryPhase::WaitingRetry => &mut snapshot.queued,
                OwnedDeliveryPhase::Running => &mut snapshot.running_handlers,
                OwnedDeliveryPhase::Settling => &mut snapshot.settling,
            };
            *gauge = gauge.saturating_add(1);
        }
        for (id, sub) in &self.subscriptions {
            if subscription_id.is_some_and(|selected| selected != *id) {
                continue;
            }
            for (lane, queue) in &sub.lanes {
                let Some(lane) = lane else {
                    continue;
                };
                let waiting = if sub.locked_lanes.contains(lane) {
                    queue.len()
                } else {
                    queue.len().saturating_sub(1)
                };
                snapshot.lane_waiting = snapshot
                    .lane_waiting
                    .saturating_add(u64::try_from(waiting).unwrap_or(u64::MAX));
            }
        }
        snapshot
    }

    /// Selects an active subscription and FIFO lane without consuming either
    /// round-robin turn. Handler-only lanes are temporarily skipped when
    /// execution capacity is full.
    ///
    /// # Returns
    /// `Some((id, index))` identifies the next grantable subscription and lane.
    /// `None` means no unlocked lane can currently execute or begin owner
    /// settlement.
    #[must_use]
    pub(super) fn selected_ready(&self) -> Option<(Id, usize)> {
        let handler_available = self.running < self.config.max_running_handlers().get();
        self.ready.iter().find_map(|id| {
            self.subscriptions
                .get(id)?
                .eligible_lane(&self.owned, handler_available)
                .map(|index| (*id, index))
        })
    }

    /// Updates one subscription's membership without disturbing existing RR
    /// order.
    ///
    /// # Parameters
    /// - `id`: registration whose eligibility may have changed.
    #[inline]
    pub(super) fn refresh_ready(&mut self, id: Id) {
        let eligible = self
            .subscriptions
            .get(&id)
            .is_some_and(|sub| sub.eligible_lane(&self.owned, true).is_some());
        if eligible {
            if !self.ready.contains(&id) {
                self.ready.push_back(id);
            }
        } else {
            self.ready.retain(|candidate| *candidate != id);
        }
    }

    /// Coalesces a notification for the currently selected runner without
    /// reserving capacity.
    #[inline]
    pub(super) fn notify_ready(&mut self) {
        if let Some((id, _)) = self.selected_ready() {
            self.notifications.insert(id);
        }
    }

    /// Fairly grants available owned credits, bypassing subscriptions at their
    /// own cap.
    pub(super) fn grant_receives(&mut self) {
        let candidates = self.receive_waiters.len();
        for _ in 0..candidates {
            if self.owned.len() >= self.config.max_owned_deliveries().get() {
                break;
            }
            let Some(id) = self.receive_waiters.pop_front() else {
                break;
            };
            let Some(sub) = self.subscriptions.get_mut(&id) else {
                continue;
            };
            if sub.stopped || !sub.receive_waiting {
                continue;
            }
            if sub.owned >= self.config.max_owned_per_subscription().get() {
                self.receive_waiters.push_back(id);
                continue;
            }
            let Some(lease) = self.next_lease else {
                self.receive_waiters.push_front(id);
                self.notifications
                    .extend(self.receive_waiters.iter().copied());
                break;
            };
            self.next_lease = lease.checked_add(1);
            sub.receive_waiting = false;
            sub.receive_reservation = Some(lease);
            sub.receive_taken = false;
            sub.owned += 1;
            self.owned.insert(
                lease,
                OwnedDeliveryRecord {
                    subscription_id: id,
                    phase: OwnedDeliveryPhase::ReservedReceive,
                    created_at: None,
                    lane: None,
                },
            );
            self.notifications.insert(id);
        }
    }

    /// Releases existing metadata exactly once; callers then issue receive and
    /// dispatch wakes.
    ///
    /// # Parameters
    /// - `lease`: owned lease to release; an absent ID is ignored.
    pub(super) fn release(&mut self, lease: u64) {
        let Some(record) = self.owned.remove(&lease) else {
            return;
        };
        let id = record.subscription_id;
        let Some(sub) = self.subscriptions.get_mut(&id) else {
            return;
        };
        sub.owned -= 1;
        if sub.receive_reservation == Some(lease) {
            sub.receive_reservation = None;
            sub.receive_taken = false;
        }
        if matches!(
            record.phase,
            OwnedDeliveryPhase::Queued | OwnedDeliveryPhase::QueuedSettlement
        ) {
            sub.remove_queued(lease);
        }
        if matches!(
            record.phase,
            OwnedDeliveryPhase::Running
                | OwnedDeliveryPhase::Settling
                | OwnedDeliveryPhase::WaitingRetry
        ) && let Some(lane) = record.lane
        {
            sub.locked_lanes.remove(&lane);
        }
        if record.phase == OwnedDeliveryPhase::Running {
            self.running -= 1;
        }
        self.notifications.insert(id);
        self.refresh_ready(id);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroUsize;

    use qubit_id::Id;

    use super::DeliverySchedulerState;
    use crate::facade::DeliverySchedulingConfig;
    use crate::facade::internal::owned_delivery_phase::OwnedDeliveryPhase;
    use crate::facade::internal::owned_delivery_record::OwnedDeliveryRecord;
    use crate::facade::internal::subscription_schedule_state::SubscriptionScheduleState;

    /// Creates scheduler state with one queued, dispatchable delivery.
    fn state_with_queued_delivery(id: Id, lease: u64) -> DeliverySchedulerState {
        let one = NonZeroUsize::new(1).expect("positive limit");
        let config =
            DeliverySchedulingConfig::new(one, one, one, one).expect("valid scheduler config");
        let mut state = DeliverySchedulerState::new(config);
        state.subscriptions.insert(
            id,
            SubscriptionScheduleState {
                dispatch_active: true,
                ..SubscriptionScheduleState::default()
            },
        );
        state.owned.insert(
            lease,
            OwnedDeliveryRecord {
                subscription_id: id,
                phase: OwnedDeliveryPhase::Queued,
                created_at: None,
                lane: None,
            },
        );
        state
            .subscriptions
            .get_mut(&id)
            .expect("subscription was registered")
            .lanes
            .push_back((None, VecDeque::from([lease])));
        state
    }

    #[test]
    fn test_refresh_ready_preserves_order_and_removes_ineligible_owners() {
        let first = Id::new(1);
        let second = Id::new(2);
        let mut state = state_with_queued_delivery(first, 11);
        state.subscriptions.insert(
            second,
            SubscriptionScheduleState {
                dispatch_active: true,
                ..SubscriptionScheduleState::default()
            },
        );
        state.owned.insert(
            22,
            OwnedDeliveryRecord {
                subscription_id: second,
                phase: OwnedDeliveryPhase::Queued,
                created_at: None,
                lane: None,
            },
        );
        state
            .subscriptions
            .get_mut(&second)
            .expect("subscription was registered")
            .lanes
            .push_back((None, VecDeque::from([22])));

        state.refresh_ready(first);
        state.refresh_ready(second);
        state.refresh_ready(first);
        assert_eq!(state.ready, VecDeque::from([first, second]));

        state
            .subscriptions
            .get_mut(&first)
            .expect("owner exists")
            .stopped = true;
        state.refresh_ready(first);
        assert_eq!(state.ready, VecDeque::from([second]));
    }

    #[test]
    fn test_notify_ready_coalesces_selected_owner_without_reserving_work() {
        let first = Id::new(1);
        let second = Id::new(2);
        let mut state = state_with_queued_delivery(first, 11);
        state.subscriptions.insert(
            second,
            SubscriptionScheduleState {
                dispatch_active: true,
                ..SubscriptionScheduleState::default()
            },
        );
        state.owned.insert(
            22,
            OwnedDeliveryRecord {
                subscription_id: second,
                phase: OwnedDeliveryPhase::Queued,
                created_at: None,
                lane: None,
            },
        );
        state
            .subscriptions
            .get_mut(&second)
            .expect("subscription was registered")
            .lanes
            .push_back((None, VecDeque::from([22])));
        state.refresh_ready(first);
        state.refresh_ready(second);

        state.notify_ready();
        state.notify_ready();

        assert_eq!(state.notifications.len(), 1);
        assert!(state.notifications.contains(&first));
        assert_eq!(state.ready, VecDeque::from([first, second]));
        assert_eq!(state.owned.len(), 2);
    }
}
