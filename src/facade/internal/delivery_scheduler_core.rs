// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded metadata-only delivery admission and dispatch.

use std::sync::PoisonError;

use qubit_clock::MonotonicInstant;
use qubit_id::Id;

use super::delivery_scheduler_state::DeliverySchedulerState;
use super::delivery_snapshot_input::DeliverySnapshotInput;
use super::owned_delivery_phase::OwnedDeliveryPhase;
use super::subscription_schedule_state::SubscriptionScheduleState;
use crate::facade::DeliveryMetricsSnapshot;
use crate::facade::DeliverySchedulingConfig;
use crate::internal::sync::Mutex;
use crate::internal::sync::MutexGuard;
use crate::pipeline::OrderingLaneKey;

/// Serializes pure scheduling transitions; adapters execute work and route
/// wakes after unlocking. All methods briefly acquire the single metadata
/// mutex; none invokes a clock or user callback.
pub(in crate::facade) struct DeliverySchedulerCore {
    /// Sole lock protecting every ownership, admission, lane, and running
    /// transition.
    state: Mutex<DeliverySchedulerState>,
}

impl DeliverySchedulerCore {
    /// Creates an empty scheduler with the supplied independent capacity
    /// limits.
    ///
    /// # Parameters
    /// - `config`: independent positive subscription, ownership, and handler
    ///   capacity bounds.
    ///
    /// # Returns
    /// An empty scheduler whose state is shared through one mutex.
    #[must_use]
    pub(in crate::facade) fn new(config: DeliverySchedulingConfig) -> Self {
        Self {
            state: Mutex::new(DeliverySchedulerState::new(config)),
        }
    }

    /// Reports permanent lease-ID exhaustion so adapters can terminate with an
    /// internal failure.
    ///
    /// # Returns
    /// `true` when no never-used lease identifier remains; otherwise `false`.
    #[must_use]
    pub(in crate::facade) fn lease_ids_exhausted(&self) -> bool {
        self.lock().next_lease.is_none()
    }

    /// Captures current gauges and bounded lease starts before the owner
    /// samples its clock.
    ///
    /// # Parameters
    /// - `subscription_id`: one registration to select, or `None` for all live
    ///   registrations.
    ///
    /// # Returns
    /// Metadata captured under one state lock. Only after this method returns
    /// should the owner sample its clock and call
    /// `DeliverySnapshotInput::at` with that instant.
    #[must_use]
    pub(in crate::facade) fn snapshot_input(
        &self,
        subscription_id: Option<Id>,
    ) -> DeliverySnapshotInput {
        self.lock().snapshot_input(subscription_id)
    }

    /// Reads accurate current phase and lane gauges without clock arithmetic.
    ///
    /// # Parameters
    /// - `subscription_id`: one registration to select, or `None` for all live
    ///   registrations.
    ///
    /// # Returns
    /// Current phase/lane gauges with no owned age and zero cumulative
    /// counters. This remains usable when a clock error prevents computing
    /// an age.
    #[must_use = "scheduler gauges are the current delivery snapshot"]
    #[inline]
    pub(in crate::facade) fn snapshot_gauges(
        &self,
        subscription_id: Option<Id>,
    ) -> DeliveryMetricsSnapshot {
        self.lock().snapshot_gauges(subscription_id)
    }

    /// Registers an initially inactive subscription; false rejects duplicates
    /// or capacity overflow.
    ///
    /// # Parameters
    /// - `subscription_id`: identity of the receiver owner to register.
    ///
    /// # Returns
    /// `true` when registration succeeds; `false` for an existing identity or a
    /// full registry.
    #[must_use]
    pub(in crate::facade) fn register(&self, subscription_id: Id) -> bool {
        let mut state = self.lock();
        if state.subscriptions.contains_key(&subscription_id)
            || state.subscriptions.len() >= state.config.max_subscriptions().get()
        {
            return false;
        }
        state
            .subscriptions
            .insert(subscription_id, SubscriptionScheduleState::default());
        true
    }

    /// Enables or pauses new lane grants; existing running leases retain their
    /// slots.
    ///
    /// # Parameters
    /// - `subscription_id`: registration whose runner availability changes.
    /// - `active`: whether new handler or settlement grants may be consumed;
    ///   stopped registrations remain inactive.
    pub(in crate::facade) fn set_dispatch_active(&self, subscription_id: Id, active: bool) {
        let mut state = self.lock();
        if let Some(sub) = state.subscriptions.get_mut(&subscription_id) {
            sub.dispatch_active = active && !sub.stopped;
        }
        state.refresh_ready(subscription_id);
        state.notify_ready();
    }

    /// Registers at most one receive demand; stopped or unregistered owners are
    /// ignored.
    ///
    /// # Parameters
    /// - `subscription_id`: registration requesting its next receive credit.
    pub(in crate::facade) fn request_receive(&self, subscription_id: Id) {
        let mut state = self.lock();
        let Some(sub) = state.subscriptions.get_mut(&subscription_id) else {
            return;
        };
        if sub.stopped || sub.receive_waiting || sub.receive_reservation.is_some() {
            return;
        }
        sub.receive_waiting = true;
        state.receive_waiters.push_back(subscription_id);
        state.grant_receives();
    }

    /// Cancels pending receive demand and returns only unclaimed reservation
    /// credit. Claimed reservations remain owned until complete; the
    /// subscription can request again.
    ///
    /// # Parameters
    /// - `subscription_id`: registration whose pending or unclaimed receive
    ///   should be cancelled.
    pub(in crate::facade) fn cancel_receive(&self, subscription_id: Id) {
        let mut state = self.lock();
        let Some(sub) = state.subscriptions.get_mut(&subscription_id) else {
            return;
        };
        sub.receive_waiting = false;
        let unclaimed = if sub.receive_taken {
            None
        } else {
            sub.receive_reservation
        };
        state.receive_waiters.retain(|id| *id != subscription_id);
        if let Some(lease) = unclaimed {
            state.release(lease);
        }
        state.grant_receives();
        state.notify_ready();
    }

    /// Claims the sole receive lease once; None means waiting, stopped, absent,
    /// or exhausted. An adapter must complete this lease on timeout,
    /// receive failure, or cancellation.
    ///
    /// # Parameters
    /// - `subscription_id`: registration claiming its granted receive credit.
    ///
    /// # Returns
    /// `Some(lease)` transfers responsibility for returning that owned credit
    /// to the adapter. `None` means absent, stopped, already claimed,
    /// waiting for capacity, or ID exhaustion.
    pub(in crate::facade) fn take_receive_reservation(&self, subscription_id: Id) -> Option<u64> {
        let mut state = self.lock();
        let sub = state.subscriptions.get_mut(&subscription_id)?;
        if sub.stopped || sub.receive_taken {
            return None;
        }
        let lease = sub.receive_reservation?;
        sub.receive_taken = true;
        Some(lease)
    }

    /// Converts a receive credit to an ordered FIFO entry or a fresh
    /// independent lane. Unknown, already queued, and stopped leases are
    /// ignored; stopped owners must complete them.
    ///
    /// # Parameters
    /// - `lease_id`: claimed receive lease now holding a successfully received
    ///   delivery.
    /// - `lane`: shared ordering identity, or `None` for an independent
    ///   delivery lane.
    pub(in crate::facade) fn enqueue(&self, lease_id: u64, lane: Option<OrderingLaneKey>) {
        self.enqueue_phase(lease_id, lane, OwnedDeliveryPhase::Queued);
    }

    /// Queues a claimed receive lease to await its FIFO ordering lane without a
    /// handler. The adapter must retain the disposition and wait for
    /// `take_settlement_ready` before SPI calls. Unknown, unclaimed,
    /// stopped, or already queued leases are unchanged.
    ///
    /// # Parameters
    /// - `lease_id`: claimed receive lease whose disposition is owned by the
    ///   adapter.
    /// - `lane`: shared ordering identity, or `None` for an independent
    ///   delivery lane.
    pub(in crate::facade) fn enqueue_settlement(
        &self,
        lease_id: u64,
        lane: Option<OrderingLaneKey>,
    ) {
        self.enqueue_phase(lease_id, lane, OwnedDeliveryPhase::QueuedSettlement);
    }

    /// Takes a settlement-only lane grant without consuming handler capacity.
    ///
    /// # Parameters
    /// - `subscription_id`: active owner attempting to consume its selected
    ///   lane turn.
    ///
    /// # Returns
    /// `Some(lease)` grants permission to begin owner settlement while holding
    /// its lane. `None` means stopped, paused, no eligible lane, another
    /// subscription, or a handler turn.
    pub(in crate::facade) fn take_settlement_ready(&self, subscription_id: Id) -> Option<u64> {
        self.take_grant(subscription_id, OwnedDeliveryPhase::QueuedSettlement)
    }

    /// Atomically consumes the selected active subscription's next unlocked
    /// FIFO head. None means no capacity, no eligible lane, or another
    /// subscription or grant kind's turn. Selection alone never occupies a
    /// handler slot; adapters must route all notifications. Owners must
    /// poll both grant kinds so a selected settlement turn can make progress.
    ///
    /// # Parameters
    /// - `subscription_id`: active runner attempting to consume its round-robin
    ///   turn.
    ///
    /// # Returns
    /// `Some(lease)` is the FIFO delivery that atomically acquired a running
    /// slot and lane. `None` means no slot, no eligible lane, an
    /// inactive/absent registration, or another turn.
    pub(in crate::facade) fn take_ready(&self, subscription_id: Id) -> Option<u64> {
        self.take_grant(subscription_id, OwnedDeliveryPhase::Queued)
    }

    /// Releases only the handler slot once; ownership and the ordering lane
    /// last until complete.
    ///
    /// # Parameters
    /// - `lease_id`: running lease whose handler has returned; other phases are
    ///   unchanged.
    pub(in crate::facade) fn handler_finished(&self, lease_id: u64) {
        let mut state = self.lock();
        let Some(record) = state.owned.get_mut(&lease_id) else {
            return;
        };
        if record.phase != OwnedDeliveryPhase::Running {
            return;
        }
        record.phase = OwnedDeliveryPhase::Settling;
        state.running -= 1;
        state.notify_ready();
    }

    /// Marks `lease_id` as waiting after actual job exit released H. Duplicate
    /// notifications are ignored; ownership and the ordering lane stay held.
    pub(in crate::facade) fn finish_attempt_waiting(&self, lease_id: u64) {
        let mut state = self.lock();
        if let Some(record) = state.owned.get_mut(&lease_id)
            && record.phase == OwnedDeliveryPhase::Settling
        {
            record.phase = OwnedDeliveryPhase::WaitingRetry;
        }
    }

    /// Requeues a due retry at its lane's head under the same transition lock.
    /// `lease_id` must be waiting; duplicates and stopped subscriptions do
    /// nothing. No new owned credit is acquired and successors cannot overtake.
    pub(in crate::facade) fn wake_retry(&self, lease_id: u64) {
        let mut state = self.lock();
        let Some(record) = state.owned.get(&lease_id) else {
            return;
        };
        if record.phase != OwnedDeliveryPhase::WaitingRetry {
            return;
        }
        let id = record.subscription_id;
        let lane = record.lane.clone();
        let Some(sub) = state.subscriptions.get_mut(&id) else {
            return;
        };
        if sub.stopped {
            return;
        }
        if let Some(key) = &lane {
            sub.locked_lanes.remove(key);
        }
        if lane.is_some()
            && let Some((_, queue)) = sub.lanes.iter_mut().find(|(key, _)| *key == lane)
        {
            queue.push_front(lease_id);
        } else {
            sub.lanes
                .push_back((lane, std::collections::VecDeque::from([lease_id])));
        }
        state
            .owned
            .get_mut(&lease_id)
            .expect("waiting lease remains owned")
            .phase = OwnedDeliveryPhase::Queued;
        state.refresh_ready(id);
        state.notify_ready();
    }

    /// Releases a lease, owned credit, and any running slot or lane exactly
    /// once.
    ///
    /// # Parameters
    /// - `lease_id`: lease whose receive or complete delivery lifecycle has
    ///   ended; unknown IDs are ignored.
    pub(in crate::facade) fn complete(&self, lease_id: u64) {
        let mut state = self.lock();
        state.release(lease_id);
        state.grant_receives();
        state.notify_ready();
    }

    /// Fences new receive/dispatch grants and returns unclaimed receive credit.
    /// Claimed or received work retains ownership until the adapter calls
    /// complete.
    ///
    /// # Parameters
    /// - `subscription_id`: registration to permanently fence while its adapter
    ///   drains owned work.
    pub(in crate::facade) fn stop_subscription(&self, subscription_id: Id) {
        let mut state = self.lock();
        let Some(sub) = state.subscriptions.get_mut(&subscription_id) else {
            return;
        };
        sub.stopped = true;
        sub.dispatch_active = false;
        sub.receive_waiting = false;
        let unclaimed = if sub.receive_taken {
            None
        } else {
            sub.receive_reservation
        };
        state.receive_waiters.retain(|id| *id != subscription_id);
        if let Some(lease) = unclaimed {
            state.release(lease);
        }
        state.refresh_ready(subscription_id);
        state.notifications.insert(subscription_id);
        state.grant_receives();
        state.notify_ready();
    }

    /// Removes only an empty registration; false means absent or still owning
    /// delivery credits.
    ///
    /// # Parameters
    /// - `subscription_id`: empty registration whose metadata should be
    ///   reclaimed.
    ///
    /// # Returns
    /// `true` when removed; `false` if the registration does not exist or still
    /// owns any lease.
    #[must_use]
    pub(in crate::facade) fn unregister(&self, subscription_id: Id) -> bool {
        let mut state = self.lock();
        if state
            .subscriptions
            .get(&subscription_id)
            .is_none_or(|sub| sub.owned != 0)
        {
            return false;
        }
        state.subscriptions.remove(&subscription_id);
        state.ready.retain(|id| *id != subscription_id);
        state.receive_waiters.retain(|id| *id != subscription_id);
        state.notifications.remove(&subscription_id);
        state.notify_ready();
        true
    }

    /// Drains coalesced wake targets, bounded by live registrations, for
    /// routing after unlocking.
    ///
    /// # Returns
    /// Distinct live subscription IDs accumulated since the previous drain. The
    /// adapter must route every returned ID after the state lock is
    /// released.
    #[must_use]
    pub(in crate::facade) fn take_notifications(&self) -> Vec<Id> {
        self.lock().notifications.drain().collect()
    }

    /// Records the first externally sampled start of a claimed receive lease.
    /// Unclaimed, unknown, already queued, and already timestamped leases are
    /// unchanged.
    ///
    /// # Parameters
    /// - `lease_id`: claimed receive reservation before handler or settlement
    ///   enqueue.
    /// - `created_at`: monotonic instant sampled by the owner outside the core
    ///   mutex.
    pub(in crate::facade) fn record_owned_start(
        &self,
        lease_id: u64,
        created_at: MonotonicInstant,
    ) {
        let mut state = self.lock();
        let Some(record) = state.owned.get(&lease_id) else {
            return;
        };
        if record.phase != OwnedDeliveryPhase::ReservedReceive || record.created_at.is_some() {
            return;
        }
        if !state
            .subscriptions
            .get(&record.subscription_id)
            .is_some_and(|sub| sub.receive_taken)
        {
            return;
        }
        if let Some(record) = state.owned.get_mut(&lease_id) {
            record.created_at = Some(created_at);
        }
    }

    /// Enqueues one claimed receive credit into the shared FIFO for its grant
    /// kind.
    ///
    /// # Parameters
    /// - `lease_id`: receive lease currently owned by the adapter.
    /// - `lane`: ordering identity, or `None` for an independent lane.
    /// - `phase`: handler-queued or settlement-queued state chosen by the
    ///   public-in-facade wrapper.
    fn enqueue_phase(
        &self,
        lease_id: u64,
        lane: Option<OrderingLaneKey>,
        phase: OwnedDeliveryPhase,
    ) {
        let mut state = self.lock();
        let Some(record) = state.owned.get(&lease_id) else {
            return;
        };
        if record.phase != OwnedDeliveryPhase::ReservedReceive {
            return;
        }
        let id = record.subscription_id;
        let Some(sub) = state.subscriptions.get_mut(&id) else {
            return;
        };
        if sub.stopped || !sub.receive_taken {
            return;
        }
        sub.receive_reservation = None;
        sub.receive_taken = false;
        sub.enqueue(lease_id, lane.clone());
        if let Some(record) = state.owned.get_mut(&lease_id) {
            record.phase = phase;
            record.lane = lane;
        }
        state.refresh_ready(id);
        state.notify_ready();
    }

    /// Consumes a selected lane only when its FIFO head matches the owner's
    /// requested grant kind.
    ///
    /// # Parameters
    /// - `subscription_id`: active owner attempting to consume the shared
    ///   round-robin turn.
    /// - `expected`: queued phase matching handler dispatch or settlement-only
    ///   dispatch.
    ///
    /// # Returns
    /// `Some(lease)` owns its lane and, for a handler grant, one execution
    /// slot. `None` leaves turns unconsumed when another owner or grant
    /// kind must make progress.
    fn take_grant(&self, subscription_id: Id, expected: OwnedDeliveryPhase) -> Option<u64> {
        let mut state = self.lock();
        let (selected, index) = state.selected_ready()?;
        if selected != subscription_id {
            state.notify_ready();
            return None;
        }
        let (_, queue) = state.subscriptions.get(&selected)?.lanes.get(index)?;
        let candidate = *queue.front()?;
        if state.owned.get(&candidate)?.phase != expected {
            state.notify_ready();
            return None;
        }
        let lease = state.subscriptions.get_mut(&selected)?.take_ready(index)?;
        state.ready.retain(|id| *id != selected);
        if expected == OwnedDeliveryPhase::Queued {
            state.owned.get_mut(&lease)?.phase = OwnedDeliveryPhase::Running;
            state.running += 1;
        } else {
            state.owned.get_mut(&lease)?.phase = OwnedDeliveryPhase::Settling;
        }
        state.refresh_ready(selected);
        state.notify_ready();
        Some(lease)
    }

    /// Locks metadata briefly, recovering poison because no user code runs
    /// under this lock.
    ///
    /// # Returns
    /// An exclusive metadata guard; acquiring it briefly blocks competing
    /// transitions.
    fn lock(&self) -> MutexGuard<'_, DeliverySchedulerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread::scope;
    use std::time::Duration;

    use qubit_clock::ClockDomain;
    use qubit_clock::MonotonicInstant;
    use qubit_clock::TimeError;
    use qubit_id::Id;

    use super::DeliverySchedulerCore;
    use crate::facade::DeliveryMetricsSnapshot;
    use crate::facade::DeliverySchedulingConfig;
    use crate::facade::internal::owned_delivery_phase::OwnedDeliveryPhase;
    use crate::pipeline::OrderingLaneKey;

    /// Creates a scheduler with explicit small limits for state transitions.
    fn scheduler(
        running: usize,
        owned: usize,
        per_sub: usize,
        subscriptions: usize,
    ) -> DeliverySchedulerCore {
        let positive = |value| NonZeroUsize::new(value).expect("positive test limit");
        DeliverySchedulerCore::new(
            DeliverySchedulingConfig::new(
                positive(running),
                positive(owned),
                positive(per_sub),
                positive(subscriptions),
            )
            .expect("valid test limits"),
        )
    }

    /// Receives one lease and associates its optional ordering lane.
    fn queue(core: &DeliverySchedulerCore, sub: Id, key: Option<&str>) -> u64 {
        core.request_receive(sub);
        let lease = core
            .take_receive_reservation(sub)
            .expect("receive credit available");
        core.enqueue(
            lease,
            key.map(|key| OrderingLaneKey::new("topic", Some(key), sub)),
        );
        lease
    }

    #[test]
    fn test_delivery_scheduler_retry_retains_credit_and_lane_and_releases_handler() {
        let core = scheduler(1, 3, 3, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let first = queue(&core, sub, Some("a"));
        let successor = queue(&core, sub, Some("a"));
        let other = queue(&core, sub, Some("b"));
        assert_eq!(core.take_ready(sub), Some(first));
        core.handler_finished(first);
        core.finish_attempt_waiting(first);
        core.finish_attempt_waiting(first);
        assert_eq!(core.snapshot_gauges(None).running_handlers, 0);
        assert_eq!(core.lock().owned.len(), 3);
        assert_eq!(core.take_ready(sub), Some(other));
        core.complete(other);
        assert_eq!(core.take_ready(sub), None);
        core.wake_retry(first);
        core.wake_retry(first);
        assert_eq!(core.take_ready(sub), Some(first));
        core.handler_finished(first);
        assert_eq!(core.take_ready(sub), None);
        core.complete(first);
        assert_eq!(core.take_ready(sub), Some(successor));
        core.complete(successor);
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_reservation_counts_as_owned_and_timeout_releases_once() {
        let core = scheduler(1, 1, 1, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(second));
        core.request_receive(first);
        let lease = core
            .take_receive_reservation(first)
            .expect("first receive granted");
        core.request_receive(first);
        core.request_receive(second);
        assert_eq!(core.take_receive_reservation(first), None);
        assert_eq!(core.take_receive_reservation(second), None);
        core.complete(lease);
        core.complete(lease);
        let next = core
            .take_receive_reservation(second)
            .expect("timeout returned owned credit");
        assert_ne!(lease, next);
        core.request_receive(first);
        assert_eq!(core.take_receive_reservation(first), None);
    }

    #[test]
    fn test_delivery_scheduler_settling_keeps_lane_but_frees_handler() {
        let core = scheduler(1, 3, 3, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let first = queue(&core, sub, Some("a"));
        let second = queue(&core, sub, Some("a"));
        let different = queue(&core, sub, Some("b"));
        assert_eq!(core.take_ready(sub), Some(first));
        core.handler_finished(first);
        assert_eq!(core.take_ready(sub), Some(different));
        core.handler_finished(different);
        assert_eq!(core.take_ready(sub), None);
        core.complete(first);
        assert_eq!(core.take_ready(sub), Some(second));
    }

    #[test]
    fn test_delivery_scheduler_paused_selection_does_not_occupy_handler_slot() {
        let core = scheduler(1, 2, 1, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(second));
        core.set_dispatch_active(first, true);
        core.set_dispatch_active(second, true);
        let first_lease = queue(&core, first, None);
        let second_lease = queue(&core, second, None);
        assert_eq!(core.take_ready(second), None);
        assert!(core.take_notifications().contains(&first));
        core.set_dispatch_active(first, false);
        assert_eq!(core.take_ready(second), Some(second_lease));
        core.set_dispatch_active(second, false);
        core.set_dispatch_active(first, true);
        assert_eq!(core.take_ready(first), None);
        core.handler_finished(second_lease);
        assert_eq!(core.take_ready(first), Some(first_lease));
    }
    #[test]
    fn test_delivery_scheduler_two_level_rotation_and_fifo() {
        let core = scheduler(1, 8, 6, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(second));
        core.set_dispatch_active(first, true);
        core.set_dispatch_active(second, true);
        let a1 = queue(&core, first, Some("a"));
        let a2 = queue(&core, first, Some("a"));
        let b1 = queue(&core, first, Some("b"));
        let b2 = queue(&core, first, Some("b"));
        let c1 = queue(&core, second, None);
        let c2 = queue(&core, second, None);
        for (sub, lease) in [
            (first, a1),
            (second, c1),
            (first, b1),
            (second, c2),
            (first, a2),
            (first, b2),
        ] {
            assert_eq!(core.take_ready(sub), Some(lease));
            core.complete(lease);
        }
        assert!(core.unregister(first));
        assert!(core.unregister(second));
    }

    #[test]
    fn test_delivery_scheduler_per_subscription_cap_leaves_global_credit() {
        let core = scheduler(2, 4, 2, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(second));
        queue(&core, first, None);
        queue(&core, first, None);
        core.request_receive(first);
        assert_eq!(core.take_receive_reservation(first), None);
        queue(&core, second, None);
        queue(&core, second, None);
    }

    #[test]
    fn test_delivery_scheduler_stop_preserves_owned_until_cleanup() {
        let core = scheduler(1, 3, 3, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        assert!(!core.register(sub));
        assert!(!core.register(Id::new(2)));
        core.set_dispatch_active(sub, true);
        let running = queue(&core, sub, None);
        let queued = queue(&core, sub, None);
        assert_eq!(core.take_ready(sub), Some(running));
        core.request_receive(sub);
        core.stop_subscription(sub);
        assert_eq!(core.take_receive_reservation(sub), None);
        assert_eq!(core.take_ready(sub), None);
        assert!(!core.unregister(sub));
        core.complete(queued);
        core.handler_finished(running);
        assert!(!core.unregister(sub));
        core.complete(running);
        assert!(core.unregister(sub));
        assert!(!core.unregister(sub));
        assert!(core.take_notifications().is_empty());
    }

    #[test]
    fn test_delivery_scheduler_unordered_leases_have_independent_lanes() {
        let core = scheduler(2, 2, 2, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let first = queue(&core, sub, None);
        let second = queue(&core, sub, None);
        assert_eq!(core.take_ready(sub), Some(first));
        assert_eq!(core.take_ready(sub), Some(second));
    }

    /// Checks private capacity and phase invariants after adversarial
    /// transitions.
    fn assert_invariants(core: &DeliverySchedulerCore) {
        let state = core.lock();
        assert!(state.owned.len() <= state.config.max_owned_deliveries().get());
        assert!(state.running <= state.config.max_running_handlers().get());
        assert_eq!(
            state.running,
            state
                .owned
                .values()
                .filter(|record| record.phase == OwnedDeliveryPhase::Running)
                .count()
        );
        assert!(state.notifications.len() <= state.subscriptions.len());
        assert!(state.receive_waiters.len() <= state.subscriptions.len());
        assert!(state.ready.len() <= state.subscriptions.len());
        for (id, sub) in &state.subscriptions {
            assert_eq!(
                sub.owned,
                state
                    .owned
                    .values()
                    .filter(|record| record.subscription_id == *id)
                    .count()
            );
            assert!(sub.owned <= state.config.max_owned_per_subscription().get());
            assert!(sub.lanes.len() <= sub.owned);
            assert!(sub.locked_lanes.len() <= sub.owned);
            for (_, queue) in &sub.lanes {
                assert!(!queue.is_empty());
                for lease in queue {
                    assert!(matches!(
                        state.owned[lease].phase,
                        OwnedDeliveryPhase::Queued | OwnedDeliveryPhase::QueuedSettlement
                    ));
                }
            }
        }
    }

    #[test]
    fn test_delivery_scheduler_exhaustion_rejects_without_wrapping() {
        let core = scheduler(1, 2, 1, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(second));
        core.lock().next_lease = Some(u64::MAX);
        core.request_receive(first);
        assert_eq!(core.take_receive_reservation(first), Some(u64::MAX));
        assert!(core.lease_ids_exhausted());
        core.complete(u64::MAX);
        let _ = core.take_notifications();
        core.request_receive(second);
        assert_eq!(core.take_receive_reservation(second), None);
        assert!(core.take_notifications().contains(&second));
        assert!(core.lock().owned.is_empty());
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_receive_waiters_rotate_and_deduplicate() {
        let core = scheduler(1, 1, 1, 3);
        let first = Id::new(1);
        let second = Id::new(2);
        let third = Id::new(3);
        for id in [first, second, third] {
            assert!(core.register(id));
        }
        core.request_receive(first);
        let initial = core
            .take_receive_reservation(first)
            .expect("initial credit");
        for _ in 0..100 {
            core.request_receive(second);
            core.request_receive(third);
        }
        assert_eq!(core.lock().receive_waiters.len(), 2);
        core.complete(initial);
        let next = core
            .take_receive_reservation(second)
            .expect("first waiter receives credit");
        core.request_receive(first);
        core.complete(next);
        let last = core
            .take_receive_reservation(third)
            .expect("second waiter precedes new requester");
        core.complete(last);
        assert!(core.take_receive_reservation(first).is_some());
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_missing_ordering_key_is_a_shared_lane() {
        let core = scheduler(2, 2, 2, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let mut leases = Vec::new();
        for _ in 0..2 {
            core.request_receive(sub);
            let lease = core.take_receive_reservation(sub).expect("receive credit");
            core.enqueue(lease, Some(OrderingLaneKey::new("topic", None, sub)));
            leases.push(lease);
        }
        assert_eq!(core.take_ready(sub), Some(leases[0]));
        assert_eq!(core.take_ready(sub), None);
        core.handler_finished(leases[0]);
        assert_eq!(core.take_ready(sub), None);
        core.complete(leases[0]);
        assert_eq!(core.take_ready(sub), Some(leases[1]));
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_stop_reserve_grant_complete_race() {
        for iteration in 0..64 {
            let core = Arc::new(scheduler(1, 2, 2, 1));
            let sub = Id::new(1);
            assert!(core.register(sub));
            core.set_dispatch_active(sub, true);
            let barrier = Barrier::new(3);
            scope(|scope| {
                scope.spawn(|| {
                    barrier.wait();
                    core.request_receive(sub);
                    if let Some(lease) = core.take_receive_reservation(sub) {
                        if iteration % 2 == 0 {
                            core.enqueue_settlement(lease, None);
                            let _ = core.take_settlement_ready(sub);
                        } else {
                            core.enqueue(lease, None);
                            let _ = core.take_ready(sub);
                        }
                        core.handler_finished(lease);
                        core.complete(lease);
                        core.complete(lease);
                    }
                });
                scope.spawn(|| {
                    barrier.wait();
                    core.stop_subscription(sub);
                    assert_eq!(
                        core.take_ready(sub),
                        None,
                        "no grant may cross the stop fence"
                    );
                    assert_eq!(
                        core.take_settlement_ready(sub),
                        None,
                        "no settlement grant may cross the stop fence"
                    );
                    core.request_receive(sub);
                    assert_eq!(core.take_receive_reservation(sub), None);
                });
                barrier.wait();
            });
            assert_invariants(&core);
            assert!(core.lock().owned.is_empty());
            assert_eq!(core.lock().running, 0);
            assert!(core.unregister(sub));
            assert!(core.take_notifications().is_empty());
        }
    }

    #[test]
    fn test_delivery_scheduler_complete_request_race_preserves_notification() {
        for _ in 0..64 {
            let core = scheduler(1, 1, 1, 2);
            let first = Id::new(1);
            let second = Id::new(2);
            assert!(core.register(first));
            assert!(core.register(second));
            core.request_receive(first);
            let lease = core.take_receive_reservation(first).expect("first credit");
            let _ = core.take_notifications();
            let barrier = Barrier::new(3);
            scope(|scope| {
                scope.spawn(|| {
                    barrier.wait();
                    core.complete(lease);
                    core.complete(lease);
                });
                scope.spawn(|| {
                    barrier.wait();
                    core.request_receive(second);
                });
                barrier.wait();
            });
            assert!(
                core.take_notifications().contains(&second),
                "granted receiver must retain its wake"
            );
            assert!(core.take_receive_reservation(second).is_some());
            assert_invariants(&core);
        }
    }

    #[test]
    fn test_delivery_scheduler_registration_race_and_bounded_notifications() {
        let core = scheduler(1, 1, 1, 1);
        let sub = Id::new(1);
        let barrier = Barrier::new(3);
        let (first, second) = scope(|scope| {
            let first = scope.spawn(|| {
                barrier.wait();
                core.register(sub)
            });
            let second = scope.spawn(|| {
                barrier.wait();
                core.register(sub)
            });
            barrier.wait();
            (
                first.join().expect("registration thread"),
                second.join().expect("registration thread"),
            )
        });
        assert_ne!(first, second, "exactly one registration wins");
        for _ in 0..100 {
            core.request_receive(sub);
            core.stop_subscription(sub);
            core.set_dispatch_active(sub, true);
        }
        assert_eq!(core.take_notifications(), vec![sub]);
        assert_invariants(&core);
        assert!(core.unregister(sub));
        assert!(core.take_notifications().is_empty());
    }

    #[test]
    fn test_delivery_scheduler_queued_cleanup_cannot_unlock_running_lane() {
        let core = scheduler(2, 3, 3, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let running = queue(&core, sub, Some("a"));
        let cancelled = queue(&core, sub, Some("a"));
        let waiting = queue(&core, sub, Some("a"));
        assert_eq!(core.take_ready(sub), Some(running));
        core.complete(cancelled);
        assert_eq!(core.take_ready(sub), None);
        core.handler_finished(running);
        core.handler_finished(running);
        assert_eq!(core.take_ready(sub), None);
        assert_invariants(&core);
        core.complete(running);
        core.complete(running);
        assert_eq!(core.take_ready(sub), Some(waiting));
        core.complete(waiting);
        assert_invariants(&core);
        let state = core.lock();
        assert!(state.owned.is_empty());
        assert!(state.subscriptions[&sub].lanes.is_empty());
        assert!(state.subscriptions[&sub].locked_lanes.is_empty());
    }

    #[test]
    fn test_delivery_scheduler_cancel_receive_releases_only_unclaimed_and_can_resume() {
        let core = scheduler(1, 1, 1, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(second));
        core.request_receive(first);
        core.request_receive(second);
        let _ = core.take_notifications();
        core.cancel_receive(first);
        assert_eq!(core.take_receive_reservation(first), None);
        let second_lease = core
            .take_receive_reservation(second)
            .expect("unclaimed reservation returned to next waiter");
        assert!(core.take_notifications().contains(&second));
        core.request_receive(first);
        core.cancel_receive(first);
        core.cancel_receive(second);
        assert_eq!(
            core.lock().owned.len(),
            1,
            "claimed reservation is still owned"
        );
        core.complete(second_lease);
        assert_eq!(
            core.take_receive_reservation(first),
            None,
            "cancelled waiter gets no stale grant"
        );
        core.request_receive(first);
        assert!(
            core.take_receive_reservation(first).is_some(),
            "registration remains usable after cancellation"
        );
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_snapshot_tracks_phases_lane_waiting_and_subscription_scope() {
        let core = scheduler(1, 5, 4, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        let domain = ClockDomain::new();
        let started = MonotonicInstant::new(domain, Duration::from_secs(2));
        let now = MonotonicInstant::new(domain, Duration::from_secs(10));
        assert!(core.register(first));
        assert!(core.register(second));
        core.set_dispatch_active(first, true);
        let mut leases = Vec::new();
        for key in ["a", "a", "b", "b"] {
            core.request_receive(first);
            let lease = core
                .take_receive_reservation(first)
                .expect("receive reservation");
            core.record_owned_start(lease, started);
            core.enqueue(lease, Some(OrderingLaneKey::new("topic", Some(key), first)));
            leases.push(lease);
        }
        core.request_receive(second);
        let before = core
            .snapshot_input(Some(first))
            .at(now)
            .expect("same-domain snapshot");
        assert_eq!(
            (before.queued, before.lane_waiting),
            (4, 2),
            "each ordered lane's tail waits for its FIFO predecessor"
        );
        assert_eq!(core.take_ready(first), Some(leases[0]));
        let global = core.snapshot_input(None).at(now).expect("global snapshot");
        assert_eq!(
            (
                global.reserved_receives,
                global.queued,
                global.running_handlers,
                global.settling
            ),
            (1, 3, 1, 0)
        );
        assert_eq!(global.lane_waiting, 2);
        assert_eq!(global.oldest_owned_age, Some(Duration::from_secs(8)));
        let local = core
            .snapshot_input(Some(second))
            .at(now)
            .expect("unclaimed reservation snapshot");
        assert_eq!(local.reserved_receives, 1);
        assert_eq!(local.queued, 0);
        assert_eq!(local.oldest_owned_age, None);
        core.handler_finished(leases[0]);
        let settling = core
            .snapshot_input(Some(first))
            .at(now)
            .expect("settling snapshot");
        assert_eq!(
            (
                settling.running_handlers,
                settling.settling,
                settling.lane_waiting
            ),
            (0, 1, 2)
        );
        core.complete(leases[0]);
        assert_eq!(
            core.snapshot_input(Some(first))
                .at(now)
                .expect("unlocked snapshot")
                .lane_waiting,
            1
        );
        for lease in leases {
            core.complete(lease);
        }
        core.cancel_receive(second);
        assert_eq!(
            core.snapshot_input(None).at(now).expect("idle snapshot"),
            DeliveryMetricsSnapshot::default()
        );
    }

    #[test]
    fn test_delivery_scheduler_owned_age_start_is_claimed_once_and_released() {
        let core = scheduler(1, 1, 1, 1);
        let sub = Id::new(1);
        let domain = ClockDomain::new();
        let zero = MonotonicInstant::new(domain, Duration::ZERO);
        let now = MonotonicInstant::new(domain, Duration::from_secs(9));
        assert!(core.register(sub));
        core.request_receive(sub);
        let unclaimed = core.lock().subscriptions[&sub]
            .receive_reservation
            .expect("core reservation");
        core.record_owned_start(unclaimed, zero);
        assert_eq!(
            core.snapshot_input(None)
                .at(now)
                .expect("unclaimed snapshot")
                .oldest_owned_age,
            None
        );
        let lease = core
            .take_receive_reservation(sub)
            .expect("claim reservation");
        core.record_owned_start(lease, zero);
        core.record_owned_start(lease, now);
        assert_eq!(
            core.snapshot_input(None)
                .at(now)
                .expect("claimed age")
                .oldest_owned_age,
            Some(Duration::from_secs(9))
        );
        core.enqueue(lease, None);
        core.record_owned_start(lease, now);
        assert_eq!(
            core.snapshot_input(None)
                .at(now)
                .expect("queue age")
                .oldest_owned_age,
            Some(Duration::from_secs(9))
        );
        core.complete(lease);
        assert_eq!(
            core.snapshot_input(None)
                .at(now)
                .expect("completed age")
                .oldest_owned_age,
            None
        );
    }

    #[test]
    fn test_delivery_scheduler_snapshot_rejects_clock_domain_and_order_errors() {
        let core = scheduler(1, 1, 1, 1);
        let sub = Id::new(1);
        let domain = ClockDomain::new();
        let start = MonotonicInstant::new(domain, Duration::from_secs(5));
        assert!(core.register(sub));
        core.request_receive(sub);
        let lease = core
            .take_receive_reservation(sub)
            .expect("claim reservation");
        core.record_owned_start(lease, start);
        let foreign = MonotonicInstant::new(ClockDomain::new(), Duration::from_secs(7));
        assert!(matches!(
            core.snapshot_input(None).at(foreign),
            Err(TimeError::ClockDomainMismatch { .. })
        ));
        let earlier = MonotonicInstant::new(domain, Duration::from_secs(4));
        assert!(matches!(
            core.snapshot_input(None).at(earlier),
            Err(TimeError::InvalidInstantOrder { .. })
        ));
        assert_eq!(
            core.snapshot_input(Some(Id::new(99)))
                .at(foreign)
                .expect("unrelated subscription has no timestamp"),
            DeliveryMetricsSnapshot::default()
        );
    }

    #[test]
    fn test_delivery_scheduler_registration_cycles_reclaim_all_metadata() {
        let core = scheduler(1, 1, 1, 1);
        let now = MonotonicInstant::new(ClockDomain::new(), Duration::ZERO);
        for value in 1..=1000 {
            let sub = Id::new(value);
            assert!(core.register(sub));
            core.set_dispatch_active(sub, true);
            core.request_receive(sub);
            let lease = core
                .take_receive_reservation(sub)
                .expect("receive reservation");
            core.record_owned_start(lease, now);
            let lane = Some(OrderingLaneKey::new("topic", Some("key"), sub));
            if value % 2 == 0 {
                core.enqueue_settlement(lease, lane);
                assert_eq!(core.take_settlement_ready(sub), Some(lease));
            } else {
                core.enqueue(lease, lane);
                assert_eq!(core.take_ready(sub), Some(lease));
                core.handler_finished(lease);
            }
            core.complete(lease);
            core.stop_subscription(sub);
            assert!(core.unregister(sub));
            let state = core.lock();
            assert!(state.subscriptions.is_empty());
            assert!(state.owned.is_empty());
            assert!(state.notifications.is_empty());
            assert!(state.receive_waiters.is_empty());
            assert!(state.ready.is_empty());
            assert_eq!(state.running, 0);
        }
        assert_eq!(
            core.snapshot_input(None)
                .at(now)
                .expect("empty final snapshot"),
            DeliveryMetricsSnapshot::default()
        );
    }

    #[test]
    fn test_delivery_scheduler_unordered_rejection_grant_does_not_consume_handler_capacity() {
        let core = scheduler(1, 2, 2, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let running = queue(&core, sub, Some("key"));
        assert_eq!(core.take_ready(sub), Some(running));
        core.request_receive(sub);
        let failed = core
            .take_receive_reservation(sub)
            .expect("decode-failed receive lease");
        core.enqueue_settlement(failed, None);
        assert_eq!(core.take_settlement_ready(sub), Some(failed));
        assert_eq!(core.take_settlement_ready(sub), None);
        let state = core.lock();
        assert_eq!(state.owned[&failed].phase, OwnedDeliveryPhase::Settling);
        assert!(state.owned[&failed].lane.is_none());
        assert_eq!(
            state.running, 1,
            "decode failure never consumes handler capacity"
        );
        assert_eq!(state.owned.len(), 2);
        assert_eq!(state.subscriptions[&sub].receive_reservation, None);
        drop(state);
        core.complete(failed);
        assert_eq!(core.take_ready(sub), None);
        core.complete(running);
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_snapshot_gauges_survives_invalid_age_clock() {
        let core = scheduler(1, 1, 1, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.request_receive(sub);
        let lease = core
            .take_receive_reservation(sub)
            .expect("receive reservation");
        core.record_owned_start(
            lease,
            MonotonicInstant::new(ClockDomain::new(), Duration::ZERO),
        );
        assert!(
            core.snapshot_input(None)
                .at(MonotonicInstant::new(ClockDomain::new(), Duration::ZERO))
                .is_err()
        );
        let gauges = core.snapshot_gauges(Some(sub));
        assert_eq!(gauges.reserved_receives, 1);
        assert_eq!(gauges.oldest_owned_age, None);
        assert_eq!(gauges.completed, 0);
    }

    #[test]
    fn test_delivery_scheduler_snapshot_capture_excludes_later_owned_starts() {
        let core = scheduler(1, 2, 2, 1);
        let sub = Id::new(1);
        let domain = ClockDomain::new();
        let initial = MonotonicInstant::new(domain, Duration::ZERO);
        let sampled_after_capture = MonotonicInstant::new(domain, Duration::from_secs(5));
        let later_start = MonotonicInstant::new(domain, Duration::from_secs(10));
        assert!(core.register(sub));
        core.request_receive(sub);
        let first = core
            .take_receive_reservation(sub)
            .expect("first reservation");
        core.record_owned_start(first, initial);
        core.enqueue(first, None);
        let captured = core.snapshot_input(Some(sub));
        core.request_receive(sub);
        let later = core
            .take_receive_reservation(sub)
            .expect("later reservation");
        core.record_owned_start(later, later_start);
        let snapshot = captured
            .at(sampled_after_capture)
            .expect("post-capture lease cannot invalidate this sample");
        assert_eq!(snapshot.oldest_owned_age, Some(Duration::from_secs(5)));
        assert_eq!((snapshot.queued, snapshot.reserved_receives), (1, 0));
        assert_eq!(
            core.snapshot_gauges(Some(sub)).reserved_receives,
            1,
            "later lease is live but not part of the capture"
        );
        core.complete(first);
        core.complete(later);
        assert_invariants(&core);
    }

    /// Receives a lease whose adapter must wait for a settlement-only lane
    /// grant.
    fn queue_settlement(core: &DeliverySchedulerCore, sub: Id, key: Option<&str>) -> u64 {
        core.request_receive(sub);
        let lease = core
            .take_receive_reservation(sub)
            .expect("settlement receive credit");
        core.enqueue_settlement(
            lease,
            key.map(|key| OrderingLaneKey::new("topic", Some(key), sub)),
        );
        lease
    }

    #[test]
    fn test_delivery_scheduler_rejection_waits_for_predecessor_and_blocks_same_lane_successor() {
        let core = scheduler(1, 4, 4, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let predecessor = queue(&core, sub, Some("a"));
        assert_eq!(core.take_ready(sub), Some(predecessor));
        let rejection = queue_settlement(&core, sub, Some("a"));
        assert_eq!(
            core.snapshot_gauges(Some(sub)).queued,
            1,
            "decode rejection must queue for its lane"
        );
        let successor = queue(&core, sub, Some("a"));
        let different = queue(&core, sub, Some("b"));
        assert_eq!(
            core.take_settlement_ready(sub),
            None,
            "running predecessor holds the lane"
        );
        core.handler_finished(predecessor);
        assert_eq!(
            core.take_settlement_ready(sub),
            None,
            "settling predecessor still holds the lane"
        );
        assert_eq!(
            core.take_ready(sub),
            Some(different),
            "other key can consume the free handler slot"
        );
        core.complete(predecessor);
        assert_eq!(
            core.take_settlement_ready(sub),
            Some(rejection),
            "settlement-only grant works even with H full"
        );
        let gauges = core.snapshot_gauges(Some(sub));
        assert_eq!(
            (
                gauges.queued,
                gauges.lane_waiting,
                gauges.running_handlers,
                gauges.settling
            ),
            (1, 1, 1, 1)
        );
        core.complete(different);
        assert_eq!(
            core.take_ready(sub),
            None,
            "same-key successor cannot bypass Reject backoff"
        );
        core.complete(rejection);
        assert_eq!(core.take_ready(sub), Some(successor));
        core.complete(successor);
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_settlement_grants_bypass_full_handler_capacity_but_not_lane_order() {
        let core = scheduler(1, 3, 3, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let running = queue(&core, sub, Some("a"));
        assert_eq!(core.take_ready(sub), Some(running));
        let _ = core.take_notifications();
        let settlement = queue_settlement(&core, sub, Some("b"));
        assert!(
            core.take_notifications().contains(&sub),
            "H saturation must not suppress settlement wake"
        );
        assert_eq!(core.take_settlement_ready(sub), Some(settlement));
        assert_eq!(core.snapshot_gauges(Some(sub)).running_handlers, 1);
        core.complete(settlement);
        core.complete(running);
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_wrong_grant_kind_preserves_both_round_robin_turns() {
        let core = scheduler(2, 4, 3, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(second));
        core.set_dispatch_active(first, true);
        core.set_dispatch_active(second, true);
        let handler = queue(&core, first, Some("a"));
        let first_settlement = queue_settlement(&core, first, Some("b"));
        let second_settlement = queue_settlement(&core, second, Some("c"));
        for _ in 0..3 {
            assert_eq!(core.take_settlement_ready(first), None);
            assert_eq!(core.take_ready(second), None);
        }
        assert_eq!(core.take_ready(first), Some(handler));
        assert_eq!(
            core.take_ready(second),
            None,
            "wrong kind must leave second's turn intact"
        );
        assert_eq!(
            core.take_settlement_ready(first),
            None,
            "first cannot overtake second"
        );
        assert_eq!(core.take_settlement_ready(second), Some(second_settlement));
        assert_eq!(
            core.take_ready(first),
            None,
            "lane RR next selects the queued settlement"
        );
        assert_eq!(core.take_settlement_ready(first), Some(first_settlement));
        for lease in [handler, first_settlement, second_settlement] {
            core.complete(lease);
        }
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_paused_and_stopped_settlement_grants_stay_owned_until_cleanup() {
        let core = scheduler(1, 2, 2, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        let first = queue_settlement(&core, sub, Some("a"));
        assert_eq!(
            core.take_settlement_ready(sub),
            None,
            "registration begins inactive"
        );
        core.set_dispatch_active(sub, true);
        assert_eq!(core.take_settlement_ready(sub), Some(first));
        let queued = queue_settlement(&core, sub, Some("b"));
        core.set_dispatch_active(sub, false);
        assert_eq!(core.take_settlement_ready(sub), None);
        core.stop_subscription(sub);
        core.set_dispatch_active(sub, true);
        assert_eq!(core.take_settlement_ready(sub), None);
        assert!(!core.unregister(sub));
        core.complete(queued);
        core.complete(first);
        assert!(core.unregister(sub));
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_cancelled_queued_rejection_keeps_predecessor_lane_locked() {
        let core = scheduler(1, 3, 3, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let predecessor = queue(&core, sub, Some("a"));
        assert_eq!(core.take_ready(sub), Some(predecessor));
        let rejected = queue_settlement(&core, sub, Some("a"));
        let successor = queue(&core, sub, Some("a"));
        core.complete(rejected);
        core.complete(rejected);
        core.handler_finished(predecessor);
        core.handler_finished(predecessor);
        assert_eq!(
            core.take_ready(sub),
            None,
            "queued rejection cleanup cannot unlock a settling predecessor"
        );
        assert_eq!(core.snapshot_gauges(Some(sub)).lane_waiting, 1);
        core.complete(predecessor);
        assert_eq!(core.take_ready(sub), Some(successor));
        core.complete(successor);
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_full_handler_slots_skip_handler_only_subscription_turn() {
        let core = scheduler(1, 3, 2, 2);
        let first = Id::new(1);
        let second = Id::new(2);
        assert!(core.register(first));
        assert!(core.register(second));
        core.set_dispatch_active(first, true);
        core.set_dispatch_active(second, true);
        let running = queue(&core, first, Some("a"));
        assert_eq!(core.take_ready(first), Some(running));
        let waiting_handler = queue(&core, first, Some("b"));
        let rejection = queue_settlement(&core, second, Some("c"));
        assert_eq!(
            core.take_ready(second),
            None,
            "wrong grant kind must not consume the selected rejection"
        );
        assert_eq!(
            core.take_settlement_ready(second),
            Some(rejection),
            "full H cannot let first's handler-only turn block second's settlement"
        );
        core.handler_finished(running);
        assert_eq!(
            core.take_ready(first),
            Some(waiting_handler),
            "bypassed handler turn remains ready when H is returned"
        );
        for lease in [running, waiting_handler, rejection] {
            core.complete(lease);
        }
        assert_invariants(&core);
    }

    #[test]
    fn test_delivery_scheduler_rejection_cannot_skip_queued_handler_head_when_capacity_is_full() {
        let core = scheduler(1, 3, 3, 1);
        let sub = Id::new(1);
        assert!(core.register(sub));
        core.set_dispatch_active(sub, true);
        let running = queue(&core, sub, None);
        assert_eq!(core.take_ready(sub), Some(running));
        let predecessor = queue(&core, sub, Some("a"));
        let rejection = queue_settlement(&core, sub, Some("a"));
        assert_eq!(
            core.take_settlement_ready(sub),
            None,
            "H-free work cannot skip the handler at this same lane's FIFO head"
        );
        core.complete(running);
        assert_eq!(core.take_settlement_ready(sub), None);
        assert_eq!(core.take_ready(sub), Some(predecessor));
        core.handler_finished(predecessor);
        assert_eq!(core.take_settlement_ready(sub), None);
        core.complete(predecessor);
        assert_eq!(core.take_settlement_ready(sub), Some(rejection));
        core.complete(rejection);
        assert_invariants(&core);
    }
}

// Exercises the real scheduler state transitions with instrumented
// synchronization.
#[cfg(all(test, loom))]
#[path = "delivery_scheduler_core_loom_tests.rs"]
mod loom_tests;
