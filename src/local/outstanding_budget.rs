// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic provider-wide admission budget shared by local queues.

use std::num::NonZeroUsize;
use std::sync::PoisonError;

use super::LocalCapacitySnapshot;
use crate::internal::sync::Mutex;
use crate::model::AdmissionRejectionKind;

/// Integer reservations updated together under the provider admission lock.
#[derive(Default)]
struct OutstandingState {
    /// Number of queued or unsettled delivery copies.
    count_used: usize,
    /// Sum of their declared weights, or zero when weight accounting is
    /// disabled.
    weight_used: usize,
    rejected_subscription_closed: u64,
    rejected_subscription_capacity: u64,
    rejected_provider_count_capacity: u64,
    rejected_provider_weight_capacity: u64,
}

/// Atomically reserves count and declared-weight capacity for delivery copies.
///
/// The mutex only protects integer accounting; it never owns a payload or
/// invokes application callbacks.
pub(super) struct OutstandingBudget {
    /// Maximum number of outstanding delivery copies.
    max_count: usize,
    /// Optional maximum sum of outstanding delivery-copy weight in bytes.
    max_weight: Option<NonZeroUsize>,
    /// Counts updated in one critical section.
    state: Mutex<OutstandingState>,
}

impl OutstandingBudget {
    /// Creates an empty budget with a positive count limit and optional weight
    /// limit.
    ///
    /// # Parameters
    /// - `max_count`: maximum simultaneously reserved delivery copies.
    /// - `max_weight`: positive byte budget, or `None` to ignore weights.
    ///
    /// # Returns
    /// A budget with both counters initially zero.
    ///
    /// # Panics
    /// Panics when `max_count` is zero.
    #[must_use]
    pub(super) fn new(max_count: usize, max_weight: Option<NonZeroUsize>) -> Self {
        assert!(max_count > 0, "outstanding budget must be positive");
        Self {
            max_count,
            max_weight,
            state: Mutex::new(OutstandingState::default()),
        }
    }

    /// Returns whether publications must declare a native payload weight.
    #[must_use]
    #[inline]
    pub(super) fn requires_weight(&self) -> bool {
        self.max_weight.is_some()
    }

    /// Reserves one delivery copy and its weight atomically. Count has stable precedence.
    pub(super) fn try_reserve(&self, weight: usize) -> Result<(), AdmissionRejectionKind> {
        let weight = if self.requires_weight() { weight } else { 0 };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(next_count) = state.count_used.checked_add(1).filter(|count| *count <= self.max_count) else {
            state.rejected_provider_count_capacity = state.rejected_provider_count_capacity.saturating_add(1);
            return Err(AdmissionRejectionKind::ProviderCountCapacity);
        };
        let Some(next_weight) = state
            .weight_used
            .checked_add(weight)
            .filter(|weight| self.max_weight.is_none_or(|limit| *weight <= limit.get()))
        else {
            state.rejected_provider_weight_capacity = state.rejected_provider_weight_capacity.saturating_add(1);
            return Err(AdmissionRejectionKind::ProviderWeightCapacity);
        };
        state.count_used = next_count;
        state.weight_used = next_weight;
        Ok(())
    }

    /// Records a rejection made by a subscription queue while holding the same
    /// lock used for provider-wide reservations.
    pub(super) fn record_rejection(&self, kind: AdmissionRejectionKind) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let count = match kind {
            AdmissionRejectionKind::SubscriptionClosed => &mut state.rejected_subscription_closed,
            AdmissionRejectionKind::SubscriptionCapacity => &mut state.rejected_subscription_capacity,
            _ => return,
        };
        *count = count.saturating_add(1);
    }

    /// Copies configuration, usage, and rejection totals at one budget-lock
    /// observation point.
    pub(super) fn snapshot(&self, queue_capacity: usize) -> LocalCapacitySnapshot {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        LocalCapacitySnapshot {
            outstanding_count: state.count_used,
            max_total_outstanding: self.max_count,
            outstanding_weight_bytes: state.weight_used,
            max_total_outstanding_weight_bytes: self.max_weight,
            queue_capacity,
            rejected_subscription_closed: state.rejected_subscription_closed,
            rejected_subscription_capacity: state.rejected_subscription_capacity,
            rejected_provider_count_capacity: state.rejected_provider_count_capacity,
            rejected_provider_weight_capacity: state.rejected_provider_weight_capacity,
        }
    }

    /// Compatibility helper for existing internal boolean assertions.
    #[cfg(test)]
    #[must_use]
    pub(super) fn try_acquire(&self, weight: usize) -> bool {
        self.try_reserve(weight).is_ok()
    }

    /// Releases only reservations for deliveries already removed from queues.
    ///
    /// # Parameters
    /// - `count`: number of removed delivery copies.
    /// - `weight`: sum of their reserved weights; ignored when disabled.
    ///
    /// # Panics
    /// Panics before changing either counter if count or weight would
    /// underflow, indicating an internal accounting error such as repeated
    /// release.
    pub(super) fn release(&self, count: usize, weight: usize) {
        let weight = if self.requires_weight() { weight } else { 0 };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(state.count_used >= count, "outstanding budget underflow");
        assert!(state.weight_used >= weight, "outstanding weight budget underflow");
        state.count_used -= count;
        state.weight_used -= weight;
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    use super::OutstandingBudget;

    /// Both limits are checked together and released reservations are reusable.
    #[test]
    fn test_outstanding_budget_reuses_count_and_weight() {
        let budget = OutstandingBudget::new(2, NonZeroUsize::new(6));
        assert!(budget.try_acquire(4));
        assert!(!budget.try_acquire(3));
        assert!(budget.try_acquire(2));
        assert!(!budget.try_acquire(0));
        budget.release(1, 4);
        assert!(budget.try_acquire(4));
        budget.release(2, 6);
        assert!(budget.try_acquire(6));
        budget.release(1, 6);
    }

    /// Count rejection wins when both provider-wide limits are exhausted.
    #[test]
    fn test_outstanding_budget_count_rejection_has_priority() {
        let budget = OutstandingBudget::new(1, NonZeroUsize::new(1));
        assert_eq!(budget.try_reserve(1), Ok(()));
        assert_eq!(
            budget.try_reserve(1),
            Err(crate::model::AdmissionRejectionKind::ProviderCountCapacity)
        );
        budget.release(1, 1);
    }

    /// Disabled weight budgets cannot reject arbitrary declared weights.
    #[test]
    fn test_outstanding_budget_disabled_weight_is_ignored() {
        let budget = OutstandingBudget::new(2, None);
        assert!(budget.try_acquire(usize::MAX));
        assert!(budget.try_acquire(usize::MAX));
        budget.release(2, 0);
        assert!(budget.try_acquire(usize::MAX));
    }

    /// Weight addition overflow is rejected without changing either counter.
    #[test]
    fn test_outstanding_budget_rejects_weight_overflow() {
        let budget = OutstandingBudget::new(2, NonZeroUsize::new(usize::MAX));
        assert!(budget.try_acquire(usize::MAX));
        assert_eq!(
            budget.try_reserve(1),
            Err(crate::model::AdmissionRejectionKind::ProviderWeightCapacity)
        );
        budget.release(1, usize::MAX);
        assert!(budget.try_acquire(1));
        budget.release(1, 1);
    }

    /// Count arithmetic cannot wrap even at the largest representable limit.
    #[test]
    fn test_outstanding_budget_rejects_count_overflow() {
        let budget = OutstandingBudget::new(usize::MAX, None);
        budget.state.lock().expect("budget lock").count_used = usize::MAX;
        assert_eq!(
            budget.try_reserve(1),
            Err(crate::model::AdmissionRejectionKind::ProviderCountCapacity)
        );
        budget.release(usize::MAX, 0);
        assert!(budget.try_acquire(1));
    }

    /// A corrupted repeated owner release fails instead of wrapping counters.
    #[test]
    #[should_panic(expected = "outstanding budget underflow")]
    fn test_outstanding_budget_duplicate_release_panics() {
        let budget = OutstandingBudget::new(1, NonZeroUsize::new(4));
        assert!(budget.try_acquire(4));
        budget.release(1, 4);
        budget.release(1, 4);
    }

    /// Weight underflow is checked before any count mutation.
    #[test]
    #[should_panic(expected = "outstanding weight budget underflow")]
    fn test_outstanding_budget_weight_underflow_panics() {
        let budget = OutstandingBudget::new(2, NonZeroUsize::new(4));
        assert!(budget.try_acquire(2));
        budget.release(1, 3);
    }

    #[test]
    fn test_rejection_counters_saturate_and_snapshot_copies_both_usage_and_limits() {
        let budget = OutstandingBudget::new(2, None);
        {
            let mut state = budget.state.lock().expect("budget lock");
            state.rejected_subscription_closed = u64::MAX;
        }
        budget.record_rejection(AdmissionRejectionKind::SubscriptionClosed);
        budget.record_rejection(AdmissionRejectionKind::SubscriptionCapacity);
        assert!(budget.try_reserve(usize::MAX).is_ok());
        let snapshot = budget.snapshot(5);
        assert_eq!(snapshot.outstanding_count, 1);
        assert_eq!(snapshot.max_total_outstanding, 2);
        assert_eq!(snapshot.outstanding_weight_bytes, 0);
        assert_eq!(snapshot.max_total_outstanding_weight_bytes, None);
        assert_eq!(snapshot.queue_capacity, 5);
        assert_eq!(snapshot.rejected_subscription_closed, u64::MAX);
        assert_eq!(snapshot.rejected_subscription_capacity, 1);
        assert_eq!(snapshot.rejected_provider_count_capacity, 0);
        assert_eq!(snapshot.rejected_provider_weight_capacity, 0);
        budget.release(1, 0);
    }

    /// Racing destinations share both the count and weight admission gate.
    #[test]
    fn test_outstanding_budget_allows_one_racing_acquisition() {
        for (count, weight) in [(1, None), (2, NonZeroUsize::new(3))] {
            let budget = Arc::new(OutstandingBudget::new(count, weight));
            let barrier = Arc::new(Barrier::new(3));
            let mut threads = Vec::new();
            for _ in 0..2 {
                let budget = Arc::clone(&budget);
                let barrier = Arc::clone(&barrier);
                threads.push(thread::spawn(move || {
                    barrier.wait();
                    budget.try_acquire(2)
                }));
            }
            barrier.wait();
            let acquired: usize = threads
                .into_iter()
                .map(|thread| usize::from(thread.join().expect("join")))
                .sum();
            assert_eq!(acquired, 1);
            budget.release(1, 2);
            assert!(budget.try_acquire(2));
        }
    }
}

#[cfg(all(test, loom))]
mod tests {
    use std::num::NonZeroUsize;

    use loom::model::Builder;
    use loom::sync::Arc;
    use loom::thread;

    use super::OutstandingBudget;

    /// Exhausts schedules with three threads and at most two preemptions.
    fn check_model(model: impl Fn() + Send + Sync + 'static) {
        let mut builder = Builder::new();
        builder.max_threads = 3;
        builder.preemption_bound = Some(2);
        builder.max_branches = 1_000;
        builder.check(model);
    }

    /// Independent destinations cannot overbook the last weight allowance.
    #[test]
    fn test_loom_production_budget_racing_admission() {
        check_model(|| {
            let budget = Arc::new(OutstandingBudget::new(2, NonZeroUsize::new(3)));
            let first_budget = Arc::clone(&budget);
            let first = thread::spawn(move || first_budget.try_acquire(2));
            let second_budget = Arc::clone(&budget);
            let second = thread::spawn(move || second_budget.try_acquire(2));
            let acquired = usize::from(first.join().expect("first admission completes"))
                + usize::from(second.join().expect("second admission completes"));
            assert_eq!(acquired, 1);
            assert!(!budget.try_acquire(2));
            budget.release(1, 2);
            assert!(budget.try_acquire(3));
            budget.release(1, 3);
        });
    }

    /// Release and reuse never overbook either member of the atomic
    /// reservation.
    #[test]
    fn test_loom_production_budget_release_races_reuse() {
        check_model(|| {
            let budget = Arc::new(OutstandingBudget::new(1, NonZeroUsize::new(5)));
            assert!(budget.try_acquire(5));
            let releasing_budget = Arc::clone(&budget);
            let releasing = thread::spawn(move || releasing_budget.release(1, 5));
            let acquiring_budget = Arc::clone(&budget);
            let acquiring = thread::spawn(move || {
                if acquiring_budget.try_acquire(3) {
                    assert!(!acquiring_budget.try_acquire(1));
                    acquiring_budget.release(1, 3);
                }
            });
            releasing.join().expect("original owner releases");
            acquiring.join().expect("new owner completes");
            assert!(budget.try_acquire(5));
            assert!(!budget.try_acquire(1));
            budget.release(1, 5);
        });
    }

    /// Distinct owners release unequal weights exactly once, permitting reuse.
    #[test]
    fn test_loom_production_budget_competing_owner_release() {
        check_model(|| {
            let budget = Arc::new(OutstandingBudget::new(2, NonZeroUsize::new(5)));
            assert!(budget.try_acquire(2));
            assert!(budget.try_acquire(3));
            let first_budget = Arc::clone(&budget);
            let first = thread::spawn(move || first_budget.release(1, 2));
            let second_budget = Arc::clone(&budget);
            let second = thread::spawn(move || second_budget.release(1, 3));
            first.join().expect("first owner releases");
            second.join().expect("second owner releases");
            assert!(budget.try_acquire(5));
            assert!(!budget.try_acquire(1));
            budget.release(1, 5);
        });
    }

    /// Failed reservations leave usage unchanged, while concurrent snapshots
    /// never observe values above the configured limits.
    #[test]
    fn test_loom_failed_reservation_and_capacity_snapshot_bounds() {
        check_model(|| {
            let budget = Arc::new(OutstandingBudget::new(1, NonZeroUsize::new(2)));
            assert!(budget.try_acquire(2));
            let reader_budget = Arc::clone(&budget);
            let reader = thread::spawn(move || {
                let snapshot = reader_budget.snapshot(3);
                assert!(snapshot.outstanding_count <= snapshot.max_total_outstanding);
                assert!(
                    snapshot.outstanding_weight_bytes <= snapshot.max_total_outstanding_weight_bytes.unwrap().get()
                );
            });
            let before = budget.snapshot(3);
            assert_eq!(
                budget.try_reserve(1),
                Err(crate::model::AdmissionRejectionKind::ProviderCountCapacity)
            );
            let after = budget.snapshot(3);
            assert_eq!(after.outstanding_count, before.outstanding_count);
            assert_eq!(after.outstanding_weight_bytes, before.outstanding_weight_bytes);
            reader.join().expect("snapshot reader completes");
            budget.release(1, 2);
        });
    }
}
