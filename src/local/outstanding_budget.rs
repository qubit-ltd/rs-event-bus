// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic provider-wide admission budget shared by local queues.

use crate::internal::sync::AtomicUsize;
use crate::internal::sync::Ordering;

/// Counts accepted deliveries that are queued or have not reached settlement.
pub(super) struct OutstandingBudget {
    /// Maximum number of outstanding delivery slots.
    limit: usize,
    /// Current number of reserved slots.
    used: AtomicUsize,
}

impl OutstandingBudget {
    /// Creates a positive delivery budget.
    ///
    /// # Parameters
    /// - `limit`: maximum number of simultaneously reserved delivery slots.
    ///
    /// # Returns
    /// A budget with no slots currently reserved.
    ///
    /// # Panics
    /// Panics when `limit` is zero.
    #[must_use]
    pub(super) fn new(limit: usize) -> Self {
        assert!(limit > 0, "outstanding budget must be positive");
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    /// Reserves one delivery slot, returning false when the limit is reached.
    ///
    /// # Returns
    /// `true` when a slot was reserved, otherwise `false` at capacity.
    #[must_use]
    pub(super) fn try_acquire(&self) -> bool {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            if current >= self.limit {
                return false;
            }
            match self
                .used
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Releases slots for deliveries already removed from their queues.
    ///
    /// # Parameters
    /// - `count`: number of reserved slots released by queue removal.
    ///
    /// # Panics
    /// Panics if `count` exceeds the number of currently reserved slots,
    /// indicating an internal accounting error.
    pub(super) fn release(&self, count: usize) {
        if count == 0 {
            return;
        }
        let previous = self.used.fetch_sub(count, Ordering::AcqRel);
        assert!(previous >= count, "outstanding budget underflow");
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;

    use super::OutstandingBudget;

    /// Checks bounded acquisition and explicit slot reuse after release.
    #[test]
    fn test_outstanding_budget_reuses_released_slots() {
        let budget = OutstandingBudget::new(2);
        assert!(budget.try_acquire());
        assert!(budget.try_acquire());
        assert!(!budget.try_acquire());
        budget.release(1);
        assert!(budget.try_acquire());
        assert!(!budget.try_acquire());
        budget.release(2);
    }

    /// Checks that racing callers cannot reserve more than the configured
    /// limit.
    #[test]
    fn test_outstanding_budget_allows_one_racing_acquisition() {
        let budget = Arc::new(OutstandingBudget::new(1));
        let barrier = Arc::new(Barrier::new(3));
        let first = {
            let budget = Arc::clone(&budget);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                budget.try_acquire()
            })
        };
        let second = {
            let budget = Arc::clone(&budget);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                budget.try_acquire()
            })
        };
        barrier.wait();

        let acquired = usize::from(u8::from(first.join().unwrap())) + usize::from(u8::from(second.join().unwrap()));
        assert_eq!(1, acquired);
    }
}

#[cfg(all(test, loom))]
mod tests {
    use loom::model::Builder;
    use loom::sync::Arc;
    use loom::thread;

    use super::OutstandingBudget;
    use crate::internal::sync::Ordering;

    /// Exhausts schedules with three threads and at most two preemptions.
    fn check_model(model: impl Fn() + Send + Sync + 'static) {
        let mut builder = Builder::new();
        builder.max_threads = 3;
        builder.preemption_bound = Some(2);
        builder.max_branches = 1_000;
        builder.check(model);
    }

    /// Checks real atomic admission cannot overbook the sole delivery slot.
    #[test]
    fn test_loom_production_budget_racing_admission() {
        check_model(|| {
            let budget = Arc::new(OutstandingBudget::new(1));
            let first_budget = Arc::clone(&budget);
            let first = thread::spawn(move || first_budget.try_acquire());
            let second_budget = Arc::clone(&budget);
            let second = thread::spawn(move || second_budget.try_acquire());
            let acquired = usize::from(first.join().expect("first admission completes"))
                + usize::from(second.join().expect("second admission completes"));
            assert_eq!(acquired, 1);
            assert_eq!(budget.used.load(Ordering::Acquire), 1);
            assert!(!budget.try_acquire());
            budget.release(acquired);
            assert_eq!(budget.used.load(Ordering::Acquire), 0);
        });
    }

    /// Checks a release racing reuse never underflows or exceeds capacity.
    #[test]
    fn test_loom_production_budget_release_races_reuse() {
        check_model(|| {
            let budget = Arc::new(OutstandingBudget::new(1));
            assert!(budget.try_acquire());
            let releasing_budget = Arc::clone(&budget);
            let releasing = thread::spawn(move || releasing_budget.release(1));
            let acquiring_budget = Arc::clone(&budget);
            let acquiring = thread::spawn(move || {
                if acquiring_budget.try_acquire() {
                    assert_eq!(acquiring_budget.used.load(Ordering::Acquire), 1);
                    acquiring_budget.release(1);
                }
            });
            releasing.join().expect("original owner releases");
            acquiring.join().expect("new owner completes");
            assert_eq!(budget.used.load(Ordering::Acquire), 0);
            assert!(budget.try_acquire());
            assert!(!budget.try_acquire());
            budget.release(1);
        });
    }

    /// Checks distinct delivery owners release their slots exactly once.
    #[test]
    fn test_loom_production_budget_competing_owner_release() {
        check_model(|| {
            let budget = Arc::new(OutstandingBudget::new(2));
            assert!(budget.try_acquire());
            assert!(budget.try_acquire());
            let first_budget = Arc::clone(&budget);
            let first = thread::spawn(move || first_budget.release(1));
            let second_budget = Arc::clone(&budget);
            let second = thread::spawn(move || second_budget.release(1));
            first.join().expect("first owner releases");
            second.join().expect("second owner releases");
            assert_eq!(budget.used.load(Ordering::Acquire), 0);
            assert!(budget.try_acquire());
            assert!(budget.try_acquire());
            assert!(!budget.try_acquire());
            budget.release(2);
        });
    }
}
