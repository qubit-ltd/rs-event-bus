// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic provider-wide admission budget shared by local queues.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Counts accepted deliveries that are queued or have not reached settlement.
pub(super) struct OutstandingBudget {
    limit: usize,
    used: AtomicUsize,
}

impl OutstandingBudget {
    /// Creates a positive delivery budget.
    pub(super) fn new(limit: usize) -> Self {
        assert!(limit > 0, "outstanding budget must be positive");
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    /// Reserves one delivery slot, returning false when the limit is reached.
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

#[cfg(test)]
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
