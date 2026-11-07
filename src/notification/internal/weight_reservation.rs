// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII release for an accepted notification's declared weight.

use std::sync::Arc;

use super::WeightBudget;

/// Releases one accepted weight when its queued item is destroyed.
pub(in crate::notification) struct WeightReservation {
    /// Budget restored when this guard is dropped.
    budget: Arc<WeightBudget>,
    /// Amount reserved from `budget`.
    weight: usize,
}

impl WeightReservation {
    /// Creates a reservation after its weight was charged to the budget.
    ///
    /// # Parameters
    /// - `budget`: shared budget that owns the reservation.
    /// - `weight`: amount already charged to the budget.
    pub(in crate::notification) fn new(budget: Arc<WeightBudget>, weight: usize) -> Self {
        Self { budget, weight }
    }
}

impl Drop for WeightReservation {
    /// Returns the reserved weight to its admission budget exactly once.
    fn drop(&mut self) {
        self.budget.release(self.weight);
    }
}
