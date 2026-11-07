// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cumulative declared-weight accounting for notification admission.

use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::sync::PoisonError;

/// A limit and mutex-protected outstanding declared weight.
pub(in crate::notification) struct WeightBudget {
    /// Maximum cumulative outstanding declared weight.
    limit: usize,
    /// Total weight currently reserved by queued and in-flight entries.
    used: Mutex<usize>,
}

impl WeightBudget {
    /// Creates an empty budget with a nonzero limit.
    ///
    /// # Parameters
    /// - `limit`: maximum cumulative outstanding weight.
    pub(in crate::notification) fn new(limit: NonZeroUsize) -> Self {
        Self {
            limit: limit.get(),
            used: Mutex::new(0),
        }
    }

    /// Reserves weight if the cumulative total remains within the limit.
    ///
    /// # Parameters
    /// - `weight`: positive weight requested by a notification.
    ///
    /// # Returns
    /// A reservation guard when the request fits, or `None` otherwise.
    pub(in crate::notification) fn reserve(
        self: &std::sync::Arc<Self>,
        weight: NonZeroUsize,
    ) -> Option<super::WeightReservation> {
        let mut used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        let next = used.checked_add(weight.get()).filter(|next| *next <= self.limit)?;
        *used = next;
        Some(super::WeightReservation::new(self.clone(), weight.get()))
    }

    /// Releases a previously admitted weight.
    ///
    /// # Parameters
    /// - `weight`: amount previously charged by the corresponding guard.
    pub(in crate::notification) fn release(&self, weight: usize) {
        let mut used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        *used = used.checked_sub(weight).expect("weight reservation released once");
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::notification::internal::QueuedNotification;
    use crate::notification::internal::WeightBudget;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("test values are nonzero")
    }

    #[test]
    fn test_reservation_releases_once_and_restores_full_capacity() {
        let budget = Arc::new(WeightBudget::new(nz(10)));
        let reservation = budget.reserve(nz(6)).expect("weight is available");
        assert!(budget.reserve(nz(5)).is_none());
        drop(reservation);
        assert!(budget.reserve(nz(10)).is_some());
    }

    #[test]
    fn test_dropping_queued_entries_releases_reservations_during_receiver_unwind() {
        let budget = Arc::new(WeightBudget::new(nz(10)));
        let (checked_tx, checked_rx) = std::sync::mpsc::channel();
        struct ReentrantDrop {
            budget: Arc<WeightBudget>,
            checked: std::sync::mpsc::Sender<bool>,
            probe: NonZeroUsize,
        }
        impl Drop for ReentrantDrop {
            fn drop(&mut self) {
                let released = match self.budget.reserve(self.probe) {
                    Some(reservation) => {
                        drop(reservation);
                        true
                    }
                    None => false,
                };
                let _ = self.checked.send(released);
            }
        }
        let (sender, receiver) = std::sync::mpsc::sync_channel(2);
        sender
            .send(QueuedNotification {
                payload: ReentrantDrop {
                    budget: budget.clone(),
                    checked: checked_tx.clone(),
                    probe: nz(4),
                },
                reservation: Some(budget.reserve(nz(4)).unwrap()),
            })
            .unwrap();
        sender
            .send(QueuedNotification {
                payload: ReentrantDrop {
                    budget: budget.clone(),
                    checked: checked_tx,
                    probe: nz(10),
                },
                reservation: Some(budget.reserve(nz(6)).unwrap()),
            })
            .unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _receiver = receiver;
            panic!("simulate worker unwind while its receiver owns queued items");
        }));
        assert!(result.is_err());
        assert!(checked_rx.recv().unwrap());
        assert!(checked_rx.recv().unwrap());
        assert!(budget.reserve(nz(10)).is_some());
    }
}
