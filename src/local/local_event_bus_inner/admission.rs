// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Delivery admission for the local event bus.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::LocalEventBusInner;
use crate::EventBusError;
use crate::EventBusResult;

mod delivery_permit {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    /// RAII reservation for one accepted subscriber delivery.
    pub(crate) enum DeliveryPermit {
        /// Reservation backed by the shared in-flight counter.
        Counted(Arc<AtomicUsize>),
    }
}

pub(crate) use delivery_permit::DeliveryPermit;

impl Drop for DeliveryPermit {
    /// Releases the delivery reservation when its processing task is dropped.
    fn drop(&mut self) {
        let Self::Counted(in_flight) = self;
        in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LocalEventBusInner {
    /// Acquires one global delivery budget permit.
    ///
    /// # Returns
    /// A permit that releases its in-flight reservation on drop.
    ///
    /// # Errors
    /// Returns [`EventBusError::ExecutionRejected`] when the configured
    /// in-flight delivery limit is saturated.
    pub(crate) fn try_acquire_delivery_permit(&self) -> EventBusResult<DeliveryPermit> {
        let limit = self.delivery_limits.max_in_flight();
        self.in_flight_delivery_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                (current < limit).then_some(current + 1)
            })
            .map(|_| DeliveryPermit::Counted(Arc::clone(&self.in_flight_delivery_count)))
            .map_err(|_| EventBusError::execution_rejected("maximum in-flight deliveries are saturated"))
    }
}
