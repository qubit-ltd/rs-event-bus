// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Limits controlling local subscriber delivery admission and execution queues.

use crate::EventBusError;
use crate::EventBusResult;

/// Default maximum number of accepted subscriber deliveries.
pub const DEFAULT_MAX_IN_FLIGHT_DELIVERIES: usize = 4096;

/// Independent limits for subscriber delivery admission and handler execution.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DeliveryLimits {
    max_in_flight: usize,
    handler_queue_capacity: Option<usize>,
}

impl DeliveryLimits {
    /// Creates delivery limits.
    pub const fn new(max_in_flight: usize, handler_queue_capacity: Option<usize>) -> Self {
        Self {
            max_in_flight,
            handler_queue_capacity,
        }
    }

    /// Returns the maximum number of accepted deliveries.
    pub const fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Returns the optional executor queue capacity.
    pub const fn handler_queue_capacity(&self) -> Option<usize> {
        self.handler_queue_capacity
    }

    /// Validates configured limits.
    pub fn validate(self) -> EventBusResult<Self> {
        if self.max_in_flight == 0 {
            return Err(EventBusError::invalid_argument(
                "max_in_flight",
                "maximum in-flight deliveries must be greater than zero",
            ));
        }
        if self.handler_queue_capacity == Some(0) {
            return Err(EventBusError::invalid_argument(
                "handler_queue_capacity",
                "handler queue capacity must be greater than zero",
            ));
        }
        Ok(self)
    }
}

impl Default for DeliveryLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_IN_FLIGHT_DELIVERIES, None)
    }
}
