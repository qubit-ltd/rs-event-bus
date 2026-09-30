// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Order fields carried by the user-guide wire format for the example application's committed event.

/// Order fields carried by the user-guide wire format.
pub struct OrderCreated {
    /// Committed order identifier.
    pub order_id: String,
    /// Customer owning the order.
    pub customer_id: String,
    /// Order total in cents.
    pub total_cents: u64,
}
