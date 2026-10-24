// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Order fields carried by the user-guide wire format for the example application's committed event.

/// Order fields carried by the user-guide wire format.
///
/// # Examples
///
/// ```
/// use event_bus_documentation_consumer::orders::events::OrderCreated;
///
/// let event = OrderCreated {
///     order_id: "order-42".to_owned(),
///     customer_id: "customer-7".to_owned(),
///     total_cents: 2_500,
/// };
/// assert_eq!(event.total_cents, 2_500);
/// ```
pub struct OrderCreated {
    /// Committed order identifier.
    pub order_id: String,
    /// Customer owning the order.
    pub customer_id: String,
    /// Order total in cents.
    pub total_cents: u64,
}
