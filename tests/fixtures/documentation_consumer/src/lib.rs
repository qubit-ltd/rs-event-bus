// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Consumer fixture that verifies the documented provider service surface.

pub mod provider_spec;

/// Codec from the user guides.
pub mod order_created_codec;

/// Application domain used by the user-guide codec example.
pub mod orders {
    /// Events emitted after the order transaction commits.
    pub mod events {
        /// Order fields carried by the user-guide wire format.
        pub struct OrderCreated {
            /// Committed order identifier.
            pub order_id: String,
            /// Customer owning the order.
            pub customer_id: String,
            /// Order total in cents.
            pub total_cents: u64,
        }
    }
}
