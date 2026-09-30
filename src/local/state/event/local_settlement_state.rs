// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Settlement identity and outcome retained by local deliveries.

use std::sync::Arc;
use std::sync::Mutex;

use crate::spi::DeliveryDisposition;

/// Shared settlement state stored by both the receiver and its opaque token.
pub(in crate::local) type LocalSettlementHandle = Arc<Mutex<LocalSettlementState>>;

/// Remembers the stable token identity and any terminal result for retries.
pub(in crate::local) struct LocalSettlementState {
    /// Per-delivery key that selects its in-flight queue entry.
    pub(in crate::local) token_id: Box<str>,
    /// First successfully applied disposition, if settlement has completed.
    pub(in crate::local) disposition: Option<DeliveryDisposition>,
}
