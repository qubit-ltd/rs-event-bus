// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal disposition for a received delivery.

/// Action applied to a delivery through its provider settlement token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeliveryDisposition {
    /// Accept and remove/commit the delivery.
    Accept,
    /// Retry or release the delivery for later consumption.
    Retry,
    /// Reject the delivery without retry.
    Reject,
}
