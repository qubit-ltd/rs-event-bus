// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Delivery settlement failures.

use crate::error::SpiError;

/// A delivery could not be acknowledged or rejected.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SettlementError {
    /// The delivery has already received its first terminal decision.
    #[error("delivery has already been settled")]
    AlreadySettled,
    /// The backend rejected the settlement.
    #[error(transparent)]
    Spi(#[from] SpiError),
}
