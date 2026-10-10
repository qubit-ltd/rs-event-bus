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
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::SettlementError;
///
/// assert_eq!(SettlementError::AlreadySettled.to_string(), "delivery has already been settled");
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum SettlementError {
    /// The delivery has already received its first terminal decision.
    #[error("delivery has already been settled")]
    AlreadySettled,
    /// The backend rejected the settlement.
    #[error(transparent)]
    Spi(
        /// Failure returned by the provider settlement operation.
        #[from]
        SpiError,
    ),
}
