// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Conversion from synchronous handler return values to delivery results.

use crate::error::DeliveryError;

/// Converts a synchronous subscriber handler's return value into a delivery
/// result.
///
/// Unit denotes successful completion. A returned [`DeliveryError`] is kept
/// as the source of a handler error so diagnostics retain its error chain.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::{DeliveryError, IntoHandlerResult};
///
/// let result = Ok::<(), DeliveryError>(()).into_handler_result();
/// assert!(result.is_ok());
/// ```
pub trait IntoHandlerResult {
    /// Turns an accepted return value into success or a source-preserving
    /// handler error.
    ///
    /// # Returns
    /// `Ok(())` for successful completion, or a [`DeliveryError`] retaining
    /// the handler failure as its source.
    #[must_use]
    fn into_handler_result(self) -> Result<(), DeliveryError>;
}

impl IntoHandlerResult for () {
    /// Treats a handler returning unit as successful completion.
    #[inline]
    fn into_handler_result(self) -> Result<(), DeliveryError> {
        Ok(())
    }
}

impl IntoHandlerResult for Result<(), DeliveryError> {
    /// Preserves a handler error as the source of a delivery failure.
    #[inline]
    fn into_handler_result(self) -> Result<(), DeliveryError> {
        self.map_err(|source| DeliveryError::Handler {
            source: Box::new(source),
        })
    }
}
