// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal recovery signal when stop publication precedes handler admission.

use crate::DeliveryError;

/// Requests provider recovery without reporting a successful application call.
#[derive(Debug, thiserror::Error)]
#[error("subscription stopped before handler invocation")]
pub(in crate::facade) struct HandlerStartRejected;

impl HandlerStartRejected {
    /// Identifies this private signal before invoking application error
    /// policies.
    ///
    /// # Parameters
    /// - `error`: direct result of a handler or middleware attempt.
    ///
    /// # Returns
    /// True only for the internal stopped-handler signal.
    #[must_use]
    pub(in crate::facade) fn is_rejection(error: &DeliveryError) -> bool {
        matches!(error, DeliveryError::Handler { source } if source.is::<Self>())
    }
}
