// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result of one subscriber handler attempt.

use crate::error::DeliveryError;

/// Outcome after applying the configured acknowledgement mode.
#[must_use]
#[derive(Debug)]
pub(crate) enum DeliveryOutcome {
    /// The handler accepted this delivery attempt.
    Success,
    /// The handler failed or a manual acknowledgement was not affirmative.
    Failure(
        /// Error returned by the handler or acknowledgement policy.
        DeliveryError,
    ),
}
