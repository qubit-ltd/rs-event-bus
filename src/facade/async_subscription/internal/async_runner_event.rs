// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal asynchronous subscription state.

use super::PendingDelivery;
use crate::spi::ReceiveOutcome;

/// Event returned by the runner's select loop.
///
/// # Type Parameters
/// - `T`: payload type retained by completed delivery tasks.
pub(in crate::facade::async_subscription) enum AsyncRunnerEvent<T: 'static> {
    /// One completed owned delivery task.
    Delivery(
        /// Completed delivery and its retained admission and tracking state.
        PendingDelivery<T>,
    ),
    /// Result of the provider receive operation.
    Receive(
        /// Provider receive result or operation error.
        Result<ReceiveOutcome, crate::error::SpiError>,
    ),
    /// The stop signal won the current poll.
    Stopped,
}
