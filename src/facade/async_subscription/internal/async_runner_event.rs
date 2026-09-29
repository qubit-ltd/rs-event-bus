// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::facade::async_subscription::internal::pending_delivery::PendingDelivery;
use crate::spi::ReceiveOutcome;

/// Event returned by the runner's select loop.
pub(in crate::facade::async_subscription) enum AsyncRunnerEvent<T: 'static> {
    /// One completed owned delivery task.
    Delivery(PendingDelivery<T>),
    /// Result of the provider receive operation.
    Receive(Result<ReceiveOutcome, crate::error::SpiError>),
    /// The stop signal won the current poll.
    Stopped,
}
